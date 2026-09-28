#!/usr/bin/env python3
"""Run the large .test queries through a standalone NES worker and nes-cli."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_DIR = ROOT / "nes-systests" / "benchmark"
DEFAULT_DATA_DIR = ROOT / "build" / "nes-systests" / "testdata"
DEFAULT_CLI = ROOT / "build" / "nes-frontend" / "apps" / "nes-cli"
DEFAULT_WORKER = ROOT / "build" / "nes-single-node-worker" / "nes-single-node-worker"
SCHEMA_PATTERN = re.compile(r"\s*([A-Za-z_][\w.]*)\s+([A-Za-z][A-Za-z0-9]*)\s*(NOT\s+NULL)?\s*", re.I)
LOGICAL_PATTERN = re.compile(r"CREATE\s+LOGICAL\s+SOURCE\s+(\w+)\s*\((.*?)\)\s*;", re.I | re.S)
PHYSICAL_PATTERN = re.compile(r"CREATE\s+PHYSICAL\s+SOURCE\s+FOR\s+(\w+)\s+TYPE\s+File\s*;\s*ATTACH\s+FILE\s+(\S+)", re.I)
SINK_PATTERN = re.compile(r"CREATE\s+SINK\s+(\w+)\s*\((.*?)\)\s*TYPE\s+(\w+)\s*;", re.I | re.S)
INTO_PATTERN = re.compile(r"\bINTO\s+(\w+)\s*;\s*$", re.I)
EXPECTED_PATTERN = re.compile(r"\s*(\d+)\s*[,\s]+\s*(\d+)\s*")
UDB_TRACE_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_]*\Z")


@dataclass(frozen=True)
class BenchmarkCase:
    key: str
    file: str
    number: int
    query: str
    sink: str
    expected_count: int
    expected_checksum: int


@dataclass(frozen=True)
class BenchmarkFile:
    path: Path
    logical: dict[str, list[dict[str, Any]]]
    physical: dict[str, str]
    sinks: dict[str, tuple[str, list[dict[str, Any]]]]
    cases: list[BenchmarkCase]


def parse_schema(raw_schema: str) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for raw_field in raw_schema.split(","):
        match = SCHEMA_PATTERN.fullmatch(raw_field)
        if match is None:
            raise ValueError(f"Unsupported schema field: {raw_field!r}")
        fields.append(
            {
                "name": match.group(1).replace(".", "$"),
                "type": match.group(2).upper(),
                "nullable": match.group(3) is None,
            }
        )
    return fields


def parse_benchmark(path: Path) -> BenchmarkFile:
    contents = path.read_text(encoding="utf-8")
    logical = {name.upper(): parse_schema(schema) for name, schema in LOGICAL_PATTERN.findall(contents)}
    physical = {name.upper(): attached for name, attached in PHYSICAL_PATTERN.findall(contents)}
    sinks = {
        name.upper(): (sink_type, parse_schema(schema))
        for name, schema, sink_type in SINK_PATTERN.findall(contents)
    }
    if not logical or set(logical) != set(physical):
        raise ValueError(f"Source definitions are incomplete in {path}")

    cases: list[BenchmarkCase] = []
    query_lines: list[str] = []
    awaiting_expected = False
    for line in contents.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if awaiting_expected:
            if not stripped:
                continue
            expected = EXPECTED_PATTERN.fullmatch(stripped)
            if expected is None:
                raise ValueError(f"Unsupported expected result in {path}: {stripped!r}")
            query = "\n".join(query_lines).strip()
            sink_match = INTO_PATTERN.search(query)
            if sink_match is None or sink_match.group(1).upper() not in sinks:
                raise ValueError(f"Unknown named sink for query in {path}: {query[-100:]}")
            number = len(cases) + 1
            cases.append(
                BenchmarkCase(
                    key=f"{path.stem}:{number:02d}",
                    file=path.name,
                    number=number,
                    query=query,
                    sink=sink_match.group(1).upper(),
                    expected_count=int(expected.group(1)),
                    expected_checksum=int(expected.group(2)),
                )
            )
            query_lines = []
            awaiting_expected = False
            continue
        if query_lines:
            if stripped == "----":
                awaiting_expected = True
            else:
                query_lines.append(line)
        elif re.match(r"^SELECT\b", stripped, re.I):
            query_lines = [line]
    if query_lines or awaiting_expected or not cases:
        raise ValueError(f"Unfinished query or no query in {path}")
    return BenchmarkFile(path=path, logical=logical, physical=physical, sinks=sinks, cases=cases)


def load_benchmarks(only: set[str]) -> list[tuple[BenchmarkFile, BenchmarkCase]]:
    selected: list[tuple[BenchmarkFile, BenchmarkCase]] = []
    for path in sorted(BENCHMARK_DIR.glob("*.test")):
        benchmark = parse_benchmark(path)
        selected.extend((benchmark, case) for case in benchmark.cases if not only or case.key in only)
    missing = only - {case.key for _, case in selected}
    if missing:
        raise ValueError(f"Unknown benchmark keys: {', '.join(sorted(missing))}")
    return selected


def with_udb_trace(query: str, trace_name: str) -> str:
    """Attach Undo live-record to this query. The clause has to precede the statement semicolon."""
    if UDB_TRACE_NAME.fullmatch(trace_name) is None:
        raise ValueError(f"Invalid UDB trace name: {trace_name}")
    stripped = query.rstrip()
    if not stripped.endswith(";"):
        raise ValueError("Query does not end with a semicolon")
    return f"{stripped[:-1]} TIME_TRAVEL_UDB {trace_name} TRACE_SIZE 2GB;"


def prepare(
    run_dir: Path, data_dir: Path, host: str, data_address: str, only: set[str],
    udb_trace: str | None = None, udb_trace_key: str | None = None,
) -> list[dict[str, Any]]:
    cases = load_benchmarks(only)
    (run_dir / "topologies").mkdir(parents=True, exist_ok=True)
    (run_dir / "output").mkdir(parents=True, exist_ok=True)
    (run_dir / "state").mkdir(parents=True, exist_ok=True)
    prepared: list[dict[str, Any]] = []
    for benchmark, case in cases:
        sink_type, sink_schema = benchmark.sinks[case.sink]
        if sink_type.upper() != "CHECKSUM":
            raise ValueError(f"{case.key} uses unsupported sink type {sink_type}")
        safe_key = case.key.replace(":", "-")
        output_path = (run_dir / "output" / f"{safe_key}.csv").resolve()
        physical: list[dict[str, Any]] = []
        for source_name, attached_path in benchmark.physical.items():
            source_path = (data_dir / attached_path).resolve(strict=True)
            physical.append(
                {
                    "logical": source_name,
                    "type": "File",
                    "host": host,
                    "parser_config": {"type": "CSV"},
                    "source_config": {"file_path": str(source_path)},
                }
            )
        records_case = udb_trace is not None and udb_trace_key in (None, case.key)
        query = with_udb_trace(case.query, udb_trace) if records_case else case.query
        topology: dict[str, Any] = {
            "query": query,
            "workers": [{"host": host, "data_address": data_address, "max_operators": 100000}],
            "logical": [{"name": name, "schema": schema} for name, schema in benchmark.logical.items()],
            "physical": physical,
            "sinks": [
                {
                    "name": case.sink,
                    "type": sink_type,
                    "host": host,
                    "schema": sink_schema,
                    "config": {"file_path": str(output_path)},
                    "parser_config": {"quote_strings": "true"},
                }
            ],
            "optimizer": {"join_strategy": "HASH_JOIN"},
        }
        topology_path = run_dir / "topologies" / f"{safe_key}.json"
        topology_path.write_text(json.dumps(topology, indent=2) + "\n", encoding="utf-8")
        prepared.append({**asdict(case), "topology": str(topology_path.resolve()), "output": str(output_path)})
    (run_dir / "inventory.json").write_text(json.dumps(prepared, indent=2) + "\n", encoding="utf-8")
    return prepared


def cli_command(cli: Path, topology: str, state_dir: Path, operation: str, query_id: str = "") -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    environment["XDG_STATE_HOME"] = str(state_dir)
    arguments = [str(cli), "-t", topology, operation]
    if query_id:
        arguments.append(query_id)
    return subprocess.run(arguments, text=True, capture_output=True, timeout=240, env=environment, check=False)


def submit_case(cli: Path, state_dir: Path, case: dict[str, Any]) -> dict[str, Any]:
    started_ns = time.monotonic_ns()
    result = cli_command(cli, str(case["topology"]), state_dir, "start")
    query_id = result.stdout.strip().splitlines()[-1] if result.returncode == 0 and result.stdout.strip() else ""
    return {
        **case,
        "query_id": query_id,
        "submit_ns": started_ns,
        "submit_duration_ms": (time.monotonic_ns() - started_ns) / 1_000_000,
        "submit_exit_code": result.returncode,
        "submit_stdout": result.stdout,
        "submit_stderr": result.stderr,
    }


def query_status(cli: Path, state_dir: Path, case: dict[str, Any]) -> dict[str, Any]:
    query_id = str(case.get("query_id", ""))
    if not query_id:
        return {"status": "SubmissionFailed"}
    result = cli_command(cli, str(case["topology"]), state_dir, "status", query_id)
    if result.returncode != 0:
        return {"status": "StatusFailed", "exit_code": result.returncode, "stderr": result.stderr}
    try:
        rows = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"status": "StatusInvalid", "stdout": result.stdout}
    statuses = [str(row.get("query_status", "")) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
    return {"status": statuses[0] if statuses else "Unknown", "rows": rows}


def output_result(path: Path) -> tuple[int, int] | None:
    if not path.is_file():
        return None
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) != 2:
        return None
    match = EXPECTED_PATTERN.fullmatch(lines[-1])
    return (int(match.group(1)), int(match.group(2))) if match else None


def collect(cli: Path, run_dir: Path, manifest: list[dict[str, Any]]) -> list[dict[str, Any]]:
    state_dir = run_dir / "state"
    previous_path = run_dir / "status.json"
    previous_rows = json.loads(previous_path.read_text(encoding="utf-8")) if previous_path.exists() else []
    previous_by_key = {row["key"]: row for row in previous_rows}
    observed_at_ns = time.monotonic_ns()
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, max(1, len(manifest)))) as executor:
        statuses = list(executor.map(lambda case: query_status(cli, state_dir, case), manifest))
    result: list[dict[str, Any]] = []
    for case, status in zip(manifest, statuses, strict=True):
        observed = output_result(Path(str(case["output"])))
        checksum_pass = observed == (case["expected_count"], case["expected_checksum"])
        terminal = checksum_pass or status["status"] in {"Failed", "Stopped", "SubmissionFailed", "StatusFailed"}
        previous_completed_ns = previous_by_key.get(case["key"], {}).get("completed_ns")
        completed_ns = previous_completed_ns if previous_completed_ns is not None else observed_at_ns if terminal else None
        result.append(
            {
                "key": case["key"],
                "query_id": case.get("query_id", ""),
                **status,
                "expected": [case["expected_count"], case["expected_checksum"]],
                "observed": list(observed) if observed else None,
                "checksum_pass": checksum_pass,
                "completed_ns": completed_ns,
                "elapsed_ms": (completed_ns - case["submit_ns"]) / 1_000_000 if completed_ns is not None else None,
            }
        )
    (run_dir / "status.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def start_worker(worker: Path, run_dir: Path, host: str, data_address: str) -> subprocess.Popen[str]:
    log_file = (run_dir / "worker.stdout.log").open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            str(worker),
            f"--grpc={host}",
            f"--data_address={data_address}",
            "--worker.default_query_execution.execution_mode=COMPILER",
            "--worker.query_engine.number_of_worker_threads=16",
            "--worker.number_of_buffers_in_global_buffer_manager=200000",
            "--worker.default_query_execution.slice_cache.enable_slice_cache=true",
        ],
        cwd=run_dir,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    log_file.close()
    try:
        wait_for_worker(host, process)
    except (RuntimeError, TimeoutError):
        process.terminate()
        process.wait(timeout=15)
        raise
    return process


def wait_for_worker(host: str, process: subprocess.Popen[str] | None = None) -> None:
    address, port_text = host.rsplit(":", 1)
    for _ in range(100):
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"Worker exited early with code {process.returncode}")
        try:
            with socket.create_connection((address, int(port_text)), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError("Worker did not open its gRPC port")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["inventory", "prepare", "run", "status", "stop", "verify"])
    parser.add_argument("--run-dir", type=Path, default=ROOT / "build" / "benchmark-run")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--cli", type=Path, default=DEFAULT_CLI)
    parser.add_argument("--worker", type=Path, default=DEFAULT_WORKER)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--host", default="127.0.0.1:18080")
    parser.add_argument("--data-address", default="127.0.0.1:19090")
    parser.add_argument("--only", action="append", default=[], help="Benchmark key such as Nexmark:01")
    parser.add_argument("--udb-trace", help="Record prepared queries with TIME_TRAVEL_UDB under this trace name")
    parser.add_argument("--udb-trace-key", help="Record only this benchmark key; defaults to every prepared query")
    parser.add_argument("--existing-worker", action="store_true")
    parser.add_argument("--submit-only", action="store_true")
    parser.add_argument("--timeout", type=int, default=3600)
    arguments = parser.parse_args()
    run_dir = arguments.run_dir.resolve()
    selected = set(arguments.only)

    if arguments.operation == "inventory":
        print(json.dumps([asdict(case) for _, case in load_benchmarks(selected)], indent=2))
        return 0
    if arguments.operation in {"prepare", "run"}:
        if run_dir.exists() and (run_dir / "manifest.json").exists() and arguments.operation == "run":
            raise FileExistsError(f"Refusing to overwrite existing run: {run_dir}")
        run_dir.mkdir(parents=True, exist_ok=True)
        prepared = prepare(
            run_dir, arguments.data_dir.resolve(), arguments.host, arguments.data_address, selected,
            arguments.udb_trace, arguments.udb_trace_key,
        )
        if arguments.operation == "prepare":
            print(f"Prepared {len(prepared)} cases in {run_dir}")
            return 0
        if arguments.submit_only and not arguments.existing_worker:
            raise ValueError("--submit-only requires --existing-worker")
        worker_process = None if arguments.existing_worker else start_worker(arguments.worker.resolve(), run_dir, arguments.host, arguments.data_address)
        try:
            if arguments.existing_worker:
                wait_for_worker(arguments.host)
            state_dir = run_dir / "state"
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, len(prepared))) as executor:
                manifest = list(executor.map(lambda case: submit_case(arguments.cli.resolve(), state_dir, case), prepared))
            (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            failed_submissions = sum(not case["query_id"] for case in manifest)
            print(f"Submitted {len(manifest) - failed_submissions}/{len(manifest)} queries; manifest: {run_dir / 'manifest.json'}", flush=True)
            if arguments.submit_only:
                return 1 if failed_submissions else 0
            deadline = time.monotonic() + arguments.timeout
            while time.monotonic() < deadline:
                snapshot = collect(arguments.cli.resolve(), run_dir, manifest)
                completed = sum(row["checksum_pass"] for row in snapshot)
                print(f"Checksums complete: {completed}/{len(snapshot)}", flush=True)
                if all(row["checksum_pass"] or row["status"] in {"Failed", "SubmissionFailed", "StatusFailed"} for row in snapshot):
                    return 0 if completed == len(snapshot) else 1
                time.sleep(5)
            print("Timed out; inspect status.json and worker logs", file=sys.stderr)
            return 2
        finally:
            if worker_process is not None and worker_process.poll() is None:
                worker_process.send_signal(signal.SIGTERM)
                try:
                    worker_process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    worker_process.kill()
                    worker_process.wait()

    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    if selected:
        manifest = [case for case in manifest if case["key"] in selected]
    if arguments.operation == "verify":
        output_dir = arguments.output_dir.resolve() if arguments.output_dir else run_dir / "output"
        verification: list[dict[str, Any]] = []
        for case in manifest:
            output_path = output_dir / f"{case['key'].replace(':', '-')}.csv"
            observed = output_result(output_path)
            expected = (case["expected_count"], case["expected_checksum"])
            verification.append(
                {
                    "key": case["key"],
                    "output": str(output_path),
                    "expected": list(expected),
                    "observed": list(observed) if observed else None,
                    "passed": observed == expected,
                }
            )
        result_path = run_dir / f"verification-{output_dir.name}.json"
        result_path.write_text(json.dumps(verification, indent=2) + "\n", encoding="utf-8")
        passed = sum(row["passed"] for row in verification)
        print(f"{output_dir.name}: {passed}/{len(verification)} matching single-record checksums")
        return 0 if passed == len(verification) else 1
    if arguments.operation == "status":
        snapshot = collect(arguments.cli.resolve(), run_dir, manifest)
        print(json.dumps(snapshot, indent=2))
        return 0
    if arguments.operation == "stop":
        submitted = [case for case in manifest if case.get("query_id")]
        cli = arguments.cli.resolve()

        def stop_case(case: dict[str, Any]) -> subprocess.CompletedProcess[str]:
            return cli_command(cli, str(case["topology"]), run_dir / "state", "stop", str(case["query_id"]))

        # Parallel is only safe because QueryCatalog::stopQuery now joins source threads after
        # releasing the catalog mutex; before that fix, concurrent stops deadlocked the worker pool
        # (smoke-3 shadow-backtraces.txt).
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(submitted))) as executor:
            results = list(executor.map(stop_case, submitted))
        for case, result in zip(submitted, results, strict=True):
            print(f"{case['key']}: {result.returncode} {result.stderr.strip()}")
        return 1 if any(result.returncode != 0 for result in results) else 0
    raise AssertionError("Unreachable operation")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"benchmark runner: {error}", file=sys.stderr)
        raise SystemExit(2) from error
