#!/usr/bin/env python3
"""Offload one running query from a busy NES worker to a CRIU-restored shadow and record it there with UDB.

Commands:
  check   verify tools and run a rootless CRIU dump/restore smoke test
  run     baseline and offload trials for one run ID
  report  render the Quarto notebook (it runs the trials if they are missing, then cleans up)
  clean   delete the heavy artifacts of one run ID
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psutil


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = Path(__file__).resolve()
HERE = SCRIPT.parent
RUNNER = HERE / "runner.py"
RESULTS = ROOT / "build" / "criu-bench"
UDB_RECORDINGS = ROOT / "build" / "udb-rec"
WORKER = ROOT / "build" / "nes-single-node-worker" / "nes-single-node-worker"
CLI = ROOT / "build" / "nes-frontend" / "apps" / "nes-cli"
OFFLOAD_KEY = "Nexmark_with_varsized:01"
RUN_ID = re.compile(r"[A-Za-z0-9_-]+\Z")
# Relative on purpose: it resolves against the worker's cwd, which the shadow bind-mounts privately.
UDB_TRIGGER_NAME = "udb-trigger"
WORKER_FLAGS = (
    "--grpc=127.0.0.1:8080 --data_address=127.0.0.1:9090 "
    "--worker.default_query_execution.execution_mode=COMPILER "
    "--worker.query_engine.number_of_worker_threads=16 "
    "--worker.number_of_buffers_in_global_buffer_manager=200000 "
    "--worker.default_query_execution.slice_cache.enable_slice_cache=true"
)
# Everything a trial writes that is large or only needed while it runs.
# Bound on waiting for the other queries to report Stopped on the shadow. They stop while live-record
# serializes the process, so this is generous; the trial continues either way.
SHADOW_STOP_GRACE_SECONDS = 600
# Early enough that the selected query (about 330 s under full load) still has most of its work left
# after the checkpoint: stopping the other 31 queries on the shadow takes 70-140 s (smoke-4, smoke-5),
# and the selected query must still be running when recording starts.
DEFAULT_DELAY_SECONDS = 15
# Default for --stall-seconds (0 disables): a worker with pending work that burns less than
# STALL_CPU_EPSILON_SECONDS of CPU over this window counts as deadlocked.
DEFAULT_STALL_SECONDS = 300
STALL_CPU_EPSILON_SECONDS = 1.0
# Pointer to the one CRIU image kept from a failed offload trial (about 45 GB).
KEPT_FAILED_IMAGE = RESULTS / "kept-failed-image.json"
HEAVY_TRIAL_DIRS = ("images", "runtime", "output", "state", "topologies", "shadow-runtime", "shadow-output", "shadow-state")


def progress(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def run_checked(command: list[str], *, output: Path | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(result.stdout + result.stderr, encoding="utf-8")
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {shlex.join(command)}\n{result.stderr[-2000:]}")
    return result


def run_dir_of(run_id: str) -> Path:
    if RUN_ID.fullmatch(run_id) is None:
        raise ValueError("Run IDs may contain only letters, digits, underscore, and hyphen")
    return RESULTS / run_id


def trace_name_of(run_id: str, trial: str) -> str:
    return "Offload_" + re.sub(r"[^A-Za-z0-9]", "_", f"{run_id}_{trial}")


def resolve_tool(name: str) -> Path:
    found = shutil.which(name)
    if found is None:
        raise RuntimeError(f"{name} is not on PATH; run through `direnv exec .` or `nix develop .`")
    return Path(found).resolve()


def resolve_live_record() -> Path:
    """NES execs UDB_BINARY_PATH directly, so it must be an absolute path to live-record."""
    configured = os.environ.get("UDB_BINARY_PATH", "live-record")
    candidate = Path(configured)
    resolved = candidate.resolve() if candidate.is_file() else resolve_tool(configured)
    os.environ["UDB_BINARY_PATH"] = str(resolved)
    return resolved


def environment_snapshot() -> dict[str, Any]:
    criu = resolve_tool("criu")
    return {
        "git_commit": run_checked(["git", "rev-parse", "HEAD"]).stdout.strip(),
        "git_dirty": bool(run_checked(["git", "status", "--porcelain"]).stdout.strip()),
        "kernel": platform.release(),
        "machine": platform.machine(),
        "logical_cpus": psutil.cpu_count(logical=True),
        "memory_bytes": psutil.virtual_memory().total,
        "python": platform.python_version(),
        "criu_version": run_checked([str(criu), "--version"]).stdout.strip(),
        "live_record": str(resolve_live_record()),
        "harness_sha256": sha256_file(SCRIPT),
        "flake_lock_sha256": sha256_file(ROOT / "flake.lock"),
        "uv_lock_sha256": sha256_file(ROOT / "uv.lock"),
        "worker_flags": WORKER_FLAGS,
    }


# --- Namespaces --------------------------------------------------------------------------------


def enter(holder_pid: int, command: list[str]) -> list[str]:
    return ["nsenter", "--target", str(holder_pid), "--net", "--pid", "--mount", f"--wdns={ROOT}", *command]


@dataclass
class NamespaceHolder:
    """A PID+net+mount namespace kept alive by namespace_init.py; one per worker."""

    launcher: subprocess.Popen[str]
    pid: int

    @classmethod
    def start(cls) -> NamespaceHolder:
        launcher = subprocess.Popen(
            [
                "unshare", "--mount", "--net", "--pid", "--fork", "--mount-proc",
                "sh", "-c", (
                    "mount --make-rprivate / && ip link set lo up && exec "
                    f"{shlex.quote(sys.executable)} {shlex.quote(str(HERE / 'namespace_init.py'))}"
                ),
            ],
            cwd=ROOT, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        assert launcher.stdout is not None
        if launcher.stdout.readline().strip() != "READY":
            assert launcher.stderr is not None
            raise RuntimeError(f"Namespace startup failed: {launcher.stderr.read()}")
        children = Path(f"/proc/{launcher.pid}/task/{launcher.pid}/children").read_text(encoding="utf-8").split()
        if len(children) != 1:
            raise RuntimeError(f"Expected one namespace init child, found {children}")
        return cls(launcher=launcher, pid=int(children[0]))

    def run(self, command: list[str], *, output: Path | None = None) -> subprocess.CompletedProcess[str]:
        return run_checked(enter(self.pid, command), output=output)

    def stop(self) -> None:
        try:
            os.kill(self.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            self.launcher.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.launcher.kill()
            self.launcher.wait(timeout=10)


def in_rootless_namespace(operation: str, run_dir: Path, *extra: str) -> bool:
    """Re-enter this script as root in a fresh user namespace; CRIU runs --unprivileged there."""
    command = [
        "unshare", "--user", "--map-root-user", "--mount", "--net", "--ipc", "--pid",
        "--fork", "--mount-proc", sys.executable, str(SCRIPT), operation, "--trial-dir", str(run_dir), *extra,
    ]
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    (run_dir / "namespace.log").write_text(result.stdout + result.stderr, encoding="utf-8")
    return result.returncode == 0


# --- Worker, workload, CRIU --------------------------------------------------------------------


def launch_worker(holder: NamespaceHolder, runtime: Path, *, arm_udb_trigger: bool) -> subprocess.Popen[str]:
    runtime.mkdir(parents=True)
    trigger_env = f"NES_UDB_RECORD_ON_TRIGGER={UDB_TRIGGER_NAME} " if arm_udb_trigger else ""
    worker_command = (
        f"cd {shlex.quote(str(runtime))} && exec env {trigger_env}setsid {shlex.quote(str(WORKER))} "
        f"{WORKER_FLAGS} </dev/null >worker.stdout.log 2>&1"
    )
    launcher = subprocess.Popen(enter(holder.pid, ["sh", "-c", worker_command]), cwd=ROOT)
    log_path = runtime / "worker.stdout.log"
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if log_path.exists() and "Server listening" in log_path.read_text(encoding="utf-8", errors="replace"):
            return launcher
        if launcher.poll() not in (None, 0):
            raise RuntimeError(f"Worker exited before becoming ready; inspect {log_path}")
        time.sleep(0.1)
    raise RuntimeError(f"Worker did not become ready; inspect {log_path}")


def worker_pid(holder: NamespaceHolder) -> int:
    pids = holder.run(["pidof", "nes-single-node-worker"]).stdout.split()
    if len(pids) != 1:
        raise RuntimeError(f"Expected one worker in namespace, found {pids}")
    return int(pids[0])


def tracer_pid(holder: NamespaceHolder, namespace_pid: int) -> int:
    for line in holder.run(["cat", f"/proc/{namespace_pid}/status"]).stdout.splitlines():
        if line.startswith("TracerPid:"):
            return int(line.split()[1])
    return 0


def runner(holder: NamespaceHolder, operation: str, trial_dir: Path, *arguments: str, log: str) -> None:
    holder.run(
        [sys.executable, str(RUNNER), operation, "--run-dir", str(trial_dir), "--cli", str(CLI), *arguments],
        output=trial_dir / log,
    )


def submit_all(holder: NamespaceHolder, trial_dir: Path, udb_trace: str | None) -> tuple[list[dict[str, Any]], int]:
    arguments = ["--existing-worker", "--submit-only", "--host", "127.0.0.1:8080", "--data-address", "127.0.0.1:9090"]
    if udb_trace is not None:
        arguments += ["--udb-trace", udb_trace, "--udb-trace-key", OFFLOAD_KEY]
    runner(holder, "run", trial_dir, *arguments, log="submit.log")
    submitted_ns = time.monotonic_ns()
    manifest: list[dict[str, Any]] = read_json(trial_dir / "manifest.json")
    if len(manifest) != 32 or any(not case.get("query_id") for case in manifest):
        raise RuntimeError(f"Expected 32 submitted queries; inspect {trial_dir / 'submit.log'}")
    return manifest, submitted_ns


def stop_queries(holder: NamespaceHolder, trial_dir: Path, keys: list[str], log: str) -> None:
    runner(holder, "stop", trial_dir, *[argument for key in keys for argument in ("--only", key)], log=log)


def wait_until_stopped(holder: NamespaceHolder, trial_dir: Path, keys: list[str], deadline: float) -> int:
    """Poll until every key reports Stopped or the deadline passes; returns how many stopped."""
    only = [argument for key in keys for argument in ("--only", key)]
    stopped = 0
    while time.monotonic() < deadline:
        runner(holder, "status", trial_dir, *only, log="status-stop.log")
        rows: list[dict[str, Any]] = read_json(trial_dir / "status.json")
        stopped = sum(row.get("status") == "Stopped" for row in rows)
        if stopped == len(keys):
            break
        # Each poll spawns one nes-cli per query, so a tighter loop would compete with the shadow.
        time.sleep(1)
    return stopped


def criu_command(criu: Path, operation: str, images: Path, pid: int | None = None) -> list[str]:
    command = [str(criu), operation, "-D", str(images), "--unprivileged", "--shell-job", "-v4"]
    if operation == "dump":
        assert pid is not None
        return command + ["-t", str(pid), "--leave-running", "-o", "dump.log"]
    return ["setsid", *command, "--restore-detached", "-o", "restore.log"]


def prepare_restore_namespace(holder: NamespaceHolder) -> None:
    # CRIU must not occupy a PID that belongs to the restored tree.
    holder.run(["sh", "-c", "for i in $(seq 1 2048); do /bin/true; done"])


def restore(holder: NamespaceHolder, criu: Path, images: Path, timeout_seconds: int = 900) -> None:
    """Call prepare_restore_namespace first; it is kept out of this call so restore timing excludes it."""
    result = subprocess.run(enter(holder.pid, criu_command(criu, "restore", images)), text=True, capture_output=True, check=False)
    (images.parent / "restore-command.log").write_text(result.stdout + result.stderr, encoding="utf-8")
    log_path = images / "restore.log"
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if log_path.exists():
            with log_path.open("rb") as log_file:
                log_file.seek(max(0, log_path.stat().st_size - 65536))
                tail = log_file.read().decode("utf-8", errors="replace")
            if "Restore finished successfully" in tail:
                return
            if "Restoring FAILED" in tail:
                raise RuntimeError(f"CRIU restore failed; inspect {log_path}")
        time.sleep(0.2)
    raise TimeoutError(f"CRIU restore did not finish; inspect {log_path}")


def image_bytes(images: Path) -> int:
    return int(run_checked(["du", "-sb", str(images)]).stdout.split()[0])


# --- Output observation ------------------------------------------------------------------------


def valid_output(path: Path, expected: tuple[int, int]) -> bool:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return False
    if len(lines) != 2:
        return False
    numbers = re.fullmatch(r"\s*(\d+)\s*[,\s]+\s*(\d+)\s*", lines[1])
    return numbers is not None and (int(numbers[1]), int(numbers[2])) == expected


def completed_outputs(manifest: list[dict[str, Any]], output_dir: Path) -> set[str]:
    return {
        str(case["key"]) for case in manifest
        if valid_output(
            output_dir / f"{str(case['key']).replace(':', '-')}.csv",
            (int(case["expected_count"]), int(case["expected_checksum"])),
        )
    }


@dataclass
class OutputObserver:
    """Timestamps each checksum file as it becomes valid; polling adds up to 0.5 s of uncertainty."""

    manifest: list[dict[str, Any]]
    output_dir: Path
    completed_ns: dict[str, int]
    stop_event: threading.Event
    thread: threading.Thread

    @classmethod
    def start(cls, manifest: list[dict[str, Any]], output_dir: Path) -> OutputObserver:
        observer = cls(manifest, output_dir, {}, threading.Event(), threading.Thread())

        def observe() -> None:
            while not observer.stop_event.is_set():
                for key in completed_outputs(observer.manifest, observer.output_dir):
                    observer.completed_ns.setdefault(key, time.monotonic_ns())
                observer.stop_event.wait(0.5)

        observer.thread = threading.Thread(target=observe, daemon=True)
        observer.thread.start()
        return observer

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=2)

    def seconds_since(self, start_ns: int) -> dict[str, float]:
        return {key: (done_ns - start_ns) / 1e9 for key, done_ns in self.completed_ns.items()}


def worker_resources(holder: NamespaceHolder) -> tuple[float, int] | None:
    """CPU seconds and RSS of the worker inside this holder's network namespace."""
    namespace_inode = Path(f"/proc/{holder.pid}/ns/net").stat().st_ino
    for process in psutil.process_iter(["pid", "name"]):
        try:
            if Path(process.exe()).name != "nes-single-node-worker":
                continue
            if Path(f"/proc/{process.pid}/ns/net").stat().st_ino != namespace_inode:
                continue
            cpu = process.cpu_times()
            return cpu.user + cpu.system, process.memory_info().rss
        except (OSError, psutil.Error):
            continue
    return None


def trace_bytes(recording: Path) -> int:
    """Size of the trace, finished or still being written by live-record (<name>.undo.part)."""
    for path in (recording, recording.with_name(recording.name + ".part")):
        try:
            return path.stat().st_size
        except FileNotFoundError:
            continue
    return 0


class StallWatchdog:
    """Fails a trial whose worker still has work but has not used CPU for stall_seconds (0 disables).

    A deadlocked worker (smoke-3: every thread parked on futexes) otherwise holds the trial until
    its multi-hour timeout.
    """

    def __init__(self, holder: NamespaceHolder, stall_seconds: int) -> None:
        self.holder = holder
        self.stall_seconds = stall_seconds
        self.last_cpu = -1.0
        self.last_output_bytes = 0
        self.progressed_at = time.monotonic()

    def check(self, role: str, output_bytes: int = 0) -> None:
        """output_bytes: a growing artifact (the UDB trace) that also counts as progress while the worker idles."""
        if self.stall_seconds == 0:
            return
        sampled = worker_resources(self.holder)
        if sampled is None:
            raise RuntimeError(f"The {role} worker exited before its queries finished")
        if sampled[0] > self.last_cpu + STALL_CPU_EPSILON_SECONDS or output_bytes > self.last_output_bytes:
            self.last_cpu, self.last_output_bytes = max(self.last_cpu, sampled[0]), output_bytes
            self.progressed_at = time.monotonic()
        elif time.monotonic() - self.progressed_at > self.stall_seconds:
            raise RuntimeError(f"The {role} worker stalled: no CPU progress for {self.stall_seconds}s")


def wait_for(done: Any, timeout_seconds: int, what: str, poll_seconds: float = 1.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if done():
            return
        time.sleep(poll_seconds)
    raise TimeoutError(f"Timed out waiting for {what}")


# --- Trials ------------------------------------------------------------------------------------


def stop_launcher(launcher: subprocess.Popen[str] | None) -> None:
    if launcher is None:
        return
    try:
        launcher.wait(timeout=5)
    except subprocess.TimeoutExpired:
        launcher.kill()
        launcher.wait(timeout=5)


def baseline_inner(trial_dir: Path, timeout_seconds: int, stall_seconds: int) -> int:
    """All 32 queries on one worker; no CRIU, no recording."""
    run_checked(["ip", "link", "set", "lo", "up"])
    holder = NamespaceHolder.start()
    launcher: subprocess.Popen[str] | None = None
    observer: OutputObserver | None = None
    metrics: dict[str, Any] = {"kind": "baseline", "trial": trial_dir.name, "success": False}
    try:
        launcher = launch_worker(holder, trial_dir / "runtime", arm_udb_trigger=False)
        manifest, submitted_ns = submit_all(holder, trial_dir, None)
        observer = OutputObserver.start(manifest, trial_dir / "output")
        watchdog = StallWatchdog(holder, stall_seconds)

        def all_outputs_done() -> bool:
            if len(observer.completed_ns) == len(manifest):
                return True
            watchdog.check("baseline")
            return False

        wait_for(all_outputs_done, timeout_seconds, "baseline outputs")
        observer.stop()
        metrics.update({
            "original_passed": len(observer.completed_ns),
            "cohort_seconds": (max(observer.completed_ns.values()) - submitted_ns) / 1e9,
            "per_query_seconds": observer.seconds_since(submitted_ns),
            "offload_query_seconds": observer.seconds_since(submitted_ns)[OFFLOAD_KEY],
            "success": True,
        })
        return 0
    except (OSError, RuntimeError, subprocess.TimeoutExpired, TimeoutError, ValueError, KeyError) as error:
        metrics["error"] = str(error)
        return 1
    finally:
        if observer is not None:
            observer.stop()
        write_json(trial_dir / "metrics.json", metrics)
        holder.stop()
        stop_launcher(launcher)


def offload_inner(
    trial_dir: Path, delay_seconds: int, timeout_seconds: int, trace_name: str | None, stall_seconds: int,
) -> int:
    """Checkpoint the busy worker, move OFFLOAD_KEY to a restored shadow, and record it only there."""
    run_checked(["ip", "link", "set", "lo", "up"])
    criu = resolve_tool("criu")
    # Without a trace name this is the control: the same offload with no TIME_TRAVEL_UDB clause at all.
    recorded = trace_name is not None
    if recorded:
        resolve_live_record()
    recording = UDB_RECORDINGS / f"{trace_name}.undo"
    original = NamespaceHolder.start()
    shadow: NamespaceHolder | None = None
    launcher: subprocess.Popen[str] | None = None
    observers: list[OutputObserver] = []
    metrics: dict[str, Any] = {
        "kind": "offload" if recorded else "offload-unrecorded", "trial": trial_dir.name, "offload_key": OFFLOAD_KEY,
        "delay_seconds": delay_seconds, "recording_path": str(recording), "success": False,
    }

    def phase(message: str) -> None:
        with (trial_dir / "progress.log").open("a", encoding="utf-8") as log:
            log.write(f"{time.strftime('%H:%M:%S')} {message}\n")

    try:
        runtime, output, state, images = (trial_dir / name for name in ("runtime", "output", "state", "images"))
        images.mkdir()
        launcher = launch_worker(original, runtime, arm_udb_trigger=True)
        manifest, submitted_ns = submit_all(original, trial_dir, trace_name)
        others = [str(case["key"]) for case in manifest if case["key"] != OFFLOAD_KEY]
        original_observer = OutputObserver.start(manifest, output)
        observers.append(original_observer)
        phase(f"submitted 32 queries; waiting {delay_seconds}s before the checkpoint")
        time.sleep(delay_seconds)
        finished_before_dump = completed_outputs(manifest, output)
        if OFFLOAD_KEY in finished_before_dump:
            raise RuntimeError(f"{OFFLOAD_KEY} finished before the {delay_seconds}s checkpoint; lower --delay")
        # The pressure the checkpoint is taken under: queries still running in the worker at the dump.
        metrics["active_at_checkpoint"] = len(manifest) - len(finished_before_dump)
        pid = worker_pid(original)
        if tracer_pid(original, pid) != 0:
            raise RuntimeError("Original worker is traced before the dump; the deferred UDB trigger fired early")

        dump_started_ns = time.monotonic_ns()
        original.run(criu_command(criu, "dump", images, pid), output=trial_dir / "dump-command.log")
        metrics["dump_seconds"] = (time.monotonic_ns() - dump_started_ns) / 1e9
        # Snapshot before the original writes more; CRIU validates restored file sizes against the dump.
        shadow_dirs = {runtime: trial_dir / "shadow-runtime", output: trial_dir / "shadow-output", state: trial_dir / "shadow-state"}
        for source, copy in shadow_dirs.items():
            shutil.copytree(source, copy)
        metrics["image_bytes"] = image_bytes(images)
        stop_queries(original, trial_dir, [OFFLOAD_KEY], log="stop-original.log")
        phase("dumped; offloaded query stopped on the original")
        # Armed before restore, so live-record attaches as the shadow resumes: stopping the other
        # queries takes about as long as the selected query needs to finish (e2e-1, smoke-4/5/7), so
        # "stop first, record second" loses that race. The trace also holds the others' teardown.
        if recorded:
            (trial_dir / "shadow-runtime" / UDB_TRIGGER_NAME).touch()
        shadow = NamespaceHolder.start()
        for source, copy in shadow_dirs.items():
            shadow.run(["mount", "--bind", str(copy), str(source)])
        prepare_restore_namespace(shadow)
        restore_started_ns = time.monotonic_ns()
        restore(shadow, criu, images)
        restored_ns = time.monotonic_ns()
        metrics["restore_seconds"] = (restored_ns - restore_started_ns) / 1e9
        shadow_observer = OutputObserver.start(
            [case for case in manifest if case["key"] == OFFLOAD_KEY], trial_dir / "shadow-output"
        )
        observers.append(shadow_observer)
        shadow_pid = worker_pid(shadow)
        starting_cpu = (worker_resources(shadow) or (0.0, 0))[0]

        stop_outcome: dict[str, Any] = {}

        def drop_others_on_shadow() -> None:
            try:
                stop_queries(shadow, trial_dir, others, log="stop-shadow.log")
                stop_outcome["stop_rpcs_seconds"] = (time.monotonic_ns() - restored_ns) / 1e9
                stop_outcome["stopped"] = wait_until_stopped(
                    shadow, trial_dir, others, time.monotonic() + SHADOW_STOP_GRACE_SECONDS
                )
                stop_outcome["stop_others_seconds"] = (time.monotonic_ns() - restored_ns) / 1e9
            except (OSError, RuntimeError, ValueError) as error:
                stop_outcome["error"] = str(error)

        # Parallel with the attach wait: the stops must not wait for live-record to attach.
        stopper = threading.Thread(target=drop_others_on_shadow, daemon=True)
        stopper.start()
        if recorded:
            wait_for(lambda: tracer_pid(shadow, shadow_pid) != 0, 120, "live-record to attach", poll_seconds=0.05)
        attached_ns = time.monotonic_ns()
        metrics["attach_seconds"] = (attached_ns - restored_ns) / 1e9
        phase("shadow restored; live-record attached" if recorded else "shadow restored; unrecorded control")
        stopper.join(timeout=SHADOW_STOP_GRACE_SECONDS + 300)
        if "error" in stop_outcome:
            raise RuntimeError(f"Stopping the other queries on the shadow failed: {stop_outcome['error']}")
        metrics["stop_rpcs_seconds"] = stop_outcome.get("stop_rpcs_seconds")
        metrics["stop_others_seconds"] = stop_outcome.get("stop_others_seconds")
        metrics["shadow_others_stopped"] = stop_outcome.get("stopped", 0)
        phase(f"{metrics['shadow_others_stopped']}/31 other queries stopped on the shadow")

        peak_rss = 0
        last_cpu = starting_cpu
        deadline = time.monotonic() + timeout_seconds
        watchdogs = {"shadow": StallWatchdog(shadow, stall_seconds), "original": StallWatchdog(original, stall_seconds)}
        while time.monotonic() < deadline:
            sampled = worker_resources(shadow)
            if sampled is not None:
                last_cpu, peak_rss = max(last_cpu, sampled[0]), max(peak_rss, sampled[1])
            shadow_done = OFFLOAD_KEY in shadow_observer.completed_ns
            original_done = set(others) <= set(original_observer.completed_ns)
            if shadow_done and original_done:
                break
            if not shadow_done:
                # live-record saves after the query terminates; the worker idles while the trace grows.
                watchdogs["shadow"].check("shadow", trace_bytes(recording))
            if not original_done:
                watchdogs["original"].check("original")
            time.sleep(1)
        else:
            raise TimeoutError("Offload trial did not finish in time")
        # The checksum can land before live-record has written the trace (it saves on pipeline terminate).
        if recorded:
            # nes-udb writes the checksum only after the trace is saved, so this should already hold.
            wait_for(recording.is_file, 120, f"UDB trace {recording}")
        original_seconds = original_observer.seconds_since(submitted_ns)
        metrics.update({
            "original_passed": len(set(others) & set(original_observer.completed_ns)),
            "shadow_passed": 1,
            "original_ran_offload_key": OFFLOAD_KEY in original_observer.completed_ns,
            "cohort_seconds": max(original_seconds[key] for key in others),
            "per_query_seconds": {key: original_seconds[key] for key in others},
            "shadow_after_restore_seconds": (shadow_observer.completed_ns[OFFLOAD_KEY] - restored_ns) / 1e9,
            "shadow_after_attach_seconds": (shadow_observer.completed_ns[OFFLOAD_KEY] - attached_ns) / 1e9,
            "offload_query_seconds": (shadow_observer.completed_ns[OFFLOAD_KEY] - submitted_ns) / 1e9,
            "shadow_cpu_seconds": max(0.0, last_cpu - starting_cpu),
            "shadow_peak_rss_bytes": peak_rss,
            "recording_bytes": recording.stat().st_size if recorded else 0,
            "success": True,
        })
        phase("done")
        return 0
    except (OSError, RuntimeError, subprocess.TimeoutExpired, TimeoutError, ValueError, KeyError) as error:
        metrics["error"] = str(error)
        phase(f"failed: {error}")
        return 1
    finally:
        for observer in observers:
            observer.stop()
        write_json(trial_dir / "metrics.json", metrics)
        if shadow is not None:
            shadow.stop()
        original.stop()
        stop_launcher(launcher)


def check_inner(trial_dir: Path) -> int:
    """Rootless CRIU smoke test: dump a sleeping process and restore it in a second namespace."""
    run_checked(["ip", "link", "set", "lo", "up"])
    criu = resolve_tool("criu")
    result = subprocess.run([str(criu), "check", "--unprivileged"], text=True, capture_output=True, check=False)
    (trial_dir / "criu-check.log").write_text(result.stdout + result.stderr, encoding="utf-8")
    original = NamespaceHolder.start()
    shadow: NamespaceHolder | None = None
    try:
        images = trial_dir / "images"
        images.mkdir()
        pid_file = trial_dir / "smoke.pid"
        subprocess.Popen(
            enter(original.pid, ["setsid", "sh", "-c", f"echo $$ > {shlex.quote(str(pid_file))}; exec sleep 120 </dev/null >/dev/null 2>&1"]),
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        wait_for(pid_file.exists, 10, "the smoke process", poll_seconds=0.1)
        pid = int(pid_file.read_text(encoding="utf-8"))
        original.run(criu_command(criu, "dump", images, pid), output=trial_dir / "dump-command.log")
        shadow = NamespaceHolder.start()
        prepare_restore_namespace(shadow)
        restore(shadow, criu, images)
        shadow.run(["kill", "-0", str(pid)])
        return 0
    except (OSError, RuntimeError, subprocess.TimeoutExpired, TimeoutError) as error:
        (trial_dir / "error.log").write_text(str(error), encoding="utf-8")
        return 1
    finally:
        if shadow is not None:
            shadow.stop()
        original.stop()


# --- Commands ----------------------------------------------------------------------------------


def check() -> bool:
    for binary in (WORKER, CLI):
        if not binary.is_file():
            raise RuntimeError(f"{binary} is missing; build nes-single-node-worker and nes-cli first")
    progress(f"live-record: {resolve_live_record()}")
    check_dir = RESULTS / "check"
    shutil.rmtree(check_dir, ignore_errors=True)
    check_dir.mkdir(parents=True)
    passed = in_rootless_namespace("_check", check_dir)
    shutil.rmtree(check_dir / "images", ignore_errors=True)
    progress(f"rootless CRIU dump/restore smoke test {'passed' if passed else f'FAILED; see {check_dir}'}")
    return passed


def keep_failed_image(trial_dir: Path, error: str) -> None:
    """Keep the CRIU image of the latest failed offload trial for post-mortem restores; drop the previous one.

    The image stays in its trial dir because it references that trial's runtime, output, and state trees
    by absolute path; `clean` for that run removes both.
    """
    if KEPT_FAILED_IMAGE.is_file():
        previous = Path(str(read_json(KEPT_FAILED_IMAGE)["images"]))
        if previous != trial_dir / "images":
            shutil.rmtree(previous, ignore_errors=True)
    write_json(KEPT_FAILED_IMAGE, {"images": str(trial_dir / "images"), "trial_dir": str(trial_dir), "error": error})
    progress(f"kept the failed trial's CRIU image for inspection: {trial_dir / 'images'}")


def run_experiment(
    run_id: str, reps: int = 3, delay_seconds: int = DEFAULT_DELAY_SECONDS, timeout_seconds: int = 7200, record: bool = True,
    stall_seconds: int = DEFAULT_STALL_SECONDS,
) -> bool:
    """Run `reps` baseline and `reps` offload trials. A completed run is kept, not repeated."""
    run_dir = run_dir_of(run_id)
    manifest_path = run_dir / "manifest.json"
    if manifest_path.is_file():
        if read_json(manifest_path).get("status") == "complete":
            progress(f"run {run_id} already complete; reusing its results")
            return True
        raise FileExistsError(f"{run_dir} holds an unfinished run; clean it or pick a new run ID")
    if (run_dir / "trials").exists():
        raise FileExistsError(f"{run_dir} holds trials without a manifest; clean it or pick a new run ID")
    # `report` creates the run dir first as Quarto's output dir, so an empty one is expected.
    run_dir.mkdir(parents=True, exist_ok=True)
    trials = [f"baseline-{rep}" for rep in range(1, reps + 1)] + [f"offload-{rep}" for rep in range(1, reps + 1)]
    manifest: dict[str, Any] = {
        "run_id": run_id, "status": "running", "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "settings": {"reps": reps, "delay_seconds": delay_seconds, "timeout_seconds": timeout_seconds, "offload_key": OFFLOAD_KEY, "record": record,
                     "stall_seconds": stall_seconds},
        "environment": environment_snapshot(), "trials": trials,
    }
    write_json(manifest_path, manifest)
    passed = True
    for trial in trials:
        trial_dir = run_dir / "trials" / trial
        trial_dir.mkdir(parents=True)
        progress(f"{trial}: starting ({trial_dir})")
        if trial.startswith("baseline"):
            ok = in_rootless_namespace(
                "_baseline", trial_dir, "--timeout", str(timeout_seconds), "--stall-seconds", str(stall_seconds),
            )
        else:
            ok = in_rootless_namespace(
                "_offload", trial_dir, "--timeout", str(timeout_seconds), "--delay", str(delay_seconds),
                "--stall-seconds", str(stall_seconds),
                *(["--trace-name", trace_name_of(run_id, trial)] if record else []),
            )
        metrics: dict[str, Any] = read_json(trial_dir / "metrics.json") if (trial_dir / "metrics.json").is_file() else {}
        progress(f"{trial}: {'passed' if ok else 'FAILED: ' + str(metrics.get('error', 'see namespace.log'))}")
        for criu_log in (trial_dir / "images").glob("*.log"):
            shutil.copy2(criu_log, trial_dir / f"criu-{criu_log.name}")
        if not ok and trial.startswith("offload") and (trial_dir / "images").is_dir():
            keep_failed_image(trial_dir, str(metrics.get("error", "")))
        else:
            # Images are gigabytes each; keeping several would exhaust disk before the run ends.
            shutil.rmtree(trial_dir / "images", ignore_errors=True)
        passed = passed and ok
    manifest["status"] = "complete" if passed else "failed"
    write_json(manifest_path, manifest)
    return passed


def clean(run_id: str, *, keep_recordings: bool = False) -> list[str]:
    """Delete heavy artifacts of one run; metrics, logs, manifests and the report stay."""
    run_dir = run_dir_of(run_id)
    if not run_dir.is_dir() or run_dir.is_symlink():
        raise FileNotFoundError(run_dir)
    removed: list[str] = []
    for trial_dir in sorted((run_dir / "trials").glob("*")):
        for name in HEAVY_TRIAL_DIRS:
            path = trial_dir / name
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
                removed.append(str(path))
        recording = UDB_RECORDINGS / f"{trace_name_of(run_id, trial_dir.name)}.undo"
        if not keep_recordings and recording.is_file():
            recording.unlink()
            removed.append(str(recording))
    if KEPT_FAILED_IMAGE.is_file() and Path(str(read_json(KEPT_FAILED_IMAGE)["trial_dir"])).is_relative_to(run_dir):
        KEPT_FAILED_IMAGE.unlink()
    write_json(run_dir / "cleanup.json", {"removed": removed, "kept_recordings": keep_recordings})
    return removed


def report(run_id: str, reps: int, delay_seconds: int, keep_artifacts: bool, stall_seconds: int) -> bool:
    run_dir = run_dir_of(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    command = [
        "quarto", "render", str(HERE / "report.qmd"), "--to", "html",
        "--output", f"report-{run_id}.html", "--output-dir", str(run_dir),
        "-P", f"run_id:{run_id}", "-P", f"reps:{reps}", "-P", f"delay:{delay_seconds}",
        "-P", f"keep_artifacts:{str(keep_artifacts).lower()}", "-P", f"stall_seconds:{stall_seconds}",
    ]
    progress(f"rendering the report for {run_id}; the notebook runs any missing trials")
    passed = subprocess.run(command, cwd=HERE, check=False).returncode == 0
    progress(f"report: {run_dir / f'report-{run_id}.html'}" if passed else "report rendering FAILED")
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check", help="verify tools and rootless CRIU")
    for name in ("run", "report"):
        command = commands.add_parser(name)
        command.add_argument("--run-id", default=time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
        command.add_argument("--reps", type=int, default=3, help="baseline and offload trials each (default 3)")
        command.add_argument(
            "--delay", type=int, default=DEFAULT_DELAY_SECONDS,
            help=f"seconds between submission and checkpoint (default {DEFAULT_DELAY_SECONDS})",
        )
        command.add_argument("--timeout", type=int, default=7200)
        command.add_argument(
            "--stall-seconds", type=int, default=DEFAULT_STALL_SECONDS,
            help=f"fail a trial whose worker makes no CPU progress this long; 0 disables (default {DEFAULT_STALL_SECONDS})",
        )
        if name == "run":
            command.add_argument("--no-record", action="store_true", help="control: offload without UDB recording")
        if name == "report":
            command.add_argument("--keep-artifacts", action="store_true", help="keep runtime trees and UDB traces")
    clean_command = commands.add_parser("clean")
    clean_command.add_argument("--run-id", required=True)
    clean_command.add_argument("--keep-recordings", action="store_true")
    for internal in ("_check", "_baseline", "_offload"):
        command = commands.add_parser(internal)
        command.add_argument("--trial-dir", type=Path, required=True)
        command.add_argument("--timeout", type=int, default=7200)
        command.add_argument("--delay", type=int, default=DEFAULT_DELAY_SECONDS)
        command.add_argument("--trace-name")
        command.add_argument("--stall-seconds", type=int, default=DEFAULT_STALL_SECONDS)
    arguments = parser.parse_args()

    match arguments.command:
        case "check":
            return 0 if check() else 1
        case "run":
            progress(f"run ID {arguments.run_id}")
            return 0 if run_experiment(
                arguments.run_id, arguments.reps, arguments.delay, arguments.timeout, record=not arguments.no_record,
                stall_seconds=arguments.stall_seconds,
            ) else 1
        case "report":
            return 0 if report(
                arguments.run_id, arguments.reps, arguments.delay, arguments.keep_artifacts, arguments.stall_seconds,
            ) else 1
        case "clean":
            progress(f"removed {len(clean(arguments.run_id, keep_recordings=arguments.keep_recordings))} paths")
            return 0
        case "_check":
            return check_inner(arguments.trial_dir)
        case "_baseline":
            return baseline_inner(arguments.trial_dir, arguments.timeout, arguments.stall_seconds)
        case "_offload":
            return offload_inner(
                arguments.trial_dir, arguments.delay, arguments.timeout, arguments.trace_name, arguments.stall_seconds,
            )
    raise AssertionError(arguments.command)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileExistsError, FileNotFoundError, OSError, RuntimeError, ValueError) as error:
        print(f"criu-bench: {error}", file=sys.stderr)
        raise SystemExit(2) from error
