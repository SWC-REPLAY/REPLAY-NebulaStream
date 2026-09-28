"""Safety checks for checksum acceptance, trace naming, and per-run cleanup."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import bench  # noqa: E402


def test_output_requires_one_exact_result_record(tmp_path: Path) -> None:
    output = tmp_path / "query.csv"
    output.write_text("count,checksum\n12,34\n", encoding="utf-8")
    assert bench.valid_output(output, (12, 34))
    assert not bench.valid_output(output, (12, 35))
    output.write_text("count,checksum\n12,34\n12,34\n", encoding="utf-8")
    assert not bench.valid_output(output, (12, 34))


def test_clean_removes_only_heavy_artifacts_of_one_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bench, "RESULTS", tmp_path / "results")
    monkeypatch.setattr(bench, "UDB_RECORDINGS", tmp_path / "udb-rec")
    trial = tmp_path / "results" / "run-1" / "trials" / "offload-1"
    (trial / "images").mkdir(parents=True)
    (trial / "images" / "pages.img").write_bytes(b"large")
    (trial / "shadow-runtime").mkdir()
    (trial / "metrics.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "udb-rec").mkdir()
    recording = tmp_path / "udb-rec" / f"{bench.trace_name_of('run-1', 'offload-1')}.undo"
    recording.write_bytes(b"trace")
    other_run = tmp_path / "results" / "run-2" / "trials" / "offload-1" / "images"
    other_run.mkdir(parents=True)

    bench.clean("run-1")

    assert not (trial / "images").exists()
    assert not (trial / "shadow-runtime").exists()
    assert not recording.exists()
    assert (trial / "metrics.json").exists()
    assert other_run.exists()
    with pytest.raises(ValueError):
        bench.clean("../run-2")


def test_trace_names_are_valid_udb_identifiers() -> None:
    assert re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", bench.trace_name_of("20260928T120000Z", "offload-2"))


def test_stall_watchdog_fails_only_without_cpu_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    samples = iter([(10.0, 0), (20.0, 0), (20.2, 0)])
    now = iter([0.0, 100.0, 200.0, 600.0])
    monkeypatch.setattr(bench, "worker_resources", lambda _holder: next(samples))
    monkeypatch.setattr(bench.time, "monotonic", lambda: next(now))
    watchdog = bench.StallWatchdog(holder=None, stall_seconds=300)  # type: ignore[arg-type]
    watchdog.check("shadow")
    watchdog.check("shadow")
    with pytest.raises(RuntimeError, match="stalled"):
        watchdog.check("shadow")


def test_stall_watchdog_can_be_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bench, "worker_resources", lambda _holder: pytest.fail("sampled while disabled"))
    bench.StallWatchdog(holder=None, stall_seconds=0).check("shadow")  # type: ignore[arg-type]


def test_stall_watchdog_counts_trace_growth_as_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    now = iter([0.0, 100.0, 400.0, 700.0])
    monkeypatch.setattr(bench, "worker_resources", lambda _holder: (5.0, 0))
    monkeypatch.setattr(bench.time, "monotonic", lambda: next(now))
    watchdog = bench.StallWatchdog(holder=None, stall_seconds=300)  # type: ignore[arg-type]
    watchdog.check("shadow", output_bytes=10)
    watchdog.check("shadow", output_bytes=20)
    watchdog.check("shadow", output_bytes=30)


def test_plots_render_without_successful_offloads() -> None:
    import analysis

    trials = analysis.pd.DataFrame([{"trial": "offload-1", "kind": "offload", "success": False}])
    queries = analysis.pd.DataFrame(columns=["trial", "kind", "query", "role", "seconds"])
    for figure in (
        analysis.plot_offload_query(trials), analysis.plot_offload_phases(trials),
        analysis.plot_cohort(queries), analysis.plot_costs(trials),
    ):
        assert figure is not None
