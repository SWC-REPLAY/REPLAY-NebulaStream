#!/usr/bin/env python3
"""Sweep the unit size against the BENCHMARK build, with a store-free baseline in every row.

    ./compression_benchmarks/scripts/grid_release.py \\
        --test tests/solar_standalone.test --dataset debs_solar_panels \\
        --configuration standalone --points 256:256 1024:1024 4096:4096 16384:16384 \\
        -N 1 --rows 42700000 --prefix solar_standalone_release --filestore-total-mb 4096

Same experiment as grid.py, three things different:

  It builds and runs cmake-build-release, not cmake-build-debug. grid.py hardcodes the debug tree
  and does not forward a systest path, which is why this is a separate script rather than a flag.
  The Debug build is ~88% build overhead on the read path, enough to put FileStore -- which decodes
  nothing -- within 2% of LossyStore at 1%, which runs three decode stages. It also inverts the
  unit-size ranking on encode. Byte counts are identical in both and reproduce digit for digit.

  Every row carries a baseline_s column: the same query with no store at all, from
  tests/solar_baseline.test, measured at each grid point. Without it a store's cost can only be
  quoted as an absolute; with it, it is a difference. On this workload three quarters of a write
  query is the CSV parse, so the absolute is mostly not the store.

  It leaves grid.py and bench.py untouched and imports their machinery -- the knob definitions and
  patch/restore from one, the store-file byte walking and timing parse from the other. Those are
  what produced every published number here and they are not worth reimplementing.

grid.py's rules still apply and are not restated in code: a grid point is `filestore_kb:tail_kb`
and the two must be equal, --filestore-total-mb is for standalone only, and the run refuses to
start unless the headers read their declared defaults.
"""

import argparse
import csv
import importlib.util
import statistics
import subprocess
import time
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
# Benchmark-build results live in their own directory; the Debug sweeps keep results/ 
# itself, so a number from one build is never mistaken for a number from the other.
RESULTS = REPO / "compression_benchmarks/results/benchmark"
SCRIPTS = Path(__file__).resolve().parent


def _load(name: str):
    """Import a sibling script by path. They are scripts, not a package, and both guard their
    main() behind __name__, so importing has no side effects."""
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


bench = _load("bench")
grid = _load("grid")

RELEASE = REPO / "cmake-build-release"
SYSTEST = RELEASE / "nes-systests/systest/systest"
STORE_DIR = RELEASE / "nes-store-manager/working-dir"
# The release tree stages its own testdata now (ENABLE_LARGE_TESTS=ON plus a shared
# ExternalData object store), so nothing here depends on cmake-build-debug surviving.
DATA = REPO / "cmake-build-release/nes-systests/testdata"
BASELINE_TEST = REPO / "compression_benchmarks/tests/solar_baseline.test"

# A DEDICATED scratch directory, outside every build tree, exactly as bench.py does it.
# SystestExecutor.cpp:276 calls std::filesystem::remove_all on --workingDir, so pointing this at a
# directory that contains anything else deletes it. Pointed at cmake-build-release/nes-systests
# once, it removed the systest binary it was about to run.
WORKING_DIR = REPO / "compression_benchmarks/working-dir-release"


def build(jobs: int) -> None:
    """Rebuild systest, at `jobs` parallel compiles.

    This is the hot part of a sweep, not the queries: a query pulls about three cores because the
    source is single-threaded, while an unbounded ninja saturates all twelve for the length of the
    compile. On a thin laptop that is the difference between warm and thermally throttled, and
    capping it costs a couple of minutes per grid point.
    """
    out = subprocess.run(["nix", "develop", "--command", "ninja", "-C", str(RELEASE),
                          "-j", str(jobs), "systest"],
                         cwd=REPO, capture_output=True, text=True)
    if out.returncode != 0:
        errors = [ln for ln in out.stdout.splitlines() + out.stderr.splitlines() if "error" in ln.lower()]
        raise RuntimeError("build failed:\n" + "\n".join(errors[:6]))


def check_working_dir() -> None:
    """Refuse to run if --workingDir would delete something that matters.

    systest calls std::filesystem::remove_all on it (SystestExecutor.cpp:276). Anything sharing
    that directory goes with it, including a build tree and the systest binary itself.
    """
    wd = WORKING_DIR.resolve()
    for other in (SYSTEST.resolve(), RELEASE.resolve(), REPO.resolve(),
                  (REPO / "cmake-build-debug").resolve()):
        if other == wd or wd in other.parents:
            raise SystemExit(f"refusing: systest removes its workingDir, and {wd} contains {other}")


def run_test(test: Path, repeat: int, llp: str) -> dict:
    """{query number: median seconds} over `repeat` runs of one test file."""
    samples: dict[int, list[float]] = {}
    for i in range(repeat):
        timings, _output = bench.run_once(SYSTEST, test, DATA, WORKING_DIR, STORE_DIR, llp)
        if not timings:
            raise RuntimeError(f"{test.name}: run {i + 1} produced no timings -- the queries failed. "
                               f"Without SEQUENTIAL_EXECUTION systest runs a file in REVERSE order "
                               f"and every read-back precedes its write.")
        for q, s in timings.items():
            samples.setdefault(q, []).append(s)
    return {q: statistics.median(v) for q, v in samples.items()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--test", type=Path, required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--configuration", required=True, choices=["standalone", "chained"])
    ap.add_argument("--points", nargs="+", required=True, help="filestore_kb:tail_kb, passed equal")
    ap.add_argument("-N", "--repeat", type=int, default=1)
    ap.add_argument("--rows", type=int, required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--filestore-total-mb", type=int, default=None,
                    help="standalone only; a chain must not raise capacity")
    ap.add_argument("--filestore-files", type=int, default=4)
    ap.add_argument("--no-baseline", action="store_true")
    ap.add_argument("--build-jobs", type=int, default=4,
                    help="parallel compiles. The default is deliberately below the core count: the "
                         "build is the hot part of a sweep, not the queries")
    ap.add_argument("--pause", type=int, default=60,
                    help="seconds to idle after each build and after each grid point, so the "
                         "machine is not held at load for the whole sweep")
    args = ap.parse_args()

    test = args.test if args.test.is_absolute() else REPO / "compression_benchmarks" / args.test
    if not test.is_file():
        print(f"no such test: {test}", file=sys.stderr)
        return 1
    if not SYSTEST.is_file():
        print(f"no systest at {SYSTEST}. Configure the Benchmark tree with "
              f"-DCMAKE_BUILD_TYPE=Benchmark -DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON.", file=sys.stderr)
        return 1
    if args.configuration == "chained" and args.filestore_total_mb:
        print("refusing: --filestore-total-mb on a chain sets the flush threshold to "
              "totalSize/filesAtCapacity, so FileStore would hold nearly the whole dataset raw and "
              "the tail tier would never see it. See grid.py's docstring.", file=sys.stderr)
        return 1

    check_working_dir()
    grid.check_or_restore_defaults()
    llp = bench.dev_shell_library_path()
    if not llp:
        print("could not read LD_LIBRARY_PATH from the dev shell", file=sys.stderr)
        return 1

    configs = bench.parse_configurations(test)
    if not configs:
        print(f"{test.name} has no REPLAYABLE queries", file=sys.stderr)
        return 1

    rows_out = []
    try:
        for point in args.points:
            file_kb, tail_kb = (int(x) for x in point.split(":"))
            print(f"\n--- {file_kb}:{tail_kb} KB — patching and rebuilding", flush=True)
            grid.patch(grid.FILESTORE, file_kb)
            for knob in grid.TAIL:
                grid.patch(knob, tail_kb)
            if args.filestore_total_mb:
                grid.apply_capacity(args.filestore_total_mb, args.filestore_files)
            build(args.build_jobs)
            if args.pause:
                print(f"    cooling {args.pause}s after the build", flush=True)
                time.sleep(args.pause)

            baseline = None
            if not args.no_baseline:
                b = run_test(BASELINE_TEST, args.repeat, llp)
                baseline = b.get(1)
                print(f"    baseline (no store): {baseline:.2f} s", flush=True)

            timings = run_test(test, args.repeat, llp)
            sizes = bench.store_sizes(STORE_DIR)
            forced = bench.forced_parameters(test)

            for write_q in sorted(configs):
                order, bound = configs[write_q]
                enc, dec = timings.get(write_q), timings.get(write_q + 1)
                per_tier = sizes.get(write_q, {})
                data, disk, units = per_tier.get("TOTAL", [0, 0, 0])
                row = {
                    "dataset": args.dataset, "configuration": args.configuration,
                    "store_order": order, "tail_tier": order.split("->")[-1], "bound_pct": bound,
                    "unit_kb": tail_kb, "filestore_kb": file_kb, "rows": args.rows,
                    "row_width_bytes": grid.ROW_WIDTH,
                    "bytes_in": args.rows * grid.ROW_WIDTH,
                    "baseline_s": f"{baseline:.4f}" if baseline else "",
                    "encode_s": f"{enc:.4f}" if enc else "",
                    "decode_s": f"{dec:.4f}" if dec else "",
                    "store_write_s": f"{enc - baseline:.4f}" if enc and baseline else "",
                    "data_out": data, "disk_bytes": disk, "units": units,
                    "framing_bytes": disk - data if disk else 0,
                    "ratio_on_data": f"{args.rows * grid.ROW_WIDTH / data:.6f}" if data else "",
                    "ratio_on_disk": f"{args.rows * grid.ROW_WIDTH / disk:.6f}" if disk else "",
                    "runs": args.repeat, "build": "Benchmark",
                }
                row.update({k: v for k, v in forced.items()})
                for tier in ["MemoryStore", "FileStore", "CompressedStore", "LossyStore"]:
                    t = per_tier.get(tier, [0, 0, 0])
                    row[f"{tier}_data"], row[f"{tier}_disk"] = t[0], t[1]
                rows_out.append(row)
            print(f"    {len(configs)} configurations recorded", flush=True)
            if args.pause:
                print(f"    cooling {args.pause}s before the next point", flush=True)
                time.sleep(args.pause)
    finally:
        grid.restore_defaults()
        print("\nheaders restored to defaults.")

    RESULTS.mkdir(parents=True, exist_ok=True)
    out = RESULTS / f"{args.prefix}.csv"
    with out.open("w", newline="") as handle:
        w = csv.DictWriter(handle, fieldnames=list(rows_out[0]))
        w.writeheader()
        w.writerows(rows_out)
    print(f"  wrote {out.name}: {len(rows_out)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
