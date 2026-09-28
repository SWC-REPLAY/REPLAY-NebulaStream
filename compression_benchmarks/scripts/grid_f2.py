#!/usr/bin/env python3
"""Sweep FileStore's segment and the tail tier's unit INDEPENDENTLY.  [f2]

    ./compression_benchmarks/scripts/grid_f2.py --test ... --dataset ... \\
        --configuration chained --points 64:64 64:256 64:1024 64:4096

A point is a PAIR, `filestore_kb:tail_kb`, so the two can be set independently. For a UNIT-SIZE
study, pass them EQUAL -- 16:16 64:64 256:256 1024:1024 4096:4096. That is the only shape that
moves every tier's unit at once:

    FileStore        seals K-byte segments
    LossyStore       one group per segment, so its group is K -- it never consults groupSize when
                     a transformation feeds it (see the declaration in LossyStore.hpp)
    CompressedStore  accumulates to its own segmentSize, which is also K

Splitting the pair looks like it isolates a variable and does the opposite. Holding FileStore at
64 and sweeping the tail pins LossyStore's group at 64, because the group IS the segment. Holding
the tail at 1024 and sweeping FileStore pins CompressedStore's block at 1024, so its data_out is
the same number at every point -- 1,714,529 four times, measuring nothing. Both were tried; both
produced rows that only appear to vary.

The merge factor (tail / filestore) is therefore NOT a useful axis on its own. Unit size is the
variable; the merge factor is a consequence of it, and equal pairs fix it at 1.

The tail must be >= FileStore's segment. Smaller and the first segment already overflows the unit,
so the tail silently runs at the segment size while the CSV claims otherwise.

grid.py and bench.py are deliberately left alone -- the results they produced stay reproducible.
"""

import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
BENCH = REPO / "compression_benchmarks/scripts/bench_f2.py"
RESULTS_DIR = REPO / "compression_benchmarks/working-dir/results"
REPORT_DIR = REPO / "compression_benchmarks/results"
ROWS, ROW_WIDTH = 200001, 28

# Each knob carries its CANONICAL DEFAULT as the last field. Restoring used to mean writing back
# whatever the file happened to say when the run started -- so one interrupted run baked a sweep
# value in as the "original", and every run after that faithfully preserved it. It survived three
# rounds that way, and left LossyStore at 1024 KB while claiming 64. The default is declared here
# instead, checked before the sweep and written back after it, so a leftover cannot propagate.
FILESTORE = (REPO / "nes-store-manager/include/FileStore.hpp",
             re.compile(r"size_t segmentSize = [^;]+;"), "size_t segmentSize = {kb}UZ * 1024UZ;",
             "size_t segmentSize = 64UZ * 1024UZ;")
TAIL = [
    (REPO / "nes-store-manager/include/LossyStore.hpp",
     re.compile(r"size_t groupSize = [^;]+;"), "size_t groupSize = {kb}UZ * 1024UZ;",
     "size_t groupSize = 64UZ * 1024UZ;"),
    (REPO / "nes-store-manager/include/CompressedStore.hpp",
     re.compile(r"size_t segmentSize = [^;]+;"), "size_t segmentSize = {kb}UZ * 1024UZ;",
     # No trailing comment: the pattern captures only as far as the semicolon.
     "size_t segmentSize = 1UZ * 1024UZ * 1024UZ;"),
]
KNOBS = [FILESTORE] + TAIL


def patch(knob, kb: int) -> None:
    path, pattern, template, _default = knob
    text = path.read_text()
    patched, n = pattern.subn(template.format(kb=kb), text, count=1)
    if n != 1:
        raise RuntimeError(f"could not find the size declaration in {path}")
    path.write_text(patched)


def check_or_restore_defaults() -> None:
    """Every knob must read its declared default before a sweep starts, and after one ends."""
    wrong = []
    for path, pattern, _t, default in KNOBS:
        found = pattern.search(path.read_text())
        if not found or found.group(0) != default:
            wrong.append(f"{path.name}: found {found.group(0) if found else '(nothing)'}, "
                         f"expected {default}")
    if wrong:
        raise SystemExit("refusing to start -- these are not at their defaults, so a previous run "
                         "left them patched:\n  " + "\n  ".join(wrong) +
                         "\n\nRestore them and re-run. If you changed a default on purpose, "
                         "update the declaration in KNOBS.")


def restore_defaults() -> None:
    for path, pattern, _t, default in KNOBS:
        path.write_text(pattern.sub(default.replace("\\", "\\\\"), path.read_text(), count=1))


def build() -> None:
    out = subprocess.run(["nix", "develop", "--command", "cmake", "--build", "cmake-build-debug",
                          "--target", "systest", "-j8"], cwd=REPO, capture_output=True, text=True)
    errors = [ln for ln in out.stdout.splitlines() + out.stderr.splitlines() if "error:" in ln]
    if errors:
        raise RuntimeError("build failed:\n" + "\n".join(errors[:5]))


def run_bench(test: Path, repeat: int, dataset: str, configuration: str, tail_kb: int) -> list[dict]:
    subprocess.run([sys.executable, str(BENCH), str(test), "--rows", str(ROWS),
                    "--row-width", str(ROW_WIDTH), "-N", str(repeat), "--dataset", dataset,
                    "--configuration", configuration, "--unit-kb", str(tail_kb)],
                   cwd=REPO, capture_output=True, text=True, check=True)
    with (RESULTS_DIR / f"{test.stem}_summary.csv").open() as handle:
        return list(csv.DictReader(handle))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--test", type=Path, required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--configuration", required=True, choices=["standalone", "chained"])
    ap.add_argument("--points", nargs="+", required=True,
                    help="filestore_kb:tail_kb pairs, e.g. 64:256")
    ap.add_argument("-N", "--repeat", type=int, default=3)
    args = ap.parse_args()
    test = args.test if args.test.is_absolute() else REPO / args.test

    points = []
    for p in args.points:
        fs, tail = (int(x) for x in p.split(":"))
        if tail < fs:
            raise SystemExit(f"point {p}: the tail unit ({tail} KB) is below FileStore's segment "
                             f"({fs} KB). The first segment would overflow the unit, so the tail "
                             f"would run at {fs} KB while the CSV claimed {tail}.")
        points.append((fs, tail))

    check_or_restore_defaults()

    collected: list[dict] = []
    try:
        for fs_kb, tail_kb in points:
            print(f"--- FileStore {fs_kb} KB, tail {tail_kb} KB "
                  f"(merge {tail_kb // fs_kb}x): patching, building, running", flush=True)
            patch(FILESTORE, fs_kb)
            for knob in TAIL:
                patch(knob, tail_kb)
            build()
            collected.extend(run_bench(test, args.repeat, args.dataset, args.configuration, tail_kb))
    finally:
        restore_defaults()
        print("--- headers restored to their declared defaults, rebuilding", flush=True)
        build()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / f"f2_{test.stem}.csv"
    with out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(collected[0]))
        writer.writeheader()
        writer.writerows(collected)
    print(f"\nwrote {out}  ({len(collected)} rows, {len(collected[0])} columns)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
