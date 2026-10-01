#!/usr/bin/env python3
"""Sweep the unit size a tier seals on, and report what it costs each configuration.

    ./compression_benchmarks/scripts/grid.py --test ... --dataset ... \\
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

Earlier generations are left alone; their results stay reproducible.

f4 adds OPTIONAL capacity knobs on top of the swept ones, via --filestore-total-mb. FileStore's
defaults -- 64 MB across 16 files -- are sized for the 5.6 MB wind input. A STANDALONE run over
1.2 GB needs them raised or the store evicts almost everything; a CHAINED run does not, and raising
them there would also raise the derived flush threshold and stop the chain chaining. Set once for
the run rather than swept, and put back afterwards.
"""

import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
BENCH = REPO / "compression_benchmarks/scripts/bench.py"
RESULTS_DIR = REPO / "compression_benchmarks/working-dir/results"
REPORT_DIR = REPO / "compression_benchmarks/results"
ROW_WIDTH = 28  # stored width; rows come from --rows because f4 runs several input sizes

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

# Set ONCE for the whole sweep and put back afterwards, rather than swept. FileStore's defaults are
# sized for the 5.6 MB wind input: 64 MB spread over 16 files would evict all but the last 64 MB of
# a 1.2 GB solar run, and the CSV would report ratios for a store that had thrown most of the data
# away. Capacity is a precondition of this measurement, not a variable in it.
#
# 4 GB over 4 files puts the 1.2 GB in two 1 GB files. It still ROLLS, which is fine and costs one
# header and table per file; what it cannot do is evict.
# OPTIONAL, via --filestore-total-mb / --filestore-files. Left alone by default, because raising
# them is right for exactly one shape of run.
#
#   STANDALONE: FileStore IS the tail, so it keeps everything. 64 MB against 1.2 GB evicts almost
#               all of it, and the CSV would report ratios for a store that threw the data away.
#               Raise it.
#   CHAINED:    FileStore migrates once its flush threshold is crossed and never accumulates, so it
#               holds at most one segment and cannot evict whatever the capacity is. Worse, the
#               threshold is DERIVED as totalSize / filesAtCapacity -- so raising capacity to
#               4 GB / 4 would set the threshold to 1 GB and make FileStore sit on nearly the whole
#               dataset uncompressed, which is the opposite of a chain. Leave it alone.
FIXED_TEMPLATE = [
    (REPO / "nes-store-manager/include/FileStore.hpp",
     re.compile(r"size_t totalSize = [^;]+;"),
     "size_t totalSize = {mb}UZ * 1024UZ * 1024UZ;",
     "size_t totalSize = 64UZ * 1024UZ * 1024UZ;"),
    (REPO / "nes-store-manager/include/FileStore.hpp",
     re.compile(r"size_t filesAtCapacity = [^;]+;"),
     "size_t filesAtCapacity = {files};",
     "size_t filesAtCapacity = 16;"),
]

# Everything that must read its declared default before a run and be written back after one. The
# capacity knobs are included whether or not they are patched: if a previous run died holding them,
# the next must refuse to start rather than measure at a capacity nobody asked for.
ALL_KNOBS = KNOBS + FIXED_TEMPLATE


def patch(knob, kb: int) -> None:
    path, pattern, template, _default = knob
    text = path.read_text()
    patched, n = pattern.subn(template.format(kb=kb), text, count=1)
    if n != 1:
        raise RuntimeError(f"could not find the size declaration in {path}")
    path.write_text(patched)


def apply_capacity(total_mb: int, files: int) -> None:
    """Raise FileStore's capacity for the duration of the sweep."""
    for path, pattern, template, _default in FIXED_TEMPLATE:
        text = path.read_text()
        patched, n = pattern.subn(template.format(mb=total_mb, files=files), text, count=1)
        if n != 1:
            raise RuntimeError(f"could not find the capacity declaration in {path}")
        path.write_text(patched)


def check_or_restore_defaults() -> None:
    """Every knob must read its declared default before a sweep starts, and after one ends."""
    wrong = []
    for path, pattern, _t, default in ALL_KNOBS:
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
    for path, pattern, _t, default in ALL_KNOBS:
        path.write_text(pattern.sub(default.replace("\\", "\\\\"), path.read_text(), count=1))


def build() -> None:
    out = subprocess.run(["nix", "develop", "--command", "cmake", "--build", "cmake-build-debug",
                          "--target", "systest", "-j8"], cwd=REPO, capture_output=True, text=True)
    errors = [ln for ln in out.stdout.splitlines() + out.stderr.splitlines() if "error:" in ln]
    if errors:
        raise RuntimeError("build failed:\n" + "\n".join(errors[:5]))


def run_bench(test: Path, repeat: int, dataset: str, configuration: str, tail_kb: int, rows: int) -> list[dict]:
    subprocess.run([sys.executable, str(BENCH), str(test), "--rows", str(rows),
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
    ap.add_argument("--rows", type=int, required=True, help="rows in the input CSV")
    ap.add_argument("--prefix", required=True,
                    help="output file stem, e.g. wind_standalone or solar_chained")
    ap.add_argument("--filestore-total-mb", type=int,
                    help="raise FileStore::Config::totalSize for the run. Standalone over a large "
                         "input needs it; a CHAIN does not, and raising it there also raises the "
                         "derived flush threshold. Left at the default when omitted.")
    ap.add_argument("--filestore-files", type=int, default=4,
                    help="filesAtCapacity, only used with --filestore-total-mb")
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
        if args.filestore_total_mb:
            apply_capacity(args.filestore_total_mb, args.filestore_files)
            print(f"--- FileStore capacity raised to {args.filestore_total_mb} MB over "
                  f"{args.filestore_files} files (flush threshold becomes "
                  f"{args.filestore_total_mb // args.filestore_files} MB)", flush=True)
        for fs_kb, tail_kb in points:
            print(f"--- FileStore {fs_kb} KB, tail {tail_kb} KB "
                  f"(merge {tail_kb // fs_kb}x): patching, building, running", flush=True)
            patch(FILESTORE, fs_kb)
            for knob in TAIL:
                patch(knob, tail_kb)
            build()
            collected.extend(run_bench(test, args.repeat, args.dataset, args.configuration, tail_kb, args.rows))
    finally:
        restore_defaults()
        print("--- headers restored to their declared defaults, rebuilding", flush=True)
        build()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / f"{args.prefix}.csv"
    with out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(collected[0]))
        writer.writeheader()
        writer.writerows(collected)
    print(f"\nwrote {out}  ({len(collected)} rows, {len(collected[0])} columns)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
