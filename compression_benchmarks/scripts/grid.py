#!/usr/bin/env python3
"""Sweep the unit size every tier stores at, and report what it costs each of them.

    ./compression_benchmarks/scripts/grid.py --sizes 16 64 256 1024 -N 3

The unit is the granule a tier seals, compresses and prunes on: FileStore's segment, LossyStore's
group, CompressedStore's segment. They are separate defaults in three headers, so the sweep patches
all three to the same value, rebuilds, and runs the standalone benchmark at each point. The headers
are restored afterwards whatever happens.

Rebuilding per point is the slow part and is unavoidable: these are compile-time defaults, and the
SQL path does not reach them.
"""

import argparse
import csv
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
BENCH = REPO / "compression_benchmarks/scripts/bench.py"
RESULTS_DIR = REPO / "compression_benchmarks/working-dir/results"
ROWS, ROW_WIDTH = 200001, 28
# The headers are patched in place, so an interrupted run would leave the tree at whatever size it
# died on -- and a later measurement would silently be taken at the wrong one. Originals are written
# here first and restored from here on the next start, so a kill cannot lose them.
STASH = REPO / "compression_benchmarks/working-dir/.grid-originals"
# bench.py overwrites one CSV per grid point, so without this the combined tables exist only in the
# terminal and are gone the moment it scrolls.
REPORT_DIR = REPO / "compression_benchmarks/results"

# Each knob is (file, regex capturing the whole declaration, template taking a KB count).
KNOBS = [
    (REPO / "nes-store-manager/include/FileStore.hpp",
     re.compile(r"size_t segmentSize = [^;]+;"), "size_t segmentSize = {kb}UZ * 1024UZ;"),
    (REPO / "nes-store-manager/include/LossyStore.hpp",
     re.compile(r"size_t groupSize = [^;]+;"), "size_t groupSize = {kb}UZ * 1024UZ;"),
    (REPO / "nes-store-manager/include/CompressedStore.hpp",
     re.compile(r"size_t segmentSize = [^;]+;"), "size_t segmentSize = {kb}UZ * 1024UZ;"),
]


def set_unit_size(kb: int) -> None:
    for path, pattern, template in KNOBS:
        text = path.read_text()
        patched, n = pattern.subn(template.format(kb=kb), text, count=1)
        if n != 1:
            raise RuntimeError(f"could not find the unit-size declaration in {path}")
        path.write_text(patched)


def build() -> None:
    out = subprocess.run(
        ["nix", "develop", "--command", "cmake", "--build", "cmake-build-debug",
         "--target", "systest", "-j8"],
        cwd=REPO, capture_output=True, text=True)
    errors = [ln for ln in out.stdout.splitlines() + out.stderr.splitlines() if "error:" in ln]
    if errors:
        raise RuntimeError("build failed:\n" + "\n".join(errors[:5]))


def run_bench(test: Path, repeat: int, kb: int, dataset: str, configuration: str) -> list[dict]:
    subprocess.run([sys.executable, str(BENCH), str(test), "--rows", str(ROWS),
                    "--row-width", str(ROW_WIDTH), "-N", str(repeat),
                    "--dataset", dataset, "--configuration", configuration, "--unit-kb", str(kb)],
                   cwd=REPO, capture_output=True, text=True, check=True)
    with (RESULTS_DIR / f"{test.stem}_summary.csv").open() as handle:
        return list(csv.DictReader(handle))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--test", type=Path, default=REPO / "compression_benchmarks/tests/standalone_large.test")
    ap.add_argument("--sizes", type=int, nargs="+", default=[16, 64, 256, 1024], help="unit sizes in KB")
    ap.add_argument("-N", "--repeat", type=int, default=3)
    ap.add_argument("--dataset", required=True, help="dataset label recorded in every row")
    ap.add_argument("--configuration", required=True, choices=["standalone", "chained"])
    args = ap.parse_args()
    test = args.test if args.test.is_absolute() else REPO / args.test

    # A previous run that was killed left the headers patched; put them back before reading them.
    if STASH.is_dir():
        for path, _, _ in KNOBS:
            saved = STASH / path.name
            if saved.is_file():
                path.write_text(saved.read_text())
        print("--- restored headers left patched by an interrupted run", flush=True)

    originals = [(path, path.read_text()) for path, _, _ in KNOBS]

    # If all three knobs already read the same sweep size, a previous run almost certainly left them
    # patched and its stash was lost -- capturing that as "original" would restore the wrong values
    # and silently mislabel every later run. The three defaults are deliberately NOT all equal.
    current = []
    for (path, pattern, _), (_, text) in zip(KNOBS, originals):
        m = pattern.search(text)
        current.append(m.group(0) if m else "")
    sizes_now = {re.search(r"(\d+)UZ \* 1024UZ;", c).group(1) for c in current if re.search(r"(\d+)UZ \* 1024UZ;", c)}
    if len(sizes_now) == 1 and int(next(iter(sizes_now))) in args.sizes:
        raise SystemExit(
            f"refusing to start: all three unit sizes read {next(iter(sizes_now))} KB, which is one of the "
            f"sweep sizes. A previous run left them patched. Restore the defaults first "
            f"(FileStore 64 KB, LossyStore 64 KB, CompressedStore 1 MB).")

    STASH.mkdir(parents=True, exist_ok=True)
    for path, text in originals:
        (STASH / path.name).write_text(text)

    collected: list[dict] = []
    try:
        for kb in args.sizes:
            print(f"--- unit size {kb} KB: patching, building, running", flush=True)
            set_unit_size(kb)
            build()
            collected.extend(run_bench(test, args.repeat, kb, args.dataset, args.configuration))
    finally:
        for path, text in originals:
            path.write_text(text)
        shutil.rmtree(STASH, ignore_errors=True)
        print("--- headers restored, rebuilding", flush=True)
        build()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / f"grid_{test.stem}.csv"
    with out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(collected[0]))
        writer.writeheader()
        writer.writerows(collected)
    print(f"\nwrote {out}  ({len(collected)} rows, {len(collected[0])} columns)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
