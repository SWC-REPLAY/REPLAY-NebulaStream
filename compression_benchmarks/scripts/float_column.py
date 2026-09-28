#!/usr/bin/env python3
"""Run float_column.test and report, per accuracy bound, what happened to the FLOAT column alone.

    ./compression_benchmarks/scripts/float_column.py

A whole-store ratio cannot answer "what does the bound buy", because the bound only touches columns
it can quantise -- here exactly one of four. This walks each LossyStore file, reads the ColumnEntry
in front of every group, and sums the columns separately, so the float's own curve is visible
against three columns that should not move at all.

It also reads `achievedError`, the error the encoder recorded for each group. That is self-reported
rather than independently verified -- it is what the quantiser believed it achieved, not a
comparison against the source rows -- but a bound being exceeded in the encoder's own accounting
would be conclusive, and it is the only per-group figure available without re-reading the CSV.
"""

import csv
import re
import shutil
import struct
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
TEST = REPO / "compression_benchmarks/tests/float_column.test"
STORE_DIR = REPO / "cmake-build-debug/nes-store-manager/working-dir"
SYSTEST = REPO / "cmake-build-debug/nes-systests/systest/systest"
REPORT = REPO / "compression_benchmarks/results/f2_float_column.csv"

GROUP_HEADER, COLUMN_ENTRY = 36, 50
HEADER_FIXED, HEADER_EXT, TABLE_SLOT = 41, 16, 40
COLS = ["producerId i32", "groupId i32", "producedPower f64", "ts u64"]
CODECS = {0: "(none)", 1: "Raw", 2: "ForBitpack", 3: "Quantized", 4: "DeltaOfDelta",
          5: "Dictionary", 6: "DictRle", 7: "Bool"}
FLOAT_COL = 2


def data_start(raw: bytes) -> int:
    schema_len = struct.unpack_from("<I", raw, HEADER_FIXED)[0]
    max_segments = struct.unpack_from("<I", raw, HEADER_FIXED + 4 + schema_len + 8)[0]
    return HEADER_FIXED + 4 + schema_len + HEADER_EXT + max_segments * TABLE_SLOT


def walk(raw: bytes):
    """Per column: bytes, the codec chosen, and the worst error the encoder recorded."""
    off = data_start(raw)
    per_col = [0] * len(COLS)
    codecs = [set() for _ in COLS]
    worst = [0.0] * len(COLS)
    groups = 0
    while off + GROUP_HEADER <= len(raw):
        glen, _rows = struct.unpack_from("<QQ", raw, off)
        ncols = struct.unpack_from("<I", raw, off + 32)[0]
        if glen == 0 or off + glen > len(raw) or not 0 < ncols <= 64:
            break
        for c in range(min(ncols, len(COLS))):
            base = off + GROUP_HEADER + c * COLUMN_ENTRY
            _vo, vlen, _ul, _no, nlen = struct.unpack_from("<QQQQQ", raw, base)
            lossy, lossless = raw[base + 40], raw[base + 41]
            err = struct.unpack_from("<d", raw, base + 42)[0]
            per_col[c] += vlen + nlen
            codecs[c].add(CODECS.get(lossy or lossless, f"?{lossy or lossless}"))
            worst[c] = max(worst[c], err)
        groups += 1
        off += glen
    return groups, per_col, codecs, worst


def main() -> int:
    if STORE_DIR.is_dir():
        for child in STORE_DIR.iterdir():
            shutil.rmtree(child) if child.is_dir() else child.unlink()

    proc = subprocess.run(
        [str(SYSTEST), "-t", str(TEST), "--data", "cmake-build-debug/nes-systests/testdata",
         "--workingDir", "compression_benchmarks/working-dir", "--sequential"],
        cwd=REPO, capture_output=True, text=True)
    if "All queries passed" not in proc.stdout:
        print("test run did not pass:\n" + proc.stdout[-1200:], file=sys.stderr)
        return 1

    # Query order in the file fixes the bound; store dirs are named after the query number.
    bounds = [0.0, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0]
    found = {}
    for store in STORE_DIR.iterdir():
        m = re.search(r"%3a(\d+)_store_", store.name)
        files = list(store.glob("lossy_*.bin"))
        if m and files:
            found[int(m.group(1))] = files[0].read_bytes()

    rows = []
    for i, q in enumerate(sorted(found)):
        groups, per_col, codecs, worst = walk(found[q])
        rows.append((bounds[i], groups, per_col, codecs, worst))

    # Long format, one row per (bound, column): the float's curve is a filter on it, and the three
    # control columns stay visible so "the bound only moved the float" is checkable rather than
    # asserted.
    base = rows[0][2]
    out = []
    for bound, groups, per_col, codecs, worst in rows:
        for c, name in enumerate(COLS):
            col, ctype = name.rsplit(" ", 1)
            out.append({
                "dataset": "debs_wind_turbines",
                "column": col,
                "type": {"i32": "INT32", "f64": "FLOAT64", "u64": "UINT64"}[ctype],
                "is_quantisable": "yes" if ctype == "f64" else "no",
                "bound_pct": bound,
                "groups": groups,
                "bytes": per_col[c],
                "bytes_per_row": f"{per_col[c] / 200001:.4f}",
                "vs_lossless_bytes": per_col[c] - base[c],
                "vs_lossless_pct": f"{100 * (per_col[c] - base[c]) / base[c]:.2f}",
                "codec": "/".join(sorted(codecs[c])),
                "worst_recorded_error": f"{worst[c]:.9g}",
            })

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with REPORT.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(out[0]))
        writer.writeheader()
        writer.writerows(out)
    print(f"  wrote {REPORT}  ({len(out)} rows)")

    # A short readout so the run is not silent about what it found.
    print(f"\n  {'bound':>7}{'float bytes':>14}{'/row':>8}{'vs lossless':>13}{'codec':>12}{'worst error':>15}")
    for r in out:
        if r["column"] == "producedPower":
            print(f"  {r['bound_pct']:>6}%{r['bytes']:>14,}{r['bytes_per_row']:>8}"
                  f"{r['vs_lossless_pct']:>12}%{r['codec']:>12}{r['worst_recorded_error']:>15}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
