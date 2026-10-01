#!/usr/bin/env python3
"""Per-stage byte breakdown of LossyStore's float pipeline, from the stored files.

    ./compression_benchmarks/scripts/lossy_breakdown.py \\
        --test compression_benchmarks/tests/solar_float_column.test \\
        --dataset debs_solar_panels --rows 42700000 --prefix solar

LossyStore encodes a quantisable column in three steps (LossyStore.cpp:309-315):

    quantizeColumn(values, bound)   doubles -> integer grid codes, and a bitWidth
    writeQuantizeHeader + bitPack   codes -> 18 B header + bitWidth bits per code
    compressor.compress(...)        zstd over that, KEPT ONLY IF SMALLER

Everything needed to measure all three is already on disk, so this reads rather than instruments:

    uncompressedLength   the bitpacked size, before zstd
    valuesLength         what was actually stored (equal to the above when zstd was rejected)
    rowCount             from the group header
    bitWidth             derived as (uncompressedLength - 18) * 8 / rowCount

QUANTISATION ON ITS OWN SAVES NOTHING and the table says so. It turns doubles into uint64 codes --
still eight bytes each, in memory. What it produces is a small bitWidth, and that only becomes
bytes when bitPack runs. Reporting a "saving" for the quantise step alone would be inventing one;
the honest split is that quantise EARNS a width, bitpack BANKS it, and zstd then adjusts it in
either direction.

The bitWidth derivation assumes the payload is header plus packed codes and nothing else. Sign
bits and exception lists would both break it, so the flags byte is checked on every group and the
run aborts rather than reporting numbers built on a bad assumption.
"""

import argparse
import csv
import struct
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RESULTS = REPO / "compression_benchmarks/results"
STORE_DIR = REPO / "cmake-build-debug/nes-store-manager/working-dir"
SYSTEST = REPO / "cmake-build-debug/nes-systests/systest/systest"

GROUP_HEADER, COLUMN_ENTRY = 36, 50
HEADER_FIXED, HEADER_EXT, TABLE_SLOT = 41, 16, 40
FLOAT_COL = 2
QUANT_HEADER = 18                      # flags(1) + minMagnitude(8) + deltaLog(8) + bitWidth(1)
FLAG_CONSTANT, FLAG_EXCEPTIONS, FLAG_SIGNS = 1, 2, 4
BOUNDS = [0.0, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0]


def data_start(raw: bytes) -> int:
    schema_len = struct.unpack_from("<I", raw, HEADER_FIXED)[0]
    max_segments = struct.unpack_from("<I", raw, HEADER_FIXED + 4 + schema_len + 8)[0]
    return HEADER_FIXED + 4 + schema_len + HEADER_EXT + max_segments * TABLE_SLOT


def walk(raw: bytes, bound: float):
    """Per-group figures for the float column. Returns a dict of totals."""
    off = data_start(raw)
    t = dict(groups=0, rows=0, packed=0, stored=0, zstd_kept=0, code_bits=0, quantised=0)
    while off + GROUP_HEADER <= len(raw):
        glen, rows = struct.unpack_from("<QQ", raw, off)
        ncols = struct.unpack_from("<I", raw, off + 32)[0]
        if glen == 0 or off + glen > len(raw) or not 0 < ncols <= 64:
            break
        base = off + GROUP_HEADER + FLOAT_COL * COLUMN_ENTRY
        voff, vlen, ulen, _noff, _nlen = struct.unpack_from("<QQQQQ", raw, base)
        payload = off + GROUP_HEADER + ncols * COLUMN_ENTRY + voff

        # The flags byte is the first byte of the payload, and it is readable whether or not zstd
        # ran -- zstd is applied to the whole payload, so when it ran we cannot read it at all.
        # Only groups where zstd was rejected can be checked, which is enough: the flags depend on
        # the DATA, not on the bound, so a column with no negatives and no zeros has none anywhere.
        if vlen == ulen:
            flags = raw[payload]
            if flags & (FLAG_EXCEPTIONS | FLAG_SIGNS):
                raise SystemExit(
                    f"bound {bound}%: a group carries sign bits or an exception list (flags={flags:#04x}). "
                    f"The bitWidth derivation assumes the payload is header plus packed codes only, so "
                    f"these numbers would be wrong. Read bitWidth directly instead.")

        width_bits = (ulen - QUANT_HEADER) * 8 / rows if rows else 0
        t["groups"] += 1
        t["rows"] += rows
        t["packed"] += ulen
        t["stored"] += vlen
        t["zstd_kept"] += 1 if vlen < ulen else 0
        t["code_bits"] += width_bits * rows
        t["quantised"] += ulen - QUANT_HEADER
        off += glen
    return t


def dev_shell_library_path() -> str:
    out = subprocess.run(["nix", "develop", "--command", "bash", "-c", 'echo -n "$LD_LIBRARY_PATH"'],
                         cwd=REPO, capture_output=True, text=True)
    return out.stdout


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--test", type=Path, required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--rows", type=int, required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--reuse", action="store_true",
                    help="skip the run and read whatever is already in the store directory")
    args = ap.parse_args()
    test = args.test if args.test.is_absolute() else REPO / args.test

    if not args.reuse:
        if STORE_DIR.is_dir():
            for child in STORE_DIR.iterdir():
                subprocess.run(["rm", "-rf", str(child)], check=True)
        print(f"running {test.name} ...", flush=True)
        proc = subprocess.run(
            [str(SYSTEST), "-t", str(test), "--data", "cmake-build-debug/nes-systests/testdata",
             "--log-path", "/dev/null", "--workingDir", "compression_benchmarks/working-dir",
             "--sequential"],
            cwd=REPO, capture_output=True, text=True,
            env={"LD_LIBRARY_PATH": dev_shell_library_path(), "PATH": "/usr/bin:/bin",
                 "HOME": str(Path.home())})
        if "All queries passed" not in proc.stdout:
            print("run did not pass:\n" + proc.stdout[-1500:], file=sys.stderr)
            return 1

    found = {}
    for store in STORE_DIR.iterdir():
        import re
        m = re.search(r"%3a(\d+)_store_", store.name)
        files = list(store.glob("lossy_*.bin"))
        if m and files:
            found[int(m.group(1))] = files[0].read_bytes()
    if len(found) != len(BOUNDS):
        print(f"expected {len(BOUNDS)} stores, found {len(found)}", file=sys.stderr)
        return 1

    raw_per_value = 8                       # FLOAT64
    out = []
    for bound, q in zip(BOUNDS, sorted(found)):
        t = walk(found[q], bound)
        rows = t["rows"]
        raw = rows * raw_per_value
        bits = t["code_bits"] / rows
        header = t["groups"] * QUANT_HEADER
        out.append({
            "dataset": args.dataset,
            "column": "producedPower",
            "bound_pct": bound,
            "rows": rows,
            "groups": t["groups"],
            "raw_bytes": raw,
            "raw_bits_per_value": f"{raw_per_value * 8:.3f}",
            # Quantisation produces codes, not bytes. This is the information content it achieved:
            # the packed size it makes POSSIBLE, header excluded.
            "quantised_code_bytes": t["quantised"],
            "quantised_bits_per_value": f"{bits:.4f}",
            "quantise_ratio": f"{raw / t['quantised']:.4f}" if t["quantised"] else "",
            "header_bytes": header,
            "bitpacked_bytes": t["packed"],
            "bitpacked_bits_per_value": f"{t['packed'] * 8 / rows:.4f}",
            "bitpack_ratio": f"{raw / t['packed']:.4f}",
            "zstd_bytes": t["stored"],
            "zstd_bits_per_value": f"{t['stored'] * 8 / rows:.4f}",
            "final_ratio": f"{raw / t['stored']:.4f}",
            # Positive means zstd helped. Negative cannot happen -- the encoder keeps the smaller.
            "zstd_saved_bytes": t["packed"] - t["stored"],
            "zstd_saved_pct": f"{100 * (t['packed'] - t['stored']) / t['packed']:.3f}",
            "zstd_kept_groups": t["zstd_kept"],
            "zstd_kept_share": f"{t['zstd_kept'] / t['groups']:.4f}",
            # What each stage contributed to the final ratio, as a multiplier.
            "x_from_quantise_bitpack": f"{raw / t['packed']:.4f}",
            "x_from_zstd": f"{t['packed'] / t['stored']:.4f}",
        })

    report = RESULTS / f"{args.prefix}_lossy_breakdown.csv"
    report.parent.mkdir(parents=True, exist_ok=True)
    with report.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(out[0]))
        writer.writeheader()
        writer.writerows(out)
    print(f"  wrote {report.name}  ({len(out)} rows)")

    w = f"  {'bound':>7}{'bits/val raw':>13}{'after pack':>12}{'after zstd':>12}{'pack x':>9}{'zstd x':>9}{'final x':>9}{'zstd kept':>11}"
    print("\n" + w)
    print("  " + "-" * (len(w) - 2))
    for r in out:
        print(f"  {r['bound_pct']:>6}%{'64.000':>13}{r['bitpacked_bits_per_value']:>12}"
              f"{r['zstd_bits_per_value']:>12}{r['x_from_quantise_bitpack']:>9}"
              f"{r['x_from_zstd']:>9}{r['final_ratio']:>9}{r['zstd_kept_share']:>11}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
