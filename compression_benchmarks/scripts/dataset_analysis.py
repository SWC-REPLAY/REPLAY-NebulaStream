#!/usr/bin/env python3
"""Describe a benchmark input CSV: per-column statistics and the facts a reader needs to judge it.

    ./compression_benchmarks/scripts/dataset_analysis.py \\
        --csv cmake-build-debug/nes-systests/testdata/large/debs/SOLAR_PANELS_TOPIC_1G.csv \\
        --dataset debs_solar_panels \\
        --source-file large/debs/SOLAR_PANELS_TOPIC_1G.csv \\
        --schema producerId:INT32 groupId:INT32 producedPower:FLOAT64 timestamp:UINT64 \\
        --prefix solar

Writes <prefix>_dataset_facts.csv and <prefix>_dataset_description.csv, in the same shape as the
wind-turbine pair the workbook scripts read, so the workbook picks them up unchanged.

Distinct counts are delegated to `sort -u` rather than a Python set. At 42.7M rows and near-total
cardinality a set costs more memory than this machine has; sort spills to disk and finishes a
column in about twenty seconds. Everything else is one streaming pass with bounded memory:
histograms over decimal places (at most 18 keys) and over timestamp deltas (capped, with the cap
reported rather than silently truncating the picture).
"""

import argparse
import csv
import subprocess
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RESULTS = REPO / "compression_benchmarks/results"

# Matches DataType::getSizeInBytesWithoutNull. VARSIZED is deliberately absent: it is 16 bytes
# there but the stores all count 4, and no store carries its payload at all, so a dataset with one
# cannot be measured honestly.
PAYLOAD = {"INT8": 1, "UINT8": 1, "BOOLEAN": 1, "CHAR": 1,
           "INT16": 2, "UINT16": 2,
           "INT32": 4, "UINT32": 4, "FLOAT32": 4,
           "INT64": 8, "UINT64": 8, "FLOAT64": 8}

# A NULLABLE column carries a one-byte indicator in the stored row, so a 4-byte INT32 occupies 5;
# a NOT NULL one does not. getSizeInBytesWithNull is literally withoutNull + nullable, so this is
# the whole of the difference -- and it is worth 12 bytes a row on a twelve-column schema.
NULL_BYTES = 1
DELTA_CAP = 4096          # distinct timestamp deltas tracked exactly before the count goes bounded


def distinct_count(csv_path: Path, field: int, tmp: Path) -> int:
    """Exact distinct values in one column, via a disk-backed sort."""
    cut = subprocess.Popen(["cut", "-d,", f"-f{field}", str(csv_path)], stdout=subprocess.PIPE,
                           env={"LC_ALL": "C", "PATH": "/usr/bin:/bin"})
    srt = subprocess.Popen(["sort", "-u", "-S", "1G", "--parallel=8", "-T", str(tmp)],
                           stdin=cut.stdout, stdout=subprocess.PIPE,
                           env={"LC_ALL": "C", "PATH": "/usr/bin:/bin"})
    cut.stdout.close()
    out = subprocess.run(["wc", "-l"], stdin=srt.stdout, capture_output=True, text=True)
    srt.stdout.close()
    cut.wait(); srt.wait()
    return int(out.stdout.strip())


def decimals(token: str) -> int:
    """Digits after the point, as WRITTEN. Trailing zeros are not stripped: the text is what the
    parser sees, and 931.60 and 931.6 are the same value but not the same source precision."""
    return len(token.split(".", 1)[1]) if "." in token else 0


def scan(csv_path: Path, names, types, ts_name="timestamp"):
    """One pass: extents, decimal-place histogram, timestamp order and delta shape."""
    n = len(names)
    mins = [None] * n
    maxs = [None] * n
    examples = [None] * n
    dec_hist = [Counter() for _ in names]
    ts_i = names.index(ts_name) if ts_name in names else None
    prev_ts = None
    sorted_ts = True
    deltas = Counter()
    delta_overflow = 0
    delta_min = delta_max = None
    delta_sum = delta_n = 0
    rows = 0
    malformed = 0

    with csv_path.open() as handle:
        for line in handle:
            parts = line.rstrip("\n").split(",")
            if len(parts) != n:
                malformed += 1
                continue
            rows += 1
            for i, (tok, ty) in enumerate(zip(parts, types)):
                if ty == "BOOLEAN":
                    v = 1 if tok.strip().lower() in ("true", "1") else 0
                else:
                    v = float(tok) if ty.startswith("FLOAT") else int(tok)
                if mins[i] is None or v < mins[i]:
                    mins[i] = v
                if maxs[i] is None or v > maxs[i]:
                    maxs[i] = v
                if examples[i] is None:
                    examples[i] = tok
                if ty.startswith("FLOAT"):
                    dec_hist[i][decimals(tok)] += 1
            if ts_i is not None:
                ts = int(parts[ts_i])
                if prev_ts is not None:
                    d = ts - prev_ts
                    if d < 0:
                        sorted_ts = False
                    if len(deltas) < DELTA_CAP or d in deltas:
                        deltas[d] += 1
                    else:
                        delta_overflow += 1
                    # Kept outside the capped histogram so they stay exact however irregular
                    # the data turns out to be.
                    delta_sum += d
                    delta_n += 1
                    if delta_min is None or d < delta_min:
                        delta_min = d
                    if delta_max is None or d > delta_max:
                        delta_max = d
                prev_ts = ts
    return dict(rows=rows, malformed=malformed, mins=mins, maxs=maxs, examples=examples,
                dec_hist=dec_hist, sorted_ts=sorted_ts, deltas=deltas,
                delta_overflow=delta_overflow, delta_min=delta_min, delta_max=delta_max,
                delta_mean=(delta_sum / delta_n if delta_n else 0.0))


def fmt_num(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return repr(round(v, 6))
    return str(v)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", type=Path, required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--source-file", required=True, help="path as the .test file names it")
    ap.add_argument("--schema", nargs="+", required=True, help="name:SQLTYPE, in column order")
    ap.add_argument("--prefix", required=True, help="output files are <prefix>_dataset_*.csv")
    ap.add_argument("--truncated", default="no - the whole shipped file")
    ap.add_argument("--not-null", action="store_true",
                    help="every column is NOT NULL, so no row carries null indicator bytes")
    ap.add_argument("--timestamp-column", default="timestamp",
                    help="which column the sortedness and delta analysis applies to")
    ap.add_argument("--tmp", type=Path, default=Path("/tmp"))
    args = ap.parse_args()

    csv_path = args.csv if args.csv.is_absolute() else REPO / args.csv
    if not csv_path.is_file():
        print(f"no such file: {csv_path}", file=sys.stderr)
        return 1
    names, types = zip(*(s.split(":") for s in args.schema))
    unknown = [t for t in types if t not in PAYLOAD]
    if unknown:
        print(f"unsupported types: {unknown}. VARSIZED cannot be measured; see the comment on "
              f"PAYLOAD.", file=sys.stderr)
        return 1
    per_null = 0 if args.not_null else NULL_BYTES
    width = sum(PAYLOAD[t] + per_null for t in types)

    print(f"scanning {csv_path.name} ({csv_path.stat().st_size:,} bytes)", flush=True)
    st = scan(csv_path, list(names), list(types), args.timestamp_column)
    rows = st["rows"]
    print(f"  {rows:,} rows, {st['malformed']} malformed", flush=True)

    print("  distinct counts (disk-backed sort, one pass per column)", flush=True)
    distinct = []
    for i, nm in enumerate(names, start=1):
        d = distinct_count(csv_path, i, args.tmp)
        distinct.append(d)
        print(f"    {nm:<16} {d:>12,}", flush=True)

    RESULTS.mkdir(parents=True, exist_ok=True)

    # ---- facts
    # A pattern is reported ONLY when the histogram is complete. Once the cap is hit, later
    # occurrences of untracked deltas are invisible, so "most common" would mean "most common
    # among the first few thousand distinct deltas seen" -- which on irregular data ranks near
    # noise and reads as though a pattern had been found.
    if not st["deltas"]:
        pattern, ndeltas = "", ""
    elif st["delta_overflow"]:
        pattern = (f"irregular - over {DELTA_CAP} distinct deltas, no repeating cycle; "
                   f"min {st['delta_min']}, max {st['delta_max']}, mean {st['delta_mean']:.1f}")
        ndeltas = f">{DELTA_CAP}"
    elif len(st["deltas"]) <= 12:
        pattern = "; ".join(f"{d}x{c}" for d, c in st["deltas"].most_common())
        ndeltas = str(len(st["deltas"]))
    else:
        # The histogram is complete, but a few thousand entries is a dump rather than a pattern --
        # one run wrote a 34 KB cell. Show the head and say how much was left out.
        top = st["deltas"].most_common(8)
        pattern = ("; ".join(f"{d}x{c}" for d, c in top)
                   + f"; ...{len(st['deltas']) - len(top)} more, counts exact; "
                     f"min {st['delta_min']}, max {st['delta_max']}, mean {st['delta_mean']:.1f}")
        ndeltas = str(len(st["deltas"]))
    ts_i = list(names).index(args.timestamp_column) if args.timestamp_column in names else None
    facts = [
        ("dataset", args.dataset),
        ("source_file", args.source_file),
        ("csv_bytes", csv_path.stat().st_size),
        ("rows", rows),
        ("columns", len(names)),
        ("stored_row_width_bytes", width),
        ("stored_bytes_total", rows * width),
        ("truncated", args.truncated),
        ("timestamp_sorted", st["sorted_ts"] if ts_i is not None else ""),
        ("timestamp_distinct_deltas", ndeltas),
        ("timestamp_delta_pattern", pattern),
        ("timestamp_delta_min", st["delta_min"] if st["delta_min"] is not None else ""),
        ("timestamp_delta_max", st["delta_max"] if st["delta_max"] is not None else ""),
        ("timestamp_delta_mean", f"{st['delta_mean']:.1f}" if st["deltas"] else ""),
        ("timestamp_range", f"{fmt_num(st['mins'][ts_i])} .. {fmt_num(st['maxs'][ts_i])}"
                            if ts_i is not None else ""),
    ]
    fp = RESULTS / f"{args.prefix}_dataset_facts.csv"
    with fp.open("w", newline="") as h:
        w = csv.writer(h)
        w.writerow(["item", "value"])
        w.writerows(facts)
    print(f"  wrote {fp.name}")

    # ---- per-column description
    dp = RESULTS / f"{args.prefix}_dataset_description.csv"
    with dp.open("w", newline="") as h:
        w = csv.writer(h)
        w.writerow(["dataset", "column", "sql_type", "payload_bytes", "null_indicator_bytes",
                    "stored_bytes_per_value", "distinct_values", "distinct_pct", "min", "max",
                    "decimal_places", "quantisable_by_accuracy_bound", "example"])
        for i, (nm, ty) in enumerate(zip(names, types)):
            hist = st["dec_hist"][i]
            dec = "/".join(f"{k}:{hist[k]}" for k in sorted(hist)) if hist else ""
            w.writerow([args.dataset, nm, ty, PAYLOAD[ty], per_null, PAYLOAD[ty] + per_null,
                        distinct[i], f"{distinct[i] / rows * 100:.2f}",
                        fmt_num(st["mins"][i]), fmt_num(st["maxs"][i]), dec,
                        "yes" if ty.startswith("FLOAT") else "no", st["examples"][i]])
    print(f"  wrote {dp.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
