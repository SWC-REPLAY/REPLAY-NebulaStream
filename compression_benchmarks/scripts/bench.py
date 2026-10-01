#!/usr/bin/env python3
"""Run one benchmark .test file and emit one row per configuration.

Driven by grid.py, which sweeps the unit size. Every size the row reports is READ BACK from
the headers rather than passed in, so a row cannot claim a configuration the build did not
have -- including filestore_total_mb and filestore_files_at_capacity, which grid.py raises
for a standalone run over a large input and leaves alone for a chained one.

    ./compression_benchmarks/scripts/bench.py compression_benchmarks/tests/wind_standalone.test \
        --rows 200001 --row-width 28 -N 3

A configuration is a write query followed by a read-back query, so the write's time is the encode
cost and the read-back's is the decode cost. Stores are named after the write query, which is how
the two are paired back up here.

bytes_out is the FILE SIZE, overhead included -- header, schema text, and every per-unit index
record. A format's own overhead is a real cost on disk, and subtracting it measures a compressor
that does not exist.

Uses --show-query-performance rather than -b. Benchmark mode walks the query list backwards and
performs no dependency check, so it runs a file's read-back before the write that fills its store,
then crashes reporting a duration for a query that never ran.
"""

import argparse
import re
import struct
import shutil
import statistics
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

TIER_OF_PREFIX = {"replay": "FileStore", "compressed": "CompressedStore", "lossy": "LossyStore"}

# Two byte counts per configuration, because they answer different questions:
#   bytes_out  the FILE SIZE, framing included -- header, preallocated table, per-unit descriptors.
#              What the data actually costs to keep, and the honest figure for a disk budget.
#   data_out   the encoded data alone, every byte of framing removed. What the codec produced, and
#              the only figure that compares one codec against another without format overhead
#              muddying it. For a lossless row-major store this is exactly bytes_in.

# File layout: [magic 8][version 4][endian 1][flags 4][fingerprint 8][minTs 8][maxTs 8]
#              [schemaLen 4][schemaText][segmentSize 8][maxSegments 4][sealedCount 4]
#              [segment table: maxSegments x 40][data]
# The 16-byte extension is written by every store, including those that reserve no table.
HEADER_FIXED, HEADER_EXT, TABLE_SLOT = 41, 16, 40
SEG_HDR, GRP_HDR, COL_ENTRY = 41, 36, 50


def _data_start(raw: bytes) -> int:
    schema_len = struct.unpack_from("<I", raw, HEADER_FIXED)[0]
    max_segments = struct.unpack_from("<I", raw, HEADER_FIXED + 4 + schema_len + 8)[0]
    return HEADER_FIXED + 4 + schema_len + HEADER_EXT + max_segments * TABLE_SLOT


def walk_file(raw: bytes, tier: str):
    """Return (units, rows, data) for one store file, where `data` excludes all framing.

    Framing is the file header plus, per unit, whatever the format keeps to find and prune it: a
    preallocated descriptor for FileStore, an inline SegmentHeader for CompressedStore, a group
    header and one entry per column for LossyStore. None of it is the codec's doing, so none of it
    belongs in a codec comparison -- but all of it is on disk, which is why the caller reports both.
    """
    off, units, rows, payload, index = _data_start(raw), 0, 0, 0, 0
    file_header = _data_start(raw)

    if tier == "FileStore":
        # Descriptors live in a preallocated table at the front; the data region is plain rows.
        schema_len = struct.unpack_from("<I", raw, HEADER_FIXED)[0]
        base = HEADER_FIXED + 4 + schema_len
        max_segments = struct.unpack_from("<I", raw, base + 8)[0]
        sealed = struct.unpack_from("<I", raw, base + 12)[0]
        table = base + HEADER_EXT
        for i in range(sealed):
            _o, blen, rc, _mn, _mx = struct.unpack_from("<QQQQQ", raw, table + i * TABLE_SLOT)
            payload += blen
            rows += rc
            units += 1
        # The whole table is reserved whether or not a slot is used, so all of it is on disk.
        return units, rows, payload

    while off + GRP_HDR <= len(raw):
        if tier == "CompressedStore":
            if off + SEG_HDR > len(raw):
                break
            _mn, _mx, rc, _ub, cb = struct.unpack_from("<QQQQQ", raw, off)
            if cb == 0 or off + SEG_HDR + cb > len(raw):
                break
            payload += cb
            index += SEG_HDR
            rows += rc
            off += SEG_HDR + cb
        else:
            glen, rc = struct.unpack_from("<QQ", raw, off)
            ncols = struct.unpack_from("<I", raw, off + 32)[0]
            if glen == 0 or off + glen > len(raw) or not 0 < ncols <= 64:
                break
            for c in range(ncols):
                base = off + GRP_HDR + c * COL_ENTRY
                _vo, vlen, _ul, _no, nlen = struct.unpack_from("<QQQQQ", raw, base)
                payload += vlen + nlen
            index += GRP_HDR + ncols * COL_ENTRY
            rows += rc
            off += glen
        units += 1

    return units, rows, payload


# One line per query:  3/10 ( 30.0%) name:03....PASSED  in 0.0204254s (2.5 kB/s / 280 Tup/s)
QUERY_LINE = re.compile(r":(\d+)\.+(?:\x1b\[[\d;]*m)+(PASSED|FAILED)(?:\s+in\s+([0-9.eE+-]+)s)?")


def dev_shell_library_path() -> str:
    out = subprocess.run(
        ["nix", "develop", "--command", "bash", "-c", 'echo -n "$LD_LIBRARY_PATH"'],
        cwd=REPO, capture_output=True, text=True)
    return out.stdout


def store_order_of(sql: str) -> str:
    m = re.search(r"'([A-Za-z>-]+)'\s+AS\s+REPLAY\.STORE_ORDER", sql, re.I)
    return m.group(1) if m else ""


def bound_of(sql: str) -> str:
    m = re.search(r"ACCURACY\s+BOUND\s+([0-9.]+)\s*%", sql, re.I)
    return m.group(1) if m else ""


def parse_configurations(test_file: Path):
    """Map write-query number -> (store order, accuracy bound), by walking the file's queries."""
    text = test_file.read_text()
    # Queries are separated by the '----' expectation blocks; count SELECTs in order.
    queries = [q.strip() for q in re.findall(r"^(SELECT.*?;)\s*$", text, re.S | re.M)]
    configs = {}
    for i, sql in enumerate(queries, start=1):
        if "REPLAYABLE" in sql.upper():
            configs[i] = (store_order_of(sql), bound_of(sql))
    return configs


def run_once(systest: Path, test_file: Path, data: Path, working_dir: Path, store_dir: Path, llp: str):
    """Clear the store directory, run the file, return {query number: seconds}."""
    if store_dir.is_dir():
        for child in store_dir.iterdir():
            shutil.rmtree(child) if child.is_dir() else child.unlink()

    # Every systest invocation writes a ~490 MB debug log and never removes it. A single 4-point
    # grid at -N 3 is well over a hundred invocations, so leaving them behind fills the disk -- 293
    # of them had reached 65 GB before this was noticed. Only logs this run created are removed.
    log_dir = REPO / "cmake-build-debug/nes-systests"
    before = set(log_dir.glob("SystemTest_*.log"))

    # --log-path /dev/null, because this build logs at DEBUG and the store read paths emit a line
    # per read() call. At 42.7M rows against a 4 KB destination that is ~292,000 calls per query,
    # and one grid point produced a 37 GB log before it was noticed -- enough to fill the disk
    # mid-sweep, and the formatting cost dominated the measurement it was supposed to be timing.
    # The log is never read on a successful run; run_once already deleted it afterwards.
    proc = subprocess.run(
        [str(systest), "-t", str(test_file), "--data", str(data), "--log-path", "/dev/null",
         "--workingDir", str(working_dir), "--show-query-performance", "--sequential"],
        cwd=REPO, capture_output=True, text=True,
        env={"LD_LIBRARY_PATH": llp, "PATH": "/usr/bin:/bin", "HOME": str(Path.home())})

    for stale in set(log_dir.glob("SystemTest_*.log")) - before:
        stale.unlink(missing_ok=True)

    timings = {}
    for m in QUERY_LINE.finditer(proc.stdout):
        if m.group(3):
            timings[int(m.group(1))] = float(m.group(3))
    return timings, proc.stdout


def forced_parameters(test_file: Path) -> dict:
    """Read the sizes actually IN FORCE, from the headers and the test file.

    Read rather than passed in, so a row cannot claim a size the build did not have. The grid
    rewrites three of these before each rebuild; the rest hold their defaults throughout, and a
    row that does not carry both is ambiguous -- `unit_kb` alone does not say what the other
    tiers were doing.
    """
    def kb(path, pattern):
        m = re.search(pattern + r"\s*=\s*(\d+)UZ((?: \* 1024UZ)+);", (REPO / path).read_text())
        if not m:
            return ""
        # Count the 1024 factors rather than matching text: "1024UZ * 1024UZ" is 1024 KB with one
        # factor, while "1UZ * 1024UZ * 1024UZ" is 1 MB with two. A substring test cannot tell
        # them apart and reported the first as 1,048,576 KB.
        factors = m.group(2).count("* 1024UZ")
        return int(m.group(1)) * (1024 ** (factors - 1))

    def plain(path, pattern):
        m = re.search(pattern + r"\s*=\s*(\d+)", (REPO / path).read_text())
        return int(m.group(1)) if m else ""

    sql = test_file.read_text()
    history = re.search(r"HISTORY OF '(\w+)'", sql)
    override = re.search(r"(\d+)\s+AS\s+REPLAY\.MAX_BUFFER_COUNT", sql, re.I)
    return {
        "filestore_segment_kb": kb("nes-store-manager/include/FileStore.hpp", "size_t segmentSize"),
        "filestore_total_mb": kb("nes-store-manager/include/FileStore.hpp", "size_t totalSize") // 1024,
        "filestore_files_at_capacity": plain("nes-store-manager/include/FileStore.hpp", "size_t filesAtCapacity"),
        "filestore_flush_threshold_kb": _flush_threshold_kb(kb, plain),
        "lossystore_group_kb": kb("nes-store-manager/include/LossyStore.hpp", "size_t groupSize"),
        "compressedstore_segment_kb": kb("nes-store-manager/include/CompressedStore.hpp", "size_t segmentSize"),
        "compressedstore_level": plain("nes-store-manager/include/CompressedStore.hpp", "int compressionLevel"),
        "memorystore_max_buffer_count": override.group(1) if override else
            plain("nes-store-manager/include/MemoryStore.hpp", "size_t maxBufferCount"),
        "pool_buffer_bytes": plain("nes-query-compiler/interface/QueryExecutionConfiguration.hpp",
                                   "DEFAULT_OPERATOR_BUFFER_SIZE"),
        "flush_threshold": history.group(1) if history else "",
    }


def _flush_threshold_kb(kb, plain):
    """Bytes FileStore accumulates before migrating, as StoreRegistry resolves it.

    Config::flushThresholdBytes of 0 means one file's worth, totalSize / filesAtCapacity, so the
    declared 0 is not the value in force and recording it would misreport every chained row.
    """
    declared = plain("nes-store-manager/include/FileStore.hpp", "size_t flushThresholdBytes")
    if declared:
        return declared // 1024
    total_kb = kb("nes-store-manager/include/FileStore.hpp", "size_t totalSize")
    files = plain("nes-store-manager/include/FileStore.hpp", "size_t filesAtCapacity")
    return total_kb // files if (total_kb and files) else ""


def store_sizes(store_dir: Path):
    """Map write-query number -> {tier: [data, disk, units]}, plus a "TOTAL" entry.

    Broken down per tier because a chain's footprint is not one number: rows that have not drained
    yet sit uncompressed in FileStore while the rest are compressed below it, and a single total
    hides which tier is holding what.
    """
    sizes = {}
    if not store_dir.is_dir():
        return sizes
    for store in sorted(store_dir.iterdir()):
        m = re.search(r"%3a(\d+)_store_", store.name)
        if not m:
            continue
        per_tier: dict[str, list[int]] = {}
        for binfile in sorted(store.glob("*.bin")):
            tier = TIER_OF_PREFIX.get(binfile.name.split("_", 1)[0], "?")
            if tier == "?":
                continue
            u, _rows, dat = walk_file(binfile.read_bytes(), tier)
            entry = per_tier.setdefault(tier, [0, 0, 0])
            entry[0] += dat
            entry[1] += binfile.stat().st_size
            entry[2] += u
        if per_tier:
            per_tier["TOTAL"] = [sum(v[0] for v in per_tier.values()),
                                 sum(v[1] for v in per_tier.values()),
                                 sum(v[2] for v in per_tier.values())]
        sizes[int(m.group(1))] = per_tier
    return sizes


def _timing_cells(samples):
    """median, min, and spread as (max - min) / median -- 0 means every run agreed."""
    if not samples:
        return "", "", ""
    med = statistics.median(samples)
    spread = (max(samples) - min(samples)) / med if med else 0.0
    return f"{med:.4f}", f"{min(samples):.4f}", f"{spread:.3f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("test_file", type=Path)
    ap.add_argument("--rows", type=int, required=True, help="rows in the input")
    ap.add_argument("--row-width", type=int, required=True, help="stored row width in bytes")
    ap.add_argument("-N", "--repeat", type=int, default=1)
    ap.add_argument("--data", type=Path, default=REPO / "cmake-build-debug/nes-systests/testdata")
    ap.add_argument("--working-dir", type=Path, default=REPO / "compression_benchmarks/working-dir")
    ap.add_argument("--store-dir", type=Path, default=REPO / "cmake-build-debug/nes-store-manager/working-dir")
    ap.add_argument("--systest", type=Path, default=REPO / "cmake-build-debug/nes-systests/systest/systest")
    # Carried through to the CSV so a row says what it measured without needing the filename.
    ap.add_argument("--dataset", default="", help="dataset label for the CSV")
    ap.add_argument("--configuration", default="", help="standalone or chained")
    ap.add_argument("--unit-kb", type=int, default=0, help="the unit size this run was built at")
    args = ap.parse_args()

    bytes_in = args.rows * args.row_width
    configs = parse_configurations(args.test_file)
    if not configs:
        print("No REPLAYABLE queries found.", file=sys.stderr)
        return 1

    llp = dev_shell_library_path()
    if not llp:
        print("Could not read LD_LIBRARY_PATH from the dev shell.", file=sys.stderr)
        return 1

    runs, sizes, last_output = [], {}, ""
    for i in range(args.repeat):
        print(f"--- run {i + 1}/{args.repeat}", flush=True)
        timings, last_output = run_once(args.systest, args.test_file, args.data,
                                        args.working_dir, args.store_dir, llp)
        runs.append(timings)
        sizes = store_sizes(args.store_dir)   # sizes are deterministic; keep the last

    # One row per configuration. Every quantity the analysis needs is a column; nothing is derived
    # later from a formatted table.
    TIERS = ["MemoryStore", "FileStore", "CompressedStore", "LossyStore"]
    header = ["dataset", "configuration", "store_order", "tail_tier", "bound_pct",
              "unit_kb", "rows", "row_width_bytes", "bytes_in",
              "encode_s", "decode_s",
              "data_out", "disk_bytes", "framing_bytes", "framing_ratio",
              "ratio_on_data", "ratio_on_disk", "units", "runs"]
    forced = forced_parameters(args.test_file)
    header += list(forced) + ["merge_factor"]
    for t in TIERS:
        header += [f"{t}_data", f"{t}_disk"]

    rows_out = []
    for write_q in sorted(configs):
        order, bound = configs[write_q]
        read_q = write_q + 1
        per_tier = sizes.get(write_q, {})
        total = per_tier.get("TOTAL", [0, 0, 0])
        data, disk, units = total

        # A store with no file on disk still holds its rows -- MemoryStore keeps them in
        # TupleBuffers. Reporting 0 data would read as "stored nothing", so the rows it holds are
        # counted as data and only the DISK figure is zero.
        if not per_tier and "MemoryStore" in order:
            data, disk, units = bytes_in, 0, 0

        framing = disk - data if disk else 0
        enc = [r[write_q] for r in runs if write_q in r]
        dec = [r[read_q] for r in runs if read_q in r]

        row = [args.dataset, args.configuration, order, order.split("->")[-1], bound,
               str(args.unit_kb), str(args.rows), str(args.row_width), str(bytes_in),
               _timing_cells(enc)[0], _timing_cells(dec)[0],
               str(data), str(disk), str(framing),
               f"{framing / disk:.6f}" if disk else "0",
               f"{bytes_in / data:.4f}" if data else "",
               f"{bytes_in / disk:.4f}" if disk else "",
               str(units), str(len(enc))]
        row += [str(v) for v in forced.values()]
        # How many FileStore segments accumulate into one unit of the tail tier. 1 means no
        # merging happens at all -- the case the tiers are explicitly not supposed to be in.
        tail = order.split("->")[-1]
        tail_kb = {"LossyStore": forced["lossystore_group_kb"],
                   "CompressedStore": forced["compressedstore_segment_kb"]}.get(tail)
        fs_kb = forced["filestore_segment_kb"]
        row.append(str(tail_kb // fs_kb) if (tail_kb and fs_kb and "->" in order) else "")
        for t in TIERS:
            got = per_tier.get(t)
            row += [str(got[0]), str(got[1])] if got else (
                [str(bytes_in), "0"] if (t == "MemoryStore" and not per_tier and t in order) else ["", ""])
        rows_out.append(row)

    widths = [max(len(header[c]), *(len(r[c]) for r in rows_out)) for c in range(len(header))]
    print()
    print("  ".join(h.ljust(w) for h, w in zip(header, widths)))
    for r in rows_out:
        print("  ".join(c.ljust(w) for c, w in zip(r, widths)))

    out = args.working_dir / "results" / f"{args.test_file.stem}_summary.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(",".join(header) + "\n" + "\n".join(",".join(r) for r in rows_out) + "\n")
    print(f"\nbytes_in = {args.rows} rows x {args.row_width} B = {bytes_in}")
    print(f"wrote {out}")

    if not any(sizes.values()):
        print("\nNo store files found -- check the run output for failures.", file=sys.stderr)
        print(last_output[-1500:], file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
