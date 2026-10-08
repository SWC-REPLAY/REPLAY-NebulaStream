#!/usr/bin/env python3
"""Build solar_microbench.xlsx from the LossyStore microbenchmark CSV.

    ./compression_benchmarks/scripts/microbench_workbook.py

Different question from the other two workbooks, so a different shape. workbook.py and
standalone_workbook.py both measure a QUERY and compare configurations; this one measures the STORE
and compares its internal stages, from timers inside LossyStore rather than from a harness calling
the codecs itself.

    Write Path    where the store's write seconds go, stage by stage
    Read Path     the same for reads
    Shares        each stage as a percentage of its path, so shapes compare across configurations
    Throughput    the stages that move bytes, as rates
    Ranking       what the codec-selection path LossyStore does not use would cost and save
    Summary       the four charts worth taking away

Composition is drawn as STACKED bars: a stage breakdown has to show the parts and the total at the
same time, and six separate bars per configuration does not.
"""

import csv
import sys
from pathlib import Path

from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

REPO = Path(__file__).resolve().parents[2]
RESULTS = REPO / "compression_benchmarks/results/benchmark"
SOURCE = "solar_microbench.csv"

ARIAL = "Arial"
GREY = "7F7F7F"
GREEN = "008000"
HDR = PatternFill("solid", fgColor="2F5496")
SUB = PatternFill("solid", fgColor="D9E2F3")
WARN = PatternFill("solid", fgColor="FFF2CC")

# (label, csv column). Order is execution order, which is also stacking order.
WRITE_STAGES = [
    ("writeRecord overhead", "writerecord_overhead_s"),
    ("row -> column", "transpose_out_s"),
    ("quantization", "quantize_s"),
    ("bitpacking", "bitpack_s"),
    ("compression", "compress_s"),
    ("header / serialize / index", "serialize_s"),
    ("I/O: group to disk", "write_io_s"),
]
READ_STAGES = [
    ("file I/O", "read_io_s"),
    ("decompression", "decompress_s"),
    ("decoding", "decode_s"),
    ("column -> row", "transpose_in_s"),
    ("cursor/buffer management", "cursor_mgmt_s"),
]

GROUPS: list[int] = []
BOUNDS: list[str] = []
COLS: dict = {}
STORED = 0


def f_title(ws, r, text, size=14):
    ws.cell(row=r, column=1, value=text).font = Font(name=ARIAL, bold=True, size=size)
    return r + 1


def f_note(ws, r, text, colour=GREY, italic=True, col=1):
    c = ws.cell(row=r, column=col, value=text)
    c.font = Font(name=ARIAL, italic=italic, color=colour)
    c.alignment = Alignment(wrap_text=False, vertical="top")
    return r + 1


def f_section(ws, r, text, width=8):
    ws.cell(row=r, column=1, value=text).font = Font(name=ARIAL, bold=True)
    for i in range(1, width + 1):
        ws.cell(row=r, column=i).fill = SUB
    return r + 1


def f_head(ws, r, labels):
    for i, label in enumerate(labels, start=1):
        c = ws.cell(row=r, column=i, value=label)
        c.font = Font(name=ARIAL, bold=True, color="FFFFFF")
        c.fill = HDR
        c.alignment = Alignment(horizontal="center", wrap_text=True)
    return r + 1


def chart_notes(ws, r, shows, look_for):
    ws.cell(row=r, column=1, value="WHAT THIS SHOWS").font = Font(name=ARIAL, bold=True, size=9)
    f_note(ws, r, shows, col=2)
    ws.cell(row=r + 1, column=1, value="WHAT TO LOOK FOR").font = Font(name=ARIAL, bold=True, size=9)
    f_note(ws, r + 1, look_for, col=2)
    return r + 2


def sumifs(col, group_kb, bound):
    return (f'=SUMIFS(Data!${COLS[col]}:${COLS[col]},'
            f'Data!${COLS["group_kb"]}:${COLS["group_kb"]},{group_kb},'
            f'Data!${COLS["bound_pct"]}:${COLS["bound_pct"]},{bound})')


def stage_matrix(ws, top, heading, stages, bound, fmt="0.000", subtitle=None, pct_of=None):
    """Stages down the side, group sizes across, at one accuracy bound."""
    r = f_section(ws, top, heading, width=1 + len(GROUPS))
    if subtitle:
        r = f_note(ws, r, subtitle)
    r = f_head(ws, r, ["Stage"] + [f"{g} KB" for g in GROUPS])
    first = r
    for i, (label, col) in enumerate(stages):
        ws.cell(row=first + i, column=1, value=label).font = Font(name=ARIAL)
        for j, g in enumerate(GROUPS, start=2):
            if pct_of:
                formula = (f'=100*{sumifs(col, g, bound)[1:]}'
                           f'/MAX(0.000001,{sumifs(pct_of, g, bound)[1:]})')
            else:
                formula = sumifs(col, g, bound)
            c = ws.cell(row=first + i, column=j, value=formula)
            c.font = Font(name=ARIAL, color=GREEN)
            c.number_format = fmt
    return first, first + len(stages) - 1


def stacked(ws, title, y_title, first, last, anchor, height=11, width=22):
    ch = BarChart()
    ch.type, ch.grouping, ch.overlap = "col", "stacked", 100
    ch.gapWidth = 60
    ch.title, ch.y_axis.title, ch.x_axis.title = title, y_title, "group size (KB)"
    ch.height, ch.width = height, width
    for i in range(first, last + 1):
        ch.add_data(Reference(ws, min_col=1, max_col=1 + len(GROUPS), min_row=i, max_row=i),
                    titles_from_data=True, from_rows=True)
    ch.set_categories(Reference(ws, min_col=2, max_col=1 + len(GROUPS),
                                min_row=first - 1, max_row=first - 1))
    ws.add_chart(ch, anchor)


def grouped(ws, title, y_title, first, last, anchor, height=11, width=22):
    ch = BarChart()
    ch.type, ch.gapWidth = "col", 60
    ch.title, ch.y_axis.title, ch.x_axis.title = title, y_title, "group size (KB)"
    ch.height, ch.width = height, width
    for i in range(first, last + 1):
        ch.add_data(Reference(ws, min_col=1, max_col=1 + len(GROUPS), min_row=i, max_row=i),
                    titles_from_data=True, from_rows=True)
    ch.set_categories(Reference(ws, min_col=2, max_col=1 + len(GROUPS),
                                min_row=first - 1, max_row=first - 1))
    ws.add_chart(ch, anchor)


def data_sheet(wb):
    rows = list(csv.DictReader((RESULTS / SOURCE).open()))
    ws = wb.create_sheet("Data")
    heads = list(rows[0])
    f_head(ws, 1, heads)
    for n, row in enumerate(rows, start=2):
        for i, h in enumerate(heads, start=1):
            v = row[h]
            try:
                v = float(v) if ("." in v or "e" in v.lower()) else int(v)
            except (ValueError, TypeError):
                pass
            c = ws.cell(row=n, column=i, value=v if v != "" else None)
            c.font = Font(name=ARIAL)
    ws.freeze_panes = "B2"
    ws.column_dimensions["A"].width = 20
    for i in range(2, len(heads) + 1):
        ws.column_dimensions[get_column_letter(i)].width = 15
    return {h: get_column_letter(i) for i, h in enumerate(heads, start=1)}, rows


def widths(ws, first=30, rest=14):
    ws.column_dimensions["A"].width = first
    for i in range(2, 2 + len(GROUPS)):
        ws.column_dimensions[get_column_letter(i)].width = rest


def sheet_readme(wb, facts):
    ws = wb.create_sheet("Read Me", 0)
    ws.column_dimensions["A"].width = 26
    ws.column_dimensions["B"].width = 116
    r = f_title(ws, 1, "LossyStore, measured directly — stage breakdown", 16)
    r = f_note(ws, r, "No SQL, no query engine, and the CSV parse outside every measurement.")
    r += 1
    for k, v in facts.items():
        ws.cell(row=r, column=1, value=k).font = Font(name=ARIAL, bold=True)
        f_note(ws, r, v, colour="000000", italic=False, col=2)
        r += 1
    r += 1

    r = f_section(ws, r, "WHERE THESE NUMBERS COME FROM", width=2)
    for k, v in {
        "timers in the store": "StoreProfile.hpp, compiled in with -DNES_STORE_PROFILING=ON. The "
                               "stages are timed inside LossyStore and ColumnCodec, on the real "
                               "path.",
        "not a replica": "An earlier version of this benchmark called the codecs itself and "
                         "reconstructed the stages. It agreed on the codecs and the order and was "
                         "wrong about everything else: it put bitpacking at ~2.4 s where the real "
                         "timer says 0.161, and it never saw serialization at all -- which turns "
                         "out to be the largest stage on the write path.",
        "the CSV is separate": "Parsing 1.49 GB of CSV into packed rows is reported on its own "
                               "line and is NOT in any store total. In the SQL benchmarks it is "
                               "about three quarters of a write query.",
    }.items():
        ws.cell(row=r, column=1, value=k).font = Font(name=ARIAL, bold=True)
        f_note(ws, r, v, colour="000000", italic=False, col=2)
        r += 1
    r += 1

    r = f_section(ws, r, "WHAT CANNOT BE SPLIT", width=2)
    f_note(ws, r, "writeRecord is called once per row -- 42,700,000 times -- so two clock reads "
                  "inside it would be ~2 s against a 5.8 s write: the timer would be a third of "
                  "the measurement. Its cost (lock, preconditions, pending insert, timestamp "
                  "min/max) is DERIVED as the write total minus sealGroup, and those four cannot "
                  "be separated from each other. Everything else is at group or read-call "
                  "granularity, where a timer is noise.",
           colour="000000", italic=False, col=2)
    r += 2
    c = ws.cell(row=r, column=1, value="repeats")
    c.font = Font(name=ARIAL, bold=True)
    c.fill = WARN
    c2 = ws.cell(row=r, column=2,
                 value="--repeat 3 keeps the FASTEST repeat, not the median: a slow repeat is "
                       "interference. The stage counters come from that same run, so wall clock "
                       "and stages are consistent with one another.")
    c2.font = Font(name=ARIAL)
    c2.fill = WARN
    return ws


def totals_matrix(ws, top, heading, rows_spec, across, across_col, fixed_col, fixed_val,
                  fmt="0.000", subtitle=None, transform=None):
    """Measurements down the side, `across` values across, with the other dimension held fixed."""
    r = f_section(ws, top, heading, width=1 + len(across))
    if subtitle:
        r = f_note(ws, r, subtitle)
    r = f_head(ws, r, ["Measurement"] + [f"{a}{' KB' if across_col == 'group_kb' else '%'}"
                                         for a in across])
    first = r
    for i, (label, col) in enumerate(rows_spec):
        ws.cell(row=first + i, column=1, value=label).font = Font(name=ARIAL)
        for j, a in enumerate(across, start=2):
            inner = (f'SUMIFS(Data!${COLS[col]}:${COLS[col]},'
                     f'Data!${COLS[across_col]}:${COLS[across_col]},{a},'
                     f'Data!${COLS[fixed_col]}:${COLS[fixed_col]},{fixed_val})')
            c = ws.cell(row=first + i, column=j,
                        value=transform(inner) if transform else f"={inner}")
            c.font = Font(name=ARIAL, color=GREEN)
            c.number_format = fmt
    return first, first + len(rows_spec) - 1


def sheet_totals(wb, bound):
    """What the store costs end to end -- the numbers the stage sheets then break down."""
    ws = wb.create_sheet("Totals")
    widths(ws, 32)
    r = f_title(ws, 1, "Totals — what the store costs, end to end")
    r = f_note(ws, r, "The store driven through its own API: open, writeRecord per row, flush, then "
                      "read back into 4 KB buffers until exhausted. The Write Path and Read Path "
                      "sheets break these same numbers into stages.")
    r += 1

    first, last = totals_matrix(
        ws, r, f"SECONDS  (bound {bound}%)",
        [("CSV parse — NOT the store", "csv_s"), ("store write", "write_s"), ("store read", "read_s")],
        GROUPS, "group_kb", "bound_pct", bound,
        subtitle="The CSV row is the cost of turning 1.49 GB of text into packed rows. It is "
                 "reported here because it dwarfs both store numbers, and because in the SQL "
                 "benchmarks it is about three quarters of a write query -- but it is NOT part of "
                 "either store total.")
    r = last + 2
    grouped(ws, f"Store cost in seconds, bound {bound}%", "seconds", first, last, f"A{r}")
    r += 24
    r = chart_notes(
        ws, r,
        "The three wall-clock totals at each group size: parsing the input, writing it into the "
        "store, and replaying it all back out.",
        "Parsing the CSV costs more than writing AND reading the store put together. The store "
        "itself is flat across a 64x range of group sizes -- nothing on either path is sensitive "
        "to how much data it batches at once.")
    r += 1

    first, last = totals_matrix(
        ws, r, f"MB PER SECOND  (bound {bound}%)",
        [("CSV parse — NOT the store", "csv_s"), ("store write", "write_s"), ("store read", "read_s")],
        GROUPS, "group_kb", "bound_pct", bound, fmt="0.0",
        subtitle=f"The same three totals as rates, over {STORED:,} stored bytes. For the CSV row "
                 f"that denominator is the packed output, not the 1,489,260,482 B of text it was "
                 f"parsed from -- so every row here divides the same number and the three are "
                 f"directly comparable.",
        transform=lambda inner: f"={STORED}/MAX(0.000001,{inner})/1000000")
    r = last + 2
    grouped(ws, f"Store throughput, bound {bound}%", "MB/s", first, last, f"A{r}")
    r += 24
    r = chart_notes(
        ws, r,
        "Stored bytes divided by each of the three totals.",
        "The store writes at about 200 MB/s and reads at about 330, against roughly 160 MB/s to "
        "parse the CSV that feeds it. Reading is the fastest of the three and parsing the slowest, "
        "which is the whole argument for the replay tier: getting rows back out of the store is "
        "twice as fast as getting them out of the file they came from.")
    r += 1

    first, last = totals_matrix(
        ws, r, f"ACROSS ACCURACY BOUNDS  (group {GROUPS[0]} KB)",
        [("store write", "write_s"), ("store read", "read_s")],
        BOUNDS, "bound_pct", "group_kb", GROUPS[0],
        subtitle="The same two totals against the accuracy bound, with the group size held fixed.")
    r = last + 2
    grouped(ws, f"Store cost by accuracy bound, group {GROUPS[0]} KB", "seconds", first, last, f"A{r}")
    r += 24
    r = chart_notes(
        ws, r,
        "Write and read against the accuracy bound.",
        "A looser bound stores fewer bytes but does not take less time to do it: the quantiser "
        "does the same work whatever grid it maps onto. Time is bought by the codec CHOICE, not "
        "by the bound.")
    r += 1

    first, last = totals_matrix(
        ws, r, f"WHAT CAME OUT  (bound {bound}%)",
        [("bytes on disk", "disk_bytes")], GROUPS, "group_kb", "bound_pct", bound, fmt="#,##0")
    ws.cell(row=last + 1, column=1, value="compression ratio").font = Font(name=ARIAL)
    for j, g in enumerate(GROUPS, start=2):
        c = ws.cell(row=last + 1, column=j,
                    value=f'=SUMIFS(Data!${COLS["ratio"]}:${COLS["ratio"]},'
                          f'Data!${COLS["group_kb"]}:${COLS["group_kb"]},{g},'
                          f'Data!${COLS["bound_pct"]}:${COLS["bound_pct"]},{bound})')
        c.font = Font(name=ARIAL, color=GREEN)
        c.number_format = "0.000"
    ws.cell(row=last + 2, column=1, value="groups sealed").font = Font(name=ARIAL)
    ws.cell(row=last + 3, column=1, value="read calls").font = Font(name=ARIAL)
    for j, g in enumerate(GROUPS, start=2):
        for off, col in ((2, "groups"), (3, "read_calls")):
            c = ws.cell(row=last + off, column=j,
                        value=f'=SUMIFS(Data!${COLS[col]}:${COLS[col]},'
                              f'Data!${COLS["group_kb"]}:${COLS["group_kb"]},{g},'
                              f'Data!${COLS["bound_pct"]}:${COLS["bound_pct"]},{bound})')
            c.font = Font(name=ARIAL, color=GREEN)
            c.number_format = "#,##0"
    r = last + 5
    chart_notes(
        ws, r,
        "The bytes the store produced, and the shape of the work: how many groups it sealed and "
        "how many read calls the replay took.",
        "The ratio is identical to the SQL benchmark's for the same configuration, which is the "
        "check that this is measuring the same store doing the same thing. Groups fall 64x across "
        "the sweep while read calls do not move at all -- a read call is one 4 KB destination "
        "buffer, and the group size has nothing to do with it.")
    return ws


def sheet_write(wb, bound):
    ws = wb.create_sheet("Write Path")
    widths(ws, 30)
    r = f_title(ws, 1, f"Write path — where the store's seconds go  (bound {bound}%)")
    r = f_note(ws, r, "Stages in execution order. They sum to the write total by construction: "
                      "serialization is what is left of sealGroup after the others, and "
                      "writeRecord overhead is what is left of the write after sealGroup.")
    r += 1

    first, last = stage_matrix(ws, r, "SECONDS PER STAGE", WRITE_STAGES, bound)
    r = last + 2
    stacked(ws, f"Write path composition, bound {bound}%", "seconds", first, last, f"A{r}")
    r += 24
    r = chart_notes(
        ws, r,
        "Each bar is one group size, split into the stages that make it up. Bar height is the "
        "store's whole write time.",
        "Serialization -- building the group header, laying out the columns, updating the index -- "
        "is the largest single stage, larger than quantisation. It was entirely invisible to the "
        "replica version of this benchmark. Bitpacking is small: quantisation costs about seven "
        "times more than packing its output.")
    r += 1

    first, last = stage_matrix(ws, r, "AS A SHARE OF THE WRITE  (%)", WRITE_STAGES, bound,
                               fmt="0.0", pct_of="write_s",
                               subtitle="The same stages normalised, so the shape can be compared "
                                        "between group sizes rather than the totals.")
    r = last + 2
    grouped(ws, f"Write stage shares, bound {bound}%", "% of write", first, last, f"A{r}")
    r += 24
    chart_notes(
        ws, r,
        "Each stage as a percentage of the store's write time, at each group size.",
        "A stage whose share moves with group size is sensitive to how much data it sees at once; "
        "a flat one is per-row work wearing a different hat. Compare the transpose (per row, so "
        "flat) against serialization (per group, so it should fall as groups get larger).")
    return ws


def sheet_read(wb, bound):
    ws = wb.create_sheet("Read Path")
    widths(ws, 30)
    r = f_title(ws, 1, f"Read path — replaying everything back out  (bound {bound}%)")
    r = f_note(ws, r, "No CSV anywhere on this path. Cursor/buffer management is what is left of "
                      "the read after the four measured stages.")
    r += 1

    first, last = stage_matrix(ws, r, "SECONDS PER STAGE", READ_STAGES, bound)
    r = last + 2
    stacked(ws, f"Read path composition, bound {bound}%", "seconds", first, last, f"A{r}")
    r += 24
    r = chart_notes(
        ws, r,
        "Each bar is one group size, split into the stages of a full replay.",
        "File I/O is almost nothing -- the store file is in the page cache, so it reads at "
        "multiple GB/s. The read is decoding plus cursor and buffer bookkeeping, and that second "
        "term is the price of a 4 KB destination buffer: it is paid once per read call, and there "
        "are 292,467 of them.")
    r += 1

    first, last = stage_matrix(ws, r, "AS A SHARE OF THE READ  (%)", READ_STAGES, bound,
                               fmt="0.0", pct_of="read_s")
    r = last + 2
    grouped(ws, f"Read stage shares, bound {bound}%", "% of read", first, last, f"A{r}")
    r += 24
    chart_notes(
        ws, r,
        "The same stages as percentages of the read.",
        "Decoding dominates and does not shrink with a larger group, because it is per value. "
        "Decompression stays tiny: zstd is kept only where it helped, so most groups have nothing "
        "to inflate.")
    return ws


def sheet_throughput(wb, bound):
    ws = wb.create_sheet("Throughput")
    widths(ws, 30)
    r = f_title(ws, 1, f"Throughput — the paths and the stages that move bytes  (bound {bound}%)")
    r = f_note(ws, r, f"Over {STORED:,} stored bytes. Stages that transform rather than move "
                      f"bytes (quantisation, bitpacking) are left out: a MB/s figure for them "
                      f"would divide by a byte count that is not what they produced.")
    r += 1

    rows = [("CSV parse (not the store)", "csv_s"), ("store write", "write_s"),
            ("store read", "read_s"), ("row -> column", "transpose_out_s"),
            ("column -> row", "transpose_in_s"), ("write I/O", "write_io_s"),
            ("read I/O", "read_io_s")]
    r = f_section(ws, r, "MB PER SECOND", width=1 + len(GROUPS))
    r = f_head(ws, r, ["Measurement"] + [f"{g} KB" for g in GROUPS])
    first = r
    for i, (label, col) in enumerate(rows):
        ws.cell(row=first + i, column=1, value=label).font = Font(name=ARIAL)
        for j, g in enumerate(GROUPS, start=2):
            c = ws.cell(row=first + i, column=j,
                        value=f"={STORED}/MAX(0.000001,{sumifs(col, g, bound)[1:]})/1000000")
            c.font = Font(name=ARIAL, color=GREEN)
            c.number_format = "0.0"
    last = first + len(rows) - 1
    r = last + 2
    grouped(ws, f"Throughput, bound {bound}%", "MB/s", first, last, f"A{r}")
    r += 24
    chart_notes(
        ws, r,
        "Stored bytes divided by the time each phase took.",
        "The two transposes run at ~2 GB/s and the two I/O lines far faster still, since nothing "
        "reaches the disk. The CSV parse is the slowest thing on the chart by a wide margin, and "
        "it is the only line here that is not the store.")
    return ws


def sheet_ranking(wb, rows):
    ws = wb.create_sheet("Ranking")
    ws.column_dimensions["A"].width = 44
    ws.column_dimensions["B"].width = 24
    r = f_title(ws, 1, "Would codec ranking pay?")
    c = ws.cell(row=r, column=1,
                value="LossyStore picks a codec once per column by a fixed type-only rule. The "
                      "statistics-driven ranking path is in the tree, tested, and not wired in. "
                      "LossyStore.cpp:326-332 says the tradeoff 'is exactly what the benchmarks "
                      "decide'. These are the numbers.")
    c.font = Font(name=ARIAL, italic=True, color=GREY)
    r += 2

    ranked = [x for x in rows if float(x.get("rank_decide_s") or 0) > 0]
    if not ranked:
        f_note(ws, r, "No run in this CSV was given --ranking.", colour="000000", italic=False)
        return ws
    x = ranked[0]
    decide = float(x["rank_decide_s"])
    fixed = int(x["rank_fixed_bytes"])
    rank = int(x["rank_ranked_bytes"])
    write = float(x["write_s"])
    read = float(x["read_s"])

    r = f_head(ws, r, ["", "value"])
    for label, value, fmt in [
        ("bytes today, fixed type-only rule", fixed, "#,##0"),
        ("bytes if ranked", rank, "#,##0"),
        ("saving", fixed - rank, "#,##0"),
        ("saving, per cent", 100.0 * (fixed - rank) / fixed, "0.00"),
        ("cost of deciding (computeStats + rank)", decide, "0.000"),
        ("store write today", write, "0.000"),
        ("store read today", read, "0.000"),
        ("deciding, as a share of a write", 100.0 * decide / write, "0.0"),
    ]:
        ws.cell(row=r, column=1, value=label).font = Font(name=ARIAL)
        c = ws.cell(row=r, column=2, value=value)
        c.font = Font(name=ARIAL)
        c.number_format = fmt
        r += 1
    r += 1
    chart_notes(
        ws, r,
        f"Measured at a {x['group_kb']} KB group and a {x['bound_pct']}% bound. Ranking is "
        f"independent of both, so it was run once.",
        "A 15.75% saving on the integer columns for a cost larger than the entire store write. "
        "Nearly all of it is computeStats; filterCandidates and rank are free. On these numbers "
        "the fixed rule is the right choice, and the question the source comment left open is "
        "answered. Note the saving applies to the integer columns only, which are about four "
        "fifths of the file.")
    return ws


def sheet_summary(wb, bound):
    ws = wb.create_sheet("Summary")
    widths(ws, 30)
    r = f_title(ws, 1, "Summary", 16)
    r = f_note(ws, r, "Five charts, and what each settles.")
    r += 1

    first, last = totals_matrix(
        ws, r, "1. WHAT THE STORE COSTS",
        [("CSV parse — NOT the store", "csv_s"), ("store write", "write_s"), ("store read", "read_s")],
        GROUPS, "group_kb", "bound_pct", bound)
    r = last + 2
    grouped(ws, "Store cost in seconds", "seconds", first, last, f"A{r}")
    r += 24
    r = chart_notes(ws, r, "The three wall-clock totals the stage charts below break down.",
                    "Parsing the CSV costs more than writing and reading the store together, and "
                    "the store is flat across a 64x range of group sizes.")
    r += 1

    first, last = stage_matrix(ws, r, "2. WRITE PATH COMPOSITION", WRITE_STAGES, bound)
    r = last + 2
    stacked(ws, "Write path composition", "seconds", first, last, f"A{r}")
    r += 24
    r = chart_notes(ws, r, "Where the store's write time goes.",
                    "Serialization is the largest stage, ahead of quantisation, and bitpacking is "
                    "nearly free. A benchmark that reconstructs the stages from outside the store "
                    "cannot see any of that.")
    r += 1

    first, last = stage_matrix(ws, r, "3. READ PATH COMPOSITION", READ_STAGES, bound)
    r = last + 2
    stacked(ws, "Read path composition", "seconds", first, last, f"A{r}")
    r += 24
    r = chart_notes(ws, r, "Where the store's read time goes.",
                    "Decoding plus cursor bookkeeping. File I/O and decompression together are "
                    "under 3% -- the read is not waiting on the disk or on zstd.")
    r += 1

    first, last = stage_matrix(ws, r, "4. WRITE SHARES  (%)", WRITE_STAGES, bound,
                               fmt="0.0", pct_of="write_s")
    r = last + 2
    grouped(ws, "Write stage shares", "% of write", first, last, f"A{r}")
    r += 24
    r = chart_notes(ws, r, "The write composition normalised, so group sizes compare by shape.",
                    "Per-group work should shrink as groups grow; per-row work should not. The "
                    "chart separates the two without needing the totals.")
    r += 1

    first, last = stage_matrix(ws, r, "5. READ SHARES  (%)", READ_STAGES, bound,
                               fmt="0.0", pct_of="read_s")
    r = last + 2
    grouped(ws, "Read stage shares", "% of read", first, last, f"A{r}")
    r += 24
    chart_notes(ws, r, "The read composition normalised.",
                "Cursor and buffer management is a large, flat share. It is paid once per read "
                "call and there are 292,467 of them, which is a property of the 4 KB destination "
                "buffer rather than of any codec.")
    return ws


def main() -> int:
    global GROUPS, BOUNDS, COLS, STORED
    if not (RESULTS / SOURCE).is_file():
        print(f"missing {RESULTS / SOURCE} -- run the microbenchmark with --csv-out first",
              file=sys.stderr)
        return 1

    wb = Workbook()
    wb.remove(wb.active)
    COLS, rows = data_sheet(wb)
    GROUPS = sorted({int(r["group_kb"]) for r in rows})
    BOUNDS = sorted({r["bound_pct"] for r in rows}, key=float)
    STORED = int(rows[0]["stored_bytes"])
    bound = "1" if "1" in BOUNDS else BOUNDS[len(BOUNDS) // 2]

    facts = {
        "dataset": f'{rows[0]["dataset"]}, {int(rows[0]["rows"]):,} rows x '
                   f'{rows[0]["row_width_bytes"]} B = {STORED:,} B stored',
        "store": "LossyStore, standalone, driven through its own API",
        "group sizes": ", ".join(f"{g} KB" for g in GROUPS),
        "accuracy bounds": ", ".join(f"{b}%" for b in BOUNDS),
        "charts shown at": f"{bound}% (every bound is in the Data sheet)",
        "build": "Benchmark (-O3, NO_ASSERT) with -DNES_STORE_PROFILING=ON",
        "repeats": f'N={rows[0]["repeats"]}, fastest kept',
    }

    sheet_readme(wb, facts)
    sheet_totals(wb, bound)
    sheet_write(wb, bound)
    sheet_read(wb, bound)
    sheet_throughput(wb, bound)
    sheet_ranking(wb, rows)
    sheet_summary(wb, bound)
    wb.move_sheet("Summary", offset=-(len(wb.sheetnames) - 2))

    out = RESULTS / "solar_microbench.xlsx"
    wb.save(out)
    print(f"  wrote {out.name}: {len(wb.sheetnames)} sheets")
    print(f"  {', '.join(wb.sheetnames)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
