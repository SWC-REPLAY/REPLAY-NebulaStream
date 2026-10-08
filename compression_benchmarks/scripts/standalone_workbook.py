#!/usr/bin/env python3
"""Build solar_standalone_release.xlsx from the merged release-build standalone CSV.

    ./compression_benchmarks/scripts/standalone_workbook.py

Organised by SUBJECT, one sheet per question, rather than by the A1..A8 analysis numbering
workbook.py uses. Each sheet answers one thing and carries its own charts:

    Sizes               how many bytes each configuration put on disk
    Ratios              what that is as a factor, and how it moves with the unit size
    Timing              seconds, split into the part that is the store and the part that is not
    Throughput          the same as rates
    Framing             what the container costs, in bytes and per unit
    Store Cost          the question the baseline column makes answerable for the first time
    Anomalies           three things in the data that are not explained
    Summary             the four charts worth taking away

Every table cell is a SUMIFS against a sheet holding the CSV verbatim, so the workbook
recalculates if the measurements are redone and no number is typed in by hand.
"""

import csv
import sys
from pathlib import Path

from openpyxl import Workbook
from openpyxl.chart import BarChart, LineChart, Reference
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

REPO = Path(__file__).resolve().parents[2]
# Benchmark-build results live in their own directory; the Debug sweeps keep results/ 
# itself, so a number from one build is never mistaken for a number from the other.
RESULTS = REPO / "compression_benchmarks/results/benchmark"
SOURCE = "solar_standalone_release.csv"

ARIAL = "Arial"
GREY = "7F7F7F"
GREEN = "008000"
HDR = PatternFill("solid", fgColor="2F5496")
SUB = PatternFill("solid", fgColor="D9E2F3")
WARN = PatternFill("solid", fgColor="FFF2CC")

# (label, store_order, bound_pct). FileStore first: it is the uncompressed reference every other
# row is read against.
CONFIGS = [
    ("FileStore (raw)", "FileStore", ""),
    ("CompressedStore", "CompressedStore", ""),
    ("LossyStore lossless", "LossyStore", ""),
    ("LossyStore 0.1%", "LossyStore", "0.1"),
    ("LossyStore 1%", "LossyStore", "1"),
    ("LossyStore 5%", "LossyStore", "5"),
]
UNITS: list[int] = []
COLS: dict = {}
STORED = 0
ROWS_IN = 0


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


def sumifs(col, order, bound, unit):
    """One configuration's value for one column at one unit size, from the verbatim sheet."""
    crit = '""' if bound == "" else f'"{bound}"'
    return (f'=SUMIFS(Data!${COLS[col]}:${COLS[col]},'
            f'Data!${COLS["store_order"]}:${COLS["store_order"]},"{order}",'
            f'Data!${COLS["bound_pct"]}:${COLS["bound_pct"]},{crit},'
            f'Data!${COLS["unit_kb"]}:${COLS["unit_kb"]},{unit})')


def matrix(ws, top, heading, col, fmt="0.00", subtitle=None, transform=None, baseline=None):
    """Configurations down the side, unit sizes across. Returns (first row, last row).

    `baseline` appends one further row -- the store-free query -- labelled and formatted like the
    rest so a chart can pick it up as its own series. It is a reference, not a configuration, which
    is why it is drawn as a line over the bars rather than as another bar beside them.
    """
    r = f_section(ws, top, heading, width=1 + len(UNITS))
    if subtitle:
        r = f_note(ws, r, subtitle)
    r = f_head(ws, r, ["Configuration"] + [f"{u} KB" for u in UNITS])
    first = r
    for i, (label, order, bound) in enumerate(CONFIGS):
        ws.cell(row=first + i, column=1, value=label).font = Font(name=ARIAL)
        for j, u in enumerate(UNITS, start=2):
            formula = sumifs(col, order, bound, u)
            if transform:
                formula = transform(formula)
            c = ws.cell(row=first + i, column=j, value=formula)
            c.font = Font(name=ARIAL, color=GREEN)
            c.number_format = fmt
    last = first + len(CONFIGS) - 1
    if baseline is not None:
        last += 1
        ws.cell(row=last, column=1, value="normal query (no store)").font = Font(
            name=ARIAL, bold=True, color="C00000")
        for j, u in enumerate(UNITS, start=2):
            c = ws.cell(row=last, column=j, value=baseline(u))
            c.font = Font(name=ARIAL, bold=True, color="C00000")
            c.number_format = fmt
    return first, last


def chart(ws, kind, title, y_title, first, last, anchor, height=10, width=21,
          exclude_first=False, baseline_row=None):
    """Unit size on the category axis, one series per configuration -- the orientation
    workbook.py uses, so a chart here reads the same way as one there.

    `baseline_row` is drawn as a LINE over the bars rather than as another bar. That is the whole
    point of it: the bars are what each configuration cost, the line is what the same query costs
    with no store at all, and the VERTICAL GAP between them is the store. Reading a gap off a chart
    is easier than reading a subtraction out of a second table, and it keeps the absolute numbers
    on screen instead of replacing them with a derived one.
    """
    ch = LineChart() if kind == "line" else BarChart()
    if kind == "bar":
        ch.type, ch.gapWidth = "col", 60
    ch.title, ch.y_axis.title, ch.x_axis.title = title, y_title, "unit size (KB)"
    ch.height, ch.width = height, width
    start = first + 1 if exclude_first else first
    stop = baseline_row - 1 if baseline_row else last
    for i in range(start, stop + 1):
        ch.add_data(Reference(ws, min_col=1, max_col=1 + len(UNITS), min_row=i, max_row=i),
                    titles_from_data=True, from_rows=True)
    cats = Reference(ws, min_col=2, max_col=1 + len(UNITS), min_row=first - 1, max_row=first - 1)
    ch.set_categories(cats)

    if baseline_row:
        line = LineChart()
        line.add_data(Reference(ws, min_col=1, max_col=1 + len(UNITS),
                                min_row=baseline_row, max_row=baseline_row),
                      titles_from_data=True, from_rows=True)
        line.set_categories(cats)
        for series in line.series:
            series.graphicalProperties.line.width = 28000  # EMU, ~2.2pt -- readable over the bars
            series.graphicalProperties.line.solidFill = "C00000"
            series.smooth = False
        ch += line
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
    ws.column_dimensions["A"].width = 22
    for i in range(2, len(heads) + 1):
        ws.column_dimensions[get_column_letter(i)].width = 14
    return {h: get_column_letter(i) for i, h in enumerate(heads, start=1)}, rows


def widths(ws, first=30, rest=14):
    ws.column_dimensions["A"].width = first
    for i in range(2, 2 + len(UNITS)):
        ws.column_dimensions[get_column_letter(i)].width = rest


# ----------------------------------------------------------------------------- sheets

def sheet_readme(wb, facts):
    ws = wb.create_sheet("Read Me", 0)
    ws.column_dimensions["A"].width = 26
    ws.column_dimensions["B"].width = 116
    r = f_title(ws, 1, "Solar panels, standalone tiers — Benchmark build", 16)
    r = f_note(ws, r, "One sheet per question. Sizes, ratios, timing, throughput, framing, and what "
                      "of the time is actually the store.")
    r += 1
    for k, v in facts.items():
        ws.cell(row=r, column=1, value=k).font = Font(name=ARIAL, bold=True)
        f_note(ws, r, v, colour="000000", italic=False, col=2)
        r += 1
    r += 1
    r = f_section(ws, r, "WHAT IS NEW HERE", width=2)
    for k, v in {
        "the build": "Benchmark (-O3, NO_ASSERT, log level NONE), not the Debug build every other "
                     "CSV in results/ was measured in. On the read path Debug is ~88% build "
                     "overhead, enough to put FileStore -- which decodes nothing -- within 2% of "
                     "LossyStore at 1%, which runs three decode stages.",
        "the baseline": "Every row carries baseline_s: the same query with no store at all. "
                        "store_write_s = encode_s - baseline_s. Without it a store's cost could "
                        "only be quoted as an absolute, and on this workload three quarters of a "
                        "write query is the CSV parse.",
        "the store fixes": "Two FileStore defects were fixed between the Debug sweep and this one "
                           "(commits 6ba85c1cbb, 1492ece8a7). The second mattered: FileStore's "
                           "read-back was 113.6 s at a 16384 KB unit against a 93 s median, and is "
                           "now on the pack median. Decode figures here are not comparable to the "
                           "Debug sweep's; byte counts are, and reproduce exactly.",
    }.items():
        ws.cell(row=r, column=1, value=k).font = Font(name=ARIAL, bold=True)
        f_note(ws, r, v, colour="000000", italic=False, col=2)
        r += 1
    r += 1
    r = f_section(ws, r, "WHAT A TIME HERE IS", width=2)
    f_note(ws, r, "systest reports stop - running. Registration, planning, query compilation and "
                  "opening the store happen before `running` and are NOT included. Nothing here is "
                  "a codec step time: no store carries a timer, so encode and decode cannot be "
                  "separated from the query that drives them. That needs the microbenchmark.",
           colour="000000", italic=False, col=2)
    return ws


def sheet_sizes(wb):
    ws = wb.create_sheet("Sizes")
    widths(ws, 30, 15)
    r = f_title(ws, 1, "Sizes — what each configuration put on disk")
    r = f_note(ws, r, "Bytes, not ratios. A ratio hides the thing a storage budget cares about: "
                      "796 MB is the decision, 3.19x is the summary.")
    r += 1

    first, last = matrix(ws, r, "BYTES ON DISK  (framing included)", "disk_bytes", fmt="#,##0")
    r = last + 2
    chart(ws, "bar", "Bytes on disk by configuration", "bytes", first, last, f"A{r}")
    r += 22
    r = chart_notes(
        ws, r,
        "The file each configuration left behind, container and all, at each unit size.",
        "FileStore is flat at 1,195,928,436 B -- 42,700,000 rows x 28 B plus framing -- and is the "
        "reference every other bar is read against. The lossy bars GROW slightly as the unit grows, "
        "which is the opposite of what a reader expects and is explained on the Anomalies sheet.")
    r += 1

    first, last = matrix(ws, r, "ENCODED BYTES  (framing removed)", "data_out", fmt="#,##0",
                         subtitle="What the codec produced, with the container subtracted. This is "
                                  "the number to compare codecs on; disk_bytes mixes in a "
                                  "per-unit overhead that has nothing to do with the codec.")
    r = last + 2
    chart(ws, "bar", "Encoded bytes by configuration", "bytes", first, last, f"A{r}")
    r += 22
    r = chart_notes(
        ws, r,
        "The same configurations with framing taken out.",
        "At a 256 KB unit the span runs from 1,195,600,000 B for FileStore down to 362 MB for "
        "LossyStore at 5% -- about 834 MB of difference, which is the actual decision. The ratio "
        "chart on the next sheet cannot tell you that.")
    r += 1

    # Laid out by matrix() for the labels and formatting, then every cell replaced: this one is a
    # difference between two SUMIFS, which the single-column transform cannot express.
    first, last = matrix(ws, r, "BYTES SAVED AGAINST FileStore", "disk_bytes", fmt="#,##0",
                         subtitle="FileStore's bytes minus this configuration's. The row for "
                                  "FileStore is zero by construction.")
    for i, (_label, order, bound) in enumerate(CONFIGS):
        for j, u in enumerate(UNITS, start=2):
            ws.cell(row=first + i, column=j).value = (
                f'={sumifs("disk_bytes", "FileStore", "", u)[1:]}-{sumifs("disk_bytes", order, bound, u)[1:]}')
    r = last + 2
    chart(ws, "bar", "Bytes saved against FileStore", "bytes saved", first, last, f"A{r}", exclude_first=True)
    r += 22
    chart_notes(
        ws, r,
        "How many bytes each configuration avoided writing, against storing the rows raw. "
        "FileStore is excluded from the chart, being zero.",
        "LossyStore at 5% saves about 834 MB at a 256 KB unit and 810 MB at 16384 KB. zstd alone "
        "saves 471 MB and does not move with the unit size at all.")
    return ws


def sheet_ratios(wb):
    ws = wb.create_sheet("Ratios")
    widths(ws)
    r = f_title(ws, 1, "Ratios — the same thing as a factor")
    r = f_note(ws, r, "Useful for comparing configurations with each other. Not a substitute for "
                      "the Sizes sheet when the question is how much disk is needed.")
    r += 1

    first, last = matrix(ws, r, "RATIO ON DISK  (stored bytes / bytes on disk)", "ratio_on_disk",
                         fmt="0.000")
    r = last + 2
    chart(ws, "line", "Compression ratio by unit size", "x", first, last, f"A{r}")
    r += 22
    r = chart_notes(
        ws, r,
        "1,195,600,000 stored bytes divided by what each configuration wrote.",
        "FileStore reads 1.000 and is really 0.9997 -- it stores rows raw, so the shortfall is its "
        "framing. CompressedStore is flat to three decimals (1.650 to 1.652): zstd does not care "
        "about the unit size at this scale. The three bounded lossy lines FALL as the unit grows, "
        "which is the headline anomaly -- see the Anomalies sheet.")
    r += 1

    first, last = matrix(ws, r, "RATIO ON DATA  (framing removed)", "ratio_on_data", fmt="0.000",
                         subtitle="The codec's own figure. Differs from ratio_on_disk by the "
                                  "container, which the Framing sheet quantifies.")
    r = last + 2
    chart(ws, "line", "Ratio on encoded data by unit size", "x", first, last, f"A{r}")
    r += 22
    chart_notes(
        ws, r,
        "The same ratio with the container taken out of the denominator.",
        "Compare against the chart above. The gap between them IS the framing, and it is widest at "
        "256 KB where there are 4,561 units to put a header on.")
    return ws


def sheet_timing(wb):
    ws = wb.create_sheet("Timing")
    widths(ws)
    r = f_title(ws, 1, "Timing — seconds, and which of them are the store")
    r = f_note(ws, r, "N=1 per grid point except 4096 KB, which is N=3 after an outlier there was "
                      "re-measured and did not reproduce. Byte counts are deterministic; the "
                      "single samples are not, so read differences of a few tenths as noise.")
    r += 1

    base = lambda u: sumifs("baseline_s", "FileStore", "", u)

    first, last = matrix(ws, r, "WRITE QUERY  (encode_s, whole query)", "encode_s", baseline=base)
    r = last + 2
    chart(ws, "bar", "Write query seconds, against a query with no store", "seconds",
          first, last, f"A{r}", baseline_row=last)
    r += 22
    r = chart_notes(
        ws, r,
        "Bars are what each configuration cost. The red line is the SAME query with no store "
        "attached, measured at every grid point. The vertical gap between a bar and the line is "
        "the store.",
        "The line sits at about 20.8 s and the bars at 27.6 to 36.2, so roughly two thirds of "
        "every bar is below the line and has nothing to do with the store -- it is reading and "
        "parsing a 1.49 GB CSV. That is also why the configurations look so alike here: they are "
        "sharing the same large constant.")
    r += 1

    first, last = matrix(ws, r, "THE GAP, AS A NUMBER  (encode_s - baseline_s)", "store_write_s",
                         subtitle="The same quantity the chart above shows as a gap, for when a "
                                  "figure is wanted rather than a picture.")
    r = last + 2
    r = chart_notes(
        ws, r,
        "Each bar's distance above the red line.",
        "6.8 to 15.5 s. FileStore is cheapest because it writes the rows as they are. "
        "CompressedStore is dearest and gets worse as the unit grows, 12.4 to 15.5 s, while the "
        "lossy configurations get cheaper -- and CompressedStore's ratio does not improve to pay "
        "for it. See the Anomalies sheet.")
    r += 1

    first, last = matrix(ws, r, "READ-BACK  (decode_s, whole query)", "decode_s", baseline=base)
    r = last + 2
    chart(ws, "bar", "Read-back seconds, against a query with no store", "seconds",
          first, last, f"A{r}", baseline_row=last)
    r += 22
    chart_notes(
        ws, r,
        "Replaying everything back out. No CSV is involved -- ReplaySource calls store->read and "
        "nothing else, formatted Native -- so here the line is not a floor but a comparison.",
        "Every bar is BELOW the line: replaying 42.7M rows out of a store takes about 10 s against "
        "20.8 s to scan the same rows out of the CSV. Reading from the store is twice as fast as "
        "reading the file it came from. Flat at 9.5 to 10.9 s everywhere except LossyStore at 5% "
        "and 4096 KB, which reads 16.26 s and is unexplained -- see Anomalies.")
    return ws


def sheet_throughput(wb):
    ws = wb.create_sheet("Throughput")
    widths(ws)
    r = f_title(ws, 1, "Throughput — the same measurements as rates")
    r = f_note(ws, r, f"Over {STORED:,} stored bytes. The CSV is 1,489,260,482 B, so a rate quoted "
                      f"on CSV bytes would be 1.25x these.")
    r += 1

    rate = lambda f: f"={STORED}/({f[1:]})/1000000"
    base_rate = lambda u: rate(sumifs("baseline_s", "FileStore", "", u))

    first, last = matrix(ws, r, "WRITE QUERY  MB/s", "encode_s", fmt="0.0",
                         transform=rate, baseline=base_rate)
    r = last + 2
    chart(ws, "bar", "Write query throughput, against a query with no store", "MB/s",
          first, last, f"A{r}", baseline_row=last)
    r += 22
    r = chart_notes(
        ws, r,
        "Stored bytes over the whole write query. The red line is the same query with no store.",
        "33 to 43 MB/s against a store-free 57 MB/s. Every bar is BELOW the line here, which is "
        "the same fact as the Timing sheet seen the other way up: attaching a store makes the "
        "query slower, and the distance tells you by how much.")
    r += 1

    first, last = matrix(ws, r, "STORE WRITE  MB/s  (baseline removed)", "store_write_s", fmt="0.0",
                         transform=rate)
    r = last + 2
    chart(ws, "bar", "Store write throughput", "MB/s", first, last, f"A{r}")
    r += 22
    r = chart_notes(
        ws, r,
        "Stored bytes divided by the store's own write time -- the gap from the chart above, as a "
        "rate. No baseline line here: it has already been taken out.",
        "77 to 177 MB/s. FileStore is fastest because it does the least. These are three to four "
        "times the whole-query rates above, which is the measure of how much of that query is not "
        "the store.")
    r += 1

    first, last = matrix(ws, r, "READ-BACK  MB/s", "decode_s", fmt="0.0",
                         transform=rate, baseline=base_rate)
    r = last + 2
    chart(ws, "bar", "Read-back throughput, against a query with no store", "MB/s",
          first, last, f"A{r}", baseline_row=last)
    r += 22
    chart_notes(
        ws, r,
        "Stored bytes over the read-back query, with the store-free scan as the line.",
        "110 to 126 MB/s, roughly double the 57 MB/s line. A store that decodes nothing and one "
        "running quantise, bitpack and zstd replay at the same speed -- the codec is not what the "
        "read path spends its time on.")
    return ws


def sheet_framing(wb):
    ws = wb.create_sheet("Framing")
    widths(ws, 30, 15)
    r = f_title(ws, 1, "Framing — what the container costs")
    r = f_note(ws, r, "Header, per-unit descriptors, preallocated tables. Everything on disk that "
                      "is not encoded data.")
    r += 1

    first, last = matrix(ws, r, "FRAMING BYTES", "framing_bytes", fmt="#,##0")
    r = last + 2
    chart(ws, "bar", "Framing bytes by configuration", "bytes", first, last, f"A{r}")
    r += 22
    r = chart_notes(
        ws, r,
        "disk_bytes minus data_out: the bytes the container spent describing the data.",
        "It falls roughly 4x per step, because each step quarters the number of units. LossyStore "
        "pays the most at every size -- 1,076,774 B at 256 KB against FileStore's 328,436 and "
        "CompressedStore's 187,379 -- because it writes a descriptor per COLUMN per group, and "
        "this schema has four columns.")
    r += 1

    first, last = matrix(ws, r, "UNITS WRITTEN", "units", fmt="#,##0",
                         subtitle="Segments, or groups. Identical across configurations because "
                                  "the grid sets every tier's unit to the same size.")
    r = last + 2
    r = chart_notes(
        ws, r,
        "How many units each configuration sealed: 4,561 at 256 KB down to 72 at 16384 KB.",
        "This is the multiplier behind the framing chart above, and behind the ratio decline on "
        "the Ratios sheet -- though for a different reason in each case.")
    r += 1

    # As above: a quotient of two SUMIFS, so the cells are written explicitly.
    first, last = matrix(ws, r, "FRAMING PER UNIT  (bytes)", "framing_bytes", fmt="0.0")
    for i, (_label, order, bound) in enumerate(CONFIGS):
        for j, u in enumerate(UNITS, start=2):
            ws.cell(row=first + i, column=j).value = (
                f'={sumifs("framing_bytes", order, bound, u)[1:]}/MAX(1,{sumifs("units", order, bound, u)[1:]})')
    r = last + 2
    chart(ws, "bar", "Framing bytes per unit", "bytes per unit", first, last, f"A{r}")
    r += 22
    r = chart_notes(
        ws, r,
        "Framing divided by the number of units -- the per-unit cost of the container, independent "
        "of how many units there happen to be.",
        "Flat across unit sizes, as it should be: about 236 B per unit for LossyStore, 72 for "
        "FileStore, 41 for CompressedStore. A constant here is the check that the framing chart's "
        "decline really is just unit count and not something changing shape.")
    r += 1

    first, last = matrix(ws, r, "FRAMING AS A SHARE OF THE FILE  (%)", "framing_bytes", fmt="0.000")
    for i, (_label, order, bound) in enumerate(CONFIGS):
        for j, u in enumerate(UNITS, start=2):
            ws.cell(row=first + i, column=j).value = (
                f'=100*{sumifs("framing_bytes", order, bound, u)[1:]}'
                f'/MAX(1,{sumifs("disk_bytes", order, bound, u)[1:]})')
    r = last + 2
    chart_notes(
        ws, r,
        "The same framing as a percentage of the file it is in.",
        "Under 0.3% everywhere and under 0.01% at the largest unit. This is why ratio_on_disk and "
        "ratio_on_data agree to three decimals at scale, and why FileStore's 0.9997 rounds to "
        "1.000 -- the container is real but it is not where the bytes are.")
    return ws


def sheet_store_cost(wb):
    ws = wb.create_sheet("Store Cost")
    widths(ws, 30, 15)
    r = f_title(ws, 1, "Store cost — how much of a query is actually the store")
    c = ws.cell(row=r, column=1,
                value="This is the sheet the baseline column exists for. Every other standalone CSV "
                      "in results/ reports encode_s alone, which makes a store look like it costs "
                      "27 to 36 seconds. Most of that is reading and parsing a 1.49 GB CSV.")
    c.font = Font(name=ARIAL, bold=True)
    for i in range(1, 2 + len(UNITS)):
        ws.cell(row=r, column=i).fill = WARN
    r += 2

    first, last = matrix(ws, r, "STORE WRITE AS A SHARE OF THE WRITE QUERY  (%)", "store_write_s",
                         fmt="0.0")
    for i, (_label, order, bound) in enumerate(CONFIGS):
        for j, u in enumerate(UNITS, start=2):
            ws.cell(row=first + i, column=j).value = (
                f'=100*{sumifs("store_write_s", order, bound, u)[1:]}'
                f'/MAX(0.0001,{sumifs("encode_s", order, bound, u)[1:]})')
    r = last + 2
    chart(ws, "bar", "Store write as a share of the write query", "% of query", first, last, f"A{r}")
    r += 22
    r = chart_notes(
        ws, r,
        "store_write_s divided by encode_s: the fraction of a write query that is the store doing "
        "store work.",
        "25% for FileStore and 39 to 43% for CompressedStore. The remaining 57 to 75% is the CSV "
        "parse and the sink, and it is the same work whichever store is attached -- which is why "
        "the configurations look so similar on the whole-query chart and so different here.")
    r += 1

    r = f_section(ws, r, "THE BASELINE ITSELF", width=1 + len(UNITS))
    r = f_note(ws, r, "The same query with no store at all, measured at every grid point. It should "
                      "not depend on the unit size, and does not.")
    r = f_head(ws, r, ["Measurement"] + [f"{u} KB" for u in UNITS])
    bfirst = r
    ws.cell(row=r, column=1, value="baseline, no store (s)").font = Font(name=ARIAL)
    for j, u in enumerate(UNITS, start=2):
        c = ws.cell(row=r, column=j, value=sumifs("baseline_s", "FileStore", "", u))
        c.font = Font(name=ARIAL, color=GREEN)
        c.number_format = "0.000"
    r += 1
    ws.cell(row=r, column=1, value="as MB/s over stored bytes").font = Font(name=ARIAL)
    for j, u in enumerate(UNITS, start=2):
        c = ws.cell(row=r, column=j, value=f"={STORED}/({sumifs('baseline_s', 'FileStore', '', u)[1:]})/1000000")
        c.font = Font(name=ARIAL, color=GREEN)
        c.number_format = "0.0"
    r += 2
    chart_notes(
        ws, r,
        "The store-free query at each of the four grid points.",
        "About 20.8 s at three of them and 21.16 at 4096 KB, which was built and measured "
        "separately -- a spread under 2% across four independent builds of the same binary. That "
        "stability is what makes subtracting it legitimate.")
    return ws


def sheet_anomalies(wb):
    ws = wb.create_sheet("Anomalies")
    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 112
    r = f_title(ws, 1, "Anomalies", 14)
    r = f_note(ws, r, "Three things in this data that are not fully explained. Stated rather than "
                      "smoothed over.")
    r += 1
    for title, body in [
        ("1. Lossy ratios FALL as the unit grows",
         "3.302 -> 3.140 at 5%, 3.189 -> 2.972 at 1%, 3.022 -> 2.828 at 0.1%, from 256 KB to "
         "16384 KB. Backwards from the intuition that a bigger block compresses better. CONFIRMED "
         "and understood: the quantiser is logarithmic and bitWidth follows the GROUP's magnitude "
         "span, so a smaller group spans less and needs fewer bits. CompressedStore, which has no "
         "per-group width, is flat at 1.650-1.652 -- which is the control that makes this "
         "explanation stick."),
        ("2. LossyStore 5% read 16.26 s at 4096 KB — RESOLVED, it was interference",
         "Found at N=1. The whole 4096 KB point was re-run at N=3 and the cell came back at "
         "9.87 s, a drop of 6.39 s, landing it second-fastest of the six rather than 61% above "
         "them. The five controls in the same re-run moved by 0.06 to 0.57 s, which is ordinary "
         "drift -- only the anomalous cell moved, and it moved onto the pack median. A single "
         "disturbed sample, not a defect. THE 4096 KB COLUMN IN THIS WORKBOOK IS NOW THE N=3 "
         "RE-RUN; the 16.26 s figure survives only in this note. Worth recording why it was chased "
         "rather than shrugged at: FileStore's 113.6 s outlier in the Debug sweep was also called "
         "interference and turned out to be a real defect. The check is cheap; being wrong about "
         "it is not."),
        ("3. CompressedStore's store write RISES with unit size",
         "12.38 s at 1024 KB to 15.50 s at 16384 KB, while every lossy configuration gets cheaper "
         "over the same range. zstd compressing a 16 MB block rather than a 1 MB one is the "
         "obvious suspect, but its RATIO does not move at all (1.650 to 1.652), so it is spending "
         "more time for no benefit. Worth a look; not investigated."),
    ]:
        ws.cell(row=r, column=1, value=title).font = Font(name=ARIAL, bold=True)
        f_note(ws, r, body, colour="000000", italic=False, col=2)
        r += 2
    return ws


def sheet_summary(wb):
    ws = wb.create_sheet("Summary")
    widths(ws, 30, 15)
    r = f_title(ws, 1, "Summary", 16)
    r = f_note(ws, r, "Four charts, and what each one settles.")
    r += 1

    first, last = matrix(ws, r, "1. BYTES ON DISK", "disk_bytes", fmt="#,##0")
    r = last + 2
    chart(ws, "bar", "Bytes on disk by configuration", "bytes", first, last, f"A{r}")
    r += 22
    r = chart_notes(ws, r, "What each configuration costs in storage.",
                    "834 MB separates raw rows from LossyStore at 5%. That is the decision this "
                    "whole tier exists to make.")
    r += 1

    first, last = matrix(ws, r, "2. COMPRESSION RATIO", "ratio_on_disk", fmt="0.000")
    r = last + 2
    chart(ws, "line", "Compression ratio by unit size", "x", first, last, f"A{r}")
    r += 22
    r = chart_notes(ws, r, "The same thing as a factor, against unit size.",
                    "Build-independent and reproduces the Debug sweep exactly. Smaller units "
                    "compress BETTER on the bounded paths -- 256 KB is the best setting on both "
                    "ratio and, per the Timing sheet, it costs nothing to choose it.")
    r += 1

    base = lambda u: sumifs("baseline_s", "FileStore", "", u)

    first, last = matrix(ws, r, "3. WRITE QUERY, AGAINST A QUERY WITH NO STORE", "encode_s",
                         baseline=base)
    r = last + 2
    chart(ws, "bar", "Write query seconds, against a query with no store", "seconds",
          first, last, f"A{r}", baseline_row=last)
    r += 22
    r = chart_notes(ws, r,
                    "Bars are the write query; the red line is the same query with no store. The "
                    "gap is the store.",
                    "About two thirds of every bar lies below the line and is the CSV parse. The "
                    "store itself is 6.8 to 15.5 s -- quoting encode_s alone overstates it three "
                    "to four times.")
    r += 1

    first, last = matrix(ws, r, "4. READ-BACK, AGAINST A QUERY WITH NO STORE", "decode_s",
                         baseline=base)
    r = last + 2
    chart(ws, "bar", "Read-back seconds, against a query with no store", "seconds",
          first, last, f"A{r}", baseline_row=last)
    r += 22
    chart_notes(ws, r, "Replaying everything back out, against scanning the same rows from the CSV.",
                "Every bar is below the line: replaying from the store takes ~10 s against 20.8 s "
                "to read the file it came from. Flat whatever the codec and whatever the unit -- "
                "and after the two FileStore fixes, FileStore is no longer the outlier it was.")
    return ws


def main() -> int:
    global UNITS, COLS, STORED, ROWS_IN
    if not (RESULTS / SOURCE).is_file():
        print(f"missing {SOURCE} -- merge the per-unit CSVs first", file=sys.stderr)
        return 1

    wb = Workbook()
    wb.remove(wb.active)
    COLS, rows = data_sheet(wb)
    UNITS = sorted({int(r["unit_kb"]) for r in rows})
    sample = rows[0]
    STORED = int(sample["bytes_in"])
    ROWS_IN = int(sample["rows"])

    facts = {
        "dataset": f'{sample["dataset"]}, {ROWS_IN:,} rows x {sample["row_width_bytes"]} B '
                   f'= {STORED:,} B stored',
        "configurations": f"{len(CONFIGS)} standalone tiers",
        "unit sizes": ", ".join(f"{u} KB" for u in UNITS)
                      + "  (FileStore segment / LossyStore group / CompressedStore segment, set equal)",
        "capacity": f'FileStore totalSize {sample["filestore_total_mb"]} MB over '
                    f'{sample["filestore_files_at_capacity"]} files',
        "build": sample["build"],
        # Read off the data rather than stated, because the grid points no longer share a repeat
        # count: 4096 KB was re-measured at N=3 after an outlier there did not reproduce.
        "repeats": ", ".join(
            f'{u} KB: N={sorted({r["runs"] for r in rows if int(r["unit_kb"]) == u})[0]}'
            for u in UNITS),
        "source": SOURCE + " (merged from four per-unit CSVs)",
    }

    sheet_readme(wb, facts)
    sheet_sizes(wb)
    sheet_ratios(wb)
    sheet_timing(wb)
    sheet_throughput(wb)
    sheet_framing(wb)
    sheet_store_cost(wb)
    sheet_anomalies(wb)
    sheet_summary(wb)
    wb.move_sheet("Summary", offset=-(len(wb.sheetnames) - 2))

    out = RESULTS / "solar_standalone_release.xlsx"
    wb.save(out)
    print(f"  wrote {out.name}: {len(wb.sheetnames)} sheets")
    print(f"  {', '.join(wb.sheetnames)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
