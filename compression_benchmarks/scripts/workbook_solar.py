#!/usr/bin/env python3
"""Assemble the SOLAR results into one annotated workbook.

    ./compression_benchmarks/scripts/workbook_solar.py

Written with openpyxl throughout, unlike workbook_wind.py, which predates openpyxl being
available here and goes via Flat ODS. This is the better shape of the two.

Ten sheets. Three carry the measurements verbatim from the CSVs; the rest are analysis, and every
analysis cell is a SUMIFS or an arithmetic formula against those three -- never a number this
script computed. Correct a measurement, rebuild, and the analysis follows.

Each chart is followed by two plain-language cells, WHAT THIS SHOWS and WHAT TO LOOK FOR, because
a chart title alone does not tell a reader which way is good or what would be surprising.

The Anomalies sheet is the point of the exercise. Four things in this data are not what a reader
would predict, and one of them is unexplained; they are listed with their evidence, whether they
reproduce, and what would settle them.
"""

import csv
import subprocess
import sys
from pathlib import Path

from openpyxl import Workbook
from openpyxl.chart import BarChart, LineChart, Reference
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

REPO = Path(__file__).resolve().parents[2]
RESULTS = REPO / "compression_benchmarks/results"
BOOK = RESULTS / "solar_analysis.xlsx"

UNITS = [256, 1024, 4096, 16384]
ROWS_IN, ROW_WIDTH = 42_700_000, 28
BYTES_IN = ROWS_IN * ROW_WIDTH

STANDALONE = [
    ("FileStore", "FileStore", None),
    ("CompressedStore", "CompressedStore", None),
    ("LossyStore lossless", "LossyStore", None),
    ("LossyStore 0.1%", "LossyStore", 0.1),
    ("LossyStore 1%", "LossyStore", 1),
    ("LossyStore 5%", "LossyStore", 5),
]
CHAINED = [
    ("-> CompressedStore", "MemoryStore->FileStore->CompressedStore", None),
    ("-> LossyStore lossless", "MemoryStore->FileStore->LossyStore", None),
    ("-> LossyStore 0.1%", "MemoryStore->FileStore->LossyStore", 0.1),
    ("-> LossyStore 1%", "MemoryStore->FileStore->LossyStore", 1),
    ("-> LossyStore 5%", "MemoryStore->FileStore->LossyStore", 5),
]
BOUNDS = [0.0, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0]

ARIAL = "Arial"
HDR = PatternFill("solid", fgColor="2F5496")
SUB = PatternFill("solid", fgColor="D9E2F3")
WARN = PatternFill("solid", fgColor="FFF2CC")
GREEN, GREY = "1F6F3D", "555555"
THIN = Border(bottom=Side(style="thin", color="B0B0B0"))


def f_title(ws, r, text, size=14):
    c = ws.cell(row=r, column=1, value=text)
    c.font = Font(name=ARIAL, bold=True, size=size)
    return r + 1


def f_note(ws, r, text, colour=GREY, italic=True, col=1):
    c = ws.cell(row=r, column=col, value=text)
    c.font = Font(name=ARIAL, italic=italic, color=colour)
    c.alignment = Alignment(wrap_text=False, vertical="top")
    return r + 1


def f_section(ws, r, text, width=8):
    c = ws.cell(row=r, column=1, value=text)
    c.font = Font(name=ARIAL, bold=True)
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
    """The two cells that make a chart readable without the analyst standing next to it."""
    ws.cell(row=r, column=1, value="WHAT THIS SHOWS").font = Font(name=ARIAL, bold=True, size=9)
    f_note(ws, r, shows, col=2)
    ws.cell(row=r + 1, column=1, value="WHAT TO LOOK FOR").font = Font(name=ARIAL, bold=True, size=9)
    f_note(ws, r + 1, look_for, col=2)
    return r + 2


def data_sheet(wb, name, csv_name, numeric_hint=()):
    """One sheet holding a CSV verbatim. Returns {column name: spreadsheet letter}."""
    rows = list(csv.DictReader((RESULTS / csv_name).open()))
    ws = wb.create_sheet(name)
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
    ws.column_dimensions["A"].width = 34
    for i in range(2, len(heads) + 1):
        ws.column_dimensions[get_column_letter(i)].width = 13
    return {h: get_column_letter(i) for i, h in enumerate(heads, start=1)}, len(rows)


def sumifs(sheet, col, cols, order, bound, unit):
    crit = '""' if bound is None else repr(bound)
    return (f'=SUMIFS({sheet}!${cols[col]}:${cols[col]},'
            f'{sheet}!${cols["store_order"]}:${cols["store_order"]},"{order}",'
            f'{sheet}!${cols["bound_pct"]}:${cols["bound_pct"]},{crit},'
            f'{sheet}!${cols["unit_kb"]}:${cols["unit_kb"]},{unit})')


def matrix(ws, top, heading, configs, sheet, cols, value_col, fmt="0.000", subtitle=None):
    r = f_section(ws, top, heading, width=1 + len(UNITS))
    if subtitle:
        r = f_note(ws, r, subtitle)
    r = f_head(ws, r, ["Configuration"] + [f"{u} KB" for u in UNITS])
    first = r
    for i, (label, order, bound) in enumerate(configs):
        ws.cell(row=first + i, column=1, value=label).font = Font(name=ARIAL)
        for j, u in enumerate(UNITS, start=2):
            c = ws.cell(row=first + i, column=j,
                        value=sumifs(sheet, value_col, cols, order, bound, u))
            c.font = Font(name=ARIAL, color=GREEN)
            c.number_format = fmt
    return first, first + len(configs) - 1


def add_chart(ws, kind, title, y_title, n_series, first, anchor, height=9, width=19, log=False):
    ch = LineChart() if kind == "line" else BarChart()
    if kind == "bar":
        ch.type, ch.gapWidth = "col", 60
    ch.title, ch.y_axis.title, ch.x_axis.title = title, y_title, "unit size (KB)"
    ch.height, ch.width = height, width
    if log:
        ch.y_axis.scaling.logBase = 10
    for i in range(n_series):
        ch.add_data(Reference(ws, min_col=1, max_col=1 + len(UNITS),
                              min_row=first + i, max_row=first + i),
                    titles_from_data=True, from_rows=True)
    ch.set_categories(Reference(ws, min_col=2, max_col=1 + len(UNITS),
                                min_row=first - 1, max_row=first - 1))
    ws.add_chart(ch, anchor)


def sheet_readme(wb):
    ws = wb.create_sheet("Read Me", 0)
    ws.column_dimensions["A"].width = 24
    ws.column_dimensions["B"].width = 118
    r = f_title(ws, 1, "Solar panel readings — replay store compression, full scale", 16)
    r = f_note(ws, r, "Measured through the real SQL query path, not by calling the codecs directly. "
                      "One run per configuration (N=1).")
    r += 1

    r = f_section(ws, r, "WHAT WAS MEASURED", 2)
    for k, v in [
        ("Input", f"{ROWS_IN:,} rows x {ROW_WIDTH} B = {BYTES_IN:,} B (1.11 GiB) of DEBS solar panel readings"),
        ("Unit sizes", "256, 1024, 4096, 16384 KB. 16 and 64 KB were dropped: at this volume they are "
                       "granule bookkeeping, not a codec measurement."),
        ("Standalone", "6 configurations. NO MemoryStore -- standing alone it holds ~8 MB and would "
                       "have wrapped ~150 times, measuring an evicting store."),
        ("Chained", "5 configurations, MemoryStore -> FileStore -> tail."),
        ("Float column", "9 accuracy bounds from lossless to 20%, write-only, at default unit sizes."),
        ("Repeats", "N=1. Byte counts are deterministic and exact; TIMINGS are single samples and "
                    "should not be read to better than a few percent."),
    ]:
        ws.cell(row=r, column=1, value=k).font = Font(name=ARIAL, bold=True)
        r = f_note(ws, r, v, col=2)
    r += 1

    r = f_section(ws, r, "HEADLINE FINDINGS", 2)
    for k, v in [
        ("Solar is hard", "Best ratio 3.31 (LossyStore, 5%, 256 KB) against wind's 8.33. Predicted by the "
                          "data: producerId is a shuffled permutation of 0..42.7M so nothing can compress it, "
                          "and the timestamp has no repeating delta."),
        ("Smaller is better", "Every BOUNDED configuration compresses BETTER at smaller units, monotonically. "
                              "This is the opposite of the usual expectation. See Anomalies."),
        ("zstd does not care", "CompressedStore is 1.650 -> 1.652 across a 64x change in unit size. Flat."),
        ("Chaining is free here", "Chained ratios are within 0.5% of standalone. FileStore keeps only 0.35-0.93% "
                                  "of the stream raw, because its 4 MB flush threshold is tiny relative to 1.2 GB."),
        ("The bound buys a lot", "The float column is 51% of a lossless store and stored RAW. The very first "
                                 "bound, 0.01%, removes 73.6% of it."),
        ("Decode is now flat", "85-95 s at every unit size. Before the per-cursor decode buffer was added, a "
                               "16384 KB unit would have cost 64x a 256 KB one."),
    ]:
        ws.cell(row=r, column=1, value=k).font = Font(name=ARIAL, bold=True)
        r = f_note(ws, r, v, col=2)
    r += 1

    r = f_section(ws, r, "HOW TO READ THIS WORKBOOK", 2)
    for k, v in [
        ("Green figures", "Formulas computed by the sheet. Black figures are measurements."),
        ("Data sheets", "Standalone, Chained and Float Column hold the CSVs verbatim. Everything else "
                        "is derived from them by formula, so correcting a measurement moves the analysis."),
        ("ratio_on_data", "bytes_in / data_out -- framing excluded. Compares codecs."),
        ("ratio_on_disk", "bytes_in / disk_bytes -- framing included. What it costs to keep."),
        ("ratio_on_tail", "Chained only. Excludes the bytes FileStore is still holding raw, so it is what "
                          "the tail codec achieved on what it was actually handed."),
        ("Anomalies sheet", "Read it before quoting any single number. Four results here are not what a "
                            "reader would predict and one is unexplained."),
    ]:
        ws.cell(row=r, column=1, value=k).font = Font(name=ARIAL, bold=True)
        r = f_note(ws, r, v, col=2)
    return ws


def sheet_dataset(wb):
    ws = wb.create_sheet("Dataset")
    ws.column_dimensions["A"].width = 30
    for c in "BCDEFGHIJKLM":
        ws.column_dimensions[c].width = 15
    r = f_title(ws, 1, "What the solar data looks like")
    r = f_note(ws, r, "From dataset_analysis.py over the whole 1.49 GB CSV: exact distinct counts via a "
                      "disk-backed sort, extents and decimal places in one streaming pass.")
    r += 1
    r = f_section(ws, r, "FACTS", 2)
    for row in csv.DictReader((RESULTS / "solar_dataset_facts.csv").open()):
        ws.cell(row=r, column=1, value=row["item"]).font = Font(name=ARIAL, bold=True)
        v = row["value"]
        try:
            v = int(v)
        except ValueError:
            pass
        c = ws.cell(row=r, column=2, value=v)
        c.font = Font(name=ARIAL)
        if isinstance(v, int):
            c.number_format = "#,##0"
        r += 1
    r += 1
    r = f_section(ws, r, "PER COLUMN", 13)
    cols = list(csv.DictReader((RESULTS / "solar_dataset_description.csv").open()))
    heads = list(cols[0])
    r = f_head(ws, r, heads)
    for row in cols:
        for i, h in enumerate(heads, start=1):
            v = row[h]
            try:
                v = float(v) if "." in v else int(v)
            except (ValueError, TypeError):
                pass
            ws.cell(row=r, column=i, value=v).font = Font(name=ARIAL)
        r += 1
    r += 1
    r = f_section(ws, r, "WHY THIS MATTERS FOR COMPRESSION", 2)
    for k, v in [
        ("producerId", "42,700,000 distinct over exactly 0..42,699,999 -- a shuffled permutation of the row "
                       "numbers. No dictionary, no run length, and delta-of-delta is worse than useless on a "
                       "shuffled sequence. Bitpacking alone: 32 bits -> 26."),
        ("groupId", "4 distinct. Two bits of real information, and the one column that compresses freely."),
        ("producedPower", "97.90% distinct at six decimal places. The ONLY quantisable column, so every byte "
                          "an accuracy bound saves comes out of this one."),
        ("timestamp", "Monotonic, so deltas are positive -- but over 4096 distinct deltas with no cycle, mean "
                      "40,342 and max 743,465. Delta plus bitpack takes 64 bits to about 20."),
        ("Against wind", "Wind had 26 distinct producerIds and exactly TWO distinct timestamp deltas. That is "
                         "why its ratios are 2-2.5x these, and why it shows a zigzag that solar does not."),
    ]:
        ws.cell(row=r, column=1, value=k).font = Font(name=ARIAL, bold=True)
        r = f_note(ws, r, v, col=2)
    return ws


def sheet_ratios(wb, S, C):
    ws = wb.create_sheet("A1 Ratio by Unit")
    ws.column_dimensions["A"].width = 24
    for c in "BCDE":
        ws.column_dimensions[c].width = 12
    r = f_title(ws, 1, "What the unit size buys")
    r = f_note(ws, r, "The unit is the granule a tier seals on: FileStore's segment, LossyStore's group, "
                      "CompressedStore's segment. All three move together in this sweep.")
    r += 1

    f1, l1 = matrix(ws, r, "STANDALONE  -  ratio_on_data (bytes_in / data_out)", STANDALONE,
                    "Standalone", S, "ratio_on_data",
                    subtitle="One store, no tiering. Higher is better.")
    add_chart(ws, "line", "Standalone: compression ratio by unit size", "ratio",
              len(STANDALONE), f1, "H3")
    r = l1 + 2
    r = chart_notes(ws, r,
        "Each line is one store configuration; the x axis is the granule it seals on, 64x from left to right.",
        "The three BOUNDED lossy lines all slope DOWN -- smaller units compress better. That is backwards "
        "from the usual intuition and is Anomaly 1. CompressedStore is a flat line at 1.65: zstd does not "
        "care about the block size here. FileStore is flat at 1.0 by definition, it is the baseline.")
    r += 1

    f2, l2 = matrix(ws, r, "STANDALONE  -  ratio_on_disk (framing included)", STANDALONE,
                    "Standalone", S, "ratio_on_disk", fmt="0.00000",
                    subtitle="Same, but counting file headers, preallocated tables and per-unit "
                             "descriptors. FIVE decimals on purpose -- see the note below.")
    add_chart(ws, "line", "Standalone: ratio on disk", "ratio", len(STANDALONE), f2, "H24")
    r = l2 + 2
    r = chart_notes(ws, r,
        "What the data actually costs to keep, as opposed to what the codec produced.",
        "It sits almost exactly on top of the previous chart. On WIND these two diverged by up to 10.8% at "
        "a 16 KB unit; here the gap is under 0.3% everywhere, because 1.2 GB is far too much data for "
        "per-unit framing to matter.")
    r = f_note(ws, r, "WHY FILESTORE LOOKS LIKE EXACTLY 1: it is not. Its framing is 328,436 B at 256 KB "
                      "falling to 5,876 B at 16384 KB, so the ratio is 0.99973 .. 0.99999. At three decimal "
                      "places that rounds to 1.000 and the framing disappears. This table therefore shows "
                      "five. A ratio is the wrong instrument for an overhead this small against 1.2 GB -- "
                      "the byte table below is the honest one.")
    r += 1

    f2b, l2b = matrix(ws, r, "FRAMING IN BYTES  -  disk_bytes minus data_out", STANDALONE,
                      "Standalone", S, "framing_bytes", fmt="#,##0",
                      subtitle="The same overhead stated absolutely, where nothing is lost to rounding.")
    add_chart(ws, "line", "Framing in bytes (log scale)", "bytes", len(STANDALONE), f2b, "H45", log=True)
    r = l2b + 2
    r = chart_notes(ws, r,
        "Actual bytes of bookkeeping per configuration, on a log axis because the range is 1000x.",
        "Every line falls ~4x per step, tracking the unit count exactly -- framing is per-unit, so four "
        "times fewer units means four times less of it. LossyStore pays the most (236 B per group: a 36 B "
        "group header plus 50 B per column), FileStore the least. Even LossyStore's worst case here, about "
        "1.1 MB, is 0.09% of the file.")
    r += 1

    f3, l3 = matrix(ws, r, "CHAINED  -  ratio_on_data (the whole chain)", CHAINED,
                    "Chained", C, "ratio_on_data",
                    subtitle="MemoryStore -> FileStore -> tail. Counts whatever FileStore is still holding raw.")
    add_chart(ws, "line", "Chained: ratio over the whole chain", "ratio", len(CHAINED), f3, "H45")
    r = l3 + 2
    r = chart_notes(ws, r,
        "The same sweep, but through the full three-tier chain instead of a single store.",
        "It should look almost identical to the standalone chart, and it does. Everything the chain costs is "
        "in the next sheet; the short version is 0.4%.")
    return ws


def sheet_chaining(wb, S, C):
    ws = wb.create_sheet("A2 Chaining Cost")
    ws.column_dimensions["A"].width = 24
    for c in "BCDE":
        ws.column_dimensions[c].width = 12
    r = f_title(ws, 1, "What the tier chain costs")
    r = f_note(ws, r, "FileStore migrates to the tail once it has accumulated its flush threshold, and keeps "
                      "the remainder UNCOMPRESSED. That remainder is what a chain costs.")
    r += 1

    f1, l1 = matrix(ws, r, "BYTES FILESTORE IS STILL HOLDING, RAW", CHAINED,
                    "Chained", C, "FileStore_data", fmt="#,##0",
                    subtitle="Read straight off the store files.")
    r = l1 + 1
    r = f_note(ws, r, "As a share of the whole stream:")
    head = f_head(ws, r, ["Configuration"] + [f"{u} KB" for u in UNITS])
    for i, (label, _o, _b) in enumerate(CHAINED):
        ws.cell(row=head + i, column=1, value=label).font = Font(name=ARIAL)
        for j in range(len(UNITS)):
            col = get_column_letter(2 + j)
            c = ws.cell(row=head + i, column=2 + j, value=f"={col}{f1 + i}/{BYTES_IN}")
            c.font, c.number_format = Font(name=ARIAL, color=GREEN), "0.00%"
    add_chart(ws, "bar", "Share of the stream FileStore keeps uncompressed", "share of bytes_in",
              len(CHAINED), head, "H3")
    r = head + len(CHAINED) + 1
    r = chart_notes(ws, r,
        "The fraction of the 1.2 GB that never reached a compressing tier.",
        "Everything is between 0.35% and 0.93%, so the chain compresses essentially all of it. The bars step "
        "up at 16384 KB because the leftover is one SEGMENT and the segment got bigger -- not because the "
        "chain behaved differently.")
    r += 2

    r = f_section(ws, r, "CHAINED RATIO / STANDALONE RATIO", 1 + len(UNITS))
    r = f_note(ws, r, "1.000 would mean the chain gave up nothing at all against a single store.")
    head2 = f_head(ws, r, ["Configuration"] + [f"{u} KB" for u in UNITS])
    for i, (label, _o, bound) in enumerate(CHAINED):
        tail = "CompressedStore" if "Compressed" in label else "LossyStore"
        ws.cell(row=head2 + i, column=1, value=label).font = Font(name=ARIAL)
        for j, u in enumerate(UNITS, start=2):
            num = sumifs("Chained", "ratio_on_data", C, CHAINED[i][1], bound, u)[1:]
            den = sumifs("Standalone", "ratio_on_data", S, tail, bound, u)[1:]
            c = ws.cell(row=head2 + i, column=j, value=f"=IFERROR(({num})/({den}),0)")
            c.font, c.number_format = Font(name=ARIAL, color=GREEN), "0.000"
    add_chart(ws, "bar", "Chained ratio as a fraction of standalone", "x", len(CHAINED), head2, "H24")
    r = head2 + len(CHAINED) + 1
    r = chart_notes(ws, r,
        "How much of a single store's compression the three-tier chain keeps.",
        "Everything is 0.995 to 1.001, so chaining is free at this scale. On WIND the same figure was 0.67 to "
        "0.87 -- the chain gave up 13-33%. The difference is NOT the code: the flush threshold is a fixed "
        "4 MB, which is 6.6% of wind's 5.6 MB but 0.2% of solar's 1.2 GB. Three cells read slightly ABOVE "
        "1.000, which should be impossible; see Anomaly 3.")
    return ws


def sheet_accuracy(wb, S, C):
    ws = wb.create_sheet("A3 Accuracy and Float")
    ws.column_dimensions["A"].width = 24
    for c in "BCDEFGHIJ":
        ws.column_dimensions[c].width = 13
    r = f_title(ws, 1, "What the accuracy bound buys")
    r = f_note(ws, r, "The bound reaches floating point columns and nothing else. Here that is exactly one "
                      "column of four, producedPower.")
    r += 1

    lossy = [c for c in STANDALONE if c[1] == "LossyStore"]
    f1, l1 = matrix(ws, r, "STANDALONE LOSSYSTORE  -  ratio by bound", lossy,
                    "Standalone", S, "ratio_on_data")
    add_chart(ws, "line", "LossyStore: ratio by accuracy bound", "ratio", len(lossy), f1, "H3")
    r = l1 + 2
    r = chart_notes(ws, r,
        "Four lines: lossless, then 0.1%, 1% and 5% error allowances, each across the unit-size sweep.",
        "The gap between the lossless line and the others is what quantisation buys -- roughly 1.6x to 1.8x. "
        "The lines do not cross, so a wider bound always beats a tighter one at whole-store level. That is "
        "NOT true column by column: see the float table below and Anomaly 2.")
    r += 1

    r = f_section(ws, r, "GAIN OVER LOSSLESS AT THE SAME UNIT SIZE", 1 + len(UNITS))
    r = f_note(ws, r, "Bounded ratio divided by the lossless ratio. 1.00 would mean the bound bought nothing.")
    head = f_head(ws, r, ["Configuration"] + [f"{u} KB" for u in UNITS])
    for i in range(1, len(lossy)):
        ws.cell(row=head + i - 1, column=1, value=lossy[i][0]).font = Font(name=ARIAL)
        for j in range(len(UNITS)):
            col = get_column_letter(2 + j)
            c = ws.cell(row=head + i - 1, column=2 + j, value=f"={col}{f1 + i}/{col}{f1}")
            c.font, c.number_format = Font(name=ARIAL, color=GREEN), "0.000"
    add_chart(ws, "bar", "Gain over lossless", "x", len(lossy) - 1, head, "H24")
    r = head + len(lossy) + 1
    r = chart_notes(ws, r,
        "How much better each bound is than storing the same data losslessly.",
        "The gain SHRINKS as the unit grows -- 1.78x at 256 KB down to 1.71x at 16384 KB for the 5% bound, "
        "and the same direction at 0.1% and 1%. Both the lossless and the bounded ratios fall with unit "
        "size, but the bounded ones fall faster (-5.2% against -1.4% over the sweep), so quantisation is "
        "worth relatively LESS at coarse granularity. That points the same way as Anomaly 1: on this data, "
        "small units and tight bounds.")
    r += 2

    r = f_section(ws, r, "THE FLOAT COLUMN ALONE  -  producedPower, all 9 bounds", 7)
    r = f_note(ws, r, "Measured at the DEFAULT unit size, write-only. The three other columns are the "
                      "control: if a bound moved them, something would be wrong.")
    head3 = f_head(ws, r, ["bound %", "bytes", "bytes/row", "vs lossless", "codec",
                           "worst error", "error ceiling"])
    fc = [x for x in csv.DictReader((RESULTS / "solar_float_column.csv").open())
          if x["column"] == "producedPower"]
    for i, x in enumerate(fc):
        rr = head3 + i
        vals = [float(x["bound_pct"]), int(x["bytes"]), float(x["bytes_per_row"]), None,
                x["codec"], float(x["worst_recorded_error"]), None]
        for j, v in enumerate(vals, start=1):
            c = ws.cell(row=rr, column=j, value=v)
            c.font = Font(name=ARIAL)
        ws.cell(row=rr, column=2).number_format = "#,##0"
        d = ws.cell(row=rr, column=4, value=f"=IFERROR(B{rr}/B{head3}-1,0)")
        d.font, d.number_format = Font(name=ARIAL, color=GREEN), "0.0%"
        e = ws.cell(row=rr, column=7, value=f"=A{rr}/100")
        e.font, e.number_format = Font(name=ARIAL, color=GREEN), "0.000000"
        if i and int(x["bytes"]) > int(fc[i - 1]["bytes"]):
            for j in range(1, 8):
                ws.cell(row=rr, column=j).fill = WARN
    r = head3 + len(fc) + 1
    r = f_note(ws, r, "HIGHLIGHTED ROW: the 5% bound produces a LARGER column than the 2% bound, +2.88%. A "
                      "looser error allowance should never cost more bytes. See Anomaly 2.")
    r = f_note(ws, r, "Losslessly this column is stored Raw at 7.65 B/row for an 8-byte value and is 326 MB "
                      "-- about 51% of the whole lossless store. That is why the very first bound, 0.01%, "
                      "removes 73.6% of it.")
    r = f_note(ws, r, "'worst error' is the encoder's own accounting, not an independent check against the "
                      "source rows. It tracks the ceiling closely at every bound, which is reassuring but "
                      "not proof.")
    return ws


def sheet_overhead(wb, S):
    ws = wb.create_sheet("A4 Overhead and Timing")
    ws.column_dimensions["A"].width = 24
    for c in "BCDE":
        ws.column_dimensions[c].width = 12
    r = f_title(ws, 1, "Framing, unit counts and time")
    r += 1

    f1, l1 = matrix(ws, r, "FRAMING AS A SHARE OF THE FILE", STANDALONE,
                    "Standalone", S, "framing_ratio", fmt="0.0000%",
                    subtitle="File header, preallocated table and per-unit descriptors, over disk_bytes.")
    add_chart(ws, "line", "Framing share of the file", "share of disk_bytes", len(STANDALONE), f1, "H3")
    r = l1 + 2
    r = chart_notes(ws, r,
        "How much of each file is bookkeeping rather than data.",
        "Under 0.3% everywhere, and falling with unit size as you would expect. On WIND this reached 10.8% "
        "at a 16 KB unit and drove the choice of unit size; at 1.2 GB it is irrelevant. This is the clearest "
        "way in which conclusions from the small dataset do not transfer.")
    r += 1

    f2, l2 = matrix(ws, r, "UNITS WRITTEN", STANDALONE, "Standalone", S, "units", fmt="#,##0")
    add_chart(ws, "line", "Units written (log scale)", "count", len(STANDALONE), f2, "H24", log=True)
    r = l2 + 2
    r = chart_notes(ws, r,
        "How many granules each unit size produces. 4,561 at 256 KB down to 72 at 16384 KB.",
        "A clean 4x step each time, which confirms the sweep actually changed what it claimed to. Framing "
        "tracks this line exactly.")
    r += 1

    f3, l3 = matrix(ws, r, "ENCODE SECONDS", STANDALONE, "Standalone", S, "encode_s", fmt="0.0")
    add_chart(ws, "line", "Encode time", "seconds", len(STANDALONE), f3, "H45")
    r = l3 + 2
    r = chart_notes(ws, r,
        "Wall time of the write query. N=1, so read these to a few percent at best.",
        "The 256 KB column is 12-19% slower than everything to its right, consistently across all six "
        "configurations -- that is 4,561 seal operations against 72. Beyond 1024 KB it is flat.")
    r += 1

    f4, l4 = matrix(ws, r, "DECODE SECONDS", STANDALONE, "Standalone", S, "decode_s", fmt="0.0")
    add_chart(ws, "line", "Decode time", "seconds", len(STANDALONE), f4, "H66")
    r = l4 + 2
    r = chart_notes(ws, r,
        "Wall time of the read-back query.",
        "FLAT at 85-95 s across a 64x change in unit size. This is the headline result for the decode cache "
        "added before this run: previously a tier re-decoded its whole unit on every 4 KB output buffer, so "
        "a 16384 KB unit cost 64x a 256 KB one. One cell stands out -- FileStore at 16384 KB, 113.6 s -- see "
        "Anomaly 4.")
    return ws


FLOAT_COLS = [("producedPower", 8), ("producerId", 4), ("groupId", 4), ("ts", 8)]


def fc_sumifs(F, col_name, bound, value="bytes"):
    return (f'SUMIFS(\'Float Column\'!${F[value]}:${F[value]},'
            f'\'Float Column\'!${F["column"]}:${F["column"]},"{col_name}",'
            f'\'Float Column\'!${F["bound_pct"]}:${F["bound_pct"]},{bound})')


def sheet_float_detail(wb, F):
    """The float column on its own terms: a ratio, not just a byte count."""
    ws = wb.create_sheet("A5 Float Column Detail")
    ws.column_dimensions["A"].width = 26
    for c in "BCDEFGH":
        ws.column_dimensions[c].width = 15
    r = f_title(ws, 1, "producedPower — what the bound actually achieves on the one column it touches")
    r = f_note(ws, r, f"A FLOAT64 costs 8 bytes, so {ROWS_IN:,} rows of it are "
                      f"{ROWS_IN * 8:,} B unencoded. Every ratio here is against that, which is the "
                      f"number a whole-store ratio cannot show you.")
    r = f_note(ws, r, "Measured at the DEFAULT unit size, write-only, so these do not vary with the unit "
                      "sweep on the other sheets.")
    r += 1

    r = f_section(ws, r, "PER BOUND", 7)
    head = f_head(ws, r, ["bound %", "stored bytes", "bits per value", "x vs FLOAT64",
                          "x vs lossless stored", "marginal step", "share of whole store"])
    first = head
    for i, b in enumerate(BOUNDS):
        rr = head + i
        ws.cell(row=rr, column=1, value=b).font = Font(name=ARIAL)
        cells = {
            2: f"={fc_sumifs(F, 'producedPower', b)}",
            3: f"=B{rr}*8/{ROWS_IN}",
            4: f"={ROWS_IN * 8}/B{rr}",
            5: f"=$B${first}/B{rr}",
            6: ("" if i == 0 else f"=B{rr}/B{rr - 1}-1"),
            7: "=B{0}/({1})".format(rr, "+".join(fc_sumifs(F, c, b) for c, _w in FLOAT_COLS)),
        }
        fmts = {2: "#,##0", 3: "0.000", 4: "0.000", 5: "0.000", 6: "+0.00%;-0.00%", 7: "0.0%"}
        for col, val in cells.items():
            if val == "":
                continue
            c = ws.cell(row=rr, column=col, value=val)
            c.font, c.number_format = Font(name=ARIAL, color=GREEN), fmts[col]
        if i and i == BOUNDS.index(5.0):
            for col in range(1, 8):
                ws.cell(row=rr, column=col).fill = WARN
    last = head + len(BOUNDS) - 1

    ch = LineChart()
    ch.title = "producedPower: compression ratio against the accuracy bound"
    ch.y_axis.title, ch.x_axis.title = "x vs unencoded FLOAT64", "accuracy bound (%)"
    ch.height, ch.width = 9, 19
    ch.add_data(Reference(ws, min_col=4, min_row=head - 1, max_row=last), titles_from_data=True)
    ch.set_categories(Reference(ws, min_col=1, min_row=head, max_row=last))
    ws.add_chart(ch, "J3")

    ch2 = LineChart()
    ch2.title = "producedPower: bits stored per value"
    ch2.y_axis.title, ch2.x_axis.title = "bits per value", "accuracy bound (%)"
    ch2.height, ch2.width = 9, 19
    ch2.add_data(Reference(ws, min_col=3, min_row=head - 1, max_row=last), titles_from_data=True)
    ch2.set_categories(Reference(ws, min_col=1, min_row=head, max_row=last))
    ws.add_chart(ch2, "J22")

    ch3 = BarChart()
    ch3.type, ch3.gapWidth = "col", 40
    ch3.title = "Share of the whole store this one column occupies"
    ch3.y_axis.title, ch3.x_axis.title = "share of all four columns", "accuracy bound (%)"
    ch3.height, ch3.width = 9, 19
    ch3.add_data(Reference(ws, min_col=7, min_row=head - 1, max_row=last), titles_from_data=True)
    ch3.set_categories(Reference(ws, min_col=1, min_row=head, max_row=last))
    ws.add_chart(ch3, "J41")

    r = last + 2
    r = chart_notes(ws, r,
        "Three views of the same nine measurements: the ratio achieved, the bits each value ends up "
        "costing, and how much of the store this column still accounts for.",
        "LOSSLESS IS THE STORY: 61.185 bits per value, a ratio of 1.046. Storing these floats without "
        "loss saves 4.6% and nothing more -- 97.9% of them are distinct at six decimal places, so there "
        "is almost no redundancy to find. The codec recorded for that row is literally 'Raw'.")
    r = f_note(ws, r, "THE FIRST BOUND IS WORTH MORE THAN ALL THE OTHERS COMBINED. Going from lossless to "
                      "0.01% takes 61.2 bits to 16.2 and removes 240 MB -- a 3.95x ratio from a bound so "
                      "tight it permits an absolute error of 0.0001. Every later widening, all the way out "
                      "to 20%, only takes it from 3.95x to 11.24x.")
    r = f_note(ws, r, "THE STEPS ARE LUMPY, not a smooth curve: -73.6%, -17.2%, -17.7%, -8.4%, -23.4%, "
                      "+2.9%, -20.2%, -10.4%. A smooth rate-distortion curve would not look like this, "
                      "because the quantiser picks INTEGER bit widths and the bound only bites when it "
                      "crosses a width boundary.")
    r = f_note(ws, r, "CAUTION -- 'bits per value' here is measured from the STORED bytes, so it mixes two "
                      "stages: the quantiser's bit width AND a zstd pass over the packed codes. They can "
                      "move in opposite directions. At 2% the quantiser uses 9 bits and zstd recovers "
                      "17.3%; at 5% it uses 7 bits and zstd recovers 1.7%. That is the whole of Anomaly 2, "
                      "and it means this column cannot be read as a measure of the quantiser alone.")
    r = f_note(ws, r, "THE COLUMN STOPS BEING THE PROBLEM. At lossless it is 51.3% of the store; by 20% it "
                      "is 8.9%. Past about 1% the bound is no longer where the bytes are -- producerId and "
                      "ts are, and neither can be quantised. That is the ceiling on what accuracy can buy "
                      "for this dataset.")
    r += 1

    r = f_section(ws, r, "ALL FOUR COLUMNS, LOSSLESS — why the other three cannot be helped", 5)
    head2 = f_head(ws, r, ["column", "unencoded width", "stored bytes", "bits per value", "x vs raw"])
    for i, (name, width) in enumerate(FLOAT_COLS):
        rr = head2 + i
        ws.cell(row=rr, column=1, value=name).font = Font(name=ARIAL)
        ws.cell(row=rr, column=2, value=f"{width} B").font = Font(name=ARIAL)
        for col, val, fmt in (
                (3, f"={fc_sumifs(F, name, 0.0)}", "#,##0"),
                (4, f"=C{rr}*8/{ROWS_IN}", "0.00"),
                (5, f"={ROWS_IN}*{width}/C{rr}", "0.000")):
            c = ws.cell(row=rr, column=col, value=val)
            c.font, c.number_format = Font(name=ARIAL, color=GREEN), fmt
    r = head2 + len(FLOAT_COLS) + 1
    r = f_note(ws, r, "producerId gets 1.18x: it is a shuffled permutation of 0..42,699,999, so the only "
                      "thing available is bitpacking 32 bits down to the 27 the range needs. groupId gets "
                      "10.5x because it holds four distinct values in 3 bits. ts gets 2.28x -- 28 bits, "
                      "which is roughly the span of one group, so it is bitpacking against the group range "
                      "rather than exploiting a delta pattern, because solar has none.")
    r = f_note(ws, r, "None of those three is a FLOAT, so no accuracy bound can touch them. They are the "
                      "control that proves the bound only moved what it should, and they are also the hard "
                      "floor: 310 MB of the store is columns that quantisation cannot reach.")
    return ws


def b_sumifs(B, col, bound):
    return (f"SUMIFS('Lossy Breakdown'!${B[col]}:${B[col]},"
            f"'Lossy Breakdown'!${B['bound_pct']}:${B['bound_pct']},{bound})")


def sheet_stages(wb, B):
    """LossyStore's float pipeline, one stage at a time."""
    ws = wb.create_sheet("A6 Lossy Stage Breakdown")
    ws.column_dimensions["A"].width = 26
    for c in "BCDEFGH":
        ws.column_dimensions[c].width = 15
    r = f_title(ws, 1, "LossyStore, stage by stage — where the bytes actually go")
    r = f_note(ws, r, "LossyStore.cpp:309-315 encodes a quantisable column in three steps: "
                      "quantizeColumn turns doubles into integer grid codes and picks a bitWidth; "
                      "bitPack writes bitWidth bits per code behind an 18 B header; then zstd runs over "
                      "the result and is KEPT ONLY IF SMALLER.")
    r = f_note(ws, r, "QUANTISATION ON ITS OWN SAVES NOTHING. It produces uint64 codes -- still eight "
                      "bytes each, in memory. What it earns is a small bitWidth, and that only becomes "
                      "bytes when bitPack runs. The two are therefore reported as ONE multiplier; "
                      "splitting them would invent a saving that does not exist.")
    r += 1

    r = f_section(ws, r, "BITS PER VALUE AT EACH STAGE, AND WHAT EACH STAGE MULTIPLIED", 8)
    head = f_head(ws, r, ["bound %", "raw", "after quantise+bitpack", "after zstd",
                          "x from quantise+bitpack", "x from zstd", "final x", "zstd kept"])
    first = head
    for i, b in enumerate(BOUNDS):
        rr = head + i
        ws.cell(row=rr, column=1, value=b).font = Font(name=ARIAL)
        ws.cell(row=rr, column=2, value=64).font = Font(name=ARIAL)
        for col, key, fmt in (
                (3, "bitpacked_bits_per_value", "0.0000"),
                (4, "zstd_bits_per_value", "0.0000"),
                (5, "x_from_quantise_bitpack", "0.0000"),
                (6, "x_from_zstd", "0.0000"),
                (7, "final_ratio", "0.0000"),
                (8, "zstd_kept_share", "0.0%")):
            c = ws.cell(row=rr, column=col, value=f"={b_sumifs(B, key, b)}")
            c.font, c.number_format = Font(name=ARIAL, color=GREEN), fmt
        if b in (2.0, 5.0):
            for col in range(1, 9):
                ws.cell(row=rr, column=col).fill = WARN
    last = head + len(BOUNDS) - 1

    def by_bound(title, y, cols, anchor, kind="line", gap=None):
        ch = LineChart() if kind == "line" else BarChart()
        if kind == "bar":
            ch.type, ch.gapWidth = "col", gap or 40
        ch.title, ch.y_axis.title, ch.x_axis.title = title, y, "accuracy bound (%)"
        ch.height, ch.width = 9, 19
        for col in cols:
            ch.add_data(Reference(ws, min_col=col, min_row=head - 1, max_row=last),
                        titles_from_data=True)
        ch.set_categories(Reference(ws, min_col=1, min_row=head, max_row=last))
        ws.add_chart(ch, anchor)

    by_bound("Bits per value after each stage", "bits per value", [3, 4], "J3")
    by_bound("What each stage multiplied the ratio by", "x", [5, 6], "J22")
    by_bound("Share of groups where zstd was kept", "share of groups", [8], "J41", kind="bar")

    r = last + 2
    r = chart_notes(ws, r,
        "The same nine bounds seen three ways: the bits a value costs after packing and after zstd, "
        "the multiplier each stage contributed, and how often zstd was worth keeping at all.",
        "THE LOSSY STAGE HAS NO ANOMALY IN IT. 'x from quantise+bitpack' climbs 1.00, 3.99, 5.10, 6.31, "
        "7.00, 7.85, 9.06, 10.54, 12.58 -- monotonic, no reversal anywhere. Every irregularity in the "
        "final ratio comes from the other line.")
    r = f_note(ws, r, "ZSTD IS THE ERRATIC STAGE: x1.063, 1.056, 1.011, 1.011, 1.004, 1.209, 1.014, 1.134, "
                      "1.085. It swings between contributing nothing and contributing 21%, with no trend "
                      "in the bound at all.")
    r = f_note(ws, r, "ANOMALY 2, IN ONE LINE (highlighted rows). At 2% the packed form is 8.1520 bits and "
                      "zstd takes it to 6.7455 -- x1.2085, kept in 91% of groups. At 5% the packed form is "
                      "BETTER at 7.0644 bits, but zstd only reaches 6.9686 -- x1.0137, kept in 39%. The "
                      "quantiser did its job; zstd handed the advantage back, and 5% ends up larger.")
    r = f_note(ws, r, "WHY: at 9 bits the packing is loose relative to the codes' entropy, so zstd finds "
                      "slack. At 7 bits it is already near-entropy and there is nothing left. Tighter "
                      "bitpacking starves the general-purpose compressor. The 'zstd kept' bars track this "
                      "exactly -- 91% and 100% where zstd pays, 13% to 45% where it does not.")
    r = f_note(ws, r, "THE FIX IS VISIBLE FROM HERE: LossyStore.cpp:314 keeps whichever of packed or "
                      "zstd(packed) is smaller, but the BIT WIDTH was already fixed before that "
                      "comparison. A width that packs tightly and compresses badly is never compared "
                      "against a looser one that compresses well.")
    r += 1

    r = f_section(ws, r, "REPRODUCED AT TWO SCALES", 4)
    r = f_note(ws, r, "The same measurement on 1,000,000 rows and on 42,700,000 -- 42.7x the data.")
    h2 = f_head(ws, r, ["bound %", "quantity", "1,000,000 rows", "42,700,000 rows"])
    for i, (b, q, a, c) in enumerate([
            (2.0, "bits after pack", 8.1507, 8.1520), (2.0, "x from zstd", 1.2086, 1.2085),
            (2.0, "final x", 9.4899, 9.4878), (5.0, "bits after pack", 7.0704, 7.0644),
            (5.0, "x from zstd", 1.0171, 1.0137), (5.0, "final x", 9.2063, 9.1840)]):
        for col, v, fmt in ((1, b, "0.0"), (2, q, None), (3, a, "0.0000"), (4, c, "0.0000")):
            cc = ws.cell(row=h2 + i, column=col, value=v)
            cc.font = Font(name=ARIAL)
            if fmt:
                cc.number_format = fmt
    r = h2 + 6 + 1
    r = f_note(ws, r, "Agreement to four decimal places at 42.7x the input. This is not sampling noise, "
                      "and the 1M input is therefore enough to chase the cause on.")
    return ws


ANOMALIES = [
    dict(n=1, title="Bounded ratios get WORSE as the unit grows",
         status="CONFIRMED in the source — mechanism identified",
         what="All three bounded LossyStore configurations fall monotonically with unit size. At 5%: "
              "3.312 -> 3.230 -> 3.161 -> 3.140 across 256 -> 16384 KB. Same shape at 0.1% and 1%.",
         why_odd="The usual expectation is the opposite. A bigger block normally gives a compressor more "
                 "context and a better ratio, and that is what CompressedStore does here (flat, very "
                 "slightly up). Wind showed a non-monotonic zigzag, not a clean decline.",
         hypothesis="CONFIRMED by reading Quantize.cpp:108-141. The quantiser is LOGARITHMIC: "
                    "deltaLog = 2*log1p(bound/100) depends on the bound and nothing else, and the code for "
                    "a value is (log|v| - log(minMagnitude)) / deltaLog, so maxCode is set by the GROUP's "
                    "own magnitude span. bitWidth = bitWidthFor(maxCode). A smaller group spans a narrower "
                    "slice of the column's 2.5e-05 .. 999.999996 range, so maxCode is smaller and fewer "
                    "bits are needed. The source comment says the step depends on the bound 'and on "
                    "nothing else', which leaves the group span as the only other variable.",
         settle="Settled. log2(logSpan/deltaLog) predicts the measured bits per value to within 0.48 bits "
                "at every bound except 2%, and 2% is explained by Anomaly 2.",
         matters="If it holds, the advice for this kind of data is the opposite of the usual: pick the "
                 "SMALLEST unit that framing allows. At 1.2 GB framing is under 0.3% even at 256 KB, so "
                 "there is nothing pushing back."),
    dict(n=2, title="The 5% bound produces MORE bytes than the 2% bound",
         status="EXPLAINED — the zstd second pass, not the quantiser",
         what="producedPower alone: 2% gives 41,348,630 B, 5% gives 42,539,407 B. The looser bound costs "
              "2.88% MORE. Every other step in the nine-bound sweep decreases.",
         why_odd="A wider error allowance can always fall back on the tighter encoding, so it should never "
                 "be worse. This is the one result here that looks like it could be a defect rather than a "
                 "property of the data.",
         hypothesis="RESOLVED by reading the stored group headers at both bounds (1M-row input, 428 "
                    "groups each). The QUANTISER behaves correctly -- at 5% it picks a 7-bit width against "
                    "2%'s 9-bit, and its bitpacked output is SMALLER: 7.070 bits/value against 8.151. The "
                    "inversion is entirely the zstd pass applied afterwards: it is kept in 392 of 428 "
                    "groups at 2%, recovering 17.26%, but in only 187 of 428 at 5%, recovering 1.68%. That "
                    "15.6-point gap more than cancels the bit the quantiser saved. At 9 bits the packing is "
                    "loose relative to the codes' entropy and zstd mops up the slack; at 7 bits it is "
                    "already near-entropy and there is nothing left to find.",
         settle="Settled by direct inspection of valuesLength against uncompressedLength per group, plus "
                "the bitWidth byte in each column payload. Not a defect: two stages optimising "
                "independently, the second giving back what the first gained.",
         matters="A user asking for 5% still gets a bigger file than one asking for 2%, so 'wider bound, "
                 "smaller file' is not a guarantee and should not be stated as one. But the cause is now "
                 "known and actionable: the encoder could try both the packed and the zstd-of-packed form "
                 "and keep the smaller, which it already does per group -- the gap is that the BIT WIDTH is "
                 "chosen before zstd is considered, so a width that packs tightly but compresses badly is "
                 "never compared against a looser one that compresses well."),
    dict(n=3, title="Three chained rows beat their standalone equivalent",
         status="Small, explicable, but N=1",
         what="At 4096 KB: LossyStore lossless 1.851 vs 1.851 (+0.02%), 0.1% 2.879 vs 2.876 (+0.10%), "
              "1% 3.020 vs 3.016 (+0.12%). Three of twenty rows.",
         why_odd="A chain holds some data uncompressed, so it should never beat the pure codec. The excess "
                 "is tiny -- at most 0.12% -- but it is the wrong sign.",
         hypothesis="Group BOUNDARIES differ between the two paths. Standalone LossyStore accumulates 4 KB "
                    "input buffers until it reaches groupSize; chained, FileStore hands it whole sealed "
                    "segments. The groups therefore contain different row sets, and on data this irregular "
                    "that can go either way by a fraction of a percent.",
         settle="Re-run those three points at N=3 and see whether the sign is stable. If it is, compare "
                "debugGroupRanges between the two paths at 4096 KB.",
         matters="Barely, at 0.12%. It matters for how the number is DESCRIBED: 'chaining is free' is "
                 "supportable, 'chaining is free or slightly better' is not, at N=1."),
    dict(n=4, title="FileStore decode at 16384 KB is 113.6 s against a median of 93",
         status="Single sample, unexplained, probably worth one re-run",
         what="Every other decode in the 24-row standalone sweep is 84.8-95.2 s. This one cell is 113.6 s, "
              "13.4 median-absolute-deviations out. FileStore does no decoding at all -- it stores rows "
              "row-major and uncompressed -- so there is nothing for a unit size to make slower.",
         why_odd="It is the only configuration where unit size should not touch decode at all, and it is "
                 "the only large outlier.",
         hypothesis="Most likely ordinary interference -- it was the last configuration of the last grid "
                    "point, so page cache and disk state were at their worst. Alternatively the 16 MB "
                    "segment interacts with the read path's buffering. Nothing in the data distinguishes "
                    "these.",
         settle="Re-run that single point. Two minutes of work and it either vanishes or it does not.",
         matters="Only if decode timings are quoted per configuration. It does not touch any byte count."),
]


def sheet_anomalies(wb):
    ws = wb.create_sheet("Anomalies")
    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 120
    r = f_title(ws, 1, "Four things that are not what a reader would predict")
    r = f_note(ws, r, "Read this before quoting any single number. Anomaly 2 is unexplained and could be a "
                      "defect; the others are understood or benign.")
    r += 1
    for a in ANOMALIES:
        r = f_section(ws, r, f"ANOMALY {a['n']}   —   {a['title']}", 2)
        st = ws.cell(row=r, column=1, value="STATUS")
        st.font = Font(name=ARIAL, bold=True, size=9)
        c = ws.cell(row=r, column=2, value=a["status"])
        c.font = Font(name=ARIAL, bold=True)
        c.fill = WARN
        r += 1
        for label, key in (("WHAT", "what"), ("WHY IT IS ODD", "why_odd"),
                           ("BEST GUESS", "hypothesis"), ("HOW TO SETTLE IT", "settle"),
                           ("WHY IT MATTERS", "matters")):
            ws.cell(row=r, column=1, value=label).font = Font(name=ARIAL, bold=True, size=9)
            r = f_note(ws, r, a[key], col=2)
        r += 1
    r = f_section(ws, r, "THINGS THAT ARE NOT ANOMALIES, CHECKED", 2)
    for k, v in [
        ("No eviction", "FileStore's data_out is exactly 1,195,600,000 B at all four unit sizes -- byte for "
                        "byte the input. The capacity was raised to 4096 MB over 4 files for the standalone "
                        "run precisely so this would hold."),
        ("Bound touches one column", "producerId, groupId and ts each have exactly ONE byte count across all "
                                     "nine bounds, all on ForBitpack. The accuracy bound moved the float and "
                                     "nothing else."),
        ("Unit sweep really swept", "Unit counts step cleanly 4,561 / 1,141 / 286 / 72 -- a clean 4x each "
                                    "time. The headers were genuinely patched at each point."),
        ("LossyStore lossless dips", "1.862 / 1.869 / 1.851 / 1.835 -- peaks at 1024 KB rather than falling "
                                     "monotonically like the bounded lines. The swing is 1.8%, far smaller "
                                     "than wind's 40-49%, and it is the lossless path so no quantiser is "
                                     "involved. Noted, not pursued."),
    ]:
        ws.cell(row=r, column=1, value=k).font = Font(name=ARIAL, bold=True)
        r = f_note(ws, r, v, col=2)
    return ws


def recalculate() -> bool:
    tmp = RESULTS / "_recalc"
    tmp.mkdir(exist_ok=True)
    subprocess.run(["soffice", "--headless", "--convert-to", "xlsx", "--outdir", str(tmp), str(BOOK)],
                   capture_output=True, text=True, timeout=600, check=True)
    produced = tmp / BOOK.name
    if not produced.is_file():
        return False
    produced.replace(BOOK)
    tmp.rmdir()
    return True


def main() -> int:
    wb = Workbook()
    wb.remove(wb.active)
    S, ns = data_sheet(wb, "Standalone", "solar_standalone.csv")
    C, nc = data_sheet(wb, "Chained", "solar_chained.csv")
    F, nf = data_sheet(wb, "Float Column", "solar_float_column.csv")
    B, nb = data_sheet(wb, "Lossy Breakdown", "solar_lossy_breakdown.csv")
    sheet_readme(wb)
    sheet_dataset(wb)
    sheet_ratios(wb, S, C)
    sheet_chaining(wb, S, C)
    sheet_accuracy(wb, S, C)
    sheet_overhead(wb, S)
    sheet_float_detail(wb, F)
    sheet_stages(wb, B)
    sheet_anomalies(wb)
    wb.move_sheet("Dataset", offset=-len(wb.sheetnames) + 2)
    wb.save(BOOK)
    print(f"  wrote {BOOK.name}: {len(wb.sheetnames)} sheets, "
          f"{ns}+{nc}+{nf}+{nb} measured rows")
    if not recalculate():
        print("  recalculation produced no file", file=sys.stderr)
        return 1
    print(f"  recalculated ({BOOK.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
