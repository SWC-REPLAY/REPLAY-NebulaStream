#!/usr/bin/env python3
"""Add the analysis sheets -- grouped subtables and charts -- to the WIND workbook.

    ./compression_benchmarks/scripts/workbook_wind.py         # data sheets first
    ./compression_benchmarks/scripts/workbook_wind_charts.py  # then this

Three sheets, each answering one question the flat grids cannot:

    Ratios by Size      what the unit size buys, standalone against chained, and how much of the
                        chained figure is the tail codec rather than the tier arrangement
    Accuracy Tradeoff   what each accuracy bound costs, across every unit size
    Overhead and Timing where the bytes and the seconds go that are not the codec's doing

Every subtable cell is a SUMIFS against the Standalone or Chained sheet, not a number this script
computed. Re-running the benchmark and rebuilding the data sheets moves the analysis with it, and
a reader can see which row a figure came from. SUMIFS because each (configuration, unit size) pair
is unique, so the sum over a match is the value itself.

openpyxl is used here and not in workbook_wind.py because the data sheets predate it being
available. workbook_solar.py does the whole job in one openpyxl script and is the better
shape; merging these two into that form is a worthwhile follow-up, not done yet.
"""

import subprocess
import sys
from pathlib import Path

from openpyxl import load_workbook
from openpyxl.chart import BarChart, LineChart, Reference
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

REPO = Path(__file__).resolve().parents[2]
RESULTS = REPO / "compression_benchmarks/results"
BOOK = RESULTS / "wind_analysis.xlsx"

UNITS = [16, 64, 256, 1024, 4096]

# Column letters on the two data sheets. Declared once: a shifted column silently rewires every
# SUMIFS below, and the failure looks like a measurement change rather than a broken reference.
S = {"order": "A", "bound": "B", "unit": "C", "encode": "L", "decode": "M",
     "data": "N", "disk": "O", "framing_ratio": "Q", "ratio_data": "R", "units": "T"}
C = {"order": "A", "bound": "B", "unit": "C", "encode": "L", "decode": "M",
     "data": "N", "disk": "O", "framing_ratio": "Q", "ratio_data": "R",
     "ratio_tail": "U", "units": "V", "fs_data": "Z"}

STANDALONE = [
    ("MemoryStore", "MemoryStore", None),
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

ARIAL = "Arial"
HDR_FILL = PatternFill("solid", fgColor="2F5496")
SUB_FILL = PatternFill("solid", fgColor="D9E2F3")
GREEN = "1F6F3D"


def title(ws, r, text):
    c = ws.cell(row=r, column=1, value=text)
    c.font = Font(name=ARIAL, bold=True, size=14)


def note(ws, r, text):
    c = ws.cell(row=r, column=1, value=text)
    c.font = Font(name=ARIAL, italic=True, color="555555")


def section(ws, r, text):
    c = ws.cell(row=r, column=1, value=text)
    c.font = Font(name=ARIAL, bold=True)
    c.fill = SUB_FILL
    for col in range(2, 7):
        ws.cell(row=r, column=col).fill = SUB_FILL


def head_row(ws, r, labels):
    for i, label in enumerate(labels, start=1):
        c = ws.cell(row=r, column=i, value=label)
        c.font = Font(name=ARIAL, bold=True, color="FFFFFF")
        c.fill = HDR_FILL
        c.alignment = Alignment(horizontal="center", wrap_text=True)


def sumifs(sheet, cols, value_col, order, bound, unit):
    """One measurement, addressed by configuration and unit size.

    The bound column is numeric and EMPTY for a lossless or non-lossy configuration, so a lossless
    row is selected with "" rather than with a number -- matching on the number 0 would find
    nothing, since the cell holds no value at all.
    """
    crit = '""' if bound is None else repr(bound)
    return (f'=SUMIFS({sheet}!${cols[value_col]}:${cols[value_col]},'
            f'{sheet}!${cols["order"]}:${cols["order"]},"{order}",'
            f'{sheet}!${cols["bound"]}:${cols["bound"]},{crit},'
            f'{sheet}!${cols["unit"]}:${cols["unit"]},{unit})')


def block(ws, top, heading, configs, sheet, cols, value_col, fmt="0.000", subtitle=None):
    """A configuration-by-unit-size subtable. Returns the row after it."""
    section(ws, top, heading)
    r = top + 1
    if subtitle:
        note(ws, r, subtitle)
        r += 1
    head_row(ws, r, ["Configuration"] + [f"{u} KB" for u in UNITS])
    first = r + 1
    for i, (label, order, bound) in enumerate(configs):
        rr = first + i
        lc = ws.cell(row=rr, column=1, value=label)
        lc.font = Font(name=ARIAL)
        for j, u in enumerate(UNITS, start=2):
            c = ws.cell(row=rr, column=j, value=sumifs(sheet, cols, value_col, order, bound, u))
            c.font = Font(name=ARIAL, color=GREEN)
            c.number_format = fmt
    return first, first + len(configs) - 1


def simple_chart(ws, kind, title_text, y_title, labels, first, last, anchor,
                 height=8, width=17, log=False):
    ch = LineChart() if kind == "line" else BarChart()
    if kind == "bar":
        ch.type = "col"
        ch.gapWidth = 60
    ch.title = title_text
    ch.y_axis.title = y_title
    ch.x_axis.title = "unit size (KB)"
    ch.height, ch.width = height, width
    if log:
        ch.y_axis.scaling.logBase = 10
    for i in range(len(labels)):
        rr = first + i
        data = Reference(ws, min_col=1, max_col=1 + len(UNITS), min_row=rr, max_row=rr)
        ch.add_data(data, titles_from_data=True, from_rows=True)
    ch.set_categories(Reference(ws, min_col=2, max_col=1 + len(UNITS),
                                min_row=first - 1, max_row=first - 1))
    ws.add_chart(ch, anchor)


def sheet_ratios(wb):
    ws = wb.create_sheet("Ratios by Size")
    ws.column_dimensions["A"].width = 26
    for col in "BCDEF":
        ws.column_dimensions[col].width = 11

    title(ws, 1, "Compression ratio against unit size")
    note(ws, 2, "Every cell is a SUMIFS against the Standalone or Chained sheet. Green means "
                "computed by the sheet, so correcting a measurement moves it.")

    f1, l1 = block(ws, 4, "STANDALONE  -  bytes_in / data_out", STANDALONE,
                   "Standalone", S, "ratio_data",
                   subtitle="One store, no tiering. Nothing is held back uncompressed.")
    simple_chart(ws, "line", "Standalone: compression ratio by unit size", "ratio",
                 STANDALONE, f1, l1, "H4")

    f2, l2 = block(ws, 15, "CHAINED  -  ratio_on_data  (the whole chain)", CHAINED,
                   "Chained", C, "ratio_data",
                   subtitle="Counts the sub-threshold remainder FileStore keeps uncompressed, "
                            "so this is what the chain costs on disk.")
    simple_chart(ws, "line", "Chained: ratio over the whole chain", "ratio",
                 CHAINED, f2, l2, "H15")

    f3, l3 = block(ws, 24, "CHAINED  -  ratio_on_tail  (the codec alone)", CHAINED,
                   "Chained", C, "ratio_tail",
                   subtitle="Excludes that remainder: what the tail achieved on the bytes it was "
                            "actually handed. Compare codecs with this row set.")
    simple_chart(ws, "line", "Chained: ratio achieved by the tail codec", "ratio",
                 CHAINED, f3, l3, "H24")

    # The gap between the two chained blocks, stated rather than left to the reader to subtract.
    top = 33
    section(ws, top, "COST OF TIERING  -  ratio_on_tail / ratio_on_data")
    note(ws, top + 1, "How much of the codec's work the uncompressed remainder gives back. "
                      "1.00 would mean the chain kept everything the codec achieved.")
    head_row(ws, top + 2, ["Configuration"] + [f"{u} KB" for u in UNITS])
    for i in range(len(CHAINED)):
        rr = top + 3 + i
        ws.cell(row=rr, column=1, value=CHAINED[i][0]).font = Font(name=ARIAL)
        for j in range(len(UNITS)):
            col = get_column_letter(2 + j)
            c = ws.cell(row=rr, column=2 + j,
                        value=f"=IFERROR({col}{f3 + i}/{col}{f2 + i},0)")
            c.font = Font(name=ARIAL, color=GREEN)
            c.number_format = "0.000"
    simple_chart(ws, "bar", "Fraction of the codec's ratio the chain keeps", "x",
                 CHAINED, top + 3, top + 2 + len(CHAINED), "H33")
    return ws


def sheet_accuracy(wb):
    ws = wb.create_sheet("Accuracy Tradeoff")
    ws.column_dimensions["A"].width = 26
    for col in "BCDEF":
        ws.column_dimensions[col].width = 11

    title(ws, 1, "What each accuracy bound buys")
    note(ws, 2, "LossyStore only. The bound is a per-value error ceiling; a wider bound lets the "
                "quantiser spend fewer bits.")

    lossy_s = [c for c in STANDALONE if c[1] == "LossyStore"]
    lossy_c = [c for c in CHAINED if "LossyStore" in c[1]]

    f1, l1 = block(ws, 4, "STANDALONE  -  ratio by bound", lossy_s,
                   "Standalone", S, "ratio_data")
    simple_chart(ws, "line", "Standalone LossyStore: ratio by bound", "ratio",
                 lossy_s, f1, l1, "H4")

    f2, l2 = block(ws, 12, "CHAINED  -  ratio_on_tail by bound", lossy_c,
                   "Chained", C, "ratio_tail")
    simple_chart(ws, "line", "Chained LossyStore: ratio achieved by the codec", "ratio",
                 lossy_c, f2, l2, "H12")

    # Against the lossless row of the same sheet, which is what "what did the bound buy" means.
    top = 20
    section(ws, top, "GAIN OVER LOSSLESS  -  bounded ratio / lossless ratio, standalone")
    note(ws, top + 1, "1.00 would mean the bound bought nothing at that unit size.")
    head_row(ws, top + 2, ["Configuration"] + [f"{u} KB" for u in UNITS])
    for i in range(1, len(lossy_s)):          # skip the lossless row itself
        rr = top + 2 + i
        ws.cell(row=rr, column=1, value=lossy_s[i][0]).font = Font(name=ARIAL)
        for j in range(len(UNITS)):
            col = get_column_letter(2 + j)
            c = ws.cell(row=rr, column=2 + j, value=f"=IFERROR({col}{f1 + i}/{col}{f1},0)")
            c.font = Font(name=ARIAL, color=GREEN)
            c.number_format = "0.000"
    simple_chart(ws, "bar", "Gain over lossless", "x",
                 lossy_s[1:], top + 3, top + 2 + len(lossy_s) - 1, "H20")
    return ws


def sheet_overhead(wb):
    ws = wb.create_sheet("Overhead and Timing")
    ws.column_dimensions["A"].width = 26
    for col in "BCDEF":
        ws.column_dimensions[col].width = 11

    title(ws, 1, "What is not the codec: framing, unit count, and time")
    note(ws, 2, "Framing is the file header, the preallocated table and the per-unit descriptors. "
                "It is charged to disk_bytes and excluded from data_out.")

    f1, l1 = block(ws, 4, "FRAMING as a share of the file  -  standalone", STANDALONE,
                   "Standalone", S, "framing_ratio", fmt="0.00%",
                   subtitle="Small units mean many descriptors. This is why a 16 KB unit can cost "
                            "more on disk than a larger one that compresses slightly worse.")
    simple_chart(ws, "line", "Framing share of the file", "share of disk_bytes",
                 STANDALONE, f1, l1, "H4")

    f2, l2 = block(ws, 15, "UNITS WRITTEN  -  standalone", STANDALONE,
                   "Standalone", S, "units", fmt="#,##0",
                   subtitle="The granule count the unit size produces. Framing tracks this.")
    simple_chart(ws, "line", "Units written", "count", STANDALONE, f2, l2, "H15", log=True)

    f3, l3 = block(ws, 26, "ENCODE seconds  -  standalone", STANDALONE,
                   "Standalone", S, "encode", fmt="0.0000")
    simple_chart(ws, "line", "Encode time", "seconds", STANDALONE, f3, l3, "H26")

    f4, l4 = block(ws, 37, "DECODE seconds  -  standalone", STANDALONE,
                   "Standalone", S, "decode", fmt="0.0000",
                   subtitle="CONFOUNDED: measured against a fixed 4 KB read destination, so a "
                            "larger unit is re-decompressed more times, not decoded more slowly. "
                            "Read this as the cost of the unit size AT THAT BUFFER SIZE.")
    simple_chart(ws, "line", "Decode time (see the caveat above)", "seconds",
                 STANDALONE, f4, l4, "H37")
    return ws


def recalculate() -> bool:
    """LibreOffice computes every formula; openpyxl writes none of their values."""
    tmp = RESULTS / "_recalc"
    tmp.mkdir(exist_ok=True)
    subprocess.run(["soffice", "--headless", "--convert-to", "xlsx", "--outdir", str(tmp),
                    str(BOOK)], capture_output=True, text=True, timeout=300, check=True)
    produced = tmp / BOOK.name
    if not produced.is_file():
        return False
    produced.replace(BOOK)
    tmp.rmdir()
    return True


def main() -> int:
    if not BOOK.is_file():
        print(f"{BOOK.name} not found -- run workbook_wind.py first", file=sys.stderr)
        return 1

    wb = load_workbook(BOOK)
    for name in ("Ratios by Size", "Accuracy Tradeoff", "Overhead and Timing"):
        if name in wb.sheetnames:
            del wb[name]
    sheet_ratios(wb)
    sheet_accuracy(wb)
    sheet_overhead(wb)
    wb.save(BOOK)
    print(f"  added 3 analysis sheets to {BOOK.name}")

    if not recalculate():
        print("  recalculation produced no file", file=sys.stderr)
        return 1
    print(f"  recalculated  ({BOOK.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
