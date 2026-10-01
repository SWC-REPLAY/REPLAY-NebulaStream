#!/usr/bin/env python3
"""Assemble the WIND results into one spreadsheet, via flat ODS and LibreOffice.

    ./compression_benchmarks/scripts/workbook_wind.py

Reads wind_standalone.csv and wind_chained.csv, and the f2 copies of the dataset
description and the float-column analysis, which were not re-measured: the f3 standalone run came
back byte-identical to f2 in all 35 rows, and both of those are standalone-only.

openpyxl is not installed and pip is unavailable here, so the workbook is written as Flat ODS
(.fods) -- a single plain-XML file LibreOffice reads natively -- and converted to .xlsx with
soffice, which is present. The result is a normal xlsx; nothing about the route shows in it.

Derived quantities are written as FORMULAS, not as the numbers Python already knows: framing is
disk minus data, the ratios are bytes_in over each, and bytes_in is itself rows x row width. So
correcting a measurement makes the sheet follow, and a reader can see how every figure was
reached instead of taking it on trust. Only the measurements themselves are literals.
"""

import csv
import subprocess
import sys
from pathlib import Path
from xml.sax.saxutils import escape

REPO = Path(__file__).resolve().parents[2]
RESULTS = REPO / "compression_benchmarks/results"
OUT_FODS = RESULTS / "wind_analysis.fods"
OUT_XLSX = RESULTS / "wind_analysis.xlsx"

NS = (
    'xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" '
    'xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0" '
    'xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0" '
    'xmlns:style="urn:oasis:names:tc:opendocument:xmlns:style:1.0" '
    'xmlns:fo="urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0" '
    'xmlns:number="urn:oasis:names:tc:opendocument:xmlns:datastyle:1.0" '
    'xmlns:of="urn:oasis:names:tc:opendocument:xmlns:of:1.2" '
    'office:version="1.2"'
)

STYLES = """
 <office:font-face-decls>
  <style:font-face style:name="Arial" svg:font-family="Arial"
    xmlns:svg="urn:oasis:names:tc:opendocument:xmlns:svg-compatible:1.0"/>
 </office:font-face-decls>
 <office:automatic-styles>
  <number:number-style style:name="NInt">
   <number:number number:decimal-places="0" number:min-integer-digits="1" number:grouping="true"/>
  </number:number-style>
  <number:number-style style:name="NSec">
   <number:number number:decimal-places="4" number:min-integer-digits="1"/>
  </number:number-style>
  <number:number-style style:name="NRatio">
   <number:number number:decimal-places="3" number:min-integer-digits="1"/>
  </number:number-style>
  <number:percentage-style style:name="NPct">
   <number:number number:decimal-places="2" number:min-integer-digits="1"/>
   <number:text>%</number:text>
  </number:percentage-style>
  <style:style style:name="title" style:family="table-cell">
   <style:text-properties style:font-name="Arial" fo:font-weight="bold" fo:font-size="14pt"/>
  </style:style>
  <style:style style:name="hdr" style:family="table-cell">
   <style:text-properties style:font-name="Arial" fo:font-weight="bold" fo:color="#ffffff"/>
   <style:table-cell-properties fo:background-color="#2f5496" fo:wrap-option="wrap"
     style:vertical-align="bottom"/>
  </style:style>
  <style:style style:name="sub" style:family="table-cell">
   <style:text-properties style:font-name="Arial" fo:font-weight="bold"/>
   <style:table-cell-properties fo:background-color="#d9e2f3"/>
  </style:style>
  <style:style style:name="txt" style:family="table-cell">
   <style:text-properties style:font-name="Arial"/>
  </style:style>
  <style:style style:name="note" style:family="table-cell">
   <style:text-properties style:font-name="Arial" fo:font-style="italic" fo:color="#555555"/>
  </style:style>
  <style:style style:name="int" style:family="table-cell" style:data-style-name="NInt">
   <style:text-properties style:font-name="Arial"/>
  </style:style>
  <style:style style:name="sec" style:family="table-cell" style:data-style-name="NSec">
   <style:text-properties style:font-name="Arial"/>
  </style:style>
  <style:style style:name="ratio" style:family="table-cell" style:data-style-name="NRatio">
   <style:text-properties style:font-name="Arial"/>
  </style:style>
  <style:style style:name="pct" style:family="table-cell" style:data-style-name="NPct">
   <style:text-properties style:font-name="Arial"/>
  </style:style>
  <style:style style:name="calc" style:family="table-cell" style:data-style-name="NInt">
   <style:text-properties style:font-name="Arial" fo:color="#1f6f3d"/>
  </style:style>
  <style:style style:name="calcr" style:family="table-cell" style:data-style-name="NRatio">
   <style:text-properties style:font-name="Arial" fo:color="#1f6f3d"/>
  </style:style>
  <style:style style:name="calcp" style:family="table-cell" style:data-style-name="NPct">
   <style:text-properties style:font-name="Arial" fo:color="#1f6f3d"/>
  </style:style>
  <style:style style:name="wide" style:family="table-column">
   <style:table-column-properties style:column-width="1.9in"/>
  </style:style>
  <style:style style:name="norm" style:family="table-column">
   <style:table-column-properties style:column-width="1.05in"/>
  </style:style>
 </office:automatic-styles>
"""


def cell(value=None, style="txt", formula=None, kind="float"):
    """One table cell. A formula carries a placeholder value; LibreOffice recomputes on load."""
    if formula is not None:
        # The formula goes into an XML ATTRIBUTE, so <, > and " in it must be entities -- an
        # unescaped `<=` or a quoted string literal makes the whole document unparseable, and
        # LibreOffice reports only "source file could not be loaded".
        expr = escape(formula, {'"': "&quot;"})
        # NO cached value. LibreOffice trusts a cached result over the formula when one is
        # present, so writing a placeholder made every formula read back as that placeholder --
        # 384 cells of zero that looked like evaluated output. With none, it must compute.
        return (f'<table:table-cell table:style-name="{style}" table:formula="of:={expr}"/>')
    if value is None or value == "":
        return f'<table:table-cell table:style-name="{style}"/>'
    if kind == "float":
        return (f'<table:table-cell table:style-name="{style}" office:value-type="float" '
                f'office:value="{value}"/>')
    return (f'<table:table-cell table:style-name="{style}" office:value-type="string">'
            f'<text:p>{escape(str(value))}</text:p></table:table-cell>')


def row(cells):
    return "<table:table-row>" + "".join(cells) + "</table:table-row>"


def numeric(text):
    try:
        float(text)
        return True
    except (TypeError, ValueError):
        return False


def col_letter(i):
    """0 -> A. The sheets stay well inside one letter plus AA..AZ."""
    s = ""
    i += 1
    while i:
        i, r = divmod(i - 1, 26)
        s = chr(65 + r) + s
    return s


def read(name):
    with (RESULTS / name).open() as h:
        return list(csv.DictReader(h))


def description_sheet():
    facts = read("wind_dataset_facts.csv")
    cols = read("wind_dataset_description.csv")
    out = ['<table:table table:name="Data Description">',
           '<table:table-column table:style-name="wide"/>',
           '<table:table-column table:style-name="wide" table:number-columns-repeated="11"/>']
    out.append(row([cell("Replay store compression benchmarks", "title", kind="s")]))
    out.append(row([cell("Every figure below was measured through the real SQL query path, "
                         "not by calling the stores directly.", "note", kind="s")]))
    out.append(row([]))

    out.append(row([cell("Dataset", "sub", kind="s"), cell("", "sub")]))
    for f in facts:
        v = f["value"]
        out.append(row([cell(f["item"], "txt", kind="s"),
                        cell(v, "int") if numeric(v) else cell(v, "txt", kind="s")]))
    out.append(row([]))

    out.append(row([cell("Columns", "sub", kind="s")] + [cell("", "sub") for _ in range(11)]))
    heads = list(cols[0])
    out.append(row([cell(h, "hdr", kind="s") for h in heads]))
    for c in cols:
        out.append(row([cell(c[h], "int") if numeric(c[h]) else cell(c[h], "txt", kind="s")
                        for h in heads]))
    out.append(row([]))

    out.append(row([cell("What the measured columns mean", "sub", kind="s")]
                   + [cell("", "sub") for _ in range(11)]))
    for a, b in [
        ("bytes_in", "rows x stored row width. Fixed by the schema, so it is the control, not a measurement."),
        ("data_out", "the encoded data alone, every byte of framing removed -- what the codec produced."),
        ("disk_bytes", "the file size. For a chain, every tier's files added together."),
        ("framing_bytes", "disk_bytes - data_out: file headers, preallocated tables, per-unit descriptors."),
        ("framing_ratio", "framing_bytes / disk_bytes."),
        ("ratio_on_data", "bytes_in / data_out. Compares codecs without format overhead in the way."),
        ("ratio_on_disk", "bytes_in / disk_bytes. What the data actually costs to keep."),
        ("tail_in_bytes", "bytes_in minus what MemoryStore and FileStore are still holding -- the bytes "
                          "that actually reached the compressing tier. Chained sheet only."),
        ("ratio_on_tail", "tail_in_bytes / the tail's data. What the codec achieved on what it was "
                          "GIVEN, with the uncompressed remainder in FileStore taken out. Compare "
                          "codecs with this; budget a deployment with ratio_on_data."),
        ("encode_s", "wall time of the write query, median of the repeats."),
        ("decode_s", "wall time of the read-back query, median of the repeats."),
        ("unit_kb", "the granule each tier seals on: FileStore segment, LossyStore group, CompressedStore segment."),
        ("MemoryStore", "holds rows in TupleBuffers and writes no file, so disk_bytes is 0 while data_out is not."),
        ("green figures", "formulas computed by the sheet; black figures are measurements."),
    ]:
        out.append(row([cell(a, "txt", kind="s"), cell(b, "note", kind="s")]))

    out.append(row([]))
    out.append(row([cell("Caveats that affect how these read", "sub", kind="s")]
                   + [cell("", "sub") for _ in range(11)]))
    for note in [
        "decode_s measures the unit size against a FIXED 4 KB read buffer, not the unit size alone. "
        "CompressedStore re-decompresses a whole segment per read() call, so a 1 MB segment is "
        "decompressed 257 times to fill 4 KB buffers. Given a larger destination it decodes in 0.47 s.",
        "LossyStore's size is not monotonic in unit size on this dataset. The timestamp column uses "
        "ForBitpack, its bit width tracks the group's span, and this data's period-3 delta pattern "
        "aligns to a byte boundary only at certain widths. It is an artifact of this data's "
        "regularity, so the 16 KB result should not be read as a recommendation.",
        "Wind turbine data is unusually compressible: 2 distinct timestamp deltas, 40% repeated "
        "floats. Treat its ratios as a best case.",
        "A CHAIN does not compress everything. FileStore migrates once it has accumulated its "
        "flush threshold, 4 MB here, and keeps the sub-threshold remainder -- 367,248 bytes, 6.6% "
        "of the stream -- uncompressed. ratio_on_data counts that remainder and is what the chain "
        "costs; ratio_on_tail excludes it and is what the codec did.",
        "This supersedes the f2 caveat that the 1024 and 4096 chained rows were the same "
        "configuration. That held while FileStore drained on command and never accumulated past "
        "MemoryStore's 1 MB, so a larger segment could not fill. It now accumulates to 4 MB, and "
        "the two points differ: 6 units against 3, and different bytes at both tails.",
        "LossyStore's groupSize is ignored in a chain: it seals one group per incoming segment, "
        "so its group is FileStore's segment. Sweeping all three sizes together is the only shape "
        "that moves every tier's unit at once.",
    ]:
        out.append(row([cell(note, "note", kind="s")]))
    out.append("</table:table>")
    return "".join(out)


def grid_sheet(name, csv_name, per_tier):
    rows = read(csv_name)
    base = ["store_order", "bound_pct", "unit_kb",
            "filestore_segment_kb", "lossystore_group_kb", "compressedstore_segment_kb",
            "filestore_flush_threshold_kb",
            "flush_threshold", "rows", "row_width_bytes", "bytes_in",
            "encode_s", "decode_s", "data_out", "disk_bytes", "framing_bytes", "framing_ratio",
            "ratio_on_data", "ratio_on_disk"]
    # Only a chain has a tier above the tail, so only a chain can hold data back from it. On the
    # standalone sheet these two would be bytes_in and a copy of ratio_on_data.
    if per_tier:
        base += ["tail_in_bytes", "ratio_on_tail"]
    base += ["units", "runs"]
    tiers = ["MemoryStore", "FileStore", "CompressedStore", "LossyStore"]
    heads = list(base)
    if per_tier:
        for t in tiers:
            heads += [f"{t}_data", f"{t}_disk"]

    idx = {h: i for i, h in enumerate(heads)}
    out = [f'<table:table table:name="{name}">',
           '<table:table-column table:style-name="wide"/>',
           f'<table:table-column table:style-name="norm" table:number-columns-repeated="{len(heads) - 1}"/>']
    out.append(row([cell(h, "hdr", kind="s") for h in heads]))

    for n, r in enumerate(rows, start=2):
        ROWS, WID = col_letter(idx["rows"]) + str(n), col_letter(idx["row_width_bytes"]) + str(n)
        BIN = col_letter(idx["bytes_in"]) + str(n)
        DAT = col_letter(idx["data_out"]) + str(n)
        DSK = col_letter(idx["disk_bytes"]) + str(n)
        FRM = col_letter(idx["framing_bytes"]) + str(n)
        cells = [
            cell(r["store_order"], "txt", kind="s"),
            cell(r["bound_pct"], "ratio") if r["bound_pct"] else cell("", "txt"),
            cell(r["unit_kb"], "int"),
            cell(r["filestore_segment_kb"], "int"), cell(r["lossystore_group_kb"], "int"),
            cell(r["compressedstore_segment_kb"], "int"),
            cell(r["filestore_flush_threshold_kb"], "int"),
            cell(r["flush_threshold"], "txt", kind="s"),
            cell(r["rows"], "int"), cell(r["row_width_bytes"], "int"),
            cell(formula=f"[.{ROWS}]*[.{WID}]", style="calc"),
            cell(r["encode_s"], "sec"), cell(r["decode_s"], "sec"),
            cell(r["data_out"], "int") if r["data_out"] else cell(0, "int"),
            cell(r["disk_bytes"], "int") if r["disk_bytes"] else cell(0, "int"),
            # Guarded: MemoryStore writes no file, so disk is 0 while data is not. Unguarded,
            # disk - data reports a large NEGATIVE framing for it rather than none.
            cell(formula=f"IF([.{DSK}]=0;0;[.{DSK}]-[.{DAT}])", style="calc"),
            cell(formula=f"IFERROR([.{FRM}]/[.{DSK}];0)", style="calcp"),
            cell(formula=f"IFERROR([.{BIN}]/[.{DAT}];0)", style="calcr"),
            cell(formula=f"IFERROR([.{BIN}]/[.{DSK}];0)", style="calcr"),
        ]
        if per_tier:
            MEM = col_letter(idx["MemoryStore_data"]) + str(n)
            FS = col_letter(idx["FileStore_data"]) + str(n)
            CS = col_letter(idx["CompressedStore_data"]) + str(n)
            LS = col_letter(idx["LossyStore_data"]) + str(n)
            TIN = col_letter(idx["tail_in_bytes"]) + str(n)
            # Bytes that actually reached the tail: everything written, less whatever the tiers
            # above it are still holding. FileStore keeps a sub-threshold remainder uncompressed,
            # so bytes_in overstates the tail's input and ratio_on_data charges the tail for rows
            # it never saw. Exactly one of the two tail columns is populated per row; the other is
            # empty and counts as zero.
            cells += [
                cell(formula=f"[.{BIN}]-[.{MEM}]-[.{FS}]", style="calc"),
                cell(formula=f"IFERROR([.{TIN}]/([.{CS}]+[.{LS}]);0)", style="calcr"),
            ]
        cells += [cell(r["units"], "int"), cell(r["runs"], "int")]
        if per_tier:
            for t in tiers:
                for suffix in ("data", "disk"):
                    v = r.get(f"{t}_{suffix}", "")
                    cells.append(cell(v, "int") if v else cell("", "txt"))
        out.append(row(cells))
    out.append("</table:table>")
    return "".join(out)


def float_sheet():
    rows = read("wind_float_column.csv")
    heads = ["column", "type", "is_quantisable", "bound_pct", "groups", "bytes",
             "bytes_per_row", "vs_lossless_bytes", "vs_lossless_pct", "codec",
             "worst_recorded_error", "bound_as_fraction", "error_within_bound"]
    out = ['<table:table table:name="Float Column">',
           '<table:table-column table:style-name="wide"/>',
           f'<table:table-column table:style-name="norm" table:number-columns-repeated="{len(heads) - 1}"/>']
    out.append(row([cell(h, "hdr", kind="s") for h in heads]))

    # Rows are bound-major, four columns per bound, so each column's lossless baseline is one of
    # the first four data rows. Referenced rather than recomputed, so the comparison is visible.
    ncols = 4
    for n, r in enumerate(rows, start=2):
        j = (n - 2) % ncols
        base_row = 2 + j
        B = "F" + str(n)
        cells = [
            cell(r["column"], "txt", kind="s"), cell(r["type"], "txt", kind="s"),
            cell(r["is_quantisable"], "txt", kind="s"),
            cell(r["bound_pct"], "ratio"), cell(r["groups"], "int"), cell(r["bytes"], "int"),
            cell(formula=f"[.{B}]/200001", style="calcr"),
            cell(formula=f"[.{B}]-[.F{base_row}]", style="calc"),
            cell(formula=f"IFERROR(([.{B}]-[.F{base_row}])/[.F{base_row}];0)", style="calcp"),
            cell(r["codec"], "txt", kind="s"),
            cell(r["worst_recorded_error"], "sec"),
            cell(formula=f"[.D{n}]/100", style="calcr"),
            cell(formula=f'IF([.K{n}]<=[.L{n}];"within";"EXCEEDED")', style="txt", kind="s"),
        ]
        out.append(row(cells))
    out.append("</table:table>")
    return "".join(out)


def main() -> int:
    body = (description_sheet()
            + grid_sheet("Standalone", "wind_standalone.csv", per_tier=False)
            + grid_sheet("Chained", "wind_chained.csv", per_tier=True)
            + float_sheet())
    doc = (f'<?xml version="1.0" encoding="UTF-8"?>\n'
           f'<office:document {NS} '
           f'office:mimetype="application/vnd.oasis.opendocument.spreadsheet">'
           f'{STYLES}<office:body><office:spreadsheet>{body}'
           f'</office:spreadsheet></office:body></office:document>')
    OUT_FODS.write_text(doc)
    print(f"  wrote {OUT_FODS.name}")

    subprocess.run(["soffice", "--headless", "--convert-to", "xlsx",
                    "--outdir", str(RESULTS), str(OUT_FODS)],
                   capture_output=True, text=True, check=True, timeout=180)
    if not OUT_XLSX.is_file():
        print("  conversion produced no xlsx", file=sys.stderr)
        return 1
    OUT_FODS.unlink()
    print(f"  wrote {OUT_XLSX.name}  ({OUT_XLSX.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
