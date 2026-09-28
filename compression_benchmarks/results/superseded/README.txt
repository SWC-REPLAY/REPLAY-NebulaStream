Superseded by the f2 set, kept only for traceability.

  final_standalone_wind.csv   28 rows, unit sizes 16/64/256/1024
  final_chained_wind.csv      20 rows, unit sizes 16/64/256/1024

Both were produced by grid.py, which swept all three unit sizes together -- the correct shape,
as it turned out. The f2 files repeat those points and add 4096 KB, so these carry no data the
f2 set lacks. They differ only in timing noise.

Not deleted because they are what the earlier analysis was written against.

Also here:
  all_benchmarks.csv            built by collect_results.py from the markdown reports
  float_column.csv              same, per-column
  final_benchmark_analysis.xlsx the first workbook, built from the superseded CSVs
  unit_size_grid_*.md           the markdown reports themselves

collect_results.py parsed those .md files into CSV. It is now obsolete: grid.py and grid_f2.py
write CSV directly, so nothing produces the markdown any more.
