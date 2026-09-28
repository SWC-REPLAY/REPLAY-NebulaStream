# Compression benchmarks

Macro benchmarks for the replay store tiers, driven through SQL by the systest runner so the
numbers come from the real engine — the binding rule, the source, buffer sealing, the
transformations between tiers — rather than from a harness wrapped around the codecs.

```
tests/     the .test files, one per experiment
scripts/   run, measure, sweep, and assemble
results/   the CSVs and the workbook; superseded/ holds earlier generations
```

## The two byte counts

Every result carries both, because they answer different questions.

| column | meaning |
|---|---|
| `data_out` | the encoded data alone, all framing removed — what the codec produced |
| `disk_bytes` | the file size: header, preallocated table, per-unit descriptors included |

`framing_bytes` is the difference, and it is not small at fine granularities — LossyStore pays
236 B per group (36 B group header plus 50 B per column), which reaches 10.7% of the file at a
16 KB group. Comparing codecs wants `data_out`; budgeting disk wants `disk_bytes`.

MemoryStore writes no file, so it reports `data_out` with `disk_bytes` at 0 rather than claiming
it stored nothing.

## Running

```bash
# one configuration set, at whatever sizes the headers currently hold
./scripts/bench_f2.py tests/standalone_large.test --rows 200001 --row-width 28 -N 3 \
    --dataset debs_wind_turbines --configuration standalone --unit-kb 64

# sweep the unit size: patches three headers, rebuilds, and runs at each point
./scripts/grid_f2.py --test tests/standalone_large.test \
    --dataset debs_wind_turbines --configuration standalone \
    --points 16:16 64:64 256:256 1024:1024 4096:4096 -N 3

./scripts/float_column.py          # per-column bytes against the accuracy bound
./scripts/build_workbook.py        # assemble the CSVs into results/f2_benchmark_analysis.xlsx
```

A grid point is `filestore_kb:tail_kb`. **Pass them equal.** That is the only shape that moves
every tier's unit at once: LossyStore seals one group per incoming segment and ignores its own
`groupSize` when a transformation feeds it, so its group *is* FileStore's segment. Splitting the
pair pins one tier or the other and produces rows that only appear to vary — `grid_f2.py`'s
docstring records both failures with the numbers.

`grid_f2.py` refuses to start unless the three headers read their declared defaults, because an
interrupted run used to leave a sweep value behind and every later run would adopt it as the
"original". That silently mislabelled three rounds of measurements before it was caught.

## What the numbers do not say

**`decode_s` is confounded.** It measures the unit size against a fixed 4 KB read destination, not
the unit size alone. CompressedStore re-reads and re-decompresses a whole segment on every
`read()` call, so a 1 MB segment is decompressed 257 times to fill 4 KB buffers. Given a
destination larger than the segment it decodes in 0.47 s — faster than any row in the tables.

**Chained, the unit is `min(segmentSize, flush threshold)`.** `HISTORY OF '1MB'` drains
MemoryStore every 1 MB, so FileStore never accumulates more and a larger segment cannot fill. The
1024 and 4096 chained rows are the same configuration. Standalone has no flush policy and does
reach 4 MB units.

**Wind turbine data is unusually compressible** — two distinct timestamp deltas, 40% repeated
floats. Its ratios are a best case. `solar_large.test` is the control: irregular timestamps,
99.99% distinct six-decimal floats, roughly half the compression, and no zigzag.

**LossyStore's size is not monotonic in unit size on the wind data.** The timestamp column uses
`ForBitpack`, its bit width tracks the group's span, and a period-3 delta pattern aligns to a byte
boundary only at certain widths. An artifact of that data's regularity — 16 KB winning is not a
recommendation, and it disappears on solar.

## Files

| test | what it measures |
|---|---|
| `standalone_large.test` | each tier alone, 7 configurations |
| `chained_large.test` | `MemoryStore->FileStore->{Compressed,Lossy}`, 5 configurations |
| `solar_large.test` | the standalone sweep over solar panel readings, as a control |
| `float_column.test` | 9 accuracy bounds, write-only; the analysis reads the files |
| `filestore_mechanics.test` | correctness, not timing: unsealed reads, pruning, rolling, chaining |
| `repro_store_selection.test` | reproduction for the store-selection issue |

`bench.py` and `grid.py` are the first generation, kept because they produced everything in
`results/superseded/`. `bench_f2.py` and `grid_f2.py` supersede them and set the tier sizes
independently.
