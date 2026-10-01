# Compression benchmarks

Macro benchmarks for the replay store tiers, driven through SQL by the systest runner so the
numbers come from the real engine — the binding rule, the source, buffer sealing, the
transformations between tiers — rather than from a harness wrapped around the codecs.

```
tests/     the .test files, one per experiment
scripts/   run, measure, sweep, and assemble
results/   the CSVs and the two workbooks
```

**Two datasets, one toolchain.** Everything is named `wind_*` or `solar_*`; there are no
generation suffixes. Earlier generations were deleted in favour of a single current set — recover
them from git history if a past number needs re-checking.

| | wind turbines | solar panels |
|---|---|---|
| rows | 200,001 | 42,700,000 |
| stored | 5,600,028 B | 1,195,600,000 B |
| unit sizes swept | 16 / 64 / 256 / 1024 / 4096 KB | 256 / 1024 / 4096 / 16384 KB |
| standalone configs | 7, MemoryStore included | 6, MemoryStore excluded |
| best ratio seen | 8.33 | 3.31 |

Solar is the control. Wind has 26 distinct producer IDs and exactly **two** distinct timestamp
deltas, which is why its ratios are a best case; solar's producer ID is a shuffled permutation of
0..42,699,999 and its timestamps have no repeating delta at all.

## The byte counts

| column | meaning |
|---|---|
| `data_out` | the encoded data alone, all framing removed — what the codec produced |
| `disk_bytes` | the file size: header, preallocated table, per-unit descriptors included |
| `ratio_on_tail` | chained only: excludes bytes FileStore still holds raw, so it is the codec's own figure |

A ratio is the wrong instrument for framing at solar's scale — 328 KB of overhead on 1.2 GB
rounds to 1.000 at three decimals. The workbook reports framing in **bytes** as well.

## Running

```bash
# one configuration set, at whatever sizes the headers currently hold
./scripts/bench.py tests/wind_standalone.test --rows 200001 --row-width 28 -N 3 \
    --dataset debs_wind_turbines --configuration standalone --unit-kb 64

# sweep the unit size: patches three headers, rebuilds, runs at each point, restores
./scripts/grid.py --test tests/solar_standalone.test \
    --dataset debs_solar_panels --configuration standalone \
    --points 256:256 1024:1024 4096:4096 16384:16384 -N 1 \
    --rows 42700000 --prefix solar_standalone --filestore-total-mb 4096

./scripts/dataset_analysis.py --csv <input.csv> --dataset <label> \
    --source-file <path as the .test names it> --prefix solar \
    --schema producerId:INT32 groupId:INT32 producedPower:FLOAT64 timestamp:UINT64

./scripts/float_column.py   --test tests/solar_float_column.test --dataset debs_solar_panels \
                            --rows 42700000 --prefix solar
./scripts/lossy_breakdown.py --test tests/solar_float_column.test --dataset debs_solar_panels \
                            --rows 42700000 --prefix solar

./scripts/workbook_wind.py && ./scripts/workbook_wind_charts.py   # -> wind_analysis.xlsx
./scripts/workbook_solar.py                                       # -> solar_analysis.xlsx
```

`--filestore-total-mb` only for a **standalone** run over a large input. A chain must not have it:
the flush threshold is derived as `totalSize / filesAtCapacity`, so raising capacity to 4 GB would
set the threshold to 1 GB and FileStore would sit on nearly the whole dataset uncompressed.

A grid point is `filestore_kb:tail_kb`. **Pass them equal.** That is the only shape that moves
every tier's unit at once: LossyStore seals one group per incoming segment and ignores its own
`groupSize` when a transformation feeds it, so its group *is* FileStore's segment. `grid.py`'s
docstring records both failure modes with the numbers.

`grid.py` refuses to start unless the patched headers read their declared defaults, because an
interrupted run used to leave a sweep value behind and every later run would adopt it as the
"original". That silently mislabelled three rounds of measurements before it was caught.

## What the numbers do not say

**`decode_s` no longer scales with unit size.** It used to: every tier re-decoded its whole unit on
each 4 KB output buffer, so a 1 MB segment was decompressed 257 times. A decoded-unit buffer now
lives on the read cursor and decode is flat at 85–95 s across a 64× change in unit size. Figures
in git history from before that change are not comparable.

**Chaining costs almost nothing on solar and a lot on wind** — 0.5% against 13–33%. That is not a
property of chaining. FileStore's leftover is a fixed ~2.5 MB set by the 4 MB flush threshold,
which is 6.6% of wind's 5.6 MB and 0.2% of solar's 1.2 GB.

**Lossy read-back checksums cannot be pinned across a unit sweep.** Quantisation depends on group
size, so a pinned checksum is wrong at every point but one. The solar tests use `TYPE Void` sinks
and assert nothing; `wind_*.test` still pins them and therefore reports three failures at every
non-default group size, silently, because the bench script never checks the exit status.

**Timings are N=1 on solar.** Byte counts are deterministic and exact; seconds are single samples.

## Anomalies

`solar_analysis.xlsx` has a sheet for these. Two were chased to the source:

- **Bounded ratios fall as the unit grows** — confirmed. The quantiser is logarithmic, and
  `bitWidth` follows the *group's* magnitude span, so a smaller group needs fewer bits.
- **The 5% bound stores more than 2%** — explained, and it is not the quantiser. At 5% it picks a
  7-bit width against 2%'s 9 and its packed output really is smaller; the zstd pass afterwards is
  kept in 91% of groups at 2% and only 39% at 5%, which more than cancels it. The bit width is
  chosen before zstd is considered, so a width that packs tightly but compresses badly is never
  compared against a looser one that compresses well.

## Files

| test | what it measures |
|---|---|
| `wind_standalone.test` | each tier alone, 7 configurations |
| `wind_chained.test` | `MemoryStore->FileStore->{Compressed,Lossy}`, 5 configurations |
| `wind_float_column.test` | 9 accuracy bounds, write-only; the analysis reads the files |
| `solar_standalone.test` | 6 configurations over the full 42.7M rows |
| `solar_chained.test` | the same chain at full scale |
| `solar_float_column.test` | 9 bounds at full scale |
| `solar_smoke.test` | 1M-row copy of `solar_standalone`; validates a pipeline before a 2-hour run |
| `filestore_mechanics.test` | correctness, not timing: unsealed reads, pruning, rolling, chaining |
| `repro_store_selection.test` | reproduction for the store-selection issue |

`workbook_solar.py` builds its whole workbook in one openpyxl pass. `workbook_wind.py` predates
openpyxl being available here, goes via Flat ODS, and needs `workbook_wind_charts.py` afterwards.
Merging the wind pair into the solar shape is a worthwhile follow-up.
