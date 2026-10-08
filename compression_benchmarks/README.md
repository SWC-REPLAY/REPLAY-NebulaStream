# Compression benchmarks

Macro benchmarks for the replay store tiers, driven through SQL by the systest runner so the
numbers come from the real engine — the binding rule, the source, buffer sealing, the
transformations between tiers — rather than from a harness wrapped around the codecs.

```
tests/              the .test files, one per experiment
scripts/            run, measure, sweep, and assemble
results/            Debug-build CSVs and the two workbooks
results/benchmark/  Benchmark-build CSVs and workbooks
```

**The two builds are kept apart on disk, deliberately.** A Debug number and a Benchmark number for
the same configuration can differ by 9x on the read path, so a directory is cheaper than a
convention for not confusing them. `bench.py`, `grid.py`, `float_column.py`, `lossy_breakdown.py`
and `workbook.py` work against `cmake-build-debug` and write to `results/`; `grid_release.py` and
`standalone_workbook.py` work against `cmake-build-release` and write to `results/benchmark/`. The
dataset descriptions sit at the top of `results/` because they describe the input CSV and not a run.

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

./scripts/workbook.py wind     # -> wind_analysis.xlsx
./scripts/workbook.py solar    # -> solar_analysis.xlsx
```

Against the **Benchmark** build, writing to `results/benchmark/`. Both patch the store headers and
restore them in a `finally`; both refuse to start unless the headers read their declared defaults.

```bash
# the standalone sweep, with a store-free baseline measured at every grid point.
# one unit size per invocation: a point that fails costs only itself, and the four
# CSVs merge afterwards. --build-jobs and --pause keep a laptop from cooking.
./scripts/grid_release.py --test tests/solar_standalone.test --dataset debs_solar_panels \
    --configuration standalone --points 256:256 -N 1 --rows 42700000 \
    --prefix solar_standalone_release_256 --filestore-total-mb 4096 \
    --build-jobs 4 --pause 60
./scripts/standalone_workbook.py             # -> benchmark/solar_standalone_release.xlsx
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
each 4 KB output buffer. Measured on wind before and after, the speedup tracks the re-decode factor
exactly -- 1.0× at a 16 KB unit, 25.3× at 4096 KB, 5.3× over all 35 configurations. Decode is now
flat on both datasets. Figures in git history from before that change are not comparable.

**One decode cell is still wrong, and it is not the codec.** FileStore at a 16384 KB unit on solar
takes 113.6 s against a 93 s median. It copies the whole unsealed segment on every `read()` call
and uses at most 4 KB of it; this dataset leaves a 4.4 MB tail at 16 MB segments against 223 KB at
the other three sizes. Latent at every unit size. See `nes-store-manager/FileStoreUnsealedCopy.md`.

**Chaining costs almost nothing on solar and a lot on wind** — 0.5% against 13–33%. That is not a
property of chaining. FileStore's leftover is a fixed ~2.5 MB set by the 4 MB flush threshold,
which is 6.6% of wind's 5.6 MB and 0.2% of solar's 1.2 GB.

**Lossy read-back checksums cannot be pinned across a unit sweep.** Quantisation depends on group
size, so a pinned value is right at one sweep point and wrong at the rest. Both datasets now use
`TYPE Void` for the three bounded-lossy read-backs. The wind writes and lossless read-backs keep
their checksums, since those are byte-exact whatever the group size; the solar ones are all Void
only because the full-scale checksum was never measured.

**`bench.py` does not check systest's exit status.** It parses timings and nothing else. That is
how three failing queries went unnoticed across four wind grid points before the Void change.

**Timings are N=3 on wind and N=1 on solar.** Byte counts are deterministic and exact either way.

**Every timing here is a Debug build, and on the read path that is 88% of it.** Measured against
the Benchmark build at full scale: the read-back floor is 10.4 s, not 93. That is why `decode_s`
cannot rank codecs — FileStore decodes nothing and LossyStore at 1% runs all three stages, yet
Debug puts them within 2% of each other. Two conclusions here do not survive an optimised build:

- **Decode is NEGATIVE.** All three compressed stores read back *faster* than raw FileStore rows,
  by 1–9%, in 12 of 12 comparisons across four sessions. The decode costs less than the I/O it
  saves — 375 MB off the disk instead of 1,196 MB. Debug shows FileStore mid-pack; it is reliably
  the *slowest* reader.
- **The unit-size ranking on encode inverts.** Debug makes 1024 KB look 16% faster than 256 KB on
  CompressedStore (135.7 s against 156.8 s); in the Benchmark build they are tied with 256 KB
  marginally ahead (33.8 s against 34.4 s). Since the byte counts also favour 256 KB, it wins on
  both axes rather than trading off.

The byte counts are unaffected and reproduce the Debug run exactly, digit for digit, across four
independent runs. `grid_release.py` is the Benchmark-build measurement; `bench.py` and `grid.py`
still run against Debug.

## Anomalies

Both workbooks have an Anomalies sheet. Three were chased to the source:

- **Solar's bounded ratios fall as the unit grows; wind's zigzag.** Both confirmed. The quantiser
  is logarithmic and `bitWidth` follows the *group's* magnitude span, so a smaller group needs
  fewer bits. Wind's zigzag is its period-3 timestamp delta aligning to a byte boundary only at
  some widths — an artifact of its regularity, which solar has none of.
- **The 5% bound stores more than 2%** — explained, and not the quantiser. At 5% it picks a 7-bit
  width against 2%'s 9 and its packed output really is smaller; the zstd pass is kept in ~92% of
  groups at 2% and ~40% at 5%, which more than cancels it. **Both datasets agree to three decimal
  places** (wind ×1.2148 / ×1.0125, solar ×1.2085 / ×1.0137), so it is a property of the
  quantiser-to-zstd handoff rather than of any data. The width is chosen before zstd is considered,
  at `LossyStore.cpp:314`.
- **FileStore's decode outlier** — not interference. See the note linked above.

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
| `solar_baseline.test` | the same query with NO store, so a store's cost is a difference |
| `filestore_mechanics.test` | correctness, not timing: unsealed reads, pruning, rolling, chaining |
| `repro_store_selection.test` | reproduction for the store-selection issue |

`workbook.py` builds both, 16 sheets and 35 charts each, ending in a Summary of six charts drawn
from the analysis sheets. One script rather than three: the machinery is shared and only a
`DATASETS` entry differs -- units, configurations, row count, and the prose. That matters because
the two experiments CONTRADICT each other on three of four axes, and a shared script makes each
summary cite the other where they disagree instead of letting them drift apart.
