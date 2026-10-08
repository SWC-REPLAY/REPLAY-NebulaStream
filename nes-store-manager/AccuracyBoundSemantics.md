# The accuracy bound is relative, and that silently destroys clustered columns

Written 2026-10-07. Not fixed. Everything below is read out of the source or computed from
`compression_benchmarks/results/solar_float_column.csv`; nothing is estimated.

## What the bound means today

`WITH ACCURACY BOUND X%` is a **relative** bound. The grammar requires the per-cent sign
(`AntlrSQL.g4:367`), the parser range-checks `0 < X < 100` and stores it as
`replayAccuracyBoundPercent` (`AntlrSQLQueryPlanCreator.cpp:1232-1248`), and the quantiser turns it
into a step in **log** space (`Quantize.cpp:107`):

```cpp
const double deltaLog = 2.0 * std::log1p(relativeBoundPercent / 100.0);
```

Magnitudes are quantised onto a uniform grid in `log|v|`, so the worst error in log space is half a
step, `ln(1+b)` with `b = X/100`. That gives `v'/v ∈ [1/(1+b), 1+b]` — the error is a fixed
*fraction* of each value. Confirmed against the measurements, which land a hair under the bound at
every point:

| declared | worst measured error | = X/100 |
|---|---|---|
| 0.01% | 9.9999994e-05 | 0.0001 |
| 0.1% | 0.000999999975 | 0.001 |
| 1% | 0.00999999956 | 0.01 |
| 5% | 0.0499999992 | 0.05 |
| 20% | 0.199999992 | 0.2 |

The bound applies to FLOAT columns only. Integers and timestamps are untouched at every bound —
same bytes, `ForBitpack`, lossless.

## The failure

A relative bound has nothing to work with when a column's **variation is small compared to its
magnitude**. Take a column clustered at 900 with the signal in the decimals, values in
[900.000, 900.999], asking for 1%:

```
logSpan    = ln(900.999 / 900.000) = ln(1.001110) = 0.00110939
step at 1% = 2 * ln(1.01)                         = 0.01990066
levels     = 0.00110939 / 0.01990066              = 0.056
```

**Fewer than one level.** Every code rounds to 0. `bitWidthFor(0)` returns 0, and `bitPack` with a
width of 0 returns without emitting a bit (`Primitives.cpp:39-42`), so the group stores *no payload
at all*. Every value reads back as exactly 900.000. The column becomes a constant.

**And the bound is honoured.** The worst deviation, 0.999 against a magnitude of 900, is a relative
error of 0.111% — comfortably inside the 1% requested. `worst_recorded_error` would report 0.00111,
every check passes, and the column has been annihilated. Nothing in the pipeline reports a problem,
because by its own definition there isn't one.

This is the dangerous shape of defect: not a wrong answer, a *plausible* one.

## The regime rule

The log grid is useful exactly when

```
span / magnitude  >>  bound
```

where `span / magnitude` means `ln(maxMagnitude / minMagnitude)` for the group.

| | span/magnitude | bound | outcome |
|---|---|---|---|
| solar `producedPower` (2.5e-05 .. 999.999996) | 17.50 (7.6 decades) | 0.0199 at 1% | 879 levels, 10 bits — works |
| clustered at 900 (900.000 .. 900.999) | 0.00111 | 0.0199 at 1% | 0.056 levels, **0 bits — total loss** |

Solar is three to four orders of magnitude clear of the failure regime, which is why nothing in
`results/` shows this. A sensor with a narrow operating band around a large offset — a temperature
in kelvin, a pressure in absolute units, a counter read as a float — sits straight in it.

To resolve an absolute resolution `r` at magnitude `M`, consecutive codes are spaced `2bM` apart,
so the bound must satisfy

```
X%  <=  100 * r / (2M)
```

For the 900.000 column resolving 0.001 that is **X ≤ 0.0000556%**. The tightest bound ever measured
here, 0.01%, is 180× too coarse: it yields 3 bits and a resolution of ~0.125, losing two of the
three decimal places the source carried.

## Related: the bound can be finer than the source data

The other end of the same mismatch. `results/solar_dataset_description.csv` records the decimal
histogram for `producedPower`: 38,427,646 of 42,700,000 values carry **six** decimal places, so the
source resolution is 1e-6. At 1%, a value of 2.5e-05 is held to ±2.5e-07 — three times finer than
the CSV can express. Bits are spent protecting precision that was never in the input.

A bound of `max(relative, source_quantum/2)` would compress strictly better with no additional real
error. Nothing in `Quantize.cpp` knows the source resolution.

## What to do about it

Three changes, smallest first. The first is worth doing whatever is decided about the rest.

**1. Refuse to annihilate silently.** When a group's `logSpan / deltaLog` rounds to zero levels, the
quantiser is about to replace a varying column with a constant while reporting success. That is a
`PRECONDITION`, or at minimum a warning carried out to the caller. Cheap, and it converts a silent
wrong answer into a loud one.

**2. Give floats the frame-of-reference path integers already have.** The `codec` column of
`solar_float_column.csv` shows integers taking `ForBitpack` — subtract the group minimum, bitpack
the residual — while floats only ever get `Quantized` or `Raw`. Subtracting 900.000 first turns the
clustered column into [0.000, 0.999], where the span *is* the magnitude, and a relative bound then
means what a reader expects: 10 bits for the full three decimals, against 0 bits and total loss
today. Choosing per group is a test on numbers `ColumnStats` already gathers.

**3. Decide what the bound should mean.** See below; this is the real question and it is not a bug
fix.

## How to define the error — the options, with their bit cost

Let `R` be the column's range `max - min`, `M` a value's magnitude, `r` the resolution to preserve.
Bits per value is `log2(levels)`.

| definition | guarantee | bits per value | fails when |
|---|---|---|---|
| **relative** (today) | `\|v'-v\| ≤ b·\|v\|` | `log2( ln(Mmax/Mmin) / 2b )` | variation ≪ magnitude |
| **absolute** | `\|v'-v\| ≤ e` | `log2( R / 2e )` | range spans many decades — small values destroyed |
| **range-relative** | `\|v'-v\| ≤ b·R` | `log2( 1 / 2b )` | an outlier widens `R` and degrades everyone |
| **max(abs, rel)** | `\|v'-v\| ≤ max(e, b·\|v\|)` | between the two | needs two parameters |

On solar `producedPower` the first three cost almost the same and mean entirely different things:

```
relative 1%          -> log2(17.50 / 0.0199)  =  9.8  -> 10 bits   (measured: 10.11)
absolute e = 0.5     -> log2(1000 / 1.0)      =  9.97 -> 10 bits
range-relative 0.1%  -> log2(1 / 0.002)       =  8.97 ->  9 bits
```

Same budget, three different error profiles. Under absolute `e = 0.5` the value 2.5e-05 is destroyed
(error 20,000× the value); under relative 1% it is held to ±2.5e-07; under range-relative 0.1% it is
held to ±1.0, also destroyed. **No single definition is right for every column.**

### Recommendation

**Range-relative is the most reasonable default, and absolute is the most defensible when the user
can state it.**

Range-relative, `|v'-v| ≤ b·R`, has the property the other two lack: the error is a fraction of the
information actually present, so it is meaningful whatever the magnitude and whatever the clustering.
Its bit cost, `log2(1/2b)`, depends on **nothing but the bound** — 9 bits at 0.1%, 6 at 1%, for any
column. That predictability is worth a great deal when the point of the tier is a storage budget.

It has one real cost, and it is the thing `Quantize.cpp:104-106` deliberately avoids today:

```cpp
/// The step depends on the requested bound and on nothing else. That is the whole point:
/// every group computes the same step, so a value's accuracy does not depend on which
/// group it landed in or on what else shared it.
```

A per-group `R` reintroduces exactly that dependence, and an outlier in one group degrades every
value beside it. Two ways out:

- **A column-wide `R`, declared by the user.** `producedPower` is a 0–1000 W sensor; that is a fact
  about the instrument, not about the batch. A declared range is group-independent, streaming-safe
  (no first pass needed), and makes the guarantee auditable. This is why an absolute bound is the
  most defensible of all: `e` and a declared `R` are the same statement, and `e` is the one a domain
  expert can actually give you — "half a watt is irrelevant to me".
- **A running column-wide `R`**, maintained as data arrives. Cheaper on the user, but early groups
  see a narrower range than later ones, so accuracy still drifts with arrival order.

So, concretely: keep the relative bound, add the FOR path so clustered columns work, and let the
query declare an **absolute** tolerance as an alternative — `WITH ACCURACY BOUND 0.5 ABSOLUTE`
alongside the existing `0.5%`. The quantiser then picks its grid from whichever the user stated, the
guarantee is checkable per value without reference to any group, and the silent-annihilation regime
disappears because an absolute tolerance cannot be satisfied by a constant unless the data really is
one.

## Related

- `compression_benchmarks/results/solar_float_column.csv` — the nine bounds and their measured worst
  errors; the numbers in the table above come from there.
- `compression_benchmarks/results/solar_dataset_description.csv` — the magnitude range and decimal
  histogram that put solar outside the failure regime.
- Anomaly 1 on the `Anomalies` sheet of `results/solar_analysis.xlsx` — `bitWidth` follows the
  *group's* magnitude span, which is why the measured bit widths come in below the global-span
  prediction above.
- `FileStoreUnsealedCopy.md` — unrelated defect, same house style.
