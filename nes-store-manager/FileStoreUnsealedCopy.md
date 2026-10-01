# FileStore copies the whole unsealed segment on every read

Found 2026-09-30 while explaining an outlier in the solar benchmark. Not fixed. Everything below
is measured or read out of the source; nothing is estimated.

## How it surfaced

One cell in the solar standalone decode table would not sit still:

```
STANDALONE  -  decode seconds
Configuration        256 KB   1024 KB   4096 KB   16384 KB
FileStore              88.0      93.4      92.9      113.6     <-
CompressedStore        85.5      94.0      93.9       90.9
LossyStore lossless    85.8      93.7      93.5       90.4
LossyStore 0.1%        88.6      93.4      93.7       93.0
LossyStore 1%          87.7      93.9      93.2       90.2
LossyStore 5%          88.1      95.2      92.9       88.6
```

I first wrote this off as interference, on the reasoning that FileStore stores rows raw and does no
decoding, so unit size should not reach it. That reasoning was wrong. It is not decoding; it is
copying.

## The mechanism

`FileStore::read` snapshots its state under the lock before doing any I/O. The sealed segments are
handled well — `writer->readAt(...)` preads exactly `chunkRows * tupleSize` bytes. The UNSEALED
segment is not on disk, so its bytes are copied out instead:

```cpp
if (activeSegment.byteLength > 0)
{
    activeIndex = divisions.size();
    divisions.push_back(Division{activeSegment, current().writer.get(), current().dataStart});
    activeSnapshot = activeBytes;          // <- full vector copy, every call
}
```

and then consumed as a slice of at most one destination buffer:

```cpp
const size_t from = rowOffset * tupleSize;
const size_t want = std::min<size_t>(chunkRows * tupleSize, activeSnapshot.size() - from);
std::memcpy(dest, activeSnapshot.data() + from, want);
```

`want` can never exceed `capacity * tupleSize`, which is the destination buffer — 4096 B by
default. So the call copies up to 4.4 MB in order to read 4 KB of it.

Worse, most calls never reach the active division at all:

```
at a 16384 KB segment:  599,186 rows per segment, 146 rows per read call
                        a reader spends 4,104 consecutive calls inside ONE sealed segment
                        calls that touch the active division:  ~1,080 of 292,466

                        0.4% of calls use the snapshot.  100% pay for it.
```

## Why only the 16384 KB cell moved

The cost is `unsealed_bytes x read_calls`. The unsealed tail is whatever the data leaves over after
the last whole segment, and 1,195,600,000 is very nearly a multiple of 256 KB, 1 MB AND 4 MB:

| unit | sealed | unsealed tail | copied per call | total copied | measured |
|---|---|---|---|---|---|
| 256 KB | 4560 | 223,360 B | 0.21 MiB | 0.06 TiB | 88.0 s |
| 1024 KB | 1140 | 223,360 B | 0.21 MiB | 0.06 TiB | 93.4 s |
| 4096 KB | 285 | 223,360 B | 0.21 MiB | 0.06 TiB | 92.9 s |
| **16384 KB** | 71 | **4,417,664 B** | **4.21 MiB** | **1.18 TiB** | **113.6 s** |

20.7 s of excess against the flat three, for 1.12 TiB of extra copying, implies **55 GiB/s** —
memcpy bandwidth. The arithmetic reaches the measurement with nothing fitted to it.

**The other three cells are small by accident, not by design.** A dataset leaving a half-full 16 MB
segment would copy 8 MB per call, about 2.3 TiB, and this would be the slowest row in the table by
a wide margin. The defect is latent at every unit size; this dataset only exposes it at one.

It is the same shape as the decode problem fixed in 078375d1b6 — work proportional to
`unit_size x read_calls` rather than to `unit_size` — in the one tier that looked immune.

## What was checked before proposing anything

**Sealing does not invalidate a cursor.** `++readGeneration` appears exactly once in FileStore.cpp,
inside `drainSegments()`. Appending and sealing preserve the meaning of a unit index, which is
correct and deliberate.

**Writers exclude readers.** `appendBuffer` takes a `unique_lock`, so reading `activeBytes` under
the shared lock cannot race an append, and the vector cannot reallocate underneath it.

**Deferring the copy would introduce a race that does not exist today.** The obvious fix — copy
lazily, only when the loop reaches the active division — is wrong. Between the snapshot and the
point of use the segment can seal: `activeBytes` is cleared and its rows move to disk, so the
reader would serve the wrong bytes. The upfront copy under the first lock is what prevents that,
and it has to stay upfront.

## Proposed fix

Keep the copy upfront and under the same lock. Copy only the window this call can consume:

```cpp
const uint64_t activeFrom = (frame.unitIndex == activeIndex) ? frame.rowOffset : 0;
const size_t fromB = activeFrom * tupleSize;
const size_t wantB = std::min<size_t>(capacity * tupleSize, activeBytes.size() - fromB);
activeSnapshot.assign(activeBytes.begin() + fromB, activeBytes.begin() + fromB + wantB);
activeWindowFrom = activeFrom;            // the loop indexes relative to this
```

A reader is either already inside the active division, in which case the window starts at its
`rowOffset`, or it spills into it from the division before, in which case it enters at row 0.
Either way at most `capacity` rows are consumable in one call, so that is all that needs copying.

Race-free, no cache, no invalidation rule, no change in semantics — 4 KB instead of 4.4 MB. About
ten lines. It needs `capacity` computed before the lock is taken, which touches no store state.

## How to verify it

1. Full store-manager suite stays green — 98 tests across 11 targets at the time of writing.
2. Re-run the standalone solar sweep at 16384 KB only. FileStore decode should fall from 113.6 s to
   roughly 90, in line with the other five configurations.
3. Falsify it the way the decode cache was falsified: restore the full copy, confirm the time goes
   back up. A fix that cannot be shown to matter is not worth the risk to the read path.
4. Worth adding a test, though it needs an observable. The decode work used `debugDecodeCount`;
   the equivalent here would be a counter of bytes copied out of the unsealed segment, which a
   test could assert is O(destination) rather than O(segment).

## Related

- `ReadContract.md` — the one-batch-per-call read contract, and the fill-the-buffer alternative
  that was considered and not taken. Same read path, different question.
- Commit 078375d1b6 — the decoded-unit buffer on the cursor frame, which fixed the same shape of
  problem in CompressedStore and LossyStore.
- Anomaly 4 on the `Anomalies` sheet of `results/solar_analysis.xlsx` still describes this as
  "probably ordinary interference". **That is now known to be wrong and should be corrected when
  this is fixed.**
