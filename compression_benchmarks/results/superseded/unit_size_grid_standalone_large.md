# Unit-size grid, standalone stores

Run 2026-09-24 on compression_benchmarks/tests/standalone_large.test
200,001 DEBS wind turbine rows x 28 B = 5,600,028 B. Median of 3 runs per point.

The unit is the granule a tier seals on: FileStore's segment, LossyStore's group,
CompressedStore's segment. All three are patched to the same value at each point.

  data_out   the encoded data alone, every byte of framing removed -- what the codec produced
  bytes_out  the file size, framing included: file header, preallocated table, per-unit headers
  MemoryStore holds everything in TupleBuffers and writes no file, so it has neither.

bytes in
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore     5,600,028     5,600,028     5,600,028     5,600,028
  LossyStore          5,600,028     5,600,028     5,600,028     5,600,028
  LossyStore 0.1%     5,600,028     5,600,028     5,600,028     5,600,028
  LossyStore 1%       5,600,028     5,600,028     5,600,028     5,600,028
  LossyStore 5%       5,600,028     5,600,028     5,600,028     5,600,028
  MemoryStore         5,600,028     5,600,028     5,600,028     5,600,028
  FileStore           5,600,028     5,600,028     5,600,028     5,600,028

data out (framing excluded)
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore     1,768,681     1,719,557     1,713,493     1,714,576
  LossyStore          1,206,685     1,499,024     1,365,416     1,560,562
  LossyStore 0.1%       808,854     1,136,554     1,018,407     1,213,496
  LossyStore 1%         730,085     1,053,788       920,140     1,120,901
  LossyStore 5%         672,699     1,000,058       861,047     1,051,816
  MemoryStore            in RAM        in RAM        in RAM        in RAM
  FileStore           5,600,028     5,600,028     5,600,028     5,600,028

bytes out (on disk, framing included)
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore     1,783,081     1,723,461     1,714,773     1,715,200
  LossyStore          1,287,775     1,519,698     1,370,986     1,562,356
  LossyStore 0.1%       889,944     1,157,228     1,023,977     1,215,290
  LossyStore 1%         811,175     1,074,462       925,710     1,122,695
  LossyStore 5%         753,789     1,020,732       866,617     1,053,610
  MemoryStore              none          none          none          none
  FileStore           5,621,264     5,605,904     5,602,064     5,601,104

encode seconds
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore        0.7072        0.6771        0.6604        0.6582
  LossyStore             0.6899        0.6684        0.6685        0.6832
  LossyStore 0.1%        0.6904        0.7097        0.7392        0.6898
  LossyStore 1%          0.7179        0.6966        0.6779        0.6716
  LossyStore 5%          0.7465        0.6865        0.6731        0.6561
  MemoryStore            0.6595        0.6570        0.6277        0.7170
  FileStore              0.6706        0.6650        0.6458        0.6846

decode seconds
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore        0.4366        0.4558        0.5409        2.0901
  LossyStore             0.4328        0.4514        0.7577        2.7314
  LossyStore 0.1%        0.4455        0.4374        0.7607        2.8352
  LossyStore 1%          0.4998        0.4412        0.7465        2.7969
  LossyStore 5%          0.4462        0.4512        0.7621        2.9677
  MemoryStore            0.4414        0.4738        0.4774        0.4535
  FileStore              0.4545        0.4635        0.4887        0.4213

ratio on data
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore         3.166         3.257         3.268         3.266
  LossyStore              4.641         3.736         4.101         3.588
  LossyStore 0.1%         6.923         4.927         5.499         4.615
  LossyStore 1%           7.670         5.314         6.086         4.996
  LossyStore 5%           8.325         5.600         6.504         5.324
  MemoryStore                 -             -             -             -
  FileStore               1.000         1.000         1.000         1.000

ratio on disk
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore         3.141         3.249         3.266         3.265
  LossyStore              4.349         3.685         4.085         3.584
  LossyStore 0.1%         6.293         4.839         5.469         4.608
  LossyStore 1%           6.904         5.212         6.049         4.988
  LossyStore 5%           7.429         5.486         6.462         5.315
  MemoryStore                 -             -             -             -
  FileStore               0.996         0.999         1.000         1.000

framing as % of disk bytes
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore          0.81          0.23          0.07          0.04
  LossyStore               6.30          1.36          0.41          0.11
  LossyStore 0.1%          9.11          1.79          0.54          0.15
  LossyStore 1%           10.00          1.92          0.60          0.16
  LossyStore 5%           10.76          2.03          0.64          0.17
  MemoryStore                 -             -             -             -
  FileStore                0.38          0.10          0.04          0.02


## Why LossyStore zigzags and CompressedStore does not

LossyStore's stored size is NOT monotonic in group size -- 1,206,685 -> 1,499,024 -> 1,365,416 ->
1,560,562 lossless, and the same shape at every bound. It is in the data column too, so it is the
encoding and not the framing. CompressedStore over the identical input is monotonic, which is the
clue: it hands row-major bytes to zstd as one block and never bit-packs, so it has no per-column
width to misalign.

The whole effect is ONE COLUMN. Sweeping the group size and summing each column separately:

    group   producerId    groupId   producedPower        ts
     16 KB     153,591     78,505         732,893   241,696
     32 KB     151,668     76,689         713,294   520,832   <- +115%
     64 KB     150,881     75,870         697,848   574,425
    128 KB     150,452     75,441         694,664   545,417
    256 KB     150,216     75,218         695,680   444,302   <- -18%
    512 KB     150,113     75,111         679,499   586,454
   1024 KB     150,063     75,061         697,184   638,254

Three columns move by fractions of a percent. The timestamp column swings by +115%, -18%, +32%.

The mechanism. The timestamp column is encoded with ForBitpack -- "subtract the block's minimum,
then bit-pack the remainders" -- not with DeltaOfDelta, the codec whose own comment says
"Timestamps." So the bit width W tracks the group's timestamp SPAN, which grows one bit per
doubling of the group: 24, 25, 26 ... 30.

The DEBS timestamps have only two distinct deltas, 0 and 60000, in a period-3 pattern: three rows
share a timestamp, then it jumps a minute. Packed at W bits each, that 3-value pattern only
realigns to a byte boundary every lcm(3W, 8) bits, and the byte compressor that runs afterwards
exploits a short repeat period and barely touches a long one:

    group     W    3W    repeat period   ts bytes/row
     16 KB   24    72         9 B            1.21     <- best, exact byte alignment
     32 KB   25    75        75 B            2.60
     64 KB   26    78        39 B            2.87
    128 KB   27    81        81 B            2.73
    256 KB   28    84        21 B            2.22     <- second best
    512 KB   29    87        87 B            2.93
   1024 KB   30    90        45 B            3.19

The two shortest repeat periods, 9 B and 21 B, are exactly the two group sizes that dip: 16 KB and
256 KB.

What this means for choosing a unit size: the 16 KB result is an ARTIFACT. 585 rows per group
happens to give a 24-bit width, and 3 x 24 = 72 is divisible by 8. It is a coincidence between this
dataset's regularity and byte alignment, and it will not survive data with irregular timestamps.
16 KB should not be recommended on this evidence.

Two fixes would remove it at the root, neither applied here:
  - choose DeltaOfDelta for the timestamp column. Its deltas are 0, 0, +60000 repeating, so the
    delta-of-delta alphabet is tiny and needs a fixed width INDEPENDENT of group size.
  - producedPower is stored Raw -- 672,816 B at 64 KB, 44% of the file, uncompressed.


## Two other things these numbers do not show

The decode column is CONFOUNDED. It measures unit size against a fixed 4 KB destination buffer, not
unit size itself. CompressedStore re-reads and re-decompresses a whole segment on every read() call,
so a 1 MB segment against a 146-row destination is decompressed 257 times. Given a destination
larger than the segment, 1 MB units decode in 0.47 s -- faster than anything in this table. The
"64 KB is the ceiling" reading is wrong; the real rule is that the unit must not exceed the
destination buffer.

FileStore's data_out is flat at 5,600,028 across the whole sweep, ratio exactly 1.000. Unit size
changes nothing it stores, only the framing: 21,236 B of header and preallocated table at 16 KB
down to 1,076 B at 1024 KB.
