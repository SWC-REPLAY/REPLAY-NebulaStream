# Unit-size grid, standalone stores

Run 2026-09-24 on compression_benchmarks/tests/chained_large.test
Sizes [16, 64, 256, 1024], median of 3 runs per point.

  data_out   the encoded data alone, framing removed -- what the codec produced
  bytes_out  the file size, framing included: header, preallocated table, per-unit headers
  MemoryStore writes no file, so it has neither.

bytes in
  store                                           16 KB         64 KB        256 KB       1024 KB
  MemoryStore->FileStore->CompressedStore     5,600,028     5,600,028     5,600,028     5,600,028
  MemoryStore->FileStore->LossyStore          5,600,028     5,600,028     5,600,028     5,600,028
  MemoryStore->FileStore->LossyStore 0.1%     5,600,028     5,600,028     5,600,028     5,600,028
  MemoryStore->FileStore->LossyStore 1%       5,600,028     5,600,028     5,600,028     5,600,028
  MemoryStore->FileStore->LossyStore 5%       5,600,028     5,600,028     5,600,028     5,600,028

data out (framing excluded)
  store                                           16 KB         64 KB        256 KB       1024 KB
  MemoryStore->FileStore->CompressedStore     1,755,874     1,720,140     1,723,679     1,714,529
  MemoryStore->FileStore->LossyStore          1,189,336     1,498,733     1,361,734     1,564,043
  MemoryStore->FileStore->LossyStore 0.1%       797,909     1,135,131     1,014,759     1,216,958
  MemoryStore->FileStore->LossyStore 1%         718,683     1,052,946       916,572     1,124,385
  MemoryStore->FileStore->LossyStore 5%         662,558       999,449       857,369     1,055,305

bytes out (on disk, framing included)
  store                                           16 KB         64 KB        256 KB       1024 KB
  MemoryStore->FileStore->CompressedStore     1,778,268     1,726,982     1,725,977     1,715,691
  MemoryStore->FileStore->LossyStore          1,265,940     1,522,345     1,368,322     1,566,375
  MemoryStore->FileStore->LossyStore 0.1%       874,513     1,158,743     1,021,347     1,219,290
  MemoryStore->FileStore->LossyStore 1%         795,287     1,076,558       923,160     1,126,717
  MemoryStore->FileStore->LossyStore 5%         739,162     1,023,061       863,957     1,057,637

encode seconds
  store                                           16 KB         64 KB        256 KB       1024 KB
  MemoryStore->FileStore->CompressedStore        0.6959        0.6710        0.6802        0.6776
  MemoryStore->FileStore->LossyStore             0.7095        0.6950        0.6970        0.7377
  MemoryStore->FileStore->LossyStore 0.1%        0.6985        0.6977        0.6877        0.7230
  MemoryStore->FileStore->LossyStore 1%          0.7180        0.6966        0.7111        0.6866
  MemoryStore->FileStore->LossyStore 5%          0.7028        0.6943        0.6918        0.7056

decode seconds
  store                                           16 KB         64 KB        256 KB       1024 KB
  MemoryStore->FileStore->CompressedStore        0.4415        0.4284        0.5385        2.0863
  MemoryStore->FileStore->LossyStore             0.4093        0.4606        0.7678        2.6538
  MemoryStore->FileStore->LossyStore 0.1%        0.4354        0.4311        0.7387        2.7790
  MemoryStore->FileStore->LossyStore 1%          0.4260        0.4383        0.7730        2.8083
  MemoryStore->FileStore->LossyStore 5%          0.4292        0.4288        0.7414        2.8241

ratio on data
  store                                           16 KB         64 KB        256 KB       1024 KB
  MemoryStore->FileStore->CompressedStore         3.189         3.256         3.249         3.266
  MemoryStore->FileStore->LossyStore              4.709         3.737         4.112         3.580
  MemoryStore->FileStore->LossyStore 0.1%         7.018         4.933         5.519         4.602
  MemoryStore->FileStore->LossyStore 1%           7.792         5.318         6.110         4.981
  MemoryStore->FileStore->LossyStore 5%           8.452         5.603         6.532         5.307

ratio on disk
  store                                           16 KB         64 KB        256 KB       1024 KB
  MemoryStore->FileStore->CompressedStore         3.149         3.243         3.245         3.264
  MemoryStore->FileStore->LossyStore              4.424         3.679         4.093         3.575
  MemoryStore->FileStore->LossyStore 0.1%         6.404         4.833         5.483         4.593
  MemoryStore->FileStore->LossyStore 1%           7.042         5.202         6.066         4.970
  MemoryStore->FileStore->LossyStore 5%           7.576         5.474         6.482         5.295

framing as % of disk bytes
  store                                           16 KB         64 KB        256 KB       1024 KB
  MemoryStore->FileStore->CompressedStore          1.26          0.40          0.13          0.07
  MemoryStore->FileStore->LossyStore               6.05          1.55          0.48          0.15
  MemoryStore->FileStore->LossyStore 0.1%          8.76          2.04          0.65          0.19
  MemoryStore->FileStore->LossyStore 1%            9.63          2.19          0.71          0.21
  MemoryStore->FileStore->LossyStore 5%           10.36          2.31          0.76          0.22


  store                                       unit    bytes_in    data_out   bytes_out  data_ratio   ratio  encode_s  decode_s
  MemoryStore->FileStore->CompressedStore    16 KB   5,600,028   1,755,874   1,778,268       3.189   3.149    0.6959    0.4415
                                             64 KB   5,600,028   1,720,140   1,726,982       3.256   3.243    0.6710    0.4284
                                            256 KB   5,600,028   1,723,679   1,725,977       3.249   3.245    0.6802    0.5385
                                           1024 KB   5,600,028   1,714,529   1,715,691       3.266   3.264    0.6776    2.0863
  MemoryStore->FileStore->LossyStore         16 KB   5,600,028   1,189,336   1,265,940       4.709   4.424    0.7095    0.4093
                                             64 KB   5,600,028   1,498,733   1,522,345       3.737   3.679    0.6950    0.4606
                                            256 KB   5,600,028   1,361,734   1,368,322       4.112   4.093    0.6970    0.7678
                                           1024 KB   5,600,028   1,564,043   1,566,375       3.580   3.575    0.7377    2.6538
  MemoryStore->FileStore->LossyStore 0.1%    16 KB   5,600,028     797,909     874,513       7.018   6.404    0.6985    0.4354
                                             64 KB   5,600,028   1,135,131   1,158,743       4.933   4.833    0.6977    0.4311
                                            256 KB   5,600,028   1,014,759   1,021,347       5.519   5.483    0.6877    0.7387
                                           1024 KB   5,600,028   1,216,958   1,219,290       4.602   4.593    0.7230    2.7790
  MemoryStore->FileStore->LossyStore 1%      16 KB   5,600,028     718,683     795,287       7.792   7.042    0.7180    0.4260
                                             64 KB   5,600,028   1,052,946   1,076,558       5.318   5.202    0.6966    0.4383
                                            256 KB   5,600,028     916,572     923,160       6.110   6.066    0.7111    0.7730
                                           1024 KB   5,600,028   1,124,385   1,126,717       4.981   4.970    0.6866    2.8083
  MemoryStore->FileStore->LossyStore 5%      16 KB   5,600,028     662,558     739,162       8.452   7.576    0.7028    0.4292
                                             64 KB   5,600,028     999,449   1,023,061       5.603   5.474    0.6943    0.4288
                                            256 KB   5,600,028     857,369     863,957       6.532   6.482    0.6918    0.7414
                                           1024 KB   5,600,028   1,055,305   1,057,637       5.307   5.295    0.7056    2.8241
