# Unit-size grid, standalone stores

Run 2026-09-24 on compression_benchmarks/tests/solar_large.test
Sizes [16, 64, 256, 1024], median of 3 runs per point.

  data_out   the encoded data alone, framing removed -- what the codec produced
  bytes_out  the file size, framing included: header, preallocated table, per-unit headers
  MemoryStore writes no file, so it has neither.

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
  CompressedStore     3,366,225     3,386,892     3,394,752     3,391,916
  LossyStore          2,954,151     2,982,453     3,004,635     3,002,157
  LossyStore 0.1%     1,746,417     1,789,120     1,849,131     1,893,465
  LossyStore 1%       1,668,166     1,706,246     1,753,016     1,802,233
  LossyStore 5%       1,610,111     1,650,501     1,685,796     1,709,580
  MemoryStore            in RAM        in RAM        in RAM        in RAM
  FileStore           5,600,028     5,600,028     5,600,028     5,600,028

bytes out (on disk, framing included)
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore     3,380,625     3,390,796     3,396,032     3,392,540
  LossyStore          3,035,241     3,003,127     3,010,205     3,003,951
  LossyStore 0.1%     1,827,507     1,809,794     1,854,701     1,895,259
  LossyStore 1%       1,749,256     1,726,920     1,758,586     1,804,027
  LossyStore 5%       1,691,201     1,671,175     1,691,366     1,711,374
  MemoryStore              none          none          none          none
  FileStore           5,621,264     5,605,904     5,602,064     5,601,104

encode seconds
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore        0.6782        0.6705        0.6598        0.6648
  LossyStore             0.6743        0.6706        0.6672        0.6766
  LossyStore 0.1%        0.7151        0.6755        0.6709        0.6706
  LossyStore 1%          0.7283        0.6967        0.6656        0.6898
  LossyStore 5%          0.6843        0.6787        0.6766        0.6834
  MemoryStore            0.6416        0.6543        0.6496        0.6506
  FileStore              0.6692        0.6442        0.6748        0.6610

decode seconds
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore        0.4350        0.4258        0.4572        1.8700
  LossyStore             0.4358        0.4609        0.6608        2.6763
  LossyStore 0.1%        0.4136        0.4240        0.7154        2.6972
  LossyStore 1%          0.4178        0.4534        0.7141        2.7632
  LossyStore 5%          0.4682        0.4420        0.7374        2.7977
  MemoryStore            0.4514        0.4271        0.4487        0.4941
  FileStore              0.4391        0.4566        0.4376        0.4490

ratio on data
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore         1.664         1.653         1.650         1.651
  LossyStore              1.896         1.878         1.864         1.865
  LossyStore 0.1%         3.207         3.130         3.028         2.958
  LossyStore 1%           3.357         3.282         3.195         3.107
  LossyStore 5%           3.478         3.393         3.322         3.276
  MemoryStore                 -             -             -             -
  FileStore               1.000         1.000         1.000         1.000

ratio on disk
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore         1.657         1.652         1.649         1.651
  LossyStore              1.845         1.865         1.860         1.864
  LossyStore 0.1%         3.064         3.094         3.019         2.955
  LossyStore 1%           3.201         3.243         3.184         3.104
  LossyStore 5%           3.311         3.351         3.311         3.272
  MemoryStore                 -             -             -             -
  FileStore               0.996         0.999         1.000         1.000

framing as % of disk bytes
  store                   16 KB         64 KB        256 KB       1024 KB
  CompressedStore          0.43          0.12          0.04          0.02
  LossyStore               2.67          0.69          0.19          0.06
  LossyStore 0.1%          4.44          1.14          0.30          0.09
  LossyStore 1%            4.64          1.20          0.32          0.10
  LossyStore 5%            4.79          1.24          0.33          0.10
  MemoryStore                 -             -             -             -
  FileStore                0.38          0.10          0.04          0.02


  store               unit    bytes_in    data_out   bytes_out  data_ratio   ratio  encode_s  decode_s
  CompressedStore    16 KB   5,600,028   3,366,225   3,380,625       1.664   1.657    0.6782    0.4350
                     64 KB   5,600,028   3,386,892   3,390,796       1.653   1.652    0.6705    0.4258
                    256 KB   5,600,028   3,394,752   3,396,032       1.650   1.649    0.6598    0.4572
                   1024 KB   5,600,028   3,391,916   3,392,540       1.651   1.651    0.6648    1.8700
  LossyStore         16 KB   5,600,028   2,954,151   3,035,241       1.896   1.845    0.6743    0.4358
                     64 KB   5,600,028   2,982,453   3,003,127       1.878   1.865    0.6706    0.4609
                    256 KB   5,600,028   3,004,635   3,010,205       1.864   1.860    0.6672    0.6608
                   1024 KB   5,600,028   3,002,157   3,003,951       1.865   1.864    0.6766    2.6763
  LossyStore 0.1%    16 KB   5,600,028   1,746,417   1,827,507       3.207   3.064    0.7151    0.4136
                     64 KB   5,600,028   1,789,120   1,809,794       3.130   3.094    0.6755    0.4240
                    256 KB   5,600,028   1,849,131   1,854,701       3.028   3.019    0.6709    0.7154
                   1024 KB   5,600,028   1,893,465   1,895,259       2.958   2.955    0.6706    2.6972
  LossyStore 1%      16 KB   5,600,028   1,668,166   1,749,256       3.357   3.201    0.7283    0.4178
                     64 KB   5,600,028   1,706,246   1,726,920       3.282   3.243    0.6967    0.4534
                    256 KB   5,600,028   1,753,016   1,758,586       3.195   3.184    0.6656    0.7141
                   1024 KB   5,600,028   1,802,233   1,804,027       3.107   3.104    0.6898    2.7632
  LossyStore 5%      16 KB   5,600,028   1,610,111   1,691,201       3.478   3.311    0.6843    0.4682
                     64 KB   5,600,028   1,650,501   1,671,175       3.393   3.351    0.6787    0.4420
                    256 KB   5,600,028   1,685,796   1,691,366       3.322   3.311    0.6766    0.7374
                   1024 KB   5,600,028   1,709,580   1,711,374       3.276   3.272    0.6834    2.7977
  MemoryStore        16 KB   5,600,028      in RAM        none           -       -    0.6416    0.4514
                     64 KB   5,600,028      in RAM        none           -       -    0.6543    0.4271
                    256 KB   5,600,028      in RAM        none           -       -    0.6496    0.4487
                   1024 KB   5,600,028      in RAM        none           -       -    0.6506    0.4941
  FileStore          16 KB   5,600,028   5,600,028   5,621,264       1.000   0.996    0.6692    0.4391
                     64 KB   5,600,028   5,600,028   5,605,904       1.000   0.999    0.6442    0.4566
                    256 KB   5,600,028   5,600,028   5,602,064       1.000   1.000    0.6748    0.4376
                   1024 KB   5,600,028   5,600,028   5,601,104       1.000   1.000    0.6610    0.4490
