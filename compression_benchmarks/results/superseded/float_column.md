# The float column under an accuracy bound

200,001 DEBS rows. LossyStore standalone at its 64 KB group default.
Only `producedPower` is a float; the other three columns are shown as the control.

bytes per column
  bound        producerId i32         groupId i32   producedPower f64              ts u64         total
    0.0%              150,881              75,870             697,848             574,425     1,499,024
   0.01%              150,881              75,870             404,387             574,425     1,205,563
    0.1%              150,881              75,870             335,378             574,425     1,136,554
    0.5%              150,881              75,870             275,907             574,425     1,077,083
    1.0%              150,881              75,870             252,612             574,425     1,053,788
    2.0%              150,881              75,870             192,392             574,425       993,568
    5.0%              150,881              75,870             198,882             574,425     1,000,058
   10.0%              150,881              75,870             158,442             574,425       959,618
   20.0%              150,881              75,870             142,123             574,425       943,299

change from lossless, per column
  bound        producerId i32         groupId i32   producedPower f64              ts u64         total
   0.01%                +0.0%               +0.0%              -42.1%               +0.0%        -19.6%
    0.1%                +0.0%               +0.0%              -51.9%               +0.0%        -24.2%
    0.5%                +0.0%               +0.0%              -60.5%               +0.0%        -28.1%
    1.0%                +0.0%               +0.0%              -63.8%               +0.0%        -29.7%
    2.0%                +0.0%               +0.0%              -72.4%               +0.0%        -33.7%
    5.0%                +0.0%               +0.0%              -71.5%               +0.0%        -33.3%
   10.0%                +0.0%               +0.0%              -77.3%               +0.0%        -36.0%
   20.0%                +0.0%               +0.0%              -79.6%               +0.0%        -37.1%

float column only: codec chosen, and the error the encoder recorded
  bound      bytes   bytes/row   vs lossless        codec   worst recorded error
    0.0%    697,848        3.49          0.0%          Raw                      0
   0.01%    404,387        2.02        -42.1%    Quantized            9.99979e-05
    0.1%    335,378        1.68        -51.9%    Quantized            0.000999989
    0.5%    275,907        1.38        -60.5%    Quantized             0.00499984
    1.0%    252,612        1.26        -63.8%    Quantized             0.00999998
    2.0%    192,392        0.96        -72.4%    Quantized                   0.02
    5.0%    198,882        0.99        -71.5%    Quantized              0.0499992
   10.0%    158,442        0.79        -77.3%    Quantized              0.0999987
   20.0%    142,123        0.71        -79.6%    Quantized               0.199997
