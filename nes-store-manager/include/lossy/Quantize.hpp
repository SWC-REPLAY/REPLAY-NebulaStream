/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace NES::Lossy
{

/// The one place in this store where information is lost.
///
/// Everything else -- frame of reference, bit-packing, the column layout itself -- is
/// bijective, so the accuracy of the whole pipeline is decided by the single rounding step
/// below and by nothing after it. That is the reason the lossy step is kept alone in its own
/// file: the boundary is worth being able to see.
///
/// ## What the bound means
///
/// The bound is given as a percentage X and is **pointwise relative**: every value comes back
/// within X% of itself.
///
///     |v - v'| / |v|  <=  X / 100          for every value, independently
///
/// It is not a percentage of the data lost, and not a percentage of anything the column as a
/// whole happens to contain. A reading of 23.456 at 1% comes back inside [23.221, 23.691],
/// whatever else is in the column and whichever group it landed in.
///
/// ## How a uniform grid delivers a relative bound
///
/// Quantizing the value directly gives a bound relative to the grid step, which is absolute.
/// Quantizing its **logarithm** gives a bound relative to the value, because a uniform grid in
/// log space is a multiplicative grid in linear space:
///
///     deltaLog = 2 * ln(1 + X/100)
///     code     = round((ln|v| - logMin) / deltaLog)
///     v'       = sign(v) * exp(logMin + code * deltaLog)
///
///     |ln|v| - ln|v'||  <=  deltaLog/2   (rounding to nearest)
///     => |v'/v| = e^d with |d| <= ln(1 + X/100)
///     => |v - v'| / |v|  <=  X/100
///
/// This is what SZ calls its point-wise relative mode, and it is implemented the same way:
/// take the log, apply an absolute-error grid, exponentiate on the way back.
///
/// ## Why relative to the value rather than to the block's range
///
/// A bound taken from the block's range makes a value's accuracy depend on what else happened
/// to be in the same 8192 rows -- the same reading is treated differently in two groups, and
/// one outlier coarsens everything around it. Worse, on a column spanning 0.001 to 1000, 1% of
/// the range is 10, so a reading of 0.5 may come back as -9.5 and the bound is still met. The
/// promise sounds strong and is not one.
///
/// Relative to the value, the guarantee holds per value and nothing about the block enters it:
/// `deltaLog` is a function of X alone, so every group computes the same step.
///
/// ## What this costs in width
///
/// The code count is ln(max|v| / min|v|) / deltaLog -- the column's *dynamic range* in decades,
/// not its span. Narrow columns get cheaper than a range-relative grid would be (temperatures
/// 20-30 at 1% need 5 bits, against 6), and columns spanning decades get dearer, which is
/// exactly where a range-relative bound was quietly failing to deliver anything.

struct QuantizedColumn
{
    /// The smallest finite non-zero magnitude in the block: the frame of reference the codes
    /// are offsets from, in log space.
    ///
    /// Kept as the magnitude rather than as its logarithm so that reconstruction is
    /// `minMagnitude * exp(code * deltaLog)`. A code of zero then returns the original value
    /// bit for bit instead of `exp(log(v))`, which differs from it by an ulp -- so a column
    /// whose values are all equal round-trips exactly rather than merely within bound.
    double minMagnitude{0.0};

    /// Grid step in log space, 2 * ln(1 + X/100). A function of the requested bound alone --
    /// no property of the data appears in it, which is what makes the guarantee independent of
    /// where group boundaries fall.
    double deltaLog{0.0};

    /// The bound that was asked for, as a fraction rather than a percentage.
    double requestedRelative{0.0};

    /// The largest |v - v'| / |v| actually observed, computed by reconstructing every value
    /// exactly as decoding will, narrowing to float included. Never inferred from `deltaLog`:
    /// rounding in the reconstruction can move the real figure either way, and a bound nobody
    /// measured is not a bound.
    double achievedError{0.0};

    uint8_t bitWidth{0};

    /// True when the block holds nothing that can go on the grid -- every value zero, or
    /// non-finite, or both. No codes are produced; the exception list carries the column.
    bool isConstant{false};

    /// Grid indices, one per row. Zero at exception positions, where they are never read.
    std::vector<uint64_t> codes;

    /// One bit per row, set when the value is negative. The grid holds magnitudes, so the sign
    /// is carried beside it and survives exactly. Empty when the column has no negative values,
    /// which costs nothing.
    std::vector<uint8_t> signBits;

    /// One bit per row, marking values held out of the grid entirely.
    ///
    /// Zero has no logarithm, and NaN and the infinities cannot be rounded onto a grid at all.
    /// All three are set aside and stored verbatim, so they survive the round trip **exactly**
    /// -- their error is zero rather than merely within bound. Empty when there are none.
    std::vector<uint8_t> exceptionBits;

    /// Raw bit patterns of the held-out values, in row order.
    std::vector<uint64_t> exceptionPatterns;
};

/// @param values                 the column, widened to double
/// @param relativeBoundPercent   X in the formulas above; must be > 0
/// @param singlePrecision        true when the values will be written back as float, so that
///                               `achievedError` accounts for the narrowing too
[[nodiscard]] QuantizedColumn quantize(std::span<const double> values, double relativeBoundPercent, bool singlePrecision);

/// Reconstruct `rowCount` values. `out` must already have room for them.
void dequantize(const QuantizedColumn& quantized, size_t rowCount, std::span<double> out);

}
