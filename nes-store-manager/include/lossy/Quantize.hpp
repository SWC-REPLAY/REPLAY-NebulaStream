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

// The one place in this store where information is lost.

// The bound is given as a percentage X and is **pointwise relative**: every value comes back
// within X% of itself.
//     |v - v'| / |v|  <=  X / 100          for every value, independently
// It is not a percentage of the data lost, and not a percentage of anything the column as a
// whole happens to contain. A reading of 23.456 at 1% comes back inside [23.221, 23.691],


// Quantizing its **logarithm** gives a bound relative to the value
//
//    deltaLog = 2 * ln(1 + X/100)
//    code     = round((ln|v| - logMin) / deltaLog)
//     v'       = sign(v) * exp(logMin + code * deltaLog)
//     |ln|v| - ln|v'||  <=  deltaLog/2   (rounding to nearest)
//    => |v'/v| = e^d with |d| <= ln(1 + X/100)
//    => |v - v'| / |v|  <=  X/100

struct QuantizedColumn
{

    double minMagnitude{0.0};


    double deltaLog{0.0};

    /// The bound that was asked for, as a fraction rather than a percentage.
    double requestedRelative{0.0};

    double achievedError{0.0};

    uint8_t bitWidth{0};

    //True when the block holds nothing that can go on the grid -- every value zero, or
    //non-finite, or both. No codes are produced; the exception list carries the column.
    bool isConstant{false};

    //Grid indices, one per row. Zero at exception positions, where they are never read.
    std::vector<uint64_t> codes;

    // One bit per row, set when the value is negative. The grid holds magnitudes, so the sign
    // is carried beside it and survives exactly. Empty when the column has no negative values,
    // which costs nothing.
    std::vector<uint8_t> signBits;

    // One bit per row, marking values held out of the grid entirely
    //Zero has no logarithm, and NaN and the infinities cannot be rounded onto a grid at all.

    std::vector<uint8_t> exceptionBits;

    //Raw bit patterns of the held-out values, in row order.
    std::vector<uint64_t> exceptionPatterns;
};

/// @param values                 the column, widened to double
/// @param relativeBoundPercent   X in the formulas above; must be > 0
/// @param singlePrecision        true when the values will be written back as float, so that
///                               `achievedError` accounts for the narrowing too
[[nodiscard]] QuantizedColumn quantize(std::span<const double> values, double relativeBoundPercent, bool singlePrecision);

void dequantize(const QuantizedColumn& quantized, size_t rowCount, std::span<double> out);

}
