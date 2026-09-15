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

#include <lossy/Quantize.hpp>

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>

#include <ErrorHandling.hpp>
#include <lossy/Primitives.hpp>

namespace NES::StoreManager::Lossy
{

namespace
{
/// Reconstruct exactly the way decoding will, sign and narrowing included, so the measured
/// error is the error a reader will actually see.
double reconstruct(
    const double minMagnitude, const double deltaLog, const uint64_t code, const bool negative, const bool singlePrecision)
{
    const double magnitude = minMagnitude * std::exp(static_cast<double>(code) * deltaLog);
    const double value = negative ? -magnitude : magnitude;
    return singlePrecision ? static_cast<double>(static_cast<float>(value)) : value;
}

/// Zero has no logarithm; NaN and the infinities have no place on a grid. All three are held
/// out and stored as they arrived.
bool goesOnTheGrid(const double value) noexcept
{
    return std::isfinite(value) && value != 0.0;
}

void setBit(std::vector<uint8_t>& bits, const size_t index, const size_t rowCount)
{
    if (bits.empty())
    {
        bits.assign((rowCount + 7) / 8, 0);
    }
    bits[index / 8] |= static_cast<uint8_t>(1U << (index % 8));
}

bool testBit(const std::vector<uint8_t>& bits, const size_t index) noexcept
{
    return !bits.empty() && (bits[index / 8] & (1U << (index % 8))) != 0;
}
}

QuantizedColumn quantize(const std::span<const double> values, const double relativeBoundPercent, const bool singlePrecision)
{
    PRECONDITION(relativeBoundPercent > 0.0, "quantize called with a non-positive bound ({})", relativeBoundPercent);

    QuantizedColumn result;
    result.requestedRelative = relativeBoundPercent / 100.0;

    /// One pass to separate what can be quantized from what cannot, and to find the frame of
    /// reference in log space.
    double minMagnitude = std::numeric_limits<double>::infinity();
    double maxMagnitude = 0.0;
    uint64_t quantizable = 0;

    for (size_t i = 0; i < values.size(); ++i)
    {
        const double value = values[i];
        if (!goesOnTheGrid(value))
        {
            setBit(result.exceptionBits, i, values.size());
            result.exceptionPatterns.push_back(std::bit_cast<uint64_t>(value));
            continue;
        }

        if (value < 0.0)
        {
            setBit(result.signBits, i, values.size());
        }
        minMagnitude = std::min(minMagnitude, std::abs(value));
        maxMagnitude = std::max(maxMagnitude, std::abs(value));
        ++quantizable;
    }

    /// Nothing to put on a grid. The exception list already holds every value exactly, so the
    /// column is complete without any codes. This happens for real -- an all-zero column, or a
    /// gap in a sensor feed -- and is a normal path, not an error.
    if (quantizable == 0)
    {
        result.isConstant = true;
        result.achievedError = 0.0;
        return result;
    }

    /// The step depends on the requested bound and on nothing else. That is the whole point:
    /// every group computes the same step, so a value's accuracy does not depend on which
    /// group it landed in or on what else shared it.
    const double deltaLog = 2.0 * std::log1p(relativeBoundPercent / 100.0);

    const double logMin = std::log(minMagnitude);
    const double logSpan = std::log(maxMagnitude) - logMin;

    PRECONDITION(
        logSpan / deltaLog < 9e18,
        "a bound of {}% is too tight for a column spanning {} decades; the grid index would overflow",
        relativeBoundPercent,
        logSpan / std::log(10.0));

    result.minMagnitude = minMagnitude;
    result.deltaLog = deltaLog;
    result.codes.assign(values.size(), 0);

    uint64_t maxCode = 0;
    double achieved = 0.0;

    for (size_t i = 0; i < values.size(); ++i)
    {
        const double value = values[i];
        if (!goesOnTheGrid(value))
        {
            continue; /// stored verbatim; its code is never read
        }

        const auto code = static_cast<uint64_t>(std::llround((std::log(std::abs(value)) - logMin) / deltaLog));
        result.codes[i] = code;
        maxCode = std::max(maxCode, code);

        const double restored = reconstruct(minMagnitude, deltaLog, code, value < 0.0, singlePrecision);
        achieved = std::max(achieved, std::abs(value - restored) / std::abs(value));
    }

    result.bitWidth = bitWidthFor(maxCode);
    result.achievedError = achieved;

    /// The guarantee, checked rather than trusted. Kept in production code, not just in tests:
    /// if the arithmetic above ever stops holding, silently handing back data that misses its
    /// stated accuracy is the worst possible failure for a store whose whole contract is that
    /// bound. The slack absorbs the rounding of log and exp themselves, nothing more -- a
    /// FLOAT32 column asked for a bound finer than the type can represent is meant to fail.
    const double slack = 8.0 * std::numeric_limits<double>::epsilon();
    INVARIANT(
        achieved <= result.requestedRelative + slack,
        "relative quantization error {} exceeds the requested bound {} (deltaLog {})",
        achieved,
        result.requestedRelative,
        deltaLog);

    return result;
}

void dequantize(const QuantizedColumn& quantized, const size_t rowCount, const std::span<double> out)
{
    PRECONDITION(out.size() >= rowCount, "output holds {} values, {} requested", out.size(), rowCount);

    size_t nextException = 0;
    for (size_t i = 0; i < rowCount; ++i)
    {
        if (testBit(quantized.exceptionBits, i))
        {
            PRECONDITION(
                nextException < quantized.exceptionPatterns.size(),
                "exception bitmap marks more values than the {} patterns stored",
                quantized.exceptionPatterns.size());
            out[i] = std::bit_cast<double>(quantized.exceptionPatterns[nextException]);
            ++nextException;
            continue;
        }

        const double magnitude = quantized.minMagnitude * std::exp(static_cast<double>(quantized.codes[i]) * quantized.deltaLog);
        out[i] = testBit(quantized.signBits, i) ? -magnitude : magnitude;
    }
}

}
