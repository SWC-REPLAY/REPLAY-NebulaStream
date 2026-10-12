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

namespace NES::Lossy
{

namespace
{
// Reconstruct exactly as decoding will, sign and narrowing included.
double reconstruct(
    const double minMagnitude, const double deltaLog, const uint64_t code, const bool negative, const bool singlePrecision)
{
    const double magnitude = minMagnitude * std::exp(static_cast<double>(code) * deltaLog);
    const double value = negative ? -magnitude : magnitude;
    return singlePrecision ? static_cast<double>(static_cast<float>(value)) : value;
}

// Zero, NaN and the infinities cannot go on the grid; held out and stored verbatim.
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

    // One pass: separate grid-able values from exceptions and find the log-space reference.
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

    // Nothing to put on a grid; the exception list already holds every value exactly.
    if (quantizable == 0)
    {
        result.isConstant = true;
        result.achievedError = 0.0;
        return result;
    }

    // Grid step depends only on the requested bound.
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
            continue; // stored verbatim; its code is never read
        }

        const auto code = static_cast<uint64_t>(std::llround((std::log(std::abs(value)) - logMin) / deltaLog));
        result.codes[i] = code;
        maxCode = std::max(maxCode, code);

        const double restored = reconstruct(minMagnitude, deltaLog, code, value < 0.0, singlePrecision);
        achieved = std::max(achieved, std::abs(value - restored) / std::abs(value));
    }

    result.bitWidth = bitWidthFor(maxCode);
    result.achievedError = achieved;

    // The bound, checked in production rather than trusted; slack absorbs log/exp rounding.
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
