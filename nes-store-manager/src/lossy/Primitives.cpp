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

#include <lossy/Primitives.hpp>

#include <algorithm>
#include <bit>
#include <cstring>
#include <iterator>
#include <ranges>

#include <ErrorHandling.hpp>

namespace NES::StoreManager::Lossy
{

uint8_t bitWidthFor(const uint64_t maxValue) noexcept
{
    return static_cast<uint8_t>(64 - std::countl_zero(maxValue));
}

void bitPack(const std::span<const uint64_t> values, const uint8_t bitWidth, std::vector<uint8_t>& out)
{
    PRECONDITION(bitWidth <= 64, "bit width {} exceeds 64", bitWidth);

    /// Every value is zero, so the packed form carries no bits at all -- the width in the
    /// header is the whole message.
    if (bitWidth == 0)
    {
        return;
    }

    /// Full width needs no shifting, and would overflow the accumulator arithmetic below.
    if (bitWidth == 64)
    {
        for (const uint64_t value : values)
        {
            for (int i = 0; i < 8; ++i)
            {
                out.push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xFFU));
            }
        }
        return;
    }

    const uint64_t mask = (uint64_t{1} << bitWidth) - 1;

    /// A 128-bit accumulator, not 64: bytes are flushed only down to a remainder of at most
    /// 7 bits, and the next value can add another 63, so 64 bits would silently drop the top
    /// of a wide value.
    __uint128_t accumulator = 0;
    int pendingBits = 0;

    for (const uint64_t value : values)
    {
        accumulator |= static_cast<__uint128_t>(value & mask) << pendingBits;
        pendingBits += bitWidth;
        while (pendingBits >= 8)
        {
            out.push_back(static_cast<uint8_t>(accumulator));
            accumulator >>= 8;
            pendingBits -= 8;
        }
    }

    if (pendingBits > 0)
    {
        out.push_back(static_cast<uint8_t>(accumulator));
    }
}

void bitUnpack(const std::span<const uint8_t> packed, const uint8_t bitWidth, const size_t count, const std::span<uint64_t> out)
{
    PRECONDITION(out.size() >= count, "output holds {} values, {} requested", out.size(), count);
    PRECONDITION(bitWidth <= 64, "bit width {} exceeds 64", bitWidth);

    if (bitWidth == 0)
    {
        std::fill_n(out.begin(), count, uint64_t{0});
        return;
    }

    if (bitWidth == 64)
    {
        PRECONDITION(packed.size() >= count * 8, "packed payload too short for {} 64-bit values", count);
        for (size_t i = 0; i < count; ++i)
        {
            uint64_t value = 0;
            for (int b = 0; b < 8; ++b)
            {
                value |= static_cast<uint64_t>(packed[(i * 8) + static_cast<size_t>(b)]) << (8 * b);
            }
            out[i] = value;
        }
        return;
    }

    PRECONDITION(
        packed.size() >= bitPackedSize(count, bitWidth),
        "packed payload is {} bytes, {} needed for {} values at {} bits",
        packed.size(),
        bitPackedSize(count, bitWidth),
        count,
        bitWidth);

    const uint64_t mask = (uint64_t{1} << bitWidth) - 1;
    __uint128_t accumulator = 0;
    int availableBits = 0;
    size_t byteIndex = 0;

    for (size_t i = 0; i < count; ++i)
    {
        while (availableBits < bitWidth)
        {
            accumulator |= static_cast<__uint128_t>(packed[byteIndex]) << availableBits;
            ++byteIndex;
            availableBits += 8;
        }
        out[i] = static_cast<uint64_t>(accumulator) & mask;
        accumulator >>= bitWidth;
        availableBits -= bitWidth;
    }
}

DeltaOfDeltaResult dodEncode(const std::span<const uint64_t> values)
{
    DeltaOfDeltaResult result;
    if (values.empty())
    {
        return result;
    }

    result.first = values[0];
    if (values.size() == 1)
    {
        return result;
    }

    /// Unsigned throughout, then reinterpreted: signed overflow is undefined, unsigned wraps,
    /// and wrapping is what makes the inverse exact.
    uint64_t previousDelta = values[1] - values[0];
    result.firstDelta = static_cast<int64_t>(previousDelta);

    result.dods.reserve(values.size() - 2);
    uint64_t maxCode = 0;
    for (size_t i = 2; i < values.size(); ++i)
    {
        const uint64_t delta = values[i] - values[i - 1];
        const uint64_t code = zigZagEncode(static_cast<int64_t>(delta - previousDelta));
        result.dods.push_back(code);
        maxCode = std::max(maxCode, code);
        previousDelta = delta;
    }
    result.bitWidth = bitWidthFor(maxCode);
    return result;
}

void dodDecode(const DeltaOfDeltaResult& encoded, const size_t count, const std::span<uint64_t> out)
{
    PRECONDITION(out.size() >= count, "output holds {} values, {} requested", out.size(), count);
    if (count == 0)
    {
        return;
    }

    out[0] = encoded.first;
    if (count == 1)
    {
        return;
    }

    PRECONDITION(encoded.dods.size() >= count - 2, "have {} second differences, {} values requested", encoded.dods.size(), count);

    uint64_t delta = static_cast<uint64_t>(encoded.firstDelta);
    out[1] = out[0] + delta;
    for (size_t i = 2; i < count; ++i)
    {
        delta += static_cast<uint64_t>(zigZagDecode(encoded.dods[i - 2]));
        out[i] = out[i - 1] + delta;
    }
}

DictionaryResult dictEncode(const std::span<const uint64_t> values)
{
    DictionaryResult result;
    if (values.empty())
    {
        return result;
    }

    result.dictionary.assign(values.begin(), values.end());
    std::ranges::sort(result.dictionary);
    const auto duplicates = std::ranges::unique(result.dictionary);
    result.dictionary.erase(duplicates.begin(), duplicates.end());

    result.codes.reserve(values.size());
    for (const uint64_t value : values)
    {
        const auto position = std::ranges::lower_bound(result.dictionary, value);
        result.codes.push_back(static_cast<uint64_t>(std::distance(result.dictionary.begin(), position)));
    }

    /// One distinct value means every code is 0, which bit-packs to nothing at all.
    result.bitWidth = bitWidthFor(result.dictionary.size() - 1);
    return result;
}

void dictDecode(const DictionaryResult& encoded, const std::span<uint64_t> out)
{
    for (size_t i = 0; i < out.size(); ++i)
    {
        PRECONDITION(
            encoded.codes[i] < encoded.dictionary.size(),
            "dictionary code {} is out of range for a {}-entry dictionary",
            encoded.codes[i],
            encoded.dictionary.size());
        out[i] = encoded.dictionary[encoded.codes[i]];
    }
}

RunLengthResult rleEncode(const std::span<const uint64_t> values)
{
    RunLengthResult result;
    for (const uint64_t value : values)
    {
        if (!result.values.empty() && result.values.back() == value)
        {
            ++result.runLengths.back();
            continue;
        }
        result.values.push_back(value);
        result.runLengths.push_back(1);
    }
    return result;
}

void rleDecode(const RunLengthResult& encoded, const size_t count, const std::span<uint64_t> out)
{
    PRECONDITION(out.size() >= count, "output holds {} values, {} requested", out.size(), count);
    PRECONDITION(
        encoded.values.size() == encoded.runLengths.size(),
        "{} run values against {} run lengths",
        encoded.values.size(),
        encoded.runLengths.size());

    size_t written = 0;
    for (size_t run = 0; run < encoded.values.size(); ++run)
    {
        PRECONDITION(
            written + encoded.runLengths[run] <= count, "run lengths sum past the {} values requested", count);
        std::fill_n(out.begin() + static_cast<std::ptrdiff_t>(written), encoded.runLengths[run], encoded.values[run]);
        written += encoded.runLengths[run];
    }

    PRECONDITION(written == count, "run lengths sum to {}, {} values requested", written, count);
}

uint64_t loadUnsigned(const uint8_t* src, const uint32_t width) noexcept
{
    uint64_t value = 0;
    std::memcpy(&value, src, width);
    return value;
}

int64_t loadSigned(const uint8_t* src, const uint32_t width) noexcept
{
    uint64_t value = 0;
    std::memcpy(&value, src, width);
    if (width < 8)
    {
        const uint32_t bits = width * 8;
        const uint64_t signBit = uint64_t{1} << (bits - 1);
        if ((value & signBit) != 0)
        {
            value |= ~((uint64_t{1} << bits) - 1);
        }
    }
    return static_cast<int64_t>(value);
}

void storeLow(uint8_t* dest, const uint64_t value, const uint32_t width) noexcept
{
    std::memcpy(dest, &value, width);
}

}
