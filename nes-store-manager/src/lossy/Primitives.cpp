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

#include <ErrorHandling.hpp>

namespace NES::Lossy
{

uint8_t bitWidthFor(const uint64_t maxValue) noexcept
{
    return static_cast<uint8_t>(64 - std::countl_zero(maxValue));
}

void bitPack(const std::span<const uint64_t> values, const uint8_t bitWidth, std::vector<uint8_t>& out)
{
    PRECONDITION(bitWidth <= 64, "bit width {} exceeds 64", bitWidth);

    // Width zero: every value is zero, so nothing is written.
    if (bitWidth == 0)
    {
        return;
    }

    // Full width: copy whole; the accumulator path below would overflow.
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

    // 128-bit accumulator: up to 7 leftover bits plus a 63-bit value would overflow 64.
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
