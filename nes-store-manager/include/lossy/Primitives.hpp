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

// Building blocks the codecs are assembled from. All lossless, all pure functions.

[[nodiscard]] uint8_t bitWidthFor(uint64_t maxValue) noexcept;

// Bytes `count` values of `bitWidth` bits each occupy once packed.
[[nodiscard]] constexpr size_t bitPackedSize(const size_t count, const uint8_t bitWidth) noexcept
{
    return ((count * bitWidth) + 7) / 8;
}


void bitPack(std::span<const uint64_t> values, uint8_t bitWidth, std::vector<uint8_t>& out);


void bitUnpack(std::span<const uint8_t> packed, uint8_t bitWidth, size_t count, std::span<uint64_t> out);

// Map signed to unsigned so that small magnitudes stay small. Needed because bit-packing counts leading zeros, and in two's complement -1 is all ones
[[nodiscard]] constexpr uint64_t zigZagEncode(const int64_t n) noexcept
{
    return (static_cast<uint64_t>(n) << 1U) ^ static_cast<uint64_t>(n >> 63);
}

[[nodiscard]] constexpr int64_t zigZagDecode(const uint64_t u) noexcept
{
    return static_cast<int64_t>(u >> 1U) ^ -static_cast<int64_t>(u & 1U);
}

// Read a `width`-byte little-endian value, zero-extended.
[[nodiscard]] uint64_t loadUnsigned(const uint8_t* src, uint32_t width) noexcept;

// Read a `width`-byte little-endian value, sign-extended.
[[nodiscard]] int64_t loadSigned(const uint8_t* src, uint32_t width) noexcept;

// Write the low `width` bytes of `value`, little-endian.
void storeLow(uint8_t* dest, uint64_t value, uint32_t width) noexcept;

}
