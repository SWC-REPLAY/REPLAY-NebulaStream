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

/// Building blocks the codecs are assembled from. All lossless, all pure functions.
///
/// None of them compresses on its own -- bit-packing shrinks values only once something has
/// made them small, and frame-of-reference makes them small only once they are packed. They
/// are useful in combination, which is why codecs, not these, are the unit the store deals
/// in.

/// Bits needed to represent every value in [0, maxValue]. Zero when maxValue is zero, which
/// is the legitimate "every value is identical, store nothing" case.
[[nodiscard]] uint8_t bitWidthFor(uint64_t maxValue) noexcept;

/// Bytes `count` values of `bitWidth` bits each occupy once packed.
[[nodiscard]] constexpr size_t bitPackedSize(const size_t count, const uint8_t bitWidth) noexcept
{
    return ((count * bitWidth) + 7) / 8;
}

/// Append `values` to `out`, each truncated to its low `bitWidth` bits, with no padding
/// between them and no alignment to byte boundaries.
///
/// This is where the space is actually saved: every other lossless encoding here exists to
/// make values small enough that a narrow `bitWidth` suffices.
void bitPack(std::span<const uint64_t> values, uint8_t bitWidth, std::vector<uint8_t>& out);

/// Inverse of bitPack. `out` must already have room for `count` values.
void bitUnpack(std::span<const uint8_t> packed, uint8_t bitWidth, size_t count, std::span<uint64_t> out);

/// Map signed to unsigned so that small magnitudes stay small.
///
/// Needed because bit-packing counts leading zeros, and in two's complement -1 is all ones:
/// packed as-is it would cost 64 bits despite being tiny. Zig-zag interleaves the signs
/// (0, -1, 1, -2, 2 -> 0, 1, 2, 3, 4), so magnitude, not sign, decides the width.
[[nodiscard]] constexpr uint64_t zigZagEncode(const int64_t n) noexcept
{
    return (static_cast<uint64_t>(n) << 1U) ^ static_cast<uint64_t>(n >> 63);
}

[[nodiscard]] constexpr int64_t zigZagDecode(const uint64_t u) noexcept
{
    return static_cast<int64_t>(u >> 1U) ^ -static_cast<int64_t>(u & 1U);
}

/// Second difference of a sequence: how much the step between neighbours changed.
///
/// The point is what happens to a regular stream. A sensor emitting every 100ms has deltas
/// that are all 100, so every second difference is 0, so `bitWidth` is 0 and the whole column
/// costs its header and nothing else. Plain deltas cannot do that -- they would still spend
/// 7 bits per row storing 100 over and over.
struct DeltaOfDeltaResult
{
    /// Kept raw; the sequence is rebuilt from these two and nothing else.
    uint64_t first{0};
    int64_t firstDelta{0};

    /// Zig-zagged second differences, one per value from index 2 onwards.
    std::vector<uint64_t> dods;
    uint8_t bitWidth{0};
};

/// All arithmetic is modulo 2^64, so the round trip is exact even when the differences
/// overflow -- which they do for adjacent values at opposite ends of the range. Narrower
/// columns are passed in zero-extended and truncated on the way back out; that stays exact
/// for the same reason.
[[nodiscard]] DeltaOfDeltaResult dodEncode(std::span<const uint64_t> values);

/// `out` must already have room for `count` values.
void dodDecode(const DeltaOfDeltaResult& encoded, size_t count, std::span<uint64_t> out);

/// Distinct values pulled out into a table, leaving an index per row.
///
/// Worth it exactly when the values are wide and few: a column of 8-byte device ids with 200
/// distinct entries becomes 8 bits per row plus a 1.6 KB table. Frame-of-reference cannot help
/// there, because the ids are spread across the whole range however few of them there are.
struct DictionaryResult
{
    /// Ascending, no duplicates.
    ///
    /// Sorted for two reasons: the table itself then compresses with frame-of-reference, and
    /// the codes preserve the ordering of the values, which is what a future predicate
    /// pushdown would need. The second holds only for unsigned columns -- these are raw bit
    /// patterns, so a negative signed value sorts above every positive one.
    std::vector<uint64_t> dictionary;

    /// One index into `dictionary` per input value.
    std::vector<uint64_t> codes;
    uint8_t bitWidth{0};
};

[[nodiscard]] DictionaryResult dictEncode(std::span<const uint64_t> values);

void dictDecode(const DictionaryResult& encoded, std::span<uint64_t> out);

/// Consecutive equal values collapsed into one value plus a repeat count.
///
/// Unlike the others this can make a column *bigger*: a sequence with no repeats pays a run
/// length for every single value. It is only ever applied once something has established that
/// the runs are long -- see the break-even condition at its call site.
struct RunLengthResult
{
    std::vector<uint64_t> values;
    std::vector<uint64_t> runLengths;
};

[[nodiscard]] RunLengthResult rleEncode(std::span<const uint64_t> values);

/// `out` must already have room for `count` values.
void rleDecode(const RunLengthResult& encoded, size_t count, std::span<uint64_t> out);

/// Read a `width`-byte little-endian value, zero-extended.
[[nodiscard]] uint64_t loadUnsigned(const uint8_t* src, uint32_t width) noexcept;

/// Read a `width`-byte little-endian value, sign-extended.
[[nodiscard]] int64_t loadSigned(const uint8_t* src, uint32_t width) noexcept;

/// Write the low `width` bytes of `value`, little-endian.
void storeLow(uint8_t* dest, uint64_t value, uint32_t width) noexcept;

}
