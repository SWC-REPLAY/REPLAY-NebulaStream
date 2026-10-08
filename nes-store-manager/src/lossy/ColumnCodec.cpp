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

#include <StoreProfile.hpp>
#include <lossy/ColumnCodec.hpp>

#include <algorithm>
#include <bit>
#include <cstring>
#include <ranges>

#include <lossy/Primitives.hpp>
#include <lossy/Quantize.hpp>

#include <ErrorHandling.hpp>

namespace NES::Lossy
{

namespace
{

/// Little-endian fixed-width fields, for the codec headers below.
void putU32(std::vector<uint8_t>& out, const uint32_t value)
{
    for (int i = 0; i < 4; ++i)
    {
        out.push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xFFU));
    }
}

uint32_t getU32(const uint8_t* src) noexcept
{
    uint32_t value = 0;
    for (int i = 0; i < 4; ++i)
    {
        value |= static_cast<uint32_t>(src[i]) << (8 * i);
    }
    return value;
}

void putU64(std::vector<uint8_t>& out, const uint64_t value)
{
    for (int i = 0; i < 8; ++i)
    {
        out.push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xFFU));
    }
}

uint64_t getU64(const uint8_t* src) noexcept
{
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i)
    {
        value |= static_cast<uint64_t>(src[i]) << (8 * i);
    }
    return value;
}

/// Flags and fixed-width fields of the quantization header, shared by the one-piece codec
/// below and by the split form the store uses when a second codec encodes the codes.
constexpr uint8_t FLAG_CONSTANT = 1U << 0U;
constexpr uint8_t FLAG_EXCEPTIONS = 1U << 1U;
constexpr uint8_t FLAG_SIGNS = 1U << 2U;
constexpr size_t QUANTIZE_FIXED_HEADER_BYTES = 1 + 8 + 8 + 1;

void putDouble(std::vector<uint8_t>& out, const double value)
{
    const auto bits = std::bit_cast<uint64_t>(value);
    for (int i = 0; i < 8; ++i)
    {
        out.push_back(static_cast<uint8_t>((bits >> (8 * i)) & 0xFFU));
    }
}

double getDouble(const uint8_t* src)
{
    uint64_t bits = 0;
    for (int i = 0; i < 8; ++i)
    {
        bits |= static_cast<uint64_t>(src[i]) << (8 * i);
    }
    return std::bit_cast<double>(bits);
}

/// Read a column into the flat u64 array the primitives work on.
///
/// Always zero-extended, never sign-extended, and that is deliberate: every codec below either
/// differences these values or looks them up in a table, and both are exact modulo 2^width.
/// Truncating back through storeLow() therefore reproduces the original bytes whether the
/// column was signed or not, so none of them has to care.
std::vector<uint64_t> loadColumn(const ColumnValues& in)
{
    std::vector<uint64_t> values(in.rowCount);
    for (uint64_t r = 0; r < in.rowCount; ++r)
    {
        values[r] = loadUnsigned(in.data + (r * in.valueWidth), in.valueWidth);
    }
    return values;
}

void storeColumn(std::span<const uint64_t> values, const MutableColumnValues& out)
{
    for (uint64_t r = 0; r < out.rowCount; ++r)
    {
        storeLow(out.data + (r * out.valueWidth), values[r], out.valueWidth);
    }
}

/// Stores a column exactly as it arrived.
///
/// Two jobs. It is the fallback for any type no other codec claims, and it is the identity
/// element the whole pipeline is measured against: a store using only this codec must
/// reproduce its input byte for byte. Every codec added later has to keep that test passing.
class RawCodec final : public ColumnCodec
{
public:
    [[nodiscard]] EncodedColumn encode(const ColumnValues& in, double) const override
    {
        return EncodedColumn{.bytes = {in.data, in.data + in.byteSize()}, .achievedError = 0.0};
    }

    void decode(const std::span<const uint8_t> bytes, const MutableColumnValues& out) const override
    {
        PRECONDITION(
            bytes.size() == out.byteSize(), "encoded column is {} bytes, output expects {}", bytes.size(), out.byteSize());
        if (!bytes.empty())
        {
            std::memcpy(out.data, bytes.data(), bytes.size());
        }
    }

    [[nodiscard]] CodecId id() const noexcept override { return CodecId::Raw; }

    [[nodiscard]] bool supports(const DataType::Type type) const noexcept override { return isSupportedType(type); }

    [[nodiscard]] bool isLossy() const noexcept override { return false; }
};

/// Subtracts the block's minimum from every value and bit-packs the remainders.
///
/// Both halves are needed. Bit-packing alone helps only when values are already small, and a
/// column of Unix millisecond timestamps needs 41 bits however narrow its span is. Subtracting
/// the minimum first turns absolute magnitude into spread, and it is the spread within a
/// group that is usually tiny -- so the two together turn "large numbers close to each other"
/// into a few bits per value.
///
/// Layout: [reference: 8 bytes][bitWidth: 1 byte][packed remainders].
///
/// Its weakness is worth naming: a single outlier widens every value in the group, because
/// one width covers them all. That is what PFOR-style exception lists exist to fix, and what
/// keeping groups small already limits the blast radius of.
class ForBitpackCodec final : public ColumnCodec
{
    static constexpr size_t HEADER_BYTES = sizeof(uint64_t) + 1;

public:
    [[nodiscard]] EncodedColumn encode(const ColumnValues& in, double) const override
    {
        PRECONDITION(supports(in.type), "ForBitpackCodec cannot encode DataType::Type {}", static_cast<int>(in.type));

        const bool isSigned = isSignedType(in.type);
        std::vector<uint64_t> offsets(in.rowCount);

        /// The reference is kept as a raw bit pattern rather than as a number: the remainder
        /// is computed in unsigned arithmetic, which wraps to the right answer for signed
        /// inputs too, so nothing downstream needs to know the column's signedness.
        uint64_t reference = 0;
        if (in.rowCount > 0)
        {
            if (isSigned)
            {
                int64_t minValue = loadSigned(in.data, in.valueWidth);
                for (uint64_t r = 1; r < in.rowCount; ++r)
                {
                    minValue = std::min(minValue, loadSigned(in.data + (r * in.valueWidth), in.valueWidth));
                }
                reference = static_cast<uint64_t>(minValue);
                for (uint64_t r = 0; r < in.rowCount; ++r)
                {
                    offsets[r] = static_cast<uint64_t>(loadSigned(in.data + (r * in.valueWidth), in.valueWidth)) - reference;
                }
            }
            else
            {
                uint64_t minValue = loadUnsigned(in.data, in.valueWidth);
                for (uint64_t r = 1; r < in.rowCount; ++r)
                {
                    minValue = std::min(minValue, loadUnsigned(in.data + (r * in.valueWidth), in.valueWidth));
                }
                reference = minValue;
                for (uint64_t r = 0; r < in.rowCount; ++r)
                {
                    offsets[r] = loadUnsigned(in.data + (r * in.valueWidth), in.valueWidth) - reference;
                }
            }
        }

        uint64_t maxOffset = 0;
        for (const uint64_t offset : offsets)
        {
            maxOffset = std::max(maxOffset, offset);
        }
        const uint8_t bitWidth = bitWidthFor(maxOffset);

        std::vector<uint8_t> out;
        out.reserve(HEADER_BYTES + bitPackedSize(in.rowCount, bitWidth));
        for (int i = 0; i < 8; ++i)
        {
            out.push_back(static_cast<uint8_t>((reference >> (8 * i)) & 0xFFU));
        }
        out.push_back(bitWidth);
        bitPack(offsets, bitWidth, out);
        return EncodedColumn{.bytes = std::move(out), .achievedError = 0.0};
    }

    void decode(const std::span<const uint8_t> bytes, const MutableColumnValues& out) const override
    {
        PRECONDITION(bytes.size() >= HEADER_BYTES, "encoded column is {} bytes, header alone needs {}", bytes.size(), HEADER_BYTES);

        uint64_t reference = 0;
        for (int i = 0; i < 8; ++i)
        {
            reference |= static_cast<uint64_t>(bytes[static_cast<size_t>(i)]) << (8 * i);
        }
        const uint8_t bitWidth = bytes[sizeof(uint64_t)];

        std::vector<uint64_t> offsets(out.rowCount);
        bitUnpack(bytes.subspan(HEADER_BYTES), bitWidth, out.rowCount, offsets);

        for (uint64_t r = 0; r < out.rowCount; ++r)
        {
            storeLow(out.data + (r * out.valueWidth), reference + offsets[r], out.valueWidth);
        }
    }

    [[nodiscard]] CodecId id() const noexcept override { return CodecId::ForBitpack; }

    [[nodiscard]] bool supports(const DataType::Type type) const noexcept override { return isIntegerLike(type); }

    [[nodiscard]] bool isLossy() const noexcept override { return false; }
};

/// Rounds every value onto a uniform grid and bit-packs the grid indices.
///
/// The only codec here that loses anything, and it loses it in exactly one place: the
/// `llround` inside quantize(). Everything downstream of that -- the packing, the group
/// layout -- is bijective, so this codec's measured error is the error of the whole store.
///
/// Layout: [flags: 1][minValue: 8][delta: 8][bitWidth: 1]
///         (if exceptions) [count: 4][bitmap: ceil(n/8)][patterns: count * 8]
///         (unless constant) [packed codes]
///
/// The reason it pays: the number of codes is `range / delta = 50 / X`, in which the range
/// cancels. A double column asked for 1% needs 6 bits per value whatever it contains.
class QuantizedCodec final : public ColumnCodec
{
public:
    [[nodiscard]] EncodedColumn encode(const ColumnValues& in, const double relativeBoundPercent) const override
    {
        /// The two stages the lossy codec is made of, timed apart: quantising maps magnitudes
        /// onto a log grid, bit-packing writes the resulting codes at the group's width.
        ///
        /// The lambda is what BOUNDS the timer. A scope guard declared plainly here would run to
        /// the end of this function and swallow the bit-pack too -- the very stage it is meant to
        /// be measured against.
        const auto quantized = [&]
        {
            NES_PROFILE_SCOPE(quantizeNs);
            return quantizeColumn(in, relativeBoundPercent);
        }();

        std::vector<uint8_t> out;
        writeQuantizeHeader(out, quantized);
        if (!quantized.isConstant)
        {
            NES_PROFILE_SCOPE(bitpackNs);
            bitPack(quantized.codes, quantized.bitWidth, out);
        }
        return EncodedColumn{.bytes = std::move(out), .achievedError = quantized.achievedError};
    }

    void decode(const std::span<const uint8_t> bytes, const MutableColumnValues& out) const override
    {
        size_t offset = 0;
        auto quantized = readQuantizeHeader(bytes, offset, out.rowCount);
        if (!quantized.isConstant)
        {
            quantized.codes.resize(out.rowCount);
            bitUnpack(bytes.subspan(offset), quantized.bitWidth, out.rowCount, quantized.codes);
        }
        dequantizeColumn(quantized, out);
    }

    [[nodiscard]] CodecId id() const noexcept override { return CodecId::Quantized; }

    [[nodiscard]] bool supports(const DataType::Type type) const noexcept override
    {
        return type == DataType::Type::FLOAT32 || type == DataType::Type::FLOAT64;
    }

    [[nodiscard]] bool isLossy() const noexcept override { return true; }
};

/// Stores how much the gap between neighbours changed, and bit-packs that.
///
/// For the column this exists for -- the declared time field -- it is the difference between
/// paying per row and paying nothing. A stream arriving at a fixed rate has a constant gap, so
/// every second difference is zero, so the bit width is zero and the packed payload is empty:
/// the group header alone reconstructs the column. Frame-of-reference cannot reach that,
/// because it still has to encode the growing offset from the group's first timestamp.
///
/// Layout: [first: 8 bytes][firstDelta: 8 bytes][bitWidth: 1 byte][packed second differences].
///
/// Jitter costs little and gaps cost little: both perturb individual second differences rather
/// than the baseline. What hurts is a rate change every few rows, which keeps the width wide.
class DeltaOfDeltaCodec final : public ColumnCodec
{
    static constexpr size_t HEADER_BYTES = 8 + 8 + 1;

    /// Values from index 2 onwards; the first two are reconstructed from the header.
    static size_t dodCount(const uint64_t rowCount) noexcept { return rowCount > 2 ? rowCount - 2 : 0; }

public:
    [[nodiscard]] EncodedColumn encode(const ColumnValues& in, double) const override
    {
        PRECONDITION(supports(in.type), "DeltaOfDeltaCodec cannot encode DataType::Type {}", static_cast<int>(in.type));

        const auto encoded = dodEncode(loadColumn(in));

        std::vector<uint8_t> out;
        out.reserve(HEADER_BYTES + bitPackedSize(encoded.dods.size(), encoded.bitWidth));
        putU64(out, encoded.first);
        putU64(out, static_cast<uint64_t>(encoded.firstDelta));
        out.push_back(encoded.bitWidth);
        bitPack(encoded.dods, encoded.bitWidth, out);
        return EncodedColumn{.bytes = std::move(out), .achievedError = 0.0};
    }

    void decode(const std::span<const uint8_t> bytes, const MutableColumnValues& out) const override
    {
        PRECONDITION(bytes.size() >= HEADER_BYTES, "encoded column is {} bytes, header alone needs {}", bytes.size(), HEADER_BYTES);

        DeltaOfDeltaResult encoded;
        encoded.first = getU64(bytes.data());
        encoded.firstDelta = static_cast<int64_t>(getU64(bytes.data() + 8));
        encoded.bitWidth = bytes[16];

        encoded.dods.resize(dodCount(out.rowCount));
        bitUnpack(bytes.subspan(HEADER_BYTES), encoded.bitWidth, encoded.dods.size(), encoded.dods);

        std::vector<uint64_t> values(out.rowCount);
        dodDecode(encoded, out.rowCount, values);
        storeColumn(values, out);
    }

    [[nodiscard]] CodecId id() const noexcept override { return CodecId::DeltaOfDelta; }

    [[nodiscard]] bool supports(const DataType::Type type) const noexcept override { return isIntegerLike(type); }

    [[nodiscard]] bool isLossy() const noexcept override { return false; }
};

/// The dictionary body shared by the two codecs below: the distinct values, sorted, stored as
/// offsets from the smallest one and bit-packed.
///
/// Sorting is what makes this cheap. Ascending entries have ascending offsets, so the table
/// costs the width of its *span* rather than the width of its values -- and the width of the
/// codes is set by how many distinct values there are, not by how big they are. That
/// separation is the whole point: 8-byte ids spread across the range cost one byte per row.
///
/// Layout: [dictSize: 4 bytes][reference: 8 bytes][bitWidth: 1 byte][packed offsets].
struct DictionaryBody
{
    static constexpr size_t HEADER_BYTES = 4 + 8 + 1;

    static void write(std::vector<uint8_t>& out, const std::vector<uint64_t>& dictionary)
    {
        /// Not just an optimisation to be sorted -- the layout depends on it. Offsets are taken
        /// from the first entry and the width from the last, so an out-of-order table would
        /// underflow into a huge offset that the recorded width does not cover, and the entry
        /// would come back truncated rather than rejected.
        PRECONDITION(std::ranges::is_sorted(dictionary), "dictionary entries must be ascending");

        const uint64_t reference = dictionary.empty() ? 0 : dictionary.front();
        std::vector<uint64_t> offsets(dictionary.size());
        for (size_t i = 0; i < dictionary.size(); ++i)
        {
            offsets[i] = dictionary[i] - reference;
        }
        const uint8_t bitWidth = bitWidthFor(offsets.empty() ? 0 : offsets.back());

        putU32(out, static_cast<uint32_t>(dictionary.size()));
        putU64(out, reference);
        out.push_back(bitWidth);
        bitPack(offsets, bitWidth, out);
    }

    /// Returns the entries and advances `offset` past them.
    static std::vector<uint64_t> read(const std::span<const uint8_t> bytes, size_t& offset)
    {
        PRECONDITION(
            bytes.size() >= offset + HEADER_BYTES, "dictionary header runs past the {} bytes available", bytes.size());

        const auto dictSize = getU32(bytes.data() + offset);
        const uint64_t reference = getU64(bytes.data() + offset + 4);
        const uint8_t bitWidth = bytes[offset + 12];
        offset += HEADER_BYTES;

        std::vector<uint64_t> entries(dictSize);
        bitUnpack(bytes.subspan(offset), bitWidth, dictSize, entries);
        offset += bitPackedSize(dictSize, bitWidth);

        for (uint64_t& entry : entries)
        {
            entry += reference;
        }
        return entries;
    }
};

/// Replaces every value by its index into a table of the distinct ones.
///
/// This is the codec for columns whose values are wide but few -- device ids, station codes,
/// enum-like integers. Frame-of-reference is helpless on those, because a handful of ids
/// scattered across the whole 64-bit range still has a 64-bit span; the count of distinct
/// values, not their spread, is what bounds the cost here.
///
/// Layout: [dictionary body][codeBitWidth: 1 byte][packed codes].
class DictionaryCodec final : public ColumnCodec
{
public:
    [[nodiscard]] EncodedColumn encode(const ColumnValues& in, double) const override
    {
        PRECONDITION(supports(in.type), "DictionaryCodec cannot encode DataType::Type {}", static_cast<int>(in.type));

        const auto encoded = dictEncode(loadColumn(in));

        std::vector<uint8_t> out;
        DictionaryBody::write(out, encoded.dictionary);
        out.push_back(encoded.bitWidth);
        bitPack(encoded.codes, encoded.bitWidth, out);
        return EncodedColumn{.bytes = std::move(out), .achievedError = 0.0};
    }

    void decode(const std::span<const uint8_t> bytes, const MutableColumnValues& out) const override
    {
        size_t offset = 0;
        DictionaryResult encoded;
        encoded.dictionary = DictionaryBody::read(bytes, offset);

        PRECONDITION(bytes.size() > offset, "dictionary column ends before its code width");
        encoded.bitWidth = bytes[offset];
        offset += 1;

        encoded.codes.resize(out.rowCount);
        bitUnpack(bytes.subspan(offset), encoded.bitWidth, out.rowCount, encoded.codes);

        std::vector<uint64_t> values(out.rowCount);
        dictDecode(encoded, values);
        storeColumn(values, out);
    }

    [[nodiscard]] CodecId id() const noexcept override { return CodecId::Dictionary; }

    [[nodiscard]] bool supports(const DataType::Type type) const noexcept override { return isIntegerLike(type); }

    [[nodiscard]] bool isLossy() const noexcept override { return false; }
};

/// A dictionary whose codes are then run-length encoded.
///
/// For a column that changes rarely -- a status, a mode, a sensor that reports the same
/// reading for minutes -- this replaces a code per row with a code per *change*. The catch is
/// that it can make a column bigger, since a run of length one costs a code plus a length. The
/// break-even point depends on how narrow the codes already are, so selection settles it from
/// the measured run count rather than from a threshold, and then confirms it by encoding: this
/// is the codec whose estimated size and whose size after the byte compressor disagree most,
/// because the repetition it removes is exactly what an LZ matcher feeds on.
///
/// Layout: [dictionary body][runCount: 4][codeBitWidth: 1][packed run codes]
///         [lengthBitWidth: 1][packed run lengths].
class DictRleCodec final : public ColumnCodec
{
public:
    [[nodiscard]] EncodedColumn encode(const ColumnValues& in, double) const override
    {
        PRECONDITION(supports(in.type), "DictRleCodec cannot encode DataType::Type {}", static_cast<int>(in.type));

        const auto dictionary = dictEncode(loadColumn(in));
        const auto runs = rleEncode(dictionary.codes);

        uint64_t longestRun = 0;
        for (const uint64_t length : runs.runLengths)
        {
            longestRun = std::max(longestRun, length);
        }
        const uint8_t lengthBitWidth = bitWidthFor(longestRun);

        std::vector<uint8_t> out;
        DictionaryBody::write(out, dictionary.dictionary);
        putU32(out, static_cast<uint32_t>(runs.values.size()));
        out.push_back(dictionary.bitWidth);
        bitPack(runs.values, dictionary.bitWidth, out);
        out.push_back(lengthBitWidth);
        bitPack(runs.runLengths, lengthBitWidth, out);
        return EncodedColumn{.bytes = std::move(out), .achievedError = 0.0};
    }

    void decode(const std::span<const uint8_t> bytes, const MutableColumnValues& out) const override
    {
        size_t offset = 0;
        DictionaryResult dictionary;
        dictionary.dictionary = DictionaryBody::read(bytes, offset);

        PRECONDITION(bytes.size() >= offset + 5, "dict-RLE column ends before its run header");
        const auto runCount = getU32(bytes.data() + offset);
        const uint8_t codeBitWidth = bytes[offset + 4];
        offset += 5;

        RunLengthResult runs;
        runs.values.resize(runCount);
        bitUnpack(bytes.subspan(offset), codeBitWidth, runCount, runs.values);
        offset += bitPackedSize(runCount, codeBitWidth);

        PRECONDITION(bytes.size() > offset, "dict-RLE column ends before its run-length width");
        const uint8_t lengthBitWidth = bytes[offset];
        offset += 1;

        runs.runLengths.resize(runCount);
        bitUnpack(bytes.subspan(offset), lengthBitWidth, runCount, runs.runLengths);

        dictionary.codes.resize(out.rowCount);
        rleDecode(runs, out.rowCount, dictionary.codes);

        std::vector<uint64_t> values(out.rowCount);
        dictDecode(dictionary, values);
        storeColumn(values, out);
    }

    [[nodiscard]] CodecId id() const noexcept override { return CodecId::DictRle; }

    [[nodiscard]] bool supports(const DataType::Type type) const noexcept override { return isIntegerLike(type); }

    [[nodiscard]] bool isLossy() const noexcept override { return false; }
};

/// One bit per value.
///
/// Frame-of-reference would already pack a boolean column to one bit; what this drops is the
/// 8-byte reference in that codec's header, which is pure waste when the minimum is known to
/// be zero. Small in absolute terms, but a boolean column is only ~1 KB per group to begin
/// with, so the header is a measurable share of it.
///
/// Layout: [packed bits], and nothing else -- the row count comes from the group header.
class BoolCodec final : public ColumnCodec
{
public:
    [[nodiscard]] EncodedColumn encode(const ColumnValues& in, double) const override
    {
        PRECONDITION(supports(in.type), "BoolCodec cannot encode DataType::Type {}", static_cast<int>(in.type));

        const auto values = loadColumn(in);

        /// A single bit per value is lossless only if that is all the column ever holds. A
        /// byte outside {0, 1} in a BOOLEAN column means something upstream is wrong, and
        /// silently truncating it here would turn that into corrupted data.
        for (const uint64_t value : values)
        {
            PRECONDITION(value <= 1, "BOOLEAN column holds the value {}, which is neither true nor false", value);
        }

        std::vector<uint8_t> out;
        out.reserve(bitPackedSize(values.size(), 1));
        bitPack(values, 1, out);
        return EncodedColumn{.bytes = std::move(out), .achievedError = 0.0};
    }

    void decode(const std::span<const uint8_t> bytes, const MutableColumnValues& out) const override
    {
        std::vector<uint64_t> values(out.rowCount);
        bitUnpack(bytes, 1, out.rowCount, values);
        storeColumn(values, out);
    }

    [[nodiscard]] CodecId id() const noexcept override { return CodecId::Bool; }

    [[nodiscard]] bool supports(const DataType::Type type) const noexcept override { return type == DataType::Type::BOOLEAN; }

    [[nodiscard]] bool isLossy() const noexcept override { return false; }
};

const RawCodec RAW_CODEC{};
const ForBitpackCodec FOR_BITPACK_CODEC{};
const QuantizedCodec QUANTIZED_CODEC{};
const DeltaOfDeltaCodec DELTA_OF_DELTA_CODEC{};
const DictionaryCodec DICTIONARY_CODEC{};
const DictRleCodec DICT_RLE_CODEC{};
const BoolCodec BOOL_CODEC{};

}

QuantizedColumn quantizeColumn(const ColumnValues& in, const double relativeBoundPercent)
{
    PRECONDITION(
        in.type == DataType::Type::FLOAT32 || in.type == DataType::Type::FLOAT64,
        "quantizeColumn cannot encode DataType::Type {}",
        static_cast<int>(in.type));
    PRECONDITION(relativeBoundPercent > 0.0, "quantization needs a positive accuracy bound");

    const bool singlePrecision = in.type == DataType::Type::FLOAT32;

    std::vector<double> values(in.rowCount);
    for (uint64_t r = 0; r < in.rowCount; ++r)
    {
        if (singlePrecision)
        {
            float value = 0;
            std::memcpy(&value, in.data + (r * in.valueWidth), sizeof(float));
            values[r] = static_cast<double>(value);
        }
        else
        {
            std::memcpy(&values[r], in.data + (r * in.valueWidth), sizeof(double));
        }
    }

    return quantize(values, relativeBoundPercent, singlePrecision);
}

void writeQuantizeHeader(std::vector<uint8_t>& out, const QuantizedColumn& quantized)
{
    uint8_t flags = 0;
    if (quantized.isConstant)
    {
        flags |= FLAG_CONSTANT;
    }
    if (!quantized.exceptionBits.empty())
    {
        flags |= FLAG_EXCEPTIONS;
    }
    if (!quantized.signBits.empty())
    {
        flags |= FLAG_SIGNS;
    }

    out.push_back(flags);
    putDouble(out, quantized.minMagnitude);
    putDouble(out, quantized.deltaLog);
    out.push_back(quantized.bitWidth);

    /// The grid holds magnitudes, so signs travel beside it. Written before the exceptions
    /// because its length follows from the row count alone, while theirs needs a count read
    /// first.
    if ((flags & FLAG_SIGNS) != 0)
    {
        out.insert(out.end(), quantized.signBits.begin(), quantized.signBits.end());
    }

    if ((flags & FLAG_EXCEPTIONS) != 0)
    {
        putU32(out, static_cast<uint32_t>(quantized.exceptionPatterns.size()));
        out.insert(out.end(), quantized.exceptionBits.begin(), quantized.exceptionBits.end());
        for (const uint64_t pattern : quantized.exceptionPatterns)
        {
            putU64(out, pattern);
        }
    }
}

QuantizedColumn readQuantizeHeader(const std::span<const uint8_t> bytes, size_t& offset, const uint64_t rowCount)
{
    PRECONDITION(
        bytes.size() >= offset + QUANTIZE_FIXED_HEADER_BYTES, "quantized column is only {} bytes", bytes.size());

    QuantizedColumn quantized;
    const uint8_t flags = bytes[offset];
    quantized.isConstant = (flags & FLAG_CONSTANT) != 0;
    quantized.minMagnitude = getDouble(bytes.data() + offset + 1);
    quantized.deltaLog = getDouble(bytes.data() + offset + 9);
    quantized.bitWidth = bytes[offset + 17];
    offset += QUANTIZE_FIXED_HEADER_BYTES;

    const size_t bitmapBytes = (rowCount + 7) / 8;

    if ((flags & FLAG_SIGNS) != 0)
    {
        PRECONDITION(bytes.size() >= offset + bitmapBytes, "quantized column ends inside its sign bitmap");
        quantized.signBits.assign(
            bytes.begin() + static_cast<std::ptrdiff_t>(offset), bytes.begin() + static_cast<std::ptrdiff_t>(offset + bitmapBytes));
        offset += bitmapBytes;
    }

    if ((flags & FLAG_EXCEPTIONS) != 0)
    {
        PRECONDITION(bytes.size() >= offset + 4, "quantized column ends before its exception count");
        const auto count = getU32(bytes.data() + offset);
        offset += 4;

        PRECONDITION(
            bytes.size() >= offset + bitmapBytes + (static_cast<size_t>(count) * 8),
            "quantized column is {} bytes, too short for {} exceptions over {} rows",
            bytes.size(),
            count,
            rowCount);

        quantized.exceptionBits.assign(
            bytes.begin() + static_cast<std::ptrdiff_t>(offset),
            bytes.begin() + static_cast<std::ptrdiff_t>(offset + bitmapBytes));
        offset += bitmapBytes;

        quantized.exceptionPatterns.resize(count);
        for (uint32_t e = 0; e < count; ++e)
        {
            quantized.exceptionPatterns[e] = getU64(bytes.data() + offset);
            offset += 8;
        }
    }

    return quantized;
}

void dequantizeColumn(const QuantizedColumn& quantized, const MutableColumnValues& out)
{
    std::vector<double> values(out.rowCount);
    dequantize(quantized, out.rowCount, values);

    for (uint64_t r = 0; r < out.rowCount; ++r)
    {
        if (out.type == DataType::Type::FLOAT32)
        {
            const auto value = static_cast<float>(values[r]);
            std::memcpy(out.data + (r * out.valueWidth), &value, sizeof(float));
        }
        else
        {
            std::memcpy(out.data + (r * out.valueWidth), &values[r], sizeof(double));
        }
    }
}

std::string_view codecName(const CodecId id) noexcept
{
    switch (id)
    {
        case CodecId::Raw:
            return "Raw";
        case CodecId::ForBitpack:
            return "ForBitpack";
        case CodecId::Quantized:
            return "Quantized";
        case CodecId::DeltaOfDelta:
            return "DeltaOfDelta";
        case CodecId::Dictionary:
            return "Dictionary";
        case CodecId::DictRle:
            return "DictRle";
        case CodecId::Bool:
            return "Bool";
    }
    return "<unknown>";
}

const ColumnCodec& getCodec(const CodecId id)
{
    switch (id)
    {
        case CodecId::Raw:
            return RAW_CODEC;
        case CodecId::ForBitpack:
            return FOR_BITPACK_CODEC;
        case CodecId::Quantized:
            return QUANTIZED_CODEC;
        case CodecId::DeltaOfDelta:
            return DELTA_OF_DELTA_CODEC;
        case CodecId::Dictionary:
            return DICTIONARY_CODEC;
        case CodecId::DictRle:
            return DICT_RLE_CODEC;
        case CodecId::Bool:
            return BOOL_CODEC;
    }
    throw InvalidConfigParameter("Unknown column codec id {}", static_cast<int>(id));
}

bool isSupportedType(const DataType::Type type) noexcept
{
    switch (type)
    {
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
            return true;
        case DataType::Type::VARSIZED:
        case DataType::Type::UNDEFINED:
            return false;
    }
    return false;
}

bool isIntegerLike(const DataType::Type type) noexcept
{
    switch (type)
    {
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
            return true;
        default:
            return false;
    }
}

bool isSignedType(const DataType::Type type) noexcept
{
    switch (type)
    {
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
            return true;
        default:
            /// BOOLEAN and CHAR are treated as unsigned. Which they really are only matters
            /// for how tight the packing gets, never for correctness: encode and decode agree
            /// either way, and the round trip is exact.
            return false;
    }
}

}
