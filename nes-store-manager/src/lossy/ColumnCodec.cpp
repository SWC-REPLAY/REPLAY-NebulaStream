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

#include <lossy/ColumnCodec.hpp>

#include <algorithm>
#include <bit>
#include <cstring>

#include <lossy/Primitives.hpp>
#include <lossy/Quantize.hpp>

#include <ErrorHandling.hpp>

namespace NES::Lossy
{

namespace
{

// Little-endian fixed-width fields for the codec headers below.
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

// Flags and fixed-width fields of the quantization header.
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

// Stores a column exactly as it arrived; the identity and fallback codec.
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

// Subtracts the block's minimum from every value and bit-packs the remainders. Integer columns.
class ForBitpackCodec final : public ColumnCodec
{
    static constexpr size_t HEADER_BYTES = sizeof(uint64_t) + 1;

public:
    [[nodiscard]] EncodedColumn encode(const ColumnValues& in, double) const override
    {
        PRECONDITION(supports(in.type), "ForBitpackCodec cannot encode DataType::Type {}", static_cast<int>(in.type));

        const bool isSigned = isSignedType(in.type);
        std::vector<uint64_t> offsets(in.rowCount);

        // Reference kept as a raw bit pattern; unsigned subtraction wraps correctly for signed inputs too.
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

// Rounds every value onto a uniform grid and bit-packs the grid indices. The only lossy codec.
class QuantizedCodec final : public ColumnCodec
{
public:
    [[nodiscard]] EncodedColumn encode(const ColumnValues& in, const double relativeBoundPercent) const override
    {
        // Quantize onto the log grid.
        const auto quantized = quantizeColumn(in, relativeBoundPercent);

        std::vector<uint8_t> out;
        writeQuantizeHeader(out, quantized);
        if (!quantized.isConstant)
        {
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

const RawCodec RAW_CODEC{};
const ForBitpackCodec FOR_BITPACK_CODEC{};
const QuantizedCodec QUANTIZED_CODEC{};

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

    // Sign bitmap, beside the magnitude grid; written before the exception list.
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
        // 4..7 are the local-only extra codecs, not built here; a shipped store never emits them.
        case CodecId::DeltaOfDelta:
        case CodecId::Dictionary:
        case CodecId::DictRle:
        case CodecId::Bool:
            break;
    }
    throw InvalidConfigParameter("column codec id {} is not available in this build", static_cast<int>(id));
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
            // BOOLEAN and CHAR are treated as unsigned; the round trip is exact either way.
            return false;
    }
}

}
