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
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <lossy/Quantize.hpp>

namespace NES::Lossy
{

// Which codec produced a column's bytes.
enum class CodecId : uint8_t
{

    Raw = 1,

    // Subtract the block's minimum, then bit-pack the remainders. Integer columns.
    ForBitpack = 2,

    // Round every value onto a fixed grid, then bit-pack the grid indices.
    Quantized = 3,

    // Store how much the step between neighbours changed, then bit-pack that. Timestamps.
    DeltaOfDelta = 4,

    // Replace each value by an index into a table of the distinct ones. Keys and categories.
    Dictionary = 5,

    // A dictionary whose codes are then run-length encoded. Categories that arrive in runs.
    DictRle = 6,

    // One bit per value.
    Bool = 7,
};

std::string_view codecName(CodecId id) noexcept;

// One column's values after columnarization: `rowCount` fixed-width values back to back.
struct ColumnValues
{
    DataType::Type type{DataType::Type::UNDEFINED};
    const uint8_t* data{nullptr};
    uint64_t rowCount{0};
    /// Bytes per value, excluding any null indicator.
    uint32_t valueWidth{0};

    [[nodiscard]] uint64_t byteSize() const noexcept { return rowCount * valueWidth; }
};

// Where a codec writes its reconstructed values.
struct MutableColumnValues
{
    DataType::Type type{DataType::Type::UNDEFINED};
    uint8_t* data{nullptr};
    uint64_t rowCount{0};
    uint32_t valueWidth{0};

    [[nodiscard]] uint64_t byteSize() const noexcept { return rowCount * valueWidth; }
};

// What a codec produced, plus what it cost in accuracy.
struct EncodedColumn
{
    std::vector<uint8_t> bytes;

    // Measured rather than assumed
    double achievedError{0.0};
};

//Turns one column into bytes, and those bytes back into the column.
class ColumnCodec
{
public:
    ColumnCodec() = default;
    ColumnCodec(const ColumnCodec&) = delete;
    ColumnCodec(ColumnCodec&&) = delete;
    ColumnCodec& operator=(const ColumnCodec&) = delete;
    ColumnCodec& operator=(ColumnCodec&&) = delete;
    virtual ~ColumnCodec() = default;

    /// @param relativeBoundPercent  Accuracy bound as a percentage of the column's value
    ///        range within this block. Zero means encode losslessly; lossless codecs ignore
    ///        it entirely.
    [[nodiscard]] virtual EncodedColumn encode(const ColumnValues& in, double relativeBoundPercent) const = 0;


    virtual void decode(std::span<const uint8_t> bytes, const MutableColumnValues& out) const = 0;

    [[nodiscard]] virtual CodecId id() const noexcept = 0;

    // Whether this codec can encode that type at all.
    [[nodiscard]] virtual bool supports(DataType::Type type) const noexcept = 0;

    // Whether encoding loses information.
    [[nodiscard]] virtual bool isLossy() const noexcept = 0;
};


const ColumnCodec& getCodec(CodecId id);

// Types the store can columnarize: everything fixed-width. VARSIZED rejected
bool isSupportedType(DataType::Type type) noexcept;


bool isIntegerLike(DataType::Type type) noexcept;


bool isSignedType(DataType::Type type) noexcept;


[[nodiscard]] QuantizedColumn quantizeColumn(const ColumnValues& in, double relativeBoundPercent);


void writeQuantizeHeader(std::vector<uint8_t>& out, const QuantizedColumn& quantized);


[[nodiscard]] QuantizedColumn readQuantizeHeader(std::span<const uint8_t> bytes, size_t& offset, uint64_t rowCount);

/// Reconstruct into a float column, narrowing to FLOAT32 when that is the output type.
void dequantizeColumn(const QuantizedColumn& quantized, const MutableColumnValues& out);

}
