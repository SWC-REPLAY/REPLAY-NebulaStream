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

namespace NES::StoreManager::Lossy
{

/// Which codec produced a column's bytes.
///
/// The numeric values are written into every group header, so they are part of the on-disk
/// format: append new ones, never renumber an existing one.
enum class CodecId : uint8_t
{
    /// Values stored verbatim. The identity codec, and the fallback for anything no other
    /// codec handles.
    Raw = 1,

    /// Subtract the block's minimum, then bit-pack the remainders. Integer columns.
    ForBitpack = 2,

    /// Round every value onto a fixed grid, then bit-pack the grid indices. The only codec
    /// that loses information.
    Quantized = 3,

    /// Store how much the step between neighbours changed, then bit-pack that. Timestamps.
    DeltaOfDelta = 4,

    /// Replace each value by an index into a table of the distinct ones. Keys and categories.
    Dictionary = 5,

    /// A dictionary whose codes are then run-length encoded. Categories that arrive in runs.
    DictRle = 6,

    /// One bit per value.
    Bool = 7,
};

std::string_view codecName(CodecId id) noexcept;

/// One column's values after columnarization: `rowCount` fixed-width values back to back.
///
/// Null indicators are deliberately absent. The store strips them into a separate validity
/// bitmap before a codec ever sees a column, so every codec gets a clean, densely packed
/// value array -- which is what encodings like bit-packing and frame-of-reference need in
/// order to be simple and fast. Values at null positions are still present (as whatever
/// bytes the row held), so the array stays exactly `rowCount * valueWidth` bytes.
struct ColumnValues
{
    DataType::Type type{DataType::Type::UNDEFINED};
    const uint8_t* data{nullptr};
    uint64_t rowCount{0};
    /// Bytes per value, excluding any null indicator.
    uint32_t valueWidth{0};

    [[nodiscard]] uint64_t byteSize() const noexcept { return rowCount * valueWidth; }
};

/// Where a codec writes its reconstructed values. Always caller-allocated and always
/// exactly `rowCount * valueWidth` bytes: decoding a column never changes its row count.
struct MutableColumnValues
{
    DataType::Type type{DataType::Type::UNDEFINED};
    uint8_t* data{nullptr};
    uint64_t rowCount{0};
    uint32_t valueWidth{0};

    [[nodiscard]] uint64_t byteSize() const noexcept { return rowCount * valueWidth; }
};

/// What a codec produced, plus what it cost in accuracy.
struct EncodedColumn
{
    std::vector<uint8_t> bytes;

    /// The largest |v - v'| the encoder actually measured over this column. Zero for
    /// lossless codecs.
    ///
    /// Measured rather than assumed: a reader deciding whether stored data meets a requested
    /// bound has to compare against what the encoder really achieved, not against what it was
    /// asked for.
    double achievedError{0.0};
};

/// Turns one column into bytes, and those bytes back into the column.
///
/// The contract is "a whole column in, bytes out" rather than "one stage of a pipeline".
/// That matters for what comes later: a cascade codec (quantize, then frame-of-reference,
/// then bit-pack) and a monolithic one that owns its entire physical layout both satisfy
/// it, so both can sit behind this interface without the store knowing which is which.
///
/// Codecs hold no state and are const-callable, so a single instance serves every column
/// of every group.
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

    /// Reconstruct into `out`, which the caller sized from the row count recorded in the
    /// group header. Must fill all of it.
    virtual void decode(std::span<const uint8_t> bytes, const MutableColumnValues& out) const = 0;

    [[nodiscard]] virtual CodecId id() const noexcept = 0;

    /// Whether this codec can encode that type at all. Checked during selection so a
    /// mismatch surfaces as a rejected column rather than as corrupt output.
    [[nodiscard]] virtual bool supports(DataType::Type type) const noexcept = 0;

    /// Whether encoding loses information.
    ///
    /// Selection uses this to keep the two kinds apart, and that separation is what makes
    /// comparing candidates by size meaningful at all: a codec that discards information is
    /// smaller *because* it discarded it, so ranking one against a lossless codec would not
    /// be a measurement, it would be a foregone conclusion. Whether loss is permitted is a
    /// policy question, decided before any size is computed; only within one answer to that
    /// question do bytes decide.
    [[nodiscard]] virtual bool isLossy() const noexcept = 0;
};

/// The codec registered under `id`. Throws if there is none, which is what a reader hitting
/// a file written by a newer build will see -- a clear failure rather than a misparse.
///
/// Adding a codec is three edits, all visible: a CodecId value, a case here, and the class.
const ColumnCodec& getCodec(CodecId id);

/// Types the store can columnarize: everything fixed-width. VARSIZED occupies only an index
/// in the packed row with its payload elsewhere, and UNDEFINED has no layout at all.
bool isSupportedType(DataType::Type type) noexcept;

/// Whether a type's values are integers, i.e. whether subtracting a reference from them and
/// bit-packing the remainder is meaningful. Deliberately excludes floats: the same
/// subtraction on their bit patterns produces noise, and narrowing them needs the lossy step
/// instead.
bool isIntegerLike(DataType::Type type) noexcept;

/// Whether values of this type are two's-complement signed, and therefore have to be
/// sign-extended before being compared.
bool isSignedType(DataType::Type type) noexcept;

/// ---- the quantization stage, in halves ---------------------------------------------------
///
/// `QuantizedCodec` above is the stage in one piece: its header, then its codes bit-packed at
/// their own width. These expose the halves separately, because the codes are just an integer
/// column and one of the lossless codecs may encode them smaller than plain bit-packing does --
/// a quantized column that varies smoothly has near-zero second differences, which
/// delta-of-delta collapses to nothing and bit-packing cannot.
///
/// Both layouts begin with the same header. Which one a column used is recorded in
/// `ColumnEntry::losslessCodecId`, where zero means the one-piece form.

/// Read a float column and round it onto the grid. The codes are left in the result rather
/// than serialized, for a second codec to encode.
[[nodiscard]] QuantizedColumn quantizeColumn(const ColumnValues& in, double relativeBoundPercent);

/// Everything the stage needs to invert itself except the codes: the grid, the flags, and any
/// non-finite values held out of the grid.
void writeQuantizeHeader(std::vector<uint8_t>& out, const QuantizedColumn& quantized);

/// Reads what `writeQuantizeHeader` wrote and advances `offset` past it. `codes` is left empty;
/// the caller fills it from whatever encoded them.
[[nodiscard]] QuantizedColumn readQuantizeHeader(std::span<const uint8_t> bytes, size_t& offset, uint64_t rowCount);

/// Reconstruct into a float column, narrowing to FLOAT32 when that is the output type.
void dequantizeColumn(const QuantizedColumn& quantized, const MutableColumnValues& out);

}
