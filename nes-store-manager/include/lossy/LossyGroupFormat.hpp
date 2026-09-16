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
#include <vector>

namespace NES::Lossy
{

/// On-disk layout of the lossy store:
///
///   [ file header ]                     (ReplayStoreFormat: MAGIC, VERSION, schema, file minTs/maxTs)
///   [ GroupHeader ][ ColumnEntry x N ][ column data ]      <- group 0
///   [ GroupHeader ][ ColumnEntry x N ][ column data ]      <- group 1
///   ...
///
/// A group holds a fixed number of rows (Config::rowsPerGroup, default 8192) reorganized
/// from the row-major bytes the tier above hands over into column-major arrays.
///
/// Everything needed to decode a group lives inside that group. Nothing refers to a
/// neighbouring group or to file-level state, which is what lets groups be decoded
/// independently, in parallel, and skipped without being read.

/// Written uncompressed, ahead of its group, for two reasons:
///   1. minTs/maxTs let a read skip the whole group without touching its data. This is what
///      makes a time-range replay cheap, and it is the reason groups are small.
///   2. groupByteLength lets a reader walk to the next group without parsing this one's
///      column table -- so skipping really does cost one seek.
struct GroupHeader
{
    /// Total bytes of this group INCLUDING this header, so `offset + groupByteLength` is
    /// the next group.
    uint64_t groupByteLength{0};
    uint64_t rowCount{0};
    /// Smallest/largest timestamp among this group's rows.
    ///
    /// Carried down from the segment the tier above handed over, where the timestamps were
    /// actually known -- so a group's range is always narrow enough to skip on, and the store
    /// never has to be told which column holds record time.
    uint64_t minTs{0};
    uint64_t maxTs{0};
    uint32_t numColumns{0};
};

/// Fixed on-disk size: four u64 + one u32 + one u8 = 37 bytes.
constexpr size_t GROUP_HEADER_BYTES = (4 * 8) + 4;

/// Where one column's bytes live inside the group's data region (i.e. relative to the end
/// of the ColumnEntry table, not to the group's file offset).
struct ColumnEntry
{
    uint64_t valuesOffset{0};
    /// Bytes occupied ON DISK. Deliberately stored rather than derived: once a codec is
    /// anything but Raw, a column's size is whatever the codec made it and has no relationship
    /// to `rowCount * valueWidth`.
    uint64_t valuesLength{0};

    /// Bytes the codec produced, before the byte compressor ran over them.
    ///
    /// Equal to `valuesLength` when the column is stored as the codec left it, which is how
    /// "not worth compressing" is recorded -- no flag, just the two lengths agreeing. That
    /// case is common and real: a bit-packed column is already dense, and zstd over it usually
    /// costs more than it saves.
    ///
    /// Compression is applied per column rather than to the group as a whole so that
    /// `valuesOffset` keeps pointing into the file. A single compressed blob per group would
    /// make the offsets point into a decompressed buffer instead, which forecloses reading one
    /// column without inflating the rest -- the thing the columnar layout exists for. Parquet
    /// and ORC compress per column chunk for the same reason.
    uint64_t uncompressedLength{0};

    /// The packed row's one-byte null indicator per row, folded down to one bit per row,
    /// least-significant bit first. Zero-length when the column is not nullable.
    ///
    /// Never compressed. At one bit per row it is already the smallest representation of
    /// itself, and a zstd frame header would frequently be larger than the bitmap.
    ///
    /// Kept out of the value array on purpose: leaving the indicator interleaved would force
    /// every codec to stride over it and would spend a byte to carry a bit.
    ///
    /// The bit records whether the indicator byte was 1; it is deliberately not interpreted
    /// as "null" or "valid", because nothing in the codebase pins down which value means
    /// which. Encoding rejects any other byte value rather than guess.
    uint64_t nullBitsOffset{0};
    uint64_t nullBitsLength{0};

    /// The lossy stage, or 0 when the column has none. Only ever `CodecId::Quantized` today.
    ///
    /// Separate from the lossless id because the two are chosen by different means and for
    /// different reasons: this one by policy, before any measurement, and the one below by
    /// which encoding of the result turned out smallest.
    uint8_t lossyCodecId{0};

    /// The lossless stage.
    ///
    /// For a column with no lossy stage this is simply the codec that produced the bytes. For a
    /// quantized one it is the codec applied to the *codes*, and 0 means they were bit-packed
    /// inline at their own width -- the one-piece form `QuantizedCodec` writes. Zero is
    /// unambiguous because CodecId starts at 1, and a column without a lossy stage always
    /// records a real codec here.
    uint8_t losslessCodecId{0};

    /// The largest |v - v'| the encoder measured for this column. Zero for lossless codecs.
    ///
    /// Recorded next to the data rather than inside it so a reader can decide whether a
    /// column meets a requested accuracy WITHOUT decoding it -- and so that a request for a
    /// tighter bound than what is stored fails openly instead of quietly returning data that
    /// does not meet it.
    double achievedError{0.0};
};

/// Fixed on-disk size: five u64 + two u8 + one double = 50 bytes.
constexpr size_t COLUMN_ENTRY_BYTES = (5 * 8) + 2 + 8;

/// Number of bytes a null-indicator bitmap for `rowCount` rows occupies.
[[nodiscard]] constexpr size_t nullBitsBytesFor(const uint64_t rowCount) noexcept
{
    return static_cast<size_t>((rowCount + 7) / 8);
}

void appendGroupHeader(std::vector<uint8_t>& out, const GroupHeader& header);
[[nodiscard]] GroupHeader parseGroupHeader(const uint8_t* buf);

void appendColumnEntry(std::vector<uint8_t>& out, const ColumnEntry& entry);
[[nodiscard]] ColumnEntry parseColumnEntry(const uint8_t* buf);

}
