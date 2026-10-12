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

// On-disk layout of the lossy store:

//  [ file header ]                     (ReplayStoreFormat: MAGIC, VERSION, schema, file minTs/maxTs)
//  [ GroupHeader ][ ColumnEntry x N ][ column data ]      <- group 0
//  [ GroupHeader ][ ColumnEntry x N ][ column data ]      <- group 1

struct GroupHeader
{
    //Total bytes of this group INCLUDING this header
    uint64_t groupByteLength{0};
    uint64_t rowCount{0};
    // Smallest/largest timestamp among this group's rows.
    uint64_t minTs{0};
    uint64_t maxTs{0};
    uint32_t numColumns{0};
};


constexpr size_t GROUP_HEADER_BYTES = (4 * 8) + 4;

/// Where one column's bytes live inside the group's data region (i.e. relative to the end
/// of the ColumnEntry table, not to the group's file offset).
struct ColumnEntry
{
    uint64_t valuesOffset{0};
    // Bytes occupied ON DISK.
    uint64_t valuesLength{0};

    /// Bytes the codec produced, before the byte compressor ran over them.
    uint64_t uncompressedLength{0};

    uint64_t nullBitsOffset{0};
    uint64_t nullBitsLength{0};

    //The lossy stage, or 0 when the column has none. Only ever `CodecId::Quantized` today.
    //Separate from the lossless id because the two are chosen by different means
    uint8_t lossyCodecId{0};

    //The lossless stage.
    uint8_t losslessCodecId{0};

    double achievedError{0.0};
};

// Fixed on-disk size: five u64 + two u8 + one double = 50 bytes.
constexpr size_t COLUMN_ENTRY_BYTES = (5 * 8) + 2 + 8;

[[nodiscard]] constexpr size_t nullBitsBytesFor(const uint64_t rowCount) noexcept
{
    return static_cast<size_t>((rowCount + 7) / 8);
}

void appendGroupHeader(std::vector<uint8_t>& out, const GroupHeader& header);
[[nodiscard]] GroupHeader parseGroupHeader(const uint8_t* buf);

void appendColumnEntry(std::vector<uint8_t>& out, const ColumnEntry& entry);
[[nodiscard]] ColumnEntry parseColumnEntry(const uint8_t* buf);

}
