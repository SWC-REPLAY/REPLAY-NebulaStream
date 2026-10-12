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

#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>

namespace NES
{

constexpr std::array<char, 8> MAGIC = {'N', 'E', 'S', 'S', 'T', 'O', 'R', 'E'};
constexpr uint32_t VERSION = 1;
constexpr uint8_t ENDIANNESS_LE = 1;

/// Fixed portion of the header before the variable-length schema text.
/// magic(8) + version(4) + endianness(1) + flags(4) + fingerprint(8) + minTs(8) + maxTs(8) = 41 bytes
constexpr size_t HEADER_FIXED_BYTES = 8 + 4 + 1 + 4 + 8 + 8 + 8;

constexpr size_t OFFSET_MIN_TS = 8 + 4 + 1 + 4 + 8;
constexpr size_t OFFSET_MAX_TS = OFFSET_MIN_TS + 8;

/// ---- segment table -----------------------------------------------------------------------
///
/// A store that divides its file into segments records, for each one, where it sits and the
/// time range of the rows inside it. That range is the whole point: a read compares it against
/// the requested range and skips the segment without touching its bytes -- the same thing
/// MemoryStore already does with its buffers, one tier down.
///
/// The table is written immediately after the schema text and is PREALLOCATED: the flush
/// policy caps the file, so the largest possible segment count is known when the file is
/// created. Slots start zeroed and each is filled in place when its segment seals, which keeps
/// the table at the front of the file where one read reaches all of it. Appending it at the end
/// instead would mean a reader has to seek to the end first, and a crash would lose it entirely.
///
/// Stores that do not segment (LossyStore, LosslessStore) write the extension with
/// `maxSegments == 0`, so the layout stays uniform and parsing needs no special case.

/// segmentSize(8) + maxSegments(4) + sealedCount(4)
constexpr size_t SEGMENT_HEADER_EXTENSION_BYTES = 8 + 4 + 4;

struct SegmentDescriptor
{
    /// Relative to the start of the data region, not to the file.
    uint64_t byteOffset{0};
    uint64_t byteLength{0};
    uint64_t rowCount{0};
    /// Range of the rows in this segment. Carried down from the buffers that filled it, never
    /// recomputed by reading the rows back.
    uint64_t minTs{UINT64_MAX};
    uint64_t maxTs{0};
};

constexpr size_t SEGMENT_DESCRIPTOR_BYTES = 5 * 8;

struct FileHeader
{
    uint32_t version{0};
    uint8_t endianness{0};
    uint32_t flags{0};
    uint64_t fingerprint{0};
    uint64_t minTs{UINT64_MAX};
    uint64_t maxTs{0};
    std::string schemaText;

    uint64_t segmentSize{0};
    uint32_t maxSegments{0};
    /// How many slots hold a sealed segment. The rest are still zeroed.
    uint32_t sealedCount{0};
    /// The first `sealedCount` descriptors.
    std::vector<SegmentDescriptor> segments;
};

/// Overwritten once records have been appended.
constexpr uint64_t HEADER_UNSET_TS = UINT64_MAX;

std::string serializeHeader(
    const std::string& schemaText, uint64_t minTs, uint64_t maxTs, uint64_t segmentSize, uint32_t maxSegments);

std::pair<FileHeader, uint64_t> parseHeader(std::ifstream& ifs);

/// Byte offset of `sealedCount`, which is rewritten in place every time a segment seals.
[[nodiscard]] size_t sealedCountOffset(size_t schemaTextLen);

/// Byte offset of the first descriptor slot.
[[nodiscard]] size_t segmentTableOffset(size_t schemaTextLen);

/// Byte offset of slot `index`.
[[nodiscard]] size_t segmentSlotOffset(size_t schemaTextLen, uint32_t index);

/// Where the data region begins: past the header, the schema text, and the whole table.
[[nodiscard]] size_t dataStartOffsetFor(size_t schemaTextLen, uint32_t maxSegments);

Schema parseSchemaFromText(const std::string& schemaText);

}
