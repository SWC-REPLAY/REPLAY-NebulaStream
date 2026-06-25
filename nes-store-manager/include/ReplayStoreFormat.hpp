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

namespace NES::StoreManager
{

constexpr std::array<char, 8> MAGIC = {'N', 'E', 'S', 'S', 'T', 'O', 'R', 'E'};
constexpr uint32_t VERSION = 2;
constexpr uint8_t ENDIANNESS_LE = 1;

/// Fixed portion of the header before the variable-length schema text.
/// magic(8) + version(4) + endianness(1) + flags(4) + fingerprint(8) + minTs(8) + maxTs(8) = 41 bytes
constexpr size_t HEADER_FIXED_BYTES = 8 + 4 + 1 + 4 + 8 + 8 + 8;

constexpr size_t OFFSET_MIN_TS = 8 + 4 + 1 + 4 + 8;
constexpr size_t OFFSET_MAX_TS = OFFSET_MIN_TS + 8;

/// Segment extension fields appended after schemaText:
/// segmentSize(8) + segmentCount(4) + activeSegmentIndex(4) + wrapCount(4) = 20 bytes
constexpr size_t SEGMENT_HEADER_EXTENSION_BYTES = 8 + 4 + 4 + 4;

/// Each segment descriptor: usedBytes(8) + minTs(8) + maxTs(8) = 24 bytes
constexpr size_t SEGMENT_DESCRIPTOR_BYTES = 8 + 8 + 8;

struct SegmentDescriptor
{
    uint64_t usedBytes{0};
    uint64_t minTs{UINT64_MAX};
    uint64_t maxTs{0};
};

struct FileHeader
{
    uint32_t version{0};
    uint8_t endianness{0};
    uint32_t flags{0};
    uint64_t fingerprint{0};
    uint64_t minTs{UINT64_MAX};
    uint64_t maxTs{0};
    std::string schemaText;

    /// Segment fields (version >= 2)
    uint64_t segmentSize{0};
    uint32_t segmentCount{0};
    uint32_t activeSegmentIndex{0};
    uint32_t wrapCount{0};
    std::vector<SegmentDescriptor> segments;
};

std::string serializeHeader(const std::string& schemaText, uint64_t minTs = UINT64_MAX, uint64_t maxTs = UINT64_MAX);

/// Serialize a full segmented header including segment table.
std::string serializeSegmentedHeader(
    const std::string& schemaText,
    uint64_t segmentSize,
    uint32_t segmentCount,
    uint64_t minTs = UINT64_MAX,
    uint64_t maxTs = UINT64_MAX);

std::pair<FileHeader, uint64_t> parseHeader(std::ifstream& ifs);

/// Compute the file offset where the segment table starts.
size_t segmentTableOffset(size_t schemaTextLen);

/// Compute the file offset where segment data areas start.
size_t segmentDataAreaOffset(size_t schemaTextLen, uint32_t segmentCount);

/// Compute the file offset of a specific segment's data area.
size_t segmentDataOffset(size_t dataAreaStart, uint32_t segmentIndex, uint64_t segmentSize);

Schema parseSchemaFromText(const std::string& schemaText);

}
