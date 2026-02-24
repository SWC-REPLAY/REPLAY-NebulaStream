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

#include <cstdint>
#include <fstream>
#include <string>
#include <utility>

#include <DataTypes/Schema.hpp>

namespace NES::StoreManager
{

constexpr char MAGIC[8] = {'N', 'E', 'S', 'S', 'T', 'O', 'R', 'E'};
constexpr uint32_t VERSION = 1;
constexpr uint8_t ENDIANNESS_LE = 1;

/// Fixed portion of the header before the variable-length schema text.
/// magic(8) + version(4) + endianness(1) + flags(4) + fingerprint(8) = 25 bytes
constexpr size_t HEADER_FIXED_BYTES = 8 + 4 + 1 + 4 + 8;

struct FileHeader
{
    uint32_t version{0};
    uint8_t endianness{0};
    uint32_t flags{0};
    uint64_t fingerprint{0};
    std::string schemaText;
};

/// Compute FNV-1a 64-bit hash.
uint64_t fnv1a64(const char* data, size_t len);

/// Serialize a complete file header (magic + fields + schema text) into a byte buffer.
std::string serializeHeader(const std::string& schemaText);

/// Parse a file header from an input stream. Returns the header and the data start offset (byte position after header).
std::pair<FileHeader, uint64_t> parseHeader(std::ifstream& ifs);

/// Reconstruct a Schema from the embedded schema text in a file header.
Schema parseSchemaFromText(const std::string& schemaText);

} // namespace NES::Replay