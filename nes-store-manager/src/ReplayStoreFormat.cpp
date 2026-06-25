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

#include <ReplayStoreFormat.hpp>

#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <regex>
#include <string>
#include <utility>

#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/FNV.hpp>
#include <ErrorHandling.hpp>

namespace NES::StoreManager
{

namespace
{
/// Serialize the common header prefix (magic through maxTs + schemaLen + schemaText).
void serializeCommonHeader(std::string& buf, const std::string& schemaText, uint64_t minTs, uint64_t maxTs)
{
    const uint64_t fingerprint = fnv1a64(schemaText.c_str(), schemaText.size());
    const auto schemaLen = static_cast<uint32_t>(schemaText.size());

    size_t off = 0;
    std::memcpy(buf.data() + off, MAGIC.data(), MAGIC.size());
    off += MAGIC.size();
    std::memcpy(buf.data() + off, &VERSION, sizeof(uint32_t));
    off += sizeof(uint32_t);
    std::memcpy(buf.data() + off, &ENDIANNESS_LE, sizeof(uint8_t));
    off += sizeof(uint8_t);
    uint32_t flags = 0;
    std::memcpy(buf.data() + off, &flags, sizeof(uint32_t));
    off += sizeof(uint32_t);
    std::memcpy(buf.data() + off, &fingerprint, sizeof(uint64_t));
    off += sizeof(uint64_t);
    std::memcpy(buf.data() + off, &minTs, sizeof(uint64_t));
    off += sizeof(uint64_t);
    std::memcpy(buf.data() + off, &maxTs, sizeof(uint64_t));
    off += sizeof(uint64_t);
    std::memcpy(buf.data() + off, &schemaLen, sizeof(uint32_t));
    off += sizeof(uint32_t);
    std::memcpy(buf.data() + off, schemaText.data(), schemaLen);
}
}

std::string serializeHeader(const std::string& schemaText, uint64_t minTs, uint64_t maxTs)
{
    const auto schemaLen = static_cast<uint32_t>(schemaText.size());
    const size_t headerSize = HEADER_FIXED_BYTES + sizeof(uint32_t) + schemaLen;
    std::string buf;
    buf.resize(headerSize);
    serializeCommonHeader(buf, schemaText, minTs, maxTs);
    return buf;
}

std::string serializeSegmentedHeader(
    const std::string& schemaText, uint64_t segmentSize, uint32_t segmentCount, uint64_t minTs, uint64_t maxTs)
{
    const auto schemaLen = static_cast<uint32_t>(schemaText.size());
    const size_t headerSize = HEADER_FIXED_BYTES + sizeof(uint32_t) + schemaLen + SEGMENT_HEADER_EXTENSION_BYTES
        + (static_cast<size_t>(segmentCount) * SEGMENT_DESCRIPTOR_BYTES);
    std::string buf;
    buf.resize(headerSize);
    serializeCommonHeader(buf, schemaText, minTs, maxTs);

    size_t off = HEADER_FIXED_BYTES + sizeof(uint32_t) + schemaLen;

    std::memcpy(buf.data() + off, &segmentSize, sizeof(uint64_t));
    off += sizeof(uint64_t);
    std::memcpy(buf.data() + off, &segmentCount, sizeof(uint32_t));
    off += sizeof(uint32_t);
    uint32_t activeSegmentIndex = 0;
    std::memcpy(buf.data() + off, &activeSegmentIndex, sizeof(uint32_t));
    off += sizeof(uint32_t);
    uint32_t wrapCount = 0;
    std::memcpy(buf.data() + off, &wrapCount, sizeof(uint32_t));
    off += sizeof(uint32_t);

    /// Initialize segment descriptors to empty
    for (uint32_t i = 0; i < segmentCount; ++i)
    {
        uint64_t usedBytes = 0;
        uint64_t segMinTs = UINT64_MAX;
        uint64_t segMaxTs = 0;
        std::memcpy(buf.data() + off, &usedBytes, sizeof(uint64_t));
        off += sizeof(uint64_t);
        std::memcpy(buf.data() + off, &segMinTs, sizeof(uint64_t));
        off += sizeof(uint64_t);
        std::memcpy(buf.data() + off, &segMaxTs, sizeof(uint64_t));
        off += sizeof(uint64_t);
    }

    return buf;
}

std::pair<FileHeader, uint64_t> parseHeader(std::ifstream& ifs)
{
    std::array<char, 8> magic{};
    ifs.read(magic.data(), magic.size());
    if (!ifs)
    {
        throw CannotOpenSink("Failed to read magic from file");
    }

    FileHeader header;
    uint32_t schemaLen = 0;
    ifs.read(reinterpret_cast<char*>(&header.version), sizeof(header.version));
    ifs.read(reinterpret_cast<char*>(&header.endianness), sizeof(header.endianness));
    ifs.read(reinterpret_cast<char*>(&header.flags), sizeof(header.flags));
    ifs.read(reinterpret_cast<char*>(&header.fingerprint), sizeof(header.fingerprint));
    ifs.read(reinterpret_cast<char*>(&header.minTs), sizeof(header.minTs));
    ifs.read(reinterpret_cast<char*>(&header.maxTs), sizeof(header.maxTs));
    ifs.read(reinterpret_cast<char*>(&schemaLen), sizeof(schemaLen));
    if (!ifs)
    {
        throw CannotOpenSink("Failed to read header fields");
    }

    header.schemaText.resize(schemaLen);
    ifs.read(header.schemaText.data(), schemaLen);
    if (!ifs)
    {
        throw CannotOpenSink("Failed to read schema text from header");
    }

    uint64_t dataStartOffset = HEADER_FIXED_BYTES + sizeof(uint32_t) + schemaLen;

    /// Parse segment extension if version >= 2
    if (header.version >= 2)
    {
        ifs.read(reinterpret_cast<char*>(&header.segmentSize), sizeof(header.segmentSize));
        ifs.read(reinterpret_cast<char*>(&header.segmentCount), sizeof(header.segmentCount));
        ifs.read(reinterpret_cast<char*>(&header.activeSegmentIndex), sizeof(header.activeSegmentIndex));
        ifs.read(reinterpret_cast<char*>(&header.wrapCount), sizeof(header.wrapCount));
        if (!ifs)
        {
            throw CannotOpenSink("Failed to read segment header extension");
        }

        header.segments.resize(header.segmentCount);
        for (uint32_t i = 0; i < header.segmentCount; ++i)
        {
            ifs.read(reinterpret_cast<char*>(&header.segments[i].usedBytes), sizeof(uint64_t));
            ifs.read(reinterpret_cast<char*>(&header.segments[i].minTs), sizeof(uint64_t));
            ifs.read(reinterpret_cast<char*>(&header.segments[i].maxTs), sizeof(uint64_t));
        }
        if (!ifs)
        {
            throw CannotOpenSink("Failed to read segment table");
        }

        dataStartOffset += SEGMENT_HEADER_EXTENSION_BYTES + (static_cast<size_t>(header.segmentCount) * SEGMENT_DESCRIPTOR_BYTES);
    }

    return {header, dataStartOffset};
}

size_t segmentTableOffset(size_t schemaTextLen)
{
    return HEADER_FIXED_BYTES + sizeof(uint32_t) + schemaTextLen + SEGMENT_HEADER_EXTENSION_BYTES;
}

size_t segmentDataAreaOffset(size_t schemaTextLen, uint32_t segmentCount)
{
    return segmentTableOffset(schemaTextLen) + (static_cast<size_t>(segmentCount) * SEGMENT_DESCRIPTOR_BYTES);
}

size_t segmentDataOffset(size_t dataAreaStart, uint32_t segmentIndex, uint64_t segmentSize)
{
    return dataAreaStart + (static_cast<size_t>(segmentIndex) * segmentSize);
}

Schema parseSchemaFromText(const std::string& schemaText)
{
    Schema schema;
    const std::regex fieldRegex(R"(Field\(name:\s*([\w.$]+),\s*DataType:\s*DataType\(type:\s*(\w+)\)\))");
    auto begin = std::sregex_iterator(schemaText.begin(), schemaText.end(), fieldRegex);
    auto end = std::sregex_iterator();
    for (auto it = begin; it != end; ++it)
    {
        const auto& match = *it;
        auto fieldName = match[1].str();
        if (const auto pos = fieldName.rfind('$'); pos != std::string::npos)
        {
            fieldName = fieldName.substr(pos + 1);
        }
        schema.addField(fieldName, DataTypeProvider::provideDataType(match[2].str()));
    }
    if (schema.getNumberOfFields() == 0)
    {
        throw InvalidConfigParameter("parseSchemaFromText: no fields parsed from: {}", schemaText);
    }
    return schema;
}

}