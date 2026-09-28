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
#include <span>
#include <string>
#include <utility>

#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/FNV.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Copy `len` bytes from `src` into `dest` at `off`, advancing `off` past the copied bytes.
void writeBytes(const std::span<char> dest, size_t& off, const void* src, const size_t len)
{
    std::memcpy(dest.subspan(off, len).data(), src, len);
    off += len;
}

/// Read the raw bytes of a single trivially copyable header field from the stream.
template <typename T>
void readField(std::ifstream& ifs, T& field)
{
    std::array<char, sizeof(T)> raw{};
    ifs.read(raw.data(), raw.size());
    std::memcpy(&field, raw.data(), sizeof(T));
}
}

size_t sealedCountOffset(const size_t schemaTextLen)
{
    return HEADER_FIXED_BYTES + sizeof(uint32_t) + schemaTextLen + sizeof(uint64_t) + sizeof(uint32_t);
}

size_t segmentTableOffset(const size_t schemaTextLen)
{
    return HEADER_FIXED_BYTES + sizeof(uint32_t) + schemaTextLen + SEGMENT_HEADER_EXTENSION_BYTES;
}

size_t segmentSlotOffset(const size_t schemaTextLen, const uint32_t index)
{
    return segmentTableOffset(schemaTextLen) + (static_cast<size_t>(index) * SEGMENT_DESCRIPTOR_BYTES);
}

size_t dataStartOffsetFor(const size_t schemaTextLen, const uint32_t maxSegments)
{
    return segmentTableOffset(schemaTextLen) + (static_cast<size_t>(maxSegments) * SEGMENT_DESCRIPTOR_BYTES);
}

std::string serializeHeader(
    const std::string& schemaText, uint64_t minTs, uint64_t maxTs, uint64_t segmentSize, uint32_t maxSegments)
{
    const uint64_t fingerprint = fnv1a64(schemaText.c_str(), schemaText.size());
    const auto schemaLen = static_cast<uint32_t>(schemaText.size());

    /// The table is written out zeroed; each slot is filled in place as its segment seals.
    const size_t headerSize = dataStartOffsetFor(schemaLen, maxSegments);
    std::string buf;
    buf.resize(headerSize);
    const std::span bufSpan{buf};
    size_t off = 0;

    writeBytes(bufSpan, off, MAGIC.data(), MAGIC.size());
    writeBytes(bufSpan, off, &VERSION, sizeof(uint32_t));
    writeBytes(bufSpan, off, &ENDIANNESS_LE, sizeof(uint8_t));
    const uint32_t flags = 0;
    writeBytes(bufSpan, off, &flags, sizeof(uint32_t));
    writeBytes(bufSpan, off, &fingerprint, sizeof(uint64_t));
    writeBytes(bufSpan, off, &minTs, sizeof(uint64_t));
    writeBytes(bufSpan, off, &maxTs, sizeof(uint64_t));
    writeBytes(bufSpan, off, &schemaLen, sizeof(uint32_t));
    writeBytes(bufSpan, off, schemaText.data(), schemaLen);

    /// Segment fields, in the order sealedCountOffset() and segmentTableOffset() assume.
    writeBytes(bufSpan, off, &segmentSize, sizeof(uint64_t));
    writeBytes(bufSpan, off, &maxSegments, sizeof(uint32_t));
    const uint32_t sealedCount = 0;
    writeBytes(bufSpan, off, &sealedCount, sizeof(uint32_t));

    /// The remainder of `buf` is the preallocated table, already zeroed by resize().
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
    readField(ifs, header.version);
    readField(ifs, header.endianness);
    readField(ifs, header.flags);
    readField(ifs, header.fingerprint);
    readField(ifs, header.minTs);
    readField(ifs, header.maxTs);
    readField(ifs, schemaLen);
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

    ifs.read(reinterpret_cast<char*>(&header.segmentSize), sizeof(header.segmentSize));
    ifs.read(reinterpret_cast<char*>(&header.maxSegments), sizeof(header.maxSegments));
    ifs.read(reinterpret_cast<char*>(&header.sealedCount), sizeof(header.sealedCount));
    if (!ifs)
    {
        throw CannotOpenSink("Failed to read segment header extension");
    }

    /// The table is sized to what one file can hold before it rolls, so a file can never outgrow
    /// it: the slot count and the roll point are the same number. Only the sealed slots carry
    /// anything; the rest are still zeroed.
    PRECONDITION(
        header.sealedCount <= header.maxSegments,
        "header claims {} sealed segments out of {} slots",
        header.sealedCount,
        header.maxSegments);

    header.segments.resize(header.sealedCount);
    for (uint32_t i = 0; i < header.sealedCount; ++i)
    {
        auto& segment = header.segments[i];
        ifs.read(reinterpret_cast<char*>(&segment.byteOffset), sizeof(segment.byteOffset));
        ifs.read(reinterpret_cast<char*>(&segment.byteLength), sizeof(segment.byteLength));
        ifs.read(reinterpret_cast<char*>(&segment.rowCount), sizeof(segment.rowCount));
        ifs.read(reinterpret_cast<char*>(&segment.minTs), sizeof(segment.minTs));
        ifs.read(reinterpret_cast<char*>(&segment.maxTs), sizeof(segment.maxTs));
    }
    if (!ifs)
    {
        throw CannotOpenSink("Failed to read segment table");
    }

    return {header, dataStartOffsetFor(schemaLen, header.maxSegments)};
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
