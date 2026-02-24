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

#include <cstring>
#include <regex>

#include <DataTypes/DataTypeProvider.hpp>
#include <ErrorHandling.hpp>

namespace NES::StoreManager
{

uint64_t fnv1a64(const char* data, size_t len)
{
    uint64_t hash = 1469598103934665603ull; // FNV offset basis
    for (size_t i = 0; i < len; ++i)
    {
        hash ^= static_cast<uint8_t>(data[i]);
        hash *= 1099511628211ull; // FNV prime
    }
    return hash;
}

std::string serializeHeader(const std::string& schemaText)
{
    const uint64_t fingerprint = fnv1a64(schemaText.c_str(), schemaText.size());
    const uint32_t schemaLen = static_cast<uint32_t>(schemaText.size());

    const size_t headerSize = sizeof(MAGIC) + sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint64_t)
        + sizeof(uint32_t) + schemaLen;
    std::string buf;
    buf.resize(headerSize);
    size_t off = 0;

    std::memcpy(buf.data() + off, MAGIC, sizeof(MAGIC));
    off += sizeof(MAGIC);
    std::memcpy(buf.data() + off, &VERSION, sizeof(uint32_t));
    off += sizeof(uint32_t);
    std::memcpy(buf.data() + off, &ENDIANNESS_LE, sizeof(uint8_t));
    off += sizeof(uint8_t);
    uint32_t flags = 0;
    std::memcpy(buf.data() + off, &flags, sizeof(uint32_t));
    off += sizeof(uint32_t);
    std::memcpy(buf.data() + off, &fingerprint, sizeof(uint64_t));
    off += sizeof(uint64_t);
    std::memcpy(buf.data() + off, &schemaLen, sizeof(uint32_t));
    off += sizeof(uint32_t);
    std::memcpy(buf.data() + off, schemaText.data(), schemaLen);

    return buf;
}

std::pair<FileHeader, uint64_t> parseHeader(std::ifstream& ifs)
{
    char magic[8];
    ifs.read(magic, 8);
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

    const uint64_t dataStartOffset = 8 + 4 + 1 + 4 + 8 + 4 + schemaLen;
    return {header, dataStartOffset};
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
        // Strip source qualifier prefix (e.g., "STREAM$ID" -> "ID")
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