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
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/FNV.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES::StoreManager
{

std::string serializeHeader(const std::string& schemaText)
{
    const uint64_t fingerprint = fnv1a64(schemaText.c_str(), schemaText.size());
    const auto schemaLen = static_cast<uint32_t>(schemaText.size());

    const size_t headerSize
        = MAGIC.size() + sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t) + schemaLen;
    std::string buf;
    buf.resize(headerSize);
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
    std::memcpy(buf.data() + off, &schemaLen, sizeof(uint32_t));
    off += sizeof(uint32_t);
    std::memcpy(buf.data() + off, schemaText.data(), schemaLen);

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

std::string serializeSchemaText(const StoreSchema& schema)
{
    std::string text;
    for (const auto& field : schema)
    {
        text += fmt::format("{} {}\n", field.getFullyQualifiedName(), magic_enum::enum_name(field.getDataType().type));
    }
    return text;
}

StoreSchema parseSchemaFromText(const std::string& schemaText)
{
    std::vector<QualifiedUnboundField> fields;
    std::istringstream lines(schemaText);
    std::string name;
    std::string type;
    while (lines >> name >> type)
    {
        fields.emplace_back(QualifiedIdentifierBase<std::dynamic_extent>::parse(name), DataTypeProvider::provideDataType(type));
    }
    if (fields.empty())
    {
        throw InvalidConfigParameter("parseSchemaFromText: no fields parsed from: {}", schemaText);
    }
    return StoreSchema{std::move(fields)};
}

}
