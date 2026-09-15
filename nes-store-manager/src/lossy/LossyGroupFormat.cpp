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

#include <lossy/LossyGroupFormat.hpp>

#include <bit>

namespace NES::StoreManager::Lossy
{

namespace
{

void putU64(std::vector<uint8_t>& out, const uint64_t v)
{
    for (int i = 0; i < 8; ++i)
    {
        out.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFFU));
    }
}

void putU32(std::vector<uint8_t>& out, const uint32_t v)
{
    for (int i = 0; i < 4; ++i)
    {
        out.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFFU));
    }
}

uint64_t getU64(const uint8_t* buf, size_t& off)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
    {
        v |= static_cast<uint64_t>(buf[off + static_cast<size_t>(i)]) << (8 * i);
    }
    off += 8;
    return v;
}

uint32_t getU32(const uint8_t* buf, size_t& off)
{
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i)
    {
        v |= static_cast<uint32_t>(buf[off + static_cast<size_t>(i)]) << (8 * i);
    }
    off += 4;
    return v;
}

}

void appendGroupHeader(std::vector<uint8_t>& out, const GroupHeader& header)
{
    putU64(out, header.groupByteLength);
    putU64(out, header.rowCount);
    putU64(out, header.minTs);
    putU64(out, header.maxTs);
    putU32(out, header.numColumns);
}

GroupHeader parseGroupHeader(const uint8_t* buf)
{
    GroupHeader header;
    size_t off = 0;
    header.groupByteLength = getU64(buf, off);
    header.rowCount = getU64(buf, off);
    header.minTs = getU64(buf, off);
    header.maxTs = getU64(buf, off);
    header.numColumns = getU32(buf, off);
    return header;
}

void appendColumnEntry(std::vector<uint8_t>& out, const ColumnEntry& entry)
{
    putU64(out, entry.valuesOffset);
    putU64(out, entry.valuesLength);
    putU64(out, entry.uncompressedLength);
    putU64(out, entry.nullBitsOffset);
    putU64(out, entry.nullBitsLength);
    out.push_back(entry.lossyCodecId);
    out.push_back(entry.losslessCodecId);
    putU64(out, std::bit_cast<uint64_t>(entry.achievedError));
}

ColumnEntry parseColumnEntry(const uint8_t* buf)
{
    ColumnEntry entry;
    size_t off = 0;
    entry.valuesOffset = getU64(buf, off);
    entry.valuesLength = getU64(buf, off);
    entry.uncompressedLength = getU64(buf, off);
    entry.nullBitsOffset = getU64(buf, off);
    entry.nullBitsLength = getU64(buf, off);
    entry.lossyCodecId = buf[off];
    ++off;
    entry.losslessCodecId = buf[off];
    ++off;
    entry.achievedError = std::bit_cast<double>(getU64(buf, off));
    return entry;
}

}
