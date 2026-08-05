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
#include <cstring>

namespace NES::StoreManager
{

/// On-disk layout of the compressed store:
///
///   [ file header  ]   (reuse ReplayStoreFormat: MAGIC, VERSION, schema, file minTs/maxTs)
///   [ SegmentHeader ][ compressed bytes ]   <- segment 0
///   [ SegmentHeader ][ compressed bytes ]   <- segment 1
///   ...
///
/// Each segment is compressed AS A WHOLE. The SegmentHeader is stored
/// UNCOMPRESSED, in front of its payload, for two reasons:
///   1. minTs/maxTs let read() skip a whole segment via TimeRange::overlaps()
///      WITHOUT decompressing it (this is the main speed win).
///   2. uncompressedBytes tells the codec how big the output buffer must be;
///      compressedBytes tells us how many bytes the payload occupies on disk.
struct SegmentHeader
{
    uint64_t minTs{0};             /// smallest timestamp of any row in this segment
    uint64_t maxTs{0};             /// largest timestamp of any row in this segment
    uint64_t rowCount{0};          /// number of rows packed into this segment
    uint64_t uncompressedBytes{0}; /// size of the raw (pre-compression) segment payload
    uint64_t compressedBytes{0};   /// size of the compressed payload on disk
    uint8_t codecId{0};            /// which CompressionCodec::Type produced it
};

/// Fixed on-disk size of a SegmentHeader: five u64 + one u8 = 41 bytes.
constexpr size_t SEGMENT_HEADER_BYTES = (5 * 8) + 1;

namespace detail
{
/// Little-endian write of a u64 into buf at offset; returns new offset.
inline size_t putU64(std::array<uint8_t, SEGMENT_HEADER_BYTES>& buf, size_t off, uint64_t v)
{
    for (int i = 0; i < 8; ++i)
    {
        buf[off + static_cast<size_t>(i)] = static_cast<uint8_t>((v >> (8 * i)) & 0xFFU);
    }
    return off + 8;
}

/// Little-endian read of a u64 from buf at offset; advances offset.
inline uint64_t getU64(const uint8_t* buf, size_t& off)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
    {
        v |= static_cast<uint64_t>(buf[off + static_cast<size_t>(i)]) << (8 * i);
    }
    off += 8;
    return v;
}
}

/// Serialize a SegmentHeader to a fixed 41-byte, little-endian buffer.
[[nodiscard]] inline std::array<uint8_t, SEGMENT_HEADER_BYTES> serializeSegmentHeader(const SegmentHeader& h)
{
    std::array<uint8_t, SEGMENT_HEADER_BYTES> buf{};
    size_t off = 0;
    off = detail::putU64(buf, off, h.minTs);
    off = detail::putU64(buf, off, h.maxTs);
    off = detail::putU64(buf, off, h.rowCount);
    off = detail::putU64(buf, off, h.uncompressedBytes);
    off = detail::putU64(buf, off, h.compressedBytes);
    buf[off] = h.codecId;
    return buf;
}

/// Parse a SegmentHeader from at least SEGMENT_HEADER_BYTES bytes.
[[nodiscard]] inline SegmentHeader parseSegmentHeader(const uint8_t* buf)
{
    SegmentHeader h;
    size_t off = 0;
    h.minTs = detail::getU64(buf, off);
    h.maxTs = detail::getU64(buf, off);
    h.rowCount = detail::getU64(buf, off);
    h.uncompressedBytes = detail::getU64(buf, off);
    h.compressedBytes = detail::getU64(buf, off);
    h.codecId = buf[off];
    return h;
}

}
