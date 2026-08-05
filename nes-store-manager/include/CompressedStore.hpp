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
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <CompressedSegmentFormat.hpp>
#include <CompressionCodec.hpp>
#include <FlushPolicy.hpp>
#include <Store.hpp>
#include <StoreTransformation.hpp>
#include <TimeRange.hpp>

namespace NES::StoreManager
{

/// Lossless, segment-based, compressed store. A full peer of MemoryStore/FileStore:
/// it can RECEIVE data from a prior layer (via a transformation calling
/// appendRawBytes) and CHAIN to a next layer (flush forwards downstream), with the
/// same method duties as MemoryStore.
///
/// Duties (mirroring MemoryStore):
///   writeRecord : append into the active buffer; seal to a segment when full;
///                 flush downstream if the flush policy trips.
///   flush       : seal the active buffer into a compressed segment (finalize own
///                 pending data), THEN transformation->execute + nextLevel->flush.
///   read        : read nextLevel first, then own segments.
///   open/close  : propagate to nextLevel.
///
/// Sealing a segment = compressing the whole active buffer once. It is the direct
/// analog of MemoryStore sealing its active TupleBuffer into `buffers`.
class CompressedStore
{
public:
    struct Config
    {
        std::string storeName;
        std::string storeDir;
        std::string schemaText;

        CompressionCodec::Type codec = CompressionCodec::Type::ZSTD;
        int compressionLevel = 3; /// zstd: 1 (fast) .. 22 (small)

        /// Seal + compress the active segment once its raw size reaches this many bytes.
        size_t segmentSize = 1UZ * 1024UZ * 1024UZ; /// 1 MB
    };

    /// Standalone (tail) store — no next level.
    CompressedStore(Config config, const Schema& schema);

    /// Chained store: flushes to `nextLevel` when the policy trips. Looks up the
    /// "CompressedStore -> <nextLevel>" transformation in the registry, exactly
    /// like MemoryStore's chained constructor.
    CompressedStore(Config config, const Schema& schema, Store nextLevel, FlushPolicy policy);

    ~CompressedStore();

    CompressedStore(const CompressedStore&) = delete;
    CompressedStore& operator=(const CompressedStore&) = delete;
    CompressedStore(CompressedStore&&) = delete;
    CompressedStore& operator=(CompressedStore&&) = delete;

    void open();
    void close(Store& self);
    void flush(Store& self);

    void writeRecord(const uint8_t* recordData, uint32_t recordSize, Timestamp ts, const Schema& writeSchema, Store& self);
    uint64_t read(TupleBuffer& buffer, const Schema& readSchema, const TimeRange& range);
    [[nodiscard]] bool hasMore() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] uint64_t size() const;

    [[nodiscard]] static std::string_view typeName() noexcept { return "CompressedStore"; }

    void removeFile();

    /// ---- bulk ingest from a prior layer (used by a transformation) ---------
    /// Mirrors FileStore::appendRawBytes / updateFileTimestamps.
    void appendRawBytes(const uint8_t* data, size_t len);
    void updateTimestamps(Timestamp minTs, Timestamp maxTs);
    /// ----------------------------------------------------------------------

    /// test introspection
    [[nodiscard]] uint64_t debugActiveRowCount() const;
    [[nodiscard]] uint64_t debugSegmentCount() const;

private:
    /// One sealed, compressed segment: metadata header + compressed bytes.
    struct CompressedSegment
    {
        SegmentHeader header;
        std::vector<uint8_t> payload;
    };

    /// Compress the active buffer into a segment and reset it.
    /// PRECONDITION: the caller already holds `mutex` (unique lock).
    void sealActiveSegment();

    /// Total bytes currently stored (compressed segments + pending buffer).
    /// PRECONDITION: the caller already holds `mutex`.
    [[nodiscard]] uint64_t currentSizeLocked() const;

    Config config;
    Schema schema;
    CompressionCodec codec;

    /// Active (uncompressed) buffer being filled before the next seal.
    std::vector<uint8_t> activeBuffer;
    Timestamp activeMinTs{Timestamp(Timestamp::INVALID_VALUE)};
    Timestamp activeMaxTs{Timestamp(Timestamp::INITIAL_VALUE)};
    uint64_t activeRowCount{0};

    /// Sealed, compressed segments (held in memory in this version).
    std::vector<CompressedSegment> segments;

    bool opened{false};
    mutable std::shared_mutex mutex;

    /// Chaining support (all empty for a standalone/tail store), mirroring
    /// MemoryStore/FileStore.
    std::optional<Store> nextLevel;
    std::optional<StoreTransformation> transformation;
    std::optional<FlushPolicy> flushPolicy;
};

}