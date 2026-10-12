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
#include <atomic>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <BinaryStoreWriter.hpp>
#include <CompressedSegmentFormat.hpp>
#include <CompressionCodec.hpp>
#include <FlushPolicy.hpp>
#include <Store.hpp>
#include <StoreTransformation.hpp>
#include <TimeRange.hpp>

namespace NES
{

// Lossless, segment-based compressed store backed by a file (one [SegmentHeader][compressed bytes]
// record per sealed segment; see CompressedSegmentFormat.hpp). A peer of MemoryStore/FileStore:
// receives data from a prior tier and chains to a next one. Sealing a segment compresses the
// whole active buffer at once.
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

        // Seal and compress the active segment once its raw size reaches this.
        size_t segmentSize = 1UZ * 1024UZ * 1024UZ; // 1 MB
    };

    // Decompressed row-major bytes of every sealed segment plus the pending buffer, for the tier draining this store.
    struct DrainedData
    {
        std::vector<uint8_t> bytes;
        Timestamp minTs{Timestamp(Timestamp::INVALID_VALUE)};
        Timestamp maxTs{Timestamp(Timestamp::INITIAL_VALUE)};
    };

    /// Standalone (tail) store — no next level.
    CompressedStore(Config config, const Schema& schema);

    // Chained store: flushes to nextLevel when the policy trips.
    CompressedStore(Config config, const Schema& schema, Store nextLevel, FlushPolicy policy);

    ~CompressedStore();

    CompressedStore(const CompressedStore&) = delete;
    CompressedStore& operator=(const CompressedStore&) = delete;
    CompressedStore(CompressedStore&&) = delete;
    CompressedStore& operator=(CompressedStore&&) = delete;

    void open();
    void close(Store& self);
    void flush(Store& self);

    void writeRecord(const uint8_t* recordData, uint32_t recordSize, Timestamp ts, Store& self);
    uint64_t read(TupleBuffer& buffer, const Schema& readSchema, const TimeRange& range, StoreReadCursor& cursor, size_t depth);
    [[nodiscard]] bool hasMore() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] uint64_t size() const;

    [[nodiscard]] static std::string_view typeName() noexcept { return "CompressedStore"; }

    void removeFile();

    [[nodiscard]] const std::string& getFilePath() const { return filePath; }

    // Take one segment from the tier above with its range; accumulate into the active block and
    // seal at segmentSize. Tiers don't share a segment size, so several accumulate into one block here.
    void appendSegment(const uint8_t* data, size_t len, uint64_t rowCount, Timestamp minTs, Timestamp maxTs);

    // Read every sealed segment (decompressed) plus the pending buffer as one row-major blob, then reset to empty.
    [[nodiscard]] DrainedData drain();

    /// test introspection
    [[nodiscard]] uint64_t debugActiveRowCount() const;
    [[nodiscard]] uint64_t debugSegmentCount() const;
    // Each sealed segment's [minTs, maxTs], in seal order.
    [[nodiscard]] std::vector<std::pair<uint64_t, uint64_t>> debugSegmentRanges() const;
    // Count of real decompressions, for resume-behaviour tests.
    [[nodiscard]] uint64_t debugDecodeCount() const { return decodeCount.load(std::memory_order_relaxed); }

private:
    // One sealed segment on disk: its file offset and header, enough to prune by range and pread it.
    struct SegmentIndexEntry
    {
        uint64_t offset;
        SegmentHeader header;
    };

    // Compress the active buffer, write [header][payload], reset it. Caller holds the lock.
    void sealActiveSegment();

    // Total bytes stored (sealed compressed size + pending buffer). Caller holds the lock.
    [[nodiscard]] uint64_t currentSizeLocked() const;

    Config config;
    Schema schema;
    CompressionCodec codec;

    /// Active (uncompressed) buffer being filled before the next seal.
    std::vector<uint8_t> activeBuffer;
    Timestamp activeMinTs{Timestamp(Timestamp::INVALID_VALUE)};
    Timestamp activeMaxTs{Timestamp(Timestamp::INITIAL_VALUE)};
    uint64_t activeRowCount{0};

    /// On-disk backing file for sealed segments.
    std::string filePath;
    BinaryStoreWriter writer;
    bool writerOpened{false};
    // Byte offset where segment data begins; drain() truncates the file back to it.
    uint64_t dataStartOffset{0};
    Timestamp fileMinTs{Timestamp(Timestamp::INVALID_VALUE)};
    Timestamp fileMaxTs{Timestamp(Timestamp::INITIAL_VALUE)};

    /// Lightweight index of sealed segments (headers only — payload lives on disk).
    std::vector<SegmentIndexEntry> segments;

    // Bumped when segments is emptied, invalidating any index into it.
    uint64_t readGeneration{0};

    bool opened{false};
    mutable std::shared_mutex mutex;

    // Counts real decompressions for resume tests; atomic because read holds only a shared lock.
    mutable std::atomic<uint64_t> decodeCount{0};

    // Chaining support (empty for a standalone/tail store).
    std::optional<Store> nextLevel;
    std::optional<StoreTransformation> transformation;
    std::optional<FlushPolicy> flushPolicy;
};

}