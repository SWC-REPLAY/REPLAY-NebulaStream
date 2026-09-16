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
#include <BinaryStoreWriter.hpp>
#include <CompressedSegmentFormat.hpp>
#include <CompressionCodec.hpp>
#include <FlushPolicy.hpp>
#include <Store.hpp>
#include <StoreTransformation.hpp>
#include <TimeRange.hpp>

namespace NES
{

/// Lossless, segment-based, compressed store, backed by a file on disk (one
/// [SegmentHeader][compressed bytes] record per sealed segment, per
/// CompressedSegmentFormat.hpp). A full peer of MemoryStore/FileStore:
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

    /// Raw row-major bytes read off of every sealed segment (decompressed) plus the
    /// still-pending active buffer, handed to whatever tier drains this store. Every
    /// transformation boundary in this store chain speaks row-major bytes matching the
    /// schema's packed layout, regardless of how a given tier stores them internally.
    struct DrainedData
    {
        std::vector<uint8_t> bytes;
        Timestamp minTs{Timestamp(Timestamp::INVALID_VALUE)};
        Timestamp maxTs{Timestamp(Timestamp::INITIAL_VALUE)};
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

    void writeRecord(const uint8_t* recordData, uint32_t recordSize, Timestamp ts, Store& self);
    uint64_t read(TupleBuffer& buffer, const Schema& readSchema, const TimeRange& range);
    [[nodiscard]] bool hasMore() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] uint64_t size() const;

    [[nodiscard]] static std::string_view typeName() noexcept { return "CompressedStore"; }

    void removeFile();

    [[nodiscard]] const std::string& getFilePath() const { return filePath; }

    /// ---- bulk ingest from a prior layer (used by a transformation) ---------
    /// Take one whole segment from the tier above, carrying the range its rows already
    /// have. This is writeRecord's path with a segment in place of a row: the bytes go
    /// into the active block, the range is unioned into the active block's range, and
    /// the block seals once it reaches config.segmentSize.
    ///
    /// The tiers do NOT share a segment size, and should not. Each one picks the unit
    /// that suits it -- FileStore 64 KB because that is its transfer and index
    /// granularity, this store 1 MB because that is what zstd compresses at once and a
    /// larger block compresses better. So several segments from above accumulate into
    /// one block here, and that block's range is the union of theirs: narrow enough to
    /// prune on, which is all read() needs.
    void appendSegment(const uint8_t* data, size_t len, uint64_t rowCount, Timestamp minTs, Timestamp maxTs);
    /// ----------------------------------------------------------------------

    /// Read every sealed segment out (decompressing each), plus the pending active
    /// buffer, as one row-major byte blob, then reset this store back to empty (segments
    /// cleared, file truncated to just its header). Used by a transformation to hand this
    /// store's data down to the next tier (e.g. ColumnStore), mirroring FileStore::drain().
    [[nodiscard]] DrainedData drain();

    /// test introspection
    [[nodiscard]] uint64_t debugActiveRowCount() const;
    [[nodiscard]] uint64_t debugSegmentCount() const;
    /// The [minTs, maxTs] of each sealed segment, in seal order. Mirrors LossyStore::debugGroupRanges.
    [[nodiscard]] std::vector<std::pair<uint64_t, uint64_t>> debugSegmentRanges() const;

private:
    /// Points at one sealed segment already written to disk: its file offset
    /// (of the SegmentHeader) plus the header itself, so read() can prune by
    /// timestamp range and know exactly where/how much to pread, without
    /// holding any compressed payload bytes in memory.
    struct SegmentIndexEntry
    {
        uint64_t offset;
        SegmentHeader header;
    };

    /// Compress the active buffer, write [header][payload] to disk, and reset it.
    /// PRECONDITION: the caller already holds `mutex` (unique lock).
    void sealActiveSegment();

    /// Total bytes currently stored (sealed segments' compressed size + pending buffer).
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

    /// On-disk backing file for sealed segments.
    std::string filePath;
    BinaryStoreWriter writer;
    bool writerOpened{false};
    /// Byte offset where segment data begins (i.e. just past the file header). Computed
    /// once in open(); drain() truncates the file back to this offset.
    uint64_t dataStartOffset{0};
    Timestamp fileMinTs{Timestamp(Timestamp::INVALID_VALUE)};
    Timestamp fileMaxTs{Timestamp(Timestamp::INITIAL_VALUE)};

    /// Lightweight index of sealed segments (headers only — payload lives on disk).
    std::vector<SegmentIndexEntry> segments;

    bool opened{false};
    mutable std::shared_mutex mutex;

    /// Chaining support (all empty for a standalone/tail store), mirroring
    /// MemoryStore/FileStore.
    std::optional<Store> nextLevel;
    std::optional<StoreTransformation> transformation;
    std::optional<FlushPolicy> flushPolicy;
};

}