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
#include <FlushPolicy.hpp>
#include <ReplayStoreFormat.hpp>
#include <Store.hpp>
#include <StoreTransformation.hpp>
#include <TimeRange.hpp>

namespace NES
{

/// File-backed store wrapping BinaryStoreWriter/ReplayStoreReader. Satisfies StoreConcept.
/// Optionally chains to a next-level store with a flush policy and transformation.
class FileStore
{
public:
    struct Config
    {
        std::string storeName;
        std::string storeDir;
        std::string schemaText;

        /// Bytes per segment. Buffers accumulate into the segment being filled; once it reaches
        /// this size the segment seals and its descriptor is written into the header.
        ///
        /// The segment is also the unit a read skips on, so this is the granularity of time-range
        /// pruning: a query that touches one minute of a file still reads whole segments.
        size_t segmentSize = 16UZ * 1024UZ;

        /// How much data the file may hold before the flush policy drains it. Fixes how many
        /// descriptor slots the header preallocates, at ceil(totalSize / segmentSize) of them.
        ///
        /// Preallocated means written out zeroed whether or not a segment ever fills them, so an
        /// over-large value is dead weight in every file: at 64 MB over 64 KB segments the table
        /// was 1024 slots and 40 KB, which a file holding 88 KB of rows paid in full for 1,022
        /// slots it never used. Sized here to one flush cycle's worth, which is what the file can
        /// actually accumulate before the policy drains it.
        size_t totalSize = 8UZ * 1024UZ * 1024UZ;
    };

    /// One segment handed to the tier below: its bytes, and the range of the rows inside them.
    ///
    struct DrainedSegment
    {
        std::vector<uint8_t> bytes;
        uint64_t rowCount{0};
        Timestamp minTs{Timestamp(Timestamp::INVALID_VALUE)};
        Timestamp maxTs{Timestamp(Timestamp::INITIAL_VALUE)};
    };

    /// Standalone constructor (no chaining).
    explicit FileStore(Config config, const Schema& schema);

    /// Chained constructor: this store flushes to nextLevel when the policy triggers.
    FileStore(Config config, const Schema& schema, Store nextLevel, StoreTransformation transformation, FlushPolicy policy);

    ~FileStore();

    FileStore(const FileStore&) = delete;
    FileStore& operator=(const FileStore&) = delete;
    FileStore(FileStore&&) = delete;
    FileStore& operator=(FileStore&&) = delete;

    void open();
    void close(Store& self);
    void flush(Store& self);

    void writeRecord(const uint8_t* recordData, uint32_t recordSize, Timestamp ts, Store& self);

    /// Append one sealed buffer together with the range of the rows in it (used by
    /// MemoryToFileTransformation).
    void appendBuffer(const uint8_t* data, size_t len, uint64_t rowCount, Timestamp minTs, Timestamp maxTs);

    uint64_t read(TupleBuffer& buffer, const Schema& readSchema, const TimeRange& range, StoreReadCursor& cursor, size_t depth);
    [[nodiscard]] bool hasMore() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] uint64_t size() const;

    [[nodiscard]] static std::string_view typeName() noexcept { return "FileStore"; }

    [[nodiscard]] const std::string& getFilePath() const { return filePath; }

    void removeFile();

    /// Calculate packed row width from schema (no padding, matching binary format).
    static uint32_t calculateRowWidth(const Schema& schema);

    /// Read every sealed segment out of the file, then truncate it back to just the header.
    /// The store is left empty (data has moved to the next tier). A partially filled segment is
    /// sealed first, so nothing is left behind.
    [[nodiscard]] std::vector<DrainedSegment> drainSegments();

    /// test introspection
    [[nodiscard]] size_t debugSegmentCount() const;

private:
    /// Write the active segment's descriptor into its preallocated slot and start a new one.
    /// PRECONDITION: the caller holds `mutex`.
    void sealActiveSegment();

    Config config;
    Schema schema;
    std::string filePath;
    Timestamp fileMinTs{Timestamp(Timestamp::INVALID_VALUE)};
    Timestamp fileMaxTs{Timestamp(Timestamp::INITIAL_VALUE)};
    BinaryStoreWriter writer;
    uint64_t dataStartOffset{0};
    bool writerOpened{false};
    mutable std::shared_mutex mutex;

    /// Sealed segments,this one exists so reads and drains need no parsing.
    std::vector<SegmentDescriptor> segments;

    /// Bumped whenever `segments` is emptied, i.e. whenever an index into it stops meaning what it
    /// did. Appending never invalidates an index, so sealing does not bump it.
    uint64_t readGeneration{0};
    /// The segment being filled. Sealed once it reaches `config.segmentSize`, or at flush.
    SegmentDescriptor activeSegment{};
    uint32_t maxSegments{0};

    /// Chaining support (all optional — empty for standalone/tail stores).
    std::optional<Store> nextLevel;
    std::optional<StoreTransformation> transformation;
    std::optional<FlushPolicy> flushPolicy;
};

}
