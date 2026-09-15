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

        size_t segmentSize = 64UZ * 1024UZ;

        /// How much data the file may hold before the flush policy drains it. Fixes how many
        /// descriptor slots the header preallocates.
        size_t totalSize = 64UZ * 1024UZ * 1024UZ;
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

    uint64_t read(TupleBuffer& buffer, const Schema& readSchema, const TimeRange& range);
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
    /// The segment being filled. Sealed once it reaches `config.segmentSize`, or at flush.
    SegmentDescriptor activeSegment{};
    uint32_t maxSegments{0};

    /// Chaining support (all optional — empty for standalone/tail stores).
    std::optional<Store> nextLevel;
    std::optional<StoreTransformation> transformation;
    std::optional<FlushPolicy> flushPolicy;
};

}
