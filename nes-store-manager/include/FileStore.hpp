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
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <BinaryStoreWriter.hpp>
#include <ErrorHandling.hpp>
#include <FlushPolicy.hpp>
#include <ReplayStoreFormat.hpp>
#include <Store.hpp>
#include <StoreTransformation.hpp>
#include <TimeRange.hpp>

namespace NES
{

// File-backed store of rolled, segmented files; optionally chains to a next-level store. Satisfies StoreConcept.
class FileStore
{
public:
    struct Config
    {
        std::string storeName;
        std::string storeDir;
        std::string schemaText;

        // Bytes per segment; also the unit a read skips on (the time-range pruning granularity).
        size_t segmentSize = 64UZ * 1024UZ;

        // Max bytes on disk across all rolled files; once exceeded, the oldest whole file is unlinked.
        size_t totalSize = 64UZ * 1024UZ * 1024UZ;

        // Files the store spreads over when full; fixes the roll point and the per-file slot count.
        size_t filesAtCapacity = 16;

        // Bytes accumulated before draining to the tier below; zero means one file's worth.
        size_t flushThresholdBytes = 0;
    };

    // One segment handed to the tier below: its bytes and the range of the rows in them.
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

    // Append one sealed buffer with the range of its rows (used by MemoryToFileTransformation).
    void appendBuffer(const uint8_t* data, size_t len, uint64_t rowCount, Timestamp minTs, Timestamp maxTs);

    uint64_t read(TupleBuffer& buffer, const Schema& readSchema, const TimeRange& range, StoreReadCursor& cursor, size_t depth);
    [[nodiscard]] bool hasMore() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] uint64_t size() const;

    [[nodiscard]] static std::string_view typeName() noexcept { return "FileStore"; }

    // The file currently being written; getFilePaths returns all rolled files, oldest first.
    [[nodiscard]] const std::string& getFilePath() const;
    [[nodiscard]] std::vector<std::string> getFilePaths() const;

    void removeFile();

    /// Calculate packed row width from schema (no padding, matching binary format).
    static uint32_t calculateRowWidth(const Schema& schema);

    // Read every sealed segment out (sealing any partial one first), then truncate the file to its header.
    [[nodiscard]] std::vector<DrainedSegment> drainSegments();

    /// test introspection
    [[nodiscard]] size_t debugSegmentCount() const;
    /// How many files the store has rolled through, including the one being written.
    [[nodiscard]] size_t debugFileCount() const;
    /// How many files have been unlinked for being over capacity.
    [[nodiscard]] size_t debugEvictedFileCount() const;
    // The range the store claims to hold; exposed because eviction shrinks it unobservably otherwise.
    [[nodiscard]] std::pair<Timestamp, Timestamp> debugStoreRange() const;

private:
    // One rolled file; the last is being written, earlier ones stay open for reads.
    struct Part
    {
        std::unique_ptr<BinaryStoreWriter> writer;
        std::string path;
        // Segments sealed into THIS file; offsets are relative to this file's data region.
        std::vector<SegmentDescriptor> segments;
        uint64_t dataStart{0};
        /// This file's own range, written into its header so a reader can dismiss the whole file.
        Timestamp minTs{Timestamp(Timestamp::INVALID_VALUE)};
        Timestamp maxTs{Timestamp(Timestamp::INITIAL_VALUE)};

        // Global position of this part's first unit; assigned at creation and never changed, so eviction needs no fixup.
        uint64_t firstUnit{0};

        // Whether this part holds the given global unit; the count of units here is segments.size().
        [[nodiscard]] bool holds(const uint64_t globalUnit) const
        {
            return globalUnit >= firstUnit && (globalUnit - firstUnit) < segments.size();
        }

        // Offset into this part's segments; exact because a part's units are consecutive (checked).
        [[nodiscard]] const SegmentDescriptor& unitAt(const uint64_t globalUnit) const
        {
            PRECONDITION(holds(globalUnit), "part starting at unit {} does not hold unit {}", firstUnit, globalUnit);
            return segments[globalUnit - firstUnit];
        }
    };

    // Where one unit physically lives: a descriptor plus the writer and data start that locate it.
    struct SegmentLocation
    {
        /// Global position, so a cursor can resume on it without anything having to be renumbered.
        uint64_t unit{0};
        SegmentDescriptor descriptor;
        /// Null for the UNSEALED segment, whose bytes are not on disk and are served from memory.
        BinaryStoreWriter* writer{nullptr};
        uint64_t dataStart{0};
    };

    /// What one read can serve.
    struct ReadableUnits
    {
        std::vector<SegmentLocation> locations;
        // Set only when the walk ran out of units; that alone exhausts a cursor (a full buffer does not).
        bool reachedEnd{false};
    };

    // Resolve a global unit to the file holding it; nullopt if evicted or past the last sealed unit.
    // Caller holds the lock.
    [[nodiscard]] std::optional<SegmentLocation> locateUnitLocked(uint64_t globalUnit) const;

    // The units one read can serve from fromUnit onward, skipping ones the range cannot match and
    // stopping once they can supply capacityRows. Caller holds the lock.
    [[nodiscard]] ReadableUnits collectReadableLocked(
        uint64_t fromUnit, uint64_t fromRowOffset, uint64_t capacityRows, const TimeRange& range, uint32_t tupleSize) const;

    // Write the active segment and start a new one, rolling to a new file first if this one is full.
    // Caller holds the lock.
    void sealActiveSegment();

    /// Finish the current file and begin the next. PRECONDITION: the caller holds `mutex`.
    void rollToNextFile();

    /// Open a file for `parts.back()`. PRECONDITION: the caller holds `mutex`.
    void openCurrentPart();

    // Unlink the oldest files until the store is back under totalSize. Caller holds the lock.
    void evictOldestOverCapacity();

    /// Segments one file holds before it rolls, which is also its preallocated slot count.
    [[nodiscard]] uint32_t segmentsPerFile() const;

    // Row bytes only, framing excluded -- what a flush threshold compares against. Caller holds the lock.
    [[nodiscard]] uint64_t dataBytesLocked() const;

    // Whole footprint, framing included -- what capacity compares against. Caller holds the lock.
    [[nodiscard]] uint64_t storedBytesLocked() const;

    [[nodiscard]] Part& current() { return parts.back(); }
    [[nodiscard]] const Part& current() const { return parts.back(); }

    Config config;
    Schema schema;
    Timestamp fileMinTs{Timestamp(Timestamp::INVALID_VALUE)};
    Timestamp fileMaxTs{Timestamp(Timestamp::INITIAL_VALUE)};
    bool writerOpened{false};
    mutable std::shared_mutex mutex;

    /// The rolled files, oldest first; the last is the one being written. Always non-empty.
    std::vector<Part> parts;
    /// Distinguishes one rolled file from the next, since two rolls can land in the same millisecond.
    uint32_t nextPartNumber{0};
    /// Files unlinked for being over capacity, for tests and for reporting.
    size_t evictedFiles{0};

    // Bumped when parts are emptied, invalidating any index into them.
    uint64_t readGeneration{0};
    /// The segment being filled. Sealed once it reaches `config.segmentSize`, or at flush.
    SegmentDescriptor activeSegment{};
    // The active segment's bytes, held until it seals; reads serve the unsealed segment from here.
    std::vector<uint8_t> activeBytes;

    /// Chaining support (all optional — empty for standalone/tail stores).
    std::optional<Store> nextLevel;
    std::optional<StoreTransformation> transformation;
    std::optional<FlushPolicy> flushPolicy;
};

}
