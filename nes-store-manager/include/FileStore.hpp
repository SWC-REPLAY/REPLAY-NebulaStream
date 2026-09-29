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
        size_t segmentSize = 64UZ * 1024UZ;

        /// How much this store may hold on disk, across every file it has rolled through. Once the
        /// total passes this, the OLDEST file is unlinked -- so the store keeps the newest
        /// `totalSize` worth of history and a write is never refused.
        ///
        /// This is the capacity, not the size of a file. A file holds `filesAtCapacity` fewer bytes
        /// than this, which is what makes eviction an unlink of one whole file rather than a rewrite.
        ///
        /// Deliberately NOT the history size from the query. That figure already decides the memory
        /// buffer and MemoryStore's flush threshold, and how much a file store may keep is a third
        /// question with no relation to it -- tying them made a '1MB' history reserve 16 descriptor
        /// slots while the default reserved 1024, so two chains differed by 40 KB of zeroed table
        /// for reasons nobody chose. Set here so the geometry is identical wherever a store is built.
        size_t totalSize = 64UZ * 1024UZ * 1024UZ;

        /// How many files the store is spread over when it is full, which fixes both the roll point
        /// (totalSize / filesAtCapacity bytes per file) and how coarsely eviction bites: dropping
        /// the oldest file discards that fraction of the history at once.
        ///
        /// The roll point is also the slot count: a file's descriptor table is preallocated to
        /// exactly the segments that file can hold, so it is always right-sized and can never be
        /// outgrown. That is only true because the file is bounded -- unbounded, the same table was
        /// a cap that silently stopped recording descriptors once it filled.
        size_t filesAtCapacity = 16;

        /// Bytes this store accumulates before migrating to the tier below.
        ///
        /// Zero means one file's worth -- totalSize / filesAtCapacity -- which is the natural
        /// unit, since the tier below then receives whole files rather than whatever the tier
        /// above happened to hand over.
        ///
        /// Raise it to make the store span SEVERAL files before draining. That is also what makes
        /// rolling engage: a threshold at or below one file's capacity migrates just before the
        /// file could fill, so the store never rolls.
        size_t flushThresholdBytes = 0;
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

    /// The file currently being written. Once the store has rolled there are older ones behind it;
    /// getFilePaths returns all of them, oldest first.
    [[nodiscard]] const std::string& getFilePath() const;
    [[nodiscard]] std::vector<std::string> getFilePaths() const;

    void removeFile();

    /// Calculate packed row width from schema (no padding, matching binary format).
    static uint32_t calculateRowWidth(const Schema& schema);

    /// Read every sealed segment out of the file, then truncate it back to just the header.
    /// The store is left empty (data has moved to the next tier). A partially filled segment is
    /// sealed first, so nothing is left behind.
    [[nodiscard]] std::vector<DrainedSegment> drainSegments();

    /// test introspection
    [[nodiscard]] size_t debugSegmentCount() const;
    /// How many files the store has rolled through, including the one being written.
    [[nodiscard]] size_t debugFileCount() const;
    /// How many files have been unlinked for being over capacity.
    [[nodiscard]] size_t debugEvictedFileCount() const;
    /// The range the store claims to hold, which read() tests a query against before touching any
    /// unit. Exposed because eviction has to shrink it and nothing else can observe that.
    [[nodiscard]] std::pair<Timestamp, Timestamp> debugStoreRange() const;

private:
    /// One file in the rolled sequence. The store writes into the last one; the earlier ones are
    /// finished and are kept open so a read can still reach their rows.
    struct Part
    {
        std::unique_ptr<BinaryStoreWriter> writer;
        std::string path;
        /// Segments sealed into THIS file. Offsets are relative to this file's data region, so a
        /// descriptor only means anything together with the part it came from.
        std::vector<SegmentDescriptor> segments;
        uint64_t dataStart{0};
        /// This file's own range, written into its header so a reader can dismiss the whole file.
        Timestamp minTs{Timestamp(Timestamp::INVALID_VALUE)};
        Timestamp maxTs{Timestamp(Timestamp::INITIAL_VALUE)};
    };

    /// Write the active segment's descriptor and payload into the current file, then start a new
    /// segment -- rolling to a new file first if this one has reached `totalSize`.
    /// PRECONDITION: the caller holds `mutex`.
    void sealActiveSegment();

    /// Finish the current file and begin the next. PRECONDITION: the caller holds `mutex`.
    void rollToNextFile();

    /// Open a file for `parts.back()`. PRECONDITION: the caller holds `mutex`.
    void openCurrentPart();

    /// Unlink the oldest files until the store is back under `totalSize`. PRECONDITION: the caller
    /// holds `mutex`.
    void evictOldestOverCapacity();

    /// Segments one file holds before it rolls, which is also its preallocated slot count.
    [[nodiscard]] uint32_t segmentsPerFile() const;

    /// Row bytes only, framing subtracted -- what a FLUSH THRESHOLD is compared against.
    /// PRECONDITION: the caller holds `mutex`.
    [[nodiscard]] uint64_t dataBytesLocked() const;

    /// The whole footprint, framing included -- what CAPACITY is compared against.
    /// PRECONDITION: the caller holds `mutex`.
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
    /// Units that have left the store through eviction. A cursor records a GLOBAL unit position,
    /// so subtracting this turns it into an index into the units still held. Never decreases, so a
    /// position never means two different units.
    uint64_t evictedUnits{0};

    /// Bumped whenever the parts are emptied, i.e. whenever an index into them stops meaning what
    /// it did. Appending never invalidates an index, so sealing does not bump it.
    uint64_t readGeneration{0};
    /// The segment being filled. Sealed once it reaches `config.segmentSize`, or at flush.
    SegmentDescriptor activeSegment{};
    /// Its bytes, held until it seals -- the descriptor written in front of them is not final
    /// until then. Reads serve the unsealed segment from here.
    std::vector<uint8_t> activeBytes;

    /// Chaining support (all optional — empty for standalone/tail stores).
    std::optional<Store> nextLevel;
    std::optional<StoreTransformation> transformation;
    std::optional<FlushPolicy> flushPolicy;
};

}
