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

/// Tests for the division of the FileStore into segments, and for the one property that makes
/// the division worth having: each segment carries the time range of the rows inside it.
///
/// That range is measured once, in MemoryStore, from the timestamps handed to writeRecord. It is
/// then carried down unchanged. Before this existed, every buffer's range was collapsed into one
/// span covering the whole drain, so every group the lossy tier built from it claimed to hold
/// everything and no read could skip any of them. These tests pin the ranges staying separate.

#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <FileStore.hpp>
#include <ReplayStoreFormat.hpp>
#include <Store.hpp>
#include <StoreReadCursor.hpp>
#include <TimeRange.hpp>

namespace NES
{

class SegmentedFileStoreTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("SegmentedFileStoreTest.log", LogLevel::LOG_DEBUG); }
};

namespace
{

Schema makeSchema()
{
    return Schema{}.addField("ts", DataType::Type::UINT64).addField("value", DataType::Type::UINT64);
}

/// `count` rows, one millisecond apart, starting at `firstTs`. 16 bytes per row.
std::vector<uint8_t> makeRows(const uint64_t count, const uint64_t firstTs)
{
    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < count; ++i)
    {
        const uint64_t ts = firstTs + i;
        const uint64_t value = i;
        const auto* tsBytes = reinterpret_cast<const uint8_t*>(&ts);
        const auto* valueBytes = reinterpret_cast<const uint8_t*>(&value);
        rows.insert(rows.end(), tsBytes, tsBytes + sizeof(ts));
        rows.insert(rows.end(), valueBytes, valueBytes + sizeof(value));
    }
    return rows;
}

FileStore::Config cfgFor(const std::string& name, const size_t segmentSize)
{
    return FileStore::Config{
        .storeName = name,
        .storeDir = "/tmp",
        .schemaText = "",
        .segmentSize = segmentSize,
        .totalSize = 1UZ * 1024UZ * 1024UZ};
}

}

/// The regression this whole change exists for. Two buffers covering disjoint windows must end
/// up as two segments with two different ranges -- not one span covering both.
TEST_F(SegmentedFileStoreTest, BuffersWithDisjointRangesBecomeSeparateSegments)
{
    const Schema schema = makeSchema();
    const auto early = makeRows(64, 1000); /// ts 1000..1063
    const auto late = makeRows(64, 9000); /// ts 9000..9063

    /// One buffer per segment, so the boundaries are unambiguous.
    Store store = makeStore<FileStore>(cfgFor("disjoint", early.size()), schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();

    fileStore.appendBuffer(early.data(), early.size(), 64, Timestamp(1000), Timestamp(1063));
    fileStore.appendBuffer(late.data(), late.size(), 64, Timestamp(9000), Timestamp(9063));

    ASSERT_EQ(fileStore.debugSegmentCount(), 2U);

    const auto segments = fileStore.drainSegments();
    ASSERT_EQ(segments.size(), 2U);

    EXPECT_EQ(segments[0].minTs.getRawValue(), 1000U);
    EXPECT_EQ(segments[0].maxTs.getRawValue(), 1063U);
    EXPECT_EQ(segments[1].minTs.getRawValue(), 9000U);
    EXPECT_EQ(segments[1].maxTs.getRawValue(), 9063U);

    /// The point of keeping them apart: neither range covers the other's window.
    const TimeRange window{.fieldName = "ts", .start = Timestamp(1000), .end = Timestamp(2000)};
    EXPECT_TRUE(window.overlaps(segments[0].minTs, segments[0].maxTs));
    EXPECT_FALSE(window.overlaps(segments[1].minTs, segments[1].maxTs));

    EXPECT_EQ(segments[0].rowCount, 64U);
    EXPECT_EQ(segments[0].bytes, early);
    EXPECT_EQ(segments[1].bytes, late);

    fileStore.removeFile();
}

/// Several buffers inside one segment union into its range rather than overwriting it.
TEST_F(SegmentedFileStoreTest, ASegmentSpansEveryBufferThatFilledIt)
{
    const Schema schema = makeSchema();
    const auto first = makeRows(32, 100);
    const auto second = makeRows(32, 500);

    /// Segment large enough to hold both buffers.
    Store store = makeStore<FileStore>(cfgFor("union", first.size() + second.size()), schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();

    fileStore.appendBuffer(first.data(), first.size(), 32, Timestamp(100), Timestamp(131));
    fileStore.appendBuffer(second.data(), second.size(), 32, Timestamp(500), Timestamp(531));

    const auto segments = fileStore.drainSegments();
    ASSERT_EQ(segments.size(), 1U);
    EXPECT_EQ(segments[0].minTs.getRawValue(), 100U) << "earliest of the two buffers";
    EXPECT_EQ(segments[0].maxTs.getRawValue(), 531U) << "latest of the two buffers";
    EXPECT_EQ(segments[0].rowCount, 64U);

    fileStore.removeFile();
}

/// writeRecord must accumulate into the segment being filled, exactly as MemoryStore
/// accumulates records into the buffer being filled. A segment per record would make the
/// division meaningless -- the descriptor would cost more than the row it describes -- and it
/// would differ from the bulk path for no reason.
TEST_F(SegmentedFileStoreTest, WriteRecordAccumulatesIntoTheActiveSegment)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes(); /// 16

    /// 64 rows per segment.
    Store store = makeStore<FileStore>(cfgFor("accumulate", rowWidth * 64), schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();

    /// Well short of a full segment: nothing may seal yet.
    for (uint64_t i = 0; i < 10; ++i)
    {
        const auto row = makeRows(1, 1000 + i);
        store.writeRecord(row.data(), rowWidth, Timestamp(1000 + i));
    }
    EXPECT_EQ(fileStore.debugSegmentCount(), 0U) << "ten records is not a segment each";

    /// Reaching exactly one segment's worth seals exactly one.
    for (uint64_t i = 10; i < 64; ++i)
    {
        const auto row = makeRows(1, 1000 + i);
        store.writeRecord(row.data(), rowWidth, Timestamp(1000 + i));
    }
    EXPECT_EQ(fileStore.debugSegmentCount(), 1U);

    /// And the next 64 seal the second, not 64 more.
    for (uint64_t i = 64; i < 128; ++i)
    {
        const auto row = makeRows(1, 1000 + i);
        store.writeRecord(row.data(), rowWidth, Timestamp(1000 + i));
    }
    EXPECT_EQ(fileStore.debugSegmentCount(), 2U);

    const auto segments = fileStore.drainSegments();
    ASSERT_EQ(segments.size(), 2U);
    EXPECT_EQ(segments[0].rowCount, 64U);
    EXPECT_EQ(segments[0].minTs.getRawValue(), 1000U);
    EXPECT_EQ(segments[0].maxTs.getRawValue(), 1063U);
    EXPECT_EQ(segments[1].rowCount, 64U);
    EXPECT_EQ(segments[1].minTs.getRawValue(), 1064U);

    fileStore.removeFile();
}

/// A partially filled segment is not lost: draining seals it first.
TEST_F(SegmentedFileStoreTest, DrainSealsThePartialSegment)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(16, 7000);

    Store store = makeStore<FileStore>(cfgFor("partial", 64UZ * 1024UZ), schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();

    fileStore.appendBuffer(rows.data(), rows.size(), 16, Timestamp(7000), Timestamp(7015));
    EXPECT_EQ(fileStore.debugSegmentCount(), 0U) << "nowhere near the segment size yet";

    const auto segments = fileStore.drainSegments();
    ASSERT_EQ(segments.size(), 1U);
    EXPECT_EQ(segments[0].minTs.getRawValue(), 7000U);
    EXPECT_EQ(segments[0].rowCount, 16U);

    fileStore.removeFile();
}

/// `totalSize` caps ONE file, not the store. Once a file has no room for the next segment the
/// store opens another and keeps going, so the cap never costs a write.
TEST_F(SegmentedFileStoreTest, RollsToANewFileWhenTotalSizeIsReached)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(64, 1000); /// 64 rows x 16 B = 1024 B

    /// Two segments per file: capacity 8 KB over 4 files is 2 KB each, which is two 1 KB segments.
    FileStore::Config cfg{
        .storeName = "rolling",
        .storeDir = "/tmp",
        .schemaText = "",
        .segmentSize = rows.size(),
        .totalSize = 8UZ * rows.size(),
        .filesAtCapacity = 4};

    Store store = makeStore<FileStore>(cfg, schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();

    EXPECT_EQ(fileStore.debugFileCount(), 1U) << "one file before anything is written";

    /// Five segments over two-segment files: 2 + 2 + 1.
    for (uint64_t i = 0; i < 5; ++i)
    {
        const auto ts = 1000 + (i * 100);
        fileStore.appendBuffer(rows.data(), rows.size(), 64, Timestamp(ts), Timestamp(ts + 63));
    }

    EXPECT_EQ(fileStore.debugSegmentCount(), 5U) << "every segment is still tracked after rolling";
    EXPECT_EQ(fileStore.debugFileCount(), 3U) << "two segments per file over five segments";
    EXPECT_EQ(fileStore.getFilePaths().size(), 3U);

    /// Each file really is a separate file on disk, and each holds at most its two segments -- so
    /// the preallocated table, sized to exactly that, is never outgrown.
    for (const auto& path : fileStore.getFilePaths())
    {
        std::ifstream ifs(path, std::ios::binary);
        ASSERT_TRUE(ifs.is_open()) << path;
        const auto [header, dataStart] = parseHeader(ifs);
        EXPECT_EQ(header.maxSegments, 2U) << path;
        EXPECT_LE(header.sealedCount, header.maxSegments) << path;
    }

    fileStore.removeFile();
}

/// The point of rolling: once the store is over capacity the OLDEST FILE is unlinked whole. No
/// rewrite, no holes to track -- which is only possible because history is spread over files
/// rather than accumulated in one. The newest rows always land; the oldest leave to make room.
TEST_F(SegmentedFileStoreTest, OldestFileIsEvictedOnceOverCapacity)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(64, 0); /// 1024 B, one segment each

    /// Two segments per file, four files at capacity -- so the store holds roughly 8 KB of rows
    /// and anything older than that is dropped a whole file at a time.
    FileStore::Config cfg{
        .storeName = "evicting",
        .storeDir = "/tmp",
        .schemaText = "",
        .segmentSize = rows.size(),
        .totalSize = 8UZ * rows.size(),
        .filesAtCapacity = 4};

    Store store = makeStore<FileStore>(cfg, schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();

    /// Twenty segments into a store that holds about eight: most of it has to go.
    for (uint64_t i = 0; i < 20; ++i)
    {
        const auto batch = makeRows(64, 1000 + (i * 100));
        fileStore.appendBuffer(batch.data(), batch.size(), 64, Timestamp(1000 + (i * 100)), Timestamp(1063 + (i * 100)));
    }

    EXPECT_GT(fileStore.debugEvictedFileCount(), 0U) << "nothing was ever dropped, so totalSize is not a limit";
    EXPECT_LE(fileStore.size(), cfg.totalSize) << "the store is over the capacity it was given";

    /// Every file still listed exists; the evicted ones are gone from disk entirely.
    const auto paths = fileStore.getFilePaths();
    EXPECT_EQ(paths.size(), fileStore.debugFileCount());
    for (const auto& path : paths)
    {
        const std::ifstream alive(path, std::ios::binary);
        EXPECT_TRUE(alive.is_open()) << path << " is listed but not on disk";
    }

    /// What survives is the NEWEST history, not the oldest. The last segment written starts at
    /// ts 2900, and the earliest survivor must be well past where the run began.
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create(rowWidth * 2048, 8);
    auto buffer = bufferManager->getBufferBlocking();
    const uint64_t got = store.read(buffer, schema, TimeRange{});
    ASSERT_GT(got, 0U);

    const auto area = buffer.getAvailableMemoryArea<const uint8_t>();
    uint64_t firstTs = 0;
    std::memcpy(&firstTs, area.data(), sizeof(firstTs));
    EXPECT_GT(firstTs, 1000U) << "the oldest rows survived, so the wrong end was evicted";

    uint64_t lastTs = 0;
    std::memcpy(&lastTs, area.data() + ((got - 1) * rowWidth), sizeof(lastTs));
    EXPECT_EQ(lastTs, 2963U) << "the newest row should always be present";

    fileStore.removeFile();
}

/// Eviction has to shrink the range the store claims, not just the data it holds. read() opens by
/// testing the query against that claim, so a claim that only ever grows makes the whole-store skip
/// useless the longer the store runs -- it keeps vouching for timestamps it has deleted.
TEST_F(SegmentedFileStoreTest, EvictionNarrowsTheClaimedRange)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(64, 0);
    FileStore::Config cfg{
        .storeName = "evictrange",
        .storeDir = "/tmp",
        .schemaText = "",
        .segmentSize = rows.size(),
        .totalSize = 8UZ * rows.size(),
        .filesAtCapacity = 4};

    Store store = makeStore<FileStore>(cfg, schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();

    for (uint64_t i = 0; i < 20; ++i)
    {
        const auto batch = makeRows(64, 1000 + (i * 100));
        fileStore.appendBuffer(batch.data(), batch.size(), 64, Timestamp(1000 + (i * 100)), Timestamp(1063 + (i * 100)));
    }
    ASSERT_GT(fileStore.debugEvictedFileCount(), 0U) << "nothing was evicted, so there is nothing to check";

    const auto [claimedMin, claimedMax] = fileStore.debugStoreRange();
    EXPECT_GT(claimedMin.getRawValue(), 1000U) << "the store still claims the timestamp of rows it deleted";
    EXPECT_EQ(claimedMax.getRawValue(), 1063U + (19 * 100)) << "the newest row must still be claimed";

    /// The claim must not narrow PAST what is still held either -- too narrow makes read() skip
    /// live rows, which is a wrong answer rather than a missed optimisation.
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create(rowWidth * 2048, 8);
    auto buffer = bufferManager->getBufferBlocking();
    const uint64_t got = store.read(buffer, schema, TimeRange{});
    ASSERT_GT(got, 0U);

    const auto area = buffer.getAvailableMemoryArea<const uint8_t>();
    uint64_t oldestHeld = 0;
    std::memcpy(&oldestHeld, area.data(), sizeof(oldestHeld));
    EXPECT_LE(claimedMin.getRawValue(), oldestHeld) << "the claim excludes rows the store still holds";

    fileStore.removeFile();
}

/// A read in progress must survive an eviction. The cursor records a GLOBAL unit position rather
/// than an index into the units currently held, so dropping the oldest file shifts nothing: the
/// read continues instead of being told the store renumbered itself.
TEST_F(SegmentedFileStoreTest, AReadInProgressSurvivesEviction)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(64, 0);
    FileStore::Config cfg{
        .storeName = "evictcursor",
        .storeDir = "/tmp",
        .schemaText = "",
        .segmentSize = rows.size(),
        .totalSize = 8UZ * rows.size(),
        .filesAtCapacity = 4};

    Store store = makeStore<FileStore>(cfg, schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();

    for (uint64_t i = 0; i < 6; ++i)
    {
        const auto batch = makeRows(64, 1000 + (i * 100));
        fileStore.appendBuffer(batch.data(), batch.size(), 64, Timestamp(1000 + (i * 100)), Timestamp(1063 + (i * 100)));
    }
    ASSERT_EQ(fileStore.debugEvictedFileCount(), 0U) << "nothing should have been evicted yet";

    /// A destination that holds one segment, so the read stops part way and the cursor keeps a
    /// position to come back to.
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create(rowWidth * 64, 8);
    StoreReadCursor cursor;

    auto first = bufferManager->getBufferBlocking();
    const uint64_t firstRead = store.read(first, schema, TimeRange{}, cursor);
    ASSERT_EQ(firstRead, 64U) << "one segment's worth, leaving the cursor mid-store";

    /// Now write enough to push the store over capacity, which unlinks the oldest file underneath
    /// the cursor.
    for (uint64_t i = 6; i < 20; ++i)
    {
        const auto batch = makeRows(64, 1000 + (i * 100));
        fileStore.appendBuffer(batch.data(), batch.size(), 64, Timestamp(1000 + (i * 100)), Timestamp(1063 + (i * 100)));
    }
    ASSERT_GT(fileStore.debugEvictedFileCount(), 0U) << "the point of this test is that it evicted";

    /// Resuming must not throw. Before unit positions were global this raised UnknownOperation,
    /// because the cursor's index no longer pointed at the unit it was written for.
    auto second = bufferManager->getBufferBlocking();
    uint64_t resumed = 0;
    EXPECT_NO_THROW(resumed = store.read(second, schema, TimeRange{}, cursor));
    EXPECT_GT(resumed, 0U) << "the read resumed but returned nothing";

    /// And it must not hand back rows it already delivered.
    const auto area = second.getAvailableMemoryArea<const uint8_t>();
    uint64_t firstTsOfSecondRead = 0;
    std::memcpy(&firstTsOfSecondRead, area.data(), sizeof(firstTsOfSecondRead));
    EXPECT_GE(firstTsOfSecondRead, 1100U) << "the resumed read repeated the first segment";

    fileStore.removeFile();
}

/// A read walks every rolled file, oldest first. Offsets are relative to the file a segment lives
/// in, so reading across a roll is where a descriptor applied to the wrong file would show up.
TEST_F(SegmentedFileStoreTest, ReadsSpanEveryRolledFile)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(64, 0);
    FileStore::Config cfg{
        .storeName = "rollread",
        .storeDir = "/tmp",
        .schemaText = "",
        .segmentSize = rows.size(),
        .totalSize = 64UZ * rows.size(),
        .filesAtCapacity = 32};

    Store store = makeStore<FileStore>(cfg, schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();

    /// Five segments of 64 rows, each segment's timestamps a distinct hundred.
    for (uint64_t i = 0; i < 5; ++i)
    {
        const auto batch = makeRows(64, 1000 + (i * 100));
        fileStore.appendBuffer(batch.data(), batch.size(), 64, Timestamp(1000 + (i * 100)), Timestamp(1063 + (i * 100)));
    }
    ASSERT_GT(fileStore.debugFileCount(), 1U) << "the point of this test is that it rolled";

    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create(rowWidth * 512, 8);
    auto buffer = bufferManager->getBufferBlocking();

    const uint64_t got = store.read(buffer, schema, TimeRange{});
    ASSERT_EQ(got, 320U) << "5 segments x 64 rows, across 3 files";

    /// Rows come back in write order, so the timestamps must run 1000..1063, 1100..1163, and so on.
    const auto area = buffer.getAvailableMemoryArea<const uint8_t>();
    for (uint64_t i = 0; i < got; ++i)
    {
        uint64_t ts = 0;
        std::memcpy(&ts, area.data() + (i * rowWidth), sizeof(ts));
        const uint64_t expected = 1000 + ((i / 64) * 100) + (i % 64);
        ASSERT_EQ(ts, expected) << "row " << i << " read from the wrong file or the wrong offset";
    }

    fileStore.removeFile();
}

/// Draining takes the rolled files with it: their rows have moved to the tier below, so they are
/// deleted and the store is left with the single file it started with.
TEST_F(SegmentedFileStoreTest, DrainCollectsEveryRolledFileAndDeletesThem)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(64, 1000);
    FileStore::Config cfg{
        .storeName = "rolldrain",
        .storeDir = "/tmp",
        .schemaText = "",
        .segmentSize = rows.size(),
        .totalSize = 64UZ * rows.size(),
        .filesAtCapacity = 32};

    Store store = makeStore<FileStore>(cfg, schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();

    for (uint64_t i = 0; i < 5; ++i)
    {
        const auto ts = 1000 + (i * 100);
        fileStore.appendBuffer(rows.data(), rows.size(), 64, Timestamp(ts), Timestamp(ts + 63));
    }
    ASSERT_EQ(fileStore.debugFileCount(), 3U);
    const auto pathsBefore = fileStore.getFilePaths();

    const auto drained = fileStore.drainSegments();
    ASSERT_EQ(drained.size(), 5U) << "every segment, from every file";
    for (const auto& segment : drained)
    {
        EXPECT_EQ(segment.rowCount, 64U);
        EXPECT_EQ(segment.bytes.size(), rows.size());
    }

    EXPECT_EQ(fileStore.debugFileCount(), 1U) << "back to one file";
    EXPECT_EQ(fileStore.debugSegmentCount(), 0U);

    /// The rolled files are gone from disk; the first survives, cut back to its header.
    for (size_t i = 1; i < pathsBefore.size(); ++i)
    {
        const std::ifstream gone(pathsBefore[i], std::ios::binary);
        EXPECT_FALSE(gone.is_open()) << pathsBefore[i] << " should have been deleted";
    }

    fileStore.removeFile();
}

/// The table is in the file, not just in memory: a reader that only has the file must be able to
/// recover every segment's range without reading a single row, in one read of the front of it.
TEST_F(SegmentedFileStoreTest, SegmentTableIsPersistedInTheHeader)
{
    const Schema schema = makeSchema();
    const auto early = makeRows(64, 1000);
    const auto late = makeRows(64, 9000);

    Store store = makeStore<FileStore>(cfgFor("persisted", early.size()), schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();
    fileStore.appendBuffer(early.data(), early.size(), 64, Timestamp(1000), Timestamp(1063));
    fileStore.appendBuffer(late.data(), late.size(), 64, Timestamp(9000), Timestamp(9063));

    const auto path = fileStore.getFilePath();
    std::ifstream ifs(path, std::ios::binary);
    ASSERT_TRUE(ifs.is_open());
    const auto [header, dataStart] = parseHeader(ifs);

    EXPECT_EQ(header.sealedCount, 2U);
    ASSERT_EQ(header.segments.size(), 2U);
    EXPECT_EQ(header.segments[0].minTs, 1000U);
    EXPECT_EQ(header.segments[0].maxTs, 1063U);
    EXPECT_EQ(header.segments[1].minTs, 9000U);
    EXPECT_EQ(header.segments[1].maxTs, 9063U);

    /// Offsets are relative to the data region and the second segment follows the first, because
    /// the descriptors live in the table at the front rather than between the payloads.
    EXPECT_EQ(header.segments[0].byteOffset, 0U);
    EXPECT_EQ(header.segments[1].byteOffset, early.size());

    /// The table is sized to the segments one file holds before rolling, so it is never outgrown.
    EXPECT_GE(header.maxSegments, header.sealedCount);
    EXPECT_EQ(dataStart, dataStartOffsetFor(header.schemaText.size(), header.maxSegments));

    fileStore.removeFile();
}

/// Segments are held in write order, not time order, so a late arrival puts a segment whose
/// range misses the query BETWEEN two that match. Each segment has to be decided on its own:
/// narrowing to a span from the first match to the last would read the one in the middle and
/// then discard its rows.
TEST_F(SegmentedFileStoreTest, NonMatchingSegmentBetweenTwoMatchesIsSkipped)
{
    const Schema schema = makeSchema();
    const auto early = makeRows(64, 1000); /// ts 1000..1063  -- matches
    const auto late = makeRows(64, 9000); /// ts 9000..9063  -- arrives second, does NOT match
    const auto alsoEarly = makeRows(64, 1100); /// ts 1100..1163  -- matches, written last

    Store store = makeStore<FileStore>(cfgFor("interleaved", early.size()), schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();

    fileStore.appendBuffer(early.data(), early.size(), 64, Timestamp(1000), Timestamp(1063));
    fileStore.appendBuffer(late.data(), late.size(), 64, Timestamp(9000), Timestamp(9063));
    fileStore.appendBuffer(alsoEarly.data(), alsoEarly.size(), 64, Timestamp(1100), Timestamp(1163));
    ASSERT_EQ(fileStore.debugSegmentCount(), 3U);

    /// Room for exactly the 128 matching rows and not one more. This is what makes the test
    /// distinguish skipping from filtering: reading a span from the first match to the last
    /// would pull in the middle segment's 64 rows too, fill the buffer with the first 128 rows
    /// read, and then discard half of them -- returning 64. Only skipping the middle segment
    /// outright fits both matching ones.
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create(rowWidth * 128, 8);
    auto buffer = bufferManager->getBufferBlocking();

    /// Spans the first and third segments; the second sits between them in write order but its
    /// range is far outside.
    const TimeRange window{.fieldName = "ts", .start = Timestamp(1000), .end = Timestamp(1200)};
    const uint64_t got = store.read(buffer, schema, window);

    /// 64 from the first plus 64 from the third, and nothing from the middle one.
    ASSERT_EQ(got, 128U);

    const auto area = buffer.getAvailableMemoryArea<const uint8_t>();
    for (uint64_t i = 0; i < got; ++i)
    {
        uint64_t ts = 0;
        std::memcpy(&ts, area.data() + (i * rowWidth), sizeof(ts));
        EXPECT_LT(ts, 2000U) << "row " << i << " came from the segment that should have been skipped";
    }

    fileStore.removeFile();
}

/// A division wholly inside the query needs no row inspected: every row in it qualifies.
TEST_F(SegmentedFileStoreTest, SegmentWhollyInsideTheRangeIsTakenEntire)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(64, 1000); /// ts 1000..1063

    Store store = makeStore<FileStore>(cfgFor("inside", rows.size()), schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();
    fileStore.appendBuffer(rows.data(), rows.size(), 64, Timestamp(1000), Timestamp(1063));

    auto bufferManager = BufferManager::create(16384, 8);
    auto buffer = bufferManager->getBufferBlocking();

    /// Comfortably containing the segment on both sides.
    const TimeRange window{.fieldName = "ts", .start = Timestamp(500), .end = Timestamp(5000)};
    EXPECT_EQ(store.read(buffer, schema, window), 64U);

    fileStore.removeFile();
}

/// Rows written but not yet sealed into a segment must still be readable.
TEST_F(SegmentedFileStoreTest, RowsInTheUnsealedSegmentAreVisibleToReads)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(16, 7000);

    /// Segment size far larger than what is written, so nothing seals.
    Store store = makeStore<FileStore>(cfgFor("unsealed", 64UZ * 1024UZ), schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();
    fileStore.appendBuffer(rows.data(), rows.size(), 16, Timestamp(7000), Timestamp(7015));
    ASSERT_EQ(fileStore.debugSegmentCount(), 0U) << "nothing should have sealed";

    auto bufferManager = BufferManager::create(16384, 8);
    auto buffer = bufferManager->getBufferBlocking();

    const TimeRange window{.fieldName = "ts", .start = Timestamp(7000), .end = Timestamp(7008)};
    EXPECT_EQ(store.read(buffer, schema, window), 8U) << "the segment being filled is still data";

    auto buffer2 = bufferManager->getBufferBlocking();
    EXPECT_EQ(store.read(buffer2, schema, TimeRange{}), 16U);

    fileStore.removeFile();
}

/// A read whose range misses every segment returns nothing, without touching the rows.
TEST_F(SegmentedFileStoreTest, ReadSkipsSegmentsOutsideTheRange)
{
    const Schema schema = makeSchema();
    const auto early = makeRows(64, 1000);
    const auto late = makeRows(64, 9000);

    Store store = makeStore<FileStore>(cfgFor("skip", early.size()), schema);
    store.open();
    auto& fileStore = store.getAs<FileStore>().getMutable();
    fileStore.appendBuffer(early.data(), early.size(), 64, Timestamp(1000), Timestamp(1063));
    fileStore.appendBuffer(late.data(), late.size(), 64, Timestamp(9000), Timestamp(9063));

    auto bufferManager = BufferManager::create(4096, 8);
    auto buffer = bufferManager->getBufferBlocking();

    /// Between the two segments: neither can contain a matching row.
    const TimeRange between{.fieldName = "ts", .start = Timestamp(5000), .end = Timestamp(6000)};
    EXPECT_EQ(store.read(buffer, schema, between), 0U);

    /// Inside the first: only its rows come back.
    auto buffer2 = bufferManager->getBufferBlocking();
    const TimeRange inFirst{.fieldName = "ts", .start = Timestamp(1000), .end = Timestamp(1010)};
    EXPECT_EQ(store.read(buffer2, schema, inFirst), 10U);

    fileStore.removeFile();
}

}
