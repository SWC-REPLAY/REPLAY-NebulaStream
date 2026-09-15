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
#include <TimeRange.hpp>

namespace NES::StoreManager
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
        store.writeRecord(row.data(), rowWidth, Timestamp(1000 + i), schema);
    }
    EXPECT_EQ(fileStore.debugSegmentCount(), 0U) << "ten records is not a segment each";

    /// Reaching exactly one segment's worth seals exactly one.
    for (uint64_t i = 10; i < 64; ++i)
    {
        const auto row = makeRows(1, 1000 + i);
        store.writeRecord(row.data(), rowWidth, Timestamp(1000 + i), schema);
    }
    EXPECT_EQ(fileStore.debugSegmentCount(), 1U);

    /// And the next 64 seal the second, not 64 more.
    for (uint64_t i = 64; i < 128; ++i)
    {
        const auto row = makeRows(1, 1000 + i);
        store.writeRecord(row.data(), rowWidth, Timestamp(1000 + i), schema);
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

/// The table is in the file, not just in memory: a reader that only has the file must be able to
/// recover every segment's range without reading a single row.
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

    /// Offsets are relative to the data region and the second segment follows the first.
    EXPECT_EQ(header.segments[0].byteOffset, 0U);
    EXPECT_EQ(header.segments[1].byteOffset, early.size());
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
