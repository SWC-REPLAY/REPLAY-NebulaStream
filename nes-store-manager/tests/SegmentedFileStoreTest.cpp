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
