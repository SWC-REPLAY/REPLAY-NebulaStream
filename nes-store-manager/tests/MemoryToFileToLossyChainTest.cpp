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

/// The whole chain, end to end: MemoryStore -> FileStore -> LossyStore.
///
/// Each tier divides its storage and each division carries the time range of the rows inside it.
/// The range is measured exactly once -- in MemoryStore, from the timestamps handed to
/// writeRecord -- and is then carried down: into the segment a buffer lands in, and into the
/// group a segment becomes. No tier below MemoryStore reads a timestamp out of the row bytes,
/// and none of them has to be told which column holds record time.
///
/// What this pins is the property the unit tests cannot see on their own: that the range really
/// survives two tier boundaries, so a query touching a narrow window decompresses only the
/// groups that could contain it.

#include <cstdint>
#include <cstring>
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
#include <FileToLossyTransformation.hpp>
#include <FlushPolicy.hpp>
#include <LossyStore.hpp>
#include <MemoryStore.hpp>
#include <MemoryToFileTransformation.hpp>
#include <Store.hpp>
#include <StoreReadCursor.hpp>
#include <TimeRange.hpp>

namespace NES
{

class MemoryToFileToLossyChainTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("MemoryToFileToLossyChainTest.log", LogLevel::LOG_ERROR); }
};

namespace
{

/// No nullable fields, so the packed row has no indicator bytes: ts at 0, value at 8.
Schema makeSchema()
{
    return Schema{}.addField("ts", DataType::Type::UINT64).addField("value", DataType::Type::UINT64);
}

struct Chain
{
    Store lossy;
    Store file;
    Store head;
};

/// MemoryStore -> FileStore -> LossyStore, built bottom-up the way StoreRegistry does it.
///
/// A threshold per tier. Both tiers check their policy in writeRecord, so one shared value moves
/// MemoryStore's drain cadence whenever FileStore's threshold is changed.
Chain makeChain(
    const std::string& name,
    const size_t segmentSize,
    const size_t memoryThreshold,
    const size_t fileThreshold,
    const std::shared_ptr<BufferManager>& bufferManager)
{
    const Schema schema = makeSchema();

    Store lossy = makeStore<LossyStore>(
        LossyStore::Config{.storeName = name + "_lossy", .storeDir = "/tmp", .schemaText = ""}, schema);

    /// MemoryStore counts whole allocated buffers, FileStore whole sealed segments. Rows in the
    /// segment still being filled are not on disk and do not count.
    const FlushPolicy memoryPolicy{.type = FlushPolicy::Type::SIZE_THRESHOLD, .sizeThreshold = memoryThreshold};
    const FlushPolicy filePolicy{.type = FlushPolicy::Type::SIZE_THRESHOLD, .sizeThreshold = fileThreshold};

    Store file = makeStore<FileStore>(
        FileStore::Config{
            .storeName = name + "_file",
            .storeDir = "/tmp",
            .schemaText = "",
            .segmentSize = segmentSize,
            .totalSize = 1UZ * 1024UZ * 1024UZ},
        schema,
        lossy,
        FileToLossyTransformation{},
        filePolicy);

    Store head = makeStore<MemoryStore>(schema, MemoryStore::Config{}, bufferManager, file, memoryPolicy);
    return Chain{.lossy = std::move(lossy), .file = std::move(file), .head = std::move(head)};
}

void writeRows(Store& store, const Schema& schema, const uint64_t count, const uint64_t firstTs)
{
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    for (uint64_t i = 0; i < count; ++i)
    {
        const uint64_t ts = firstTs + i;
        const uint64_t value = i;
        std::vector<uint8_t> row(rowWidth, 0);
        std::memcpy(row.data(), &ts, sizeof(ts));
        std::memcpy(row.data() + sizeof(uint64_t), &value, sizeof(value));
        store.writeRecord(row.data(), rowWidth, Timestamp(ts));
    }
}

std::vector<uint64_t> readTimestamps(Store& store, const Schema& schema, const TimeRange& range, const uint64_t capacityRows)
{
    auto bufferManager = BufferManager::create(schema.getSizeOfSchemaInBytes() * capacityRows, 8);
    auto buffer = bufferManager->getBufferBlocking();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    /// One bufferful per call against a cursor that survives between them, until a call yields
    /// nothing, which is how SourceThread drains a store. A chain holding rows in more than one
    /// tier gives up only the lowest tier's share per call. The three-argument overload starts a
    /// fresh cursor each time and would repeat the first call forever.
    StoreReadCursor cursor;
    std::vector<uint64_t> out;
    while (const uint64_t got = store.read(buffer, schema, range, cursor))
    {
        const auto area = buffer.getAvailableMemoryArea<const uint8_t>();
        for (uint64_t i = 0; i < got; ++i)
        {
            uint64_t ts = 0;
            std::memcpy(&ts, area.data() + (i * rowWidth), sizeof(ts));
            out.push_back(ts);
        }
    }
    return out;
}

}

/// Rows pushed all the way to the tail must come back unchanged, and each group must carry a
/// range narrow enough to be worth skipping on -- not one span covering the whole stream.
TEST_F(MemoryToFileToLossyChainTest, RangesSurviveBothTierBoundaries)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create(rowWidth * 64, 64);

    /// 64 rows per segment, so 512 rows become 8 groups with disjoint windows.
    auto chain = makeChain("chain_ranges", rowWidth * 64, rowWidth * 128, rowWidth * 128, bufferManager);
    chain.head.open();

    writeRows(chain.head, schema, 512, 1000); /// ts 1000 .. 1511
    chain.head.flush();
    chain.file.flush();

    const auto& lossy = chain.lossy.getAs<LossyStore>().get();
    /// ASSERT, not EXPECT: the checks below index into `ranges`, and a non-fatal failure here
    /// used to run straight on into front() on an empty vector and segfault.
    ASSERT_GT(lossy.debugGroupCount(), 1U) << "the stream must land as several groups, not one";

    /// The property that makes skipping possible, and the one a correct-rows assertion cannot
    /// distinguish from filtering every row: the ranges must be narrow and disjoint, not the
    /// same span repeated. Under the old collapse every group claimed [1000, 1511].
    const auto ranges = lossy.debugGroupRanges();
    ASSERT_EQ(ranges.size(), lossy.debugGroupCount());
    uint64_t previousMax = 0;
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        const auto [minTs, maxTs] = ranges[i];
        EXPECT_LE(minTs, maxTs) << "group " << i;
        EXPECT_LT(maxTs - minTs, 511U) << "group " << i << " spans the whole stream, so the range was collapsed";
        if (i > 0)
        {
            EXPECT_GT(minTs, previousMax) << "group " << i << " overlaps the one before it";
        }
        previousMax = maxTs;
    }
    EXPECT_EQ(ranges.front().first, 1000U);
    /// No matching assertion on ranges.back().second. The tail holds a PREFIX of the stream, not
    /// all of it: FileStore migrates whole sealed segments once its own threshold is crossed, so
    /// the newest rows are still upstream when this runs. Which prefix depends on the threshold
    /// and the segment size, and is not this test's subject -- that the ranges are narrow and
    /// disjoint is, and the loop above has shown it. Nothing is lost, which the read below shows.

    /// Everything comes back, in order.
    const auto all = readTimestamps(chain.head, schema, TimeRange{}, 1024);
    ASSERT_EQ(all.size(), 512U);
    for (size_t i = 0; i < all.size(); ++i)
    {
        EXPECT_EQ(all[i], 1000U + i);
    }

    chain.head.close();
    chain.lossy.getAs<LossyStore>().getMutable().removeFile();
    chain.file.getAs<FileStore>().getMutable().removeFile();
}

/// The property the whole change exists for: a narrow query returns exactly its window.
///
/// Before ranges were carried down, every group inherited one span covering the entire drain,
/// so nothing could be skipped and correctness rested entirely on row-level filtering. It still
/// has to be exactly right now that groups are skipped instead.
TEST_F(MemoryToFileToLossyChainTest, NarrowQueryReturnsExactlyItsWindow)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create(rowWidth * 64, 64);

    auto chain = makeChain("chain_window", rowWidth * 64, rowWidth * 128, rowWidth * 128, bufferManager);
    chain.head.open();

    writeRows(chain.head, schema, 512, 1000);
    chain.head.flush();
    chain.file.flush();

    /// A window in the middle, deliberately not aligned to any segment boundary.
    const TimeRange window{.fieldName = "ts", .start = Timestamp(1234), .end = Timestamp(1250)};
    const auto got = readTimestamps(chain.head, schema, window, 1024);

    ASSERT_EQ(got.size(), 16U);
    for (size_t i = 0; i < got.size(); ++i)
    {
        EXPECT_EQ(got[i], 1234U + i);
    }

    chain.head.close();
    chain.lossy.getAs<LossyStore>().getMutable().removeFile();
    chain.file.getAs<FileStore>().getMutable().removeFile();
}

/// A window before everything written returns nothing -- every group is skipped rather than
/// decompressed and filtered.
TEST_F(MemoryToFileToLossyChainTest, WindowOutsideTheStreamReturnsNothing)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create(rowWidth * 64, 64);

    auto chain = makeChain("chain_outside", rowWidth * 64, rowWidth * 128, rowWidth * 128, bufferManager);
    chain.head.open();

    writeRows(chain.head, schema, 256, 5000); /// ts 5000 .. 5255
    chain.head.flush();
    chain.file.flush();

    const TimeRange before{.fieldName = "ts", .start = Timestamp(100), .end = Timestamp(200)};
    EXPECT_TRUE(readTimestamps(chain.head, schema, before, 1024).empty());

    const TimeRange after{.fieldName = "ts", .start = Timestamp(90000), .end = Timestamp(90100)};
    EXPECT_TRUE(readTimestamps(chain.head, schema, after, 1024).empty());

    chain.head.close();
    chain.lossy.getAs<LossyStore>().getMutable().removeFile();
    chain.file.getAs<FileStore>().getMutable().removeFile();
}

/// The next two are a matched pair: same schema, same segment size, the same 256 rows, and the
/// same MemoryStore threshold, so MemoryStore drains on the same cadence in both. Only FileStore's
/// threshold differs, which leaves it as the only thing that can explain a difference.
///
/// Both assert that what FileStore still holds is below its own threshold, the one statement true
/// on either side of the boundary. They differ in whether the tail received anything.
TEST_F(MemoryToFileToLossyChainTest, BelowItsThresholdFileStoreHoldsAndTheTailStaysEmpty)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create(rowWidth * 64, 64);

    /// 4096 bytes of rows reach a FileStore whose threshold is 16384: never crossed.
    const size_t segmentBytes = rowWidth * 64;
    const size_t fileThreshold = rowWidth * 1024;
    auto chain = makeChain("chain_below_threshold", segmentBytes, rowWidth * 128, fileThreshold, bufferManager);
    chain.head.open();

    writeRows(chain.head, schema, 256, 1000); /// ts 1000 .. 1255, 4096 bytes
    chain.head.flush();

    const size_t heldSegments = chain.file.getAs<FileStore>().get().debugSegmentCount();
    EXPECT_GT(heldSegments, 0U) << "the rows left MemoryStore, so FileStore must be holding them";
    EXPECT_LT(heldSegments * segmentBytes, fileThreshold) << "held below its own threshold";
    EXPECT_EQ(chain.lossy.getAs<LossyStore>().get().debugGroupCount(), 0U)
        << "nothing reaches the tail until FileStore's threshold is crossed";

    /// Held is not lost: the read still produces every row, from the middle tier this time.
    const auto all = readTimestamps(chain.head, schema, TimeRange{}, 1024);
    ASSERT_EQ(all.size(), 256U);
    for (size_t i = 0; i < all.size(); ++i)
    {
        EXPECT_EQ(all[i], 1000U + i);
    }

    chain.head.close();
    chain.lossy.getAs<LossyStore>().getMutable().removeFile();
    chain.file.getAs<FileStore>().getMutable().removeFile();
}

TEST_F(MemoryToFileToLossyChainTest, AboveItsThresholdFileStoreMigratesToTheTail)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create(rowWidth * 64, 64);

    /// The same 4096 bytes of rows, reaching a FileStore whose threshold is 2048: crossed.
    const size_t segmentBytes = rowWidth * 64;
    const size_t fileThreshold = rowWidth * 128;
    auto chain = makeChain("chain_above_threshold", segmentBytes, rowWidth * 128, fileThreshold, bufferManager);
    chain.head.open();

    writeRows(chain.head, schema, 256, 1000); /// ts 1000 .. 1255, 4096 bytes
    chain.head.flush();

    EXPECT_GT(chain.lossy.getAs<LossyStore>().get().debugGroupCount(), 0U)
        << "the threshold was crossed, so the tail received something";

    /// Not zero. MemoryStore drains several times during the writes, so FileStore crosses its
    /// threshold and migrates more than once, and the remainder left over sits below it.
    const size_t heldSegments = chain.file.getAs<FileStore>().get().debugSegmentCount();
    EXPECT_LT(heldSegments * segmentBytes, fileThreshold) << "only a sub-threshold remainder left";

    const auto all = readTimestamps(chain.head, schema, TimeRange{}, 1024);
    ASSERT_EQ(all.size(), 256U);
    for (size_t i = 0; i < all.size(); ++i)
    {
        EXPECT_EQ(all[i], 1000U + i);
    }

    chain.head.close();
    chain.lossy.getAs<LossyStore>().getMutable().removeFile();
    chain.file.getAs<FileStore>().getMutable().removeFile();
}

}
