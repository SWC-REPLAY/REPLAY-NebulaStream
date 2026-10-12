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

/// Chain test: FileStore -> LosslessStore via FileToCompressedTransformation.
/// Writing rows to the FileStore and flushing should drain the file into the
/// compressed tier (compressing it into a segment) and truncate the file.

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <lossless/LosslessStore.hpp>
#include <FileStore.hpp>
#include <lossless/FileToCompressedTransformation.hpp>
#include <FlushPolicy.hpp>
#include <Store.hpp>
#include <TimeRange.hpp>

namespace NES
{

class FileToCompressedChainTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("FileToCompressedChainTest.log", LogLevel::LOG_ERROR); }
};

namespace
{
Schema makeSchema()
{
    return Schema{}
        .addField("id", DataType::Type::UINT64)
        .addField("value", DataType::Type::UINT64)
        .addField("ts", DataType::Type::UINT64);
}

std::vector<uint8_t> makeRow(uint32_t rowWidth, uint8_t fill)
{
    return std::vector<uint8_t>(rowWidth, fill);
}

/// A row whose bytes really hold the timestamp, so that row-level filtering has
/// something true to read: id at 0, value at 8, ts at 16.
std::vector<uint8_t> makeTimedRow(const uint32_t rowWidth, const uint64_t id, const uint64_t ts)
{
    std::vector<uint8_t> row(rowWidth, 0);
    std::memcpy(row.data(), &id, sizeof(id));
    std::memcpy(row.data() + 8, &id, sizeof(id));
    std::memcpy(row.data() + 16, &ts, sizeof(ts));
    return row;
}
}

TEST_F(FileToCompressedChainTest, FileFlushesIntoCompressed)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create();

    /// Tail: the compressed store (kept as a handle to inspect after the flush).
    LosslessStore::Config ccfg;
    ccfg.storeName = "fc_compressed";
    ccfg.storeDir = "/tmp";
    Store compressed = makeStore<LosslessStore>(ccfg, schema);

    /// Middle/head: a FileStore chained to the compressed tier. FileStore's chained
    /// constructor takes the transformation explicitly.
    FileStore::Config fcfg;
    fcfg.storeName = "fc_file";
    fcfg.storeDir = "/tmp";
    fcfg.schemaText = "";
    /// A segment every 2 rows, and a threshold of 4. Both matter: only bytes ON DISK count toward
    /// the threshold, so rows still in the unsealed segment are invisible to it -- with the
    /// default 64 KB segment these four rows would never seal and the measure would never move,
    /// whatever the threshold said.
    ///
    /// It read 64 MB before, against four rows, and passed only because FileStore::flush ignored
    /// its policy and drained on command.
    fcfg.segmentSize = rowWidth * 2;
    const FlushPolicy policy{.type = FlushPolicy::Type::SIZE_THRESHOLD, .sizeThreshold = rowWidth * 4};
    Store file = makeStore<FileStore>(fcfg, schema, compressed, FileToCompressedTransformation{}, policy);

    file.open(); /// opens the file writer and (via nextLevel) the compressed tier

    /// Write four rows straight into the FileStore.
    const uint64_t numRows = 4;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const auto row = makeRow(rowWidth, static_cast<uint8_t>(0x40 + i));
        file.writeRecord(row.data(), rowWidth, Timestamp(300 + i));
    }

    /// Flush: FileStore::flush runs the transformation, which drains the file into
    /// the compressed tier and truncates the file, then flushes the compressed tier
    /// (sealing one segment).
    file.flush();

    /// The data now lives compressed in the tail as exactly one segment.
    EXPECT_EQ(compressed.getAs<LosslessStore>().get().debugSegmentCount(), 1U);

    /// Read the compressed tier directly and confirm all rows came through.
    TupleBuffer out = bufferManager->getBufferBlocking();
    const uint64_t got = compressed.read(out, schema, TimeRange{});
    EXPECT_EQ(got, numRows);
}

namespace
{
/// FileStore -> LosslessStore where the two tiers divide their storage at DIFFERENT
/// sizes on purpose: 16 rows per file segment, 64 rows per compressed block. Four
/// segments from above therefore make one block below.
struct SizedChain
{
    Store compressed;
    Store file;
};

SizedChain makeSizedChain(const std::string& name, const uint32_t rowWidth)
{
    const Schema schema = makeSchema();

    LosslessStore::Config ccfg;
    ccfg.storeName = name + "_compressed";
    ccfg.storeDir = "/tmp";
    ccfg.segmentSize = rowWidth * 64; /// this tier's own unit: 64 rows
    Store compressed = makeStore<LosslessStore>(ccfg, schema);

    FileStore::Config fcfg;
    fcfg.storeName = name + "_file";
    fcfg.storeDir = "/tmp";
    fcfg.schemaText = "";
    fcfg.segmentSize = rowWidth * 16; /// the tier above divides four times finer
    fcfg.totalSize = rowWidth * 16 * 64;
    /// Migrate every 128 rows, i.e. every 8 of this tier's segments. The threshold is what puts
    /// data in the tail now -- flush() no longer drains on command -- so it has to be a figure the
    /// test data actually reaches. Over 256 rows that is two migrations of 8 segments each, and
    /// the tail still cuts them into blocks of 64 on its own boundary, which is the point here.
    const FlushPolicy policy{
        .type = FlushPolicy::Type::SIZE_THRESHOLD, .sizeThreshold = rowWidth * 128};
    Store file = makeStore<FileStore>(fcfg, schema, compressed, FileToCompressedTransformation{}, policy);

    return SizedChain{.compressed = std::move(compressed), .file = std::move(file)};
}
}

/// The tiers do not share a segment size, and must not have to.
///
/// Each compressed block seals on the LosslessStore's OWN threshold, and its range is
/// the union of exactly the file segments that landed in it -- so a block covers a narrow
/// window rather than the whole drain. Under the old bulk path every segment's range was
/// unioned into one active block, which sealed as a single segment spanning everything:
/// read() still returned correct rows, but had nothing it could prune.
TEST_F(FileToCompressedChainTest, EachBlockCarriesTheRangeOfTheSegmentsInsideIt)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto chain = makeSizedChain("fc_sized", rowWidth);
    chain.file.open();

    /// 256 rows -> 16 file segments -> 4 compressed blocks of 64 rows each.
    for (uint64_t i = 0; i < 256; ++i)
    {
        const auto row = makeTimedRow(rowWidth, i, 1000 + i);
        chain.file.writeRecord(row.data(), rowWidth, Timestamp(1000 + i));
    }
    /// FileStore migrated twice on the way through and kept nothing, so the division it made is
    /// visible in the tail's block boundaries rather than in its own segment count.
    EXPECT_EQ(chain.file.getAs<FileStore>().get().debugSegmentCount(), 0U) << "everything should have moved down";

    chain.file.flush();

    const auto& tail = chain.compressed.getAs<LosslessStore>().get();
    ASSERT_EQ(tail.debugSegmentCount(), 4U) << "blocks must seal on this tier's size, not on the incoming boundaries";

    /// The ranges are disjoint and narrow. One collapsed span would make every one of
    /// these [1000, 1255].
    const auto ranges = tail.debugSegmentRanges();
    ASSERT_EQ(ranges.size(), 4U);
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        EXPECT_EQ(ranges[i].first, 1000 + (i * 64)) << "block " << i;
        EXPECT_EQ(ranges[i].second, 1000 + (i * 64) + 63) << "block " << i;
    }

    chain.file.close();
    chain.compressed.getAs<LosslessStore>().getMutable().removeFile();
    chain.file.getAs<FileStore>().getMutable().removeFile();
}

/// What the narrow ranges buy: a window inside one block returns exactly its rows, and
/// the three blocks that cannot hold them are skipped without being decompressed.
TEST_F(FileToCompressedChainTest, NarrowWindowReturnsExactlyItsRows)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create(rowWidth * 512, 8);
    auto chain = makeSizedChain("fc_window", rowWidth);
    chain.file.open();

    for (uint64_t i = 0; i < 256; ++i)
    {
        const auto row = makeTimedRow(rowWidth, i, 1000 + i);
        chain.file.writeRecord(row.data(), rowWidth, Timestamp(1000 + i));
    }
    chain.file.flush();

    /// Deliberately not aligned to a block boundary, and inside the third block only.
    const TimeRange window{.fieldName = "ts", .start = Timestamp(1140), .end = Timestamp(1150)};
    TupleBuffer out = bufferManager->getBufferBlocking();
    const uint64_t got = chain.compressed.read(out, schema, window);

    ASSERT_EQ(got, 10U);
    const auto area = out.getAvailableMemoryArea<const uint8_t>();
    for (uint64_t i = 0; i < got; ++i)
    {
        uint64_t ts = 0;
        std::memcpy(&ts, area.data() + (i * rowWidth) + 16, sizeof(ts));
        EXPECT_EQ(ts, 1140 + i);
    }

    chain.file.close();
    chain.compressed.getAs<LosslessStore>().getMutable().removeFile();
    chain.file.getAs<FileStore>().getMutable().removeFile();
}

}
