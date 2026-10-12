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

/// Chain test: LosslessStore -> ColumnStore via CompressedToColumnTransformation.
/// Writing rows to the LosslessStore (forcing at least one segment seal) and
/// flushing should drain the compressed segments (decompressing them) into the
/// column tier (transposing into a row group) and reset the compressed store.

#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <ColumnStore.hpp>
#include <lossless/LosslessStore.hpp>
#include <FlushPolicy.hpp>
#include <Store.hpp>
#include <TimeRange.hpp>

namespace NES
{

class CompressedToColumnChainTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("CompressedToColumnChainTest.log", LogLevel::LOG_ERROR); }
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
}

TEST_F(CompressedToColumnChainTest, CompressedFlushesIntoColumn)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create();

    /// Tail: the column store (kept as a handle to inspect after the flush).
    ColumnStore::Config colCfg;
    colCfg.storeName = "cc_column";
    colCfg.storeDir = "/tmp";
    Store column = makeStore<ColumnStore>(colCfg, schema);

    /// Middle/head: a LosslessStore chained to the column tier. LosslessStore's
    /// chained constructor looks up the "LosslessStore -> ColumnStore" transformation
    /// internally (unlike FileStore, which needs it passed explicitly).
    LosslessStore::Config compCfg;
    compCfg.storeName = "cc_compressed";
    compCfg.storeDir = "/tmp";
    compCfg.segmentSize = 2 * rowWidth; /// force at least one segment seal well before flush
    const FlushPolicy policy{.type = FlushPolicy::Type::SIZE_THRESHOLD, .sizeThreshold = 64UZ * 1024UZ * 1024UZ};
    Store compressed = makeStore<LosslessStore>(compCfg, schema, column, policy);

    compressed.open(); /// opens the compressed writer and (via nextLevel) the column tier

    /// Write six rows straight into the LosslessStore — enough to auto-seal at least
    /// one segment (segmentSize == 2 rows) before the explicit flush() below.
    const uint64_t numRows = 6;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const auto row = makeRow(rowWidth, static_cast<uint8_t>(0x60 + i));
        compressed.writeRecord(row.data(), rowWidth, Timestamp(400 + i));
    }
    EXPECT_GE(compressed.getAs<LosslessStore>().get().debugSegmentCount(), 2U);

    /// Flush: LosslessStore::flush seals any remaining active buffer, runs the
    /// transformation (drains + decompresses all segments into the column tier,
    /// resetting the compressed store), then flushes the column tier (sealing one
    /// row group).
    compressed.flush();

    /// The compressed tier has been drained back to empty.
    EXPECT_EQ(compressed.getAs<LosslessStore>().get().debugSegmentCount(), 0U);
    EXPECT_EQ(compressed.getAs<LosslessStore>().get().debugActiveRowCount(), 0U);

    /// The data now lives, transposed, in the column tier as at least one row group.
    EXPECT_GE(column.getAs<ColumnStore>().get().debugRowGroupCount(), 1U);

    /// Read the column tier directly and confirm all rows came through, in order,
    /// with their original byte content intact.
    TupleBuffer out = bufferManager->getBufferBlocking();
    const uint64_t got = column.read(out, schema, TimeRange{});
    ASSERT_EQ(got, numRows);

    auto span = out.getAvailableMemoryArea<uint8_t>();
    for (uint64_t i = 0; i < numRows; ++i)
    {
        for (uint32_t b = 0; b < rowWidth; ++b)
        {
            EXPECT_EQ(span.data()[(i * rowWidth) + b], static_cast<uint8_t>(0x60 + i));
        }
    }
}

}
