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

/// Chain test: FileStore -> CompressedStore via FileToCompressedTransformation.
/// Writing rows to the FileStore and flushing should drain the file into the
/// compressed tier (compressing it into a segment) and truncate the file.

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <CompressedStore.hpp>
#include <FileStore.hpp>
#include <FileToCompressedTransformation.hpp>
#include <FlushPolicy.hpp>
#include <Store.hpp>
#include <TimeRange.hpp>

namespace NES::StoreManager
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
}

TEST_F(FileToCompressedChainTest, FileFlushesIntoCompressed)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    auto bufferManager = BufferManager::create();

    /// Tail: the compressed store (kept as a handle to inspect after the flush).
    CompressedStore::Config ccfg;
    ccfg.storeName = "fc_compressed";
    ccfg.storeDir = "/tmp";
    Store compressed = makeStore<CompressedStore>(ccfg, schema);

    /// Middle/head: a FileStore chained to the compressed tier. FileStore's chained
    /// constructor takes the transformation explicitly.
    FileStore::Config fcfg;
    fcfg.storeName = "fc_file";
    fcfg.storeDir = "/tmp";
    fcfg.schemaText = "";
    const FlushPolicy policy{.type = FlushPolicy::Type::SIZE_THRESHOLD, .sizeThreshold = 64UZ * 1024UZ * 1024UZ};
    Store file = makeStore<FileStore>(fcfg, schema, compressed, FileToCompressedTransformation{}, policy);

    file.open(); /// opens the file writer and (via nextLevel) the compressed tier

    /// Write four rows straight into the FileStore.
    const uint64_t numRows = 4;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const auto row = makeRow(rowWidth, static_cast<uint8_t>(0x40 + i));
        file.writeRecord(row.data(), rowWidth, Timestamp(300 + i), schema);
    }

    /// Flush: FileStore::flush runs the transformation, which drains the file into
    /// the compressed tier and truncates the file, then flushes the compressed tier
    /// (sealing one segment).
    file.flush();

    /// The data now lives compressed in the tail as exactly one segment.
    EXPECT_EQ(compressed.getAs<CompressedStore>().get().debugSegmentCount(), 1U);

    /// Read the compressed tier directly and confirm all rows came through.
    TupleBuffer out = bufferManager->getBufferBlocking();
    const uint64_t got = compressed.read(out, schema, TimeRange{});
    EXPECT_EQ(got, numRows);
}

}
