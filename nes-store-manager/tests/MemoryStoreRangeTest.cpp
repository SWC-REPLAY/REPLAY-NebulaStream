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

/// Time-range reads against MemoryStore, over the case nothing else covered: a range that
/// PARTIALLY overlaps a buffer, on a schema with no nullable fields.
///
/// MemoryStore answers a range in three levels -- skip a buffer whose range misses entirely,
/// copy one that falls wholly inside, and read the timestamp column row by row for one that
/// straddles a boundary. Only the third level touches the rows, and it was reading them at the
/// wrong offset: it skipped a null-indicator byte that a non-nullable field does not have, so
/// every row failed the comparison and a straddling buffer contributed nothing.
///
/// Unbounded reads never reach that path, and a range that happens to cover whole buffers does
/// not either, which is why it went unnoticed.

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
#include <MemoryStore.hpp>
#include <Store.hpp>
#include <TimeRange.hpp>

namespace NES::StoreManager
{

class MemoryStoreRangeTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("MemoryStoreRangeTest.log", LogLevel::LOG_DEBUG); }
};

namespace
{

/// No nullable fields: the layout has no indicator bytes at all.
Schema makeSchema()
{
    return Schema{}.addField("ts", DataType::Type::UINT64).addField("value", DataType::Type::UINT64);
}

std::vector<uint8_t> makeRow(const uint64_t ts, const uint64_t value)
{
    std::vector<uint8_t> row(sizeof(uint64_t) * 2, 0);
    std::memcpy(row.data(), &ts, sizeof(ts));
    std::memcpy(row.data() + sizeof(uint64_t), &value, sizeof(value));
    return row;
}

std::vector<uint64_t> readTimestamps(Store& store, const Schema& schema, const TimeRange& range, const uint64_t capacityRows)
{
    auto bufferManager = BufferManager::create(schema.getSizeOfSchemaInBytes() * capacityRows, 8);
    auto buffer = bufferManager->getBufferBlocking();
    const uint64_t got = store.read(buffer, schema, range);

    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    const auto area = buffer.getAvailableMemoryArea<const uint8_t>();
    std::vector<uint64_t> out;
    for (uint64_t i = 0; i < got; ++i)
    {
        uint64_t ts = 0;
        std::memcpy(&ts, area.data() + (i * rowWidth), sizeof(ts));
        out.push_back(ts);
    }
    return out;
}

}

/// The case the bug lived in: a range that starts and ends inside one buffer.
TEST_F(MemoryStoreRangeTest, PartiallyOverlappingBufferIsFilteredRowByRow)
{
    const Schema schema = makeSchema();
    auto bufferManager = BufferManager::create(4096, 16);
    Store store = makeStore<MemoryStore>(schema, MemoryStore::Config{}, bufferManager);
    store.open();

    for (uint64_t i = 0; i < 100; ++i)
    {
        const auto row = makeRow(1000 + i, i);
        store.writeRecord(row.data(), static_cast<uint32_t>(row.size()), Timestamp(1000 + i), schema);
    }
    store.flush();

    /// Strictly inside the written span, so the buffer holding it straddles both bounds.
    const TimeRange range{.fieldName = "ts", .start = Timestamp(1020), .end = Timestamp(1030)};
    const auto timestamps = readTimestamps(store, schema, range, 256);

    ASSERT_EQ(timestamps.size(), 10U) << "a straddling buffer must still contribute its matching rows";
    for (size_t i = 0; i < timestamps.size(); ++i)
    {
        EXPECT_EQ(timestamps[i], 1020U + i);
    }

    store.close();
}

/// A range covering everything still returns everything -- the level that copies whole buffers.
TEST_F(MemoryStoreRangeTest, RangeCoveringEverythingReturnsEveryRow)
{
    const Schema schema = makeSchema();
    auto bufferManager = BufferManager::create(4096, 16);
    Store store = makeStore<MemoryStore>(schema, MemoryStore::Config{}, bufferManager);
    store.open();

    for (uint64_t i = 0; i < 50; ++i)
    {
        const auto row = makeRow(2000 + i, i);
        store.writeRecord(row.data(), static_cast<uint32_t>(row.size()), Timestamp(2000 + i), schema);
    }
    store.flush();

    const TimeRange range{.fieldName = "ts", .start = Timestamp(0), .end = Timestamp(99999)};
    EXPECT_EQ(readTimestamps(store, schema, range, 256).size(), 50U);

    store.close();
}

/// A range that misses everything returns nothing -- the level that skips whole buffers.
TEST_F(MemoryStoreRangeTest, RangeOutsideEveryBufferReturnsNothing)
{
    const Schema schema = makeSchema();
    auto bufferManager = BufferManager::create(4096, 16);
    Store store = makeStore<MemoryStore>(schema, MemoryStore::Config{}, bufferManager);
    store.open();

    for (uint64_t i = 0; i < 50; ++i)
    {
        const auto row = makeRow(3000 + i, i);
        store.writeRecord(row.data(), static_cast<uint32_t>(row.size()), Timestamp(3000 + i), schema);
    }
    store.flush();

    const TimeRange range{.fieldName = "ts", .start = Timestamp(9000), .end = Timestamp(9100)};
    EXPECT_TRUE(readTimestamps(store, schema, range, 256).empty());

    store.close();
}

}
