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

/// The lossless tail, reached the way a query reaches it: by naming a store order.
///
/// CompressedStore is an alternative tail to LossyStore -- same position in the chain, byte-exact
/// instead of error-bounded. It has to keep working now that FileStore divides its file into
/// segments and hands them down one at a time rather than as a single blob.
///
/// Unlike the lossy tier, CompressedStore does not adopt a segment per group: it seals everything
/// it has been handed into one compressed segment per flush and unions the ranges as they arrive.
/// That is a deliberate difference, not an oversight -- it does not decode per group, so a finer
/// division would buy it nothing -- and these tests pin the behaviour so a later change to
/// FileStore cannot quietly break the combination.

#include <cstdint>
#include <cstring>
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
#include <CompressedStore.hpp>
#include <Store.hpp>
#include <StoreRegistry.hpp>
#include <TimeRange.hpp>

namespace NES
{

class CompressedChainConfigTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("CompressedChainConfigTest.log", LogLevel::LOG_ERROR); }
};

namespace
{

/// Upstream made StoreRegistry worker-owned rather than process-global, so this file holds one
/// instance: a store materialised in one helper stays visible to the next.
StoreRegistry& testRegistry()
{
    static StoreRegistry registry{StoreConfig{}};
    return registry;
}

Schema makeSchema()
{
    return Schema{}.addField("ts", DataType::Type::UINT64).addField("value", DataType::Type::UINT64);
}

constexpr const char* SCHEMA_TEXT = "ts UINT64, value UINT64";

std::vector<uint8_t> makeRows(const Schema& schema, const uint64_t count, const uint64_t firstTs)
{
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < count; ++i)
    {
        std::vector<uint8_t> row(rowWidth, 0);
        const uint64_t ts = firstTs + i;
        const uint64_t value = i;
        std::memcpy(row.data(), &ts, sizeof(ts));
        std::memcpy(row.data() + sizeof(uint64_t), &value, sizeof(value));
        rows.insert(rows.end(), row.begin(), row.end());
    }
    return rows;
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

/// Build a chain by naming it, exactly as a query's REPLAY.STORE_ORDER does.
Store chainFor(const std::string& storeName, const std::string& storeOrder, const Schema& schema)
{
    StoreConfig config;
    config.storeOrder = storeOrder;

    auto& registry = testRegistry();
    std::ignore = registry.getOrCreateStore(storeName, schema, SCHEMA_TEXT, config, BufferManager::create());
    auto store = registry.getStore(storeName);
    EXPECT_TRUE(store.has_value()) << storeOrder << " did not register";
    return *store;
}

}

/// The store order a query would ask for, end to end.
TEST_F(CompressedChainConfigTest, MemoryToFileToCompressedRoundTrips)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(schema, 512, 1000);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto store = chainFor("cc_full", "MemoryStore->FileStore->CompressedStore", schema);
    store.open();

    for (uint64_t i = 0; i < 512; ++i)
    {
        store.writeRecord(rows.data() + (i * rowWidth), rowWidth, Timestamp(1000 + i));
    }
    store.flush();

    const auto all = readTimestamps(store, schema, TimeRange{}, 1024);
    ASSERT_EQ(all.size(), 512U) << "every row must survive the chain";
    for (size_t i = 0; i < all.size(); ++i)
    {
        EXPECT_EQ(all[i], 1000U + i);
    }

    store.close();
}

/// The two-tier form, without a MemoryStore in front.
TEST_F(CompressedChainConfigTest, FileToCompressedRoundTrips)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(schema, 256, 4000);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto store = chainFor("cc_two_tier", "FileStore->CompressedStore", schema);
    store.open();

    for (uint64_t i = 0; i < 256; ++i)
    {
        store.writeRecord(rows.data() + (i * rowWidth), rowWidth, Timestamp(4000 + i));
    }
    store.flush();

    const auto all = readTimestamps(store, schema, TimeRange{}, 1024);
    ASSERT_EQ(all.size(), 256U);
    EXPECT_EQ(all.front(), 4000U);
    EXPECT_EQ(all.back(), 4255U);

    store.close();
}

/// A bounded read through the compressed tail returns exactly its window. The ranges reaching
/// CompressedStore now arrive one segment at a time instead of as a single span, so this is the
/// assertion that would fail if the transformation dropped or mixed them up.
TEST_F(CompressedChainConfigTest, TimeRangeThroughCompressedTailIsExact)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(schema, 512, 1000);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto store = chainFor("cc_range", "MemoryStore->FileStore->CompressedStore", schema);
    store.open();

    for (uint64_t i = 0; i < 512; ++i)
    {
        store.writeRecord(rows.data() + (i * rowWidth), rowWidth, Timestamp(1000 + i));
    }
    store.flush();

    const TimeRange window{.fieldName = "ts", .start = Timestamp(1100), .end = Timestamp(1120)};
    const auto got = readTimestamps(store, schema, window, 1024);

    ASSERT_EQ(got.size(), 20U);
    for (size_t i = 0; i < got.size(); ++i)
    {
        EXPECT_EQ(got[i], 1100U + i);
    }

    /// And a window before anything written yields nothing.
    const TimeRange before{.fieldName = "ts", .start = Timestamp(1), .end = Timestamp(100)};
    EXPECT_TRUE(readTimestamps(store, schema, before, 1024).empty());

    store.close();
}

/// Both tails must remain selectable; naming one must not have broken the other.
TEST_F(CompressedChainConfigTest, BothTailsAreConfigurable)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(schema, 128, 7000);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    for (const auto& [name, order] : std::vector<std::pair<std::string, std::string>>{
             {"tail_compressed", "MemoryStore->FileStore->CompressedStore"},
             {"tail_lossy", "MemoryStore->FileStore->LossyStore"}})
    {
        auto store = chainFor(name, order, schema);
        store.open();
        for (uint64_t i = 0; i < 128; ++i)
        {
            store.writeRecord(rows.data() + (i * rowWidth), rowWidth, Timestamp(7000 + i));
        }
        store.flush();

        /// No accuracy bound was configured, so the lossy tail is bit-exact too: both orders
        /// must return every row unchanged.
        const auto all = readTimestamps(store, schema, TimeRange{}, 512);
        ASSERT_EQ(all.size(), 128U) << order;
        EXPECT_EQ(all.front(), 7000U) << order;
        EXPECT_EQ(all.back(), 7127U) << order;

        store.close();
    }
}

}
