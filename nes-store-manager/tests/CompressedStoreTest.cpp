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

/// Standalone tests for CompressedStore (no transformations, no chaining).
/// Uses the BaseUnitTest fixture so the logger/runtime is initialized.

#include <cstdint>
#include <cstring>
#include <stdexcept>
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
#include <Util/Logger/impl/NesLogger.hpp>
#include <CompressedStore.hpp>
#include <Store.hpp>
#include <TimeRange.hpp>

namespace NES::StoreManager
{

/// Test fixture — inheriting BaseUnitTest sets up logging and the test environment.
class CompressedStoreTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("CompressedStoreTest.log", LogLevel::LOG_DEBUG); }
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

/// The filtered field itself ("ts") is nullable, so its value sits 1 byte after
/// its raw offset (behind the null indicator).
Schema makeSchemaNullableTs()
{
    return Schema{}
        .addField("id", DataType::Type::UINT64)
        .addField("value", DataType::Type::UINT64)
        .addField("ts", DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE);
}

/// A nullable field ("value") precedes the filtered field ("ts"), which is itself
/// non-nullable — exercises accumulating a prior nullable field's extra byte
/// without adding one for the target field.
Schema makeSchemaMixedNullability()
{
    return Schema{}
        .addField("id", DataType::Type::UINT64)
        .addField("value", DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE)
        .addField("ts", DataType::Type::UINT64);
}

/// Byte offset of a field's VALUE within a packed row — same accumulation
/// read() uses (getSizeInBytesWithNull per prior field), skipping the 1-byte
/// null indicator only when the field is actually nullable.
uint32_t fieldValueOffset(const Schema& schema, const std::string& fieldName)
{
    uint32_t offset = 0;
    for (size_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        const auto& field = schema.getFieldAt(i);
        if (field.getUnqualifiedName() == fieldName)
        {
            return offset + (field.dataType.nullable ? 1 : 0);
        }
        offset += field.dataType.isType(DataType::Type::VARSIZED) ? sizeof(uint32_t) : field.dataType.getSizeInBytesWithNull();
    }
    return offset;
}

std::vector<uint8_t> makeRow(uint32_t rowWidth, uint8_t fill)
{
    return std::vector<uint8_t>(rowWidth, fill);
}

std::vector<uint8_t> makeRowWithTs(const Schema& schema, uint64_t ts)
{
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    std::vector<uint8_t> row(rowWidth, 0);
    const uint32_t offset = fieldValueOffset(schema, "ts");
    std::memcpy(row.data() + offset, &ts, sizeof(uint64_t));
    return row;
}

const CompressedStore& concrete(const Store& store)
{
    return store.getAs<CompressedStore>().get();
}

CompressedStore::Config cfgFor(const std::string& name)
{
    CompressedStore::Config cfg;
    cfg.storeName = name;
    cfg.storeDir = "/tmp";
    return cfg;
}
}

TEST_F(CompressedStoreTest, FlushSealsOneSegment)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<CompressedStore>(cfgFor("seal_test"), schema);
    store.open();

    const auto row = makeRow(rowWidth, 0xAB);
    store.writeRecord(row.data(), rowWidth, Timestamp(100), schema);
    store.writeRecord(row.data(), rowWidth, Timestamp(110), schema);
    store.writeRecord(row.data(), rowWidth, Timestamp(120), schema);

    EXPECT_EQ(concrete(store).debugSegmentCount(), 0U);
    EXPECT_EQ(concrete(store).debugActiveRowCount(), 3U);

    store.flush();

    EXPECT_EQ(concrete(store).debugSegmentCount(), 1U);
    EXPECT_EQ(concrete(store).debugActiveRowCount(), 0U);
}

TEST_F(CompressedStoreTest, CompressionReducesSize)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<CompressedStore>(cfgFor("ratio_test"), schema);
    store.open();

    const auto row = makeRow(rowWidth, 0x00);
    const uint64_t numRows = 1000;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        store.writeRecord(row.data(), rowWidth, Timestamp(i), schema);
    }
    store.flush();

    EXPECT_EQ(concrete(store).debugSegmentCount(), 1U);
    EXPECT_LT(store.size(), numRows * rowWidth);
}

TEST_F(CompressedStoreTest, AutoSealsAtSegmentSize)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto cfg = cfgFor("autoseal_test");
    cfg.segmentSize = 2 * rowWidth;
    Store store = makeStore<CompressedStore>(cfg, schema);
    store.open();

    const auto row = makeRow(rowWidth, 0x01);
    for (uint64_t i = 0; i < 6; ++i)
    {
        store.writeRecord(row.data(), rowWidth, Timestamp(i), schema);
    }

    EXPECT_GE(concrete(store).debugSegmentCount(), 2U);
}

TEST_F(CompressedStoreTest, ReadUnboundedRoundTrip)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<CompressedStore>(cfgFor("roundtrip_test"), schema);
    store.open();

    const uint64_t numRows = 5;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const auto row = makeRow(rowWidth, static_cast<uint8_t>(0x10 + i));
        store.writeRecord(row.data(), rowWidth, Timestamp(100 + i), schema);
    }
    store.flush();

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();

    const uint64_t got = store.read(out, schema, TimeRange{});
    EXPECT_EQ(got, numRows);

    auto span = out.getAvailableMemoryArea<uint8_t>();
    for (uint64_t i = 0; i < numRows; ++i)
    {
        for (uint32_t b = 0; b < rowWidth; ++b)
        {
            EXPECT_EQ(span.data()[(i * rowWidth) + b], static_cast<uint8_t>(0x10 + i));
        }
    }
}

TEST_F(CompressedStoreTest, ReadPrunesByTimeRange)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<CompressedStore>(cfgFor("prune_test"), schema);
    store.open();

    for (uint64_t ts : {100U, 110U, 120U})
    {
        const auto row = makeRow(rowWidth, 0x0A);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts), schema);
    }
    store.flush();

    for (uint64_t ts : {500U, 510U, 520U})
    {
        const auto row = makeRow(rowWidth, 0x0B);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts), schema);
    }
    store.flush();

    EXPECT_EQ(concrete(store).debugSegmentCount(), 2U);

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();

    TimeRange range;
    range.fieldName = "ts";
    range.start = Timestamp(50);
    range.end = Timestamp(200);

    const uint64_t got = store.read(out, schema, range);
    EXPECT_EQ(got, 3U);
}

TEST_F(CompressedStoreTest, ReadRowLevelFilter)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<CompressedStore>(cfgFor("rowfilter_test"), schema);
    store.open();

    for (uint64_t ts : {100U, 101U, 102U, 103U, 104U})
    {
        const auto row = makeRowWithTs(schema, ts);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts), schema);
    }
    store.flush();

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();

    TimeRange range;
    range.fieldName = "ts";
    range.start = Timestamp(102);
    range.end = Timestamp(105);

    const uint64_t got = store.read(out, schema, range);
    EXPECT_EQ(got, 3U);
}

TEST_F(CompressedStoreTest, ReadRowLevelFilterNullableTsField)
{
    const Schema schema = makeSchemaNullableTs();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<CompressedStore>(cfgFor("rowfilter_nullable_ts_test"), schema);
    store.open();

    for (uint64_t ts : {100U, 101U, 102U, 103U, 104U})
    {
        const auto row = makeRowWithTs(schema, ts);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts), schema);
    }
    store.flush();

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();

    TimeRange range;
    range.fieldName = "ts";
    range.start = Timestamp(102);
    range.end = Timestamp(105);

    const uint64_t got = store.read(out, schema, range);
    EXPECT_EQ(got, 3U);
}

TEST_F(CompressedStoreTest, ReadRowLevelFilterMixedNullability)
{
    const Schema schema = makeSchemaMixedNullability();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<CompressedStore>(cfgFor("rowfilter_mixed_nullability_test"), schema);
    store.open();

    for (uint64_t ts : {100U, 101U, 102U, 103U, 104U})
    {
        const auto row = makeRowWithTs(schema, ts);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts), schema);
    }
    store.flush();

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();

    TimeRange range;
    range.fieldName = "ts";
    range.start = Timestamp(102);
    range.end = Timestamp(105);

    const uint64_t got = store.read(out, schema, range);
    EXPECT_EQ(got, 3U);
}

}
