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

/// Standalone tests for ColumnStore (no transformations, no chaining).
/// Uses the BaseUnitTest fixture so the logger/runtime is initialized.

#include <cstdint>
#include <cstring>
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
#include <ColumnStore.hpp>
#include <Store.hpp>
#include <TimeRange.hpp>

namespace NES
{

/// Test fixture — inheriting BaseUnitTest sets up logging and the test environment.
class ColumnStoreTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("ColumnStoreTest.log", LogLevel::LOG_DEBUG); }
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

/// Pack a structured row (id, value, ts all UINT64, in that schema order) for the
/// transpose/detranspose correctness test — lets us verify each field independently
/// survives the round trip, not just raw bytes.
std::vector<uint8_t> makeStructuredRow(uint64_t id, uint64_t value, uint64_t ts)
{
    std::vector<uint8_t> row(3 * sizeof(uint64_t));
    std::memcpy(row.data(), &id, sizeof(uint64_t));
    std::memcpy(row.data() + sizeof(uint64_t), &value, sizeof(uint64_t));
    std::memcpy(row.data() + (2 * sizeof(uint64_t)), &ts, sizeof(uint64_t));
    return row;
}

uint64_t readU64(const uint8_t* rowPtr, uint32_t fieldOffset)
{
    uint64_t v = 0;
    std::memcpy(&v, rowPtr + fieldOffset, sizeof(uint64_t));
    return v;
}

const ColumnStore& concrete(const Store& store)
{
    return store.getAs<ColumnStore>().get();
}

ColumnStore::Config cfgFor(const std::string& name)
{
    ColumnStore::Config cfg;
    cfg.storeName = name;
    cfg.storeDir = "/tmp";
    return cfg;
}
}

TEST_F(ColumnStoreTest, RowGroupSealsAtRowCountThreshold)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto cfg = cfgFor("seal_test");
    cfg.rowGroupRows = 3;
    Store store = makeStore<ColumnStore>(cfg, schema);
    store.open();

    const auto row = makeRow(rowWidth, 0xAB);
    store.writeRecord(row.data(), rowWidth, Timestamp(100));
    store.writeRecord(row.data(), rowWidth, Timestamp(110));

    EXPECT_EQ(concrete(store).debugRowGroupCount(), 0U);
    EXPECT_EQ(concrete(store).debugActiveRowCount(), 2U);

    store.writeRecord(row.data(), rowWidth, Timestamp(120));

    EXPECT_EQ(concrete(store).debugRowGroupCount(), 1U);
    EXPECT_EQ(concrete(store).debugActiveRowCount(), 0U);
}

TEST_F(ColumnStoreTest, TransposeDetransposeRoundTrip)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto cfg = cfgFor("transpose_test");
    cfg.rowGroupRows = 20;
    Store store = makeStore<ColumnStore>(cfg, schema);
    store.open();

    const uint64_t numRows = 20;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const auto row = makeStructuredRow(/*id=*/i, /*value=*/(i * 7) + 3, /*ts=*/1000 + i);
        store.writeRecord(row.data(), rowWidth, Timestamp(1000 + i));
    }
    /// Exactly one seal should have happened (rowGroupRows == numRows).
    EXPECT_EQ(concrete(store).debugRowGroupCount(), 1U);
    EXPECT_EQ(concrete(store).debugActiveRowCount(), 0U);

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();
    const uint64_t got = store.read(out, schema, TimeRange{});
    ASSERT_EQ(got, numRows);

    auto span = out.getAvailableMemoryArea<uint8_t>();
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const uint8_t* rowPtr = span.data() + (i * rowWidth);
        EXPECT_EQ(readU64(rowPtr, 0), i) << "row " << i << " id mismatch";
        EXPECT_EQ(readU64(rowPtr, sizeof(uint64_t)), (i * 7) + 3) << "row " << i << " value mismatch";
        EXPECT_EQ(readU64(rowPtr, 2 * sizeof(uint64_t)), 1000 + i) << "row " << i << " ts mismatch";
    }
}

TEST_F(ColumnStoreTest, MultipleRowGroupsPreserveOrderAndContent)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto cfg = cfgFor("multi_rowgroup_test");
    cfg.rowGroupRows = 4;
    Store store = makeStore<ColumnStore>(cfg, schema);
    store.open();

    const uint64_t numRows = 10; /// 2 full row groups (4 + 4) + 2 left in the active buffer
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const auto row = makeStructuredRow(i, (i * 3) + 1, 2000 + i);
        store.writeRecord(row.data(), rowWidth, Timestamp(2000 + i));
    }
    EXPECT_EQ(concrete(store).debugRowGroupCount(), 2U);
    EXPECT_EQ(concrete(store).debugActiveRowCount(), 2U);

    store.flush(); /// seals the trailing partial row group too
    EXPECT_EQ(concrete(store).debugRowGroupCount(), 3U);

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();
    const uint64_t got = store.read(out, schema, TimeRange{});
    ASSERT_EQ(got, numRows);

    auto span = out.getAvailableMemoryArea<uint8_t>();
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const uint8_t* rowPtr = span.data() + (i * rowWidth);
        EXPECT_EQ(readU64(rowPtr, 0), i) << "row " << i << " id mismatch";
        EXPECT_EQ(readU64(rowPtr, sizeof(uint64_t)), (i * 3) + 1) << "row " << i << " value mismatch";
        EXPECT_EQ(readU64(rowPtr, 2 * sizeof(uint64_t)), 2000 + i) << "row " << i << " ts mismatch";
    }
}

TEST_F(ColumnStoreTest, TimeRangeRowGroupPruning)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto cfg = cfgFor("prune_test");
    cfg.rowGroupRows = 3;
    Store store = makeStore<ColumnStore>(cfg, schema);
    store.open();

    for (uint64_t ts : {100U, 110U, 120U})
    {
        const auto row = makeRow(rowWidth, 0x0A);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts));
    }
    for (uint64_t ts : {500U, 510U, 520U})
    {
        const auto row = makeRow(rowWidth, 0x0B);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts));
    }

    EXPECT_EQ(concrete(store).debugRowGroupCount(), 2U);

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();

    TimeRange range;
    range.fieldName = "ts";
    range.start = Timestamp(50);
    range.end = Timestamp(200);

    const uint64_t got = store.read(out, schema, range);
    EXPECT_EQ(got, 3U);
}

TEST_F(ColumnStoreTest, ReadRowLevelFilter)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto cfg = cfgFor("rowfilter_test");
    cfg.rowGroupRows = 5;
    Store store = makeStore<ColumnStore>(cfg, schema);
    store.open();

    for (uint64_t ts : {100U, 101U, 102U, 103U, 104U})
    {
        const auto row = makeRowWithTs(schema, ts);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts));
    }

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();

    TimeRange range;
    range.fieldName = "ts";
    range.start = Timestamp(102);
    range.end = Timestamp(105);

    const uint64_t got = store.read(out, schema, range);
    EXPECT_EQ(got, 3U);
}

TEST_F(ColumnStoreTest, ReadRowLevelFilterNullableTsField)
{
    const Schema schema = makeSchemaNullableTs();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto cfg = cfgFor("rowfilter_nullable_ts_test");
    cfg.rowGroupRows = 5;
    Store store = makeStore<ColumnStore>(cfg, schema);
    store.open();

    for (uint64_t ts : {100U, 101U, 102U, 103U, 104U})
    {
        const auto row = makeRowWithTs(schema, ts);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts));
    }

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();

    TimeRange range;
    range.fieldName = "ts";
    range.start = Timestamp(102);
    range.end = Timestamp(105);

    const uint64_t got = store.read(out, schema, range);
    EXPECT_EQ(got, 3U);
}

TEST_F(ColumnStoreTest, ReadRowLevelFilterMixedNullability)
{
    const Schema schema = makeSchemaMixedNullability();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto cfg = cfgFor("rowfilter_mixed_nullability_test");
    cfg.rowGroupRows = 5;
    Store store = makeStore<ColumnStore>(cfg, schema);
    store.open();

    for (uint64_t ts : {100U, 101U, 102U, 103U, 104U})
    {
        const auto row = makeRowWithTs(schema, ts);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts));
    }

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();

    TimeRange range;
    range.fieldName = "ts";
    range.start = Timestamp(102);
    range.end = Timestamp(105);

    const uint64_t got = store.read(out, schema, range);
    EXPECT_EQ(got, 3U);
}

TEST_F(ColumnStoreTest, ReadUnboundedRoundTripPreservesBytes)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto cfg = cfgFor("roundtrip_test");
    cfg.rowGroupRows = 5;
    Store store = makeStore<ColumnStore>(cfg, schema);
    store.open();

    const uint64_t numRows = 5;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const auto row = makeRow(rowWidth, static_cast<uint8_t>(0x10 + i));
        store.writeRecord(row.data(), rowWidth, Timestamp(100 + i));
    }

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

TEST_F(ColumnStoreTest, DrainReturnsRowMajorBytesAndResetsSelf)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto cfg = cfgFor("drain_test");
    cfg.rowGroupRows = 3;
    Store store = makeStore<ColumnStore>(cfg, schema);
    store.open();

    /// Seal one row group...
    for (uint64_t i = 0; i < 3; ++i)
    {
        const auto row = makeRow(rowWidth, static_cast<uint8_t>(0x10 + i));
        store.writeRecord(row.data(), rowWidth, Timestamp(100 + i));
    }
    ASSERT_EQ(concrete(store).debugRowGroupCount(), 1U);

    /// ...then leave a few more rows unsealed in the active buffer.
    for (uint64_t i = 0; i < 2; ++i)
    {
        const auto row = makeRow(rowWidth, static_cast<uint8_t>(0x20 + i));
        store.writeRecord(row.data(), rowWidth, Timestamp(200 + i));
    }
    ASSERT_EQ(concrete(store).debugActiveRowCount(), 2U);

    auto drained = store.getAs<ColumnStore>().getMutable().drain();

    ASSERT_EQ(drained.bytes.size(), 5U * rowWidth);
    for (uint64_t i = 0; i < 5; ++i)
    {
        const uint8_t expectedFill = i < 3 ? static_cast<uint8_t>(0x10 + i) : static_cast<uint8_t>(0x20 + (i - 3));
        for (uint32_t b = 0; b < rowWidth; ++b)
        {
            EXPECT_EQ(drained.bytes[(i * rowWidth) + b], expectedFill);
        }
    }
    EXPECT_EQ(drained.minTs, Timestamp(100));
    EXPECT_EQ(drained.maxTs, Timestamp(201));

    EXPECT_EQ(concrete(store).debugRowGroupCount(), 0U);
    EXPECT_EQ(concrete(store).debugActiveRowCount(), 0U);

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();
    EXPECT_EQ(store.read(out, schema, TimeRange{}), 0U);
}

}
