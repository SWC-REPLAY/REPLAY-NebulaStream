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

/// Standalone tests for LosslessStore (no transformations, no chaining).
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
#include <lossless/LosslessStore.hpp>
#include <Store.hpp>
#include <StoreReadCursor.hpp>
#include <TimeRange.hpp>

namespace NES
{

/// Test fixture — inheriting BaseUnitTest sets up logging and the test environment.
class LosslessStoreTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("LosslessStoreTest.log", LogLevel::LOG_DEBUG); }
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

const LosslessStore& concrete(const Store& store)
{
    return store.getAs<LosslessStore>().get();
}

LosslessStore::Config cfgFor(const std::string& name)
{
    LosslessStore::Config cfg;
    cfg.storeName = name;
    cfg.storeDir = "/tmp";
    return cfg;
}
}

TEST_F(LosslessStoreTest, FlushSealsOneSegment)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<LosslessStore>(cfgFor("seal_test"), schema);
    store.open();

    const auto row = makeRow(rowWidth, 0xAB);
    store.writeRecord(row.data(), rowWidth, Timestamp(100));
    store.writeRecord(row.data(), rowWidth, Timestamp(110));
    store.writeRecord(row.data(), rowWidth, Timestamp(120));

    EXPECT_EQ(concrete(store).debugSegmentCount(), 0U);
    EXPECT_EQ(concrete(store).debugActiveRowCount(), 3U);

    store.flush();

    EXPECT_EQ(concrete(store).debugSegmentCount(), 1U);
    EXPECT_EQ(concrete(store).debugActiveRowCount(), 0U);
}

TEST_F(LosslessStoreTest, CompressionReducesSize)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<LosslessStore>(cfgFor("ratio_test"), schema);
    store.open();

    const auto row = makeRow(rowWidth, 0x00);
    const uint64_t numRows = 1000;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        store.writeRecord(row.data(), rowWidth, Timestamp(i));
    }
    store.flush();

    EXPECT_EQ(concrete(store).debugSegmentCount(), 1U);
    EXPECT_LT(store.size(), numRows * rowWidth);
}

TEST_F(LosslessStoreTest, AutoSealsAtSegmentSize)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto cfg = cfgFor("autoseal_test");
    cfg.segmentSize = 2 * rowWidth;
    Store store = makeStore<LosslessStore>(cfg, schema);
    store.open();

    const auto row = makeRow(rowWidth, 0x01);
    for (uint64_t i = 0; i < 6; ++i)
    {
        store.writeRecord(row.data(), rowWidth, Timestamp(i));
    }

    EXPECT_GE(concrete(store).debugSegmentCount(), 2U);
}

TEST_F(LosslessStoreTest, ReadUnboundedRoundTrip)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<LosslessStore>(cfgFor("roundtrip_test"), schema);
    store.open();

    const uint64_t numRows = 5;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const auto row = makeRow(rowWidth, static_cast<uint8_t>(0x10 + i));
        store.writeRecord(row.data(), rowWidth, Timestamp(100 + i));
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

/// A destination smaller than a segment makes several reads land in the same segment, and each
/// of those used to decompress the whole segment again -- 32 decompressions for 4 segments here.
/// The decoded bytes now live on the cursor, so a read resuming where it stopped reuses them.
///
/// Without the reuse this still returns every row: the count is the only thing that catches it,
/// which is why debugDecodeCount exists.
TEST_F(LosslessStoreTest, ResumingInsideASegmentDecompressesItOnce)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    auto cfg = cfgFor("decode_once_test");
    cfg.segmentSize = rowWidth * 64;
    Store store = makeStore<LosslessStore>(cfg, schema);
    store.open();

    constexpr uint64_t numRows = 256;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const auto row = makeRow(rowWidth, static_cast<uint8_t>(i));
        store.writeRecord(row.data(), rowWidth, Timestamp(1000 + i));
    }
    store.flush();

    const auto& impl = store.getAs<LosslessStore>().get();
    const uint64_t segments = impl.debugSegmentCount();
    ASSERT_GT(segments, 1U) << "the rows have to span several segments for this to mean anything";

    /// Eight rows per call against 64 rows per segment, so every segment is re-entered eight times.
    auto bufferManager = BufferManager::create(rowWidth * 8, 8);
    auto out = bufferManager->getBufferBlocking();
    StoreReadCursor cursor;
    std::vector<uint8_t> seen;
    uint64_t calls = 0;
    while (const uint64_t got = store.read(out, schema, TimeRange{}, cursor))
    {
        ++calls;
        const auto span = out.getAvailableMemoryArea<const uint8_t>();
        seen.insert(seen.end(), span.data(), span.data() + (got * rowWidth));
    }

    ASSERT_EQ(seen.size(), numRows * rowWidth) << "every row must still come back";
    for (uint64_t i = 0; i < numRows; ++i)
    {
        EXPECT_EQ(seen[i * rowWidth], static_cast<uint8_t>(i)) << "row " << i;
    }
    EXPECT_GT(calls, segments) << "the destination must be smaller than a segment, or this proves nothing";
    EXPECT_EQ(impl.debugDecodeCount(), segments) << "each segment decompressed exactly once";
}

TEST_F(LosslessStoreTest, ReadPrunesByTimeRange)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<LosslessStore>(cfgFor("prune_test"), schema);
    store.open();

    for (uint64_t ts : {100U, 110U, 120U})
    {
        const auto row = makeRow(rowWidth, 0x0A);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts));
    }
    store.flush();

    for (uint64_t ts : {500U, 510U, 520U})
    {
        const auto row = makeRow(rowWidth, 0x0B);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts));
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

TEST_F(LosslessStoreTest, ReadRowLevelFilter)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<LosslessStore>(cfgFor("rowfilter_test"), schema);
    store.open();

    for (uint64_t ts : {100U, 101U, 102U, 103U, 104U})
    {
        const auto row = makeRowWithTs(schema, ts);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts));
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

TEST_F(LosslessStoreTest, ReadRowLevelFilterNullableTsField)
{
    const Schema schema = makeSchemaNullableTs();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<LosslessStore>(cfgFor("rowfilter_nullable_ts_test"), schema);
    store.open();

    for (uint64_t ts : {100U, 101U, 102U, 103U, 104U})
    {
        const auto row = makeRowWithTs(schema, ts);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts));
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

TEST_F(LosslessStoreTest, DrainResetsStore)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<LosslessStore>(cfgFor("drain_test"), schema);
    store.open();

    /// Seal one segment...
    for (uint64_t i = 0; i < 3; ++i)
    {
        const auto row = makeRow(rowWidth, static_cast<uint8_t>(0x10 + i));
        store.writeRecord(row.data(), rowWidth, Timestamp(100 + i));
    }
    store.flush();
    ASSERT_EQ(concrete(store).debugSegmentCount(), 1U);

    /// ...then leave a few more rows unsealed in the active buffer.
    for (uint64_t i = 0; i < 2; ++i)
    {
        const auto row = makeRow(rowWidth, static_cast<uint8_t>(0x20 + i));
        store.writeRecord(row.data(), rowWidth, Timestamp(200 + i));
    }
    ASSERT_EQ(concrete(store).debugActiveRowCount(), 2U);

    auto drained = store.getAs<LosslessStore>().getMutable().drain();

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

    EXPECT_EQ(concrete(store).debugSegmentCount(), 0U);
    EXPECT_EQ(concrete(store).debugActiveRowCount(), 0U);

    auto bufferManager = BufferManager::create();
    TupleBuffer out = bufferManager->getBufferBlocking();
    EXPECT_EQ(store.read(out, schema, TimeRange{}), 0U);
}

TEST_F(LosslessStoreTest, ReadRowLevelFilterMixedNullability)
{
    const Schema schema = makeSchemaMixedNullability();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    Store store = makeStore<LosslessStore>(cfgFor("rowfilter_mixed_nullability_test"), schema);
    store.open();

    for (uint64_t ts : {100U, 101U, 102U, 103U, 104U})
    {
        const auto row = makeRowWithTs(schema, ts);
        store.writeRecord(row.data(), rowWidth, Timestamp(ts));
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
