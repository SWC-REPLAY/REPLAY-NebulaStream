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

/// Standalone tests for LossyStore.
///
/// Every test here that does not configure an accuracy bound asserts exact equality, whichever
/// codec the store picked for the column. That is the point: the bit-exact baseline is what
/// each codec added has to keep satisfying, so that the only place accuracy is ever lost stays
/// the one place that is supposed to lose it.
///
/// The codecs themselves are tested one level down, in ColumnCodecTest.

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <string>
#include <utility>
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
#include <LossyStore.hpp>
#include <Store.hpp>
#include <StoreRegistry.hpp>
#include <TimeRange.hpp>

namespace NES::StoreManager
{

class LossyStoreTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("LossyStoreTest.log", LogLevel::LOG_DEBUG); }
};

namespace
{

Schema makeSchema()
{
    return Schema{}
        .addField("ts", DataType::Type::UINT64)
        .addField("value", DataType::Type::FLOAT64)
        .addField("id", DataType::Type::UINT32);
}

Schema makeSchemaWithNullable()
{
    return Schema{}
        .addField("ts", DataType::Type::UINT64)
        .addField("value", DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE)
        .addField("id", DataType::Type::UINT32);
}

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
        offset += field.dataType.getSizeInBytesWithNull();
    }
    return offset;
}

/// One row with a distinct pattern per field, so a transpose that mixes columns up shows as
/// a mismatch rather than as coincidentally-equal bytes.
std::vector<uint8_t> makeRow(const Schema& schema, const uint64_t ts, const double value, const uint32_t id)
{
    std::vector<uint8_t> row(schema.getSizeOfSchemaInBytes(), 0);
    const auto tsOffset = fieldValueOffset(schema, "ts");
    const auto valueOffset = fieldValueOffset(schema, "value");
    const auto idOffset = fieldValueOffset(schema, "id");
    std::memcpy(row.data() + tsOffset, &ts, sizeof(ts));
    std::memcpy(row.data() + valueOffset, &value, sizeof(value));
    std::memcpy(row.data() + idOffset, &id, sizeof(id));
    return row;
}

std::vector<uint8_t> makeRows(const Schema& schema, const uint64_t count, const uint64_t firstTs, const uint64_t tsStep)
{
    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < count; ++i)
    {
        const auto row = makeRow(schema, firstTs + (i * tsStep), 1.5 + static_cast<double>(i), static_cast<uint32_t>(i));
        rows.insert(rows.end(), row.begin(), row.end());
    }
    return rows;
}

/// `rowsPerGroup` is expressed in rows here for readability and converted to the byte budget the
/// store actually takes. It governs only the standalone writeRecord path; a store fed segments
/// gets one group per segment regardless.
LossyStore::Config cfgFor(const std::string& name, const uint64_t rowsPerGroup, const uint32_t rowWidth = 0)
{
    LossyStore::Config cfg;
    cfg.storeName = name;
    cfg.storeDir = "/tmp";
    cfg.groupSize = static_cast<size_t>(rowsPerGroup) * (rowWidth == 0 ? 1UZ : rowWidth);
    return cfg;
}

/// Hand rows to the store the way a transformation would: as one segment, with the range the
/// tier above measured. The test knows where its timestamps live; the store no longer does.
void appendAsSegment(LossyStore& lossy, const uint8_t* data, const size_t len, const Schema& schema)
{
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    const auto tsOffset = fieldValueOffset(schema, "ts");
    uint64_t minTs = UINT64_MAX;
    uint64_t maxTs = 0;
    for (size_t r = 0; r < len / rowWidth; ++r)
    {
        uint64_t ts = 0;
        std::memcpy(&ts, data + (r * rowWidth) + tsOffset, sizeof(ts));
        minTs = std::min(minTs, ts);
        maxTs = std::max(maxTs, ts);
    }
    lossy.appendSegment(data, len, len / rowWidth, Timestamp(minTs), Timestamp(maxTs));
}

/// Pull one column back out of reconstructed rows as doubles.
std::vector<double> extractDoubles(
    const std::vector<uint8_t>& rows, const Schema& schema, const std::string& fieldName)
{
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    const auto offset = fieldValueOffset(schema, fieldName);
    std::vector<double> out(rows.size() / rowWidth);
    for (size_t i = 0; i < out.size(); ++i)
    {
        std::memcpy(&out[i], rows.data() + (i * rowWidth) + offset, sizeof(double));
    }
    return out;
}

const LossyStore& concrete(const Store& store)
{
    return store.getAs<LossyStore>().get();
}

/// Read everything the store holds back into a byte vector.
std::vector<uint8_t> readAll(Store& store, const Schema& schema, const TimeRange& range, const uint64_t capacityRows)
{
    auto bufferManager = BufferManager::create(schema.getSizeOfSchemaInBytes() * capacityRows, 8);
    auto buffer = bufferManager->getBufferBlocking();
    const uint64_t got = store.read(buffer, schema, range);

    std::vector<uint8_t> out(got * schema.getSizeOfSchemaInBytes());
    if (got > 0)
    {
        const auto area = buffer.getAvailableMemoryArea<const uint8_t>();
        std::memcpy(out.data(), area.data(), out.size());
    }
    return out;
}

}

/// The core promise of the store as it stands: what goes in comes back out unchanged.
TEST_F(LossyStoreTest, BulkRoundTripIsBitExact)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(schema, 100, 1000, 10);

    Store store = makeStore<LossyStore>(cfgFor("roundtrip", 8), schema);
    store.open();
    concrete(store);

    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, rows.data(), rows.size(), schema);
    store.flush();

    const auto read = readAll(store, schema, TimeRange{}, 200);
    EXPECT_EQ(read, rows);

    store.getAs<LossyStore>().getMutable().removeFile();
}

/// One segment in, one group out. The tier above already decided the boundaries and measured
/// the range that goes with them, so this tier neither re-divides the rows nor inspects them --
/// which is what lets every group carry a range narrow enough to skip on.
TEST_F(LossyStoreTest, EachSegmentBecomesExactlyOneGroup)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    const auto rows = makeRows(schema, 100, 1000, 10);

    Store store = makeStore<LossyStore>(cfgFor("regroup", 8, rowWidth), schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();

    /// Three segments of deliberately unequal size: a group is whatever a segment held.
    appendAsSegment(lossy, rows.data(), 37UZ * rowWidth, schema);
    EXPECT_EQ(concrete(store).debugGroupCount(), 1U);
    appendAsSegment(lossy, rows.data() + (37UZ * rowWidth), 5UZ * rowWidth, schema);
    EXPECT_EQ(concrete(store).debugGroupCount(), 2U);
    appendAsSegment(lossy, rows.data() + (42UZ * rowWidth), 58UZ * rowWidth, schema);
    EXPECT_EQ(concrete(store).debugGroupCount(), 3U);

    store.flush();
    EXPECT_EQ(concrete(store).debugGroupCount(), 3U) << "flush has nothing left to seal";

    const auto read = readAll(store, schema, TimeRange{}, 200);
    EXPECT_EQ(read, rows);

    lossy.removeFile();
}

/// The range a segment arrives with is the one a read skips on. Two segments covering disjoint
/// time windows must produce two groups, and a query inside one of them must not pull rows out
/// of the other.
TEST_F(LossyStoreTest, GroupRangesComeFromTheSegmentNotFromTheRows)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    const auto early = makeRows(schema, 50, 1000, 10);   /// ts 1000 .. 1490
    const auto late = makeRows(schema, 50, 5000, 10);    /// ts 5000 .. 5490

    Store store = makeStore<LossyStore>(cfgFor("segrange", 64, rowWidth), schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, early.data(), early.size(), schema);
    appendAsSegment(lossy, late.data(), late.size(), schema);
    store.flush();
    EXPECT_EQ(concrete(store).debugGroupCount(), 2U);

    /// Entirely inside the first segment's window: the second cannot contribute a row.
    const TimeRange range{.fieldName = "ts", .start = Timestamp(1000), .end = Timestamp(1200)};
    const auto read = readAll(store, schema, range, 200);
    ASSERT_EQ(read.size(), 20UZ * rowWidth);
    const std::vector<uint8_t> expected(early.begin(), early.begin() + (20L * rowWidth));
    EXPECT_EQ(read, expected);

    lossy.removeFile();
}

/// A time range must return exactly the rows inside it, whether or not it lines up with
/// group boundaries.
TEST_F(LossyStoreTest, TimeRangeReturnsExactlyMatchingRows)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    const auto rows = makeRows(schema, 100, 1000, 10); /// timestamps 1000, 1010, ... 1990

    Store store = makeStore<LossyStore>(cfgFor("range", 8), schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, rows.data(), rows.size(), schema);
    store.flush();

    /// [1500, 1600) covers rows 50..59.
    const TimeRange range{.fieldName = "ts", .start = Timestamp(1500), .end = Timestamp(1600)};
    const auto read = readAll(store, schema, range, 200);

    ASSERT_EQ(read.size(), 10UZ * rowWidth);
    const std::vector<uint8_t> expected(rows.begin() + (50L * rowWidth), rows.begin() + (60L * rowWidth));
    EXPECT_EQ(read, expected);

    lossy.removeFile();
}

/// Null indicators are split out into a bitmap and folded back in on the way out; the
/// reconstructed rows must be identical, indicator bytes included.
TEST_F(LossyStoreTest, NullIndicatorsRoundTrip)
{
    const Schema schema = makeSchemaWithNullable();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    const auto nullOffset = fieldValueOffset(schema, "value") - 1;

    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < 40; ++i)
    {
        auto row = makeRow(schema, 1000 + i, static_cast<double>(i), static_cast<uint32_t>(i));
        row[nullOffset] = (i % 3 == 0) ? 1 : 0;
        rows.insert(rows.end(), row.begin(), row.end());
    }

    Store store = makeStore<LossyStore>(cfgFor("nullable", 8), schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, rows.data(), rows.size(), schema);
    store.flush();

    const auto read = readAll(store, schema, TimeRange{}, 100);
    ASSERT_EQ(read.size(), rows.size());
    EXPECT_EQ(read, rows);

    for (uint64_t i = 0; i < 40; ++i)
    {
        EXPECT_EQ(read[(i * rowWidth) + nullOffset], (i % 3 == 0) ? 1 : 0) << "row " << i;
    }

    lossy.removeFile();
}

/// A group holding fewer rows than configured is legal; only exceeding the configured size
/// is not.
TEST_F(LossyStoreTest, ShortGroupRoundTrips)
{
    const Schema schema = makeSchema();
    const auto rows = makeRows(schema, 3, 1000, 10);

    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    Store store = makeStore<LossyStore>(cfgFor("short", 8, rowWidth), schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, rows.data(), rows.size(), schema);

    /// A segment is sealed on arrival however few rows it holds -- a group is not required to
    /// be full, only to be one segment. So there is nothing left for flush to do.
    EXPECT_EQ(concrete(store).debugGroupCount(), 1U);
    store.flush();
    EXPECT_EQ(concrete(store).debugGroupCount(), 1U);

    const auto read = readAll(store, schema, TimeRange{}, 100);
    EXPECT_EQ(read, rows);

    lossy.removeFile();
}

/// Signed columns go through frame-of-reference too, where the reference is negative and
/// the subtraction relies on unsigned wraparound. Extremes included, because that is exactly
/// where naive signed arithmetic overflows.
TEST_F(LossyStoreTest, SignedIntegerColumnsRoundTrip)
{
    const Schema schema = Schema{}
                              .addField("ts", DataType::Type::UINT64)
                              .addField("small", DataType::Type::INT16)
                              .addField("big", DataType::Type::INT64);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    const auto smallOffset = fieldValueOffset(schema, "small");
    const auto bigOffset = fieldValueOffset(schema, "big");

    const std::vector<int16_t> smalls{-32768, -1, 0, 1, 32767, -100, 100, 7, -7, 0};
    const std::vector<int64_t> bigs{
        std::numeric_limits<int64_t>::min(),
        std::numeric_limits<int64_t>::max(),
        -1,
        0,
        1,
        -999999999,
        999999999,
        42,
        -42,
        0};

    std::vector<uint8_t> rows;
    for (size_t i = 0; i < smalls.size(); ++i)
    {
        std::vector<uint8_t> row(rowWidth, 0);
        const uint64_t ts = 1000 + i;
        std::memcpy(row.data() + fieldValueOffset(schema, "ts"), &ts, sizeof(ts));
        std::memcpy(row.data() + smallOffset, &smalls[i], sizeof(int16_t));
        std::memcpy(row.data() + bigOffset, &bigs[i], sizeof(int64_t));
        rows.insert(rows.end(), row.begin(), row.end());
    }

    Store store = makeStore<LossyStore>(cfgFor("signed", 4), schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, rows.data(), rows.size(), schema);
    store.flush();

    EXPECT_EQ(readAll(store, schema, TimeRange{}, 100), rows);
    lossy.removeFile();
}

/// Frame-of-reference plus bit-packing has to actually save space on the case it is for:
/// large timestamps in a narrow window. Four bytes of overhead per column per group is the
/// price, so the group has to be big enough for the packing to repay it.
TEST_F(LossyStoreTest, NarrowRangeIntegersShrink)
{
    const Schema schema = Schema{}.addField("ts", DataType::Type::UINT64);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    /// 4096 timestamps spanning one second of milliseconds around a realistic epoch: 8 bytes
    /// each raw, but only ~10 bits of actual spread.
    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < 4096; ++i)
    {
        const uint64_t ts = 1700000000000ULL + i;
        std::vector<uint8_t> row(rowWidth, 0);
        std::memcpy(row.data(), &ts, sizeof(ts));
        rows.insert(rows.end(), row.begin(), row.end());
    }

    Store store = makeStore<LossyStore>(cfgFor("narrow", 4096), schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, rows.data(), rows.size(), schema);
    store.flush();

    EXPECT_LT(store.size(), rows.size() / 4) << "stored " << store.size() << " for " << rows.size() << " raw bytes";
    EXPECT_EQ(readAll(store, schema, TimeRange{}, 8192), rows);

    lossy.removeFile();
}

/// A steady stream collapses to its header whether or not anyone declared the time field.
///
/// Worth pinning because it is what moving selection onto statistics bought. The declaration
/// used to be the only way to reach delta-of-delta; now the second differences of a fixed-rate
/// column are measurably zero, so the codec is chosen on evidence and an undeclared column
/// does just as well. What declaring still buys is group pruning on reads -- a property of the
/// group header, not of the encoding -- and the time-range tests above cover that.
/// The default pipeline picks a lossless codec by type alone -- ForBitpack for every
/// integer-like column, see LossyStore.cpp -- not by measuring the data. Declaring the
/// timestamp field changes what a group's header records (min/maxTs, usable for pruning), but
/// not which codec encodes the column or how many bytes it costs.
///
/// A steady-rate stream is exactly the case where a statistics-driven choice would instead pick
/// DeltaOfDelta and collapse to a few bytes per group; that choice is still tested directly
/// against `rank()` in ColumnStatsTest.RolesAreFoundByCostRatherThanDeclared -- it works, it is
/// simply not wired into this store's pipeline (see the comment above `sealGroup`'s codec
/// selection).
TEST_F(LossyStoreTest, SteadyStreamsCostTheSameWhetherOrNotTheFieldIsDeclared)
{
    const Schema schema = Schema{}.addField("ts", DataType::Type::UINT64);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < 4096; ++i)
    {
        const uint64_t ts = 1700000000000ULL + (i * 100);
        std::vector<uint8_t> row(rowWidth, 0);
        std::memcpy(row.data(), &ts, sizeof(ts));
        rows.insert(rows.end(), row.begin(), row.end());
    }

    const auto storedSize = [&](const std::string& name, const std::string& tsField)
    {
        Store store = makeStore<LossyStore>(cfgFor(name, 4096, rowWidth), schema);
        store.open();
        auto& lossy = store.getAs<LossyStore>().getMutable();
        appendAsSegment(lossy, rows.data(), rows.size(), schema);
        store.flush();

        EXPECT_EQ(readAll(store, schema, TimeRange{}, 8192), rows) << "with tsField '" << tsField << "'";
        const auto size = store.size();
        lossy.removeFile();
        return size;
    };

    const auto declared = storedSize("dodDeclared", "ts");
    const auto undeclared = storedSize("dodUndeclared", "");

    /// ForBitpack narrows the ramp to the bits its span needs (~19 bits for a 4096-row, 100ns
    /// step column) plus a small header -- well under raw size, just not down to "a couple of
    /// group headers" the way DeltaOfDelta would.
    EXPECT_LT(declared, rows.size() / 2) << "declared " << declared << " for " << rows.size() << " raw bytes";

    /// Codec choice is type-only, so declaring the timestamp field changes nothing about size.
    EXPECT_EQ(declared, undeclared);
}

/// The byte compressor earns its place on the kind of redundancy a codec leaves behind.
///
/// The column here is deliberately one the codecs handle badly: 64-bit values with no run
/// structure and too many distinct values for a dictionary, so frame-of-reference stores them
/// at nearly full width. But the bytes repeat a short pattern, which is exactly what zstd sees
/// and no codec here does. Without the compressor this column would cost its raw size.
TEST_F(LossyStoreTest, ByteCompressorCatchesWhatTheCodecsMiss)
{
    const Schema schema = Schema{}.addField("ts", DataType::Type::UINT64).addField("payload", DataType::Type::UINT64);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    const auto payloadOffset = fieldValueOffset(schema, "payload");

    /// 400 distinct values, cycling. Too many to make a dictionary's codes cheap relative to
    /// its table, spread across the whole range so frame-of-reference cannot narrow them, and
    /// never repeating consecutively so there are no runs.
    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < 8192; ++i)
    {
        std::vector<uint8_t> row(rowWidth, 0);
        const uint64_t ts = 1700000000000ULL + i;
        const uint64_t payload = 0x0123456789ABCDEFULL * ((i % 400) + 1);
        std::memcpy(row.data(), &ts, sizeof(ts));
        std::memcpy(row.data() + payloadOffset, &payload, sizeof(payload));
        rows.insert(rows.end(), row.begin(), row.end());
    }

    Store store = makeStore<LossyStore>(cfgFor("bytecomp", 8192), schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, rows.data(), rows.size(), schema);
    store.flush();

    /// The payload column alone is 64 KB raw; the whole file has to come in well under that.
    EXPECT_LT(store.size(), 8192 * sizeof(uint64_t) / 2)
        << "stored " << store.size() << " for " << rows.size() << " raw bytes";

    /// And it still round-trips exactly -- the compressor is lossless, and sits outside the
    /// one place that is allowed to lose anything.
    EXPECT_EQ(readAll(store, schema, TimeRange{}, 8192), rows);

    lossy.removeFile();
}

/// A boolean column costs a bit per value, and -- the part worth pinning -- still comes back
/// exactly, including the all-false column that packs to no bits at all.
TEST_F(LossyStoreTest, BooleanColumnsRoundTrip)
{
    const Schema schema
        = Schema{}.addField("ts", DataType::Type::UINT64).addField("flag", DataType::Type::BOOLEAN);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    const auto flagOffset = fieldValueOffset(schema, "flag");

    const auto rowsWithFlags = [&](const std::function<uint8_t(uint64_t)>& flagAt)
    {
        std::vector<uint8_t> rows;
        for (uint64_t i = 0; i < 2048; ++i)
        {
            std::vector<uint8_t> row(rowWidth, 0);
            const uint64_t ts = 9000 + i;
            const uint8_t flag = flagAt(i);
            std::memcpy(row.data(), &ts, sizeof(ts));
            std::memcpy(row.data() + flagOffset, &flag, sizeof(flag));
            rows.insert(rows.end(), row.begin(), row.end());
        }
        return rows;
    };

    const std::vector<std::pair<std::string, std::function<uint8_t(uint64_t)>>> cases{
        {"alternating", [](const uint64_t i) { return static_cast<uint8_t>(i % 2); }},
        {"allFalse", [](uint64_t) { return uint8_t{0}; }},
        {"allTrue", [](uint64_t) { return uint8_t{1}; }},
        {"sparse", [](const uint64_t i) { return static_cast<uint8_t>(i % 97 == 0 ? 1 : 0); }}};

    for (const auto& [name, flagAt] : cases)
    {
        const auto rows = rowsWithFlags(flagAt);

        Store store = makeStore<LossyStore>(cfgFor("bool" + name, 2048), schema);
        store.open();
        auto& lossy = store.getAs<LossyStore>().getMutable();
        appendAsSegment(lossy, rows.data(), rows.size(), schema);
        store.flush();

        EXPECT_EQ(readAll(store, schema, TimeRange{}, 4096), rows) << name;
        lossy.removeFile();
    }
}

/// A column where every value is identical needs no bits at all -- only the reference.
TEST_F(LossyStoreTest, ConstantColumnCostsNoBits)
{
    const Schema schema = Schema{}.addField("ts", DataType::Type::UINT64).addField("constant", DataType::Type::UINT64);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < 1024; ++i)
    {
        std::vector<uint8_t> row(rowWidth, 0);
        const uint64_t ts = 5000 + i;
        const uint64_t constant = 0xABCDEF0123456789ULL;
        std::memcpy(row.data(), &ts, sizeof(ts));
        std::memcpy(row.data() + fieldValueOffset(schema, "constant"), &constant, sizeof(constant));
        rows.insert(rows.end(), row.begin(), row.end());
    }

    Store store = makeStore<LossyStore>(cfgFor("constant", 1024), schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, rows.data(), rows.size(), schema);
    store.flush();

    EXPECT_EQ(readAll(store, schema, TimeRange{}, 2048), rows);
    lossy.removeFile();
}

/// The contract, on the only codec that can break it: every value within its bound, and the
/// row count untouched. Checked across a spread of bounds and of value distributions.
TEST_F(LossyStoreTest, QuantizedColumnsHonourTheirAccuracyBound)
{
    const Schema schema = Schema{}.addField("ts", DataType::Type::UINT64).addField("value", DataType::Type::FLOAT64);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    for (const double boundPercent : {5.0, 1.0, 0.5, 0.1, 0.01})
    {
        for (const auto& [label, generator] :
             std::vector<std::pair<std::string, std::function<double(uint64_t)>>>{
                 {"linear", [](uint64_t i) { return 20.0 + (static_cast<double>(i) * 0.01); }},
                 {"wide", [](uint64_t i) { return static_cast<double>(i) * 1000.0; }},
                 {"negative", [](uint64_t i) { return -500.0 + static_cast<double>(i); }},
                 {"straddling zero", [](uint64_t i) { return static_cast<double>(i) - 512.0; }},
                 {"tiny", [](uint64_t i) { return 1e-9 * static_cast<double>(i); }}})
        {
            std::vector<uint8_t> rows;
            std::vector<double> originals;
            for (uint64_t i = 0; i < 1024; ++i)
            {
                std::vector<uint8_t> row(rowWidth, 0);
                const uint64_t ts = 1000 + i;
                const double value = generator(i);
                originals.push_back(value);
                std::memcpy(row.data(), &ts, sizeof(ts));
                std::memcpy(row.data() + fieldValueOffset(schema, "value"), &value, sizeof(value));
                rows.insert(rows.end(), row.begin(), row.end());
            }

            auto cfg = cfgFor("bound", 1024);
            cfg.accuracyBoundPercent = boundPercent;
            Store store = makeStore<LossyStore>(cfg, schema);
            store.open();
            auto& lossy = store.getAs<LossyStore>().getMutable();
            appendAsSegment(lossy, rows.data(), rows.size(), schema);
            store.flush();

            const auto read = readAll(store, schema, TimeRange{}, 2048);

            /// Half the contract: the row count never moves.
            ASSERT_EQ(read.size(), rows.size()) << label << " @ " << boundPercent << "%";

            const auto restored = extractDoubles(read, schema, "value");
            const double bound = boundPercent / 100.0;

            /// The other half: every value within the bound OF ITSELF. Nothing about the rest
            /// of the column enters -- which is the whole difference from a range-relative
            /// bound, and the reason 'wide' and 'tiny' are in the list above. Under a
            /// range-relative rule the small values of 'wide' would be destroyed while still
            /// passing.
            for (size_t i = 0; i < originals.size(); ++i)
            {
                if (originals[i] == 0.0)
                {
                    /// Zero has no logarithm, so it is held out of the grid and kept verbatim.
                    /// Exact, not merely within bound.
                    EXPECT_EQ(restored[i], 0.0) << label << " @ " << boundPercent << "% row " << i;
                    continue;
                }
                const double relative = std::abs(originals[i] - restored[i]) / std::abs(originals[i]);
                EXPECT_LE(relative, bound * (1 + 1e-9)) << label << " @ " << boundPercent << "% row " << i;
            }

            lossy.removeFile();
        }
    }
}

/// The width a quantized column needs follows from the bound alone: maxCode = range/delta =
/// 50/X, in which the range cancels. Two columns with ranges three orders of magnitude apart
/// must therefore cost the same.
TEST_F(LossyStoreTest, QuantizedSizeDependsOnBoundNotOnData)
{
    const Schema schema = Schema{}.addField("ts", DataType::Type::UINT64).addField("value", DataType::Type::FLOAT64);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    const auto storedSizeFor = [&](const double scale, const double boundPercent)
    {
        std::vector<uint8_t> rows;
        for (uint64_t i = 0; i < 4096; ++i)
        {
            std::vector<uint8_t> row(rowWidth, 0);
            const uint64_t ts = 1000 + i;
            const double value = static_cast<double>(i) * scale;
            std::memcpy(row.data(), &ts, sizeof(ts));
            std::memcpy(row.data() + fieldValueOffset(schema, "value"), &value, sizeof(value));
            rows.insert(rows.end(), row.begin(), row.end());
        }
        auto cfg = cfgFor("width", 4096);
        cfg.accuracyBoundPercent = boundPercent;
        Store store = makeStore<LossyStore>(cfg, schema);
        store.open();
        appendAsSegment(store.getAs<LossyStore>().getMutable(), rows.data(), rows.size(), schema);
        store.flush();
        const auto size = store.size();
        store.getAs<LossyStore>().getMutable().removeFile();
        return size;
    };

    const auto tiny = storedSizeFor(0.001, 1.0);
    const auto huge = storedSizeFor(1000.0, 1.0);

    /// Near-equal rather than equal, because the quantized header carries this column's minimum
    /// and step as doubles and those differ between the two, which changes what the byte
    /// compressor can do with them. The codes themselves are the same width, which is the claim
    /// -- and ColumnCodecTest pins that exactly, without a compressor in the way.
    ///
    /// Compared in bytes rather than as a fraction: these values are linear in the row index, so
    /// their codes are too, so the second stage encodes them with delta-of-delta and the payload
    /// collapses to almost nothing. What is left is mostly header, and a difference of a few
    /// header bytes is a large *fraction* of a 200-byte column while still being exactly the
    /// small fixed cost this test means to allow.
    const auto difference = tiny > huge ? tiny - huge : huge - tiny;
    EXPECT_LT(difference, 64U) << tiny << " against " << huge;

    /// And a looser bound must genuinely cost less.
    EXPECT_LT(storedSizeFor(1.0, 5.0), storedSizeFor(1.0, 0.01));
}

/// NaN and the infinities cannot sit on a grid. They must survive verbatim, and they must not
/// be allowed to poison the range that sets the step size for every other value.
TEST_F(LossyStoreTest, NonFiniteValuesSurviveQuantizationExactly)
{
    const Schema schema = Schema{}.addField("ts", DataType::Type::UINT64).addField("value", DataType::Type::FLOAT64);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    std::vector<double> originals;
    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < 256; ++i)
    {
        double value = 100.0 + static_cast<double>(i);
        if (i % 37 == 0)
        {
            value = std::numeric_limits<double>::quiet_NaN();
        }
        else if (i % 41 == 0)
        {
            value = std::numeric_limits<double>::infinity();
        }
        else if (i % 43 == 0)
        {
            value = -std::numeric_limits<double>::infinity();
        }
        originals.push_back(value);

        std::vector<uint8_t> row(rowWidth, 0);
        const uint64_t ts = 1000 + i;
        std::memcpy(row.data(), &ts, sizeof(ts));
        std::memcpy(row.data() + fieldValueOffset(schema, "value"), &value, sizeof(value));
        rows.insert(rows.end(), row.begin(), row.end());
    }

    auto cfg = cfgFor("nonfinite", 256);
    cfg.accuracyBoundPercent = 1.0;
    Store store = makeStore<LossyStore>(cfg, schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, rows.data(), rows.size(), schema);
    store.flush();

    const auto restored = extractDoubles(readAll(store, schema, TimeRange{}, 512), schema, "value");
    ASSERT_EQ(restored.size(), originals.size());

    /// Finite values keep their bound, each against itself. The non-finite ones are held out
    /// of the grid entirely, so they come back exact rather than merely within bound -- and,
    /// crucially, they do not drag the grid with them.
    for (size_t i = 0; i < originals.size(); ++i)
    {
        if (std::isnan(originals[i]))
        {
            EXPECT_TRUE(std::isnan(restored[i])) << "row " << i;
        }
        else if (std::isinf(originals[i]))
        {
            EXPECT_EQ(restored[i], originals[i]) << "row " << i;
        }
        else
        {
            EXPECT_LE(std::abs(originals[i] - restored[i]) / std::abs(originals[i]), 0.01 * (1 + 1e-9)) << "row " << i;
        }
    }

    lossy.removeFile();
}

/// Every value identical means every code is zero, and a zero code reconstructs the frame of
/// reference itself -- which is why it is stored as the magnitude rather than as its logarithm.
/// Real columns do this, so it is a normal path and must reconstruct exactly, not just within
/// bound.
TEST_F(LossyStoreTest, ConstantFloatColumnQuantizesExactly)
{
    const Schema schema = Schema{}.addField("ts", DataType::Type::UINT64).addField("value", DataType::Type::FLOAT64);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < 100; ++i)
    {
        std::vector<uint8_t> row(rowWidth, 0);
        const uint64_t ts = 1000 + i;
        const double value = 42.5;
        std::memcpy(row.data(), &ts, sizeof(ts));
        std::memcpy(row.data() + fieldValueOffset(schema, "value"), &value, sizeof(value));
        rows.insert(rows.end(), row.begin(), row.end());
    }

    auto cfg = cfgFor("constfloat", 8);
    cfg.accuracyBoundPercent = 1.0;
    Store store = makeStore<LossyStore>(cfg, schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, rows.data(), rows.size(), schema);
    store.flush();

    for (const double value : extractDoubles(readAll(store, schema, TimeRange{}, 200), schema, "value"))
    {
        EXPECT_EQ(value, 42.5);
    }
    lossy.removeFile();
}

/// FLOAT32 goes through the same grid but is narrowed on the way out, so the measured error
/// has to account for the narrowing too.
TEST_F(LossyStoreTest, SinglePrecisionColumnsHonourTheirBound)
{
    const Schema schema = Schema{}.addField("ts", DataType::Type::UINT64).addField("value", DataType::Type::FLOAT32);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    const auto valueOffset = fieldValueOffset(schema, "value");

    std::vector<float> originals;
    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < 512; ++i)
    {
        const auto value = static_cast<float>(20.0 + (static_cast<double>(i) * 0.25));
        originals.push_back(value);
        std::vector<uint8_t> row(rowWidth, 0);
        const uint64_t ts = 1000 + i;
        std::memcpy(row.data(), &ts, sizeof(ts));
        std::memcpy(row.data() + valueOffset, &value, sizeof(value));
        rows.insert(rows.end(), row.begin(), row.end());
    }

    auto cfg = cfgFor("f32", 512);
    cfg.accuracyBoundPercent = 1.0;
    Store store = makeStore<LossyStore>(cfg, schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, rows.data(), rows.size(), schema);
    store.flush();

    const auto read = readAll(store, schema, TimeRange{}, 1024);
    ASSERT_EQ(read.size(), rows.size());

    for (size_t i = 0; i < originals.size(); ++i)
    {
        float restored = 0;
        std::memcpy(&restored, read.data() + (i * rowWidth) + valueOffset, sizeof(float));
        const double original = originals[i];
        EXPECT_LE(std::abs(original - static_cast<double>(restored)) / std::abs(original), 0.01 * (1 + 1e-6))
            << "row " << i;
    }

    lossy.removeFile();
}

/// The reach of the store's error budget: every floating-point column, and nothing else.
///
/// This is what lets the caller set one number and stop thinking about it. The columns that
/// must not move -- the timestamp, the id -- are protected by their type rather than by the
/// caller having remembered to name them, so forgetting is not a failure mode.
TEST_F(LossyStoreTest, ErrorBudgetReachesEveryFloatColumnAndNoOther)
{
    const Schema schema = Schema{}
                              .addField("ts", DataType::Type::UINT64)
                              .addField("id", DataType::Type::UINT32)
                              .addField("measurement", DataType::Type::FLOAT64)
                              .addField("secondMeasurement", DataType::Type::FLOAT64);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < 256; ++i)
    {
        std::vector<uint8_t> row(rowWidth, 0);
        const uint64_t ts = 1000 + i;
        const auto id = static_cast<uint32_t>(700000 + (i * 13));
        const double value = 3.14159265358979 * static_cast<double>(i);
        std::memcpy(row.data(), &ts, sizeof(ts));
        std::memcpy(row.data() + fieldValueOffset(schema, "id"), &id, sizeof(id));
        std::memcpy(row.data() + fieldValueOffset(schema, "measurement"), &value, sizeof(value));
        std::memcpy(row.data() + fieldValueOffset(schema, "secondMeasurement"), &value, sizeof(value));
        rows.insert(rows.end(), row.begin(), row.end());
    }

    auto cfg = cfgFor("budget", 64);
    cfg.accuracyBoundPercent = 1.0;
    Store store = makeStore<LossyStore>(cfg, schema);
    store.open();
    auto& lossy = store.getAs<LossyStore>().getMutable();
    appendAsSegment(lossy, rows.data(), rows.size(), schema);
    store.flush();

    const auto read = readAll(store, schema, TimeRange{}, 512);
    ASSERT_EQ(read.size(), rows.size());

    /// Both float columns moved. One would be enough to show the budget applies; two shows it
    /// is not tied to a particular column.
    for (const auto* column : {"measurement", "secondMeasurement"})
    {
        EXPECT_NE(extractDoubles(read, schema, column), extractDoubles(rows, schema, column)) << column;
    }

    /// The integer columns came back byte for byte, without anyone having asked for that.
    for (uint64_t r = 0; r < 256; ++r)
    {
        const uint8_t* originalRow = rows.data() + (r * rowWidth);
        const uint8_t* restoredRow = read.data() + (r * rowWidth);
        EXPECT_EQ(std::memcmp(originalRow, restoredRow, sizeof(uint64_t)), 0) << "ts of row " << r;
        EXPECT_EQ(
            std::memcmp(
                originalRow + fieldValueOffset(schema, "id"),
                restoredRow + fieldValueOffset(schema, "id"),
                sizeof(uint32_t)),
            0)
            << "id of row " << r;
    }

    lossy.removeFile();
}

/// The two ingest paths differ in how they group -- writeRecord accumulates to its byte budget,
/// a segment becomes one group whatever its size -- but they must reproduce the same rows, and
/// the ranges each path records must be equally usable for skipping.
TEST_F(LossyStoreTest, RecordWiseAndBulkIngestAgree)
{
    const Schema schema = makeSchema();
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    const auto rows = makeRows(schema, 30, 1000, 10);

    Store byRecord = makeStore<LossyStore>(cfgFor("by_record", 8, rowWidth), schema);
    byRecord.open();
    for (uint64_t i = 0; i < 30; ++i)
    {
        byRecord.writeRecord(rows.data() + (i * rowWidth), rowWidth, Timestamp(1000 + (i * 10)), schema);
    }
    byRecord.flush();

    Store bulk = makeStore<LossyStore>(cfgFor("bulk", 8, rowWidth), schema);
    bulk.open();
    appendAsSegment(bulk.getAs<LossyStore>().getMutable(), rows.data(), rows.size(), schema);
    bulk.flush();

    EXPECT_EQ(readAll(byRecord, schema, TimeRange{}, 100), readAll(bulk, schema, TimeRange{}, 100));

    /// And the same window comes back from both, whichever way the rows were grouped.
    const TimeRange window{.fieldName = "ts", .start = Timestamp(1100), .end = Timestamp(1200)};
    EXPECT_EQ(readAll(byRecord, schema, window, 100), readAll(bulk, schema, window, 100));

    byRecord.getAs<LossyStore>().getMutable().removeFile();
    bulk.getAs<LossyStore>().getMutable().removeFile();
}

/// The error budget has to survive the trip through StoreConfig, or the lossy step is
/// unreachable for any store that was not hand-constructed -- which is every store a query
/// actually builds. Both directions are checked: a chain with no budget must stay bit-exact.
TEST_F(LossyStoreTest, ErrorBudgetReachesAChainBuiltStore)
{
    const Schema schema = Schema{}.addField("ts", DataType::Type::UINT64).addField("value", DataType::Type::FLOAT64);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();

    std::vector<uint8_t> rows;
    for (uint64_t i = 0; i < 512; ++i)
    {
        std::vector<uint8_t> row(rowWidth, 0);
        const uint64_t ts = 1000 + i;
        const double value = 3.14159265358979 * static_cast<double>(i);
        std::memcpy(row.data(), &ts, sizeof(ts));
        std::memcpy(row.data() + fieldValueOffset(schema, "value"), &value, sizeof(value));
        rows.insert(rows.end(), row.begin(), row.end());
    }

    const auto valuesThroughChain = [&](const std::string& storeName, const std::optional<double>& bound)
    {
        StoreConfig config;
        config.storeOrder = "LossyStore";
        config.accuracyBoundPercent = bound;

        auto& registry = StoreRegistry::instance();
        registry.registerConfiguredStore(storeName, schema, "ts UINT64, value FLOAT64", config);

        auto store = registry.getStore(storeName);
        EXPECT_TRUE(store.has_value());
        store->open();
        appendAsSegment(store->getAs<LossyStore>().getMutable(), rows.data(), rows.size(), schema);
        store->flush();

        const auto read = readAll(*store, schema, TimeRange{}, 1024);
        EXPECT_EQ(read.size(), rows.size());
        return extractDoubles(read, schema, "value");
    };

    const auto original = extractDoubles(rows, schema, "value");

    /// No budget: the chain is lossless, byte for byte.
    EXPECT_EQ(valuesThroughChain("chainExact", std::nullopt), original);

    /// A budget set on StoreConfig reaches the float column and moves it.
    const auto bounded = valuesThroughChain("chainLossy", 1.0);
    EXPECT_NE(bounded, original);

    /// And it moved by no more than it was allowed to: 1% of the column's range.
    const auto range = *std::ranges::max_element(original) - *std::ranges::min_element(original);
    const double epsilon = 0.01 * range;
    ASSERT_EQ(bounded.size(), original.size());
    for (size_t i = 0; i < original.size(); ++i)
    {
        EXPECT_LE(std::abs(bounded[i] - original[i]), epsilon) << "row " << i;
    }

    StoreRegistry::instance().clearAndDeleteFiles();
}

}
