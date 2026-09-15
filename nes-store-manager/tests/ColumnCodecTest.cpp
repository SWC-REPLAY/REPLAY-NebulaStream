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

/// Tests for the codecs at the ColumnCodec interface, without a store underneath.
///
/// Every codec here is reachable through selection, which filters by what each one supports and
/// then ranks the survivors by estimated size. Testing them at this level rather than through a
/// store round trip is what keeps them honest: a codec whose output drifts fails here, whereas
/// through a store it would only show up as a file that quietly got bigger.

#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <span>
#include <vector>

#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <DataTypes/DataType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <lossy/ColumnCodec.hpp>
#include <lossy/Primitives.hpp>

namespace NES::StoreManager::Lossy
{

class ColumnCodecTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("ColumnCodecTest.log", LogLevel::LOG_DEBUG); }
};

namespace
{

/// The values are given as u64 and truncated to `valueWidth`, matching what the store hands a
/// codec after columnarization.
std::vector<uint8_t> pack(const std::vector<uint64_t>& values, const uint32_t valueWidth)
{
    std::vector<uint8_t> bytes(values.size() * valueWidth, 0);
    for (size_t i = 0; i < values.size(); ++i)
    {
        storeLow(bytes.data() + (i * valueWidth), values[i], valueWidth);
    }
    return bytes;
}

std::vector<uint64_t> unpack(const std::vector<uint8_t>& bytes, const uint32_t valueWidth)
{
    std::vector<uint64_t> values(bytes.size() / valueWidth);
    for (size_t i = 0; i < values.size(); ++i)
    {
        values[i] = loadUnsigned(bytes.data() + (i * valueWidth), valueWidth);
    }
    return values;
}

struct RoundTrip
{
    std::vector<uint64_t> values;
    size_t encodedBytes{0};
};

RoundTrip roundTrip(
    const CodecId codecId, const DataType::Type type, const uint32_t valueWidth, const std::vector<uint64_t>& values)
{
    const auto& codec = getCodec(codecId);
    const auto input = pack(values, valueWidth);

    const ColumnValues in{
        .type = type, .data = input.data(), .rowCount = values.size(), .valueWidth = valueWidth};
    const auto encoded = codec.encode(in, 0.0);

    std::vector<uint8_t> output(input.size(), 0xAB);
    const MutableColumnValues out{
        .type = type, .data = output.data(), .rowCount = values.size(), .valueWidth = valueWidth};
    codec.decode(encoded.bytes, out);

    return RoundTrip{.values = unpack(output, valueWidth), .encodedBytes = encoded.bytes.size()};
}

/// Every lossless codec that accepts integers. Raw is included deliberately: it is the
/// identity the others are compared against, so a shape that breaks it breaks the premise.
constexpr CodecId LOSSLESS_INTEGER_CODECS[]{
    CodecId::Raw, CodecId::ForBitpack, CodecId::DeltaOfDelta, CodecId::Dictionary, CodecId::DictRle};

}

/// Same claim for all of them, over the shapes that tend to break one codec but not another:
/// empty, single row, all-identical, alternating, monotonic, and the extremes of the range.
TEST_F(ColumnCodecTest, LosslessCodecsReproduceTheirInput)
{
    std::mt19937_64 rng(7);
    std::vector<uint64_t> random(777);
    for (uint64_t& value : random)
    {
        value = rng();
    }

    const std::vector<std::vector<uint64_t>> shapes{
        {},
        {42},
        {42, 42},
        std::vector<uint64_t>(1000, 9),
        {0, 1, 0, 1, 0, 1, 0},
        {1700000000000ULL, 1700000000100ULL, 1700000000200ULL, 1700000000300ULL},
        {0, std::numeric_limits<uint64_t>::max(), 0, std::numeric_limits<uint64_t>::max()},
        random};

    for (const CodecId codecId : LOSSLESS_INTEGER_CODECS)
    {
        for (const auto& shape : shapes)
        {
            const auto result = roundTrip(codecId, DataType::Type::UINT64, 8, shape);
            EXPECT_EQ(result.values, shape) << codecName(codecId) << " on a " << shape.size() << "-row column";
        }
    }
}

/// Narrower columns are handed to a codec zero-extended and truncated on the way back. Every
/// codec differences or indexes those values, so the truncation has to be exact -- including
/// for signed columns, whose negative values arrive as large unsigned patterns.
TEST_F(ColumnCodecTest, NarrowAndSignedColumnsReproduceTheirInput)
{
    const std::vector<std::pair<DataType::Type, uint32_t>> narrowTypes{
        {DataType::Type::UINT8, 1},
        {DataType::Type::UINT16, 2},
        {DataType::Type::UINT32, 4},
        {DataType::Type::INT8, 1},
        {DataType::Type::INT16, 2},
        {DataType::Type::INT32, 4},
        {DataType::Type::INT64, 8}};

    std::mt19937_64 rng(11);

    for (const auto& [type, valueWidth] : narrowTypes)
    {
        /// Random bit patterns truncated to the width, so both signs are covered whatever the
        /// type's interpretation of the top bit is.
        std::vector<uint64_t> values(300);
        const uint64_t mask = valueWidth == 8 ? ~uint64_t{0} : (uint64_t{1} << (valueWidth * 8)) - 1;
        for (uint64_t& value : values)
        {
            value = rng() & mask;
        }

        for (const CodecId codecId : LOSSLESS_INTEGER_CODECS)
        {
            const auto result = roundTrip(codecId, type, valueWidth, values);
            EXPECT_EQ(result.values, values) << codecName(codecId) << " on DataType::Type " << static_cast<int>(type);
        }
    }
}

/// The point of delta-of-delta: a steady stream costs its header and nothing more, where
/// frame-of-reference still pays for every row's growing offset from the group minimum.
TEST_F(ColumnCodecTest, DeltaOfDeltaCostsNothingPerRowOnASteadyStream)
{
    std::vector<uint64_t> timestamps(8192);
    for (size_t i = 0; i < timestamps.size(); ++i)
    {
        timestamps[i] = 1700000000000ULL + (i * 100);
    }

    const auto dod = roundTrip(CodecId::DeltaOfDelta, DataType::Type::UINT64, 8, timestamps);
    const auto forBitpack = roundTrip(CodecId::ForBitpack, DataType::Type::UINT64, 8, timestamps);

    EXPECT_EQ(dod.values, timestamps);
    EXPECT_EQ(dod.encodedBytes, 17U) << "a constant rate should leave only the header";
    EXPECT_LT(dod.encodedBytes, forBitpack.encodedBytes / 100);
}

/// Jitter perturbs individual second differences rather than the baseline, so the column stays
/// cheap. This is the realistic case, not the noise-free one above.
TEST_F(ColumnCodecTest, DeltaOfDeltaStaysCheapUnderJitter)
{
    std::mt19937_64 rng(3);
    std::uniform_int_distribution<int> jitter(-2, 2);

    std::vector<uint64_t> timestamps(8192);
    uint64_t now = 1700000000000ULL;
    for (uint64_t& timestamp : timestamps)
    {
        timestamp = now;
        now += static_cast<uint64_t>(100 + jitter(rng));
    }

    const auto result = roundTrip(CodecId::DeltaOfDelta, DataType::Type::UINT64, 8, timestamps);
    EXPECT_EQ(result.values, timestamps);

    /// Second differences land in [-4, 4], so four bits plus a sign is ample.
    EXPECT_LT(result.encodedBytes, timestamps.size());
}

/// What a dictionary is for, and what frame-of-reference cannot do: the cost is set by how
/// many distinct values there are, not by how far apart they sit.
TEST_F(ColumnCodecTest, DictionaryCostFollowsDistinctCountNotMagnitude)
{
    std::mt19937_64 rng(5);

    /// 200 ids scattered across the whole 64-bit range, so the span is maximal.
    std::vector<uint64_t> distinct(200);
    for (uint64_t& id : distinct)
    {
        id = rng();
    }

    std::vector<uint64_t> values(8192);
    for (uint64_t& value : values)
    {
        value = distinct[rng() % distinct.size()];
    }

    const auto dictionary = roundTrip(CodecId::Dictionary, DataType::Type::UINT64, 8, values);
    const auto forBitpack = roundTrip(CodecId::ForBitpack, DataType::Type::UINT64, 8, values);

    EXPECT_EQ(dictionary.values, values);

    /// 8 bits per row plus a 200-entry table, against 64 bits per row for a span this wide.
    EXPECT_LT(dictionary.encodedBytes, forBitpack.encodedBytes / 4);
}

/// The trade dict-RLE makes, in both directions. Long runs turn a code per row into a code per
/// change; alternating values turn it into a code *and* a length per row, which is worse than
/// the plain dictionary. Selecting between them is what the run count is for.
TEST_F(ColumnCodecTest, DictRleShrinksLongRunsAndGrowsOnAlternation)
{
    std::vector<uint64_t> runs(8192);
    for (size_t i = 0; i < runs.size(); ++i)
    {
        runs[i] = 100 + (i / 512);
    }

    const auto rleOnRuns = roundTrip(CodecId::DictRle, DataType::Type::UINT64, 8, runs);
    const auto dictOnRuns = roundTrip(CodecId::Dictionary, DataType::Type::UINT64, 8, runs);
    EXPECT_EQ(rleOnRuns.values, runs);
    EXPECT_LT(rleOnRuns.encodedBytes, dictOnRuns.encodedBytes / 10);

    std::vector<uint64_t> alternating(8192);
    for (size_t i = 0; i < alternating.size(); ++i)
    {
        alternating[i] = 100 + (i % 2);
    }

    const auto rleOnAlternating = roundTrip(CodecId::DictRle, DataType::Type::UINT64, 8, alternating);
    const auto dictOnAlternating = roundTrip(CodecId::Dictionary, DataType::Type::UINT64, 8, alternating);
    EXPECT_EQ(rleOnAlternating.values, alternating);
    EXPECT_GT(rleOnAlternating.encodedBytes, dictOnAlternating.encodedBytes)
        << "dict-RLE is expected to lose here; that is why it is not applied unconditionally";
}

TEST_F(ColumnCodecTest, BoolSpendsOneBitPerValue)
{
    std::mt19937_64 rng(13);
    std::vector<uint64_t> flags(8192);
    for (uint64_t& flag : flags)
    {
        flag = rng() % 2;
    }

    const auto result = roundTrip(CodecId::Bool, DataType::Type::BOOLEAN, 1, flags);
    EXPECT_EQ(result.values, flags);
    EXPECT_EQ(result.encodedBytes, flags.size() / 8);
}

/// The closed form behind the quantized codec: maxCode = range / delta = 50 / X, in which the
/// column's range cancels out. So the same bound costs the same bits whatever the data spans.
///
namespace
{
size_t quantizedBytesFor(const std::vector<double>& values, const double boundPercent)
{
    std::vector<uint8_t> input(values.size() * sizeof(double));
    std::memcpy(input.data(), values.data(), input.size());

    return getCodec(CodecId::Quantized)
        .encode(
            ColumnValues{
                .type = DataType::Type::FLOAT64,
                .data = input.data(),
                .rowCount = values.size(),
                .valueWidth = sizeof(double)},
            boundPercent)
        .bytes.size();
}

std::vector<double> rampScaledBy(const double scale)
{
    std::vector<double> values(4096);
    for (size_t i = 0; i < values.size(); ++i)
    {
        values[i] = static_cast<double>(i) * scale;
    }
    return values;
}
}

/// The defining property of a relative bound: multiplying every value by a constant changes
/// nothing. The grid is uniform in the logarithm, so scaling shifts its origin and leaves the
/// spacing and the code count untouched.
///
/// Checked here rather than through a store because the store runs a byte compressor over the
/// result, and that does depend on the actual values -- the two columns carry different origin
/// doubles in their headers. This is the codec's own claim, unobscured.
TEST_F(ColumnCodecTest, QuantizedCostIsScaleInvariant)
{
    /// Same values three orders of magnitude apart, same bound, byte for byte the same cost.
    EXPECT_EQ(quantizedBytesFor(rampScaledBy(0.001), 1.0), quantizedBytesFor(rampScaledBy(1000.0), 1.0));

    /// A looser bound is genuinely cheaper.
    EXPECT_LT(quantizedBytesFor(rampScaledBy(1.0), 5.0), quantizedBytesFor(rampScaledBy(1.0), 0.01));

    /// What the width does follow is the column's dynamic range -- how many decades separate
    /// its smallest and largest magnitudes, not how far apart they are. A narrow column is
    /// cheaper at the same bound, which is where this scheme beats a range-relative grid.
    std::vector<double> narrow(4096);
    for (size_t i = 0; i < narrow.size(); ++i)
    {
        narrow[i] = 20.0 + (static_cast<double>(i) * 10.0 / 4096.0); /// 20 .. 30
    }
    EXPECT_LT(quantizedBytesFor(narrow, 1.0), quantizedBytesFor(rampScaledBy(1.0), 1.0));
}

/// The bound is on each value against itself, so it must hold for values that share a column
/// with others many orders of magnitude away -- the case a range-relative bound silently fails.
TEST_F(ColumnCodecTest, QuantizedHonoursThePointwiseBoundAcrossDecades)
{
    std::vector<double> values;
    for (int exponent = -6; exponent <= 6; ++exponent)
    {
        for (const double mantissa : {1.0, 2.5, 5.0, 7.5})
        {
            values.push_back(mantissa * std::pow(10.0, exponent));
            values.push_back(-mantissa * std::pow(10.0, exponent));
        }
    }
    values.push_back(0.0);

    std::vector<uint8_t> input(values.size() * sizeof(double));
    std::memcpy(input.data(), values.data(), input.size());

    const ColumnValues in{
        .type = DataType::Type::FLOAT64,
        .data = input.data(),
        .rowCount = values.size(),
        .valueWidth = sizeof(double)};

    constexpr double BOUND_PERCENT = 1.0;
    const auto encoded = getCodec(CodecId::Quantized).encode(in, BOUND_PERCENT);

    std::vector<uint8_t> output(values.size() * sizeof(double), 0);
    getCodec(CodecId::Quantized)
        .decode(
            encoded.bytes,
            MutableColumnValues{
                .type = DataType::Type::FLOAT64,
                .data = output.data(),
                .rowCount = values.size(),
                .valueWidth = sizeof(double)});

    std::vector<double> restored(values.size());
    std::memcpy(restored.data(), output.data(), output.size());

    for (size_t i = 0; i < values.size(); ++i)
    {
        if (values[i] == 0.0)
        {
            EXPECT_EQ(restored[i], 0.0) << "zero is held out of the grid and kept exactly";
            continue;
        }
        /// Sign survives exactly; it is carried beside the grid rather than on it.
        EXPECT_EQ(std::signbit(restored[i]), std::signbit(values[i])) << "row " << i;
        EXPECT_LE(std::abs(values[i] - restored[i]) / std::abs(values[i]), (BOUND_PERCENT / 100.0) * (1 + 1e-9))
            << "row " << i << " value " << values[i];
    }

    /// And the encoder's own measurement agrees with what a reader can check.
    EXPECT_LE(encoded.achievedError, BOUND_PERCENT / 100.0);
}

TEST_F(ColumnCodecTest, CodecsRejectTypesTheyCannotEncode)
{
    EXPECT_TRUE(getCodec(CodecId::Raw).supports(DataType::Type::FLOAT64));
    EXPECT_FALSE(getCodec(CodecId::Raw).supports(DataType::Type::VARSIZED));

    EXPECT_FALSE(getCodec(CodecId::DeltaOfDelta).supports(DataType::Type::FLOAT64));
    EXPECT_FALSE(getCodec(CodecId::Dictionary).supports(DataType::Type::FLOAT64));
    EXPECT_FALSE(getCodec(CodecId::DictRle).supports(DataType::Type::FLOAT64));

    EXPECT_TRUE(getCodec(CodecId::Bool).supports(DataType::Type::BOOLEAN));
    EXPECT_FALSE(getCodec(CodecId::Bool).supports(DataType::Type::UINT64));
}

}
