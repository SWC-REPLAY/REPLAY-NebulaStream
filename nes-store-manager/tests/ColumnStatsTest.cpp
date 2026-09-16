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

/// Tests for the statistics a column is measured by, and for the codec choice made from them.
///
/// The load-bearing test here is the one checking that an estimate equals the number of bytes
/// the codec really produces. Selection compares estimates against each other, so an estimate
/// that drifts from its codec's layout does not fail loudly -- it quietly picks the wrong
/// codec and the store just gets bigger, which no round-trip test would ever catch.

#include <algorithm>
#include <cstdint>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <DataTypes/DataType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <lossy/ColumnCodec.hpp>
#include <lossy/ColumnStats.hpp>
#include <lossy/Primitives.hpp>

namespace NES::Lossy
{

class ColumnStatsTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("ColumnStatsTest.log", LogLevel::LOG_DEBUG); }
};

namespace
{

std::vector<uint8_t> pack(const std::vector<uint64_t>& values, const uint32_t valueWidth)
{
    std::vector<uint8_t> bytes(values.size() * valueWidth, 0);
    for (size_t i = 0; i < values.size(); ++i)
    {
        storeLow(bytes.data() + (i * valueWidth), values[i], valueWidth);
    }
    return bytes;
}

ColumnStats statsFor(const std::vector<uint64_t>& values, const DataType::Type type, const uint32_t valueWidth)
{
    const auto bytes = pack(values, valueWidth);
    return computeStats(
        ColumnValues{.type = type, .data = bytes.data(), .rowCount = values.size(), .valueWidth = valueWidth});
}

size_t actualEncodedBytes(
    const CodecId codecId, const std::vector<uint64_t>& values, const DataType::Type type, const uint32_t valueWidth)
{
    const auto bytes = pack(values, valueWidth);
    return getCodec(codecId)
        .encode(
            ColumnValues{.type = type, .data = bytes.data(), .rowCount = values.size(), .valueWidth = valueWidth}, 0.0)
        .bytes.size();
}

/// The whole selection path for a column nothing is permitted to lose: filter, then rank, then
/// take the winner.
CodecId chosenFor(const std::vector<uint64_t>& values, const DataType::Type type, const uint32_t valueWidth)
{
    const CompressionPolicy lossless{};
    return rank(filterCandidates(type, lossless), statsFor(values, type, valueWidth), lossless).codecs[0];
}

bool offers(const std::vector<CodecId>& candidates, const CodecId id)
{
    return std::ranges::find(candidates, id) != candidates.end();
}

}

TEST_F(ColumnStatsTest, CountsDistinctValuesRunsAndSpans)
{
    const std::vector<uint64_t> values{5, 5, 5, 9, 9, 2};
    const auto stats = statsFor(values, DataType::Type::UINT64, 8);

    EXPECT_EQ(stats.rowCount, 6U);
    EXPECT_EQ(stats.distinctCount, 3U);
    EXPECT_EQ(stats.runCount, 3U);
    EXPECT_EQ(stats.longestRun, 3U);
    EXPECT_EQ(stats.valueSpan, 7U);
    EXPECT_EQ(stats.patternSpan, 7U);
}

/// A fixed sampling rate drives every second difference to zero. This is what lets a timestamp
/// be recognised from the data instead of from a threshold on how distinct its values are.
TEST_F(ColumnStatsTest, SteadyRateDrivesSecondDifferencesToZero)
{
    std::vector<uint64_t> steady(1000);
    for (size_t i = 0; i < steady.size(); ++i)
    {
        steady[i] = 1700000000000ULL + (i * 100);
    }
    EXPECT_EQ(statsFor(steady, DataType::Type::UINT64, 8).maxSecondDifference, 0U);

    /// One hiccup is enough to make it non-zero, and it stays small rather than exploding.
    steady[500] += 7;
    EXPECT_GT(statsFor(steady, DataType::Type::UINT64, 8).maxSecondDifference, 0U);
}

/// Frame-of-reference subtracts the column's minimum value; a dictionary sorts raw bit
/// patterns. For a signed column those are different ranges, and conflating them would make
/// one of the two estimates wrong.
TEST_F(ColumnStatsTest, SignedColumnsHaveTwoDifferentSpans)
{
    const std::vector<uint64_t> values{
        static_cast<uint64_t>(static_cast<int64_t>(-5)), 0, 7, static_cast<uint64_t>(static_cast<int64_t>(-1))};
    const auto stats = statsFor(values, DataType::Type::INT64, 8);

    /// Values run from -5 to 7.
    EXPECT_EQ(stats.valueSpan, 12U);

    /// Bit patterns run from 0 to 0xFFFF...FB, so the pattern span is enormous.
    EXPECT_GT(stats.patternSpan, stats.valueSpan);
}

/// The invariant selection rests on: what the estimate says is what the codec produces.
TEST_F(ColumnStatsTest, EstimatesEqualWhatTheCodecActuallyProduces)
{
    std::mt19937_64 rng(21);
    std::vector<uint64_t> random(500);
    for (uint64_t& value : random)
    {
        value = rng();
    }

    std::vector<uint64_t> steady(1000);
    for (size_t i = 0; i < steady.size(); ++i)
    {
        steady[i] = 1700000000000ULL + (i * 100);
    }

    std::vector<uint64_t> fewDistinct(1000);
    for (size_t i = 0; i < fewDistinct.size(); ++i)
    {
        fewDistinct[i] = 900000000ULL * ((i * 7) % 5);
    }

    std::vector<uint64_t> longRuns(1000);
    for (size_t i = 0; i < longRuns.size(); ++i)
    {
        longRuns[i] = 40 + (i / 250);
    }

    const std::vector<std::pair<const char*, std::vector<uint64_t>>> shapes{
        {"empty", {}},
        {"single", {42}},
        {"two", {42, 17}},
        {"constant", std::vector<uint64_t>(300, 8)},
        {"random", random},
        {"steady", steady},
        {"fewDistinct", fewDistinct},
        {"longRuns", longRuns}};

    constexpr CodecId LOSSLESS[]{
        CodecId::Raw, CodecId::ForBitpack, CodecId::DeltaOfDelta, CodecId::Dictionary, CodecId::DictRle};

    for (const auto& [name, values] : shapes)
    {
        const auto stats = statsFor(values, DataType::Type::UINT64, 8);
        for (const CodecId codecId : LOSSLESS)
        {
            EXPECT_EQ(estimateEncodedBytes(codecId, stats, CompressionPolicy{}), actualEncodedBytes(codecId, values, DataType::Type::UINT64, 8))
                << codecName(codecId) << " on shape '" << name << "'";
        }
    }
}

TEST_F(ColumnStatsTest, EstimatesHoldForSignedAndNarrowColumns)
{
    std::mt19937_64 rng(33);

    const std::vector<std::pair<DataType::Type, uint32_t>> types{
        {DataType::Type::UINT8, 1}, {DataType::Type::UINT16, 2}, {DataType::Type::INT16, 2}, {DataType::Type::INT32, 4}};

    constexpr CodecId LOSSLESS[]{
        CodecId::Raw, CodecId::ForBitpack, CodecId::DeltaOfDelta, CodecId::Dictionary, CodecId::DictRle};

    for (const auto& [type, valueWidth] : types)
    {
        std::vector<uint64_t> values(400);
        const uint64_t mask = (uint64_t{1} << (valueWidth * 8)) - 1;
        for (uint64_t& value : values)
        {
            value = rng() & mask;
        }

        const auto stats = statsFor(values, type, valueWidth);
        for (const CodecId codecId : LOSSLESS)
        {
            EXPECT_EQ(estimateEncodedBytes(codecId, stats, CompressionPolicy{}), actualEncodedBytes(codecId, values, type, valueWidth))
                << codecName(codecId) << " on DataType::Type " << static_cast<int>(type);
        }
    }
}

/// Filtering settles whether accuracy may be spent, before any size is computed. No measurement
/// can put a column on the lossy path that the policy did not already allow there.
TEST_F(ColumnStatsTest, OnlyFloatColumnsWithABudgetReachTheLossyCodec)
{
    const CompressionPolicy budget{.errorBoundPercent = 1.0};
    const CompressionPolicy none{};

    /// A permitted float column is offered the lossy codec and nothing else. The lossless
    /// codecs are not alternatives to weigh against it -- they answer a different question.
    for (const auto type : {DataType::Type::FLOAT64, DataType::Type::FLOAT32})
    {
        const auto candidates = filterCandidates(type, budget);
        ASSERT_EQ(candidates.size(), 1U) << "DataType::Type " << static_cast<int>(type);
        EXPECT_EQ(candidates.front(), CodecId::Quantized);
    }

    /// The same column with no budget keeps its bits exactly.
    EXPECT_FALSE(offers(filterCandidates(DataType::Type::FLOAT64, none), CodecId::Quantized));

    /// A budget is set, but these types are never offered it. Nothing here consults a role: no
    /// lossy codec claims to support an integer, so a key or a timestamp cannot reach one
    /// however the policy is configured.
    for (const auto type :
         {DataType::Type::UINT8,
          DataType::Type::UINT32,
          DataType::Type::UINT64,
          DataType::Type::INT32,
          DataType::Type::INT64,
          DataType::Type::BOOLEAN,
          DataType::Type::CHAR})
    {
        EXPECT_FALSE(offers(filterCandidates(type, budget), CodecId::Quantized))
            << "DataType::Type " << static_cast<int>(type);
    }
}

/// What used to be declared is now discovered, and the cost model is what discovers it. A
/// timestamp is not told to the store and a boolean gets no special case; both simply come out
/// cheapest under the codec their role would have pinned.
TEST_F(ColumnStatsTest, RolesAreFoundByCostRatherThanDeclared)
{
    std::vector<uint64_t> steady(500);
    for (size_t i = 0; i < steady.size(); ++i)
    {
        steady[i] = 1700000000000ULL + (i * 100);
    }
    EXPECT_EQ(chosenFor(steady, DataType::Type::UINT64, 8), CodecId::DeltaOfDelta);

    std::vector<uint64_t> flags(500);
    for (size_t i = 0; i < flags.size(); ++i)
    {
        flags[i] = i % 2;
    }
    EXPECT_EQ(chosenFor(flags, DataType::Type::BOOLEAN, 1), CodecId::Bool);

    /// And where the pinned codec would have been the wrong answer, the cost model says so. A
    /// boolean column that never changes has a span of zero, so frame-of-reference packs it at
    /// zero bits and pays only its 9-byte header -- against 63 bytes of one-bit-per-row for the
    /// codec named after the type. Pinning Bool by type would have cost 7x here.
    const std::vector<uint64_t> alwaysTrue(500, 1);
    EXPECT_EQ(chosenFor(alwaysTrue, DataType::Type::BOOLEAN, 1), CodecId::ForBitpack);
}

/// The choices the statistics are there to make. Each shape is one the wrong codec handles
/// measurably worse, so these pin behaviour rather than restating the implementation.
TEST_F(ColumnStatsTest, DataShapeDecidesAmongTheLosslessCodecs)
{
    /// A steady stream: second differences are zero, so delta-of-delta costs its header alone.
    std::vector<uint64_t> steady(2000);
    for (size_t i = 0; i < steady.size(); ++i)
    {
        steady[i] = 1700000000000ULL + (i * 100);
    }
    EXPECT_EQ(chosenFor(steady, DataType::Type::UINT64, 8), CodecId::DeltaOfDelta);

    /// Wide values, few of them, in no order: a dictionary pays where frame-of-reference
    /// cannot, because the span stays 64 bits however few distinct values there are.
    std::mt19937_64 rng(44);
    std::vector<uint64_t> ids(200);
    for (uint64_t& id : ids)
    {
        id = rng();
    }
    std::vector<uint64_t> keys(4000);
    for (uint64_t& key : keys)
    {
        key = ids[rng() % ids.size()];
    }
    EXPECT_EQ(chosenFor(keys, DataType::Type::UINT64, 8), CodecId::Dictionary);

    /// The same few values, but arriving in long runs: now the run lengths are cheaper than
    /// the codes they replace.
    std::vector<uint64_t> statuses(4000);
    for (size_t i = 0; i < statuses.size(); ++i)
    {
        statuses[i] = ids[(i / 400) % ids.size()];
    }
    EXPECT_EQ(chosenFor(statuses, DataType::Type::UINT64, 8), CodecId::DictRle);

    /// Large values in a narrow window, all distinct: frame-of-reference territory.
    std::vector<uint64_t> narrow(4000);
    for (size_t i = 0; i < narrow.size(); ++i)
    {
        narrow[i] = 1700000000000ULL + ((i * 7919) % 1000);
    }
    EXPECT_EQ(chosenFor(narrow, DataType::Type::UINT64, 8), CodecId::ForBitpack);

    /// Structureless 64-bit noise: nothing can beat storing it verbatim, and the store should
    /// not pay a header to discover that.
    std::vector<uint64_t> noise(4000);
    for (uint64_t& value : noise)
    {
        value = rng();
    }
    EXPECT_EQ(chosenFor(noise, DataType::Type::UINT64, 8), CodecId::Raw);
}

}
