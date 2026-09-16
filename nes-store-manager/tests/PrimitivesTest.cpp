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

/// Tests for the lossless building blocks, in isolation from any store.
///
/// Bit-packing gets the most attention because it is the piece most able to fail quietly:
/// an off-by-one in the accumulator corrupts values without corrupting sizes, so a store
/// round trip would notice only for some widths and some counts.

#include <cstdint>
#include <limits>
#include <random>
#include <span>
#include <vector>

#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <lossy/Primitives.hpp>

namespace NES::Lossy
{

class PrimitivesTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("PrimitivesTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(PrimitivesTest, BitWidthForBoundaries)
{
    EXPECT_EQ(bitWidthFor(0), 0);
    EXPECT_EQ(bitWidthFor(1), 1);
    EXPECT_EQ(bitWidthFor(2), 2);
    EXPECT_EQ(bitWidthFor(3), 2);
    EXPECT_EQ(bitWidthFor(255), 8);
    EXPECT_EQ(bitWidthFor(256), 9);
    EXPECT_EQ(bitWidthFor(std::numeric_limits<uint64_t>::max()), 64);
}

/// Every width against every awkward count. The counts straddle byte boundaries in both
/// directions, which is where a packing bug lives if it exists at all.
TEST_F(PrimitivesTest, RoundTripsAtEveryWidthAndAwkwardCounts)
{
    std::mt19937_64 rng(1234);

    for (uint8_t bitWidth = 0; bitWidth <= 64; ++bitWidth)
    {
        for (const size_t count : {size_t{0}, 1UZ, 2UZ, 7UZ, 8UZ, 9UZ, 63UZ, 64UZ, 65UZ, 1000UZ})
        {
            const uint64_t mask = bitWidth == 0 ? 0 : (bitWidth == 64 ? ~uint64_t{0} : (uint64_t{1} << bitWidth) - 1);

            std::vector<uint64_t> values(count);
            for (auto& value : values)
            {
                value = rng() & mask;
            }

            std::vector<uint8_t> packed;
            bitPack(values, bitWidth, packed);
            EXPECT_EQ(packed.size(), bitPackedSize(count, bitWidth)) << "width " << int(bitWidth) << " count " << count;

            std::vector<uint64_t> restored(count);
            bitUnpack(packed, bitWidth, count, restored);
            EXPECT_EQ(restored, values) << "width " << int(bitWidth) << " count " << count;
        }
    }
}

/// All-ones at each width: catches a mask that is one bit too narrow, which random values
/// would only sometimes reveal.
TEST_F(PrimitivesTest, RoundTripsAtMaximumValuePerWidth)
{
    for (uint8_t bitWidth = 1; bitWidth <= 64; ++bitWidth)
    {
        const uint64_t maxValue = bitWidth == 64 ? ~uint64_t{0} : (uint64_t{1} << bitWidth) - 1;
        const std::vector<uint64_t> values(100, maxValue);

        std::vector<uint8_t> packed;
        bitPack(values, bitWidth, packed);

        std::vector<uint64_t> restored(values.size());
        bitUnpack(packed, bitWidth, values.size(), restored);
        EXPECT_EQ(restored, values) << "width " << int(bitWidth);
    }
}

/// Width zero means every value is zero, so nothing at all is written.
TEST_F(PrimitivesTest, ZeroWidthStoresNoBytes)
{
    const std::vector<uint64_t> values(500, 0);
    std::vector<uint8_t> packed;
    bitPack(values, 0, packed);
    EXPECT_TRUE(packed.empty());

    std::vector<uint64_t> restored(values.size(), 0xDEAD);
    bitUnpack(packed, 0, values.size(), restored);
    EXPECT_EQ(restored, values);
}

TEST_F(PrimitivesTest, ZigZagMapsSmallMagnitudesToSmallValues)
{
    EXPECT_EQ(zigZagEncode(0), 0U);
    EXPECT_EQ(zigZagEncode(-1), 1U);
    EXPECT_EQ(zigZagEncode(1), 2U);
    EXPECT_EQ(zigZagEncode(-2), 3U);
    EXPECT_EQ(zigZagEncode(2), 4U);

    for (const int64_t n :
         {std::numeric_limits<int64_t>::min(), int64_t{-1000}, int64_t{-1}, int64_t{0}, int64_t{1}, std::numeric_limits<int64_t>::max()})
    {
        EXPECT_EQ(zigZagDecode(zigZagEncode(n)), n) << n;
    }
}

TEST_F(PrimitivesTest, LoadSignExtendsAndStoreTruncates)
{
    std::vector<uint8_t> buffer(8, 0);

    storeLow(buffer.data(), static_cast<uint64_t>(int64_t{-1}), 2);
    EXPECT_EQ(loadSigned(buffer.data(), 2), -1);
    EXPECT_EQ(loadUnsigned(buffer.data(), 2), 0xFFFFU);

    storeLow(buffer.data(), static_cast<uint64_t>(int64_t{-128}), 1);
    EXPECT_EQ(loadSigned(buffer.data(), 1), -128);

    storeLow(buffer.data(), static_cast<uint64_t>(int64_t{127}), 1);
    EXPECT_EQ(loadSigned(buffer.data(), 1), 127);

    storeLow(buffer.data(), 0x0123456789ABCDEFULL, 8);
    EXPECT_EQ(loadUnsigned(buffer.data(), 8), 0x0123456789ABCDEFULL);
}

namespace
{

std::vector<uint64_t> dodRoundTrip(const std::vector<uint64_t>& values)
{
    const auto encoded = dodEncode(values);
    std::vector<uint64_t> restored(values.size(), 0xDEAD);
    dodDecode(encoded, values.size(), restored);
    return restored;
}

}

/// The case delta-of-delta exists for: a fixed sampling rate makes every second difference
/// zero, so the sequence costs its two header values and no bits at all.
TEST_F(PrimitivesTest, RegularSequencesCostNoBits)
{
    std::vector<uint64_t> values(4096);
    for (size_t i = 0; i < values.size(); ++i)
    {
        values[i] = 1700000000000ULL + (i * 100);
    }

    const auto encoded = dodEncode(values);
    EXPECT_EQ(encoded.bitWidth, 0);
    EXPECT_EQ(encoded.first, 1700000000000ULL);
    EXPECT_EQ(encoded.firstDelta, 100);
    EXPECT_EQ(dodRoundTrip(values), values);
}

/// A rate that changes, gaps, and going backwards -- none of which the codec is optimised for,
/// all of which it still has to reproduce exactly.
TEST_F(PrimitivesTest, IrregularSequencesRoundTrip)
{
    EXPECT_EQ(dodRoundTrip({}), std::vector<uint64_t>{});
    EXPECT_EQ(dodRoundTrip({42}), std::vector<uint64_t>{42});
    EXPECT_EQ(dodRoundTrip({42, 17}), (std::vector<uint64_t>{42, 17}));
    EXPECT_EQ(dodRoundTrip({5, 5, 5}), (std::vector<uint64_t>{5, 5, 5}));

    /// A stream that stalls, resumes at a different rate, then jumps backwards.
    const std::vector<uint64_t> uneven{1000, 1100, 1200, 1200, 1200, 1500, 1501, 900, 40000, 40001};
    EXPECT_EQ(dodRoundTrip(uneven), uneven);

    std::mt19937_64 rng(99);
    std::vector<uint64_t> random(1000);
    for (uint64_t& value : random)
    {
        value = rng();
    }
    EXPECT_EQ(dodRoundTrip(random), random);
}

/// Neighbouring values at opposite ends of the range make the differences overflow. The
/// encoding is modular, so that has to round trip like anything else rather than saturate.
TEST_F(PrimitivesTest, DifferencesThatOverflowStillRoundTrip)
{
    const std::vector<uint64_t> extremes{
        0,
        std::numeric_limits<uint64_t>::max(),
        0,
        std::numeric_limits<uint64_t>::max(),
        std::numeric_limits<uint64_t>::max() / 2,
        1};
    EXPECT_EQ(dodRoundTrip(extremes), extremes);
}

TEST_F(PrimitivesTest, DictionaryIsSortedDeduplicatedAndSizedByDistinctCount)
{
    const std::vector<uint64_t> values{700, 100, 700, 400, 100, 100, 400};
    const auto encoded = dictEncode(values);

    EXPECT_EQ(encoded.dictionary, (std::vector<uint64_t>{100, 400, 700}));
    EXPECT_EQ(encoded.codes, (std::vector<uint64_t>{2, 0, 2, 1, 0, 0, 1}));

    /// Three entries need two bits, whatever the entries themselves are worth.
    EXPECT_EQ(encoded.bitWidth, 2);

    std::vector<uint64_t> restored(values.size(), 0xDEAD);
    dictDecode(encoded, restored);
    EXPECT_EQ(restored, values);
}

/// One distinct value means every code is the same, so the codes cost nothing -- the table
/// alone is the column.
TEST_F(PrimitivesTest, SingleEntryDictionaryNeedsNoCodeBits)
{
    const std::vector<uint64_t> values(500, 0xABCDEF0123456789ULL);
    const auto encoded = dictEncode(values);

    EXPECT_EQ(encoded.dictionary.size(), 1U);
    EXPECT_EQ(encoded.bitWidth, 0);

    std::vector<uint64_t> restored(values.size(), 0xDEAD);
    dictDecode(encoded, restored);
    EXPECT_EQ(restored, values);
}

TEST_F(PrimitivesTest, RunLengthCollapsesRunsAndSurvivesAlternation)
{
    const std::vector<uint64_t> runs{7, 7, 7, 7, 3, 3, 9};
    const auto encoded = rleEncode(runs);
    EXPECT_EQ(encoded.values, (std::vector<uint64_t>{7, 3, 9}));
    EXPECT_EQ(encoded.runLengths, (std::vector<uint64_t>{4, 2, 1}));

    std::vector<uint64_t> restored(runs.size(), 0xDEAD);
    rleDecode(encoded, runs.size(), restored);
    EXPECT_EQ(restored, runs);

    /// The adversarial case, and the reason this is never applied unconditionally: alternating
    /// values produce as many runs as there are values, so nothing is collapsed.
    std::vector<uint64_t> alternating(1000);
    for (size_t i = 0; i < alternating.size(); ++i)
    {
        alternating[i] = i % 2;
    }
    const auto worstCase = rleEncode(alternating);
    EXPECT_EQ(worstCase.values.size(), alternating.size());

    std::vector<uint64_t> restoredWorstCase(alternating.size(), 0xDEAD);
    rleDecode(worstCase, alternating.size(), restoredWorstCase);
    EXPECT_EQ(restoredWorstCase, alternating);
}

TEST_F(PrimitivesTest, RunLengthHandlesEmptyAndSingleRunInput)
{
    const auto empty = rleEncode({});
    EXPECT_TRUE(empty.values.empty());

    std::vector<uint64_t> nothing;
    rleDecode(empty, 0, nothing);

    const std::vector<uint64_t> uniform(8192, 4);
    const auto encoded = rleEncode(uniform);
    EXPECT_EQ(encoded.values, std::vector<uint64_t>{4});
    EXPECT_EQ(encoded.runLengths, std::vector<uint64_t>{8192});

    std::vector<uint64_t> restored(uniform.size(), 0xDEAD);
    rleDecode(encoded, uniform.size(), restored);
    EXPECT_EQ(restored, uniform);
}

}
