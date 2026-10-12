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

/// Tests for the shipped lossless building blocks, in isolation from any store: bit-packing,
/// zig-zag, and the little-endian load/store helpers.
///
/// Bit-packing gets the most attention because it is the piece most able to fail quietly:
/// an off-by-one in the accumulator corrupts values without corrupting sizes, so a store
/// round trip would notice only for some widths and some counts.
///
/// The delta-of-delta, dictionary and run-length primitives live in the local-only
/// ExtraPrimitives (not pushed); their tests are in ExtraPrimitivesTest.

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

}
