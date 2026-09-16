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

#include <lossy/ColumnStats.hpp>

#include <algorithm>
#include <limits>
#include <unordered_set>

#include <ErrorHandling.hpp>
#include <lossy/Primitives.hpp>

namespace NES::Lossy
{

namespace
{

/// Byte counts of the codec headers, taken from the layouts documented on each codec.
constexpr uint64_t FOR_BITPACK_HEADER = 8 + 1;
constexpr uint64_t DELTA_OF_DELTA_HEADER = 8 + 8 + 1;
constexpr uint64_t DICTIONARY_BODY_HEADER = 4 + 8 + 1;
constexpr uint64_t CODE_WIDTH_BYTE = 1;
constexpr uint64_t RUN_COUNT_FIELD = 4;
constexpr uint64_t RUN_LENGTH_WIDTH_BYTE = 1;
/// flags + minValue + delta + bitWidth, excluding any exception list.
constexpr uint64_t QUANTIZE_HEADER = 1 + 8 + 8 + 1;

uint64_t dictionaryBodyBytes(const ColumnStats& stats)
{
    return DICTIONARY_BODY_HEADER + bitPackedSize(stats.distinctCount, bitWidthFor(stats.patternSpan));
}

/// Bits a dictionary code occupies. Zero distinct values cannot happen for a non-empty column,
/// and one distinct value needs no bits at all.
uint8_t codeBitWidth(const ColumnStats& stats)
{
    return stats.distinctCount == 0 ? 0 : bitWidthFor(stats.distinctCount - 1);
}

}

ColumnStats computeStats(const ColumnValues& in)
{
    ColumnStats stats;
    stats.rowCount = in.rowCount;
    stats.valueWidth = in.valueWidth;
    if (in.rowCount == 0)
    {
        return stats;
    }

    const bool isSigned = isSignedType(in.type);

    /// Both orderings are tracked because two different codecs pack two different ranges:
    /// frame-of-reference subtracts the column's minimum *value*, a dictionary sorts raw bit
    /// patterns. For unsigned columns these coincide; for signed ones they do not.
    int64_t minSigned = std::numeric_limits<int64_t>::max();
    int64_t maxSigned = std::numeric_limits<int64_t>::min();
    uint64_t minPattern = std::numeric_limits<uint64_t>::max();
    uint64_t maxPattern = 0;

    std::unordered_set<uint64_t> distinct;
    distinct.reserve(in.rowCount);

    uint64_t previousPattern = 0;
    uint64_t previousDelta = 0;
    uint64_t currentRun = 0;

    for (uint64_t r = 0; r < in.rowCount; ++r)
    {
        const uint8_t* at = in.data + (r * in.valueWidth);
        const uint64_t pattern = loadUnsigned(at, in.valueWidth);

        distinct.insert(pattern);
        minPattern = std::min(minPattern, pattern);
        maxPattern = std::max(maxPattern, pattern);

        if (isSigned)
        {
            const int64_t value = loadSigned(at, in.valueWidth);
            minSigned = std::min(minSigned, value);
            maxSigned = std::max(maxSigned, value);
        }

        if (r == 0 || pattern != previousPattern)
        {
            ++stats.runCount;
            currentRun = 1;
        }
        else
        {
            ++currentRun;
        }
        stats.longestRun = std::max(stats.longestRun, currentRun);

        /// Second differences, computed exactly as the codec computes them: modulo 2^64, so
        /// values at opposite ends of the range give the same answer here as there.
        if (r >= 1)
        {
            const uint64_t delta = pattern - previousPattern;
            if (r >= 2)
            {
                const uint64_t code = zigZagEncode(static_cast<int64_t>(delta - previousDelta));
                stats.maxSecondDifference = std::max(stats.maxSecondDifference, code);
            }
            previousDelta = delta;
        }
        previousPattern = pattern;
    }

    stats.distinctCount = distinct.size();
    stats.patternSpan = maxPattern - minPattern;
    stats.valueSpan = isSigned ? static_cast<uint64_t>(maxSigned) - static_cast<uint64_t>(minSigned) : stats.patternSpan;
    return stats;
}

uint64_t estimateEncodedBytes(const CodecId codec, const ColumnStats& stats, const CompressionPolicy&) noexcept
{
    switch (codec)
    {
        case CodecId::Raw:
            return stats.rowCount * stats.valueWidth;

        case CodecId::ForBitpack:
            return FOR_BITPACK_HEADER + bitPackedSize(stats.rowCount, bitWidthFor(stats.valueSpan));

        case CodecId::DeltaOfDelta:
            return DELTA_OF_DELTA_HEADER
                + bitPackedSize(stats.rowCount > 2 ? stats.rowCount - 2 : 0, bitWidthFor(stats.maxSecondDifference));

        case CodecId::Dictionary:
            return dictionaryBodyBytes(stats) + CODE_WIDTH_BYTE + bitPackedSize(stats.rowCount, codeBitWidth(stats));

        case CodecId::DictRle:
            return dictionaryBodyBytes(stats) + RUN_COUNT_FIELD + CODE_WIDTH_BYTE
                + bitPackedSize(stats.runCount, codeBitWidth(stats)) + RUN_LENGTH_WIDTH_BYTE
                + bitPackedSize(stats.runCount, bitWidthFor(stats.longestRun));

        case CodecId::Bool:
            return bitPackedSize(stats.rowCount, 1);

        case CodecId::Quantized:
            /// Not priceable from these statistics. The grid is uniform in the *logarithm* of
            /// the value, so the code count is ln(max|v| / min|v|) / deltaLog -- and nothing
            /// here measures a float column's dynamic range, deliberately: every field above is
            /// computed from raw bit patterns, and differencing those on floats yields noise.
            ///
            /// Unreachable in practice, because filtering hands the lossy codec over as the
            /// sole candidate rather than ranking it against anything. The maximum is returned
            /// rather than zero so that if it ever did reach a ranking, it would lose instead
            /// of winning every comparison by default.
            return std::numeric_limits<uint64_t>::max();
    }
    return 0;
}

std::vector<CodecId> filterCandidates(const DataType::Type type, const CompressionPolicy& policy)
{
    /// Every registered codec. Adding one here is the only edit selection needs: what it can
    /// encode and whether it loses anything are answered by the codec itself, so a codec cannot
    /// be added and left unreachable the way a hand-maintained candidate list allows.
    constexpr CodecId ALL[]{
        CodecId::Raw,
        CodecId::ForBitpack,
        CodecId::Quantized,
        CodecId::DeltaOfDelta,
        CodecId::Dictionary,
        CodecId::DictRle,
        CodecId::Bool};

    std::vector<CodecId> lossless;
    std::vector<CodecId> lossy;
    for (const CodecId id : ALL)
    {
        const ColumnCodec& codec = getCodec(id);
        if (!codec.supports(type))
        {
            continue;
        }
        (codec.isLossy() ? lossy : lossless).push_back(id);
    }

    /// The budget is spent here or nowhere. When it is spent the lossless codecs are not
    /// alternatives to weigh against the lossy ones -- they answer a different question -- so
    /// they leave the field entirely.
    if (!lossy.empty() && policy.errorBoundPercent > 0.0)
    {
        return lossy;
    }
    return lossless;
}

CodecCandidates rank(const std::span<const CodecId> candidates, const ColumnStats& stats, const CompressionPolicy& policy)
{
    PRECONDITION(!candidates.empty(), "ranking an empty candidate set");

    /// Each codec's layout is closed-form in the statistics, so this is a computation rather
    /// than a heuristic -- there is no distinct-ratio threshold to be tuned or defended, and a
    /// codec wins only where it actually produces fewer bytes. Raw is a candidate on purpose:
    /// a column with no structure should pay no header.
    CodecId best = candidates.front();
    CodecId runnerUp = candidates.front();
    uint64_t bestBytes = std::numeric_limits<uint64_t>::max();
    uint64_t runnerUpBytes = std::numeric_limits<uint64_t>::max();
    for (const CodecId candidate : candidates)
    {
        const uint64_t bytes = estimateEncodedBytes(candidate, stats, policy);
        if (bytes < bestBytes)
        {
            runnerUp = best;
            runnerUpBytes = bestBytes;
            best = candidate;
            bestBytes = bytes;
        }
        else if (bytes < runnerUpBytes)
        {
            runnerUp = candidate;
            runnerUpBytes = bytes;
        }
    }

    if (candidates.size() == 1 || runnerUpBytes == std::numeric_limits<uint64_t>::max())
    {
        return CodecCandidates{.codecs = {best, best}, .count = 1};
    }
    return CodecCandidates{.codecs = {best, runnerUp}, .count = 2};
}

}
