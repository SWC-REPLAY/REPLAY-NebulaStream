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

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <lossy/ColumnCodec.hpp>

namespace NES::StoreManager::Lossy
{

/// What one pass over a column reveals about it.
///
/// The schema says a column is UINT64; it does not say whether that column is record time, a
/// device id or a counter, and those three want three different codecs. These are the cheap
/// measurements that separate them -- cheap enough to take on every column of every group, all
/// in the single pass that already columnarizes the rows.
///
/// Each field exists because some codec's cost is a function of it. Nothing is collected
/// speculatively.
struct ColumnStats
{
    uint64_t rowCount{0};

    /// Bytes per value, carried along because it is what the identity codec's cost is made of.
    uint32_t valueWidth{0};

    /// How many different values appear. Sets the width of a dictionary code.
    uint64_t distinctCount{0};

    /// How many maximal blocks of consecutive equal values. Sets what run-length encoding
    /// would have to store.
    uint64_t runCount{0};
    uint64_t longestRun{0};

    /// max - min in the column's own signedness: the range frame-of-reference packs.
    uint64_t valueSpan{0};

    /// max - min over raw bit patterns: the range a dictionary's sorted table packs. Equal to
    /// valueSpan for unsigned columns; different for signed ones, where a negative value's
    /// pattern sorts above every positive one.
    uint64_t patternSpan{0};

    /// Largest zig-zagged second difference. Sets the width delta-of-delta would pack, and is
    /// the reason a steady stream can be recognised without guessing at a threshold: a fixed
    /// sampling rate drives this to zero.
    uint64_t maxSecondDifference{0};
};

/// One pass. `in` is the column as the store just columnarized it.
[[nodiscard]] ColumnStats computeStats(const ColumnValues& in);

/// What the caller permits for a column. Nothing in it describes the data.
///
/// Kept apart from the statistics on purpose. Permission is the user's answer and is known
/// before a byte is read; the statistics are the data's answer and cost a pass to obtain. The
/// first decides which codecs may compete, the second decides which of them wins, and running
/// them together is what would let a measurement talk the store into rounding a column it was
/// never allowed to round.
struct CompressionPolicy
{
    /// Accuracy bound as a percentage of the column's value range within a group. Zero -- the
    /// default -- permits no loss, which keeps every lossy codec out of the candidate set.
    ///
    /// Configured for the store today and reaching floating-point columns only, so it cannot
    /// round a key or a timestamp whatever it is set to. The eventual per-column form comes
    /// from SQL and changes only where this value comes from, not what it means.
    double errorBoundPercent{0.0};
};

/// Bytes `codec` would produce for a column with these statistics and this policy, from its
/// published layout.
///
/// An estimate only in the sense that it is computed rather than measured. For the lossless
/// codecs the layout is closed-form in the statistics, so it is exact. For the quantizing one
/// it is a lower bound: the code width follows from the policy alone (see below), but the cost
/// of any non-finite values held out of the grid does not, and those are not measured here.
[[nodiscard]] uint64_t
estimateEncodedBytes(CodecId codec, const ColumnStats& stats, const CompressionPolicy& policy) noexcept;

/// Every codec that may encode this column, in CodecId order. Touches no data.
///
/// Two rules, and the second is the one that matters:
///
///   - the codec must `supports()` the type;
///   - if any lossy codec survives that and the policy permits loss, the lossy ones are the
///     answer and the lossless ones are dropped -- not merged with them.
///
/// The partition is deliberate. A lossy codec is smaller than a lossless one because it threw
/// information away, so putting both in one ranking does not compare them, it just elects the
/// lossy one by definition. Whether to spend accuracy is settled here, by policy; only after it
/// is settled do bytes decide anything.
[[nodiscard]] std::vector<CodecId> filterCandidates(DataType::Type type, const CompressionPolicy& policy);

/// The cheapest candidates by estimate, best first.
///
/// Two, not one, because the estimate ranks what a codec *produces* while the store keeps
/// whichever of {codec output, byte-compressed codec output} is smaller -- and those two orders
/// can disagree. Run-length encoding is where they disagree most: it removes exactly the
/// repetition an LZ matcher is best at, so it can win here and lose on disk. Estimating that is
/// not possible; only compressing both settles it.
///
/// So the estimate narrows the field and the caller decides between what is left. Two rather
/// than all of them because encoding is not free, and past the runner-up the estimate is not
/// close enough for the compressor to overturn.
struct CodecCandidates
{
    static constexpr size_t MAX = 2;

    /// Only the first `count` entries are meaningful.
    std::array<CodecId, MAX> codecs{CodecId::Raw, CodecId::Raw};
    size_t count{0};
};

/// `candidates` must be non-empty, and must already be filtered -- ranking a mixed pool of
/// lossy and lossless codecs is the comparison `filterCandidates` exists to prevent.
[[nodiscard]] CodecCandidates
rank(std::span<const CodecId> candidates, const ColumnStats& stats, const CompressionPolicy& policy);

}
