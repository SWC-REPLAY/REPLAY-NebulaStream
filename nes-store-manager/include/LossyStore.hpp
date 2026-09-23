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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <BinaryStoreWriter.hpp>
#include <CompressionCodec.hpp>
#include <Store.hpp>
#include <TimeRange.hpp>
#include <lossy/ColumnCodec.hpp>
#include <lossy/ColumnStats.hpp>
#include <lossy/LossyGroupFormat.hpp>

namespace NES
{

/// Column-oriented tail of the store chain, backed by one file of independently decodable
/// groups (see LossyGroupFormat.hpp for the on-disk layout).
///
/// Receives row-major bytes from the tier above and reorganizes them into columns, because
/// that is what every useful encoding needs: a column holds one type drawn from one
/// distribution, and both lightweight encodings and error-bounded lossy ones only have
/// something to exploit once the values are homogeneous and adjacent. Row-major bytes
/// interleave a float, an id and a flag, and no encoding can find structure in that.
///
/// The encoding pipeline per column is three stages, all of them present:
///   1. an optional lossy step (quantization), where the error budget is spent, once;
///   2. a lossless logical encoding (frame-of-reference, delta, dictionary, RLE, bit-pack);
///   3. an optional byte compressor.
///
/// They compose because stage 1 leaves an ordinary integer column behind -- one code per row --
/// so stage 2 encodes a quantized float column with exactly the codecs it would use on any
/// integer column, and nothing about it is special-cased. Which two stages a column used is
/// recorded per column per group, so the choice is visible in the file rather than implied.
///
/// A column is lossy only where the store's accuracy bound reaches it, which is floating-point
/// columns and nothing else. With no bound set the store is bit-exact end to end -- the
/// baseline every codec is measured against.
///
/// Tail only -- no next level, no transformation, no flush policy.
class LossyStore
{
public:
    struct Config
    {
        std::string storeName;
        std::string storeDir;
        std::string schemaText;

        /// Bytes per group, used only by the standalone write path.
        ///
        /// In a chain a group is one segment from the tier above -- see
        /// FileStore::Config::segmentSize, which is where the size is really set. The rows
        /// arrive already grouped and already carrying the range that describes them, so this
        /// tier never decides a boundary or recovers a timestamp of its own. This value only
        /// governs how much writeRecord accumulates before sealing when there is no tier above.
        size_t groupSize = 64UZ * 1024UZ;

        /// The store's error budget: no value may move by more than this percentage of its
        /// column's value range within a group. Zero, the default, makes the whole store
        /// lossless.
        ///
        /// One number for the store rather than one per column, and the store decides who it
        /// applies to. Floating-point columns get it; every other type is stored losslessly no
        /// matter what is set here.
        ///
        /// The asymmetry is deliberate. A FLOAT64 is a measurement in practice -- rounding one
        /// costs precision that was never exact to begin with. An integer may be a key, an id
        /// or a timestamp, and rounding those corrupts joins and ordering silently; nothing in
        /// the schema distinguishes them from an integer measurement, so none of them is
        /// offered the lossy path. That keeps the columns that must not move safe without
        /// asking the caller to enumerate them.
        double accuracyBoundPercent = 0.0;

        /// Which codec spends the bound above, for every column it reaches.
        ///
        /// Fixed by configuration rather than measured, for the same reason the lossless step
        /// is fixed (see `sealGroup` in LossyStore.cpp): comparing lossy codecs against each
        /// other has no cheap closed-form estimate the way the lossless ones do, so "let the
        /// store pick the smaller one" would mean actually encoding with every candidate and
        /// comparing bytes -- a real measurement pass, and the kind of data-driven decision this
        /// store is deliberately not making. One external knob, applied uniformly, also happens
        /// to be exactly what benchmarking different lossy methods against each other needs: run
        /// the store once per value, compare the results, rather than have the store hide the
        /// choice.
        ///
        /// Must name a codec for which `ColumnCodec::isLossy()` is true; checked at construction.
        Lossy::CodecId lossyCodec = Lossy::CodecId::Quantized;
    };

    LossyStore(Config config, const Schema& schema);
    ~LossyStore();

    LossyStore(const LossyStore&) = delete;
    LossyStore& operator=(const LossyStore&) = delete;
    LossyStore(LossyStore&&) = delete;
    LossyStore& operator=(LossyStore&&) = delete;

    void open();
    void close(Store& self);
    void flush(Store& self);

    void writeRecord(const uint8_t* recordData, uint32_t recordSize, Timestamp ts, Store& self);
    uint64_t read(TupleBuffer& buffer, const Schema& readSchema, const TimeRange& range, StoreReadCursor& cursor, size_t depth);
    [[nodiscard]] bool hasMore() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] uint64_t size() const;

    [[nodiscard]] static std::string_view typeName() noexcept { return "LossyStore"; }

    /// ---- bulk ingest from the tier above (used by a transformation) -------------------
    /// Take one segment from the tier above and seal it as one group.
    ///
    /// The range arrives with the segment rather than being recovered from the rows: the tier
    /// that knew the timestamps measured them, and every tier below carries them. Nothing here
    /// needs to be told which column holds record time.
    void appendSegment(const uint8_t* data, size_t len, uint64_t rowCount, Timestamp minTs, Timestamp maxTs);
    /// ----------------------------------------------------------------------------------

    void removeFile();
    [[nodiscard]] const std::string& getFilePath() const { return filePath; }

    /// test introspection
    [[nodiscard]] uint64_t debugGroupCount() const;

    /// Each group's [minTs, maxTs], in write order. Exposed so a test can assert the ranges are
    /// genuinely narrow and disjoint -- the property that makes skipping possible, and the one
    /// that a correct-rows assertion cannot distinguish from filtering every row.
    [[nodiscard]] std::vector<std::pair<uint64_t, uint64_t>> debugGroupRanges() const;

private:
    /// Where one schema field sits inside a packed row, resolved once at construction.
    struct FieldLayout
    {
        std::string name;
        DataType::Type type{DataType::Type::UNDEFINED};
        /// Offset of the VALUE, i.e. already past the null indicator when there is one.
        uint32_t valueOffset{0};
        uint32_t valueWidth{0};
        bool nullable{false};

        /// What this column is permitted to lose. Already zero for every column the store's
        /// budget does not reach, so selection needs no further exception for keys or times.
        Lossy::CompressionPolicy policy{};

        /// The two stages, matching ColumnEntry. Only meaningful once `codecsSelected` is set;
        /// until then they are the placeholders the column was constructed with, not decisions.
        uint8_t lossyCodec{0};
        uint8_t losslessCodec{0};
    };

    /// One sealed group: its file offset plus its header, so a read can prune by timestamp
    /// and know exactly how much to pread without holding any column bytes in memory.
    struct GroupIndexEntry
    {
        uint64_t offset{0};
        Lossy::GroupHeader header;
    };

    /// Columnarize `rowCount` rows, encode each column, and append the group to the file.
    /// The range describes those rows and is written straight into the group header.
    /// PRECONDITION: the caller holds `mutex` (unique lock).
    void sealGroup(const uint8_t* rows, uint64_t rowCount, Timestamp minTs, Timestamp maxTs);

    /// Seal whatever the standalone write path has accumulated, with the range built alongside
    /// it. No-op when nothing is pending.
    /// PRECONDITION: the caller holds `mutex` (unique lock).
    void sealPending();

    /// PRECONDITION: the caller holds `mutex`.
    [[nodiscard]] uint64_t currentSizeLocked() const;

    Config config;
    Schema schema;
    uint32_t rowWidth{0};
    std::vector<FieldLayout> fields;

    /// Runs over each column after its codec, and is undone before the codec on the way back.
    /// Stateless; held here only to avoid rebuilding it per column per group.
    CompressionCodec compressor;

    /// Whether `FieldLayout::codec` holds a real choice yet.
    ///
    /// Codecs are picked from statistics over the first group that carries rows, because the
    /// schema alone cannot say whether a UINT64 column is record time, a key or a counter.
    /// Fixing the choice after that keeps a column's encoding stable across the file rather
    /// than dependent on where group boundaries fell.
    bool codecsSelected{false};


    std::string filePath;
    BinaryStoreWriter writer;
    bool writerOpened{false};
    /// Where group data begins, i.e. just past the file header.
    uint64_t dataStartOffset{0};

    std::vector<GroupIndexEntry> groups;

    /// Bumped whenever `groups` is emptied, i.e. whenever an index into it stops meaning what it
    /// did. Appending never invalidates an index, so sealing a group does not bump it.
    uint64_t readGeneration{0};

    /// Rows received through writeRecord but not yet sealed, and their range. Unused when the
    /// store is fed segments by a transformation.
    std::vector<uint8_t> pending;
    Timestamp pendingMinTs{Timestamp(Timestamp::INVALID_VALUE)};
    Timestamp pendingMaxTs{Timestamp(Timestamp::INITIAL_VALUE)};

    Timestamp fileMinTs{Timestamp(Timestamp::INVALID_VALUE)};
    Timestamp fileMaxTs{Timestamp(Timestamp::INITIAL_VALUE)};

    bool opened{false};
    mutable std::shared_mutex mutex;
};

}
