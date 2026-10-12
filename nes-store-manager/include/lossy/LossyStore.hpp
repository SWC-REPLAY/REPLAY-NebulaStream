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
#include <atomic>
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
#include <lossless/CompressionCodec.hpp>
#include <Store.hpp>
#include <TimeRange.hpp>
#include <lossy/ColumnCodec.hpp>
#include <lossy/LossyGroupFormat.hpp>

namespace NES
{

// Column-oriented tail of the store chain: one file of independently decodable groups.
// Reorganizes row-major input into columns and encodes each by a type-fixed codec (Quantized
// for bounded floats, ForBitpack for integers, Raw otherwise), then byte-compresses the result.
// Tail only -- no next level, no transformation, no flush policy.
class LossyStore
{
public:
    struct Config
    {
        std::string storeName;
        std::string storeDir;
        std::string schemaText;

        // Bytes per group for the standalone write path; in a chain the upstream segment size sets it.
        size_t groupSize = 64UZ * 1024UZ;

        // Error budget as a percentage; zero (default) makes the store lossless. Applies to
        // floating-point columns only -- integers (keys, ids, timestamps) are always lossless.
        double accuracyBoundPercent = 0.0;

        // The lossy codec for bounded floating-point columns; Quantized is the only one today.
        // A config knob, not a measured choice. Must be a codec with isLossy() true (checked at construction).
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

    // Take one segment from the tier above and seal it as one group, with the range it arrived with.
    void appendSegment(const uint8_t* data, size_t len, uint64_t rowCount, Timestamp minTs, Timestamp maxTs);

    void removeFile();
    [[nodiscard]] const std::string& getFilePath() const { return filePath; }

    /// test introspection
    [[nodiscard]] uint64_t debugGroupCount() const;

    // Each group's [minTs, maxTs] in write order, for tests asserting ranges are narrow and disjoint.
    [[nodiscard]] std::vector<std::pair<uint64_t, uint64_t>> debugGroupRanges() const;
    // Count of real group decodes, for resume-behaviour tests.
    [[nodiscard]] uint64_t debugDecodeCount() const { return decodeCount.load(std::memory_order_relaxed); }

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

        // Percentage this column may lose; non-zero only for float columns the bound reaches.
        double errorBoundPercent{0.0};

        // The two stages, matching ColumnEntry; fixed at construction from type and bound.
        uint8_t lossyCodec{0};
        uint8_t losslessCodec{0};
    };

    // One sealed group: its file offset and header, enough to prune by timestamp and pread it.
    struct GroupIndexEntry
    {
        uint64_t offset{0};
        Lossy::GroupHeader header;
    };

    // Columnarize rowCount rows, encode each column, append the group. Caller holds the unique lock.
    void sealGroup(const uint8_t* rows, uint64_t rowCount, Timestamp minTs, Timestamp maxTs);

    // Seal whatever the standalone write path accumulated; no-op when empty. Caller holds the lock.
    void sealPending();

    // Caller holds the lock.
    [[nodiscard]] uint64_t currentSizeLocked() const;

    Config config;
    Schema schema;
    uint32_t rowWidth{0};
    std::vector<FieldLayout> fields;

    // Byte compressor run over each column after its codec; stateless, held to avoid rebuilding it.
    CompressionCodec compressor;

    std::string filePath;
    BinaryStoreWriter writer;
    bool writerOpened{false};
    /// Where group data begins, i.e. just past the file header.
    uint64_t dataStartOffset{0};

    std::vector<GroupIndexEntry> groups;

    // Bumped when groups is emptied, invalidating any cursor index into it.
    uint64_t readGeneration{0};

    // Rows received via writeRecord but not yet sealed, with their range. Unused when fed segments.
    std::vector<uint8_t> pending;
    Timestamp pendingMinTs{Timestamp(Timestamp::INVALID_VALUE)};
    Timestamp pendingMaxTs{Timestamp(Timestamp::INITIAL_VALUE)};

    Timestamp fileMinTs{Timestamp(Timestamp::INVALID_VALUE)};
    Timestamp fileMaxTs{Timestamp(Timestamp::INITIAL_VALUE)};

    bool opened{false};
    mutable std::shared_mutex mutex;

    // Counts real group decodes for resume tests; atomic because read holds only a shared lock.
    mutable std::atomic<uint64_t> decodeCount{0};
};

}
