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
#include <deque>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <FlushPolicy.hpp>
#include <Store.hpp>
#include <StoreTransformation.hpp>

namespace NES::StoreManager
{

/// Buffer with its timestamp range (min/max timestamps of records within).
struct TimedBuffer
{
    TupleBuffer buffer;
    Timestamp minTs{Timestamp(Timestamp::INVALID_VALUE)};
    Timestamp maxTs{Timestamp(Timestamp::INITIAL_VALUE)};
};

/// In-memory store that holds TimedBuffers. Satisfies StoreConcept.
/// Optionally chains to a next-level store with a flush policy and transformation.
class MemoryStore
{
public:
    struct Config
    {
        size_t maxBufferSize = 64UZ * 1024UZ * 1024UZ; /// 64 MB default /// NOLINT(readability-magic-numbers)
    };

    /// Standalone constructor (no chaining).
    MemoryStore(const Schema& schema, std::shared_ptr<BufferManager> bufferManager);
    MemoryStore(const Schema& schema, Config config, std::shared_ptr<BufferManager> bufferManager);

    /// Chained constructor: this store flushes to nextLevel when the policy triggers.
    /// The transformation is validated at construction to ensure the store pair is compatible.
    MemoryStore(const Schema& schema, Config config, std::shared_ptr<BufferManager> bufferManager, Store nextLevel, FlushPolicy policy);

    void open();
    void close(Store& self);
    void flush(Store& self);

    void writeRecord(const uint8_t* recordData, uint32_t recordSize, Timestamp ts, const Schema& writeSchema, Store& self);
    uint64_t read(TupleBuffer& buffer, const Schema& schema);
    [[nodiscard]] bool hasMore() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] uint64_t size() const;

    [[nodiscard]] static std::string_view typeName() noexcept { return "MemoryStore"; }

    /// Check whether the buffer has reached capacity.
    [[nodiscard]] bool isFull() const;

    /// Drain all stored TimedBuffers and return them. Resets internal storage.
    std::vector<TimedBuffer> drain();

private:
    /// Allocate a new TupleBuffer from the BufferManager and set it as the active buffer.
    void allocateActiveBuffer();

    Schema schema;
    Config config;
    std::shared_ptr<BufferManager> bufferManager;
    std::deque<TimedBuffer> buffers;
    uint64_t currentSize{0};
    mutable std::shared_mutex mutex;
    bool opened{false};

    /// Active buffer being filled record-by-record.
    std::optional<TimedBuffer> activeBuffer;
    uint64_t activeWriteOffset{0}; /// byte offset into the active buffer's memory

    /// Chaining support (all optional — empty for standalone stores).
    std::optional<Store> nextLevel;
    std::optional<StoreTransformation> transformation;
    std::optional<FlushPolicy> flushPolicy;
};

}
