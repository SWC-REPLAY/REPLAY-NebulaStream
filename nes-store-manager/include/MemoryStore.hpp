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
#include <shared_mutex>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::StoreManager
{

/// In-memory store that holds TupleBuffers. Satisfies StoreConcept.
class MemoryStore
{
public:
    struct Config
    {
        size_t maxBufferSize = 64 * 1024 * 1024; // 64 MB default
    };

    explicit MemoryStore(Schema schema);
    MemoryStore(Schema schema, Config config);

    void open();
    void close();
    void flush();

    void write(TupleBuffer buffer, const Schema& schema);
    uint64_t read(TupleBuffer& buffer, const Schema& schema);
    [[nodiscard]] bool hasMore() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] uint64_t size() const;
    [[nodiscard]] std::string_view typeName() const noexcept { return "MemoryStore"; }

    /// Check whether the buffer has reached capacity.
    [[nodiscard]] bool isFull() const;

    /// Drain all stored TupleBuffers and return them. Resets internal storage.
    std::vector<TupleBuffer> drain();

private:
    Schema schema;
    Config config;
    std::deque<TupleBuffer> buffers;
    uint64_t currentSize{0};
    mutable std::shared_mutex mutex;
    bool opened{false};
};

} // namespace NES::StoreManager