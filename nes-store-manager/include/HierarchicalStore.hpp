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
#include <shared_mutex>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Store.hpp>

namespace NES::StoreManager
{

/// Policy that determines when to trigger a flush from one level to the next.
struct FlushPolicy
{
    enum class Type
    {
        SIZE_THRESHOLD
    };
    Type type = Type::SIZE_THRESHOLD;
    size_t sizeThreshold = 64 * 1024 * 1024; // 64 MB
};

/// A level in the hierarchical store chain.
struct StoreLevel
{
    Store store;
    FlushPolicy policy;
};

/// A composable store that chains multiple stores with automatic flush between levels.
/// Satisfies StoreConcept.
/// Example: MemoryStore(64MB) -> FileStore (flush when memory is full).
class HierarchicalStore
{
public:
    explicit HierarchicalStore(std::vector<StoreLevel> levels, Schema schema);

    void open();
    void close();
    void flush();

    void write(TupleBuffer buffer, const Schema& schema);
    uint64_t read(TupleBuffer& buffer, const Schema& schema);
    [[nodiscard]] bool hasMore() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] uint64_t size() const;
    [[nodiscard]] std::string_view typeName() const noexcept { return "HierarchicalStore"; }

private:
    /// Check if level i should flush to level i+1, and do so if needed.
    void maybeFlush(size_t levelIndex);

    /// Force flush from level i to level i+1.
    void flushLevel(size_t levelIndex);

    std::vector<StoreLevel> levels;
    Schema schema;
    mutable std::shared_mutex mutex;
};

} // namespace NES::StoreManager