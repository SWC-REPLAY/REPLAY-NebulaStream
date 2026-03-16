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

#include <HierarchicalStore.hpp>

#include <string>

#include <ErrorHandling.hpp>

namespace NES::StoreManager
{

HierarchicalStore::HierarchicalStore(std::vector<StoreLevel> levels, Schema schema) : levels(std::move(levels)), schema(std::move(schema))
{
    PRECONDITION(!this->levels.empty(), "HierarchicalStore requires at least one level");
}

void HierarchicalStore::open()
{
    for (auto& [store, _, __] : levels)
    {
        store.open();
    }
}

void HierarchicalStore::close()
{
    // Flush all levels before closing
    flush();
    for (auto& [store, _, __] : levels)
    {
        store.close();
    }
}

void HierarchicalStore::flush()
{
    std::unique_lock const lock(mutex);
    // Flush from top to bottom
    for (size_t i = 0; i + 1 < levels.size(); ++i)
    {
        flushLevel(i);
    }
    // Flush the last level (e.g., fsync for FileStore)
    levels.back().store.flush();
}

void HierarchicalStore::write(TupleBuffer buffer, const Schema& writeSchema)
{
    std::unique_lock const lock(mutex);
    // Write to the top level
    levels.at(0).store.write(std::move(buffer), writeSchema);
    // Check if top level needs flushing
    maybeFlush(0);
}

uint64_t HierarchicalStore::read(TupleBuffer& buffer, const Schema& readSchema)
{
    // Read from the last level (the persistent store)
    return levels.back().store.read(buffer, readSchema);
}

bool HierarchicalStore::hasMore() const
{
    return levels.back().store.hasMore();
}

Schema HierarchicalStore::getSchema() const
{
    return schema;
}

uint64_t HierarchicalStore::size() const
{
    std::shared_lock const lock(mutex);
    uint64_t total = 0;
    for (const auto& level : levels)
    {
        total += level.store.size();
    }
    return total;
}

void HierarchicalStore::maybeFlush(const size_t levelIndex)
{
    if (levelIndex + 1 >= levels.size())
    {
        return;
    }

    if (auto& [store, policy, __] = levels.at(levelIndex);
        policy.type == FlushPolicy::Type::SIZE_THRESHOLD && store.size() >= policy.sizeThreshold)
    {
        flushLevel(levelIndex);
        // Cascade: check if next level also needs flushing
        maybeFlush(levelIndex + 1);
    }
}

void HierarchicalStore::flushLevel(const size_t levelIndex)
{
    PRECONDITION(levelIndex + 1 < levels.size(), "Cannot flush last level to next");

    auto& [sourceStore, _, transformation] = levels.at(levelIndex);
    auto& destStore = levels.at(levelIndex + 1).store;

    PRECONDITION(transformation.has_value(), "No transformation set for level {}", levelIndex);
    transformation->execute(sourceStore, destStore);
}

} // namespace NES::StoreManager
