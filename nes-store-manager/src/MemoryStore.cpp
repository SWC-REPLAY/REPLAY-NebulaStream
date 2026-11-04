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

#include <MemoryStore.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <vector>
#include <DataTypes/UnboundField.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FlushPolicy.hpp>
#include <Store.hpp>
#include <StoreTransformationRegistry.hpp>

namespace NES::StoreManager
{

MemoryStore::MemoryStore(const StoreSchema& schema) : schema(schema)
{
}

MemoryStore::MemoryStore(const StoreSchema& schema, Config config) : schema(schema), config(config)
{
}

MemoryStore::MemoryStore(const StoreSchema& schema, const Config config, Store nextLevel, FlushPolicy policy)
    : schema(schema), config(config), nextLevel(std::move(nextLevel)), flushPolicy(policy)
{
    auto foundTransformation = StoreTransformationRegistry::instance().findTransformation(
        NES::StoreManager::MemoryStore::typeName(), this->nextLevel->typeName());
    INVARIANT(
        foundTransformation.has_value(), "No transformation registered for '{}' -> '{}'", this->typeName(), this->nextLevel->typeName());
    transformation = std::move(*foundTransformation);
}

void MemoryStore::open()
{
    const std::unique_lock lock(mutex);
    opened = true;
    if (nextLevel)
    {
        nextLevel->open();
    }
}

void MemoryStore::close(Store& self)
{
    NES_DEBUG("MemoryStore closing")
    {
        std::unique_lock lock(mutex);
        if (!buffers.empty())
        {
            lock.unlock();
            flush(self);
            lock.lock();
        }
        buffers.clear();
        currentSize = 0;
        opened = false;
    }
    if (nextLevel)
    {
        nextLevel->close();
    }
}

void MemoryStore::flush(Store& self)
{
    if (nextLevel && transformation)
    {
        transformation->execute(self, *nextLevel);
        nextLevel->flush();
    }
}

void MemoryStore::write(TupleBuffer buffer, const StoreSchema& writeSchema, Store& self)
{
    NES_DEBUG("Writing buffer with {} tuples and total size {} to memory store", buffer.getNumberOfTuples(), buffer.getBufferSize());
    std::unique_lock lock(mutex);
    PRECONDITION(opened, "MemoryStore must be opened before writing");
    /// Update schema from the write-time schema which has resolved types
    /// (the construction-time schema may have UNDEFINED types if created before type inference)
    if (writeSchema.size() > 0 && schema.size() == 0)
    {
        schema = writeSchema;
    }
    currentSize += buffer.getBufferSize();
    buffers.push_back(std::move(buffer));

    /// Check flush policy and flush to next level if triggered
    if (flushPolicy && flushPolicy->shouldFlush(currentSize))
    {
        lock.unlock();
        flush(self);
    }
}

uint64_t MemoryStore::read(TupleBuffer& buffer, const StoreSchema& readSchema)
{
    if (nextLevel)
    {
        NES_DEBUG("Checking if next level has data to be read")
        if (const uint64_t nextLevelRead = nextLevel->read(buffer, readSchema); nextLevelRead != 0)
        {
            return nextLevelRead;
        }
    }
    NES_DEBUG("Read from memory store into buffer");
    const std::unique_lock lock(mutex);
    if (!buffers.empty())
    {
        auto& front = buffers.front();
        const uint64_t numTuples = front.getNumberOfTuples();

        auto srcSpan = front.getAvailableMemoryArea<uint8_t>();
        auto destSpan = buffer.getAvailableMemoryArea<uint8_t>();
        const size_t bytesToCopy = std::min(srcSpan.size(), destSpan.size());
        std::memcpy(destSpan.data(), srcSpan.data(), bytesToCopy);
        buffer.setNumberOfTuples(numTuples);

        currentSize -= front.getBufferSize();
        buffers.pop_front();

        return numTuples;
    }
    return 0;
}

bool MemoryStore::hasMore() const
{
    std::shared_lock lock(mutex);
    if (!buffers.empty())
    {
        return true;
    }
    lock.unlock();
    /// Own buffers exhausted — check /
    if (nextLevel)
    {
        return nextLevel->hasMore();
    }
    return false;
}

StoreSchema MemoryStore::getSchema() const
{
    return schema;
}

uint64_t MemoryStore::size() const
{
    const std::shared_lock lock(mutex);
    return currentSize;
}

bool MemoryStore::isFull() const
{
    const std::shared_lock lock(mutex);
    return currentSize >= config.maxBufferSize;
}

std::vector<TupleBuffer> MemoryStore::drain()
{
    const std::unique_lock lock(mutex);
    std::vector<TupleBuffer> result;
    result.reserve(buffers.size());
    for (auto& buf : buffers)
    {
        result.push_back(std::move(buf));
    }
    buffers.clear();
    currentSize = 0;
    return result;
}

}
