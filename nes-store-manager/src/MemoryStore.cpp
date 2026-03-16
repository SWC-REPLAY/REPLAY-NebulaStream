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
#include <cstring>
#include <ErrorHandling.hpp>

namespace NES::StoreManager
{

MemoryStore::MemoryStore(Schema schema) : schema(std::move(schema)), config() { }

MemoryStore::MemoryStore(Schema schema, Config config) : schema(std::move(schema)), config(config) { }

void MemoryStore::open()
{
    std::unique_lock lock(mutex);
    opened = true;
}

void MemoryStore::close()
{
    std::unique_lock lock(mutex);
    buffers.clear();
    currentSize = 0;
    opened = false;
}

void MemoryStore::flush()
{
    // No-op for standalone MemoryStore.
    // In a HierarchicalStore, the hierarchy handles flushing to the next level.
}

void MemoryStore::write(TupleBuffer buffer, const Schema& writeSchema)
{
    std::unique_lock lock(mutex);
    PRECONDITION(opened, "MemoryStore must be opened before writing");
    // Update schema from the write-time schema which has resolved types
    // (the construction-time schema may have UNDEFINED types if created before type inference)
    if (writeSchema.getSizeOfSchemaInBytes() > 0 && schema.getSizeOfSchemaInBytes() == 0)
    {
        schema = writeSchema;
    }
    currentSize += buffer.getBufferSize();
    buffers.push_back(std::move(buffer));
}

uint64_t MemoryStore::read(TupleBuffer& buffer, const Schema& /*schema*/)
{
    std::unique_lock lock(mutex);
    if (buffers.empty())
    {
        return 0;
    }

    auto& front = buffers.front();
    uint64_t numTuples = front.getNumberOfTuples();

    // Copy the data from the stored buffer into the provided buffer
    auto srcSpan = front.getAvailableMemoryArea<uint8_t>();
    auto destSpan = buffer.getAvailableMemoryArea<uint8_t>();
    size_t bytesToCopy = std::min(srcSpan.size(), destSpan.size());
    std::memcpy(destSpan.data(), srcSpan.data(), bytesToCopy);
    buffer.setNumberOfTuples(numTuples);

    currentSize -= front.getBufferSize();
    buffers.pop_front();

    return numTuples;
}

bool MemoryStore::hasMore() const
{
    std::shared_lock lock(mutex);
    return !buffers.empty();
}

Schema MemoryStore::getSchema() const
{
    return schema;
}

uint64_t MemoryStore::size() const
{
    std::shared_lock lock(mutex);
    return currentSize;
}

bool MemoryStore::isFull() const
{
    std::shared_lock lock(mutex);
    return currentSize >= config.maxBufferSize;
}

std::vector<TupleBuffer> MemoryStore::drain()
{
    std::unique_lock lock(mutex);
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

} // namespace NES::StoreManager

// Registry registration functions
#include <StoreTypeRegistry.hpp>

namespace NES
{
StoreTypeRegistryReturnType StoreTypeGeneratedRegistrar::RegisterMemoryStoreStoreType(StoreTypeRegistryArguments args)
{
    return StoreManager::makeStore<StoreManager::MemoryStore>(std::move(args.schema));
}
}