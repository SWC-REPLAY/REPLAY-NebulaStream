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
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FlushPolicy.hpp>
#include <Store.hpp>
#include <StoreTransformationRegistry.hpp>
#include <StoreTypeRegistry.hpp>

namespace NES::StoreManager
{

MemoryStore::MemoryStore(const Schema& schema, std::shared_ptr<BufferManager> bufferManager)
    : schema(schema), bufferManager(std::move(bufferManager))
{
}

MemoryStore::MemoryStore(const Schema& schema, Config config, std::shared_ptr<BufferManager> bufferManager)
    : schema(schema), config(config), bufferManager(std::move(bufferManager))
{
}

MemoryStore::MemoryStore(
    const Schema& schema, const Config config, std::shared_ptr<BufferManager> bufferManager, Store nextLevel, FlushPolicy policy)
    : schema(schema), config(config), bufferManager(std::move(bufferManager)), nextLevel(std::move(nextLevel)), flushPolicy(policy)
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
        /// Seal the active buffer if it has any data
        if (activeBuffer.has_value() && activeWriteOffset > 0)
        {
            /// We need recordSize to compute tuple count. Use schema to derive it.
            const auto rowWidth = schema.getSizeOfSchemaInBytes();
            if (rowWidth > 0)
            {
                activeBuffer->buffer.setNumberOfTuples(activeWriteOffset / rowWidth);
            }
            currentSize += activeBuffer->buffer.getBufferSize();
            buffers.push_back(std::move(*activeBuffer));
        }
        activeBuffer.reset();
        activeWriteOffset = 0;

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

void MemoryStore::writeRecord(
    const uint8_t* recordData, uint32_t recordSize, Timestamp ts, const Schema& writeSchema, Store& self)
{
    std::unique_lock lock(mutex);
    PRECONDITION(opened, "MemoryStore must be opened before writing");
    /// Update schema from the write-time schema which has resolved types
    if (writeSchema.getSizeOfSchemaInBytes() > 0 && schema.getSizeOfSchemaInBytes() == 0)
    {
        schema = writeSchema;
    }

    /// Allocate an active buffer if we don't have one yet
    if (!activeBuffer.has_value())
    {
        allocateActiveBuffer();
    }

    /// Check if the active buffer has space for this record
    auto& active = *activeBuffer;
    const auto bufferSize = active.buffer.getBufferSize();
    if (activeWriteOffset + recordSize > bufferSize)
    {
        /// Seal the active buffer and push to completed deque
        active.buffer.setNumberOfTuples(activeWriteOffset / recordSize);
        currentSize += bufferSize;
        buffers.push_back(std::move(active));
        allocateActiveBuffer();
    }

    /// Copy record into active buffer
    auto& buf = activeBuffer.value();
    auto destSpan = buf.buffer.getAvailableMemoryArea<uint8_t>();
    std::memcpy(destSpan.data() + activeWriteOffset, recordData, recordSize);
    activeWriteOffset += recordSize;

    /// Update active buffer's min/max timestamps
    if (ts < buf.minTs)
    {
        buf.minTs = ts;
    }
    if (ts > buf.maxTs)
    {
        buf.maxTs = ts;
    }

    /// Check flush policy and flush to next level if triggered
    if (flushPolicy && flushPolicy->shouldFlush(currentSize))
    {
        lock.unlock();
        flush(self);
    }
}

void MemoryStore::allocateActiveBuffer()
{
    auto tb = bufferManager->getBufferBlocking();
    activeBuffer = TimedBuffer{std::move(tb), Timestamp(Timestamp::INVALID_VALUE), Timestamp(Timestamp::INITIAL_VALUE)};
    activeWriteOffset = 0;
}

uint64_t MemoryStore::read(TupleBuffer& buffer, const Schema& readSchema)
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
        const uint64_t numTuples = front.buffer.getNumberOfTuples();

        auto srcSpan = front.buffer.getAvailableMemoryArea<uint8_t>();
        auto destSpan = buffer.getAvailableMemoryArea<uint8_t>();
        const size_t bytesToCopy = std::min(srcSpan.size(), destSpan.size());
        std::memcpy(destSpan.data(), srcSpan.data(), bytesToCopy);
        buffer.setNumberOfTuples(numTuples);

        currentSize -= front.buffer.getBufferSize();
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

Schema MemoryStore::getSchema() const
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

std::vector<TimedBuffer> MemoryStore::drain()
{
    const std::unique_lock lock(mutex);
    std::vector<TimedBuffer> result;
    result.reserve(buffers.size() + (activeBuffer.has_value() ? 1 : 0));
    for (auto& buf : buffers)
    {
        result.push_back(std::move(buf));
    }
    buffers.clear();

    /// Seal and include the active buffer if it has data
    if (activeBuffer.has_value() && activeWriteOffset > 0)
    {
        const auto rowWidth = schema.getSizeOfSchemaInBytes();
        if (rowWidth > 0)
        {
            activeBuffer->buffer.setNumberOfTuples(activeWriteOffset / rowWidth);
        }
        result.push_back(std::move(*activeBuffer));
        activeBuffer.reset();
        activeWriteOffset = 0;
    }

    currentSize = 0;
    return result;
}

}

namespace NES
{
/// NOLINTNEXTLINE(performance-unnecessary-value-param)
StoreTypeRegistryReturnType StoreTypeGeneratedRegistrar::RegisterMemoryStoreStoreType(StoreTypeRegistryArguments args)
{
    return StoreManager::makeStore<StoreManager::MemoryStore>(std::move(args.schema), std::move(args.bufferManager));
}
}
