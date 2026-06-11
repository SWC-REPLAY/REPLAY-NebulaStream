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

#include <MemoryToFileTransformation.hpp>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <Util/Logger/Logger.hpp>
#include <FileStore.hpp>
#include <MemoryStore.hpp>
#include <Store.hpp>
#include <StoreTransformationRegistry.hpp>
#include <Time/Timestamp.hpp>

namespace NES::StoreManager
{

void MemoryToFileTransformation::execute(Store& source, Store& dest)
{
    auto typedSource = source.getAs<MemoryStore>();
    auto& memStore = typedSource.getMutable();
    auto schema = memStore.getSchema();
    const uint32_t rowWidth = FileStore::calculateRowWidth(schema);

    auto buffers = memStore.drain();
    NES_DEBUG("MemoryToFileTransformation: drained {} buffers from MemoryStore", buffers.size());

    /// Scan min/max timestamps across all drained buffers
    Timestamp overallMin(Timestamp::INVALID_VALUE);
    Timestamp overallMax(Timestamp::INITIAL_VALUE);
    for (const auto& timedBuf : buffers)
    {
        if (timedBuf.minTs < overallMin)
        {
            overallMin = timedBuf.minTs;
        }
        if (timedBuf.maxTs > overallMax)
        {
            overallMax = timedBuf.maxTs;
        }
    }

    /// Write all buffer data to the FileStore using bulk append
    auto typedDest = dest.getAs<FileStore>();
    auto& fileStore = typedDest.getMutable();
    for (auto& timedBuf : buffers)
    {
        const uint64_t numTuples = timedBuf.buffer.getNumberOfTuples();
        if (numTuples == 0)
        {
            continue;
        }
        auto srcSpan = timedBuf.buffer.getAvailableMemoryArea<uint8_t>();
        NES_DEBUG("MemoryToFileTransformation: writing {} tuples, {} bytes", numTuples, static_cast<size_t>(numTuples) * rowWidth);
        fileStore.appendRawBytes(srcSpan.data(), static_cast<size_t>(numTuples) * rowWidth);
    }

    /// Update the file header with the scanned overall min/max timestamps
    fileStore.updateFileTimestamps(overallMin, overallMax);
}

}

namespace NES
{
StoreTransformationRegistryReturnType
StoreTransformationGeneratedRegistrar::RegisterMemoryStore_to_FileStoreStoreTransformation(StoreTransformationRegistryArguments)
{
    return StoreManager::MemoryToFileTransformation{};
}
}
