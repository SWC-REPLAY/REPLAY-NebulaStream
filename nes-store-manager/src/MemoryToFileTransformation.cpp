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
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <FileStore.hpp>
#include <MemoryStore.hpp>
#include <Store.hpp>
#include <StoreTransformationRegistry.hpp>

namespace NES
{

void MemoryToFileTransformation::execute(Store& source, Store& dest)
{
    auto typedSource = source.getAs<MemoryStore>();
    auto& memStore = typedSource.getMutable();
    auto schema = memStore.getSchema();
    const uint32_t rowWidth = FileStore::calculateRowWidth(schema);

    auto buffers = memStore.drain();
    NES_DEBUG("MemoryToFileTransformation: drained {} buffers from MemoryStore", buffers.size());

    /// Each buffer carries its own range down; nothing is pooled across them.
    ///
    /// Collapsing every buffer's range into one for the whole drain is what this used to do,
    /// and it was where the information died: each segment, and then each group the lossy tier
    /// cut from it, inherited the same span covering everything, so no read could skip
    /// anything. The ranges already exist -- MemoryStore computed them from the timestamps
    /// handed to writeRecord -- so they are carried, never recomputed from the row bytes.
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
        fileStore.appendBuffer(
            srcSpan.data(), static_cast<size_t>(numTuples) * rowWidth, numTuples, timedBuf.minTs, timedBuf.maxTs);
    }
}

StoreTransformationRegistryReturnType
StoreTransformationGeneratedRegistrar::RegisterMemoryStore_to_FileStoreStoreTransformation(StoreTransformationRegistryArguments)
{
    return MemoryToFileTransformation{};
}
}
