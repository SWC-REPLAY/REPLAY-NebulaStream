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

#include <FileToCompressedTransformation.hpp>

#include <Util/Logger/Logger.hpp>
#include <CompressedStore.hpp>
#include <FileStore.hpp>
#include <Store.hpp>
#include <StoreTransformationRegistry.hpp>

namespace NES::StoreManager
{

void FileToCompressedTransformation::execute(Store& source, Store& dest)
{
    /// 1. Drain the FileStore: read its segments off disk and truncate its file.
    ///    (drainSegments() empties the source, so the data has genuinely moved down a tier.)
    auto typedSource = source.getAs<FileStore>();
    auto& fileStore = typedSource.getMutable();
    auto segments = fileStore.drainSegments();

    if (segments.empty())
    {
        return; /// nothing to move
    }

    /// 2. Hand each segment's bytes and its own range to the CompressedStore, which seals
    ///    everything it has been given into one compressed segment on the following flush.
    ///    It does not divide its file the way the lossy tier does, so it simply unions the
    ///    ranges as they arrive -- giving it a per-segment layout is a separate change.
    auto typedDest = dest.getAs<CompressedStore>();
    auto& compressedStore = typedDest.getMutable();
    for (const auto& segment : segments)
    {
        if (segment.bytes.empty())
        {
            continue;
        }
        NES_DEBUG("FileToCompressedTransformation: moving a {}-byte segment from FileStore", segment.bytes.size());
        compressedStore.appendRawBytes(segment.bytes.data(), segment.bytes.size());
        compressedStore.updateTimestamps(segment.minTs, segment.maxTs);
    }
}

}

namespace NES
{
/// NOLINTNEXTLINE(performance-unnecessary-value-param)
StoreTransformationRegistryReturnType
StoreTransformationGeneratedRegistrar::RegisterFileStore_to_CompressedStoreStoreTransformation(StoreTransformationRegistryArguments)
{
    return StoreManager::FileToCompressedTransformation{};
}
}
