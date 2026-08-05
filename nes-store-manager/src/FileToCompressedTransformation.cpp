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
    /// 1. Drain the FileStore: read all its rows off disk and truncate its file.
    ///    (drain() empties the source, so the data has genuinely moved down a tier.)
    auto typedSource = source.getAs<FileStore>();
    auto& fileStore = typedSource.getMutable();
    auto drained = fileStore.drain();

    if (drained.bytes.empty())
    {
        return; /// nothing to move
    }
    NES_DEBUG("FileToCompressedTransformation: draining {} bytes from FileStore", drained.bytes.size());

    /// 2. Hand the raw bytes + timestamp range to the CompressedStore. The
    ///    subsequent nextLevel->flush() (issued by FileStore::flush) seals them
    ///    into one compressed segment.
    auto typedDest = dest.getAs<CompressedStore>();
    auto& compressedStore = typedDest.getMutable();
    compressedStore.appendRawBytes(drained.bytes.data(), drained.bytes.size());
    compressedStore.updateTimestamps(drained.minTs, drained.maxTs);
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
