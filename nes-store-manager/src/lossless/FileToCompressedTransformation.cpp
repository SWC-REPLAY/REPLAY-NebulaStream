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

#include <lossless/FileToCompressedTransformation.hpp>

#include <Util/Logger/Logger.hpp>
#include <lossless/LosslessStore.hpp>
#include <FileStore.hpp>
#include <Store.hpp>
#include <StoreTransformationRegistry.hpp>

namespace NES
{

void FileToCompressedTransformation::execute(Store& source, Store& dest)
{
    // Drain the FileStore (empties the source, moving its data down a tier).
    auto typedSource = source.getAs<FileStore>();
    auto& fileStore = typedSource.getMutable();
    auto segments = fileStore.drainSegments();

    if (segments.empty())
    {
        return; // nothing to move
    }

    // Hand each segment down with its range; the LosslessStore accumulates them into its own blocks.
    auto typedDest = dest.getAs<LosslessStore>();
    auto& compressedStore = typedDest.getMutable();
    for (const auto& segment : segments)
    {
        if (segment.bytes.empty())
        {
            continue;
        }
        NES_DEBUG("FileToCompressedTransformation: moving a {}-byte segment from FileStore", segment.bytes.size());
        compressedStore.appendSegment(
            segment.bytes.data(), segment.bytes.size(), segment.rowCount, segment.minTs, segment.maxTs);
    }
}

}

namespace NES
{
/// NOLINTNEXTLINE(performance-unnecessary-value-param)
StoreTransformationRegistryReturnType
StoreTransformationGeneratedRegistrar::RegisterFileStore_to_LosslessStoreStoreTransformation(StoreTransformationRegistryArguments)
{
    return FileToCompressedTransformation{};
}
}
