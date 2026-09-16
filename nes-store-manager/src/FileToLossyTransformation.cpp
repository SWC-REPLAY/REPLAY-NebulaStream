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

#include <FileToLossyTransformation.hpp>

#include <Util/Logger/Logger.hpp>
#include <FileStore.hpp>
#include <LossyStore.hpp>
#include <Store.hpp>
#include <StoreTransformationRegistry.hpp>

namespace NES
{

void FileToLossyTransformation::execute(Store& source, Store& dest)
{
    auto typedSource = source.getAs<FileStore>();
    auto& fileStore = typedSource.getMutable();
    auto segments = fileStore.drainSegments();

    if (segments.empty())
    {
        return;
    }

    auto typedDest = dest.getAs<LossyStore>();
    auto& lossyStore = typedDest.getMutable();

    /// One segment becomes one group, carrying the range that arrived with it. Nothing is
    /// re-divided here and no timestamp is read back out of the row bytes: the tier above knows
    /// both the boundaries and the ranges already, so both are simply handed over.
    for (const auto& segment : segments)
    {
        if (segment.bytes.empty())
        {
            continue;
        }
        NES_DEBUG(
            "FileToLossyTransformation: moving a {}-row segment from FileStore", segment.rowCount);
        lossyStore.appendSegment(segment.bytes.data(), segment.bytes.size(), segment.rowCount, segment.minTs, segment.maxTs);
    }
}

}

namespace NES
{
/// NOLINTNEXTLINE(performance-unnecessary-value-param)
StoreTransformationRegistryReturnType
StoreTransformationGeneratedRegistrar::RegisterFileStore_to_LossyStoreStoreTransformation(StoreTransformationRegistryArguments)
{
    return FileToLossyTransformation{};
}
}
