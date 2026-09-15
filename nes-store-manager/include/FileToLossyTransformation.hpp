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

#pragma once

#include <string_view>

#include <Store.hpp>

namespace NES::StoreManager
{

/// Moves a FileStore's rows down into a LossyStore.
///
/// Same shape as the other transformations: drain the source (which empties it, so the data
/// has genuinely moved a tier down) and hand the raw row-major bytes to the destination.
/// The destination decides how to group and encode them.
struct FileToLossyTransformation
{
    void execute(Store& source, Store& dest);

    [[nodiscard]] static std::string_view name() noexcept { return "FileStore_to_LossyStore"; }
};

}
