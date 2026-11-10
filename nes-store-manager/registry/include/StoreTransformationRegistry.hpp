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

#include <optional>
#include <string>
#include <string_view>

#include <Util/RuntimeRegistry.hpp>
#include <StoreTransformation.hpp>

namespace NES
{

/// Maps "<sourceType>_to_<destType>" to the transformation moving data between those store types.
class StoreTransformationRegistry : public RuntimeRegistry<StoreTransformationRegistry, std::string, StoreManager::StoreTransformation>
{
public:
    static StoreTransformationRegistry& instance();

    [[nodiscard]] std::optional<StoreManager::StoreTransformation>
    findTransformation(const std::string_view sourceTypeName, const std::string_view destTypeName) const
    {
        return find(std::string(sourceTypeName) + "_to_" + std::string(destTypeName));
    }
};

}
