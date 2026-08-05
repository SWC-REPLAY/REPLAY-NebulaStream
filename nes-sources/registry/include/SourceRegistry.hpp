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

#include <memory>
#include <string>

#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Pointers.hpp>
#include <Util/Registry.hpp>

namespace NES::StoreManager
{
class StoreRegistry;
}

namespace NES
{

using SourceRegistryReturnType = std::unique_ptr<Source>;

struct SourceRegistryArguments
{
    SourceDescriptor sourceDescriptor;
    /// The store registry of the worker this source runs on, borrowed and outliving the source. A replay source resolves
    /// its store from it; every other source ignores it, and it is empty on a worker without replay stores.
    OptionalRef<StoreManager::StoreRegistry> storeRegistry;
};

class SourceRegistry : public BaseRegistry<SourceRegistry, std::string, SourceRegistryReturnType, SourceRegistryArguments>
{
};

}

#define INCLUDED_FROM_SOURCE_REGISTRY
#include <SourceGeneratedRegistrar.inc>
#undef INCLUDED_FROM_SOURCE_REGISTRY
