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

#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>

namespace NES
{

class ReplayConfiguration final : public BaseConfiguration
{
public:
    ReplayConfiguration() = default;
    ReplayConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { }

    /// Maximum size (in bytes) of a single memory buffer. Default: 64 MB.
    UIntOption memoryBufferSize
        = {"memory_buffer_size", "67108864", "Maximum size in bytes of a single memory buffer (default 64 MB)"};

    /// Maximum number of sealed buffers before wraparound evicts the oldest. Default: 128.
    UIntOption maxBufferCount = {"max_buffer_count", "128", "Maximum sealed buffers before wraparound evicts oldest"};

    /// Store chain order, e.g. "MemoryStore->FileStore". Default: "MemoryStore->FileStore".
    ScalarOption<std::string> storeOrder
        = {"store_order", "MemoryStore->FileStore", "Store chain order, e.g. MemoryStore->FileStore"};

private:
    std::vector<BaseOption*> getOptions() override;
};

}