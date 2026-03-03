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

#include <StoreRegistry.hpp>

#include <filesystem>
#include <string>
#include <mutex>
#include <optional>
#include <shared_mutex>

namespace NES::StoreManager
{

StoreRegistry& StoreRegistry::instance()
{
    static StoreRegistry registry;
    return registry;
}

std::string StoreRegistry::registerStore(const std::string& storeId)
{
    std::unique_lock const lock(mutex);

    std::filesystem::create_directories(STORE_MANAGER_WORKING_DIR);
    std::string filePath = std::string(STORE_MANAGER_WORKING_DIR) + "/replay_" + storeId + ".bin";

    stores[storeId] = filePath;
    latestStoreId = storeId;
    return filePath;
}

std::optional<std::string> StoreRegistry::getFilePath(const std::string& storeId) const
{
    std::shared_lock const lock(mutex);
    auto it = stores.find(storeId);
    if (it != stores.end())
    {
        return it->second;
    }
    return std::nullopt;
}

std::optional<std::string> StoreRegistry::getLatestStorePath() const
{
    std::shared_lock const lock(mutex);
    if (latestStoreId.empty())
    {
        return std::nullopt;
    }
    auto it = stores.find(latestStoreId);
    if (it != stores.end())
    {
        return it->second;
    }
    return std::nullopt;
}

void StoreRegistry::unregisterStore(const std::string& storeId)
{
    std::unique_lock const lock(mutex);
    stores.erase(storeId);
    if (latestStoreId == storeId)
    {
        latestStoreId.clear();
    }
}

void StoreRegistry::clear()
{
    std::unique_lock const lock(mutex);
    stores.clear();
    latestStoreId.clear();
}

} // namespace NES::Replay
