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

#include <ReplayStoreRegistry.hpp>

#include <filesystem>

namespace NES::Replay
{

ReplayStoreRegistry& ReplayStoreRegistry::instance()
{
    static ReplayStoreRegistry registry;
    return registry;
}

std::string ReplayStoreRegistry::registerStore(const std::string& storeId, const std::string& baseDir)
{
    std::unique_lock lock(mutex);

    std::filesystem::create_directories(baseDir);
    std::string filePath = baseDir + "/replay_" + storeId + ".bin";

    stores[storeId] = filePath;
    latestStoreId = storeId;
    return filePath;
}

std::optional<std::string> ReplayStoreRegistry::getFilePath(const std::string& storeId) const
{
    std::shared_lock lock(mutex);
    auto it = stores.find(storeId);
    if (it != stores.end())
    {
        return it->second;
    }
    return std::nullopt;
}

std::optional<std::string> ReplayStoreRegistry::getLatestStorePath() const
{
    std::shared_lock lock(mutex);
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

void ReplayStoreRegistry::unregisterStore(const std::string& storeId)
{
    std::unique_lock lock(mutex);
    stores.erase(storeId);
    if (latestStoreId == storeId)
    {
        latestStoreId.clear();
    }
}

void ReplayStoreRegistry::clear()
{
    std::unique_lock lock(mutex);
    stores.clear();
    latestStoreId.clear();
}

} // namespace NES::Replay