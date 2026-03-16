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

#include <chrono>
#include <filesystem>
#include <iomanip>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <ErrorHandling.hpp>
#include <FileStore.hpp>
#include <HierarchicalStore.hpp>
#include <MemoryStore.hpp>
#include <StoreTransformationRegistry.hpp>

namespace NES::StoreManager
{

StoreRegistry& StoreRegistry::instance()
{
    static StoreRegistry registry;
    return registry;
}

void StoreRegistry::registerStore(const std::string& storeName, Store store)
{
    const std::unique_lock lock(mutex);
    stores.emplace(storeName, std::move(store));
    latestStoreId = storeName;
}

std::string StoreRegistry::registerDefaultStore(const std::string& storeName, const Schema& schema, const std::string& schemaText)
{
    const std::unique_lock lock(mutex);

    auto filePath = generateFilePath(storeName);

    // Create a HierarchicalStore: MemoryStore -> FileStore
    // Look up the transformation at construction time to validate it exists
    auto transformation = StoreTransformationRegistry::instance().create("MemoryStore_To_FileStore", StoreTransformationRegistryArguments{});
    PRECONDITION(transformation.has_value(), "No transformation registered for 'MemoryStore_To_FileStore'");

    std::vector<StoreLevel> levels;
    levels.push_back(StoreLevel{.store = makeStore<MemoryStore>(schema), .policy = FlushPolicy{.type = FlushPolicy::Type::SIZE_THRESHOLD}, .transformation = std::move(transformation)});
    levels.push_back(StoreLevel{.store = makeStore<FileStore>(FileStore::Config{.storeName = storeName, .filePath = filePath, .schemaText = schemaText}, schema),
        .policy = FlushPolicy{}, .transformation = std::nullopt});

    stores.emplace(storeName, makeStore<HierarchicalStore>(std::move(levels), schema));
    filePaths[storeName] = filePath;
    latestStoreId = storeName;
    return filePath;
}

std::optional<Store> StoreRegistry::getStore(const std::string& storeName) const
{
    const std::shared_lock lock(mutex);
    auto it = stores.find(storeName);
    if (it != stores.end())
    {
        return it->second;
    }
    return std::nullopt;
}

std::optional<std::string> StoreRegistry::getFilePath(const std::string& storeName) const
{
    const std::shared_lock lock(mutex);
    auto it = filePaths.find(storeName);
    if (it != filePaths.end())
    {
        return it->second;
    }
    // Fallback: check if the store is a FileStore directly
    auto storeIt = stores.find(storeName);
    if (storeIt != stores.end())
    {
        if (auto fileStore = storeIt->second.tryGetAs<FileStore>())
        {
            return fileStore->get().getFilePath();
        }
    }
    return std::nullopt;
}

std::optional<std::string> StoreRegistry::getLatestStorePath() const
{
    const std::shared_lock lock(mutex);
    if (latestStoreId.empty())
    {
        return std::nullopt;
    }
    auto it = filePaths.find(latestStoreId);
    if (it != filePaths.end())
    {
        return it->second;
    }
    return std::nullopt;
}

void StoreRegistry::unregisterStore(const std::string& storeId)
{
    const std::unique_lock lock(mutex);
    stores.erase(storeId);
    if (latestStoreId == storeId)
    {
        latestStoreId.clear();
    }
}

void StoreRegistry::clear()
{
    const std::unique_lock lock(mutex);
    stores.clear();
    filePaths.clear();
    latestStoreId.clear();
}

void StoreRegistry::clearAndDeleteFiles()
{
    const std::unique_lock lock(mutex);
    for (auto& [name, store] : stores)
    {
        // Try to find and remove file stores
        if (auto fileStore = store.tryGetAs<FileStore>())
        {
            fileStore->getMutable().removeFile();
        }
        // For hierarchical stores, the file store is inside
        // Close the store which will flush and clean up
        store.close();
    }
    // Also remove any files matching our pattern in the working directory
    if (std::filesystem::exists(STORE_MANAGER_WORKING_DIR))
    {
        for (const auto& entry : std::filesystem::directory_iterator(STORE_MANAGER_WORKING_DIR))
        {
            if (entry.path().extension() == ".bin")
            {
                std::filesystem::remove(entry.path());
            }
        }
    }
    stores.clear();
    filePaths.clear();
    latestStoreId.clear();
}

std::string StoreRegistry::generateFilePath(const std::string& storeName)
{
    std::filesystem::create_directories(STORE_MANAGER_WORKING_DIR);

    const auto now = std::chrono::system_clock::now();
    const auto timeT = std::chrono::system_clock::to_time_t(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::ostringstream ts;
    ts << std::put_time(std::localtime(&timeT), "%Y%m%d_%H%M%S") << '_' << std::setfill('0') << std::setw(3) << ms.count();

    return std::string(STORE_MANAGER_WORKING_DIR) + "/replay_" + storeName + "_" + ts.str() + ".bin";
}

} // namespace NES::StoreManager
