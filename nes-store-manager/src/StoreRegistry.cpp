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

#include <algorithm>
#include <filesystem>
#include <format>
#include <mutex>
#include <optional>
#include <ranges>
#include <shared_mutex>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FileStore.hpp>
#include <FlushPolicy.hpp>
#include <MemoryStore.hpp>
#include <Store.hpp>
#include <StoreTransformationRegistry.hpp>

namespace NES::StoreManager
{

StoreRegistry::StoreRegistry() : bufferManager(BufferManager::create())
{
}

StoreRegistry& StoreRegistry::instance()
{
    static StoreRegistry registry;
    return registry;
}

void StoreRegistry::registerStore(const std::string& storeName, Store store)
{
    const std::unique_lock lock(mutex);
    stores.emplace(storeName, std::move(store));
}

void StoreRegistry::registerDefaultStore(const std::string& storeName, const Schema& schema, const std::string& schemaText)
{
    registerConfiguredStore(storeName, schema, schemaText, StoreConfig{});
}

void StoreRegistry::registerConfiguredStore(
    const std::string& storeName, const Schema& schema, const std::string& schemaText, const StoreConfig& config)
{
    const std::unique_lock lock(mutex);
    NES_DEBUG("Registering configured store with name {} and schema {}", storeName, schemaText);

    const auto storeDir = generateStoreDir(storeName);
    const auto storeOrder = config.storeOrder.value_or("MemoryStore->FileStore");

    /// Parse the store order string by splitting on "->"
    std::vector<std::string> storeNames;
    {
        std::string remaining = storeOrder;
        while (true)
        {
            auto pos = remaining.find("->");
            if (pos == std::string::npos)
            {
                storeNames.push_back(remaining);
                break;
            }
            storeNames.push_back(remaining.substr(0, pos));
            remaining = remaining.substr(pos + 2);
        }
    }

    PRECONDITION(!storeNames.empty(), "Store order must not be empty");
    for (const auto& name : storeNames)
    {
        PRECONDITION(name == "MemoryStore" || name == "FileStore", "Unknown store type '{}' in store order '{}'", name, storeOrder);
    }

    /// Build the store chain bottom-up (last in the order is the tail).
    /// Supported chains: "MemoryStore->FileStore", "MemoryStore", "FileStore"
    const bool hasMemoryStore = std::ranges::find(storeNames, "MemoryStore") != storeNames.end();
    const bool hasFileStore = std::ranges::find(storeNames, "FileStore") != storeNames.end();

    const auto fileTotalSize = config.fileTotalSize.value_or(FileStore::Config{}.totalSize);
    const auto fileSegmentSize = config.fileSegmentSize.value_or(FileStore::Config{}.segmentSize);

    if (hasMemoryStore && hasFileStore)
    {
        auto transformation = StoreTransformationRegistry::instance().findTransformation("MemoryStore", "FileStore");
        PRECONDITION(transformation.has_value(), "No transformation registered for 'MemoryStore' -> 'FileStore'");

        auto fileStore = makeStore<FileStore>(
            FileStore::Config{
                .storeName = storeName,
                .storeDir = storeDir,
                .schemaText = schemaText,
                .totalSize = fileTotalSize,
                .segmentSize = fileSegmentSize},
            schema);

        const auto bufferSize = config.memoryBufferSize.value_or(MemoryStore::Config{}.maxBufferSize);
        const FlushPolicy policy{.type = FlushPolicy::Type::SIZE_THRESHOLD, .sizeThreshold = bufferSize};

        auto headStore
            = makeStore<MemoryStore>(schema, MemoryStore::Config{.maxBufferSize = bufferSize}, bufferManager, std::move(fileStore), policy);

        stores.emplace(storeName, headStore);
    }
    else if (hasMemoryStore)
    {
        const auto bufferSize = config.memoryBufferSize.value_or(MemoryStore::Config{}.maxBufferSize);
        auto headStore = makeStore<MemoryStore>(schema, MemoryStore::Config{.maxBufferSize = bufferSize}, bufferManager);
        stores.emplace(storeName, headStore);
    }
    else if (hasFileStore)
    {
        auto headStore = makeStore<FileStore>(
            FileStore::Config{
                .storeName = storeName,
                .storeDir = storeDir,
                .schemaText = schemaText,
                .totalSize = fileTotalSize,
                .segmentSize = fileSegmentSize},
            schema);
        stores.emplace(storeName, headStore);
    }
}

std::optional<Store> StoreRegistry::getStore(const std::string& storeName) const
{
    const std::shared_lock lock(mutex);
    NES_DEBUG("Getting store with name {}", storeName);
    auto it = stores.find(storeName);
    if (it != stores.end())
    {
        return it->second;
    }
    return std::nullopt;
}

void StoreRegistry::unregisterStore(const std::string& storeName)
{
    const std::unique_lock lock(mutex);
    NES_DEBUG("Unregistering store with the id {}", storeName);
    stores.erase(storeName);
}

void StoreRegistry::clear()
{
    const std::unique_lock lock(mutex);
    stores.clear();
}

void StoreRegistry::clearAndDeleteFiles()
{
    const std::unique_lock lock(mutex);
    for (auto& [name, store] : stores)
    {
        if (auto fileStore = store.tryGetAs<FileStore>())
        {
            fileStore->getMutable().removeFile();
        }
        store.close();
    }
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
}

std::string StoreRegistry::generateStoreDir(const std::string& storeName)
{
    const std::string storeDir = std::format("{}{}", STORE_MANAGER_WORKING_DIR, storeName);
    std::error_code err;
    std::filesystem::create_directories(storeDir, err);
    if (err)
    {
        throw StoreManagerInitFailure("Could not create " + storeName + " directory: " + err.message());
    }
    return storeDir;
}

}
