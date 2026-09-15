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
#include <cctype>
#include <filesystem>
#include <format>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <shared_mutex>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <CompressedStore.hpp>
#include <ErrorHandling.hpp>
#include <FileStore.hpp>
#include <FlushPolicy.hpp>
#include <LossyStore.hpp>
#include <MemoryStore.hpp>
#include <Store.hpp>
#include <StoreTransformationRegistry.hpp>

namespace NES
{

namespace
{
/// Split a store order like "MemoryStore->FileStore" into its links, rejecting anything that does not name a supported
/// store type.
///
/// This throws rather than asserting because the value is user input either way: it comes from a query's SET(...) or
/// from the worker's replay configuration. Aborting the worker on an assertion would take down every other query with
/// it, so a bad chain has to fail just the query that asked for it.
std::vector<std::string> parseStoreOrder(const std::string& storeOrder)
{
    if (storeOrder.empty())
    {
        throw InvalidConfigParameter("Store order must not be empty");
    }

    std::vector<std::string> storeNames;
    std::string remaining = storeOrder;
    while (true)
    {
        const auto pos = remaining.find("->");
        if (pos == std::string::npos)
        {
            storeNames.push_back(remaining);
            break;
        }
        storeNames.push_back(remaining.substr(0, pos));
        remaining = remaining.substr(pos + 2);
    }

    for (const auto& name : storeNames)
    {
        if (name != "MemoryStore" && name != "FileStore" && name != "CompressedStore" && name != "LossyStore")
        {
            throw InvalidConfigParameter(
                "Unknown store type '{}' in store order '{}'; expected MemoryStore, FileStore, CompressedStore or LossyStore, chained with '->'",
                name,
                storeOrder);
        }
    }
    return storeNames;
}

/// Per field: what the query asked for, else what the worker configured, else nothing (the store type decides).
StoreConfig mergeOverDefaults(const StoreConfig& overrides, const StoreConfig& defaults)
{
    return StoreConfig{
        .memoryBufferSize = overrides.memoryBufferSize.has_value() ? overrides.memoryBufferSize : defaults.memoryBufferSize,
        .maxBufferCount = overrides.maxBufferCount.has_value() ? overrides.maxBufferCount : defaults.maxBufferCount,
        .storeOrder = overrides.storeOrder.has_value() ? overrides.storeOrder : defaults.storeOrder,
        .accuracyBoundPercent
        = overrides.accuracyBoundPercent.has_value() ? overrides.accuracyBoundPercent : defaults.accuracyBoundPercent};
}
}

StoreRegistry::StoreRegistry(StoreConfig defaults) : defaults(std::move(defaults))
{
}

void StoreRegistry::registerStore(const std::string& storeName, Store store)
{
    const std::unique_lock lock(mutex);
    stores.emplace(storeName, std::move(store));
}

Store StoreRegistry::getOrCreateStore(
    const std::string& storeName,
    const Schema& schema,
    const std::string& schemaText,
    const StoreConfig& overrides,
    const std::shared_ptr<AbstractBufferProvider>& bufferProvider)
{
    const std::unique_lock lock(mutex);
    if (const auto existing = stores.find(storeName); existing != stores.end())
    {
        return existing->second;
    }

    NES_DEBUG("Materialising store with name {} and schema {}", storeName, schemaText);

    const auto config = mergeOverDefaults(overrides, defaults);
    const auto storeOrder = config.storeOrder.value_or("MemoryStore->FileStore");
    const auto storeNames = parseStoreOrder(storeOrder);

    const auto storeDir = generateStoreDir(storeName);

    /// Build the store chain bottom-up (last in the order is the tail).
    /// Supported chains: "MemoryStore->FileStore->CompressedStore", "MemoryStore->FileStore",
    /// "FileStore->CompressedStore", "MemoryStore->FileStore->LossyStore", "FileStore->LossyStore",
    /// "MemoryStore", "FileStore", "CompressedStore", "LossyStore"
    const bool hasMemoryStore = std::ranges::find(storeNames, "MemoryStore") != storeNames.end();
    const bool hasFileStore = std::ranges::find(storeNames, "FileStore") != storeNames.end();
    const bool hasCompressedStore = std::ranges::find(storeNames, "CompressedStore") != storeNames.end();
    const bool hasLossyStore = std::ranges::find(storeNames, "LossyStore") != storeNames.end();

    /// The lossy tier columnarises row-major bytes handed to it by a FileStore. There is no
    /// CompressedStore -> LossyStore transformation, so reject the combination rather than
    /// silently dropping one of the two tiers.
    PRECONDITION(
        !(hasCompressedStore && hasLossyStore),
        "CompressedStore and LossyStore cannot appear in the same store order '{}'",
        storeOrder);

    /// A group is one segment handed down by the FileStore, carrying the range measured where
    /// the timestamps were actually known. Nothing here has to name a timestamp column.
    const LossyStore::Config lossyConfig{
        .storeName = storeName,
        .storeDir = storeDir,
        .schemaText = schemaText,
        /// Unset means lossless, which is what every chain gets unless a budget was asked for.
        .accuracyBoundPercent = config.accuracyBoundPercent.value_or(0.0)};

    if (hasMemoryStore && hasFileStore && hasCompressedStore)
    {
        auto memoryToFile = StoreTransformationRegistry::instance().findTransformation("MemoryStore", "FileStore");
        PRECONDITION(memoryToFile.has_value(), "No transformation registered for 'MemoryStore' -> 'FileStore'");
        auto fileToCompressed = StoreTransformationRegistry::instance().findTransformation("FileStore", "CompressedStore");
        PRECONDITION(fileToCompressed.has_value(), "No transformation registered for 'FileStore' -> 'CompressedStore'");

        auto compressedStore = makeStore<CompressedStore>(
            CompressedStore::Config{.storeName = storeName, .storeDir = storeDir, .schemaText = schemaText}, schema);

        const auto bufferSize = config.memoryBufferSize.value_or(MemoryStore::Config{}.maxBufferSize);
        const auto bufferCount = config.maxBufferCount.value_or(MemoryStore::Config{}.maxBufferCount);
        const FlushPolicy policy{.type = FlushPolicy::Type::SIZE_THRESHOLD, .sizeThreshold = bufferSize};

        auto fileStore = makeStore<FileStore>(
            FileStore::Config{
                .storeName = storeName,
                .storeDir = storeDir,
                .schemaText = schemaText,
                /// The segment table is preallocated from this, and the flush policy
                /// drains the file at the same figure -- so the slots always suffice.
                .totalSize = bufferSize},
            schema,
            std::move(compressedStore),
            *fileToCompressed,
            policy);

        auto headStore = makeStore<MemoryStore>(
            schema,
            MemoryStore::Config{.maxBufferSize = bufferSize, .maxBufferCount = bufferCount},
            bufferProvider,
            std::move(fileStore),
            policy);

        return stores.emplace(storeName, headStore).first->second;
    }
    if (hasMemoryStore && hasFileStore && hasLossyStore)
    {
        auto memoryToFile = StoreTransformationRegistry::instance().findTransformation("MemoryStore", "FileStore");
        PRECONDITION(memoryToFile.has_value(), "No transformation registered for 'MemoryStore' -> 'FileStore'");
        auto fileToLossy = StoreTransformationRegistry::instance().findTransformation("FileStore", "LossyStore");
        PRECONDITION(fileToLossy.has_value(), "No transformation registered for 'FileStore' -> 'LossyStore'");

        auto lossyStore = makeStore<LossyStore>(lossyConfig, schema);

        const auto bufferSize = config.memoryBufferSize.value_or(MemoryStore::Config{}.maxBufferSize);
        const auto bufferCount = config.maxBufferCount.value_or(MemoryStore::Config{}.maxBufferCount);
        const FlushPolicy policy{.type = FlushPolicy::Type::SIZE_THRESHOLD, .sizeThreshold = bufferSize};

        auto fileStore = makeStore<FileStore>(
            FileStore::Config{
                .storeName = storeName,
                .storeDir = storeDir,
                .schemaText = schemaText,
                /// The segment table is preallocated from this, and the flush policy
                /// drains the file at the same figure -- so the slots always suffice.
                .totalSize = bufferSize},
            schema,
            std::move(lossyStore),
            *fileToLossy,
            policy);

        auto headStore = makeStore<MemoryStore>(
            schema,
            MemoryStore::Config{.maxBufferSize = bufferSize, .maxBufferCount = bufferCount},
            bufferManager,
            std::move(fileStore),
            policy);

        return stores.emplace(storeName, headStore).first->second;
    }
    if (hasMemoryStore && hasFileStore)
    {
        auto transformation = StoreTransformationRegistry::instance().findTransformation("MemoryStore", "FileStore");
        PRECONDITION(transformation.has_value(), "No transformation registered for 'MemoryStore' -> 'FileStore'");

        auto fileStore
            = makeStore<FileStore>(FileStore::Config{.storeName = storeName, .storeDir = storeDir, .schemaText = schemaText}, schema);

        const auto bufferSize = config.memoryBufferSize.value_or(MemoryStore::Config{}.maxBufferSize);
        const auto bufferCount = config.maxBufferCount.value_or(MemoryStore::Config{}.maxBufferCount);
        const FlushPolicy policy{.type = FlushPolicy::Type::SIZE_THRESHOLD, .sizeThreshold = bufferSize};

        auto headStore = makeStore<MemoryStore>(
            schema,
            MemoryStore::Config{.maxBufferSize = bufferSize, .maxBufferCount = bufferCount},
            bufferProvider,
            std::move(fileStore),
            policy);

        return stores.emplace(storeName, headStore).first->second;
    }
    if (hasFileStore && hasCompressedStore)
    {
        auto transformation = StoreTransformationRegistry::instance().findTransformation("FileStore", "CompressedStore");
        PRECONDITION(transformation.has_value(), "No transformation registered for 'FileStore' -> 'CompressedStore'");

        auto compressedStore = makeStore<CompressedStore>(
            CompressedStore::Config{.storeName = storeName, .storeDir = storeDir, .schemaText = schemaText}, schema);

        const auto bufferSize = config.memoryBufferSize.value_or(MemoryStore::Config{}.maxBufferSize);
        const FlushPolicy policy{.type = FlushPolicy::Type::SIZE_THRESHOLD, .sizeThreshold = bufferSize};

        auto headStore = makeStore<FileStore>(
            FileStore::Config{
                .storeName = storeName,
                .storeDir = storeDir,
                .schemaText = schemaText,
                /// The segment table is preallocated from this, and the flush policy
                /// drains the file at the same figure -- so the slots always suffice.
                .totalSize = bufferSize},
            schema,
            std::move(compressedStore),
            *transformation,
            policy);

        return stores.emplace(storeName, headStore).first->second;
    }
    if (hasFileStore && hasLossyStore)
    {
        auto transformation = StoreTransformationRegistry::instance().findTransformation("FileStore", "LossyStore");
        PRECONDITION(transformation.has_value(), "No transformation registered for 'FileStore' -> 'LossyStore'");

        auto lossyStore = makeStore<LossyStore>(lossyConfig, schema);

        const auto bufferSize = config.memoryBufferSize.value_or(MemoryStore::Config{}.maxBufferSize);
        const FlushPolicy policy{.type = FlushPolicy::Type::SIZE_THRESHOLD, .sizeThreshold = bufferSize};

        auto headStore = makeStore<FileStore>(
            FileStore::Config{
                .storeName = storeName,
                .storeDir = storeDir,
                .schemaText = schemaText,
                /// The segment table is preallocated from this, and the flush policy
                /// drains the file at the same figure -- so the slots always suffice.
                .totalSize = bufferSize},
            schema,
            std::move(lossyStore),
            *transformation,
            policy);

        return stores.emplace(storeName, headStore).first->second;
    }
    if (hasMemoryStore)
    {
        const auto bufferSize = config.memoryBufferSize.value_or(MemoryStore::Config{}.maxBufferSize);
        const auto bufferCount = config.maxBufferCount.value_or(MemoryStore::Config{}.maxBufferCount);
        auto headStore = makeStore<MemoryStore>(
            schema, MemoryStore::Config{.maxBufferSize = bufferSize, .maxBufferCount = bufferCount}, bufferProvider);
        return stores.emplace(storeName, headStore).first->second;
    }
    if (hasCompressedStore)
    {
        auto headStore = makeStore<CompressedStore>(
            CompressedStore::Config{.storeName = storeName, .storeDir = storeDir, .schemaText = schemaText}, schema);
        return stores.emplace(storeName, headStore).first->second;
    }
    if (hasLossyStore)
    {
        auto headStore = makeStore<LossyStore>(lossyConfig, schema);
        return stores.emplace(storeName, headStore).first->second;
    }
    auto headStore
        = makeStore<FileStore>(FileStore::Config{.storeName = storeName, .storeDir = storeDir, .schemaText = schemaText}, schema);
    return stores.emplace(storeName, headStore).first->second;
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

void StoreRegistry::closeAll()
{
    const std::unique_lock lock(mutex);
    for (auto& [name, store] : stores)
    {
        store.close();
    }
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

namespace
{
/// Percent-encodes everything outside `[A-Za-z0-9_-]` so a store name can be used as a directory name.
///
/// The encoding is reversible on purpose. A lossy substitution — mapping every awkward character to '_' — would let two
/// distinct store names collapse onto one directory, and two stores sharing a directory would silently read and write
/// each other's rows.
std::string encodeForPath(const std::string& storeName)
{
    std::string encoded;
    encoded.reserve(storeName.size());
    for (const auto character : storeName)
    {
        if (std::isalnum(static_cast<unsigned char>(character)) != 0 || character == '_' || character == '-')
        {
            encoded.push_back(character);
        }
        else
        {
            encoded += std::format("%{:02x}", static_cast<unsigned char>(character));
        }
    }
    return encoded;
}
}

std::string StoreRegistry::generateStoreDir(const std::string& storeName)
{
    const std::string storeDir = std::format("{}{}", STORE_MANAGER_WORKING_DIR, encodeForPath(storeName));
    std::error_code err;
    std::filesystem::create_directories(storeDir, err);
    if (err)
    {
        throw StoreManagerInitFailure("Could not create " + storeName + " directory: " + err.message());
    }
    return storeDir;
}

}
