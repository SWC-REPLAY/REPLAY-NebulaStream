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

#include <cstddef>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Store.hpp>

namespace NES::StoreManager
{

/// Per-query store configuration. All fields default to nullopt, meaning "use hardcoded default".
struct StoreConfig
{
    std::optional<size_t> memoryBufferSize;
    std::optional<size_t> maxBufferCount;
    std::optional<std::string> storeOrder;
};

/// Holds the live store instances a worker has materialised, keyed by the names the StoreCatalog assigned.
///
/// Owned by the worker rather than process-global: a store is a physical thing that lives where its operator was
/// placed, so in a deployment with several workers each of them holds its own. A worker also materialises a store per
/// store operator, which is what lets one query record more than one cut of its plan.
class StoreRegistry
{
public:
    StoreRegistry();

    /// Register a store under a name.
    void registerStore(const std::string& storeName, Store store);

    /// Register a default hierarchical store (MemoryStore -> FileStore).
    void registerDefaultStore(const std::string& storeName, const Schema& schema, const std::string& schemaText);

    /// Register a store with custom configuration (store order, buffer sizes, etc.).
    void
    registerConfiguredStore(const std::string& storeName, const Schema& schema, const std::string& schemaText, const StoreConfig& config);

    /// Look up the store for a given name.
    [[nodiscard]] std::optional<Store> getStore(const std::string& storeName) const;

    /// Remove a store registration.
    void unregisterStore(const std::string& storeName);

    /// Clear all registrations.
    void clear();

    /// Close and delete all registered store files from disk and clear the registry.
    void clearAndDeleteFiles();

private:
    /// Generate a unique file path for a store.
    static std::string generateStoreDir(const std::string& storeName);

    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, Store> stores; /// store name -> Store instance
    std::shared_ptr<BufferManager> bufferManager;
};

}
