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
#include <Runtime/AbstractBufferProvider.hpp>
#include <Store.hpp>

namespace NES
{

/// Store configuration. Every field is optional; an unset field falls back to the registry's worker-level defaults, and
/// where those leave it unset too, to the store type's own default.
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
///
/// The registry also holds that worker's replay defaults. They are worker-scoped exactly like the registry itself, so
/// keeping them here spares everything in between from carrying them alongside it.
class StoreRegistry
{
public:
    /// `defaults` is the worker's replay configuration; a query that configures its own store overrides it per field.
    explicit StoreRegistry(StoreConfig defaults);

    /// Register a store under a name.
    void registerStore(const std::string& storeName, Store store);

    /// Get the store, materialising it on first call. Unset fields in `overrides` fall back to the worker defaults.
    /// Idempotent, so concurrent pipeline starts share one instance. `bufferProvider` comes from the caller so stores
    /// draw from the engine's pool rather than one this registry allocates.
    /// @throws InvalidConfigParameter on an unsupported store order.
    Store getOrCreateStore(
        const std::string& storeName,
        const Schema& schema,
        const std::string& schemaText,
        const StoreConfig& overrides,
        const std::shared_ptr<AbstractBufferProvider>& bufferProvider);

    /// Look up the store for a given name.
    [[nodiscard]] std::optional<Store> getStore(const std::string& storeName) const;

    /// Remove a store registration.
    void unregisterStore(const std::string& storeName);

    /// Clear all registrations.
    void clear();

    /// Flush every store to its backing level and drop it. Must run before the buffer provider the stores were
    /// materialised with is torn down, since a store holds buffers from it.
    void closeAll();

    /// Close and delete all registered store files from disk and clear the registry.
    void clearAndDeleteFiles();

private:
    /// Generate a unique file path for a store.
    static std::string generateStoreDir(const std::string& storeName);

    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, Store> stores; /// store name -> Store instance
    /// This worker's replay configuration, used for every parameter a query leaves unset.
    StoreConfig defaults;
};

}
