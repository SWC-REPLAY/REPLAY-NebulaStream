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
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include <Store.hpp>

namespace NES::StoreManager
{

/// Manages named store instances, allowing concurrent TIME_TRAVEL queries to each use their own store.
class StoreRegistry
{
public:
    static StoreRegistry& instance();

    /// Register a store under a name.
    void registerStore(const std::string& storeName, Store store);

    /// Register a default hierarchical store (MemoryStore -> FileStore).
    /// Returns the generated file path for backward compatibility.
    std::string registerDefaultStore(const std::string& storeName, const Schema& schema, const std::string& schemaText);

    /// Look up the store for a given name.
    [[nodiscard]] std::optional<Store> getStore(const std::string& storeName) const;

    /// Look up the file path for a given store name (backward compat).
    /// Traverses the store hierarchy to find a FileStore.
    [[nodiscard]] std::optional<std::string> getFilePath(const std::string& storeName) const;

    /// Get the file path of the most recently registered store.
    [[nodiscard]] std::optional<std::string> getLatestStorePath() const;

    /// Remove a store registration.
    void unregisterStore(const std::string& storeName);

    /// Clear all registrations.
    void clear();

    /// Close and delete all registered store files from disk and clear the registry.
    void clearAndDeleteFiles();

private:
    StoreRegistry() = default;

    /// Generate a unique file path for a store.
    static std::string generateFilePath(const std::string& storeName);

    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, Store> stores;
    std::unordered_map<std::string, std::string> filePaths; // backward compat: store name -> file path
    std::string latestStoreId;
};

} // namespace NES::StoreManager
