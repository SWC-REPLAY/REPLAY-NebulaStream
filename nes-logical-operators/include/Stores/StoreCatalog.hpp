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

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryId.hpp>

namespace NES
{

/// Which notion of time a store's records were stamped with. Recorded so a reader asking for one kind of time is never
/// served a store that recorded another, which would return plausible but wrong rows. Switches on this type are written
/// without a `default` so a new enumerator makes the compiler point at every site that needs a decision.
enum class StoreTimeType : uint8_t
{
    /// Stamped from a value carried by the record itself.
    EventTime,
    /// Stamped from when the record was ingested, independent of its contents.
    IngestionTime,
};

/// Metadata describing one replay store, not the store itself: the instance is created per worker during lowering. That
/// separation is what lets a reader query be bound before the writer query has ever run.
struct StoreEntry
{
    /// Internal name. Not user facing — readers reference the *source*, not the store.
    std::string name;

    /// The query that owns this store, for attribution and cleanup.
    QueryId queryId = INVALID_QUERY_ID;

    /// The logical source the recorded data derives from, empty when the recorded plan reads an inline source. An index
    /// key over `subplan`, not the definition of the store's content; a store without one cannot be found by source.
    std::string sourceName;

    /// Schema of the recorded data, i.e. the input schema of the store operator.
    Schema schema;

    StoreTimeType timeType{StoreTimeType::EventTime};

    /// The subplan whose output this store recorded — the real definition of its content.
    LogicalPlan subplan;
};

/// Outcome of registering a store, distinguishing the two ways a name can already be taken.
enum class StoreRegistration : uint8_t
{
    Registered,
    /// The same query was analysed again and re-derived a store it had already registered; benign.
    AlreadyRegisteredBySameQuery,
    /// Two different queries produced the same store name. Names derive from the query id, so this means name
    /// derivation is broken — the second store would silently read and write the first one's rows.
    NameCollision,
};

/// Catalog of replay store metadata, alongside the source and sink catalogs.
///
/// Selection currently answers only "which store holds the history of logical source X", but is expressed in terms of a
/// subplan so that the general case stays a change to these functions rather than to their callers.
class StoreCatalog
{
public:
    StoreCatalog() = default;
    ~StoreCatalog() = default;

    StoreCatalog(const StoreCatalog&) = delete;
    StoreCatalog(StoreCatalog&&) = delete;
    StoreCatalog& operator=(const StoreCatalog&) = delete;
    StoreCatalog& operator=(StoreCatalog&&) = delete;

    /// Registers a store. Nothing is changed unless the result is `Registered`.
    ///
    /// The decision is made under the catalog's lock rather than by the caller looking the name up first, so that two
    /// queries registering concurrently cannot both conclude the name is free.
    [[nodiscard]] StoreRegistration registerStore(StoreEntry entry);

    [[nodiscard]] std::optional<StoreEntry> getStore(const std::string& storeName) const;

    /// All stores whose recorded data derives from the given logical source, in registration order.
    /// Does not check whether they can actually answer a read — use `findStoresForSourceHistory` for that.
    [[nodiscard]] std::vector<StoreEntry> getStoresForSource(const std::string& sourceName) const;

    /// Stores that can serve a read of the history of `sourceName`.
    ///
    /// A store only qualifies if it recorded one row per row of the source. A store sitting above a selection or an
    /// aggregation holds a derived stream, and serving it as the source's history would silently return wrong tuples.
    /// This is the narrow, checked case of general view matching; when store placement becomes free this grows into
    /// "find stores whose view definition subsumes this subplan", and only this function changes.
    [[nodiscard]] std::vector<StoreEntry> findStoresForSourceHistory(const std::string& sourceName) const;

    [[nodiscard]] bool removeStore(const std::string& storeName);

    void clear();

private:
    mutable std::recursive_mutex catalogMutex;
    std::unordered_map<std::string, StoreEntry> namesToStores;
    /// Registration order per source, so selection is deterministic while there is no cost model to rank by.
    std::unordered_map<std::string, std::vector<std::string>> sourcesToStoreNames;
};

}
