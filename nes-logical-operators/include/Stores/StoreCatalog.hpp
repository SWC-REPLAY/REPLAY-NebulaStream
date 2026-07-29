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

namespace NES
{

/// Which notion of time a store's records were stamped with.
///
/// The store itself is indifferent: it is handed a timestamp per record and filters whatever range it is later given.
/// The notion matters only so that a reader asking for one kind of time is never served a store that recorded another,
/// which would return plausible but wrong rows. Adding a further notion — watermark-based, say — means adding an
/// enumerator here and handling it wherever this type is switched on; those switches are deliberately written without a
/// `default` so the compiler points at every site that needs a decision.
enum class StoreTimeType : uint8_t
{
    /// Stamped from a value carried by the record itself.
    EventTime,
    /// Stamped from when the record was ingested, independent of its contents.
    IngestionTime,
};

/// Metadata describing one replay store. This is deliberately *not* the store itself: the catalog records what a store
/// contains so that a later query can be bound against it, while the actual store instance is created per worker during
/// lowering and resolved at runtime. Keeping the two apart is what allows a reader query to be bound before the writer
/// query has ever run.
struct StoreEntry
{
    /// Internal name. Not user facing — readers reference the *source*, not the store.
    std::string name;

    /// The query that owns this store, for attribution and cleanup. Empty for stores whose query had no id.
    std::string queryId;

    /// The logical source the recorded data derives from, or empty when the recorded plan reads an inline source. This
    /// is an index key for the common case, not the definition of the store's content — `viewDefinition` is. A store
    /// with no source name is registered but cannot be found by source.
    std::string sourceName;

    /// Schema of the recorded data, i.e. the input schema of the store operator.
    Schema schema;

    /// Which notion of time the store operator stamped these records with. Recorded so a reader can refuse a store that
    /// measured time differently than the read asks for. How the timestamp was extracted is not recorded: that is the
    /// writer's business, and the store filters on the timestamp it was handed rather than re-deriving one.
    StoreTimeType timeType{StoreTimeType::EventTime};

    /// The subplan whose output this store recorded. This is the real definition of the store's content and the input
    /// to store selection; `sourceName` is only a fast path over it.
    LogicalPlan viewDefinition;
};

/// Outcome of registering a store, distinguishing the two ways a name can already be taken.
enum class StoreRegistration : uint8_t
{
    Registered,
    /// The same query was analysed again and re-derived a store it had already registered. The existing entry already
    /// describes it, so this is benign.
    AlreadyRegisteredBySameQuery,
    /// Two different queries produced the same store name. Names are derived from the query id, so this cannot happen
    /// unless name derivation is broken — it must not be papered over, because the second store would silently read
    /// and write the first one's rows.
    NameCollision,
};

/// Catalog of replay store metadata, alongside the source and sink catalogs.
///
/// Store selection currently answers one question — "which store holds the history of logical source X" — because a
/// store operator is always placed directly above the sink of a plain scan. It is expressed in terms of `findStores`
/// taking a subplan so that the general case (a store recorded at an arbitrary cut in the plan, several stores per
/// query, choosing between them) is a change to this one function rather than to its callers.
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
