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

#include <Stores/StoreCatalog.hpp>

#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

namespace
{
/// True if `op` is the expansion of `sourceName` across its physical sources: a union whose children are all distinct
/// physical sources of it. That fan-out still yields exactly the source's rows, unlike a union written by the user,
/// which could combine a source with itself and duplicate them.
bool isPhysicalSourceExpansionOf(const LogicalOperator& op, const std::string& sourceName)
{
    const auto children = op.getChildren();
    if (!op.tryGetAs<UnionLogicalOperator>().has_value() || children.empty())
    {
        return false;
    }

    std::unordered_set<PhysicalSourceId> seenPhysicalSources;
    for (const auto& child : children)
    {
        const auto descriptor = child.tryGetAs<SourceDescriptorLogicalOperator>();
        if (!descriptor.has_value() || descriptor.value()->getSourceDescriptor().getLogicalSource().getLogicalSourceName() != sourceName)
        {
            return false;
        }
        if (!seenPhysicalSources.insert(descriptor.value()->getSourceDescriptor().getPhysicalSourceId()).second)
        {
            return false;
        }
    }
    return true;
}

/// True if `plan` records one row per row of `sourceName`, in order, for as long as it ran.
///
/// Only projections are tolerated between the source and the store. A projection narrows or renames columns but emits
/// exactly one row per input row, so the store still holds the source's full history — just fewer columns of it, which
/// a reader asking for those columns can be served from. Anything that drops or combines rows (a selection, a window
/// aggregation, a join) makes the recorded data a derived stream that must never be handed back as the source's
/// history: doing so would silently return the wrong tuples rather than fail.
///
/// A store narrower than the read is not rejected here. Binding the read against the store's schema means a field the
/// store does not hold simply fails to resolve downstream, which is a safe failure rather than a wrong answer.
bool recordsHistoryOf(const LogicalOperator& op, const std::string& sourceName)
{
    if (const auto name = logicalSourceNameOf(op); name.has_value())
    {
        return name.value() == sourceName;
    }

    if (isPhysicalSourceExpansionOf(op, sourceName))
    {
        return true;
    }

    const auto children = op.getChildren();
    if (!op.tryGetAs<ProjectionLogicalOperator>().has_value() || children.size() != 1)
    {
        return false;
    }
    return recordsHistoryOf(children.front(), sourceName);
}

bool recordsHistoryOf(const LogicalPlan& plan, const std::string& sourceName)
{
    const auto roots = plan.getRootOperators();
    return roots.size() == 1 && recordsHistoryOf(roots.front(), sourceName);
}
}

StoreRegistration StoreCatalog::registerStore(StoreEntry entry)
{
    const std::scoped_lock lock(catalogMutex);
    if (const auto existing = namesToStores.find(entry.name); existing != namesToStores.end())
    {
        return existing->second.queryId == entry.queryId ? StoreRegistration::AlreadyRegisteredBySameQuery
                                                         : StoreRegistration::NameCollision;
    }
    NES_DEBUG("Registering store '{}' over source '{}' for query '{}'", entry.name, entry.sourceName, entry.queryId);
    /// A store over an inline source has no logical source name to be found by, so it is registered but not indexed
    /// rather than filed under an empty key nothing can look up.
    if (!entry.sourceName.empty())
    {
        sourcesToStoreNames[entry.sourceName].push_back(entry.name);
    }
    namesToStores.emplace(entry.name, std::move(entry));
    return StoreRegistration::Registered;
}

std::optional<StoreEntry> StoreCatalog::getStore(const std::string& storeName) const
{
    const std::scoped_lock lock(catalogMutex);
    if (const auto it = namesToStores.find(storeName); it != namesToStores.end())
    {
        return it->second;
    }
    return std::nullopt;
}

std::vector<StoreEntry> StoreCatalog::getStoresForSource(const std::string& sourceName) const
{
    const std::scoped_lock lock(catalogMutex);
    std::vector<StoreEntry> result;
    const auto it = sourcesToStoreNames.find(sourceName);
    if (it == sourcesToStoreNames.end())
    {
        return result;
    }
    result.reserve(it->second.size());
    for (const auto& storeName : it->second)
    {
        if (const auto store = namesToStores.find(storeName); store != namesToStores.end())
        {
            result.push_back(store->second);
        }
    }
    return result;
}

std::vector<StoreEntry> StoreCatalog::findStoresForSourceHistory(const std::string& sourceName) const
{
    const std::scoped_lock lock(catalogMutex);
    std::vector<StoreEntry> result;
    const auto it = sourcesToStoreNames.find(sourceName);
    if (it == sourcesToStoreNames.end())
    {
        return result;
    }
    /// Walks the index rather than going through getStoresForSource, so that only the stores that survive the check are
    /// copied. Each entry carries a whole LogicalPlan, so copying every candidate first is not free.
    for (const auto& storeName : it->second)
    {
        if (const auto store = namesToStores.find(storeName);
            store != namesToStores.end() && recordsHistoryOf(store->second.subplan, sourceName))
        {
            result.push_back(store->second);
        }
    }
    return result;
}

bool StoreCatalog::removeStore(const std::string& storeName)
{
    const std::scoped_lock lock(catalogMutex);
    const auto it = namesToStores.find(storeName);
    if (it == namesToStores.end())
    {
        return false;
    }
    if (const auto bySource = sourcesToStoreNames.find(it->second.sourceName); bySource != sourcesToStoreNames.end())
    {
        std::erase(bySource->second, storeName);
        if (bySource->second.empty())
        {
            sourcesToStoreNames.erase(bySource);
        }
    }
    namesToStores.erase(it);
    return true;
}

void StoreCatalog::clear()
{
    const std::scoped_lock lock(catalogMutex);
    namesToStores.clear();
    sourcesToStoreNames.clear();
}

}
