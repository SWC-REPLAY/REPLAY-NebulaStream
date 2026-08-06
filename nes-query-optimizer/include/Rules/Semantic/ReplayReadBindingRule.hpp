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

#include <memory>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Stores/StoreCatalog.hpp>

namespace NES
{

/// Turns a `FOR EVENT_TIME` source reference into a read of a replay store, rewriting it into an inline `Replay` source
/// carrying the store name and range. Picking a store is a catalog question, which is why the parser only marks the
/// source with a ReplayReadTrait and leaves the resolution here.
///
/// Reads only the store's metadata, never the StoreRegistry — the instance is created per worker during lowering. That
/// is what lets a reader query be bound before the writer query has produced a row.
class ReplayReadBindingRule
{
public:
    ReplayReadBindingRule(std::shared_ptr<const StoreCatalog> storeCatalog, std::shared_ptr<const SourceCatalog> sourceCatalog)
        : storeCatalog(std::move(storeCatalog)), sourceCatalog(std::move(sourceCatalog))
    {
    }

    static constexpr std::string_view NAME = "ReplayReadBindingRule";

    [[nodiscard]] static const std::type_info& getType();
    [[nodiscard]] static std::string_view getName();
    [[nodiscard]] std::set<std::type_index> dependsOn() const;
    [[nodiscard]] std::set<std::type_index> requiredBy() const;

    /// @throws UnsupportedQuery if no registered store can answer the requested range
    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    bool operator==(const ReplayReadBindingRule& other) const;

private:
    std::shared_ptr<const StoreCatalog> storeCatalog;
    std::shared_ptr<const SourceCatalog> sourceCatalog;
};

static_assert(RuleConcept<ReplayReadBindingRule, LogicalPlan>);
}
