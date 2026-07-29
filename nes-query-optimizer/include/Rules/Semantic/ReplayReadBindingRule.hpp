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

/// Turns a `FOR EVENT_TIME` source reference into a read of a replay store.
///
/// The parser marks such a source with a ReplayReadTrait but cannot resolve it: picking a store is a catalog question.
/// This rule answers it and rewrites the source into an inline `Replay` source carrying the store name and the range.
///
/// It deliberately does not touch the StoreRegistry. Only the store's *metadata* is needed to build a plan; the store
/// instance itself is created per worker during lowering and looked up at runtime. That is what lets a reader query be
/// bound before the writer query has ever produced a row.
///
/// Store selection is a single store today. When the optimizer starts placing several stores per query and recreating
/// the read from them, the choice made here is the thing that grows.
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
