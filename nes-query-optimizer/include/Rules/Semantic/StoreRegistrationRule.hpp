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
#include <Stores/StoreCatalog.hpp>

namespace NES
{

/// Names every replay store in the plan and records what it will contain in the StoreCatalog.
///
/// Names derive from the owning query id, so a store traces back to the query that created it and two queries
/// recording the same source never collide.
///
/// Runs after type inference: a store's schema is the input schema of its operator, known only once schemas have been
/// propagated.
class StoreRegistrationRule
{
public:
    explicit StoreRegistrationRule(std::shared_ptr<StoreCatalog> storeCatalog) : storeCatalog(std::move(storeCatalog)) { }

    static constexpr std::string_view NAME = "StoreRegistrationRule";

    [[nodiscard]] static const std::type_info& getType();
    [[nodiscard]] static std::string_view getName();
    [[nodiscard]] std::set<std::type_index> dependsOn() const;
    [[nodiscard]] std::set<std::type_index> requiredBy() const;

    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    bool operator==(const StoreRegistrationRule& other) const;

private:
    std::shared_ptr<StoreCatalog> storeCatalog;
};

static_assert(RuleConcept<StoreRegistrationRule, LogicalPlan>);
}
