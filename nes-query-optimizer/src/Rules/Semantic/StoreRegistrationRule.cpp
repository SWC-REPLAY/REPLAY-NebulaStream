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

#include <Rules/Semantic/StoreRegistrationRule.hpp>

#include <cstddef>
#include <set>
#include <string>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ReplayStoreLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Stores/StoreCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// The logical source the recorded rows ultimately come from, or empty when the plan reads an inline source. Only used
/// as an index into the catalog — what the store actually contains is described by its recorded subplan.
std::string findSourceNameBelow(const LogicalOperator& op)
{
    for (const auto& candidate : BFSRange(op))
    {
        if (const auto sourceName = logicalSourceNameOf(candidate); sourceName.has_value())
        {
            return sourceName.value();
        }
    }
    return {};
}
}

const std::type_info& StoreRegistrationRule::getType()
{
    return typeid(StoreRegistrationRule);
}

std::string_view StoreRegistrationRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> StoreRegistrationRule::dependsOn() const
{
    /// A store's schema is the input schema of its operator, so schemas have to be propagated first.
    return {typeid(TypeInferenceRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> StoreRegistrationRule::requiredBy() const
{
    return {};
}

bool StoreRegistrationRule::operator==(const StoreRegistrationRule& other) const
{
    return storeCatalog == other.storeCatalog;
}

LogicalPlan StoreRegistrationRule::apply(LogicalPlan queryPlan) const
{
    auto storeOperators = getOperatorByType<ReplayStoreLogicalOperator>(queryPlan);
    if (storeOperators.empty())
    {
        return queryPlan;
    }

    /// Store names hang off the query id, which is normally only minted when the query is registered. A query that
    /// records history needs a stable identity earlier than that, so mint it here; QueryManager keeps an id that is
    /// already set.
    if (!queryPlan.getQueryId().isDistributed())
    {
        queryPlan.setQueryId(QueryId::createDistributed(getNextDistributedQueryId()));
    }
    const auto queryId = queryPlan.getQueryId().getDistributedQueryId().getRawValue();

    for (size_t storeIndex = 0; storeIndex < storeOperators.size(); ++storeIndex)
    {
        const auto& storeOperator = storeOperators.at(storeIndex);

        /// The suffix is redundant while a query records a single cut of its plan, and is what keeps names unique once
        /// the optimizer records several.
        const auto storeName = fmt::format("{}_store_{}", queryId, storeIndex);

        const auto inputSchemas = storeOperator->getInputSchemas();
        INVARIANT(inputSchemas.size() == 1, "A replay store records exactly one input stream");

        const auto children = storeOperator.getChildren();
        INVARIANT(children.size() == 1, "A replay store has exactly one child");

        StoreEntry entry{
            .name = storeName,
            .queryId = queryId,
            .sourceName = findSourceNameBelow(children.front()),
            .schema = inputSchemas.front(),
            /// LowerToPhysicalReplayStore builds an EventTimeFunction unconditionally, so every store records event
            /// time today. Once the store operator carries a time characteristic of its own, read it from there.
            .timeType = StoreTimeType::EventTime,
            .viewDefinition = LogicalPlan{queryPlan.getQueryId(), {children.front()}}};

        switch (storeCatalog->registerStore(std::move(entry)))
        {
            case StoreRegistration::Registered:
                break;
            case StoreRegistration::AlreadyRegisteredBySameQuery:
                /// Re-analysing the same plan must not fail; the entry from the first pass already describes this store.
                NES_DEBUG("Store '{}' was already registered by an earlier analysis of query '{}'", storeName, queryId);
                break;
            case StoreRegistration::NameCollision:
                /// Names are derived from the query id, so this means name derivation is broken rather than anything the
                /// user did. Continuing would point two queries at one store and silently mix their rows.
                INVARIANT(false, "Store name '{}' derived for query '{}' is already used by a different query", storeName, queryId);
                break;
        }

        auto config = storeOperator->getConfig();
        config[std::string{ReplayStoreLogicalOperator::ConfigParameters::STORE_NAME.name}] = storeName;
        auto renamed = storeOperator->withConfig(std::move(config));
        auto replacement = renamed.withChildren(children);
        auto rewritten = replaceOperator(queryPlan, storeOperator.getId(), LogicalOperator{replacement});
        INVARIANT(
            rewritten.has_value(), "Could not assign name '{}' to its store operator; the plan would reach lowering unnamed", storeName);
        queryPlan = std::move(rewritten.value());
    }

    return queryPlan;
}

}
