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

#include <Rules/Semantic/ReplayReadBindingRule.hpp>

#include <algorithm>
#include <set>
#include <string>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/InlineSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/InlineSourceBindingRule.hpp>
#include <Rules/Semantic/SourceInferenceRule.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Stores/StoreCatalog.hpp>
#include <Traits/ReplayReadTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

const std::type_info& ReplayReadBindingRule::getType()
{
    return typeid(ReplayReadBindingRule);
}

std::string_view ReplayReadBindingRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> ReplayReadBindingRule::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> ReplayReadBindingRule::requiredBy() const
{
    /// Replaces a named source with an inline `Replay` source, so it must run before the rules that would resolve that
    /// name against the source catalog.
    return {typeid(InlineSourceBindingRule), typeid(SourceInferenceRule)};
}

bool ReplayReadBindingRule::operator==(const ReplayReadBindingRule& other) const
{
    return storeCatalog == other.storeCatalog && sourceCatalog == other.sourceCatalog;
}

LogicalPlan ReplayReadBindingRule::apply(LogicalPlan queryPlan) const
{
    for (const auto& sourceOperator : getOperatorByType<SourceNameLogicalOperator>(queryPlan))
    {
        const auto replayRead = sourceOperator.getTraitSet().tryGet<ReplayReadTrait>();
        if (!replayRead.has_value())
        {
            continue;
        }

        const auto sourceName = sourceOperator->getLogicalSourceName();
        const auto candidates = storeCatalog->findStoresForSourceHistory(sourceName);
        if (candidates.empty())
        {
            /// The two cases differ: a missing REPLAYABLE query, versus a store that exists but recorded something
            /// other than this source's history.
            if (storeCatalog->getStoresForSource(sourceName).empty())
            {
                throw UnsupportedQuery(
                    "Cannot replay source '{}': no replay store holds its history. A preceding query must use REPLAYABLE WITH HISTORY OF "
                    "to record it before a FOR EVENT_TIME query can read it.",
                    sourceName);
            }
            throw UnsupportedQuery(
                "Cannot replay source '{}': the replay stores recorded from it sit above a filter or an aggregation, so they hold a "
                "derived stream rather than the source's history and cannot answer a FOR EVENT_TIME query against it.",
                sourceName);
        }

        /// One store per query for now. This is where ranking candidates belongs once several can answer the same read.
        const auto& store = candidates.front();
        if (candidates.size() > 1)
        {
            NES_DEBUG("{} stores can serve source '{}', using '{}'", candidates.size(), sourceName, store.name);
        }

        /// A store that measured a different notion of time cannot answer FOR EVENT_TIME; the rows would look
        /// plausible and be wrong. No `default`, so a new StoreTimeType cannot silently fall through to accepted.
        switch (store.timeType)
        {
            case StoreTimeType::EventTime:
                break;
            case StoreTimeType::IngestionTime:
                throw UnsupportedQuery(
                    "Cannot answer a FOR EVENT_TIME read of source '{}' from store '{}': it recorded ingestion time, so its timestamps "
                    "do not correspond to the event times the query asks for.",
                    sourceName,
                    store.name);
        }

        std::unordered_map<std::string, std::string> sourceConfig{{"store_name", store.name}};
        if (const auto& start = replayRead.value()->getStart(); start.has_value())
        {
            sourceConfig["replay_start_timestamp"] = fmt::format("{}", *start);
        }
        if (const auto& end = replayRead.value()->getEnd(); end.has_value())
        {
            sourceConfig["replay_end_timestamp"] = fmt::format("{}", *end);
        }

        /// Served from wherever the source it replays lives; becomes the store operator's placement once placement
        /// knows about stores.
        const auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        if (!logicalSource.has_value())
        {
            throw UnknownSourceName("Logical source not registered. Source Name: {}", sourceName);
        }
        const auto physicalSources = sourceCatalog->getPhysicalSources(logicalSource.value());
        if (!physicalSources.has_value() || physicalSources->empty())
        {
            throw UnsupportedQuery(
                "Cannot replay source '{}': it has no physical source, so there is no host to read the store on.", sourceName);
        }
        /// Lowest physical source id rather than the first of an unordered set, so the same query always plans the same way.
        const auto& chosenSource = *std::ranges::min_element(
            *physicalSources, {}, [](const SourceDescriptor& descriptor) { return descriptor.getPhysicalSourceId().getRawValue(); });
        sourceConfig["host"] = chosenSource.getHost().getRawValue();

        /// Qualify the store's field names with the source name, matching what expanding a named source would produce.
        auto qualifiedSchema = Schema{};
        for (const auto& field : store.schema.getFields())
        {
            qualifiedSchema.addField(fmt::format("{}${}", sourceName, field.getUnqualifiedName()), field.dataType);
        }

        const LogicalOperator replaySource = TypedLogicalOperator<InlineSourceLogicalOperator>{
            "Replay", qualifiedSchema, std::move(sourceConfig), std::unordered_map<std::string, std::string>{{"type", "Native"}}};

        auto rewritten = replaceSubtree(queryPlan, sourceOperator.getId(), replaySource);
        INVARIANT(
            rewritten.has_value(),
            "Could not replace source '{}' with a read of store '{}'; the query would silently read the live source instead",
            sourceName,
            store.name);
        queryPlan = std::move(rewritten.value());
    }

    return queryPlan;
}

}
