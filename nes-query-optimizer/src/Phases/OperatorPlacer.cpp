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

#include <Phases/OperatorPlacer.hpp>

#include <ranges>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UdbRecordingLogicalOperator.hpp>
#include <Placement/BottomUpPlacement.hpp>
#include <Placement/QueryDecomposition.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

Host placementOf(const LogicalOperator& op)
{
    return op.getTraitSet().get<PlacementTrait>()->onNode;
}

/// Inherits placement and memory-layout traits from `donor`; without them lowering and network
/// channel construction fail.
LogicalOperator makeRecording(const DescriptorConfig::Config& config, const LogicalOperator& donor, LogicalOperator below)
{
    const auto donorTraits = donor.getTraitSet();
    TraitSet traits;
    tryInsert(traits, PlacementTrait(donorTraits.get<PlacementTrait>()->onNode));
    if (const auto layout = donorTraits.tryGet<MemoryLayoutTypeTrait>())
    {
        tryInsert(traits, MemoryLayoutTypeTrait{layout.value()->memoryLayout});
    }

    const LogicalOperator recording = UdbRecordingLogicalOperator(config);
    return recording.withChildren({std::move(below)}).withTraitSet(std::move(traits));
}

/// One recording per node (not per edge) because Linux permits only a single ptrace tracer per process.
LogicalOperator addRecordingsBelow(const LogicalOperator& op, const DescriptorConfig::Config& config, std::unordered_set<Host>& covered)
{
    const auto opNode = placementOf(op);
    auto children = op.getChildren()
        | std::views::transform(
                        [&](const LogicalOperator& child) -> LogicalOperator
                        {
                            auto rebuilt = addRecordingsBelow(child, config, covered);
                            const auto childNode = placementOf(child);
                            if (childNode == opNode || !covered.insert(childNode).second)
                            {
                                return rebuilt;
                            }
                            return makeRecording(config, child, std::move(rebuilt));
                        })
        | std::ranges::to<std::vector>();

    return op.withChildren(std::move(children));
}

LogicalPlan addRecordingPerNode(const LogicalPlan& plan)
{
    const auto existing = getOperatorByType<UdbRecordingLogicalOperator>(plan);
    if (existing.empty())
    {
        return plan;
    }

    /// Single TIME_TRAVEL_UDB per query; recording store is segregated per node so sharing the config is safe.
    const auto& config = existing.front()->getConfig();

    std::unordered_set<Host> covered;
    for (const auto& recording : existing)
    {
        covered.insert(placementOf(recording));
    }

    const auto root = plan.getRootOperators().front();
    auto rebuilt = addRecordingsBelow(root, config, covered);

    /// Root has no incoming edge to splice on; insert beneath it to keep the sink on top for decomposition.
    if (const auto rootNode = placementOf(root); covered.insert(rootNode).second)
    {
        auto children = rebuilt.getChildren();
        INVARIANT(!children.empty(), "BUG: the root of a placed plan is a sink and must have a child");
        children.front() = makeRecording(config, root, children.front());
        rebuilt = rebuilt.withChildren(std::move(children));
    }

    return plan.withRootOperators({rebuilt});
}

}

DistributedLogicalPlan OperatorPlacer::place(LogicalPlan plan) const
{
    BottomUpOperatorPlacer(copyPtr(workerCatalog)).apply(plan);

    plan = addRecordingPerNode(plan);

    return QueryDecomposer(copyPtr(workerCatalog), copyPtr(sourceCatalog), copyPtr(sinkCatalog))
        .decompose(plan, defaultQueryOptimization.network);
}
}
