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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalUdbRecording.hpp>

#include <memory>
#include <vector>

#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/UdbRecordingLogicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <PhysicalOperator.hpp>
#include <UdbRecording.hpp>
#include <UdbRecordingPhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalUdbRecording::apply(LogicalOperator logicalOperator)
{
    const auto udbOp = logicalOperator.getAs<UdbRecordingLogicalOperator>();

    const auto traitSet = logicalOperator.getTraitSet();
    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema = createPhysicalOutputSchema(udbOp->getChild()->getTraitSet());

    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        UdbRecordingPhysicalOperator(RecordingConfig{.traceName = udbOp->getTraceName(), .traceSize = udbOp->getTraceSize()}),
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leaves = {leaves}};
}

}
