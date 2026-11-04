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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalReplayStore.hpp>

#include <memory>
#include <utility>
#include <vector>

#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ReplayStoreLogicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <ReplayStoreOperatorHandler.hpp>
#include <ReplayStorePhysicalOperator.hpp>
#include <StoreRegistry.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalReplayStore::apply(LogicalOperator logicalOperator)
{
    const auto storeOp = logicalOperator.getAs<ReplayStoreLogicalOperator>();
    const auto& storeName = storeOp->getStoreName();

    const auto traitSet = logicalOperator.getTraitSet();
    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema = createPhysicalOutputSchema(storeOp->getChild()->getTraitSet());

    auto& registry = StoreManager::StoreRegistry::instance();
    registry.registerDefaultStore(storeName, inputSchema);
    auto registeredStore = registry.getStore(storeName);
    INVARIANT(registeredStore.has_value(), "Store '{}' was registered above", storeName);

    const auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<ReplayStoreOperatorHandler>(
        ReplayStoreOperatorHandler::Config{.storeName = storeName, .schema = inputSchema}, std::move(*registeredStore));

    auto bufferRef = LowerSchemaProvider::lowerSchema(conf.operatorBufferSize, inputSchema, memoryLayoutType);
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        ReplayStorePhysicalOperator(handlerId, std::move(bufferRef)),
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leaves = {leaves}};
}

}
