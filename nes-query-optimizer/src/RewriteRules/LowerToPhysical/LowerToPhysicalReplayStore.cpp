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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalReplayStore.hpp>

#include <memory>
#include <sstream>
#include <utility>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ReplayStoreLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <ReplayStoreOperatorHandler.hpp>
#include <ReplayStorePhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <StoreRegistry.hpp>

#include "Configurations/Descriptor.hpp"
#include "Runtime/Execution/OperatorHandler.hpp"

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalReplayStore::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<ReplayStoreLogicalOperator>(), "Expected a ReplayStoreLogicalOperator");
    auto store = logicalOperator.getAs<ReplayStoreLogicalOperator>();

    auto cfgCopy = DescriptorConfig::Config(store->getConfig());
    Descriptor logicalCfg(std::move(cfgCopy));
    const auto storeName = logicalCfg.getFromConfig(ReplayStoreLogicalOperator::ConfigParameters::STORE_NAME);

    auto registeredStore = StoreManager::StoreRegistry::instance().getStore(storeName);
    PRECONDITION(registeredStore.has_value(), "Store '{}' must be registered before lowering to physical", storeName);

    const auto inputSchema = logicalOperator.getInputSchemas()[0];

    const ReplayStoreOperatorHandler::Config handlerCfg{
        .storeName = storeName,
        .schema = inputSchema,
    };

    auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<ReplayStoreOperatorHandler>(handlerCfg, registeredStore.value());

    const auto outputSchema = logicalOperator.getOutputSchema();
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    const auto bufferSize = conf.pageSize.getValue();
    const auto bufferRef = LowerSchemaProvider::lowerSchema(bufferSize, inputSchema, memoryLayoutType);
    auto physicalOperator = ReplayStorePhysicalOperator(handlerId, inputSchema, bufferRef);
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    const std::vector leafs{wrapper};
    return {.root = wrapper, .leafs = leafs};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterReplayStoreRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalReplayStore>(argument.conf);
}

}