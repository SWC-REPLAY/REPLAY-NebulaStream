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

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ReplayStoreLogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <ReplayStoreOperatorHandler.hpp>
#include <ReplayStorePhysicalOperator.hpp>
#include <StoreRegistry.hpp>

namespace NES
{

namespace
{
/// Parse a size string like "64MB" into bytes.
size_t parseSizeString(const std::string& s)
{
    size_t pos = 0;
    const auto number = std::stoull(s, &pos);
    const auto suffix = s.substr(pos);
    if (suffix.empty() || suffix == "B")
    {
        return number;
    }
    if (suffix == "KB")
    {
        return number * 1024UZ;
    }
    if (suffix == "MB")
    {
        return number * 1024UZ * 1024UZ;
    }
    if (suffix == "GB")
    {
        return number * 1024UZ * 1024UZ * 1024UZ;
    }
    throw InvalidConfigParameter("Cannot parse size string: '{}'", s);
}

/// Only what this query configured for itself. Whatever stays unset is filled in by the worker's replay defaults when
/// the store is materialised. The store operator's parameters default to empty/zero precisely so "the query said
/// nothing" stays distinguishable from "the query said this".
StoreConfig readStoreOverrides(const Descriptor& logicalCfg)
{
    StoreConfig overrides;
    if (const auto sizeStr = logicalCfg.tryGetFromConfig(ReplayStoreLogicalOperator::ConfigParameters::MEMORY_BUFFER_SIZE);
        sizeStr.has_value() && !sizeStr->empty())
    {
        overrides.memoryBufferSize = parseSizeString(*sizeStr);
    }
    if (const auto orderStr = logicalCfg.tryGetFromConfig(ReplayStoreLogicalOperator::ConfigParameters::STORE_ORDER);
        orderStr.has_value() && !orderStr->empty())
    {
        overrides.storeOrder = *orderStr;
    }
    if (const auto maxBuf = logicalCfg.tryGetFromConfig(ReplayStoreLogicalOperator::ConfigParameters::MAX_BUFFER_COUNT);
        maxBuf.has_value() && *maxBuf > 0)
    {
        overrides.maxBufferCount = maxBuf;
    }
    return overrides;
}

/// The store records field names without their source qualifier.
Schema unqualify(const Schema& schema)
{
    Schema storeSchema;
    for (const auto& field : schema.getFields())
    {
        storeSchema.addField(field.getUnqualifiedName(), field.dataType);
    }
    return storeSchema;
}
}

LoweringRuleResultSubgraph LowerToPhysicalReplayStore::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<ReplayStoreLogicalOperator>(), "Expected a StoreLogicalOperator");
    auto storeOp = logicalOperator.getAs<ReplayStoreLogicalOperator>();

    auto cfgCopy = DescriptorConfig::Config(storeOp->getConfig());
    const Descriptor logicalCfg(std::move(cfgCopy));
    const auto storeName = logicalCfg.getFromConfig(ReplayStoreLogicalOperator::ConfigParameters::STORE_NAME);

    const auto outputSchema = logicalOperator.getOutputSchema();

    PRECONDITION(!storeName.empty(), "Store '{}' was not named; StoreRegistrationRule must run before lowering", storeName);

    const auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(storeOp->tsExtractionFunction);
    EventTimeFunction timeFunction(physicalFunction, storeOp->unit);

    /// The store itself is materialised on the worker when the pipeline starts; lowering only records what to build.
    auto storeSchema = unqualify(outputSchema);
    std::stringstream schemaStream;
    schemaStream << storeSchema;

    ReplayStoreOperatorHandler::Config handlerCfg{
        .storeName = storeName,
        .schema = outputSchema,
        .unit = storeOp->unit,
        .onField = storeOp->tsExtractionFunction,
        .storeSchema = storeSchema,
        .schemaText = schemaStream.str(),
        .storeOverrides = readStoreOverrides(logicalCfg),
    };

    auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<ReplayStoreOperatorHandler>(std::move(handlerCfg));

    const auto inputSchema = logicalOperator.getInputSchemas()[0];
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;
    auto bufferRef = LowerSchemaProvider::lowerSchema(conf.pageSize.getValue(), inputSchema, memoryLayoutType);
    auto physicalOperator = ReplayStorePhysicalOperator(handlerId, inputSchema, std::move(bufferRef), std::move(timeFunction));
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

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterReplayStoreLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalReplayStore>(argument.conf);
}

}
