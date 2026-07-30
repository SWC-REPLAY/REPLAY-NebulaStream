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
#include <string_view>
#include <utility>
#include <Configurations/Descriptor.hpp>
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
#include "DataTypes/Schema.hpp"

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

/// Rejects a malformed store chain before it reaches the registry, which only asserts on it.
///
/// The value comes either from a query's SET(...) or from the worker's replay configuration — both user input. Letting
/// it through would abort the worker on an assertion instead of failing the one query that asked for it.
void validateStoreOrder(const std::string& storeOrder)
{
    std::string_view remaining{storeOrder};
    while (!remaining.empty())
    {
        const auto separator = remaining.find("->");
        const auto name = remaining.substr(0, separator);
        if (name != "MemoryStore" && name != "FileStore")
        {
            throw InvalidConfigParameter(
                "Unknown store type '{}' in store order '{}'; expected MemoryStore, FileStore, or MemoryStore->FileStore",
                name,
                storeOrder);
        }
        if (separator == std::string_view::npos)
        {
            return;
        }
        remaining.remove_prefix(separator + 2);
    }
    throw InvalidConfigParameter("Store order must not be empty");
}

/// Materialise the store on this worker if it has not been already. The catalog says a store with this name exists and
/// what it holds; this is where the instance that actually holds the rows comes into being.
void ensureStoreRegistered(
    StoreManager::StoreRegistry& storeRegistry,
    const std::string& storeName,
    const Schema& outputSchema,
    const Descriptor& logicalCfg,
    const StoreManager::StoreConfig& defaults)
{
    if (storeRegistry.getStore(storeName).has_value())
    {
        return;
    }

    /// Build unqualified schema for the store.
    Schema storeSchema;
    for (const auto& field : outputSchema.getFields())
    {
        storeSchema.addField(field.getUnqualifiedName(), field.dataType);
    }

    /// Anything the query configured itself wins; whatever it left unset falls back to the worker's replay
    /// configuration. The store operator's parameters default to empty/zero precisely so the two are distinguishable.
    StoreManager::StoreConfig storeConfig = defaults;
    if (const auto sizeStr = logicalCfg.tryGetFromConfig(ReplayStoreLogicalOperator::ConfigParameters::MEMORY_BUFFER_SIZE);
        sizeStr.has_value() && !sizeStr->empty())
    {
        storeConfig.memoryBufferSize = parseSizeString(*sizeStr);
    }
    if (const auto orderStr = logicalCfg.tryGetFromConfig(ReplayStoreLogicalOperator::ConfigParameters::STORE_ORDER);
        orderStr.has_value() && !orderStr->empty())
    {
        storeConfig.storeOrder = *orderStr;
    }
    if (const auto maxBuf = logicalCfg.tryGetFromConfig(ReplayStoreLogicalOperator::ConfigParameters::MAX_BUFFER_COUNT);
        maxBuf.has_value() && *maxBuf > 0)
    {
        storeConfig.maxBufferCount = maxBuf;
    }

    if (storeConfig.storeOrder.has_value())
    {
        validateStoreOrder(*storeConfig.storeOrder);
    }

    std::stringstream schemaStream;
    schemaStream << storeSchema;
    storeRegistry.registerConfiguredStore(storeName, storeSchema, schemaStream.str(), storeConfig);
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

    PRECONDITION(storeRegistry != nullptr, "Lowering a replay store needs the worker's store registry");
    PRECONDITION(!storeName.empty(), "Store '{}' was not named; StoreRegistrationRule must run before lowering", storeName);

    /// Register the store on-demand if it hasn't been registered yet.
    ensureStoreRegistered(*storeRegistry, storeName, outputSchema, logicalCfg, defaultStoreConfig);

    auto registeredStore = storeRegistry->getStore(storeName);
    PRECONDITION(registeredStore.has_value(), "Store '{}' must be registered before lowering", storeName);

    const auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(storeOp->tsExtractionFunction);
    EventTimeFunction timeFunction(physicalFunction, storeOp->unit);

    ReplayStoreOperatorHandler::Config handlerCfg{
        .storeName = storeName,
        .schema = outputSchema,
        .unit = storeOp->unit,
        .onField = storeOp->tsExtractionFunction,
    };

    auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<ReplayStoreOperatorHandler>(std::move(handlerCfg), std::move(*registeredStore));

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
    return std::make_unique<LowerToPhysicalReplayStore>(argument.conf, argument.storeRegistry, argument.defaultStoreConfig);
}

}
