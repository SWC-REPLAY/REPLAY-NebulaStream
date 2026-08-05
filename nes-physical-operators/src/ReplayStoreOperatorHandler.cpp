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

#include <ReplayStoreOperatorHandler.hpp>

#include <cstdint>
#include <utility>

#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <Store.hpp>
#include <StoreRegistry.hpp>

namespace NES
{

ReplayStoreOperatorHandler::ReplayStoreOperatorHandler(Config cfg) : config(std::move(cfg))
{
}

void ReplayStoreOperatorHandler::start(PipelineExecutionContext& pipelineExecutionContext, uint32_t)
{
    auto registry = pipelineExecutionContext.getStoreRegistry();
    if (!registry.has_value())
    {
        throw StoreManagerInitFailure("Store '{}' needs a worker with replay stores configured", config.storeName);
    }

    /// The buffer provider comes from the engine, so the store draws from the same pool as the rest of the pipeline.
    store = registry->get().getOrCreateStore(
        config.storeName, config.storeSchema, config.schemaText, config.storeOverrides, pipelineExecutionContext.getBufferManager());
    store->open();
}

void ReplayStoreOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
    if (store.has_value())
    {
        store->flush();
    }
}

void ReplayStoreOperatorHandler::writeRecord(const uint8_t* data, uint32_t size, Timestamp ts)
{
    PRECONDITION(store.has_value(), "Store '{}' must be materialised by start() before records are written", config.storeName);
    NES_DEBUG("ReplayStoreOperatorHandler::writeRecord: size={}, ts={}, store={}", size, ts, config.storeName);
    store->writeRecord(data, size, ts, config.schema);
}

}
