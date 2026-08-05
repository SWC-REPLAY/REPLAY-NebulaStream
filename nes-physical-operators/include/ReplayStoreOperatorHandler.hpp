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

#include <cstdint>
#include <optional>
#include <string>

#include <DataTypes/Schema.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Time/Timestamp.hpp>
#include <Store.hpp>
#include <StoreRegistry.hpp>

namespace NES
{

/// Operator handler that delegates store I/O to a type-erased Store.
class ReplayStoreOperatorHandler final : public OperatorHandler
{
public:
    struct Config
    {
        std::string storeName;
        Schema schema;
        Windowing::TimeUnit unit;
        LogicalFunction onField;
        /// Unqualified schema the store records, and its rendered form for the store header.
        Schema storeSchema;
        std::string schemaText;
        /// What the query configured for itself. Unset fields fall back to the worker's defaults on materialisation,
        /// which is why they stay unresolved here: the defaults belong to the worker, not to the plan.
        StoreManager::StoreConfig storeOverrides;
    };

    explicit ReplayStoreOperatorHandler(Config cfg);
    ~ReplayStoreOperatorHandler() override = default;

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    /// Write a single record to the store.
    void writeRecord(const uint8_t* data, uint32_t size, Timestamp ts);

private:
    /// Materialised on `start` from the worker's registry, so the plan this handler belongs to stays independent of
    /// the worker it is compiled on.
    std::optional<StoreManager::Store> store;
    Config config;
};

}
