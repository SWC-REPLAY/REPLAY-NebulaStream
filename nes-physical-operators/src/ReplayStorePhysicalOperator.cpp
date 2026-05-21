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

#include <ReplayStorePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Util.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <ReplayStoreOperatorHandler.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{

void setupHandlerProxy(OperatorHandler* handler, PipelineExecutionContext* pipelineCtx)
{
    if ((handler == nullptr) || (pipelineCtx == nullptr))
    {
        return;
    }
    handler->start(*pipelineCtx, 0);
}

void stopHandlerProxy(OperatorHandler* handler, PipelineExecutionContext* pipelineCtx)
{
    PRECONDITION(handler != nullptr, "OperatorHandler must not be null");
    PRECONDITION(pipelineCtx != nullptr, "PipelineExecutionContext must not be null");
    handler->stop(QueryTerminationType::Graceful, *pipelineCtx);
}

}

/// OperatorState subclass that holds the staging buffer for accumulating parsed records.
class ReplayStoreState : public OperatorState
{
public:
    explicit ReplayStoreState(const RecordBuffer& resultBuffer)
        : batchMinTs(nautilus::val<uint64_t>(Timestamp::INVALID_VALUE))
        , batchMaxTs(nautilus::val<uint64_t>(Timestamp::INITIAL_VALUE))
        , resultBuffer(resultBuffer)
        , bufferMemoryArea(resultBuffer.getMemArea())
    {
    }

    nautilus::val<uint64_t> outputIndex = 0;
    nautilus::val<uint64_t> batchMinTs;
    nautilus::val<uint64_t> batchMaxTs;
    RecordBuffer resultBuffer;
    nautilus::val<int8_t*> bufferMemoryArea;
};

ReplayStorePhysicalOperator::ReplayStorePhysicalOperator(
    OperatorHandlerId handlerId, const Schema& inputSchema, std::shared_ptr<TupleBufferRef> bufferRef, EventTimeFunction timeFunction)
    : handlerId(handlerId), inputSchema(inputSchema), bufferRef(std::move(bufferRef)), timeFunction(std::move(timeFunction))
{
}

void ReplayStorePhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    if (child.has_value())
    {
        setupChild(executionCtx, compilationContext);
    }
    nautilus::invoke(setupHandlerProxy, executionCtx.getGlobalOperatorHandler(handlerId), executionCtx.pipelineContext);
}

void ReplayStorePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (child.has_value())
    {
        openChild(executionCtx, recordBuffer);
    }

    executionCtx.watermarkTs = nautilus::val<Timestamp>(Timestamp(Timestamp::INITIAL_VALUE));
    timeFunction.open(executionCtx, recordBuffer);

    /// Allocate a staging buffer and create state to accumulate parsed records
    const auto stagingBufferRef = executionCtx.allocateBuffer();
    const auto stagingBuffer = RecordBuffer(stagingBufferRef);
    auto state = std::make_unique<ReplayStoreState>(stagingBuffer);
    state->batchMinTs = nautilus::val<uint64_t>(Timestamp::INVALID_VALUE);
    state->batchMaxTs = nautilus::val<uint64_t>(Timestamp::INITIAL_VALUE);
    executionCtx.setLocalOperatorState(id, std::move(state));
}

void ReplayStorePhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    auto* const state = dynamic_cast<ReplayStoreState*>(executionCtx.getLocalState(id));

    const auto ts = timeFunction.getTs(executionCtx, record);
    NES_DEBUG_EXEC("ReplayStorePhysicalOperator::execute: timestamp=" << ts);
    const auto tsRaw = ts.convertToValue();
    if (ts > executionCtx.watermarkTs)
    {
        executionCtx.watermarkTs = ts;
    }
    if (tsRaw < state->batchMinTs)
    {
        state->batchMinTs = tsRaw;
    }
    if (tsRaw > state->batchMaxTs)
    {
        state->batchMaxTs = tsRaw;
    }

    /// If staging buffer is full, flush it to the store and allocate a new one
    if (state->outputIndex >= getMaxRecordsPerBuffer())
    {
        state->resultBuffer.setNumRecords(state->outputIndex);
        /// Write the full staging buffer to the store
        auto handler = executionCtx.getGlobalOperatorHandler(handlerId);
        auto tbRef = state->resultBuffer.getReference();
        auto batchMinTs = state->batchMinTs;
        auto batchMaxTs = state->batchMaxTs;
        nautilus::invoke(
            +[](TupleBuffer* tb, OperatorHandler* handler, uint64_t minTsRaw, uint64_t maxTsRaw)
            {
                if (!tb || !handler)
                {
                    return;
                }
                if (auto* storeHandler = dynamic_cast<ReplayStoreOperatorHandler*>(handler))
                {
                    TupleBuffer buffer(*tb);
                    storeHandler->writeBuffer(std::move(buffer), Timestamp(minTsRaw), Timestamp(maxTsRaw));
                }
            },
            tbRef,
            handler,
            batchMinTs,
            batchMaxTs);

        /// Allocate a fresh staging buffer
        const auto newBufferRef = executionCtx.allocateBuffer();
        state->resultBuffer = RecordBuffer(newBufferRef);
        state->bufferMemoryArea = state->resultBuffer.getMemArea();
        state->outputIndex = nautilus::val<uint64_t>(0);
        state->batchMinTs = nautilus::val<uint64_t>(Timestamp::INVALID_VALUE);
        state->batchMaxTs = nautilus::val<uint64_t>(Timestamp::INITIAL_VALUE);
    }

    /// Write the parsed record into the staging buffer
    bufferRef->writeRecord(state->outputIndex, state->resultBuffer, record, executionCtx.pipelineMemoryProvider.bufferProvider);
    state->outputIndex = state->outputIndex + 1;

    /// Forward the record to the child (pass-through)
    if (child.has_value())
    {
        executeChild(executionCtx, record);
    }
}

void ReplayStorePhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (child.has_value())
    {
        closeChild(executionCtx, recordBuffer);
    }

    /// Write the remaining records in the staging buffer to the store
    auto* const state = dynamic_cast<ReplayStoreState*>(executionCtx.getLocalState(id));
    state->resultBuffer.setNumRecords(state->outputIndex);

    auto handler = executionCtx.getGlobalOperatorHandler(handlerId);
    auto tbRef = state->resultBuffer.getReference();
    auto batchMinTs = state->batchMinTs;
    auto batchMaxTs = state->batchMaxTs;
    nautilus::invoke(
        +[](TupleBuffer* tb, OperatorHandler* handler, uint64_t minTsRaw, uint64_t maxTsRaw)
        {
            if (!tb || !handler)
            {
                return;
            }
            if (auto* storeHandler = dynamic_cast<ReplayStoreOperatorHandler*>(handler))
            {
                TupleBuffer buffer(*tb);
                NES_DEBUG("ReplayStorePhysicalOperator::close: writing {} tuples to store", buffer.getNumberOfTuples());
                storeHandler->writeBuffer(std::move(buffer), Timestamp(minTsRaw), Timestamp(maxTsRaw));
            }
        },
        tbRef,
        handler,
        batchMinTs,
        batchMaxTs);
}

void ReplayStorePhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    nautilus::invoke(stopHandlerProxy, executionCtx.getGlobalOperatorHandler(handlerId), executionCtx.pipelineContext);
    if (child.has_value())
    {
        terminateChild(executionCtx);
    }
}

uint64_t ReplayStorePhysicalOperator::getMaxRecordsPerBuffer() const
{
    return bufferRef->getCapacity();
}

std::optional<PhysicalOperator> ReplayStorePhysicalOperator::getChild() const
{
    return child;
}

void ReplayStorePhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
