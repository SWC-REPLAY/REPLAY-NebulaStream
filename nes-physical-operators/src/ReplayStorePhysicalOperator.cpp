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

/// OperatorState subclass that holds a scratch buffer for serializing individual records.
class ReplayStoreState : public OperatorState
{
public:
    explicit ReplayStoreState(const RecordBuffer& scratchBuffer)
        : scratchBuffer(scratchBuffer), scratchMemoryArea(scratchBuffer.getMemArea())
    {
    }

    RecordBuffer scratchBuffer;
    nautilus::val<int8_t*> scratchMemoryArea;
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

    /// Allocate a scratch buffer for serializing individual records
    const auto scratchBufferRef = executionCtx.allocateBuffer();
    const auto scratchBuffer = RecordBuffer(scratchBufferRef);
    auto state = std::make_unique<ReplayStoreState>(scratchBuffer);
    executionCtx.setLocalOperatorState(id, std::move(state));
}

void ReplayStorePhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    auto* const state = dynamic_cast<ReplayStoreState*>(executionCtx.getLocalState(id));

    const auto ts = timeFunction.getTs(executionCtx, record);
    NES_DEBUG_EXEC("ReplayStorePhysicalOperator::execute: timestamp=" << ts);
    if (ts > executionCtx.watermarkTs)
    {
        executionCtx.watermarkTs = ts;
    }

    /// Serialize the record into the scratch buffer at index 0
    nautilus::val<uint64_t> writeIndex = 0;
    bufferRef->writeRecord(writeIndex, state->scratchBuffer, record, executionCtx.pipelineMemoryProvider.bufferProvider);

    /// Pass the serialized record bytes to the store via the handler
    auto handler = executionCtx.getGlobalOperatorHandler(handlerId);
    auto memArea = state->scratchMemoryArea;
    auto tsRaw = ts.convertToValue();
    auto tupleSize = nautilus::val<uint32_t>(static_cast<uint32_t>(bufferRef->getTupleSize()));
    nautilus::invoke(
        +[](const int8_t* data, const uint32_t size, const uint64_t tsVal, OperatorHandler* h)
        {
            if (!data || !h)
            {
                return;
            }
            if (auto* storeHandler = dynamic_cast<ReplayStoreOperatorHandler*>(h))
            {
                storeHandler->writeRecord(reinterpret_cast<const uint8_t*>(data), size, Timestamp(tsVal));
            }
        },
        memArea,
        tupleSize,
        tsRaw,
        handler);

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
    /// Every record was already written individually in execute(), nothing to flush.
}

void ReplayStorePhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    nautilus::invoke(stopHandlerProxy, executionCtx.getGlobalOperatorHandler(handlerId), executionCtx.pipelineContext);
    if (child.has_value())
    {
        terminateChild(executionCtx);
    }
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
