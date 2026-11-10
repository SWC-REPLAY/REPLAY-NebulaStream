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

#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
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
    PRECONDITION(handler != nullptr, "OperatorHandler must not be null");
    PRECONDITION(pipelineCtx != nullptr, "PipelineExecutionContext must not be null");
    handler->start(*pipelineCtx);
}

void stopHandlerProxy(OperatorHandler* handler, PipelineExecutionContext* pipelineCtx)
{
    PRECONDITION(handler != nullptr, "OperatorHandler must not be null");
    PRECONDITION(pipelineCtx != nullptr, "PipelineExecutionContext must not be null");
    handler->stop(QueryTerminationType::Graceful, *pipelineCtx);
}

void writeBufferProxy(TupleBuffer* buffer, OperatorHandler* handler)
{
    PRECONDITION(buffer != nullptr, "Expects a valid buffer");
    PRECONDITION(handler != nullptr, "Expects a valid handler");
    dynamic_cast<ReplayStoreOperatorHandler&>(*handler).writeBuffer(TupleBuffer(*buffer));
}

}

/// OperatorState subclass that holds the staging buffer for accumulating parsed records.
class ReplayStoreState : public OperatorState
{
public:
    explicit ReplayStoreState(const RecordBuffer& resultBuffer) : resultBuffer(resultBuffer) { }

    nautilus::val<uint64_t> outputIndex = 0;
    RecordBuffer resultBuffer;
};

ReplayStorePhysicalOperator::ReplayStorePhysicalOperator(OperatorHandlerId handlerId, std::shared_ptr<TupleBufferRef> bufferRef)
    : handlerId(handlerId), bufferRef(std::move(bufferRef))
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
    const RecordBuffer stagingBuffer{executionCtx.allocateBuffer()};
    executionCtx.setLocalOperatorState(id, std::make_unique<ReplayStoreState>(stagingBuffer));
}

void ReplayStorePhysicalOperator::writeStagingBuffer(ExecutionContext& executionCtx, ReplayStoreState& state) const
{
    state.resultBuffer.setNumRecords(state.outputIndex);
    nautilus::invoke(writeBufferProxy, state.resultBuffer.getReference(), executionCtx.getGlobalOperatorHandler(handlerId));
}

void ReplayStorePhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    auto* const state = dynamic_cast<ReplayStoreState*>(executionCtx.getLocalState(id));

    auto writeResult
        = bufferRef->writeRecord(state->outputIndex, state->resultBuffer, record, executionCtx.pipelineMemoryProvider.bufferProvider);
    /// A failed write means the staging buffer is full: hand it to the store and retry on a fresh buffer.
    if (!writeResult.successful)
    {
        writeStagingBuffer(executionCtx, *state);
        state->resultBuffer = RecordBuffer{executionCtx.allocateBuffer()};
        state->outputIndex = uint64_t{0};
        writeResult
            = bufferRef->writeRecord(state->outputIndex, state->resultBuffer, record, executionCtx.pipelineMemoryProvider.bufferProvider);
    }
    state->outputIndex = state->outputIndex + writeResult.writtenRecords;

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
    writeStagingBuffer(executionCtx, *dynamic_cast<ReplayStoreState*>(executionCtx.getLocalState(id)));
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
