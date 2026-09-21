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

#include <UdbRecordingPhysicalOperator.hpp>

#include <optional>
#include <utility>

#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <UdbRecording.hpp>

namespace NES
{

UdbRecordingPhysicalOperator::UdbRecordingPhysicalOperator(RecordingConfig config) : config(std::move(config))
{
}

UdbRecordingPhysicalOperator::UdbRecordingPhysicalOperator(const UdbRecordingPhysicalOperator& other)
    : PhysicalOperatorConcept(other), config(other.config), child(other.child)
{
}

void UdbRecordingPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    if (child.has_value())
    {
        setupChild(executionCtx, compilationContext);
    }

    NES_INFO("Spawning and attaching udb");
    /// Deliberately not swallowed: a TIME_TRAVEL_UDB query that cannot be recorded is a failed query,
    /// not a query that quietly runs without producing a trace.
    recording.emplace(config);
}

void UdbRecordingPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (child.has_value())
    {
        openChild(executionCtx, recordBuffer);
    }
}

void UdbRecordingPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    if (child.has_value())
    {
        executeChild(executionCtx, record);
    }
}

void UdbRecordingPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (child.has_value())
    {
        closeChild(executionCtx, recordBuffer);
    }
}

void UdbRecordingPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    if (child.has_value())
    {
        terminateChild(executionCtx);
    }

    NES_INFO("Saving udb recording and stopping udb");
    /// Blocks until the trace is on disk.
    recording.reset();
}

std::optional<PhysicalOperator> UdbRecordingPhysicalOperator::getChild() const
{
    return child;
}

void UdbRecordingPhysicalOperator::setChild(PhysicalOperator newChild)
{
    child = std::move(newChild);
}

}
