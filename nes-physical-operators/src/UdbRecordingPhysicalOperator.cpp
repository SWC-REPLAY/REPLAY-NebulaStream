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

#include <chrono>
#include <optional>
#include <string_view>
#include <utility>

#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <UdbRecorder.hpp>

namespace NES
{

namespace
{
/// Benchmark instrumentation. Emitted at WARNING because RelWithDebInfo compiles out everything
/// below it, and the two phases bracketed here are exactly what the systest's stop-minus-running
/// metric cannot show: attach happens before the query reaches Running, and the save is buried
/// inside the reported elapsed time.
template <typename F>
void logPhase(const std::string_view phase, F&& phaseFn)
{
    const auto begin = std::chrono::steady_clock::now();
    std::forward<F>(phaseFn)();
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    NES_WARNING("UDBBENCH phase={} ms={}", phase, std::chrono::duration<double, std::milli>(elapsed).count());
}
}

UdbRecordingPhysicalOperator::UdbRecordingPhysicalOperator(UdbRecorder::Options options) : options(std::move(options))
{
}

/// Recording starts here rather than in open(), because sources only start once every pipeline's
/// setup() has returned; that is what makes the whole query execution part of the trace.
void UdbRecordingPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    if (child.has_value())
    {
        setupChild(executionCtx, compilationContext);
    }
    logPhase("attach", [this] { UdbRecorder::instance().start(options); });
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
    logPhase("save", [] { UdbRecorder::instance().saveAndStop(); });
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
