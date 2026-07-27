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

#include <optional>

#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <UdbRecorder.hpp>

namespace NES
{

/// Records the worker process into an Undo trace for as long as this operator's pipeline lives.
/// Tuples pass through unchanged: recording starts during pipeline setup and the trace is saved
/// when the pipeline terminates (query stop or completion).
class UdbRecordingPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    explicit UdbRecordingPhysicalOperator(UdbRecorder::Options options);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    UdbRecorder::Options options;
    std::optional<PhysicalOperator> child;
};

}
