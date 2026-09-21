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
#include <UdbRecording.hpp>

namespace NES
{

/// Physical operator that spawns a udb recording process attached to the current NES process.
/// Tuples pass through unchanged. The udb process is started during pipeline setup and stopped
/// (signalled to save its recording and exit) when this operator's pipeline is terminated
/// (query stop or completion).
class UdbRecordingPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    explicit UdbRecordingPhysicalOperator(RecordingConfig config);
    /// Recording is neither copyable nor movable, so this operator needs an explicit copy
    /// constructor. A copy deliberately starts out not recording: exactly one object must own a
    /// given udb process, otherwise both would signal and reap the same pid.
    UdbRecordingPhysicalOperator(const UdbRecordingPhysicalOperator& other);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    RecordingConfig config;
    std::optional<PhysicalOperator> child;
    /// Engaged exactly while this pipeline is being recorded: constructed in setup(), destroyed in
    /// terminate().
    mutable std::optional<Recording> recording;
};

}
