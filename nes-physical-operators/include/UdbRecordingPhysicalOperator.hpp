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

#include <atomic>
#include <optional>
#include <string>
#include <sys/types.h>

#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// Physical operator that spawns a udb recording process attached to the current NES process.
/// Tuples pass through unchanged. The udb process is started during pipeline setup and stopped
/// (signalled to save its recording and exit) when this operator's pipeline is terminated
/// (query stop or completion).
class UdbRecordingPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    /// Grouped recording attributes forwarded to udb. Add a field here to extend the set of
    /// attributes; only spawnUdbProxy() and the lowering translation need to consume it.
    ///   - traceName: udb output trace name (optional).
    ///   - traceSize: udb --max-event-log-size, in the form SIZE[K|M|G] (optional).
    struct Config
    {
        std::optional<std::string> traceName;
        std::optional<std::string> traceSize;
    };

    explicit UdbRecordingPhysicalOperator(Config config);
    /// std::atomic<pid_t> isn't copyable, so this operator needs an explicit copy constructor.
    /// A copy deliberately does not inherit udbPid: exactly one object must own a given recorder
    /// process, otherwise both would signal and reap the same pid and race against pid reuse.
    UdbRecordingPhysicalOperator(const UdbRecordingPhysicalOperator& other);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    Config config;
    std::optional<PhysicalOperator> child;
    /// pid of the spawned udb process, set once in setup() and consumed once in terminate();
    /// -1 means "not running" (never started, already stopped, or spawn failed).
    mutable std::atomic<pid_t> udbPid{-1};
};

}
