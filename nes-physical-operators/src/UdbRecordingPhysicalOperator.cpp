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

#include <cstdlib>
#include <optional>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <sys/prctl.h>
#include <sys/wait.h>

#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <function.hpp>

namespace NES
{

namespace
{

/// Spawns udb attached to the current NES PID. udb detaches itself after attaching so no reaping needed.
///
/// Prerequisites:
///   1. UDB_BINARY_PATH must point to the udb executable, e.g. via direnv:
///        export UDB_BINARY_PATH=/path/to/udb
void spawnUdbProxy(const char* traceName)
{
    const char* udbBinEnv = std::getenv("UDB_BINARY_PATH");
    if (udbBinEnv == nullptr)
    {
        NES_ERROR("UDB_BINARY_PATH is not set — skipping udb recording");
        return;
    }
    /// Copy immediately so a concurrent setenv/unsetenv cannot invalidate the pointer.
    const std::string udbBin = udbBinEnv;

    /// Build strings before fork() — malloc is not async-signal-safe in the child.
    const std::string pidStr = std::to_string(static_cast<int>(::getpid()));
    const std::string traceFile = (traceName != nullptr) ? std::string(traceName) + ".undo" : std::string{};

    NES_DEBUG("UdbRecordingPhysicalOperator: spawning udb (binary={}, pid={})", udbBin, pidStr);

    /// Pipe with O_CLOEXEC on the write end: exec closes it automatically on success.
    /// If execlp fails the child writes a byte so the parent can log the error safely.
    std::array<int, 2> pipeFd{};
    if (::pipe2(pipeFd.data(), O_CLOEXEC) != 0)
    {
        NES_ERROR("UdbRecordingPhysicalOperator: pipe2 failed");
        return;
    }

    const pid_t child = ::fork();
    if (child == 0)
    {
        /// Child: write end is O_CLOEXEC — exec closes it. On failure write a byte so parent detects it.
        /// NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
        if (traceName != nullptr)
        {
            ::execl(udbBin.c_str(), udbBin.c_str(), "--pid", pidStr.c_str(), "--recording-file", traceFile.c_str(), nullptr);
        }
        else
        {
            ::execl(udbBin.c_str(), udbBin.c_str(), "--pid", pidStr.c_str(), nullptr);
        }
        /// NOLINTEND(cppcoreguidelines-pro-type-vararg)

        /// execlp only returns on failure — only async-signal-safe calls allowed here.
        const char errByte = 1;
        static_cast<void>(::write(pipeFd[1], &errByte, 1));
        constexpr int exitExecFailed = 127;
        std::_Exit(exitExecFailed);
    }

    /// Parent: close write end, then check if child signalled failure.
    ::close(pipeFd[1]);

    if (child < 0)
    {
        NES_ERROR("UdbRecordingPhysicalOperator: fork failed");
        ::close(pipeFd[0]);
        return;
    }

    /// Grant ptrace permission to exactly this child; avoids having to lower yama/ptrace_scope to 0 (c.f. man 2 prctl)
    if (::prctl(PR_SET_PTRACER, static_cast<int64_t>(child)) < 0) /// NOLINT(cppcoreguidelines-pro-type-vararg)
    {
        NES_ERROR("UdbRecordingPhysicalOperator: prctl(PR_SET_PTRACER) failed, errno={}", errno);
    }

    char result = 0;
    ssize_t nread = 0;
    do
    {
        nread = ::read(pipeFd[0], &result, 1);
    } while (nread < 0 && errno == EINTR); /// retry on SIGCHLD or other interrupts

    if (nread == 1)
    {
        NES_ERROR("UdbRecordingPhysicalOperator: execlp failed for binary '{}'", udbBin);
        ::waitpid(child, nullptr, 0);
    }
    ::close(pipeFd[0]);
}

}

UdbRecordingPhysicalOperator::UdbRecordingPhysicalOperator(std::optional<std::string> traceName) : traceName(std::move(traceName))
{
}

void UdbRecordingPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    if (child.has_value())
    {
        setupChild(executionCtx, compilationContext);
    }
    const char* traceNamePtr = traceName.has_value() ? traceName->c_str() : nullptr;
    spawnUdbProxy(traceNamePtr);
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
