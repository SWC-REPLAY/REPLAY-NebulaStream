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

#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <linux/prctl.h>
#include <sys/prctl.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

namespace
{

/// A non-zero TracerPid is the only observable signal that udb has finished attaching.
pid_t currentTracerPid()
{
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line))
    {
        constexpr std::string_view prefix = "TracerPid:";
        if (line.starts_with(prefix))
        {
            return static_cast<pid_t>(std::strtol(line.c_str() + prefix.size(), nullptr, 10));
        }
    }
    return 0;
}

/// Spawns udb attached to the current NES PID. The returned pid is the udb (live-record) controller
/// process itself in --pid (attach) mode; it must be explicitly stopped and reaped by the caller
/// (see UdbRecordingPhysicalOperator::terminate).
///
/// Prerequisites:
///   1. UDB_BINARY_PATH must point to the udb executable, e.g. via direnv:
///        export UDB_BINARY_PATH=/path/to/udb
std::optional<pid_t> spawnUdbProxy(const UdbRecordingPhysicalOperator::Config& config)
{
    const char* udbBinEnv = std::getenv("UDB_BINARY_PATH");
    if (udbBinEnv == nullptr)
    {
        NES_ERROR("UDB_BINARY_PATH is not set — skipping udb recording");
        return std::nullopt;
    }
    /// Copy immediately so a concurrent setenv/unsetenv cannot invalidate the pointer.
    const std::string udbBin = udbBinEnv;

    /// Build strings before fork() — malloc is not async-signal-safe in the child.
    const std::string pidStr = std::to_string(static_cast<int>(::getpid()));
    const std::string traceFile = config.traceName.has_value() ? *config.traceName + ".undo" : std::string{};

    NES_DEBUG("Spawning udb (binary={}, pid={})", udbBin, pidStr);

    /// Build the argv before fork() — malloc is not async-signal-safe in the child.
    std::vector<char*> execArgs{const_cast<char*>(udbBin.c_str()), const_cast<char*>("--pid"), const_cast<char*>(pidStr.c_str())};
    if (config.traceName.has_value())
    {
        execArgs.push_back(const_cast<char*>("--recording-file"));
        execArgs.push_back(const_cast<char*>(traceFile.c_str()));
    }
    if (config.traceSize.has_value())
    {
        execArgs.push_back(const_cast<char*>("--max-event-log-size"));
        execArgs.push_back(const_cast<char*>(config.traceSize->c_str()));
    }
    execArgs.push_back(nullptr);

    /// Pipe with O_CLOEXEC on the write end: exec closes it automatically on success.
    /// If execv fails the child writes a byte so the parent can log the error safely.
    std::array<int, 2> pipeFd{};
    if (::pipe2(pipeFd.data(), O_CLOEXEC) != 0)
    {
        NES_ERROR("Pipe2 failed");
        return std::nullopt;
    }

    const pid_t child = ::fork();
    if (child == 0)
    {
        /// Child: write end is O_CLOEXEC — exec closes it. On failure write a byte so parent detects it.
        ::execv(udbBin.c_str(), execArgs.data());

        /// execv only returns on failure — only async-signal-safe calls allowed here.
        const char errByte = 1;
        static_cast<void>(::write(pipeFd[1], &errByte, 1));
        constexpr int exitExecFailed = 127;
        std::_Exit(exitExecFailed);
    }

    /// Parent: close write end, then check if child signalled failure.
    ::close(pipeFd[1]);

    if (child < 0)
    {
        NES_ERROR("Fork failed");
        ::close(pipeFd[0]);
        return std::nullopt;
    }

    /// Grant ptrace permission to exactly this child; avoids having to lower yama/ptrace_scope to 0 (c.f. man 2 prctl)
    if (::prctl(PR_SET_PTRACER, static_cast<int64_t>(child)) < 0) /// NOLINT(cppcoreguidelines-pro-type-vararg)
    {
        NES_ERROR("prctl(PR_SET_PTRACER) failed, errno={}", errno);
    }

    char result = 0;
    ssize_t nread = 0;
    for (;;)
    {
        nread = ::read(pipeFd[0], &result, 1);
        if (nread >= 0 || errno != EINTR)
        {
            break;
        }
    } /// retry on SIGCHLD or other interrupts

    if (nread == 1)
    {
        NES_ERROR("execv failed for binary '{}'", udbBin);
        ::waitpid(child, nullptr, 0);
        ::close(pipeFd[0]);
        return std::nullopt;
    }
    ::close(pipeFd[0]);

    /// udb attaches asynchronously, so without this wait a fast pipeline (e.g. INTERPRETER mode,
    /// which skips the ~second-long JIT compilation that would otherwise mask the attach latency)
    /// can run and terminate the recorder before it ever attached, yielding an empty recording.
    /// Sources only start once every pipeline's setup() has returned, so blocking here is what
    /// guarantees the whole execution is recorded regardless of execution mode.
    constexpr auto attachTimeout = std::chrono::seconds(30);
    const auto deadline = std::chrono::steady_clock::now() + attachTimeout;
    while (currentTracerPid() == 0)
    {
        int status = 0;
        if (::waitpid(child, &status, WNOHANG) == child)
        {
            NES_ERROR("udb process {} exited before attaching (status={})", child, status);
            return std::nullopt;
        }
        if (std::chrono::steady_clock::now() >= deadline)
        {
            /// A process can only have one tracer, so leaving a half-attached udb behind would block
            /// every later recording for the lifetime of this process.
            NES_ERROR("Timed out waiting for udb process {} to attach", child);
            ::kill(child, SIGKILL);
            ::waitpid(child, nullptr, 0);
            return std::nullopt;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    NES_DEBUG("udb attached (tracerPid={})", currentTracerPid());
    return child;
}

}

UdbRecordingPhysicalOperator::UdbRecordingPhysicalOperator(Config config) : config(std::move(config))
{
}

UdbRecordingPhysicalOperator::UdbRecordingPhysicalOperator(const UdbRecordingPhysicalOperator& other)
    : config(other.config), child(other.child)
{
}

void UdbRecordingPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    if (child.has_value())
    {
        setupChild(executionCtx, compilationContext);
    }
    udbPid = spawnUdbProxy(config).value_or(-1);
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

    const pid_t pid = udbPid.exchange(-1);
    if (pid <= 0)
    {
        return;
    }
    if (::kill(pid, SIGUSR1) != 0)
    {
        NES_ERROR("Failed to signal udb process {} to stop recording, errno={}", pid, errno);
        ::waitpid(pid, nullptr, WNOHANG);
        return;
    }
    /// Block until udb has saved the recording and exited, so the recording is guaranteed
    /// finalized by the time terminate() returns. Bounded, because a wedged udb would otherwise
    /// hang query teardown for good.
    constexpr auto saveTimeout = std::chrono::seconds(60);
    const auto deadline = std::chrono::steady_clock::now() + saveTimeout;
    int status = 0;
    for (;;)
    {
        const pid_t reaped = ::waitpid(pid, &status, WNOHANG);
        if (reaped == pid)
        {
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
            {
                NES_ERROR("udb process {} did not save the recording cleanly (status={})", pid, status);
            }
            return;
        }
        if (reaped < 0 && errno != EINTR)
        {
            NES_ERROR("Failed to reap udb process {}, errno={}", pid, errno);
            return;
        }
        if (std::chrono::steady_clock::now() >= deadline)
        {
            NES_ERROR("Timed out waiting for udb process {} to save the recording, killing it", pid);
            ::kill(pid, SIGKILL);
            ::waitpid(pid, nullptr, 0);
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
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
