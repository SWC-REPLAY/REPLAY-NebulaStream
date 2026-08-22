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

#include <UdbRecording.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>
#include <fcntl.h>
/// The POSIX spellings alongside <csignal>/<cstdlib>: kill, SIGKILL and the W* status macros are
/// POSIX, not standard C++, so include-cleaner will not accept the C++ headers as their provider.
#include <signal.h> /// NOLINT(modernize-deprecated-headers)
#include <stdlib.h> /// NOLINT(modernize-deprecated-headers)
#include <unistd.h>
#include <linux/prctl.h>
#include <sys/prctl.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <Util/Files.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <Thread.hpp>

namespace NES
{

namespace
{

constexpr auto pollInterval = std::chrono::milliseconds(5);
constexpr auto attachTimeout = std::chrono::seconds(30);
/// Bounded because a wedged live-record would otherwise hang query teardown for good.
constexpr auto saveTimeout = std::chrono::seconds(60);
/// live-record detaches only after writing the trace, so the previous recording can still hold this
/// process briefly after its query was reported as stopped.
constexpr auto detachTimeout = std::chrono::seconds(30);

/// A non-zero TracerPid is the only observable signal that live-record has finished attaching.
pid_t currentTracerPid()
{
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line))
    {
        constexpr std::string_view prefix = "TracerPid:";
        if (line.starts_with(prefix))
        {
            return NES::from_chars<pid_t>(std::string_view{line}.substr(prefix.size())).value_or(0);
        }
    }
    return 0;
}

/// Whether some live Recording already owns this process. See Recording::Recording. The claim is
/// process-wide because the invariant it guards is: Linux permits one tracer per process.
std::atomic<bool> processIsRecording{false}; /// NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

/// Prerequisite: UDB_BINARY_PATH must point to the live-record executable, e.g. via direnv:
///     export UDB_BINARY_PATH=/path/to/live-record
std::string udbBinaryPath()
{
    const char* udbBinEnv = std::getenv("UDB_BINARY_PATH"); /// NOLINT(concurrency-mt-unsafe)
    if (udbBinEnv == nullptr)
    {
        throw UdbRecordingFailure("UDB_BINARY_PATH is not set");
    }
    /// Copy immediately so a concurrent setenv/unsetenv cannot invalidate the pointer.
    return {udbBinEnv};
}

/// Traces are separated per worker node. UDB_RECORDING_STORE_DIR is compiled in, so every worker
/// process built from the same tree shares it, and two nodes recording one distributed query would
/// otherwise write the same trace file. The node id is thread-local and inherited from the worker
/// thread that runs the operator's setup(), so it is already correct here.
std::filesystem::path recordingStoreDir()
{
    auto node = Thread::getThisWorkerNodeId().getRawValue();
    /// ':' is a legal path character on Linux, but a hostname:port directory trips up enough tooling
    /// to be worth avoiding.
    std::ranges::replace(node, ':', '_');
    const std::filesystem::path storeDir = std::filesystem::path{UDB_RECORDING_STORE_DIR} / ("node-" + node);
    std::error_code errorCode;
    std::filesystem::create_directories(storeDir, errorCode);
    if (errorCode)
    {
        throw UdbRecordingFailure("cannot create recording store directory '{}': {}", storeDir.string(), errorCode.message());
    }
    return storeDir;
}

/// Blocks until nothing traces this process. Linux permits exactly one tracer, so an overlapping
/// recording would make live-record fail to attach with a far less obvious error than this one.
void waitUntilNotTraced()
{
    const auto deadline = std::chrono::steady_clock::now() + detachTimeout;
    while (const pid_t tracerPid = currentTracerPid())
    {
        if (std::chrono::steady_clock::now() >= deadline)
        {
            throw UdbRecordingFailure("process is already being traced by pid {}", tracerPid);
        }
        std::this_thread::sleep_for(pollInterval);
    }
}

/// Spawns live-record attached to this process. The returned pid is the live-record controller
/// itself: the launcher script execs it, so the pid survives, and the caller must signal and reap
/// it exactly once.
pid_t spawnLiveRecorder(const RecordingConfig& config)
{
    const std::string udbBin = udbBinaryPath();
    const std::filesystem::path storeDir = recordingStoreDir();

    /// Build every string before fork() - malloc is not async-signal-safe in the child.
    const std::string pidStr = std::to_string(static_cast<int>(::getpid()));
    const std::string recordingPath = (storeDir / (config.traceName + ".undo")).string();

    /// Same for the argv itself.
    const std::vector<const char*> execArgs{
        udbBin.c_str(),
        "--pid",
        pidStr.c_str(),
        "--recording-file",
        recordingPath.c_str(),
        "--max-event-log-size",
        config.traceSize.c_str(),
        nullptr};

    NES_DEBUG("Spawning live-record (binary={}, pid={})", udbBin, pidStr);

    /// Pipe with O_CLOEXEC on the write end: exec closes it automatically on success.
    /// If execv fails the child writes a byte so the parent can report the error safely.
    std::array<int, 2> pipeFd{};
    if (::pipe2(pipeFd.data(), O_CLOEXEC) != 0)
    {
        throw UdbRecordingFailure("pipe2 failed: {}", getErrorMessageFromERRNO());
    }

    const pid_t child = ::fork();
    if (child == 0)
    {
        /// POSIX guarantees execv does not modify argv, hence the cast away from const.
        ::execv(udbBin.c_str(), const_cast<char* const*>(execArgs.data())); /// NOLINT(cppcoreguidelines-pro-type-const-cast)

        /// execv only returns on failure - only async-signal-safe calls allowed here.
        const char errByte = 1;
        static_cast<void>(::write(pipeFd[1], &errByte, 1));
        constexpr int exitExecFailed = 127;
        std::_Exit(exitExecFailed);
    }

    /// Parent: close write end, then check whether the child signalled failure.
    ::close(pipeFd[1]);

    if (child < 0)
    {
        /// Capture before close(), which may overwrite errno.
        const std::string forkError = getErrorMessageFromERRNO();
        ::close(pipeFd[0]);
        throw UdbRecordingFailure("fork failed: {}", forkError);
    }

    /// Grant ptrace permission to exactly this child; avoids having to lower yama/ptrace_scope to 0
    /// (c.f. man 2 prctl).
    if (::prctl(PR_SET_PTRACER, static_cast<int64_t>(child)) < 0) /// NOLINT(cppcoreguidelines-pro-type-vararg)
    {
        NES_ERROR("prctl(PR_SET_PTRACER) failed: {}", getErrorMessageFromERRNO());
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
    ::close(pipeFd[0]);

    if (nread == 1)
    {
        ::waitpid(child, nullptr, 0);
        throw UdbRecordingFailure("execv failed for binary '{}'", udbBin);
    }

    return child;
}

/// live-record attaches asynchronously, so without this wait a fast pipeline (e.g. INTERPRETER
/// mode, which skips the query compilation that would otherwise mask the attach latency) can run
/// and terminate the recorder before it ever attached, yielding an empty recording. Sources only
/// start once every pipeline's setup() has returned, so blocking here is what guarantees the whole
/// execution is recorded regardless of execution mode.
void waitUntilAttached(const pid_t udbPid)
{
    const auto deadline = std::chrono::steady_clock::now() + attachTimeout;
    while (currentTracerPid() == 0)
    {
        int status = 0;
        if (::waitpid(udbPid, &status, WNOHANG) == udbPid)
        {
            throw UdbRecordingFailure("live-record process {} exited before attaching (status={})", udbPid, status);
        }
        if (std::chrono::steady_clock::now() >= deadline)
        {
            /// A process can only have one tracer, so leaving a half-attached live-record behind
            /// would block every later recording for the lifetime of this process.
            ::kill(udbPid, SIGKILL);
            ::waitpid(udbPid, nullptr, 0);
            throw UdbRecordingFailure("timed out waiting for live-record process {} to attach", udbPid);
        }
        std::this_thread::sleep_for(pollInterval);
    }
    NES_DEBUG("live-record {} attached", udbPid);
}

/// Blocks until live-record has saved the trace and exited, so the trace is guaranteed finalized by
/// the time the caller returns.
void waitUntilSaved(const pid_t udbPid) noexcept
{
    const auto deadline = std::chrono::steady_clock::now() + saveTimeout;
    int status = 0;
    for (;;)
    {
        const pid_t reaped = ::waitpid(udbPid, &status, WNOHANG);
        if (reaped == udbPid)
        {
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
            {
                NES_ERROR("live-record process {} did not save the recording cleanly (status={})", udbPid, status);
            }
            return;
        }
        if (reaped < 0 && errno != EINTR)
        {
            NES_ERROR("Failed to reap live-record process {}: {}", udbPid, getErrorMessageFromERRNO());
            return;
        }
        if (std::chrono::steady_clock::now() >= deadline)
        {
            NES_ERROR("Timed out waiting for live-record process {} to save the recording, killing it", udbPid);
            ::kill(udbPid, SIGKILL);
            ::waitpid(udbPid, nullptr, 0);
            return;
        }
        std::this_thread::sleep_for(pollInterval);
    }
}

}

Recording::Recording(const RecordingConfig& config)
{
    /// Linux permits one tracer per process, and a distributed plan places a recording operator on
    /// every node - which is several operators in one process whenever those nodes are embedded
    /// workers rather than separate processes. Claim the process here so exactly one construction
    /// records it and the rest become no-ops.
    ///
    /// waitUntilNotTraced alone cannot do this: it only sees tracers that have already attached, so
    /// two constructions racing between spawn and attach both observe an untraced process, both
    /// spawn, and the loser later fails to save its trace.
    if (bool unclaimed = false; not processIsRecording.compare_exchange_strong(unclaimed, true))
    {
        NES_DEBUG("Process is already being recorded; skipping redundant recording '{}'", config.traceName);
        return;
    }

    /// Hands the claim back if the spawn or the attach throws, so one failed recording does not lock
    /// the process out of every later one.
    struct ClaimGuard
    {
        bool armed = true;

        ClaimGuard() = default;
        ClaimGuard(const ClaimGuard&) = delete;
        ClaimGuard(ClaimGuard&&) = delete;
        ClaimGuard& operator=(const ClaimGuard&) = delete;
        ClaimGuard& operator=(ClaimGuard&&) = delete;

        ~ClaimGuard()
        {
            if (armed)
            {
                processIsRecording.store(false);
            }
        }
    } claimGuard;

    /// Must precede the spawn: a still-attached previous recorder would make this one fail to attach.
    waitUntilNotTraced();
    udbPid = spawnLiveRecorder(config);
    waitUntilAttached(udbPid);
    claimGuard.armed = false;
}

Recording::~Recording()
{
    /// A construction that found the process already claimed owns no udb process to save or reap.
    if (udbPid == -1)
    {
        return;
    }

    if (::kill(udbPid, SIGUSR1) != 0)
    {
        NES_ERROR("Failed to signal live-record process {} to save the recording: {}", udbPid, getErrorMessageFromERRNO());
        ::waitpid(udbPid, nullptr, WNOHANG);
        processIsRecording.store(false);
        return;
    }
    waitUntilSaved(udbPid);
    processIsRecording.store(false);
}

}
