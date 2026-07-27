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

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

namespace NES
{

/// Drives Undo's LiveRecorder API to record this process into a replayable .undo trace.
///
/// Every call is funnelled onto one dedicated thread because the LiveRecorder API forbids
/// concurrent invocation: "It is not safe to call functions in this API concurrently, either from
/// two or more threads, or from the same thread with one of calls from a signal handler."
///
/// LiveRecorder records the whole process rather than an individual query, and rejects a second
/// undolr_start() with ALREADY_TRACED, so exactly one recorder exists per worker process and at
/// most one recording is in flight at a time.
///
/// Recording is best-effort: every failure is logged and swallowed so that a query never fails
/// because it could not be recorded.
class UdbRecorder
{
public:
    struct Options
    {
        /// Trace file name without the .undo suffix; synthesised from pid and wall clock if unset.
        std::optional<std::string> traceName;
        /// Size of LiveRecorder's circular event log, which bounds how much history a trace holds.
        std::optional<uint64_t> eventLogSizeBytes;
    };

    static UdbRecorder& instance();

    /// Returns once the process is being recorded, so that everything a caller does afterwards is
    /// guaranteed to be part of the trace.
    void start(const Options& options);

    /// Writes the trace and stops recording. Saving suspends every thread in the process until it
    /// completes.
    void saveAndStop();

    UdbRecorder(const UdbRecorder&) = delete;
    UdbRecorder& operator=(const UdbRecorder&) = delete;
    UdbRecorder(UdbRecorder&&) = delete;
    UdbRecorder& operator=(UdbRecorder&&) = delete;

private:
    UdbRecorder();
    ~UdbRecorder();

    /// Runs task on the recorder thread and blocks until it has completed.
    void submit(std::function<void()> task);
    void run();

    std::mutex mutex;
    /// Signals both "a task is waiting" to the recorder thread and "the slot is free" to submitters.
    std::condition_variable slotChanged;
    std::packaged_task<void()> pendingTask;
    bool shuttingDown{false};
    std::thread recorderThread;

    /// Only ever touched from within a submitted task, hence unguarded.
    bool recording{false};
    std::string recordingFile;
};

}
