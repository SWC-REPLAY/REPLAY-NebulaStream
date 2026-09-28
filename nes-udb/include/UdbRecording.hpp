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

#include <filesystem>
#include <optional>
#include <string>
#include <thread>
#include <sys/types.h>

namespace NES
{

struct RecordingConfig
{
    /// Without the .undo suffix.
    std::string traceName;
    /// History bound, as SIZE[KB|MB|GB].
    std::string traceSize;
};

/// RAII owner of the udb process that records this NES process.
///
/// Construction returns only once udb has attached, so everything after it is in the trace.
/// Destruction saves the trace, waits for it to be written, and reaps udb. A live object therefore
/// means "this process is being recorded" - there is no idle state.
///
/// Neither copyable nor movable: the pid must be signalled and reaped exactly once. Construct it in
/// place (e.g. std::optional::emplace) inside whatever owns the recording's lifetime.
class Recording
{
public:
    /// @throws UdbRecordingFailure if udb could not be spawned or never attached.
    explicit Recording(const RecordingConfig& config);

    /// Saves the trace and reaps udb; returns only once the trace is on disk.
    ~Recording();

    Recording(const Recording&) = delete;
    Recording& operator=(const Recording&) = delete;
    Recording(Recording&&) = delete;
    Recording& operator=(Recording&&) = delete;

private:
    /// pid of the udb process. Always valid for a live object: construction either attaches or throws.
    pid_t udbPid{-1};
};

/// Owns the recording of one TIME_TRAVEL_UDB pipeline, immediate or deferred.
///
/// If NES_UDB_RECORD_ON_TRIGGER is unset, recording starts in the constructor, exactly like Recording.
/// If it names a file (relative paths resolve against the working directory), a watcher thread
/// starts live-record once that file appears. This lets a worker be checkpointed with CRIU while
/// no tracer is attached (CRIU cannot seize a ptraced process): the watcher thread is restored with
/// the process, and the trigger file is created only in the restored shadow's private runtime dir.
///
/// A deferred recording that fails to attach is logged, not thrown: the query is already running
/// by then and there is no caller left to fail.
class RecordingSession
{
public:
    /// @throws UdbRecordingFailure if an immediate recording could not be started.
    explicit RecordingSession(RecordingConfig config);

    /// Stops the watcher, then saves the trace if recording started.
    ~RecordingSession() = default;

    RecordingSession(const RecordingSession&) = delete;
    RecordingSession& operator=(const RecordingSession&) = delete;
    RecordingSession(RecordingSession&&) = delete;
    RecordingSession& operator=(RecordingSession&&) = delete;

private:
    void recordOnceTriggered(const std::stop_token& stopToken, const std::filesystem::path& triggerPath);

    RecordingConfig config;
    /// Written only by the watcher thread until it is joined.
    std::optional<Recording> recording;
    /// Declared last so it is joined before recording is destroyed.
    std::jthread watcher;
};

}
