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

#include <string>
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
/// Only the first live object does the recording. Linux permits one tracer per process, and a
/// distributed plan places a recording operator on every node - which is several operators in one
/// process whenever those nodes are embedded workers rather than separate ones. Constructing while
/// another object already owns the process succeeds and does nothing, so callers never have to know
/// how nodes map onto processes.
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
    /// pid of the udb process, or -1 when another object already owned the process at construction
    /// and this one records nothing.
    pid_t udbPid{-1};
};

}
