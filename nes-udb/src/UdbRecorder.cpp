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

#include <UdbRecorder.hpp>

#include <cerrno>
#include <chrono>
#include <functional>
#include <future>
#include <mutex>
#include <string>
#include <utility>
#include <unistd.h>

#include <Util/Logger/Logger.hpp>
#include <fmt/chrono.h>
#include <fmt/format.h>

#include <undolr.h>

namespace NES
{

namespace
{

/// Both causes are environmental rather than programming errors, and neither can be repaired from
/// inside an already running process, so spell out what has to change instead of just the code.
void logStartHint(const undolr_error_t error)
{
    switch (error)
    {
        case undolr_error_RSEQ_IN_USE:
            NES_ERROR("Restart the worker with GLIBC_TUNABLES=glibc.pthread.rseq=0 in its environment to allow recording");
            break;
        case undolr_error_NO_ATTACH_YAMA:
            NES_ERROR("LiveRecorder was denied ptrace access; lower /proc/sys/kernel/yama/ptrace_scope to allow recording");
            break;
        default:
            break;
    }
}

std::string defaultTraceName()
{
    return fmt::format("nes-{}-{:%Y%m%d-%H%M%S}", ::getpid(), std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now()));
}

}

UdbRecorder& UdbRecorder::instance()
{
    static UdbRecorder recorder;
    return recorder;
}

UdbRecorder::UdbRecorder() : recorderThread(&UdbRecorder::run, this)
{
}

/// Leaves an in-flight recording alone: the undolr_save_on_termination() armed by start() is what
/// writes a trace for a process that goes down without a matching saveAndStop().
UdbRecorder::~UdbRecorder()
{
    {
        const std::scoped_lock lock(mutex);
        shuttingDown = true;
    }
    slotChanged.notify_all();
    recorderThread.join();
}

void UdbRecorder::run()
{
    std::unique_lock lock(mutex);
    while (true)
    {
        slotChanged.wait(lock, [this] { return shuttingDown || pendingTask.valid(); });
        if (!pendingTask.valid())
        {
            return;
        }

        /// Unlocked around the call so that a LiveRecorder save, which suspends the process for as
        /// long as it takes to write the trace, does not also hold the queue's lock.
        auto task = std::move(pendingTask);
        pendingTask = {};
        lock.unlock();
        task();
        lock.lock();

        slotChanged.notify_all();
    }
}

void UdbRecorder::submit(std::function<void()> task)
{
    std::packaged_task<void()> packaged(std::move(task));
    auto completed = packaged.get_future();
    {
        std::unique_lock lock(mutex);
        slotChanged.wait(lock, [this] { return !pendingTask.valid(); });
        pendingTask = std::move(packaged);
    }
    slotChanged.notify_all();

    /// Callers rely on the LiveRecorder call having happened by the time this returns, so wait for
    /// the task itself rather than merely for it to be picked up.
    completed.wait();
}

void UdbRecorder::start(const Options& options)
{
    submit(
        [this, &options]
        {
            if (recording)
            {
                NES_ERROR("Already recording into '{}'; LiveRecorder records the whole process, so this request is ignored", recordingFile);
                return;
            }

            /// Must precede undolr_start(); the event log cannot be resized once recording runs.
            if (options.eventLogSizeBytes.has_value()
                && ::undolr_event_log_size_set(static_cast<long>(*options.eventLogSizeBytes)) != 0)
            {
                NES_ERROR("Failed to set the event log size to {} bytes, errno={}", *options.eventLogSizeBytes, errno);
            }

            auto error = undolr_error_NONE;
            if (::undolr_start(&error) != 0)
            {
                NES_ERROR("Failed to start recording: {}, errno={}", ::undolr_error_string(error), errno);
                logStartHint(error);
                return;
            }

            recordingFile = options.traceName.value_or(defaultTraceName()) + ".undo";
            if (::undolr_save_on_termination(recordingFile.c_str()) != 0)
            {
                NES_ERROR("Failed to arm save-on-termination, a crash will not produce a recording, errno={}", errno);
            }
            recording = true;
            NES_INFO("Recording into '{}'", recordingFile);
        });
}

void UdbRecorder::saveAndStop()
{
    submit(
        [this]
        {
            if (!recording)
            {
                return;
            }

            /// LiveRecorder suspends every thread in the process for the duration of the save.
            if (::undolr_save(recordingFile.c_str()) != 0)
            {
                NES_ERROR("Failed to save the recording to '{}', errno={}", recordingFile, errno);
            }
            else
            {
                NES_INFO("Saved the recording to '{}'", recordingFile);
            }

            /// Also cancels the save-on-termination armed by start().
            if (::undolr_stop(nullptr) != 0)
            {
                NES_ERROR("Failed to stop recording, errno={}", errno);
            }
            recording = false;
        });
}

}
