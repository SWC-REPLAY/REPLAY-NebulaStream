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
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// One exception as recorded by the ErrorLog.
struct ErrorRecord
{
    ErrorCode code;
    /// The factory message at construction, without its trailing newline. Context that a catch site appends later is not included.
    std::string message;
    /// LogContext (spdlog MDC) of the throwing thread, e.g. {"Task": "<queryId>-<pipelineId>"}; empty outside any scope.
    std::map<std::string, std::string> context;
    std::chrono::system_clock::time_point timestamp;
};

/// Records the exceptions constructed on the threads of one worker, so that queries can read them through an Error source
/// (discussion #153). All readers share one history bounded by `retentionBytes`. A reader keeps its own position (a sequence
/// number) instead of a private copy, so the log does not know its readers.
class ErrorLog
{
public:
    struct ReadResult
    {
        std::optional<ErrorRecord> record;
        /// Records that were evicted before this reader got to them.
        uint64_t numberOfMissedRecords = 0;
    };

    /// Owns a log and keeps it registered for a worker; unregisters on destruction. A moved-from registration does nothing.
    class Registration
    {
    public:
        Registration(const Registration&) = delete;
        Registration& operator=(const Registration&) = delete;
        Registration(Registration&& other) noexcept;
        Registration& operator=(Registration&& other) noexcept;
        ~Registration();

    private:
        friend class ErrorLog;
        Registration(Host worker, std::shared_ptr<ErrorLog> log);
        void unregister() noexcept;

        Host worker;
        std::shared_ptr<ErrorLog> log;
    };

    explicit ErrorLog(size_t retentionBytes);

    void append(ErrorRecord record);

    /// Starting positions for a new reader: replay what is retained, or only what comes next.
    [[nodiscard]] uint64_t oldestPosition() const;
    [[nodiscard]] uint64_t latestPosition() const;

    /// Blocks until a record at or after `position` exists or stop is requested, and advances `position` past the returned
    /// record. Returns no record if stop was requested.
    ReadResult read(uint64_t& position, const std::stop_token& stopToken);
    /// Like read, but returns no record instead of blocking.
    ReadResult tryRead(uint64_t& position);

    /// Exceptions constructed on threads whose worker id is `worker` are recorded in `log` while the registration lives.
    static Registration registerForWorker(const Host& worker, std::shared_ptr<ErrorLog> log);

    /// Fast path for the exception constructor: false while no worker has an error log.
    static bool anyRegistered() noexcept { return numberOfRegisteredLogs.load(std::memory_order_relaxed) > 0; }

    /// The log of the worker this thread belongs to (Thread::getThisWorkerNodeId()), or nullptr.
    static std::shared_ptr<ErrorLog> forThisThread();
    /// Called from the Exception constructor. Never throws: reporting a failure would construct another Exception.
    static void recordException(const Exception& exception) noexcept;

private:
    struct Entry
    {
        size_t bytes = 0;
        ErrorRecord record;
    };

    /// Requires `mutex`. Sequence numbers are contiguous, so the record at a position is at index `position - oldest`.
    [[nodiscard]] uint64_t oldestPositionLocked() const { return nextSequenceNumber - history.size(); }
    ReadResult readLocked(uint64_t& position);

    /// Mirrors the registry size. Trivially destructible, so it stays valid during static destruction.
    static inline std::atomic<size_t> numberOfRegisteredLogs{0}; /// NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

    const size_t retentionBytes;
    mutable std::mutex mutex;
    std::condition_variable_any recordAppended;
    /// Oldest at the front.
    std::deque<Entry> history;
    size_t retainedBytes = 0;
    uint64_t nextSequenceNumber = 0;
};

}
