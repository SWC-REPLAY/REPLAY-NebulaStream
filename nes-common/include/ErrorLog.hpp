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
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <vector>
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

/// Passes the exceptions constructed on the threads of one worker to the Error sources reading them (discussion #153).
/// The log keeps no history: an error reaches only the subscriptions that exist when it is recorded. Errors that must be
/// kept are written into a sink by an error query.
class ErrorLog
{
public:
    /// The errors recorded since a reader subscribed, in order. The reader drains it; it is unbounded.
    class Subscription
    {
    public:
        /// Blocks until an error arrives; returns nothing if stop is requested.
        std::optional<ErrorRecord> read(const std::stop_token& stopToken);
        /// Returns nothing instead of blocking.
        std::optional<ErrorRecord> tryRead();

    private:
        friend class ErrorLog;
        void push(const ErrorRecord& record);

        std::mutex mutex;
        std::condition_variable_any recordPushed;
        std::deque<ErrorRecord> pending;
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

    /// Delivers every error recorded after this call, until the subscription is destroyed.
    std::shared_ptr<Subscription> subscribe();
    void append(const ErrorRecord& record);

    /// Exceptions constructed on threads whose worker id is `worker` are recorded in `log` while the registration lives.
    static Registration registerForWorker(const Host& worker, std::shared_ptr<ErrorLog> log);

    /// Fast path for the exception constructor: false while no worker has an error log.
    static bool anyRegistered() noexcept { return numberOfRegisteredLogs.load(std::memory_order_relaxed) > 0; }

    /// The log of the worker this thread belongs to (Thread::getThisWorkerNodeId()), or nullptr.
    static std::shared_ptr<ErrorLog> forThisThread();
    /// Called from the Exception constructor. Never throws: reporting a failure would construct another Exception.
    static void recordException(const Exception& exception) noexcept;

private:
    /// Mirrors the registry size. Trivially destructible, so it stays valid during static destruction.
    static inline std::atomic<size_t> numberOfRegisteredLogs{0}; /// NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

    std::mutex mutex;
    /// Expired ones are removed on the next append, so a reader unsubscribes by dropping its subscription.
    std::vector<std::weak_ptr<Subscription>> subscriptions;
};

}
