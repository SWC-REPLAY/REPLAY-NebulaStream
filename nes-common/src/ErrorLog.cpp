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

#include <ErrorLog.hpp>

#include <atomic>
#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Util/Strings.hpp>
#include <spdlog/mdc.h>
#include <ErrorHandling.hpp>
#include <TaskContext.hpp>
#include <Thread.hpp>

namespace NES
{

namespace
{
struct StringHash
{
    using is_transparent = void;

    size_t operator()(const std::string_view value) const noexcept { return std::hash<std::string_view>{}(value); }
};

struct Registry
{
    std::mutex mutex;
    /// Keyed by the raw worker id, so that a lookup with Host::view() does not copy the string (std::hash<Host> does).
    std::unordered_map<std::string, std::weak_ptr<ErrorLog>, StringHash, std::equal_to<>> logs;
};

/// Leaked on purpose: exceptions can be constructed during static destruction (e.g. on Rust network threads), after a
/// static registry would be gone.
Registry& registry()
{
    static auto* const Instance = new Registry(); /// NOLINT(cppcoreguidelines-owning-memory)
    return *Instance;
}
}

std::optional<ErrorRecord> ErrorLog::Subscription::read(const std::stop_token& stopToken)
{
    std::unique_lock lock(mutex);
    if (!recordPushed.wait(lock, stopToken, [this] { return !pending.empty(); }))
    {
        return std::nullopt;
    }
    auto record = std::move(pending.front());
    pending.pop_front();
    return record;
}

std::optional<ErrorRecord> ErrorLog::Subscription::tryRead()
{
    const std::scoped_lock lock(mutex);
    if (pending.empty())
    {
        return std::nullopt;
    }
    auto record = std::move(pending.front());
    pending.pop_front();
    return record;
}

void ErrorLog::Subscription::push(const ErrorRecord& record)
{
    {
        const std::scoped_lock lock(mutex);
        pending.push_back(record);
    }
    recordPushed.notify_all();
}

std::shared_ptr<ErrorLog::Subscription> ErrorLog::subscribe()
{
    auto subscription = std::make_shared<Subscription>();
    const std::scoped_lock lock(mutex);
    subscriptions.push_back(subscription);
    return subscription;
}

void ErrorLog::append(const ErrorRecord& record)
{
    const std::scoped_lock lock(mutex);
    std::erase_if(subscriptions, [](const auto& subscription) { return subscription.expired(); });
    for (const auto& subscription : subscriptions)
    {
        if (const auto live = subscription.lock())
        {
            live->push(record);
        }
    }
}

ErrorLog::Registration::Registration(Host worker, std::shared_ptr<ErrorLog> log) : worker(std::move(worker)), log(std::move(log))
{
}

ErrorLog::Registration::Registration(Registration&& other) noexcept : worker(std::move(other.worker)), log(std::move(other.log))
{
}

ErrorLog::Registration& ErrorLog::Registration::operator=(Registration&& other) noexcept
{
    if (this != &other)
    {
        unregister();
        worker = std::move(other.worker);
        log = std::move(other.log);
    }
    return *this;
}

ErrorLog::Registration::~Registration()
{
    unregister();
}

void ErrorLog::Registration::unregister() noexcept
{
    if (!log)
    {
        return;
    }
    auto& [mutex, logs] = registry();
    const std::scoped_lock lock(mutex);
    if (const auto entry = logs.find(worker.view()); entry != logs.end())
    {
        logs.erase(entry);
    }
    numberOfRegisteredLogs.store(logs.size(), std::memory_order_relaxed);
    log.reset();
}

ErrorLog::Registration ErrorLog::registerForWorker(const Host& worker, std::shared_ptr<ErrorLog> log)
{
    auto& [mutex, logs] = registry();
    const std::scoped_lock lock(mutex);
    const auto [entry, inserted] = logs.try_emplace(std::string(worker.view()), log);
    INVARIANT(inserted, "Worker {} already has an error log", worker);
    numberOfRegisteredLogs.store(logs.size(), std::memory_order_relaxed);
    return Registration{worker, std::move(log)};
}

std::shared_ptr<ErrorLog> ErrorLog::forThisThread()
{
    auto& [mutex, logs] = registry();
    const std::scoped_lock lock(mutex);
    if (const auto entry = logs.find(Thread::getThisWorkerNodeId().view()); entry != logs.end())
    {
        return entry->second.lock();
    }
    return nullptr;
}

void ErrorLog::recordException(const Exception& exception) noexcept
{
    try
    {
        if (const auto log = forThisThread())
        {
            /// Relies on LogContext writing the spdlog MDC in every build type (NesLogger.hpp). Engine tasks set `Task`,
            /// query start sets `queryId`.
            const auto& logContext = spdlog::mdc::get_context();
            const auto task = logContext.find("Task");
            const auto query = task != logContext.end() ? task : logContext.find("queryId");
            auto [queryId, distributedQueryId, pipelineId] = query != logContext.end() ? parseTaskContext(query->second) : ParsedTaskContext{};
            log->append(
                ErrorRecord{
                    .code = exception.code(),
                    /// Factories with a message end it in '\n' (ErrorHandling.hpp); it is not part of the error.
                    .message = std::string(trimCharsRight(exception.what(), '\n')),
                    .queryId = std::move(queryId),
                    .distributedQueryId = std::move(distributedQueryId),
                    .pipelineId = pipelineId,
                    .timestamp = exception.when()});
        }
    }
    /// Dropping the record is the only safe option: reporting would itself construct an Exception and recurse.
    /// NOLINTNEXTLINE(bugprone-empty-catch)
    catch (...) /// NOLINT(no-raw-catch-all)
    {
    }
}

}
