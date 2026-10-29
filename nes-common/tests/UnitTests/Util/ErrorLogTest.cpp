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

#include <chrono>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <Thread.hpp>

namespace NES
{
namespace
{
ErrorRecord recordWithMessage(std::string message)
{
    return ErrorRecord{
        .code = ErrorCode::UnknownException, .message = std::move(message), .context = {}, .timestamp = std::chrono::system_clock::now()};
}

std::vector<ErrorRecord> readAvailable(ErrorLog::Subscription& subscription)
{
    std::vector<ErrorRecord> records;
    for (auto record = subscription.tryRead(); record; record = subscription.tryRead())
    {
        records.push_back(std::move(*record));
    }
    return records;
}
}

TEST(ErrorLogTest, SubscriptionReceivesRecordsInOrder)
{
    ErrorLog log;
    const auto subscription = log.subscribe();
    log.append(recordWithMessage("a"));
    log.append(recordWithMessage("b"));

    const auto first = subscription->read(std::stop_token{});
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->message, "a");
    const auto second = subscription->tryRead();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->message, "b");
    EXPECT_FALSE(subscription->tryRead().has_value());
}

TEST(ErrorLogTest, RecordsBeforeSubscribingAreNotKept)
{
    ErrorLog log;
    log.append(recordWithMessage("before"));
    const auto subscription = log.subscribe();
    log.append(recordWithMessage("after"));

    const auto records = readAvailable(*subscription);
    ASSERT_EQ(records.size(), 1);
    EXPECT_EQ(records.front().message, "after");
}

TEST(ErrorLogTest, EverySubscriptionReceivesEachRecord)
{
    ErrorLog log;
    const auto first = log.subscribe();
    auto dropped = log.subscribe();
    const auto second = log.subscribe();
    dropped.reset();
    log.append(recordWithMessage("shared"));

    EXPECT_EQ(readAvailable(*first).size(), 1);
    EXPECT_EQ(readAvailable(*second).size(), 1);
}

TEST(ErrorLogTest, ReadReturnsNothingWhenStopIsRequested)
{
    ErrorLog log;
    const auto subscription = log.subscribe();
    std::stop_source stopSource;
    stopSource.request_stop();
    EXPECT_FALSE(subscription->read(stopSource.get_token()).has_value());
}

TEST(ErrorLogTest, BlockedReadWakesUpOnAppend)
{
    ErrorLog log;
    const auto subscription = log.subscribe();
    std::optional<ErrorRecord> result;
    std::jthread reader([&] { result = subscription->read(std::stop_token{}); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    log.append(recordWithMessage("late"));
    reader.join();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->message, "late");
}

TEST(ErrorLogTest, RegistryFindsTheLogOfTheThreadsWorker)
{
    const auto log = std::make_shared<ErrorLog>();
    {
        const auto registration = ErrorLog::registerForWorker(Host("registry-worker"), log);
        EXPECT_TRUE(ErrorLog::anyRegistered());
        Thread sameWorker("same-worker", Host("registry-worker"), [&] { EXPECT_EQ(ErrorLog::forThisThread(), log); });
        Thread otherWorker("other-worker", Host("other-worker"), [] { EXPECT_EQ(ErrorLog::forThisThread(), nullptr); });
    }
    EXPECT_FALSE(ErrorLog::anyRegistered());
}

TEST(ErrorLogTest, MovedRegistrationUnregistersOnce)
{
    std::optional<ErrorLog::Registration> moved;
    {
        auto registration = ErrorLog::registerForWorker(Host("moved-worker"), std::make_shared<ErrorLog>());
        moved.emplace(std::move(registration));
    }
    /// The moved-from registration went out of scope without unregistering.
    EXPECT_TRUE(ErrorLog::anyRegistered());
    moved.reset();
    EXPECT_FALSE(ErrorLog::anyRegistered());
}

TEST(ErrorLogTest, MoveAssignmentUnregistersThePreviousLog)
{
    const auto second = std::make_shared<ErrorLog>();
    auto registration = ErrorLog::registerForWorker(Host("first-worker"), std::make_shared<ErrorLog>());
    registration = ErrorLog::registerForWorker(Host("second-worker"), second);
    Thread firstWorker("first-worker", Host("first-worker"), [] { EXPECT_EQ(ErrorLog::forThisThread(), nullptr); });
    Thread secondWorker("second-worker", Host("second-worker"), [&] { EXPECT_EQ(ErrorLog::forThisThread(), second); });
}

TEST(ErrorLogTest, ExceptionIsRecordedOnceWithItsLogContext)
{
    const auto log = std::make_shared<ErrorLog>();
    const auto registration = ErrorLog::registerForWorker(Host("hook-worker"), log);
    const auto subscription = log->subscribe();
    Thread worker(
        "hook-worker",
        Host("hook-worker"),
        []
        {
            const LogContext context("Task", "query-1");
            const auto exception = CannotOpenSource("test");
            const auto copy = exception; /// NOLINT(performance-unnecessary-copy-initialization) copies must not record again
        });
    worker = Thread{};

    const auto records = readAvailable(*subscription);
    ASSERT_EQ(records.size(), 1);
    const auto& record = records.front();
    EXPECT_EQ(record.code, ErrorCode::CannotOpenSource);
    EXPECT_EQ(record.message, "failed to open a source; test");
    EXPECT_EQ(record.context.at("Task"), "query-1");
}

TEST(ErrorLogTest, ExceptionOnAnotherWorkerIsNotRecorded)
{
    const auto log = std::make_shared<ErrorLog>();
    const auto registration = ErrorLog::registerForWorker(Host("recording-worker"), log);
    const auto subscription = log->subscribe();
    Thread otherWorker("other-worker", Host("unregistered-worker"), [] { [[maybe_unused]] const auto exception = CannotOpenSource("test"); });
    otherWorker = Thread{};
    EXPECT_FALSE(subscription->tryRead().has_value());
}

}
