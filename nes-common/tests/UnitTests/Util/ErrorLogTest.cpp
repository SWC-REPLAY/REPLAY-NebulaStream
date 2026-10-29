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
#include <cstddef>
#include <cstdint>
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
constexpr size_t DEFAULT_RETENTION_BYTES = 1024UL * 1024UL;

ErrorRecord recordWithMessage(std::string message)
{
    return ErrorRecord{
        .code = ErrorCode::UnknownException, .message = std::move(message), .context = {}, .timestamp = std::chrono::system_clock::now()};
}

struct AvailableRecords
{
    std::vector<ErrorRecord> records;
    uint64_t numberOfMissedRecords = 0;
};

AvailableRecords readAvailable(ErrorLog& log, uint64_t& position)
{
    AvailableRecords available;
    for (auto result = log.tryRead(position); result.record || result.numberOfMissedRecords > 0; result = log.tryRead(position))
    {
        available.numberOfMissedRecords += result.numberOfMissedRecords;
        if (result.record)
        {
            available.records.push_back(std::move(*result.record));
        }
    }
    return available;
}
}

TEST(ErrorLogTest, PositionsOfEmptyAndFilledLog)
{
    ErrorLog log(DEFAULT_RETENTION_BYTES);
    EXPECT_EQ(log.oldestPosition(), 0);
    EXPECT_EQ(log.latestPosition(), 0);

    log.append(recordWithMessage("first"));
    log.append(recordWithMessage("second"));
    EXPECT_EQ(log.oldestPosition(), 0);
    EXPECT_EQ(log.latestPosition(), 2);
}

TEST(ErrorLogTest, ReadReturnsRecordsInOrderAndAdvancesPosition)
{
    ErrorLog log(DEFAULT_RETENTION_BYTES);
    for (const auto* message : {"a", "b"})
    {
        log.append(recordWithMessage(message));
    }

    uint64_t position = 0;
    const auto first = log.read(position, std::stop_token{});
    ASSERT_TRUE(first.record.has_value());
    EXPECT_EQ(first.record->message, "a");
    EXPECT_EQ(position, 1);

    const auto second = log.read(position, std::stop_token{});
    ASSERT_TRUE(second.record.has_value());
    EXPECT_EQ(second.record->message, "b");
    EXPECT_EQ(position, 2);

    EXPECT_FALSE(log.tryRead(position).record.has_value());
    EXPECT_EQ(position, 2);
}

TEST(ErrorLogTest, EvictsOldestRecordsBeyondRetentionAndCountsMissedOnes)
{
    /// Each record is a bit more than 10000 bytes, so 35000 bytes retain the newest three.
    ErrorLog log(35000);
    for (int i = 0; i < 10; ++i)
    {
        log.append(recordWithMessage(std::string(10000, static_cast<char>('0' + i))));
    }
    EXPECT_EQ(log.oldestPosition(), 7);
    EXPECT_EQ(log.latestPosition(), 10);

    uint64_t position = 0;
    const auto available = readAvailable(log, position);
    EXPECT_EQ(available.numberOfMissedRecords, 7);
    ASSERT_EQ(available.records.size(), 3);
    EXPECT_EQ(available.records.front().message.front(), '7');
    EXPECT_EQ(available.records.back().message.front(), '9');
    EXPECT_EQ(position, 10);
}

TEST(ErrorLogTest, ReadReturnsNothingWhenStopIsRequested)
{
    ErrorLog log(DEFAULT_RETENTION_BYTES);
    uint64_t position = log.latestPosition();
    std::stop_source stopSource;
    stopSource.request_stop();
    const auto result = log.read(position, stopSource.get_token());
    EXPECT_FALSE(result.record.has_value());
    EXPECT_EQ(result.numberOfMissedRecords, 0);
    EXPECT_EQ(position, 0);
}

TEST(ErrorLogTest, BlockedReadWakesUpOnAppend)
{
    ErrorLog log(DEFAULT_RETENTION_BYTES);
    std::optional<ErrorLog::ReadResult> result;
    std::jthread reader(
        [&]
        {
            uint64_t position = log.latestPosition();
            result = log.read(position, std::stop_token{});
        });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    log.append(recordWithMessage("late"));
    reader.join();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->record.has_value());
    EXPECT_EQ(result->record->message, "late");
}

TEST(ErrorLogTest, RegistryFindsTheLogOfTheThreadsWorker)
{
    const auto log = std::make_shared<ErrorLog>(DEFAULT_RETENTION_BYTES);
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
        auto registration = ErrorLog::registerForWorker(Host("moved-worker"), std::make_shared<ErrorLog>(DEFAULT_RETENTION_BYTES));
        moved.emplace(std::move(registration));
    }
    /// The moved-from registration went out of scope without unregistering.
    EXPECT_TRUE(ErrorLog::anyRegistered());
    moved.reset();
    EXPECT_FALSE(ErrorLog::anyRegistered());
}

TEST(ErrorLogTest, MoveAssignmentUnregistersThePreviousLog)
{
    const auto second = std::make_shared<ErrorLog>(DEFAULT_RETENTION_BYTES);
    auto registration = ErrorLog::registerForWorker(Host("first-worker"), std::make_shared<ErrorLog>(DEFAULT_RETENTION_BYTES));
    registration = ErrorLog::registerForWorker(Host("second-worker"), second);
    Thread firstWorker("first-worker", Host("first-worker"), [] { EXPECT_EQ(ErrorLog::forThisThread(), nullptr); });
    Thread secondWorker("second-worker", Host("second-worker"), [&] { EXPECT_EQ(ErrorLog::forThisThread(), second); });
}

}
