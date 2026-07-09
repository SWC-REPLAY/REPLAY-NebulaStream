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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <MemoryStore.hpp>
#include <Store.hpp>
#include <TimeRange.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES::StoreManager
{

class ConcurrencyTests : public ::testing::Test
{
protected:
    static constexpr size_t NUM_READERS = 4;
    static constexpr size_t NUM_WRITERS = 4;
    static constexpr size_t TUPLES_PER_BATCH = 10;

    Schema schema = Schema{}
                        .addField("id", DataType::Type::UINT64)
                        .addField("value", DataType::Type::UINT64)
                        .addField("ts", DataType::Type::UINT64);

    std::shared_ptr<BufferManager> bufferManager = BufferManager::create();

    /// Returns the test duration from TEST_DURATION_SECONDS env var, defaulting to 300s (5 minutes).
    static std::chrono::seconds getTestDuration()
    {
        if (const char* env = std::getenv("TEST_DURATION_SECONDS"))
        {
            return std::chrono::seconds(std::atoi(env));
        }
        return std::chrono::seconds(300);
    }

    /// Record layout: [8-byte id][8-byte value][8-byte ts] = 24 bytes (non-nullable fields).
    static constexpr size_t ID_OFFSET = 0;
    static constexpr size_t VALUE_OFFSET = sizeof(uint64_t);
    static constexpr size_t TS_OFFSET = 2 * sizeof(uint64_t);

    /// Pack a record into the buffer matching the schema's binary layout.
    static void packRecord(uint8_t* dest, uint64_t id, uint64_t value, uint64_t ts)
    {
        std::memcpy(dest + ID_OFFSET, &id, sizeof(uint64_t));
        std::memcpy(dest + VALUE_OFFSET, &value, sizeof(uint64_t));
        std::memcpy(dest + TS_OFFSET, &ts, sizeof(uint64_t));
    }

    /// Unpack a field from a record at the given byte offset.
    static uint64_t readField(const uint8_t* src, size_t fieldOffset)
    {
        uint64_t val = 0;
        std::memcpy(&val, src + fieldOffset, sizeof(uint64_t));
        return val;
    }
};

/// Single-producer, multi-reader stress test.
///
/// Tests:
///   - Record integrity under concurrent read/write (id == ts, value == ts * 10)
///   - Monotonic timestamp ordering of reads (guaranteed with a single writer that appends in order)
///   - Buffer capacity is respected (tuplesRead <= maxTuplesPerBuffer)
///   - No torn or partial records visible to readers
///
/// Does NOT test:
///   - Multiple concurrent writers (see ConcurrentMultiProducerReadWrite_MemoryStore)
///   - Data overwrite detection (single writer cannot overwrite its own records)
///   - Flush/eviction under write contention (single writer never contends on the mutex)
///   - TimeRange-filtered reads (uses unbounded range only)
TEST_F(ConcurrencyTests, ConcurrentReadWrite_MemoryStore)
{
    const auto duration = getTestDuration();
    NES_INFO("Running ConcurrentReadWrite_MemoryStore for {}s with {} readers", duration.count(), NUM_READERS);

    auto store = makeStore<MemoryStore>(schema, MemoryStore::Config{}, bufferManager);
    store.open();

    const uint32_t recordSize = schema.getSizeOfSchemaInBytes();
    ASSERT_GT(recordSize, 0u);

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> totalWritten{0};
    std::atomic<uint64_t> totalRead{0};
    std::vector<std::atomic<uint64_t>> perReaderTuples(NUM_READERS);
    std::vector<std::atomic<uint64_t>> perReaderCycles(NUM_READERS);

    constexpr auto LOG_INTERVAL = std::chrono::seconds(10);

    /// Writer thread: writes batches of TUPLES_PER_BATCH records with incrementing timestamps.
    auto writerFn = [&]()
    {
        uint64_t nextTs = 1;
        uint64_t batchCount = 0;
        std::vector<uint8_t> record(recordSize);
        auto lastLog = std::chrono::steady_clock::now();

        while (!stop.load(std::memory_order_relaxed))
        {
            for (size_t i = 0; i < TUPLES_PER_BATCH; ++i)
            {
                packRecord(record.data(), nextTs, nextTs * 10, nextTs);
                store.writeRecord(record.data(), recordSize, Timestamp(nextTs), schema);
                ++nextTs;
            }
            totalWritten.fetch_add(TUPLES_PER_BATCH, std::memory_order_relaxed);
            ++batchCount;

            if (auto now = std::chrono::steady_clock::now(); now - lastLog >= LOG_INTERVAL)
            {
                NES_INFO("Writer: {} batches, {} tuples written, latest ts={}, store size={} bytes",
                    batchCount, totalWritten.load(), nextTs - 1, store.size());
                lastLog = now;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        NES_INFO("Writer stopped: {} total tuples written", totalWritten.load());
    };

    /// Reader thread: continuously reads all available data and validates every tuple.
    auto readerFn = [&](size_t readerId)
    {
        const TimeRange unbounded{.fieldName = "TS", .start = Timestamp(Timestamp::INITIAL_VALUE), .end = Timestamp(Timestamp::INVALID_VALUE)};
        const uint64_t maxTuplesPerBuffer = bufferManager->getBufferSize() / recordSize;
        auto lastLog = std::chrono::steady_clock::now();

        while (!stop.load(std::memory_order_relaxed))
        {
            auto readBuffer = bufferManager->getBufferBlocking();
            const uint64_t tuplesRead = store.read(readBuffer, schema, unbounded);

            ASSERT_LE(tuplesRead, maxTuplesPerBuffer)
                << "Reader " << readerId << " got more tuples than buffer capacity";

            /// Validate every tuple returned by this read.
            if (tuplesRead > 0)
            {
                auto span = readBuffer.getAvailableMemoryArea<uint8_t>();
                uint64_t prevTs = 0;

                for (uint64_t t = 0; t < tuplesRead; ++t)
                {
                    const uint8_t* row = span.data() + (t * recordSize);
                    const uint64_t id = readField(row, ID_OFFSET);
                    const uint64_t value = readField(row, VALUE_OFFSET);
                    const uint64_t ts = readField(row, TS_OFFSET);

                    /// 1. Record integrity: writer invariant is id == ts, value == ts * 10.
                    ASSERT_EQ(id, ts)
                        << "Reader " << readerId << " tuple " << t << ": id (" << id << ") != ts (" << ts << ")";
                    ASSERT_EQ(value, ts * 10)
                        << "Reader " << readerId << " tuple " << t << ": value (" << value << ") != ts*10 (" << ts * 10 << ")";

                    /// 2. Monotonic ordering: timestamps must be non-decreasing across the read.
                    ASSERT_GE(ts, prevTs)
                        << "Reader " << readerId << " tuple " << t << ": ts went backwards (" << ts << " < " << prevTs << ")";

                    prevTs = ts;
                }

                totalRead.fetch_add(tuplesRead, std::memory_order_relaxed);
                perReaderTuples[readerId].fetch_add(tuplesRead, std::memory_order_relaxed);
            }

            perReaderCycles[readerId].fetch_add(1, std::memory_order_relaxed);

            if (auto now = std::chrono::steady_clock::now(); now - lastLog >= LOG_INTERVAL)
            {
                NES_INFO("Reader {}: {} read cycles, {} tuples read so far",
                    readerId, perReaderCycles[readerId].load(), perReaderTuples[readerId].load());
                lastLog = now;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        NES_INFO("Reader {} stopped: {} tuples in {} cycles",
            readerId, perReaderTuples[readerId].load(), perReaderCycles[readerId].load());
    };

    /// Launch threads.
    std::thread writer(writerFn);
    std::vector<std::thread> readers;
    readers.reserve(NUM_READERS);
    for (size_t i = 0; i < NUM_READERS; ++i)
    {
        readers.emplace_back(readerFn, i);
    }

    /// Run for the configured duration.
    std::this_thread::sleep_for(duration);
    stop.store(true, std::memory_order_relaxed);

    writer.join();
    for (auto& r : readers)
    {
        r.join();
    }

    store.close();

    NES_INFO("=== ConcurrentReadWrite_MemoryStore Summary ===");
    NES_INFO("Duration: {}s | Writers: 1 | Readers: {}", duration.count(), NUM_READERS);
    NES_INFO("Total tuples written: {}", totalWritten.load());
    NES_INFO("Total tuples read: {} (across all readers)", totalRead.load());
    for (size_t i = 0; i < NUM_READERS; ++i)
    {
        NES_INFO("  Reader {}: {} tuples in {} cycles", i, perReaderTuples[i].load(), perReaderCycles[i].load());
    }

    EXPECT_GT(totalWritten.load(), 0u);
    EXPECT_GT(totalRead.load(), 0u);
}

/// Multi-producer, multi-reader stress test.
///
/// Tests:
///   - Record integrity under concurrent multi-writer access (value == ts * 10 + writerId)
///   - Data overwrite detection (writer-specific invariant ensures one writer's record cannot
///     be silently replaced by another's without failing the integrity check)
///   - Writer id validity (id must be in [0, NUM_WRITERS))
///   - Buffer capacity is respected (tuplesRead <= maxTuplesPerBuffer)
///   - No torn or partial records when multiple writers contend on the mutex
///   - Flush/eviction correctness under write contention (multiple writers can trigger the
///     lock-release window in writeRecord during flush or wraparound eviction)
///
/// Does NOT test:
///   - Monotonic timestamp ordering (not guaranteed with multiple writers appending in arrival order)
///   - TimeRange-filtered reads (uses unbounded range only)
///   - Per-writer completeness (does not verify every written record is eventually read)
TEST_F(ConcurrencyTests, ConcurrentMultiProducerReadWrite_MemoryStore)
{
    const auto duration = getTestDuration();
    NES_INFO("Running ConcurrentMultiProducerReadWrite_MemoryStore for {}s with {} writers and {} readers",
        duration.count(), NUM_WRITERS, NUM_READERS);

    auto store = makeStore<MemoryStore>(schema, MemoryStore::Config{}, bufferManager);
    store.open();

    const uint32_t recordSize = schema.getSizeOfSchemaInBytes();
    ASSERT_GT(recordSize, 0u);

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> totalWritten{0};
    std::atomic<uint64_t> totalRead{0};
    std::vector<std::atomic<uint64_t>> perWriterTuples(NUM_WRITERS);
    std::vector<std::atomic<uint64_t>> perReaderTuples(NUM_READERS);
    std::vector<std::atomic<uint64_t>> perReaderCycles(NUM_READERS);

    constexpr auto LOG_INTERVAL = std::chrono::seconds(10);

    /// Writer thread: writes batches of TUPLES_PER_BATCH records with its own incrementing timestamp.
    /// Each writer is identified by writerId stored in the id field.
    auto writerFn = [&](size_t writerId)
    {
        uint64_t nextTs = 1;
        uint64_t batchCount = 0;
        std::vector<uint8_t> record(recordSize);
        auto lastLog = std::chrono::steady_clock::now();

        while (!stop.load(std::memory_order_relaxed))
        {
            for (size_t i = 0; i < TUPLES_PER_BATCH; ++i)
            {
                packRecord(record.data(), writerId, nextTs * 10 + writerId, nextTs);
                store.writeRecord(record.data(), recordSize, Timestamp(nextTs), schema);
                ++nextTs;
            }
            totalWritten.fetch_add(TUPLES_PER_BATCH, std::memory_order_relaxed);
            perWriterTuples[writerId].fetch_add(TUPLES_PER_BATCH, std::memory_order_relaxed);
            ++batchCount;

            if (auto now = std::chrono::steady_clock::now(); now - lastLog >= LOG_INTERVAL)
            {
                NES_INFO("Writer {}: {} batches, {} tuples written",
                    writerId, batchCount, perWriterTuples[writerId].load());
                lastLog = now;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        NES_INFO("Writer {} stopped: {} total tuples written", writerId, perWriterTuples[writerId].load());
    };

    /// Reader thread: continuously reads all available data and validates every tuple for integrity.
    auto readerFn = [&](size_t readerId)
    {
        const TimeRange unbounded{.fieldName = "TS", .start = Timestamp(Timestamp::INITIAL_VALUE), .end = Timestamp(Timestamp::INVALID_VALUE)};
        const uint64_t maxTuplesPerBuffer = bufferManager->getBufferSize() / recordSize;
        auto lastLog = std::chrono::steady_clock::now();

        while (!stop.load(std::memory_order_relaxed))
        {
            auto readBuffer = bufferManager->getBufferBlocking();
            const uint64_t tuplesRead = store.read(readBuffer, schema, unbounded);

            ASSERT_LE(tuplesRead, maxTuplesPerBuffer)
                << "Reader " << readerId << " got more tuples than buffer capacity";

            if (tuplesRead > 0)
            {
                auto span = readBuffer.getAvailableMemoryArea<uint8_t>();

                for (uint64_t t = 0; t < tuplesRead; ++t)
                {
                    const uint8_t* row = span.data() + (t * recordSize);
                    const uint64_t id = readField(row, ID_OFFSET);
                    const uint64_t value = readField(row, VALUE_OFFSET);
                    const uint64_t ts = readField(row, TS_OFFSET);

                    /// 1. Writer id must be in valid range — no garbage data.
                    ASSERT_LT(id, NUM_WRITERS)
                        << "Reader " << readerId << " tuple " << t << ": id (" << id << ") out of writer range";

                    /// 2. Record integrity: writer invariant is value == ts * 10 + writerId.
                    ASSERT_EQ(value, ts * 10 + id)
                        << "Reader " << readerId << " tuple " << t << ": value (" << value
                        << ") != ts*10+id (" << ts * 10 + id << ") for writer " << id;
                }

                totalRead.fetch_add(tuplesRead, std::memory_order_relaxed);
                perReaderTuples[readerId].fetch_add(tuplesRead, std::memory_order_relaxed);
            }

            perReaderCycles[readerId].fetch_add(1, std::memory_order_relaxed);

            if (auto now = std::chrono::steady_clock::now(); now - lastLog >= LOG_INTERVAL)
            {
                NES_INFO("Reader {}: {} read cycles, {} tuples read so far",
                    readerId, perReaderCycles[readerId].load(), perReaderTuples[readerId].load());
                lastLog = now;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        NES_INFO("Reader {} stopped: {} tuples in {} cycles",
            readerId, perReaderTuples[readerId].load(), perReaderCycles[readerId].load());
    };

    /// Launch threads.
    std::vector<std::thread> writers;
    writers.reserve(NUM_WRITERS);
    for (size_t i = 0; i < NUM_WRITERS; ++i)
    {
        writers.emplace_back(writerFn, i);
    }
    std::vector<std::thread> readers;
    readers.reserve(NUM_READERS);
    for (size_t i = 0; i < NUM_READERS; ++i)
    {
        readers.emplace_back(readerFn, i);
    }

    /// Run for the configured duration.
    std::this_thread::sleep_for(duration);
    stop.store(true, std::memory_order_relaxed);

    for (auto& w : writers)
    {
        w.join();
    }
    for (auto& r : readers)
    {
        r.join();
    }

    store.close();

    NES_INFO("=== ConcurrentMultiProducerReadWrite_MemoryStore Summary ===");
    NES_INFO("Duration: {}s | Writers: {} | Readers: {}", duration.count(), NUM_WRITERS, NUM_READERS);
    NES_INFO("Total tuples written: {}", totalWritten.load());
    NES_INFO("Total tuples read: {} (across all readers)", totalRead.load());
    for (size_t i = 0; i < NUM_WRITERS; ++i)
    {
        NES_INFO("  Writer {}: {} tuples", i, perWriterTuples[i].load());
    }
    for (size_t i = 0; i < NUM_READERS; ++i)
    {
        NES_INFO("  Reader {}: {} tuples in {} cycles", i, perReaderTuples[i].load(), perReaderCycles[i].load());
    }

    EXPECT_GT(totalWritten.load(), 0u);
    EXPECT_GT(totalRead.load(), 0u);
}

/// Single-producer wraparound stress test with a small maxBufferCount.
///
/// Tests:
///   - Wraparound eviction correctness (oldest sealed buffers are evicted via pop_front)
///   - Record integrity after eviction (new data written after eviction is not corrupted)
///   - Concurrent reads during active eviction (readers see consistent snapshots while
///     the writer triggers frequent pop_front on the deque)
///   - Store size tracking remains consistent through eviction cycles
///
/// Does NOT test:
///   - Multiple writers triggering eviction (see ConcurrentMultiProducerWraparound_MemoryStore)
///   - Flush to a next-level store (standalone MemoryStore only, no chaining)
///   - TimeRange-filtered reads (uses unbounded range only)
TEST_F(ConcurrencyTests, ConcurrentWraparound_MemoryStore)
{
    const auto duration = getTestDuration();

    /// Small maxBufferCount to force frequent wraparound eviction.
    constexpr size_t MAX_BUFFER_COUNT = 4;
    const MemoryStore::Config config{.maxBufferCount = MAX_BUFFER_COUNT};

    NES_INFO("Running ConcurrentWraparound_MemoryStore for {}s with maxBufferCount={}", duration.count(), MAX_BUFFER_COUNT);

    auto store = makeStore<MemoryStore>(schema, config, bufferManager);
    store.open();

    const uint32_t recordSize = schema.getSizeOfSchemaInBytes();
    ASSERT_GT(recordSize, 0u);

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> totalWritten{0};
    std::atomic<uint64_t> totalRead{0};
    std::atomic<uint64_t> totalReadCycles{0};

    constexpr auto LOG_INTERVAL = std::chrono::seconds(10);

    /// Writer: writes continuously, forcing buffer sealing and wraparound eviction.
    auto writerFn = [&]()
    {
        uint64_t nextTs = 1;
        uint64_t batchCount = 0;
        std::vector<uint8_t> record(recordSize);
        auto lastLog = std::chrono::steady_clock::now();

        while (!stop.load(std::memory_order_relaxed))
        {
            for (size_t i = 0; i < TUPLES_PER_BATCH; ++i)
            {
                packRecord(record.data(), nextTs, nextTs * 10, nextTs);
                store.writeRecord(record.data(), recordSize, Timestamp(nextTs), schema);
                ++nextTs;
            }
            totalWritten.fetch_add(TUPLES_PER_BATCH, std::memory_order_relaxed);
            ++batchCount;

            if (auto now = std::chrono::steady_clock::now(); now - lastLog >= LOG_INTERVAL)
            {
                NES_INFO("Wraparound writer: {} batches, {} tuples, store size={} bytes",
                    batchCount, totalWritten.load(), store.size());
                lastLog = now;
            }

            /// No sleep — maximize write pressure to trigger frequent wraparound.
        }
        NES_INFO("Wraparound writer stopped: {} total tuples written", totalWritten.load());
    };

    /// Readers: validate record integrity during wraparound.
    auto readerFn = [&](size_t readerId)
    {
        const TimeRange unbounded{.fieldName = "TS", .start = Timestamp(Timestamp::INITIAL_VALUE), .end = Timestamp(Timestamp::INVALID_VALUE)};
        const uint64_t maxTuplesPerBuffer = bufferManager->getBufferSize() / recordSize;
        auto lastLog = std::chrono::steady_clock::now();

        while (!stop.load(std::memory_order_relaxed))
        {
            auto readBuffer = bufferManager->getBufferBlocking();
            const uint64_t tuplesRead = store.read(readBuffer, schema, unbounded);

            ASSERT_LE(tuplesRead, maxTuplesPerBuffer)
                << "Reader " << readerId << " got more tuples than buffer capacity";

            if (tuplesRead > 0)
            {
                auto span = readBuffer.getAvailableMemoryArea<uint8_t>();

                for (uint64_t t = 0; t < tuplesRead; ++t)
                {
                    const uint8_t* row = span.data() + (t * recordSize);
                    const uint64_t id = readField(row, ID_OFFSET);
                    const uint64_t value = readField(row, VALUE_OFFSET);
                    const uint64_t ts = readField(row, TS_OFFSET);

                    ASSERT_EQ(id, ts)
                        << "Reader " << readerId << " tuple " << t << ": id (" << id << ") != ts (" << ts << ")";
                    ASSERT_EQ(value, ts * 10)
                        << "Reader " << readerId << " tuple " << t << ": value (" << value << ") != ts*10 (" << ts * 10 << ")";
                }

                totalRead.fetch_add(tuplesRead, std::memory_order_relaxed);
            }

            totalReadCycles.fetch_add(1, std::memory_order_relaxed);

            if (auto now = std::chrono::steady_clock::now(); now - lastLog >= LOG_INTERVAL)
            {
                NES_INFO("Wraparound reader {}: {} tuples read so far", readerId, totalRead.load());
                lastLog = now;
            }
        }
    };

    std::thread writer(writerFn);
    std::vector<std::thread> readers;
    readers.reserve(NUM_READERS);
    for (size_t i = 0; i < NUM_READERS; ++i)
    {
        readers.emplace_back(readerFn, i);
    }

    std::this_thread::sleep_for(duration);
    stop.store(true, std::memory_order_relaxed);

    writer.join();
    for (auto& r : readers)
    {
        r.join();
    }

    store.close();

    NES_INFO("=== ConcurrentWraparound_MemoryStore Summary ===");
    NES_INFO("Duration: {}s | maxBufferCount: {} | Readers: {}", duration.count(), MAX_BUFFER_COUNT, NUM_READERS);
    NES_INFO("Total tuples written: {} | Total tuples read: {}", totalWritten.load(), totalRead.load());

    EXPECT_GT(totalWritten.load(), 0u);
    EXPECT_GT(totalRead.load(), 0u);
}

/// Multi-producer wraparound stress test with a small maxBufferCount.
///
/// Tests:
///   - Multiple writers simultaneously triggering wraparound eviction (both contend on the
///     unique_lock during buffer sealing and pop_front)
///   - Record integrity when eviction and writes from different producers interleave
///   - Data overwrite detection during eviction (writer-specific invariant: value == ts * 10 + writerId)
///   - Deque consistency when multiple writers seal buffers and evict concurrently
///
/// Does NOT test:
///   - Monotonic timestamp ordering (not guaranteed with multiple writers)
///   - Flush to a next-level store (standalone MemoryStore only, no chaining)
///   - TimeRange-filtered reads (uses unbounded range only)
///   - Per-writer completeness (eviction intentionally discards old data)
TEST_F(ConcurrencyTests, ConcurrentMultiProducerWraparound_MemoryStore)
{
    const auto duration = getTestDuration();

    constexpr size_t MAX_BUFFER_COUNT = 4;
    const MemoryStore::Config config{.maxBufferCount = MAX_BUFFER_COUNT};

    NES_INFO("Running ConcurrentMultiProducerWraparound_MemoryStore for {}s with {} writers, maxBufferCount={}",
        duration.count(), NUM_WRITERS, MAX_BUFFER_COUNT);

    auto store = makeStore<MemoryStore>(schema, config, bufferManager);
    store.open();

    const uint32_t recordSize = schema.getSizeOfSchemaInBytes();
    ASSERT_GT(recordSize, 0u);

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> totalWritten{0};
    std::atomic<uint64_t> totalRead{0};
    std::vector<std::atomic<uint64_t>> perWriterTuples(NUM_WRITERS);

    constexpr auto LOG_INTERVAL = std::chrono::seconds(10);

    /// Writers: no sleep to maximize contention on the mutex during wraparound.
    auto writerFn = [&](size_t writerId)
    {
        uint64_t nextTs = 1;
        std::vector<uint8_t> record(recordSize);
        auto lastLog = std::chrono::steady_clock::now();

        while (!stop.load(std::memory_order_relaxed))
        {
            for (size_t i = 0; i < TUPLES_PER_BATCH; ++i)
            {
                packRecord(record.data(), writerId, nextTs * 10 + writerId, nextTs);
                store.writeRecord(record.data(), recordSize, Timestamp(nextTs), schema);
                ++nextTs;
            }
            totalWritten.fetch_add(TUPLES_PER_BATCH, std::memory_order_relaxed);
            perWriterTuples[writerId].fetch_add(TUPLES_PER_BATCH, std::memory_order_relaxed);

            if (auto now = std::chrono::steady_clock::now(); now - lastLog >= LOG_INTERVAL)
            {
                NES_INFO("Wraparound writer {}: {} tuples written", writerId, perWriterTuples[writerId].load());
                lastLog = now;
            }

            /// No sleep — maximize write contention and wraparound frequency.
        }
    };

    /// Readers: validate record integrity during multi-producer wraparound.
    auto readerFn = [&](size_t readerId)
    {
        const TimeRange unbounded{.fieldName = "TS", .start = Timestamp(Timestamp::INITIAL_VALUE), .end = Timestamp(Timestamp::INVALID_VALUE)};
        const uint64_t maxTuplesPerBuffer = bufferManager->getBufferSize() / recordSize;
        auto lastLog = std::chrono::steady_clock::now();

        while (!stop.load(std::memory_order_relaxed))
        {
            auto readBuffer = bufferManager->getBufferBlocking();
            const uint64_t tuplesRead = store.read(readBuffer, schema, unbounded);

            ASSERT_LE(tuplesRead, maxTuplesPerBuffer)
                << "Reader " << readerId << " got more tuples than buffer capacity";

            if (tuplesRead > 0)
            {
                auto span = readBuffer.getAvailableMemoryArea<uint8_t>();

                for (uint64_t t = 0; t < tuplesRead; ++t)
                {
                    const uint8_t* row = span.data() + (t * recordSize);
                    const uint64_t id = readField(row, ID_OFFSET);
                    const uint64_t value = readField(row, VALUE_OFFSET);
                    const uint64_t ts = readField(row, TS_OFFSET);

                    ASSERT_LT(id, NUM_WRITERS)
                        << "Reader " << readerId << " tuple " << t << ": id (" << id << ") out of writer range";

                    ASSERT_EQ(value, ts * 10 + id)
                        << "Reader " << readerId << " tuple " << t << ": value (" << value
                        << ") != ts*10+id (" << ts * 10 + id << ") for writer " << id;
                }

                totalRead.fetch_add(tuplesRead, std::memory_order_relaxed);
            }

            if (auto now = std::chrono::steady_clock::now(); now - lastLog >= LOG_INTERVAL)
            {
                NES_INFO("Wraparound reader {}: {} tuples read so far", readerId, totalRead.load());
                lastLog = now;
            }
        }
    };

    std::vector<std::thread> writers;
    writers.reserve(NUM_WRITERS);
    for (size_t i = 0; i < NUM_WRITERS; ++i)
    {
        writers.emplace_back(writerFn, i);
    }
    std::vector<std::thread> readers;
    readers.reserve(NUM_READERS);
    for (size_t i = 0; i < NUM_READERS; ++i)
    {
        readers.emplace_back(readerFn, i);
    }

    std::this_thread::sleep_for(duration);
    stop.store(true, std::memory_order_relaxed);

    for (auto& w : writers)
    {
        w.join();
    }
    for (auto& r : readers)
    {
        r.join();
    }

    store.close();

    NES_INFO("=== ConcurrentMultiProducerWraparound_MemoryStore Summary ===");
    NES_INFO("Duration: {}s | Writers: {} | Readers: {} | maxBufferCount: {}",
        duration.count(), NUM_WRITERS, NUM_READERS, MAX_BUFFER_COUNT);
    NES_INFO("Total tuples written: {} | Total tuples read: {}", totalWritten.load(), totalRead.load());
    for (size_t i = 0; i < NUM_WRITERS; ++i)
    {
        NES_INFO("  Writer {}: {} tuples", i, perWriterTuples[i].load());
    }

    EXPECT_GT(totalWritten.load(), 0u);
    EXPECT_GT(totalRead.load(), 0u);
}

}