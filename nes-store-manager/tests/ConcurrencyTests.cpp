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

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include <filesystem>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <FileStore.hpp>
#include <MemoryStore.hpp>
#include <Store.hpp>
#include <TimeRange.hpp>

namespace NES
{

class ConcurrencyTests : public ::testing::Test
{
protected:
    static constexpr size_t NUM_READERS = 4;
    static constexpr size_t NUM_WRITERS = 4;
    static constexpr size_t TUPLES_PER_BATCH = 10;
    /// Writers encode `value = ts * VALUE_TS_MULTIPLIER + writerId` so readers can detect a torn or mismatched record
    /// from its contents alone.
    static constexpr uint64_t VALUE_TS_MULTIPLIER = 10;
    /// Long enough to interleave writers and readers many times over, short enough to belong in an ordinary test run.
    static constexpr std::chrono::seconds DEFAULT_TEST_DURATION{10};

    Schema schema
        = Schema{}.addField("id", DataType::Type::UINT64).addField("value", DataType::Type::UINT64).addField("ts", DataType::Type::UINT64);

    std::shared_ptr<BufferManager> bufferManager = BufferManager::create();

    /// How long each stress test runs, from the TEST_DURATION_SECONDS env var.
    ///
    /// The default is short enough that these stay usable in an ordinary test run: at five minutes each, six of them
    /// added half an hour to every full ctest and looked like a hang rather than a test. Raise it via the env var to
    /// soak for races, which is what the knob is for — every assertion here is satisfied within the first moments, so
    /// a longer run buys interleavings, not coverage.
    static std::chrono::seconds getTestDuration()
    {
        if (const char* env = std::getenv("TEST_DURATION_SECONDS"))
        {
            const std::string_view text{env};
            int seconds = 0;
            const auto [parseEnd, errorCode] = std::from_chars(text.begin(), text.end(), seconds);
            if (errorCode == std::errc{} && parseEnd == text.end() && seconds > 0)
            {
                return std::chrono::seconds{seconds};
            }
            NES_WARNING("Ignoring malformed TEST_DURATION_SECONDS='{}', using the default", text);
        }
        return DEFAULT_TEST_DURATION;
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

    /// Writer loop: writes batches of records with the invariant (id=writerId, value=ts*10+writerId, ts=ts).
    void writerLoop(Store& store, size_t writerId, const std::atomic<bool>& stop, std::atomic<uint64_t>& totalWritten, bool shouldSleep)
    {
        const uint32_t recordSize = schema.getSizeOfSchemaInBytes();
        uint64_t nextTs = 1;
        uint64_t batchCount = 0;
        std::vector<uint8_t> record(recordSize);
        auto lastLog = std::chrono::steady_clock::now();
        constexpr auto logInterval = std::chrono::seconds(10);

        while (!stop.load(std::memory_order_relaxed))
        {
            for (size_t i = 0; i < TUPLES_PER_BATCH; ++i)
            {
                packRecord(record.data(), writerId, (nextTs * VALUE_TS_MULTIPLIER) + writerId, nextTs);
                store.writeRecord(record.data(), recordSize, Timestamp(nextTs), schema);
                ++nextTs;
            }
            totalWritten.fetch_add(TUPLES_PER_BATCH, std::memory_order_relaxed);
            ++batchCount;

            if (auto now = std::chrono::steady_clock::now(); now - lastLog >= logInterval)
            {
                NES_INFO("Writer {}: {} batches, {} tuples written", writerId, batchCount, totalWritten.load());
                lastLog = now;
            }

            if (shouldSleep)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        NES_INFO("Writer {} stopped: {} batches written", writerId, batchCount);
    }

    /// Reader loop: continuously reads and validates every tuple.
    /// When checkOrdering is true, asserts that timestamps are non-decreasing within each read.
    void readerLoop(
        Store& store,
        size_t readerId,
        size_t numWriters,
        const std::atomic<bool>& stop,
        std::atomic<uint64_t>& totalRead,
        bool checkOrdering)
    {
        const uint32_t recordSize = schema.getSizeOfSchemaInBytes();
        const TimeRange unbounded{
            .fieldName = "TS", .start = Timestamp(Timestamp::INITIAL_VALUE), .end = Timestamp(Timestamp::INVALID_VALUE)};
        const uint64_t maxTuplesPerBuffer = bufferManager->getBufferSize() / recordSize;
        auto lastLog = std::chrono::steady_clock::now();
        constexpr auto logInterval = std::chrono::seconds(10);
        uint64_t readCycles = 0;

        while (!stop.load(std::memory_order_relaxed))
        {
            auto readBuffer = bufferManager->getBufferBlocking();
            const uint64_t tuplesRead = store.read(readBuffer, schema, unbounded);

            ASSERT_LE(tuplesRead, maxTuplesPerBuffer) << "Reader " << readerId << " got more tuples than buffer capacity";

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

                    ASSERT_LT(id, numWriters) << "Reader " << readerId << " tuple " << t << ": id (" << id << ") out of writer range [0, "
                                              << numWriters << ")";

                    ASSERT_EQ(value, (ts * VALUE_TS_MULTIPLIER) + id)
                        << "Reader " << readerId << " tuple " << t << ": value (" << value << ") != ts*" << VALUE_TS_MULTIPLIER << "+id ("
                        << (ts * VALUE_TS_MULTIPLIER) + id << ") for writer " << id;

                    if (checkOrdering)
                    {
                        ASSERT_GE(ts, prevTs) << "Reader " << readerId << " tuple " << t << ": ts went backwards (" << ts << " < " << prevTs
                                              << ")";
                        prevTs = ts;
                    }
                }

                totalRead.fetch_add(tuplesRead, std::memory_order_relaxed);
            }

            ++readCycles;

            if (auto now = std::chrono::steady_clock::now(); now - lastLog >= logInterval)
            {
                NES_INFO("Reader {}: {} read cycles, {} tuples read so far", readerId, readCycles, totalRead.load());
                lastLog = now;
            }
        }
        NES_INFO("Reader {} stopped: {} read cycles", readerId, readCycles);
    }

    struct TestConfig
    {
        size_t numWriters = 1;
        size_t numReaders = NUM_READERS;
        bool writerSleep = true;
        bool checkOrdering = false;
    };

    struct TestResult
    {
        uint64_t totalWritten;
        uint64_t totalRead;
    };

    /// Run a concurrency test with the given store and configuration for the specified duration.
    /// Launches writer and reader threads, waits, joins, logs summary, and asserts basic results.
    /// Does NOT close the store — the caller is responsible for closing after any post-checks.
    TestResult runTest(Store& store, const TestConfig& config, std::chrono::seconds duration)
    {
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> totalWritten{0};
        std::atomic<uint64_t> totalRead{0};

        std::vector<std::thread> writers;
        writers.reserve(config.numWriters);
        for (size_t i = 0; i < config.numWriters; ++i)
        {
            writers.emplace_back(
                &ConcurrencyTests::writerLoop, this, std::ref(store), i, std::cref(stop), std::ref(totalWritten), config.writerSleep);
        }

        std::vector<std::thread> readers;
        readers.reserve(config.numReaders);
        for (size_t i = 0; i < config.numReaders; ++i)
        {
            readers.emplace_back(
                &ConcurrencyTests::readerLoop,
                this,
                std::ref(store),
                i,
                config.numWriters,
                std::cref(stop),
                std::ref(totalRead),
                config.checkOrdering);
        }

        std::this_thread::sleep_for(duration);
        stop.store(true, std::memory_order_relaxed);

        for (auto& wr : writers)
        {
            wr.join();
        }
        for (auto& rd : readers)
        {
            rd.join();
        }

        NES_INFO("Duration: {}s | Writers: {} | Readers: {}", duration.count(), config.numWriters, config.numReaders);
        NES_INFO("Total tuples written: {} | Total tuples read: {}", totalWritten.load(), totalRead.load());

        EXPECT_GT(totalWritten.load(), 0U);
        EXPECT_GT(totalRead.load(), 0U);

        return {.totalWritten = totalWritten.load(), .totalRead = totalRead.load()};
    }

    /// Create a unique temporary directory for FileStore tests.
    static std::filesystem::path createTempDir(const std::string& testName)
    {
        auto dir = std::filesystem::temp_directory_path()
            / ("nes_concurrency_" + testName + "_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(dir);
        return dir;
    }
};

/// Single-producer, multi-reader stress test.
///
/// Tests:
///   - Record integrity under concurrent read/write (value == ts * 10 + writerId)
///   - Monotonic timestamp ordering of reads (guaranteed with a single writer that appends in order)
///   - Buffer capacity is respected (tuplesRead <= maxTuplesPerBuffer)
///   - No torn or partial records visible to readers
///
/// Does NOT test:
///   - Multiple concurrent writers (see ConcurrentMultiProducerReadWrite_MemoryStore)
///   - Flush/eviction under write contention (single writer never contends on the mutex)
///   - TimeRange-filtered reads (uses unbounded range only)
TEST_F(ConcurrencyTests, ConcurrentReadWriteMemoryStore)
{
    auto store = makeStore<MemoryStore>(schema, MemoryStore::Config{}, bufferManager);
    store.open();
    runTest(store, {.numWriters = 1, .writerSleep = true, .checkOrdering = true}, getTestDuration());
    store.close();
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
///
/// Does NOT test:
///   - Monotonic timestamp ordering (not guaranteed with multiple writers appending in arrival order)
///   - TimeRange-filtered reads (uses unbounded range only)
///   - Per-writer completeness (does not verify every written record is eventually read)
TEST_F(ConcurrencyTests, ConcurrentMultiProducerReadWriteMemoryStore)
{
    auto store = makeStore<MemoryStore>(schema, MemoryStore::Config{}, bufferManager);
    store.open();
    runTest(store, {.numWriters = NUM_WRITERS, .writerSleep = true, .checkOrdering = false}, getTestDuration());
    store.close();
}

/// Single-producer wraparound stress test with small buffers and a small maxBufferCount.
///
/// Tests:
///   - Wraparound eviction correctness (oldest sealed buffers are evicted via pop_front)
///   - Verifies eviction actually occurred (totalWritten exceeds ring capacity)
///   - Verifies old data was evicted (smallest ts in final read > 1)
///   - Record integrity after eviction (new data written after eviction is not corrupted)
///   - Concurrent reads during active eviction (readers see consistent snapshots while
///     the writer triggers frequent pop_front on the deque)
///
/// Does NOT test:
///   - Multiple writers triggering eviction (see ConcurrentMultiProducerWraparound_MemoryStore)
///   - Flush to a next-level store (standalone MemoryStore only, no chaining)
///   - TimeRange-filtered reads (uses unbounded range only)
TEST_F(ConcurrencyTests, ConcurrentWraparoundMemoryStore)
{
    const uint32_t recordSize = schema.getSizeOfSchemaInBytes();
    /// Small buffers: 10 records per buffer. With maxBufferCount=4, the ring holds 40 records total.
    constexpr size_t tuplesPerBuffer = 10;
    constexpr size_t maxBufferCount = 4;
    auto smallBufferManager = BufferManager::create(tuplesPerBuffer * recordSize);
    auto store = makeStore<MemoryStore>(schema, MemoryStore::Config{.maxBufferCount = maxBufferCount}, smallBufferManager);
    store.open();

    /// Temporarily swap bufferManager so readerLoop uses the small one for read buffers.
    auto originalBufferManager = bufferManager;
    bufferManager = smallBufferManager;

    const auto result = runTest(store, {.numWriters = 1, .writerSleep = false, .checkOrdering = false}, getTestDuration());

    /// Verify wraparound actually occurred.
    const uint64_t ringCapacity = maxBufferCount * tuplesPerBuffer;
    EXPECT_GT(result.totalWritten, ringCapacity)
        << "Not enough data written to trigger wraparound (wrote " << result.totalWritten << ", ring capacity " << ringCapacity << ")";

    /// Read remaining data and verify old timestamps were evicted.
    auto readBuffer = smallBufferManager->getBufferBlocking();
    const TimeRange unbounded{.fieldName = "TS", .start = Timestamp(Timestamp::INITIAL_VALUE), .end = Timestamp(Timestamp::INVALID_VALUE)};
    const uint64_t tuplesRead = store.read(readBuffer, schema, unbounded);
    if (tuplesRead > 0)
    {
        /// The earliest record in the store should not be ts=1 — it must have been evicted.
        const uint64_t firstTs = readField(readBuffer.getAvailableMemoryArea<uint8_t>().data() + TS_OFFSET, 0);
        EXPECT_GT(firstTs, 1U) << "Earliest timestamp should have been evicted by wraparound";
    }

    store.close();
    bufferManager = originalBufferManager;
}

/// Multi-producer wraparound stress test with small buffers and a small maxBufferCount.
///
/// Tests:
///   - Multiple writers simultaneously triggering wraparound eviction (both contend on the
///     unique_lock during buffer sealing and pop_front)
///   - Verifies eviction actually occurred (totalWritten exceeds ring capacity)
///   - Verifies old data was evicted (smallest ts in final read > 1 for at least one writer)
///   - Record integrity when eviction and writes from different producers interleave
///   - Data overwrite detection during eviction (writer-specific invariant: value == ts * 10 + writerId)
///   - Deque consistency when multiple writers seal buffers and evict concurrently
///
/// Does NOT test:
///   - Monotonic timestamp ordering (not guaranteed with multiple writers)
///   - Flush to a next-level store (standalone MemoryStore only, no chaining)
///   - TimeRange-filtered reads (uses unbounded range only)
///   - Per-writer completeness (eviction intentionally discards old data)
TEST_F(ConcurrencyTests, ConcurrentMultiProducerWraparoundMemoryStore)
{
    const uint32_t recordSize = schema.getSizeOfSchemaInBytes();
    constexpr size_t tuplesPerBuffer = 10;
    constexpr size_t maxBufferCount = 4;
    auto smallBufferManager = BufferManager::create(tuplesPerBuffer * recordSize);
    auto store = makeStore<MemoryStore>(schema, MemoryStore::Config{.maxBufferCount = maxBufferCount}, smallBufferManager);
    store.open();

    auto originalBufferManager = bufferManager;
    bufferManager = smallBufferManager;

    const auto result = runTest(store, {.numWriters = NUM_WRITERS, .writerSleep = false, .checkOrdering = false}, getTestDuration());

    const uint64_t ringCapacity = maxBufferCount * tuplesPerBuffer;
    EXPECT_GT(result.totalWritten, ringCapacity)
        << "Not enough data written to trigger wraparound (wrote " << result.totalWritten << ", ring capacity " << ringCapacity << ")";

    /// Read remaining data and verify old timestamps were evicted.
    auto readBuffer = smallBufferManager->getBufferBlocking();
    const TimeRange unbounded{.fieldName = "TS", .start = Timestamp(Timestamp::INITIAL_VALUE), .end = Timestamp(Timestamp::INVALID_VALUE)};
    const uint64_t tuplesRead = store.read(readBuffer, schema, unbounded);
    if (tuplesRead > 0)
    {
        auto span = readBuffer.getAvailableMemoryArea<uint8_t>();
        /// Find the smallest ts across all records in the read buffer.
        uint64_t minTs = std::numeric_limits<uint64_t>::max();
        for (uint64_t t = 0; t < tuplesRead; ++t)
        {
            const uint64_t ts = readField(span.data() + (t * recordSize), TS_OFFSET);
            minTs = std::min(minTs, ts);
        }
        EXPECT_GT(minTs, 1U) << "Earliest timestamp should have been evicted by wraparound";
    }

    store.close();
    bufferManager = originalBufferManager;
}

/// Single-producer, multi-reader FileStore stress test.
///
/// Tests:
///   - Record integrity under concurrent read/write (value == ts * 10 + writerId)
///   - pread-based reading is thread-safe alongside pwrite-based writing
///   - Buffer capacity is respected (tuplesRead <= maxTuplesPerBuffer)
///   - No torn or partial records visible to readers
///
/// Does NOT test:
///   - Multiple concurrent writers (see ConcurrentMultiProducerReadWrite_FileStore)
///   - Monotonic timestamp ordering (FileStore row order depends on atomic tail reservation)
///   - TimeRange-filtered reads (uses unbounded range only)
TEST_F(ConcurrencyTests, ConcurrentReadWriteFileStore)
{
    auto tmpDir = createTempDir("concurrent_rw");
    auto store = makeStore<FileStore>(
        FileStore::Config{.storeName = "test", .storeDir = tmpDir.string(), .schemaText = "id:UINT64,value:UINT64,ts:UINT64"}, schema);
    store.open();
    runTest(store, {.numWriters = 1, .writerSleep = true, .checkOrdering = false}, getTestDuration());
    store.close();
    std::filesystem::remove_all(tmpDir);
}

/// Multi-producer, multi-reader FileStore stress test.
///
/// Tests:
///   - Record integrity under concurrent multi-writer access (value == ts * 10 + writerId)
///   - Writer thread safety: multiple threads call writeRecord simultaneously, testing
///     for torn records and interleaved writes via atomic tail + pwrite
///   - Data overwrite detection (writer-specific invariant ensures one writer's record cannot
///     be silently replaced by another's without failing the integrity check)
///   - Writer id validity (id must be in [0, NUM_WRITERS))
///   - pread-based reading is thread-safe alongside concurrent pwrite-based writing
///
/// Does NOT test:
///   - Monotonic timestamp ordering (not guaranteed with multiple writers)
///   - TimeRange-filtered reads (uses unbounded range only)
///   - Per-writer completeness (does not verify every written record is eventually read)
TEST_F(ConcurrencyTests, ConcurrentMultiProducerReadWriteFileStore)
{
    auto tmpDir = createTempDir("concurrent_mp_rw");
    auto store = makeStore<FileStore>(
        FileStore::Config{.storeName = "test", .storeDir = tmpDir.string(), .schemaText = "id:UINT64,value:UINT64,ts:UINT64"}, schema);
    store.open();
    runTest(store, {.numWriters = NUM_WRITERS, .writerSleep = true, .checkOrdering = false}, getTestDuration());
    store.close();
    std::filesystem::remove_all(tmpDir);
}

}
