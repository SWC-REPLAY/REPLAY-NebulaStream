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
#include <cstdint>

/// Stage timers for the store, compiled in only when NES_STORE_PROFILING is defined.
///
/// Why this exists at all: a benchmark that calls the codecs itself measures a REPLICA of the
/// store's work, not the store's work. It agrees on the codecs and the order and then silently
/// disagrees about everything the store does around them -- the transpose it fuses into the column
/// build, the compressor it skips when the result grew, the bytes it writes per group. These timers
/// are inside the real path, so what they report is what actually ran.
///
/// WHAT IS NOT HERE, AND WHY. There is no timer inside writeRecord. It is called once per row --
/// 42,700,000 times on the solar dataset -- and two clock reads each is about two seconds against a
/// write that takes five and a half. The timer would be a third of the measurement. So the per-row
/// cost (the lock, the preconditions, the pending insert, the timestamp min/max) is DERIVED as the
/// write total minus `sealGroup`, and the three cannot be separated from one another. Everything
/// below is at group or read-call granularity, where ~20 ns per sample is noise.
///
/// Counters are global rather than per-store because the codec stages run in ColumnCodec.cpp, which
/// has no store to hang them off. That makes them meaningless with two stores running at once;
/// `reset()` before a measurement and do one thing at a time.
namespace NES::StoreProfile
{

/// Nanoseconds accumulated per stage. Atomic because `read` holds only a shared lock, so several
/// readers may add at once; relaxed ordering throughout, since nothing synchronises on these.
struct Counters
{
    /// ---- write path -------------------------------------------------------------------
    /// Whole of sealGroup. The write total minus this is the per-row writeRecord overhead.
    std::atomic<uint64_t> sealGroupNs{0};
    /// Row-major bytes scattered into per-column arrays. This IS the transpose -- the store
    /// builds columns directly rather than staging a row-major copy first.
    std::atomic<uint64_t> transposeOutNs{0};
    std::atomic<uint64_t> quantizeNs{0};
    std::atomic<uint64_t> bitpackNs{0};
    /// The byte compressor over the encoded column. Note it runs on every column but its output
    /// is KEPT only when smaller, so this is paid whether or not it is used.
    std::atomic<uint64_t> compressNs{0};
    /// Appending the serialized group to the file.
    std::atomic<uint64_t> writeIoNs{0};
    std::atomic<uint64_t> groupsSealed{0};

    /// ---- read path --------------------------------------------------------------------
    /// Whole of read(). Minus the four below is cursor and buffer bookkeeping.
    std::atomic<uint64_t> readTotalNs{0};
    std::atomic<uint64_t> readIoNs{0};
    std::atomic<uint64_t> decompressNs{0};
    std::atomic<uint64_t> decodeNs{0};
    /// Reconstructed columns scattered back into row-major order.
    std::atomic<uint64_t> transposeInNs{0};
    std::atomic<uint64_t> readCalls{0};
};

/// A plain copy of the counters at one instant. Separate from `Counters` because that holds
/// std::atomic, which is neither copyable nor movable -- so a caller cannot take one home, store it
/// beside a wall-clock reading, or keep the best of several repeats without this.
struct Snapshot
{
    uint64_t sealGroupNs = 0;
    uint64_t transposeOutNs = 0;
    uint64_t quantizeNs = 0;
    uint64_t bitpackNs = 0;
    uint64_t compressNs = 0;
    uint64_t writeIoNs = 0;
    uint64_t groupsSealed = 0;
    uint64_t readTotalNs = 0;
    uint64_t readIoNs = 0;
    uint64_t decompressNs = 0;
    uint64_t decodeNs = 0;
    uint64_t transposeInNs = 0;
    uint64_t readCalls = 0;

    /// Seconds, which is what a report wants.
    [[nodiscard]] double s(const uint64_t field) const { return static_cast<double>(field) / 1e9; }
};

inline Counters& counters()
{
    static Counters instance;
    return instance;
}

inline Snapshot snapshot()
{
    const Counters& c = counters();
    const auto get = [](const std::atomic<uint64_t>& a) { return a.load(std::memory_order_relaxed); };
    return Snapshot{
        .sealGroupNs = get(c.sealGroupNs),
        .transposeOutNs = get(c.transposeOutNs),
        .quantizeNs = get(c.quantizeNs),
        .bitpackNs = get(c.bitpackNs),
        .compressNs = get(c.compressNs),
        .writeIoNs = get(c.writeIoNs),
        .groupsSealed = get(c.groupsSealed),
        .readTotalNs = get(c.readTotalNs),
        .readIoNs = get(c.readIoNs),
        .decompressNs = get(c.decompressNs),
        .decodeNs = get(c.decodeNs),
        .transposeInNs = get(c.transposeInNs),
        .readCalls = get(c.readCalls),
    };
}

inline void reset()
{
    Counters& c = counters();
    for (auto* f : {&c.sealGroupNs, &c.transposeOutNs, &c.quantizeNs, &c.bitpackNs, &c.compressNs,
                    &c.writeIoNs, &c.groupsSealed, &c.readTotalNs, &c.readIoNs, &c.decompressNs,
                    &c.decodeNs, &c.transposeInNs, &c.readCalls})
    {
        f->store(0, std::memory_order_relaxed);
    }
}

#ifdef NES_STORE_PROFILING

/// Adds its lifetime to one counter. Accumulating on destruction rather than per-sample keeps the
/// hot path to two clock reads and one relaxed add.
class Scope
{
public:
    explicit Scope(std::atomic<uint64_t>& sink) : sink(sink), start(std::chrono::steady_clock::now()) { }
    Scope(const Scope&) = delete;
    Scope& operator=(const Scope&) = delete;
    Scope(Scope&&) = delete;
    Scope& operator=(Scope&&) = delete;
    ~Scope()
    {
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
        sink.fetch_add(static_cast<uint64_t>(ns), std::memory_order_relaxed);
    }

private:
    std::atomic<uint64_t>& sink;
    std::chrono::steady_clock::time_point start;
};

#define NES_PROFILE_CONCAT_INNER(a, b) a##b
#define NES_PROFILE_CONCAT(a, b) NES_PROFILE_CONCAT_INNER(a, b)
/// Times the enclosing scope into counters().<field>.
#define NES_PROFILE_SCOPE(field) \
    const ::NES::StoreProfile::Scope NES_PROFILE_CONCAT(nesProfileScope, __LINE__) \
    { \
        ::NES::StoreProfile::counters().field \
    }
#define NES_PROFILE_COUNT(field) \
    ::NES::StoreProfile::counters().field.fetch_add(1, std::memory_order_relaxed)

#else

/// Not merely empty: the arguments must still parse, so a typo in a field name is caught by every
/// build rather than only by the profiling one.
#define NES_PROFILE_SCOPE(field) static_assert(sizeof(&::NES::StoreProfile::Counters::field) > 0, "")
#define NES_PROFILE_COUNT(field) static_assert(sizeof(&::NES::StoreProfile::Counters::field) > 0, "")

#endif

}
