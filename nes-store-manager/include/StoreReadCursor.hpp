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

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include <ErrorHandling.hpp>

namespace NES
{

/// Where a reader got to inside a store chain.
///
/// A read fills the buffer the caller provides and stops when it is full, so a result larger than
/// one buffer takes several calls. This is what makes the later calls continue rather than repeat
///
/// Owned by the READER, never by a store. One store instance is shared by the registry, the write
/// handler and any number of readers, so a position kept inside it would be corrupted by the second
/// reader and would force `read` to take a unique lock. Keeping it out here leaves reads concurrent
/// and the store effectively stateless, which is also how every other source in the tree tracks its
/// own offset.
struct StoreReadCursor
{
    /// One frame per tier. A chain is at most three tiers today (MemoryStore -> FileStore ->
    /// LossyStore | CompressedStore); the fourth is slack so adding one does not silently overflow.
    static constexpr size_t MAX_TIERS = 4;

    struct Frame
    {
        /// The unit -- buffer, segment or group -- this tier stopped inside.
        uint64_t unitIndex{0};

        /// How many rows of that unit have been CONSUMED, which means SCANNED rather than emitted.
        ///
        /// The two differ exactly where it matters. A unit straddling a range bound is filtered row
        /// by row, so a call may scan 100 rows and emit 40 of them; resuming at 40 would re-scan
        /// the 60 already rejected and re-emit any that matched. Counting scanned rows is what
        /// makes the resume point mean "the next row nobody has looked at yet".
        uint64_t rowOffset{0};

        /// The tier's unit generation when this frame was first used. Units are addressed by index,
        /// and an index only stays meaningful while the tier appends; a drain or an eviction
        /// renumbers everything. Recording the generation turns that into a thrown error instead of
        /// silently skipped or repeated rows.
        uint64_t generation{0};

        /// Whether `generation` holds a real value yet. A frame starts at zero, and so may a tier,
        /// so "unset" cannot be spelled as a generation value.
        bool started{false};

        /// This tier has nothing further for this range. Set when its units run out, and read by
        /// the tier above so that a zero return means "done" rather than "empty this time".
        bool exhausted{false};

        /// The unit named by `decodedUnit`, already decoded into rows.
        ///
        /// Only the compressing tiers fill it. A destination smaller than a unit means a reader
        /// re-enters the unit it stopped in, and for MemoryStore and FileStore that costs a memcpy
        /// from an offset -- but CompressedStore decompresses the whole segment again, and
        /// LossyStore decodes every column and transposes them again. Holding the decoded bytes
        /// here makes that once per UNIT rather than once per DESTINATION BUFFER: at a 1 MB
        /// segment and a 4 KB destination it is the difference between 1 and 257 decodes.
        ///
        /// Here and not on the store deliberately. `read` takes a shared lock, so bytes kept on
        /// the store would be a data race between concurrent readers and would need their own
        /// mutex plus an eviction rule -- and would cost the store the stateless property this
        /// file's header commits to. Per cursor it needs neither, and `generation` above is
        /// already the invalidation: bindCursorFrame throws if the tier renumbered its units, so
        /// bytes decoded against an older numbering cannot be reached.
        std::vector<uint8_t> decoded;
        uint64_t decodedUnit{0};
        bool decodedValid{false};
    };

    std::array<Frame, MAX_TIERS> frames{};
};

/// Bind `frame` to the tier's current generation on first use, and afterwards check it has not
/// moved.
///
/// Throws rather than asserting, and deliberately: PRECONDITION and INVARIANT both call
/// std::terminate, which would take every other query on the worker down with this one, and both
/// compile away under NO_ASSERT -- leaving exactly the silent corruption this guard exists to
/// prevent in the build where it matters most.
///
/// @throws UnknownOperation when the tier dropped or renumbered its units between two reads sharing
///         this cursor, which makes `unitIndex` refer to data other than what it was recorded
///         against.
inline void bindCursorFrame(StoreReadCursor::Frame& frame, const uint64_t tierGeneration, const std::string_view tierName)
{
    if (!frame.started)
    {
        frame.generation = tierGeneration;
        frame.started = true;
        return;
    }
    if (frame.generation != tierGeneration)
    {
        throw UnknownOperation(
            "{} dropped or renumbered its units while a read was in progress (cursor holds generation {}, store is at {}). A store must "
            "not be flushed, drained or evicted while it is being read.",
            tierName,
            frame.generation,
            tierGeneration);
    }
}

}
