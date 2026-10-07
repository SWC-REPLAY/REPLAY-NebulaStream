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

#include <FileStore.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <sys/types.h>
#include <ErrorHandling.hpp>
#include <FlushPolicy.hpp>
#include <ReplayStoreFormat.hpp>
#include <Store.hpp>
#include <StoreTransformation.hpp>
#include <StoreTypeRegistry.hpp>
#include <TimeRange.hpp>

namespace NES
{

namespace
{
std::string generateFilePath(const FileStore::Config& cfg, const uint32_t partNumber)
{
    const auto now = std::chrono::system_clock::now();
    const auto timeT = std::chrono::system_clock::to_time_t(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::ostringstream ts;
    ts << std::put_time(std::localtime(&timeT), "%Y%m%d_%H%M%S") << '_' << std::setfill('0') << std::setw(3) << ms.count();
    std::ostringstream part;
    part << std::setfill('0') << std::setw(4) << partNumber;
    /// The part number, not just the timestamp: two rolls can land in the same millisecond.
    return cfg.storeDir + "/replay_" + cfg.storeName + "_" + ts.str() + "_" + part.str() + ".bin";
}
}

FileStore::FileStore(Config config, const Schema& schema) : config(std::move(config)), schema(schema)
{
    parts.emplace_back();
}

FileStore::FileStore(Config config, const Schema& schema, Store nextLevel, StoreTransformation transformation, FlushPolicy policy)
    : config(std::move(config))
    , schema(schema)
    , nextLevel(std::move(nextLevel))
    , transformation(std::move(transformation))
    , flushPolicy(policy)
{
}

FileStore::~FileStore() = default;

uint32_t FileStore::segmentsPerFile() const
{
    /// A file is the capacity divided by how many files hold it, and it must hold at least one
    /// segment or it could never make progress.
    const size_t perFile = config.totalSize / std::max<size_t>(config.filesAtCapacity, 1);
    return static_cast<uint32_t>(std::max<size_t>(perFile / std::max<size_t>(config.segmentSize, 1), 1));
}

uint64_t FileStore::dataBytesLocked() const
{
    /// Rows only, each file's header and preallocated table subtracted. This is what a FLUSH
    /// THRESHOLD is measured against.

    uint64_t total = 0;
    for (const auto& part : parts)
    {
        if (part.writer)
        {
            total += part.writer->size() - part.dataStart;
        }
    }
    return total;
}

uint64_t FileStore::storedBytesLocked() const
{
    /// The whole footprint, framing included. This is what CAPACITY is measured against: a disk
    /// budget has to count the bytes the disk actually holds, and the table is among them.
    uint64_t total = 0;
    for (const auto& part : parts)
    {
        if (part.writer)
        {
            total += part.writer->size();
        }
    }
    return total;
}

void FileStore::evictOldestOverCapacity()
{
    /// The oldest file goes whole, which is the entire reason for rolling: retention costs one
    /// unlink, with no rewrite and no holes to track. The newest file is never dropped -- it is the
    /// one being written so a capacity smaller than a single file degrades to keeping that one.
    bool dropped = false;
    while (parts.size() > 1 && storedBytesLocked() > config.totalSize)
    {
        parts.front().writer->close();
        parts.front().writer->removeFile();
        /// Nothing to count. Unit positions are global and never reused, and every surviving part
        /// already carries its own firstUnit, so a file leaving renumbers nothing.
        parts.erase(parts.begin());
        ++evictedFiles;
        dropped = true;
    }

    if (!dropped)
    {
        return;
    }


    /// After eviction, timestamps are updated.
    fileMinTs = Timestamp(Timestamp::INVALID_VALUE);
    fileMaxTs = Timestamp(Timestamp::INITIAL_VALUE);
    for (const auto& part : parts)
    {
        fileMinTs = Timestamp(std::min(fileMinTs.getRawValue(), part.minTs.getRawValue()));
        fileMaxTs = Timestamp(std::max(fileMaxTs.getRawValue(), part.maxTs.getRawValue()));
    }
    if (activeSegment.byteLength > 0)
    {
        fileMinTs = Timestamp(std::min(fileMinTs.getRawValue(), activeSegment.minTs));
        fileMaxTs = Timestamp(std::max(fileMaxTs.getRawValue(), activeSegment.maxTs));
    }
}

void FileStore::openCurrentPart()
{
    auto& part = current();
    part.path = generateFilePath(config, nextPartNumber++);
    part.writer = std::make_unique<BinaryStoreWriter>(BinaryStoreWriter::Config{
        .storeName = config.storeName,
        .filePath = part.path,
        .schemaText = config.schemaText,
        .segmentSize = config.segmentSize,
        /// Exactly the segments this file can hold before it rolls. Right-sized rather than a cap:
        /// the file rolls when the table fills, so the two conditions are the same one.
        .maxSegments = segmentsPerFile()});
    part.writer->open();
    part.writer->ensureHeader();
    part.dataStart = dataStartOffsetFor(config.schemaText.size(), segmentsPerFile());
    part.segments.clear();
    part.minTs = Timestamp(Timestamp::INVALID_VALUE);
    part.maxTs = Timestamp(Timestamp::INITIAL_VALUE);
}

void FileStore::rollToNextFile()
{
    /// The finished file is NOT closed , its rows are still part of this store and a read has to
    /// reach them. It simply stops being written to.
    ///
    /// Read before the emplace: `current()` is `parts.back()`, so growing the vector first would
    /// make this the number of the new, empty part rather than of the one being finished.
    const uint64_t nextFirstUnit = current().firstUnit + current().segments.size();
    parts.emplace_back();
    parts.back().firstUnit = nextFirstUnit;
    openCurrentPart();
}

void FileStore::open()
{
    std::unique_lock lock(mutex);
    fileMinTs = Timestamp(Timestamp::INVALID_VALUE);
    fileMaxTs = Timestamp(Timestamp::INITIAL_VALUE);
    /// Start from one empty file. Anything left from a previous open is dropped
    parts.clear();
    parts.emplace_back();
    nextPartNumber = 0;
    openCurrentPart();
    activeSegment = SegmentDescriptor{};
    activeBytes.clear();
    writerOpened = true;
    lock.unlock();
    if (nextLevel)
    {
        nextLevel->open();
    }
}

void FileStore::close([[maybe_unused]] Store& self)
{
    {
        const std::unique_lock lock(mutex);
        if (writerOpened)
        {
            /// The segment still being filled is held in memory, so closing without sealing it
            /// drops every row since the last seal -- up to one segment's worth. Reads inside this
            /// process would still find them, which is what makes the loss quiet: it only shows up
            /// in the file, and only once someone opens it again.
            sealActiveSegment();
            for (auto& part : parts)
            {
                if (part.writer)
                {
                    part.writer->close();
                }
            }
            writerOpened = false;
        }
    }
    if (nextLevel)
    {
        nextLevel->close();
    }
}

void FileStore::flush(Store& self)
{
    if (!nextLevel || !transformation)
    {
        return;
    }

    /// This tier's own policy decides when its contents move down. Until now it never asked:
    /// MemoryStore::flush ends with nextLevel->flush(), so FileStore drained everything on every
    /// MemoryStore flush and never held anything at all. Its capacity, its rolling and its
    /// eviction were unreachable in a chain -- every chained measurement showed 0 data bytes
    /// here, the file on disk nothing but a header and an empty table.
    ///
    /// A tier with no policy -- a tail, or a standalone store -- still drains on demand, because
    /// the guard only applies when there is a policy to consult.
    uint64_t accumulated = 0;
    {
        const std::shared_lock lock(mutex);
        accumulated = dataBytesLocked();
    }
    if (flushPolicy && !flushPolicy->shouldFlush(accumulated))
    {
        return;
    }

    transformation->execute(self, *nextLevel);
    nextLevel->flush();
}

void FileStore::writeRecord(const uint8_t* recordData, uint32_t recordSize, Timestamp ts, [[maybe_unused]] Store& self)
{
    PRECONDITION(ts.getRawValue() != Timestamp::INVALID_VALUE, "FileStore was passed a record with an invalid timestamp!");


    /// Accumulated into the segment being filled, exactly as MemoryStore accumulates a record
    /// into the buffer being filled. A record does NOT become a segment of its own -- the
    /// segment seals when it reaches `segmentSize`, so this path builds the same segments the
    /// bulk path does and nothing has to special-case it.
    appendBuffer(recordData, recordSize, 1, ts, ts);

    /// Check flush policy and flush to next level if triggered (mirrors MemoryStore::writeRecord).
    /// Measured across every rolled file, not just the one being written -- they are all still this
    /// store's data until something drains them.
    uint64_t stored = 0;
    {
        const std::shared_lock lock(mutex);
        /// The same measure flush uses -- one threshold, one meaning.
        stored = dataBytesLocked();
    }
    if (flushPolicy && flushPolicy->shouldFlush(stored))
    {
        flush(self);
    }
}

void FileStore::appendBuffer(
    const uint8_t* data, const size_t len, const uint64_t rowCount, const Timestamp minTs, const Timestamp maxTs)
{
    PRECONDITION(writerOpened, "FileStore must be opened before writing");
    if (len == 0)
    {
        return;
    }

    /// Serialises the whole append: the segment being filled, the file-wide range and the
    /// append frontier have to move together, or a concurrent writer could land its bytes
    /// inside another's segment.
    const std::unique_lock lock(mutex);

    /// Held in memory until the segment seals. The descriptor is written immediately in front of
    /// its payload, and it is not final until then -- byteLength, rowCount and the range are all
    /// still moving -- so the bytes cannot go down before it. Where they land is settled at seal.
    activeBytes.insert(activeBytes.end(), data, data + len);

    activeSegment.byteLength += len;
    activeSegment.rowCount += rowCount;

    /// Union rather than overwrite: a segment spans every buffer that went into it. This is the
    /// whole point -- the range is carried down from where it was measured, never recomputed by
    /// reading the rows back.
    if (minTs.getRawValue() != Timestamp::INVALID_VALUE)
    {
        activeSegment.minTs = std::min(activeSegment.minTs, minTs.getRawValue());
        activeSegment.maxTs = std::max(activeSegment.maxTs, maxTs.getRawValue());

        if (minTs < fileMinTs)
        {
            fileMinTs = minTs;
        }
        if (maxTs > fileMaxTs)
        {
            fileMaxTs = maxTs;
        }
    }

    if (activeSegment.byteLength >= config.segmentSize)
    {
        sealActiveSegment();
    }
}

void FileStore::sealActiveSegment()
{
    if (activeSegment.byteLength == 0)
    {
        return;
    }

    /// [descriptor][payload], written together. byteOffset points past the descriptor at the rows
    /// themselves, so a read needs no second hop, and nothing has to be reserved up front: the file
    /// costs SEGMENT_DESCRIPTOR_BYTES per segment that actually exists and nothing for ones that
    /// do not. There is no slot count left to outgrow.
    /// A full table and a full file are the same condition, because the table is sized to the roll
    /// point. So this is where the file rolls -- and it is why the table can never be outgrown, the
    /// failure the old unbounded version had.
    if (current().segments.size() >= segmentsPerFile())
    {
        rollToNextFile();
    }

    auto& part = current();

    /// The payload is appended past the table, which stays at the front where one read reaches all
    /// of it. Offsets are relative to THIS file, so a descriptor only means anything with its part.
    activeSegment.byteOffset = part.writer->size() - part.dataStart;
    part.writer->append(activeBytes.data(), activeBytes.size());
    activeBytes.clear();

    /// The descriptor goes into its preallocated slot, in place.
    const auto index = static_cast<uint32_t>(part.segments.size());
    part.writer->writeAt(&activeSegment, SEGMENT_DESCRIPTOR_BYTES, segmentSlotOffset(config.schemaText.size(), index));
    const uint32_t sealedCount = index + 1;
    part.writer->writeAt(&sealedCount, sizeof(sealedCount), sealedCountOffset(config.schemaText.size()));

    /// Each file carries its own range, not the store's, so a reader holding one file can dismiss
    /// it on its own terms. Written here rather than per append: it is a pwrite, and the append
    /// path carries one row at a time when this store stands alone.
    part.minTs = Timestamp(std::min(part.minTs.getRawValue(), activeSegment.minTs));
    part.maxTs = Timestamp(std::max(part.maxTs.getRawValue(), activeSegment.maxTs));
    part.writer->updateTimestamps(part.minTs.getRawValue(), part.maxTs.getRawValue());

    part.segments.push_back(activeSegment);
    activeSegment = SegmentDescriptor{};

    /// Capacity is enforced after the write, never before it: the newest rows always land, and the
    /// oldest file leaves to make room.
    evictOldestOverCapacity();
}

size_t FileStore::debugSegmentCount() const
{
    const std::shared_lock lock(mutex);
    size_t total = 0;
    for (const auto& part : parts)
    {
        total += part.segments.size();
    }
    return total;
}

namespace
{
/// Compute the byte offset of a field within a row, using the packed binary layout.
std::optional<uint32_t> findFieldOffset(const Schema& schema, const std::string& fieldName)
{
    uint32_t offset = 0;
    for (size_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        const auto& field = schema.getFieldAt(i);
        if (field.getUnqualifiedName() == fieldName)
        {
            /// Past the null indicator when the field has one. Returning the field's start
            /// instead and assuming an indicator unconditionally reads a value straddling two
            /// fields on a non-nullable schema, which matches nothing and silently drops every
            /// row.
            return offset + (field.dataType.nullable ? 1 : 0);
        }
        offset += field.dataType.isType(DataType::Type::VARSIZED) ? sizeof(uint32_t) : field.dataType.getSizeInBytesWithNull();
    }
    return std::nullopt;
}

/// How far a filtering pass got, in both senses. The two differ whenever the range drops rows, and
/// a resumable read needs them apart: `kept` says how much of the destination was used, `scanned`
/// says which row nobody has looked at yet.
struct FilterProgress
{
    uint64_t kept{0};
    uint64_t scanned{0};
};

/// Filter rows in a buffer in-place, keeping only rows whose timestamp field falls within the range.
/// Stops once `maxKept` rows have been kept, so the caller can fill a destination exactly.
FilterProgress filterBufferRows(
    const std::span<char> data,
    const uint64_t numRows,
    const uint32_t rowWidth,
    const uint32_t tsFieldOffset,
    const TimeRange& range,
    const uint64_t maxKept)
{
    FilterProgress progress;
    for (uint64_t i = 0; i < numRows && progress.kept < maxKept; ++i)
    {
        const auto row = data.subspan(i * rowWidth, rowWidth);
        uint64_t tsValue = 0;
        /// tsFieldOffset already points past any null indicator.
        const auto tsBytes = row.subspan(tsFieldOffset, sizeof(uint64_t));
        std::memcpy(&tsValue, tsBytes.data(), tsBytes.size());
        if (range.contains(Timestamp(tsValue)))
        {
            if (progress.kept != i)
            {
                std::memmove(data.subspan(progress.kept * rowWidth, rowWidth).data(), row.data(), rowWidth);
            }
            ++progress.kept;
        }
        progress.scanned = i + 1;
    }
    return progress;
}

}

std::optional<FileStore::SegmentLocation> FileStore::locateUnitLocked(const uint64_t globalUnit) const
{
    for (const Part& part : parts)
    {
        if (part.holds(globalUnit))
        {
            return SegmentLocation{globalUnit, part.unitAt(globalUnit), part.writer.get(), part.dataStart};
        }
    }
    return std::nullopt;
}

FileStore::ReadableUnits FileStore::collectReadableLocked(
    const uint64_t fromUnit, const uint64_t fromRowOffset, const uint64_t capacityRows, const TimeRange& range,
    const uint32_t tupleSize) const
{
    ReadableUnits readable;
    /// The unsealed segment sits one past the last sealed unit of the last part. It has no
    /// descriptor in any file header yet, but its rows are already readable.
    const uint64_t unsealedUnit = current().firstUnit + current().segments.size();
    const uint64_t budget = capacityRows + fromRowOffset;
    uint64_t gathered = 0;

    for (uint64_t unit = fromUnit; gathered < budget; ++unit)
    {
        SegmentLocation location;
        if (const auto sealed = locateUnitLocked(unit))
        {
            location = *sealed;
        }
        else if (unit == unsealedUnit && activeSegment.byteLength > 0)
        {
            location = SegmentLocation{unit, activeSegment, nullptr, current().dataStart};
        }
        else
        {
            readable.reachedEnd = true;
            break;
        }

        /// Pruned here, so a unit the range cannot match is never carried out of the lock. Its rows
        /// do not count against the budget either -- skipping it bought no destination space.
        if (!range.isUnbounded()
            && !range.overlaps(Timestamp(location.descriptor.minTs), Timestamp(location.descriptor.maxTs)))
        {
            continue;
        }

        gathered += location.descriptor.byteLength / tupleSize;
        readable.locations.push_back(location);
    }
    return readable;
}

uint64_t FileStore::read(TupleBuffer& buffer, const Schema& readSchema, const TimeRange& range, StoreReadCursor& cursor, size_t depth)
{
    PRECONDITION(depth + 1 < StoreReadCursor::MAX_TIERS, "store chain is deeper than a cursor can describe ({} tiers)", depth + 1);

    if (nextLevel && !cursor.frames.at(depth + 1).exhausted)
    {
        NES_DEBUG("Checking if next level has data to be read");
        if (const uint64_t nextLevelRead = nextLevel->read(buffer, readSchema, range, cursor, depth + 1); nextLevelRead != 0)
        {
            return nextLevelRead;
        }
        cursor.frames.at(depth + 1).exhausted = true;
    }

    /// Skip entire file if its timestamp range falls outside the query range
    if (!range.isUnbounded())
    {
        const std::shared_lock lock(mutex);
        if (!range.overlaps(fileMinTs, fileMaxTs))
        {
            NES_DEBUG("FileStore::read: skipping all {} files (ts range outside query range)", parts.size());
            return 0;
        }
    }

    const uint32_t tupleSize = calculateRowWidth(readSchema);
    PRECONDITION(tupleSize > 0, "Schema must have at least one field to compute row width");

    /// The field the range filters on, resolved once rather than per segment.
    std::optional<uint32_t> tsOffset;
    if (!range.isUnbounded())
    {
        tsOffset = findFieldOffset(readSchema, range.fieldName);
        PRECONDITION(tsOffset.has_value(), "TimeRange field '{}' not found in schema", range.fieldName);
    }

    /// Resolved before the lock: it reads the destination buffer and not the store, and the
    /// collector below needs it to know when it has gathered enough units.
    auto destSpan = buffer.getAvailableMemoryArea<char>();
    const uint64_t capacity = destSpan.size() / tupleSize;
    /// A destination that cannot hold one row would return zero forever, and a zero is read as
    /// end of stream -- so the whole replay would come back empty with no error at all.
    PRECONDITION(capacity > 0, "destination buffer of {} bytes cannot hold a {}-byte row", destSpan.size(), tupleSize);

    /// Snapshot under the lock, then read without holding it: pread is position-independent, and a
    /// concurrent writer only ever appends past what is snapshotted. The writers live on the heap,
    /// so growing `parts` does not move them and the pointers stay good once the lock is dropped.
    ///
    /// Only the units THIS CALL can consume are taken. Flattening every unit in the store to serve
    /// one destination used to cost a vector of every segment on every call -- 4,560 entries built
    /// to read 146 rows, on each of 292,466 calls of a full replay.
    ReadableUnits readable;
    /// The unsealed segment is held in memory, not on disk, so its bytes are copied out here.
    /// Only this tier serves unsealed rows at all.
    std::vector<uint8_t> activeSnapshot;
    uint64_t rowOffset = 0;
    auto& frame = cursor.frames.at(depth);
    {
        const std::shared_lock lock(mutex);
        bindCursorFrame(frame, readGeneration, typeName());

        /// Where this read had got to may have been evicted. Those rows are genuinely gone, so it
        /// resumes at the oldest unit still held rather than failing -- a replay of a store under
        /// a retention limit is allowed to lose its tail, not to stop working.
        const uint64_t oldestUnit = parts.front().firstUnit;
        const uint64_t fromUnit = std::max(frame.unitIndex, oldestUnit);
        rowOffset = frame.unitIndex < oldestUnit ? 0 : frame.rowOffset;

        readable = collectReadableLocked(fromUnit, rowOffset, capacity, range, tupleSize);
        if (!readable.locations.empty() && readable.locations.back().writer == nullptr)
        {
            activeSnapshot = activeBytes;
        }
    }

    uint64_t destTuples = 0;

    /// Two decisions per unit, as MemoryStore makes them per buffer: take all of it, or inspect its
    /// rows. The third -- skip it -- was already made under the lock, which is what makes the skip
    /// real: a span from the first match to the last would read everything in between, and segments
    /// are held in write order, so a late arrival puts a non-matching one between two matches.
    size_t taken = 0;
    for (; taken < readable.locations.size(); ++taken, rowOffset = 0)
    {
        if (destTuples >= capacity)
        {
            break;
        }

        const SegmentLocation& location = readable.locations[taken];
        const SegmentDescriptor& division = location.descriptor;

        const uint64_t rowsInDivision = division.byteLength / tupleSize;
        if (rowOffset >= rowsInDivision)
        {
            continue;
        }

        const bool whollyInRange
            = range.isUnbounded() || (division.minTs >= range.start.getRawValue() && division.maxTs < range.end.getRawValue());

        /// Read in chunks of at most the remaining destination capacity. One chunk suffices when
        /// every row qualifies, because then rows read and rows kept are the same number. Filtering
        /// breaks that equality -- a chunk may yield almost nothing -- so that case keeps pulling
        /// chunks until the destination is full or the division runs out, instead of returning a
        /// nearly empty buffer and a resume point.
        while (rowOffset < rowsInDivision && destTuples < capacity)
        {
            const uint64_t chunkRows = std::min(rowsInDivision - rowOffset, capacity - destTuples);
            char* dest = destSpan.data() + (destTuples * tupleSize);
            ssize_t bytesRead = 0;
            /// A null writer IS the unsealed segment: the location says how to read it, so there is
            /// no index to compare against.
            if (location.writer == nullptr)
            {
                const size_t from = rowOffset * tupleSize;
                const size_t want = std::min<size_t>(chunkRows * tupleSize, activeSnapshot.size() - from);
                std::memcpy(dest, activeSnapshot.data() + from, want);
                bytesRead = static_cast<ssize_t>(want);
            }
            else
            {
                bytesRead = location.writer->readAt(
                    dest, chunkRows * tupleSize, location.dataStart + division.byteOffset + (rowOffset * tupleSize));
            }
            if (bytesRead <= 0)
            {
                break;
            }
            const auto rowsRead = static_cast<uint64_t>(bytesRead) / tupleSize;
            if (rowsRead == 0)
            {
                break;
            }

            /// 2. Wholly inside the query: every row qualifies, so none of them is examined.
            if (whollyInRange)
            {
                destTuples += rowsRead;
                rowOffset += rowsRead;
                continue;
            }

            /// 3. Straddling a bound: the only case that reads timestamps. Filtering compacts the
            /// survivors in place, so the next chunk continues straight after them.
            const auto progress = filterBufferRows(
                std::span<char>(dest, rowsRead * tupleSize), rowsRead, tupleSize, *tsOffset, range, capacity - destTuples);
            destTuples += progress.kept;
            /// Rows LOOKED AT, not rows kept: a row this pass rejected has been decided.
            rowOffset += progress.scanned;
        }

        if (rowOffset < rowsInDivision)
        {
            break; /// destination filled part-way through this division; resume here
        }
    }

    /// Positions are global, so there is nothing to convert back: a position means the same unit
    /// however many files have since been evicted.
    if (taken < readable.locations.size())
    {
        frame.unitIndex = readable.locations[taken].unit;
        frame.rowOffset = rowOffset;
    }
    else if (!readable.locations.empty())
    {
        frame.unitIndex = readable.locations.back().unit + 1;
        frame.rowOffset = 0;
    }
    /// Exhausted only when the WALK ran out of units. Running out of collected ones means the
    /// destination filled, and ending the replay there would silently truncate it.
    if (readable.reachedEnd && taken >= readable.locations.size())
    {
        frame.exhausted = true;
    }

    NES_DEBUG("FileStore::read: {} units of {} files, {} tuples", readable.locations.size(), parts.size(), destTuples);

    buffer.setNumberOfTuples(destTuples);
    return destTuples;
}

bool FileStore::hasMore() const
{
    {
        const std::shared_lock lock(mutex);
        if (writerOpened && !activeBytes.empty())
        {
            return true;
        }
        for (const auto& part : parts)
        {
            if (part.writer && part.writer->size() > part.dataStart)
            {
                return true;
            }
        }
    }
    if (nextLevel)
    {
        return nextLevel->hasMore();
    }
    return false;
}

Schema FileStore::getSchema() const
{
    const std::shared_lock lock(mutex);
    return schema;
}

uint64_t FileStore::size() const
{
    /// Just the locking wrapper. The sum lives in storedBytesLocked and only there -- it was
    /// written out twice, which is how three call sites came to disagree about what a threshold
    /// counts in the first place.
    const std::shared_lock lock(mutex);
    return storedBytesLocked();
}


void FileStore::removeFile()
{
    const std::unique_lock lock(mutex);
    for (auto& part : parts)
    {
        if (part.writer)
        {
            part.writer->removeFile();
        }
    }
}

const std::string& FileStore::getFilePath() const
{
    const std::shared_lock lock(mutex);
    return current().path;
}

std::vector<std::string> FileStore::getFilePaths() const
{
    const std::shared_lock lock(mutex);
    std::vector<std::string> paths;
    paths.reserve(parts.size());
    for (const auto& part : parts)
    {
        paths.push_back(part.path);
    }
    return paths;
}

size_t FileStore::debugFileCount() const
{
    const std::shared_lock lock(mutex);
    return parts.size();
}

size_t FileStore::debugEvictedFileCount() const
{
    const std::shared_lock lock(mutex);
    return evictedFiles;
}

std::pair<Timestamp, Timestamp> FileStore::debugStoreRange() const
{
    const std::shared_lock lock(mutex);
    return {fileMinTs, fileMaxTs};
}

std::vector<FileStore::DrainedSegment> FileStore::drainSegments()
{
    std::unique_lock lock(mutex);

    /// Anything still accumulating goes down too, so the drain leaves nothing behind.
    sealActiveSegment();

    std::vector<DrainedSegment> out;
    for (const auto& part : parts)
    {
        for (const auto& descriptor : part.segments)
        {
            DrainedSegment segment;
            segment.rowCount = descriptor.rowCount;
            segment.minTs = Timestamp(descriptor.minTs);
            segment.maxTs = Timestamp(descriptor.maxTs);
            segment.bytes.resize(descriptor.byteLength);
            const ssize_t got
                = part.writer->readAt(segment.bytes.data(), descriptor.byteLength, part.dataStart + descriptor.byteOffset);
            segment.bytes.resize(got > 0 ? static_cast<size_t>(got) : 0);
            out.push_back(std::move(segment));
        }
    }

    /// Let go of the data: cut the file back past the header and the (now stale) table, and
    /// reset both the table and the file-wide range. Every index into `segments` stops meaning
    /// anything, so a cursor holding one has to be told rather than left to read the wrong bytes.
    /// Let go of the data. Every index into the parts stops meaning anything, so a cursor holding
    /// one has to be told rather than left to read the wrong bytes.
    ++readGeneration;

    /// The rolled files are deleted outright -- their rows have moved to the tier below and nothing
    /// will be appended to them again. The first is kept and cut back to its header instead, so the
    /// store keeps one open file and the path it has been reporting all along.
    for (size_t i = 1; i < parts.size(); ++i)
    {
        parts[i].writer->close();
        parts[i].writer->removeFile();
    }
    parts.resize(1);

    auto& first = parts.front();
    first.writer->truncateTo(first.dataStart);
    const uint32_t sealedCount = 0;
    first.writer->writeAt(&sealedCount, sizeof(sealedCount), sealedCountOffset(config.schemaText.size()));
    first.segments.clear();
    first.minTs = Timestamp(Timestamp::INVALID_VALUE);
    first.maxTs = Timestamp(Timestamp::INITIAL_VALUE);
    activeSegment = SegmentDescriptor{};
    activeBytes.clear();
    fileMinTs = Timestamp(Timestamp::INVALID_VALUE);
    fileMaxTs = Timestamp(Timestamp::INITIAL_VALUE);

    return out;
}

uint32_t FileStore::calculateRowWidth(const Schema& schema)
{
    uint32_t width = 0;
    for (size_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        auto type = schema.getFieldAt(i).dataType;
        if (type.isType(DataType::Type::VARSIZED))
        {
            width += sizeof(uint32_t); /// TODO #11: Add Varsized Support
        }
        else
        {
            width += type.getSizeInBytesWithNull();
        }
    }
    return width;
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
StoreTypeRegistryReturnType StoreTypeGeneratedRegistrar::RegisterFileStoreStoreType(StoreTypeRegistryArguments args)
{
    PRECONDITION(args.config.contains("store_dir"), "args must contain store_dir");
    PRECONDITION(args.config.contains("store_type"), "args must contain store_type");
    PRECONDITION(args.config.contains("schema_text"), "args must contain schema_text");

    const auto filePath = args.config.at("store_dir");
    const auto storeName = args.config.at("store_name");
    const auto schemaText = args.config.at("schema_text");
    return makeStore<FileStore>(
        FileStore::Config{.storeName = storeName, .storeDir = filePath, .schemaText = schemaText}, std::move(args.schema));
}
}
