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
    // The part number, not just the timestamp: two rolls can land in the same millisecond.
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
    // A file holds totalSize/filesAtCapacity bytes, and at least one segment.
    const size_t perFile = config.totalSize / std::max<size_t>(config.filesAtCapacity, 1);
    return static_cast<uint32_t>(std::max<size_t>(perFile / std::max<size_t>(config.segmentSize, 1), 1));
}

uint64_t FileStore::dataBytesLocked() const
{
    // Row bytes only, each file's header and table subtracted -- what a flush threshold measures against.
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
    // Whole footprint, framing included -- what capacity measures against.
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
    // The oldest file is dropped whole; the newest (being written) is never dropped.
    bool dropped = false;
    while (parts.size() > 1 && storedBytesLocked() > config.totalSize)
    {
        parts.front().writer->close();
        parts.front().writer->removeFile();
        // Unit positions are global and never reused, so a file leaving renumbers nothing.
        parts.erase(parts.begin());
        ++evictedFiles;
        dropped = true;
    }

    if (!dropped)
    {
        return;
    }


    // Recompute the store range after eviction.
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
        // Exactly the segments this file can hold before it rolls.
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
    // The finished file is not closed; its rows stay readable, it just stops being written to.
    // Read firstUnit before the emplace, while current() is still the part being finished.
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
    // Start from one empty file; anything left from a previous open is dropped.
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
            // Seal the in-memory active segment, or closing drops up to one segment's worth of rows.
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

    // This tier's own policy decides when its contents move down; with no policy it drains on demand.
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


    // Accumulated into the segment being filled, like MemoryStore accumulates into a buffer.
    appendBuffer(recordData, recordSize, 1, ts, ts);

    // Flush if the policy triggers, measured across every rolled file.
    uint64_t stored = 0;
    {
        const std::shared_lock lock(mutex);
        // The same measure flush uses.
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

    // Serialises the whole append so a concurrent writer can't land bytes inside another's segment.
    const std::unique_lock lock(mutex);

    // Held in memory until the segment seals; where the bytes land is settled at seal.
    activeBytes.insert(activeBytes.end(), data, data + len);

    activeSegment.byteLength += len;
    activeSegment.rowCount += rowCount;

    // Union, not overwrite: a segment spans every buffer in it; the range is carried down, never recomputed.
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

    // A full table and a full file are the same condition, so this is where the file rolls.
    if (current().segments.size() >= segmentsPerFile())
    {
        rollToNextFile();
    }

    auto& part = current();

    // Payload appended past the table; offsets are relative to THIS file.
    activeSegment.byteOffset = part.writer->size() - part.dataStart;
    part.writer->append(activeBytes.data(), activeBytes.size());
    activeBytes.clear();

    // Descriptor written into its preallocated slot.
    const auto index = static_cast<uint32_t>(part.segments.size());
    part.writer->writeAt(&activeSegment, SEGMENT_DESCRIPTOR_BYTES, segmentSlotOffset(config.schemaText.size(), index));
    const uint32_t sealedCount = index + 1;
    part.writer->writeAt(&sealedCount, sizeof(sealedCount), sealedCountOffset(config.schemaText.size()));

    // Each file carries its own range in its header so a reader can dismiss the whole file.
    part.minTs = Timestamp(std::min(part.minTs.getRawValue(), activeSegment.minTs));
    part.maxTs = Timestamp(std::max(part.maxTs.getRawValue(), activeSegment.maxTs));
    part.writer->updateTimestamps(part.minTs.getRawValue(), part.maxTs.getRawValue());

    part.segments.push_back(activeSegment);
    activeSegment = SegmentDescriptor{};

    // Capacity enforced after the write: the newest rows always land, the oldest file leaves.
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
// Byte offset of a field within a packed row.
std::optional<uint32_t> findFieldOffset(const Schema& schema, const std::string& fieldName)
{
    uint32_t offset = 0;
    for (size_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        const auto& field = schema.getFieldAt(i);
        if (field.getUnqualifiedName() == fieldName)
        {
            // Past the null indicator when the field has one.
            return offset + (field.dataType.nullable ? 1 : 0);
        }
        offset += field.dataType.isType(DataType::Type::VARSIZED) ? sizeof(uint32_t) : field.dataType.getSizeInBytesWithNull();
    }
    return std::nullopt;
}

// How far a filtering pass got: kept = destination rows used, scanned = next row to look at.
struct FilterProgress
{
    uint64_t kept{0};
    uint64_t scanned{0};
};

// Filter rows in place, keeping those whose timestamp falls in range; stops after maxKept kept.
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
        // tsFieldOffset already points past any null indicator.
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
    // The unsealed segment sits one past the last sealed unit; its rows are readable with no descriptor yet.
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

        // Pruned here, so a non-matching unit never leaves the lock and costs no budget.
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

    // Skip everything if the store's range misses the query.
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

    // The field the range filters on, resolved once.
    std::optional<uint32_t> tsOffset;
    if (!range.isUnbounded())
    {
        tsOffset = findFieldOffset(readSchema, range.fieldName);
        PRECONDITION(tsOffset.has_value(), "TimeRange field '{}' not found in schema", range.fieldName);
    }

    // Resolved before the lock: reads the destination, not the store.
    auto destSpan = buffer.getAvailableMemoryArea<char>();
    const uint64_t capacity = destSpan.size() / tupleSize;
    // A zero-capacity destination would return 0 forever, read as end of stream.
    PRECONDITION(capacity > 0, "destination buffer of {} bytes cannot hold a {}-byte row", destSpan.size(), tupleSize);

    // Snapshot under the lock, read without it: pread is position-independent and writers only append.
    // Only the units this call can consume are taken.
    ReadableUnits readable;
    // The unsealed segment is in memory, so its bytes are copied out here.
    std::vector<uint8_t> activeSnapshot;
    // Which row of the unsealed segment the snapshot window begins at.
    uint64_t activeWindowFrom = 0;
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
            // Copy only what this call can consume, under the same lock: sealing would clear activeBytes.
            // A read already inside the segment resumes at rowOffset; one spilling in from earlier enters at row 0.
            const uint64_t windowFrom = readable.locations.size() == 1 ? rowOffset : 0;

            // A capacity-sized window is enough only when every row qualifies; filtered reads keep the whole copy.
            const bool everyRowQualifies = range.isUnbounded()
                || (activeSegment.minTs >= range.start.getRawValue() && activeSegment.maxTs < range.end.getRawValue());

            if (everyRowQualifies)
            {
                const size_t fromB = std::min<size_t>(windowFrom * tupleSize, activeBytes.size());
                const size_t wantB = std::min<size_t>(capacity * tupleSize, activeBytes.size() - fromB);
                activeSnapshot.assign(
                    activeBytes.begin() + static_cast<std::ptrdiff_t>(fromB),
                    activeBytes.begin() + static_cast<std::ptrdiff_t>(fromB + wantB));
                activeWindowFrom = windowFrom;
            }
            else
            {
                activeSnapshot = activeBytes;
                activeWindowFrom = 0;
            }
        }
    }

    uint64_t destTuples = 0;

    // Two decisions per unit: take all of it, or inspect its rows. Skip was already done under the lock.
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

        // Read in chunks of the remaining capacity; filtering may yield little, so keep pulling until full.
        while (rowOffset < rowsInDivision && destTuples < capacity)
        {
            const uint64_t chunkRows = std::min(rowsInDivision - rowOffset, capacity - destTuples);
            char* dest = destSpan.data() + (destTuples * tupleSize);
            ssize_t bytesRead = 0;
            // A null writer is the unsealed segment, served from the snapshot.
            if (location.writer == nullptr)
            {
                // Relative to the window, which may start part-way into the segment.
                PRECONDITION(
                    rowOffset >= activeWindowFrom,
                    "unsealed window starts at row {} but the read is at row {}",
                    activeWindowFrom,
                    rowOffset);
                const size_t from = (rowOffset - activeWindowFrom) * tupleSize;
                const size_t want
                    = from < activeSnapshot.size() ? std::min<size_t>(chunkRows * tupleSize, activeSnapshot.size() - from) : 0;
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

            // Wholly inside the query: every row qualifies, none is examined.
            if (whollyInRange)
            {
                destTuples += rowsRead;
                rowOffset += rowsRead;
                continue;
            }

            // Straddling a bound: filter, compacting survivors in place.
            const auto progress = filterBufferRows(
                std::span<char>(dest, rowsRead * tupleSize), rowsRead, tupleSize, *tsOffset, range, capacity - destTuples);
            destTuples += progress.kept;
            // Rows LOOKED AT, not rows kept.
            rowOffset += progress.scanned;
        }

        if (rowOffset < rowsInDivision)
        {
            break; // destination filled mid-division; resume here
        }
    }

    // Positions are global, so nothing to convert back after eviction.
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
    // Exhausted only when the walk ran out of units, not when the destination filled.
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
    // Locking wrapper; the sum lives only in storedBytesLocked.
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

    // Seal the active segment too, so the drain leaves nothing behind.
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

    // Let go of the data; every index into parts stops meaning anything, so bump the generation.
    ++readGeneration;

    // Rolled files are deleted; the first is kept and cut back to its header.
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
            width += sizeof(uint32_t); // TODO #11: Add Varsized Support
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
