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
std::string generateFilePath(const FileStore::Config& cfg)
{
    const auto now = std::chrono::system_clock::now();
    const auto timeT = std::chrono::system_clock::to_time_t(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::ostringstream ts;
    ts << std::put_time(std::localtime(&timeT), "%Y%m%d_%H%M%S") << '_' << std::setfill('0') << std::setw(3) << ms.count();
    return cfg.storeDir + "/replay_" + cfg.storeName + "_" + ts.str() + ".bin";
}
}

FileStore::FileStore(Config config, const Schema& schema)
    : config(std::move(config))
    , schema(schema)
    , filePath(generateFilePath(this->config))
    , writer(BinaryStoreWriter::Config{
          .storeName = this->config.storeName,
          .filePath = filePath,
          .schemaText = this->config.schemaText,
          .segmentSize = this->config.segmentSize,
          .maxSegments = static_cast<uint32_t>((this->config.totalSize + this->config.segmentSize - 1) / this->config.segmentSize)})
{
}

FileStore::FileStore(Config config, const Schema& schema, Store nextLevel, StoreTransformation transformation, FlushPolicy policy)
    : config(std::move(config))
    , schema(schema)
    , filePath(generateFilePath(this->config))
    , writer(BinaryStoreWriter::Config{
          .storeName = this->config.storeName,
          .filePath = filePath,
          .schemaText = this->config.schemaText,
          .segmentSize = this->config.segmentSize,
          .maxSegments = static_cast<uint32_t>((this->config.totalSize + this->config.segmentSize - 1) / this->config.segmentSize)})
    , nextLevel(std::move(nextLevel))
    , transformation(std::move(transformation))
    , flushPolicy(policy)
{
}

FileStore::~FileStore() = default;

void FileStore::open()
{
    std::unique_lock lock(mutex);
    fileMinTs = Timestamp(Timestamp::INVALID_VALUE);
    fileMaxTs = Timestamp(Timestamp::INITIAL_VALUE);
    writer.open();
    writer.ensureHeader();
    maxSegments = static_cast<uint32_t>((config.totalSize + config.segmentSize - 1) / config.segmentSize);
    dataStartOffset = dataStartOffsetFor(config.schemaText.size(), maxSegments);
    segments.clear();
    activeSegment = SegmentDescriptor{};
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
            writer.close();
            writerOpened = false;
        }
    }
    if (nextLevel)
    {
        nextLevel->close();
    }
}

void FileStore::flush([[maybe_unused]] Store& self)
{

        if (nextLevel && transformation)
        {
            transformation->execute(self, *nextLevel);   // drain me into the next tier
            nextLevel->flush();
        }
    /// BinaryStoreWriter uses pwrite which is immediately durable after fsync.
    /// A full flush is performed on close().
    /*if (nextLevel)
    {
        nextLevel->flush();
    }*/
}

void FileStore::writeRecord(const uint8_t* recordData, uint32_t recordSize, Timestamp ts, [[maybe_unused]] Store& self)
{
    PRECONDITION(ts.getRawValue() != Timestamp::INVALID_VALUE, "FileStore was passed a record with an invalid timestamp!");

    NES_DEBUG("FileStore::writeRecord: recordSize={}, ts={}, file={}", recordSize, ts, filePath);

    /// Accumulated into the segment being filled, exactly as MemoryStore accumulates a record
    /// into the buffer being filled. A record does NOT become a segment of its own -- the
    /// segment seals when it reaches `segmentSize`, so this path builds the same segments the
    /// bulk path does and nothing has to special-case it.
    appendBuffer(recordData, recordSize, 1, ts, ts);

    /// Check flush policy and flush to next level if triggered (mirrors MemoryStore::writeRecord).
    if (flushPolicy && flushPolicy->shouldFlush(writer.size() - dataStartOffset))
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

    /// The first buffer of a segment fixes where it starts.
    if (activeSegment.byteLength == 0)
    {
        activeSegment.byteOffset = writer.size() - dataStartOffset;
    }
    writer.append(data, len);

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
        /// Kept so a read can dismiss the whole file before looking at any segment -- the
        /// coarsest of the three levels.
        writer.updateTimestamps(fileMinTs.getRawValue(), fileMaxTs.getRawValue());
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

    const auto index = static_cast<uint32_t>(segments.size());

    /// The table is preallocated from `totalSize`, which a chained store never exceeds because
    /// the flush policy drains it first. A store with no tier below it has nothing to drain into
    /// and can outgrow the table -- so running out of slots stops persisting descriptors rather
    /// than failing the write. Reads and drains use the in-memory copy either way; only a reader
    /// opening the file fresh loses the ranges past that point, and it can still scan.
    if (index < maxSegments)
    {
        /// Fill the preallocated slot in place. Appends continue past the end of the file
        /// untouched, so the table stays at the front where a single read reaches all of it.
        writer.writeAt(&activeSegment, SEGMENT_DESCRIPTOR_BYTES, segmentSlotOffset(config.schemaText.size(), index));
        const uint32_t sealedCount = index + 1;
        writer.writeAt(&sealedCount, sizeof(sealedCount), sealedCountOffset(config.schemaText.size()));
    }

    segments.push_back(activeSegment);
    activeSegment = SegmentDescriptor{};
}

size_t FileStore::debugSegmentCount() const
{
    const std::shared_lock lock(mutex);
    return segments.size();
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

/// Filter rows in a buffer in-place, keeping only rows whose timestamp field falls within the range.
/// Returns the number of rows remaining.
uint64_t filterBufferRows(const std::span<char> data, uint64_t numRows, uint32_t rowWidth, uint32_t tsFieldOffset, const TimeRange& range)
{
    uint64_t kept = 0;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const auto row = data.subspan(i * rowWidth, rowWidth);
        uint64_t tsValue = 0;
        /// tsFieldOffset already points past any null indicator.
        const auto tsBytes = row.subspan(tsFieldOffset, sizeof(uint64_t));
        std::memcpy(&tsValue, tsBytes.data(), tsBytes.size());
        if (range.contains(Timestamp(tsValue)))
        {
            if (kept != i)
            {
                std::memmove(data.subspan(kept * rowWidth, rowWidth).data(), row.data(), rowWidth);
            }
            ++kept;
        }
    }
    return kept;
}

}

uint64_t FileStore::read(TupleBuffer& buffer, const Schema& readSchema, const TimeRange& range)
{
    if (nextLevel)
    {
        NES_DEBUG("Checking if next level has data to be read");
        if (const uint64_t nextLevelRead = nextLevel->read(buffer, readSchema, range); nextLevelRead != 0)
        {
            return nextLevelRead;
        }
    }

    /// Skip entire file if its timestamp range falls outside the query range
    if (!range.isUnbounded())
    {
        const std::shared_lock lock(mutex);
        if (!range.overlaps(fileMinTs, fileMaxTs))
        {
            NES_DEBUG("FileStore::read: skipping file {} (ts range outside query range)", filePath);
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

    /// Snapshot the divisions under the lock, then read them without holding it: pread is
    /// position-independent, and a concurrent writer only ever appends past what is snapshotted.
    ///
    /// The segment still being filled is included as a division of its own. It has no descriptor
    /// in the header yet, but its rows are already on disk and a read must see them.
    std::vector<SegmentDescriptor> divisions;
    {
        const std::shared_lock lock(mutex);
        divisions = segments;
        if (activeSegment.byteLength > 0)
        {
            divisions.push_back(activeSegment);
        }
    }

    auto destSpan = buffer.getAvailableMemoryArea<char>();
    const uint64_t capacity = destSpan.size() / tupleSize;
    uint64_t destTuples = 0;

    /// Three decisions per division, exactly as MemoryStore makes them per buffer: skip it, take
    /// all of it, or inspect its rows. Deciding per division is what makes the skip real -- a
    /// span from the first match to the last would read everything in between, and segments are
    /// held in write order, so a late arrival puts a non-matching one between two matches.
    for (const auto& division : divisions)
    {
        if (destTuples >= capacity)
        {
            break;
        }

        /// 1. Its range cannot meet the query: never touched.
        if (!range.isUnbounded() && !range.overlaps(Timestamp(division.minTs), Timestamp(division.maxTs)))
        {
            continue;
        }

        const uint64_t rowsInDivision = division.byteLength / tupleSize;
        const uint64_t rowsToRead = std::min(rowsInDivision, capacity - destTuples);
        if (rowsToRead == 0)
        {
            continue;
        }

        char* dest = destSpan.data() + (destTuples * tupleSize);
        const ssize_t bytesRead
            = writer.readAt(dest, rowsToRead * tupleSize, dataStartOffset + division.byteOffset);
        if (bytesRead <= 0)
        {
            continue;
        }
        const auto rowsRead = static_cast<uint64_t>(bytesRead) / tupleSize;

        /// 2. Wholly inside the query: every row qualifies, so none of them is examined.
        if (range.isUnbounded() || (division.minTs >= range.start.getRawValue() && division.maxTs < range.end.getRawValue()))
        {
            destTuples += rowsRead;
            continue;
        }

        /// 3. Straddling a bound: the only case that reads timestamps. Filtering compacts the
        /// survivors in place, so the next division continues straight after them.
        destTuples += filterBufferRows(std::span<char>(dest, rowsRead * tupleSize), rowsRead, tupleSize, *tsOffset, range);
    }

    NES_DEBUG(
        "FileStore::read: {} of {} divisions considered, {} tuples, file={}",
        divisions.size(),
        segments.size(),
        destTuples,
        filePath);

    buffer.setNumberOfTuples(destTuples);
    return destTuples;
}

bool FileStore::hasMore() const
{
    {
        const std::shared_lock lock(mutex);
        if (writerOpened && writer.size() > dataStartOffset)
        {
            return true;
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
    return writer.size();
}

void FileStore::removeFile()
{
    writer.removeFile();
}

std::vector<FileStore::DrainedSegment> FileStore::drainSegments()
{
    std::unique_lock lock(mutex);

    /// Anything still accumulating goes down too, so the drain leaves nothing behind.
    sealActiveSegment();

    std::vector<DrainedSegment> out;
    out.reserve(segments.size());
    for (const auto& descriptor : segments)
    {
        DrainedSegment segment;
        segment.rowCount = descriptor.rowCount;
        segment.minTs = Timestamp(descriptor.minTs);
        segment.maxTs = Timestamp(descriptor.maxTs);
        segment.bytes.resize(descriptor.byteLength);
        const ssize_t got
            = writer.readAt(segment.bytes.data(), descriptor.byteLength, dataStartOffset + descriptor.byteOffset);
        segment.bytes.resize(got > 0 ? static_cast<size_t>(got) : 0);
        out.push_back(std::move(segment));
    }

    /// Let go of the data: cut the file back past the header and the (now stale) table, and
    /// reset both the table and the file-wide range.
    writer.truncateTo(dataStartOffset);
    const uint32_t sealedCount = 0;
    writer.writeAt(&sealedCount, sizeof(sealedCount), sealedCountOffset(config.schemaText.size()));
    segments.clear();
    activeSegment = SegmentDescriptor{};
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
