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

#include <CompressedStore.hpp>

#include <algorithm>
#include <cstring>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <StoreTransformationRegistry.hpp>

namespace NES::StoreManager
{

CompressedStore::CompressedStore(Config cfg, const Schema& schemaIn)
    : config(std::move(cfg)), schema(schemaIn), codec(config.codec, config.compressionLevel)
{
    activeBuffer.reserve(config.segmentSize);
}

CompressedStore::CompressedStore(Config cfg, const Schema& schemaIn, Store next, FlushPolicy policy)
    : config(std::move(cfg))
    , schema(schemaIn)
    , codec(config.codec, config.compressionLevel)
    , nextLevel(std::move(next))
    , flushPolicy(policy)
{
    activeBuffer.reserve(config.segmentSize);

    /// Look up the transformation for "CompressedStore -> <nextLevel>", exactly
    /// like MemoryStore's chained constructor does.
    auto found = StoreTransformationRegistry::instance().findTransformation(CompressedStore::typeName(), this->nextLevel->typeName());
    INVARIANT(found.has_value(), "No transformation registered for '{}' -> '{}'", this->typeName(), this->nextLevel->typeName());
    transformation = std::move(*found);
}

CompressedStore::~CompressedStore() = default;

void CompressedStore::open()
{
    {
        const std::unique_lock lock(mutex);
        opened = true;
    }
    if (nextLevel)
    {
        nextLevel->open();
    }
}

void CompressedStore::writeRecord(
    const uint8_t* recordData, uint32_t recordSize, Timestamp ts, const Schema& writeSchema, Store& self)
{
    std::unique_lock lock(mutex);
    PRECONDITION(opened, "CompressedStore must be opened before writing");

    if (writeSchema.getSizeOfSchemaInBytes() > 0 && schema.getSizeOfSchemaInBytes() == 0)
    {
        schema = writeSchema;
    }

    /// Append the record and widen the active segment's timestamp range.
    activeBuffer.insert(activeBuffer.end(), recordData, recordData + recordSize);
    if (ts < activeMinTs)
    {
        activeMinTs = ts;
    }
    if (ts > activeMaxTs)
    {
        activeMaxTs = ts;
    }
    ++activeRowCount;

    /// Seal into a compressed segment once the block is big enough.
    if (activeBuffer.size() >= config.segmentSize)
    {
        sealActiveSegment();
    }

    /// Push downstream if the flush policy trips (mirrors MemoryStore lines 197-201).
    const bool doFlush = flushPolicy && flushPolicy->shouldFlush(currentSizeLocked());
    if (doFlush)
    {
        lock.unlock();
        flush(self);
    }
}

void CompressedStore::appendRawBytes(const uint8_t* data, size_t len)
{
    const std::unique_lock lock(mutex);
    if (len == 0)
    {
        return;
    }
    /// Accumulate into the active buffer; the following flush() seals + compresses it.
    activeBuffer.insert(activeBuffer.end(), data, data + len);
    const uint32_t rowWidth = schema.getSizeOfSchemaInBytes();
    if (rowWidth > 0)
    {
        activeRowCount += len / rowWidth;
    }
}

void CompressedStore::updateTimestamps(Timestamp minTs, Timestamp maxTs)
{
    const std::unique_lock lock(mutex);
    if (minTs.getRawValue() != Timestamp::INVALID_VALUE && minTs < activeMinTs)
    {
        activeMinTs = minTs;
    }
    if (maxTs.getRawValue() != Timestamp::INITIAL_VALUE && maxTs > activeMaxTs)
    {
        activeMaxTs = maxTs;
    }
}

void CompressedStore::sealActiveSegment()
{
    /// Caller holds the unique lock.
    if (activeBuffer.empty())
    {
        return;
    }

    std::vector<uint8_t> payload = codec.compress(activeBuffer.data(), activeBuffer.size());

    SegmentHeader header;
    header.minTs = activeMinTs.getRawValue();
    header.maxTs = activeMaxTs.getRawValue();
    header.rowCount = activeRowCount;
    header.uncompressedBytes = activeBuffer.size();
    header.compressedBytes = payload.size();
    header.codecId = codec.getTypeId();

    segments.push_back(CompressedSegment{.header = header, .payload = std::move(payload)});

    activeBuffer.clear();
    activeMinTs = Timestamp(Timestamp::INVALID_VALUE);
    activeMaxTs = Timestamp(Timestamp::INITIAL_VALUE);
    activeRowCount = 0;
}

void CompressedStore::flush(Store& self)
{
    /// 1. Finalize our own pending data: seal it into a compressed segment.
    ///    (Direct analog of MemoryStore::flush sealing its active buffer.)
    {
        const std::unique_lock lock(mutex);
        sealActiveSegment();
    }

    /// 2. Push downstream and flush the next level (mirrors MemoryStore lines 126-130).
    if (nextLevel && transformation)
    {
        transformation->execute(self, *nextLevel);
        nextLevel->flush();
    }
}

void CompressedStore::close(Store& self)
{
    {
        const std::unique_lock lock(mutex);
        sealActiveSegment();
        opened = false;
    }
    /// If chained, hand the freshly sealed data downstream before closing.
    if (nextLevel && transformation)
    {
        transformation->execute(self, *nextLevel);
    }
    if (nextLevel)
    {
        nextLevel->close();
    }
}

/// Compute the byte offset of a field's VALUE within a packed row (same layout MemoryStore
/// uses). Nullable fields store a 1-byte null indicator immediately before their value;
/// non-nullable fields have no such byte, so the skip must be conditional on `nullable`.
static std::optional<uint32_t> findFieldValueOffset(const Schema& schema, const std::string& fieldName)
{
    uint32_t offset = 0;
    for (size_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        const auto& field = schema.getFieldAt(i);
        if (field.getUnqualifiedName() == fieldName)
        {
            return offset + (field.dataType.nullable ? 1 : 0);
        }
        offset += field.dataType.isType(DataType::Type::VARSIZED) ? sizeof(uint32_t) : field.dataType.getSizeInBytesWithNull();
    }
    return std::nullopt;
}

uint64_t CompressedStore::read(TupleBuffer& buffer, const Schema& readSchema, const TimeRange& range)
{
    /// Read the lower tier first (older data), like MemoryStore::read.
    if (nextLevel)
    {
        if (const uint64_t nextLevelRead = nextLevel->read(buffer, readSchema, range); nextLevelRead != 0)
        {
            return nextLevelRead;
        }
    }

    const std::shared_lock lock(mutex);

    const uint32_t rowWidth = readSchema.getSizeOfSchemaInBytes();
    if (rowWidth == 0)
    {
        return 0;
    }

    auto destSpan = buffer.getAvailableMemoryArea<uint8_t>();
    const uint64_t maxDestTuples = destSpan.size() / rowWidth;
    uint64_t destTuples = 0;

    for (const auto& seg : segments)
    {
        if (destTuples >= maxDestTuples)
        {
            break;
        }

        const Timestamp segMin(seg.header.minTs);
        const Timestamp segMax(seg.header.maxTs);

        /// Prune whole segments outside the range WITHOUT decompressing them.
        if (!range.isUnbounded() && !range.overlaps(segMin, segMax))
        {
            continue;
        }

        const std::vector<uint8_t> raw
            = codec.decompress(seg.payload.data(), seg.header.compressedBytes, seg.header.uncompressedBytes);
        const uint64_t numTuples = seg.header.rowCount;

        /// Whole segment in range -> copy all rows.
        if (range.isUnbounded() || (segMin >= range.start && segMax < range.end))
        {
            const uint64_t tuplesToCopy = std::min(numTuples, maxDestTuples - destTuples);
            std::memcpy(destSpan.data() + (destTuples * rowWidth), raw.data(), tuplesToCopy * rowWidth);
            destTuples += tuplesToCopy;
            continue;
        }

        /// Partial overlap -> per-row timestamp filtering.
        const auto tsOffset = findFieldValueOffset(readSchema, range.fieldName);
        PRECONDITION(tsOffset.has_value(), "TimeRange field '{}' not found in schema", range.fieldName);

        for (uint64_t i = 0; i < numTuples && destTuples < maxDestTuples; ++i)
        {
            const uint8_t* rowPtr = raw.data() + (i * rowWidth);
            uint64_t tsValue = 0;
            std::memcpy(&tsValue, rowPtr + *tsOffset, sizeof(uint64_t));
            if (range.contains(Timestamp(tsValue)))
            {
                std::memcpy(destSpan.data() + (destTuples * rowWidth), rowPtr, rowWidth);
                ++destTuples;
            }
        }
    }

    if (destTuples > 0)
    {
        buffer.setNumberOfTuples(destTuples);
    }
    return destTuples;
}

bool CompressedStore::hasMore() const
{
    {
        const std::shared_lock lock(mutex);
        if (!segments.empty())
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

Schema CompressedStore::getSchema() const
{
    const std::shared_lock lock(mutex);
    return schema;
}

uint64_t CompressedStore::currentSizeLocked() const
{
    uint64_t total = activeBuffer.size();
    for (const auto& seg : segments)
    {
        total += seg.header.compressedBytes;
    }
    return total;
}

uint64_t CompressedStore::size() const
{
    const std::shared_lock lock(mutex);
    return currentSizeLocked();
}

void CompressedStore::removeFile()
{
    const std::unique_lock lock(mutex);
    segments.clear();
    activeBuffer.clear();
    activeRowCount = 0;
}

uint64_t CompressedStore::debugActiveRowCount() const
{
    const std::shared_lock lock(mutex);
    return activeRowCount;
}

uint64_t CompressedStore::debugSegmentCount() const
{
    const std::shared_lock lock(mutex);
    return static_cast<uint64_t>(segments.size());
}

}