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

namespace NES::StoreManager
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
    , writer(BinaryStoreWriter::Config{.storeName = this->config.storeName, .filePath = filePath, .schemaText = this->config.schemaText})
{
}

FileStore::FileStore(Config config, const Schema& schema, Store nextLevel, StoreTransformation transformation, FlushPolicy policy)
    : config(std::move(config))
    , schema(schema)
    , filePath(generateFilePath(this->config))
    , writer(BinaryStoreWriter::Config{.storeName = this->config.storeName, .filePath = filePath, .schemaText = this->config.schemaText})
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
    dataStartOffset = HEADER_FIXED_BYTES + sizeof(uint32_t) + config.schemaText.size();
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
    /// BinaryStoreWriter uses pwrite which is immediately durable after fsync.
    /// A full flush is performed on close().
    if (nextLevel)
    {
        nextLevel->flush();
    }
}

void FileStore::writeRecord(
    const uint8_t* recordData, uint32_t recordSize, Timestamp ts, const Schema& writeSchema, [[maybe_unused]] Store& self)
{
    Timestamp currentMin{Timestamp(Timestamp::INVALID_VALUE)};
    Timestamp currentMax{Timestamp(Timestamp::INITIAL_VALUE)};
    {
        const std::unique_lock lock(mutex);
        PRECONDITION(writerOpened, "FileStore must be opened before writing");
        PRECONDITION(ts.getRawValue() != Timestamp::INVALID_VALUE, "FileStore was passed a record with an invalid timestamp!");
        /// Update schema from the write-time schema which has resolved types
        if (writeSchema.getSizeOfSchemaInBytes() > 0 && schema.getSizeOfSchemaInBytes() == 0)
        {
            schema = writeSchema;
        }

        if (ts < fileMinTs)
        {
            fileMinTs = ts;
        }
        if (ts > fileMaxTs)
        {
            fileMaxTs = ts;
        }
        currentMin = fileMinTs;
        currentMax = fileMaxTs;
    }
    /// These calls are thread-safe (atomic tail + pwrite).
    writer.updateTimestamps(currentMin.getRawValue(), currentMax.getRawValue());

    NES_DEBUG("FileStore::writeRecord: recordSize={}, ts={}, file={}", recordSize, ts, filePath);
    writer.append(recordData, recordSize);
}

void FileStore::appendRawBytes(const uint8_t* data, const size_t len)
{
    PRECONDITION(writerOpened, "FileStore must be opened before writing");
    writer.append(data, len);
}

void FileStore::updateFileTimestamps(const Timestamp minTs, const Timestamp maxTs)
{
    PRECONDITION(
        minTs.getRawValue() != Timestamp::INVALID_VALUE && maxTs.getRawValue() != Timestamp::INITIAL_VALUE,
        "updating file timestamps requires valid timestamps!");
    if (minTs.getRawValue() != Timestamp::INVALID_VALUE && minTs < fileMinTs)
    {
        fileMinTs = minTs;
    }
    if (maxTs.getRawValue() != Timestamp::INITIAL_VALUE && maxTs > fileMaxTs)
    {
        fileMaxTs = maxTs;
    }
    writer.updateTimestamps(fileMinTs.getRawValue(), fileMaxTs.getRawValue());
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
            return offset;
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
        /// Skip the 1-byte null indicator before the actual value
        const auto tsBytes = row.subspan(tsFieldOffset + 1, sizeof(uint64_t));
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
        if (const uint64_t nextLevelRead = nextLevel->read(buffer, readSchema, range); nextLevelRead == 0)
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

    /// Read the current write frontier (atomic) and compute how many complete rows exist.
    const uint64_t currentTail = writer.size();
    if (currentTail <= dataStartOffset)
    {
        return 0;
    }

    const uint64_t availableBytes = currentTail - dataStartOffset;
    const uint64_t completeRows = availableBytes / tupleSize;
    const uint64_t capacity = buffer.getBufferSize() / tupleSize;
    const uint64_t rowsToRead = std::min(completeRows, capacity);

    if (rowsToRead == 0)
    {
        return 0;
    }

    const uint64_t bytesToRead = rowsToRead * tupleSize;
    const auto dest = buffer.getAvailableMemoryArea<char>();

    /// pread is position-independent and thread-safe — no lock needed.
    const ssize_t bytesRead = writer.readAt(dest.data(), bytesToRead, dataStartOffset);
    if (bytesRead <= 0)
    {
        return 0;
    }

    uint64_t totalTuples = static_cast<uint64_t>(bytesRead) / tupleSize;
    NES_DEBUG("FileStore::read: totalTuples={}, tupleSize={}, capacity={}, file={}", totalTuples, tupleSize, capacity, filePath);

    /// Apply row-level filtering if a time range is specified
    if (!range.isUnbounded())
    {
        const auto tsOffset = findFieldOffset(readSchema, range.fieldName);
        PRECONDITION(tsOffset.has_value(), "TimeRange field '{}' not found in schema", range.fieldName);
        totalTuples = filterBufferRows(dest, totalTuples, tupleSize, *tsOffset, range);
    }

    buffer.setNumberOfTuples(totalTuples);
    return totalTuples;
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

}

namespace NES
{

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
StoreTypeRegistryReturnType StoreTypeGeneratedRegistrar::RegisterFileStoreStoreType(StoreTypeRegistryArguments args)
{
    PRECONDITION(args.config.contains("store_dir"), "args must contain store_dir");
    PRECONDITION(args.config.contains("store_type"), "args must contain store_type");
    PRECONDITION(args.config.contains("schema_text"), "args must contain schema_text");

    const auto filePath = args.config.at("store_dir");
    const auto storeName = args.config.at("store_name");
    const auto schemaText = args.config.at("schema_text");
    return StoreManager::makeStore<StoreManager::FileStore>(
        StoreManager::FileStore::Config{.storeName = storeName, .storeDir = filePath, .schemaText = schemaText}, std::move(args.schema));
}
}
