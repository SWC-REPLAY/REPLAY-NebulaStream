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

#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FlushPolicy.hpp>
#include <ReplayStoreReader.hpp>
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
    fileMinTs = Timestamp(Timestamp::INVALID_VALUE);
    fileMaxTs = Timestamp(Timestamp::INITIAL_VALUE);
    writer.open();
    writer.ensureHeader();
    writerOpened = true;
    if (nextLevel)
    {
        nextLevel->open();
    }
}

void FileStore::close([[maybe_unused]] Store& self)
{
    if (writerOpened)
    {
        writer.close();
        writerOpened = false;
    }
    if (reader)
    {
        reader->close();
        reader.reset();
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
    writer.updateTimestamps(fileMinTs.getRawValue(), fileMaxTs.getRawValue());

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

/// Compute the byte offset of a field within a row, using the packed binary layout.
static std::optional<uint32_t> findFieldOffset(const Schema& schema, const std::string& fieldName)
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
static uint64_t filterBufferRows(char* data, uint64_t numRows, uint32_t rowWidth, uint32_t tsFieldOffset, const TimeRange& range)
{
    uint64_t kept = 0;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        const char* rowPtr = data + i * rowWidth;
        uint64_t tsValue = 0;
        /// Skip the 1-byte null indicator before the actual value
        std::memcpy(&tsValue, rowPtr + tsFieldOffset + 1, sizeof(uint64_t));
        if (range.contains(Timestamp(tsValue)))
        {
            if (kept != i)
            {
                std::memmove(data + kept * rowWidth, rowPtr, rowWidth);
            }
            ++kept;
        }
    }
    return kept;
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
    if (!reader)
    {
        if (writerOpened)
        {
            writer.close();
            writerOpened = false;
        }
        reader = std::make_unique<ReplayStoreReader>(filePath);
        reader->open();
        NES_DEBUG("FileStore::read: opened reader for file={}, dataStartOffset={}", filePath, reader->getDataStartOffset());

        /// Skip entire file if its timestamp range falls outside the query range
        if (!range.isUnbounded() && !range.overlaps(Timestamp(reader->getMinTs()), Timestamp(reader->getMaxTs())))
        {
            NES_DEBUG(
                "FileStore::read: skipping file {} (ts range [{}, {}] outside query range)",
                filePath,
                reader->getMinTs(),
                reader->getMaxTs());
            reader->close();
            reader.reset();
            buffer.setLastChunk(true);
            return 0;
        }
    }

    if (!reader->isEof())
    {
        const uint32_t tupleSize = calculateRowWidth(readSchema);
        PRECONDITION(tupleSize > 0, "Schema must have at least one field to compute row width");
        const uint64_t capacity = buffer.getBufferSize() / tupleSize;
        char* dest = buffer.getAvailableMemoryArea<char>().data();

        uint64_t tuplesRead = reader->readRows(dest, capacity, tupleSize, readSchema);
        NES_DEBUG("FileStore::read: tuplesRead={}, tupleSize={}, capacity={}, file={}", tuplesRead, tupleSize, capacity, filePath);

        /// Apply row-level filtering if a time range is specified
        if (!range.isUnbounded() && tuplesRead > 0)
        {
            const auto tsOffset = findFieldOffset(readSchema, range.fieldName);
            PRECONDITION(tsOffset.has_value(), "TimeRange field '{}' not found in schema", range.fieldName);
            tuplesRead = filterBufferRows(dest, tuplesRead, tupleSize, *tsOffset, range);
        }

        buffer.setNumberOfTuples(tuplesRead);

        if (tuplesRead > 0)
        {
            const bool atEof = reader->isEof() || reader->peek() == std::char_traits<char>::eof();
            if (atEof && !nextLevel)
            {
                buffer.setLastChunk(true);
            }
            return tuplesRead;
        }
    }

    NES_DEBUG("FileStore::read: own data exhausted, file={}", filePath);

    /// Own data exhausted — delegate to next level
    if (nextLevel)
    {
        return nextLevel->read(buffer, readSchema, range);
    }

    buffer.setLastChunk(true);
    return 0;
}

bool FileStore::hasMore() const
{
    if (!reader)
    {
        return true; /// Haven't started reading yet, assume data exists
    }
    if (!reader->isEof())
    {
        return true;
    }
    /// Own data exhausted — check next level
    if (nextLevel)
    {
        return nextLevel->hasMore();
    }
    return false;
}

Schema FileStore::getSchema() const
{
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
