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

#include <cstdint>
#include <cstring>
#include <memory>

#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <ReplayStoreReader.hpp>

namespace NES::StoreManager
{

FileStore::FileStore(Config config, Schema schema)
    : config(std::move(config))
    , schema(std::move(schema))
    , writer(BinaryStoreWriter::Config{this->config.storeName, this->config.filePath, this->config.schemaText})
{
}

//fix: stores are now removed with the destruction of the StoreRegistry
FileStore::~FileStore() = default;

void FileStore::open()
{
    writer.open();
    writer.ensureHeader();
    writerOpened = true;
}

void FileStore::close()
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
}

void FileStore::flush()
{
    // BinaryStoreWriter uses pwrite which is immediately durable after fsync.
    // A full flush is performed on close().
}

void FileStore::write(TupleBuffer buffer, const Schema& writeSchema)
{
    PRECONDITION(writerOpened, "FileStore must be opened before writing");

    // Update schema from the write-time schema which has resolved types
    if (writeSchema.getSizeOfSchemaInBytes() > 0 && schema.getSizeOfSchemaInBytes() == 0)
    {
        schema = writeSchema;
    }

    const uint64_t numTuples = buffer.getNumberOfTuples();
    if (numTuples == 0)
    {
        return;
    }

    const uint32_t rowWidth = calculateRowWidth(writeSchema);
    auto srcSpan = buffer.getAvailableMemoryArea<uint8_t>();

    // The TupleBuffer row layout is packed (no padding), matching the binary file format.
    // We can write all rows as a contiguous block.
    const size_t totalBytes = static_cast<size_t>(numTuples) * rowWidth;
    NES_DEBUG("FileStore::write: {} tuples, rowWidth={}, totalBytes={}, file={}", numTuples, rowWidth, totalBytes, config.filePath);
    writer.append(srcSpan.data(), totalBytes);
}

uint64_t FileStore::read(TupleBuffer& buffer, const Schema& readSchema)
{
    if (!reader)
    {
        // Close writer before reading (if it was opened for writing)
        if (writerOpened)
        {
            writer.close();
            writerOpened = false;
        }
        reader = std::make_unique<ReplayStoreReader>(config.filePath);
        reader->open();
        NES_DEBUG("FileStore::read: opened reader for file={}, dataStartOffset={}", config.filePath, reader->getDataStartOffset());
    }

    if (reader->isEof())
    {
        NES_DEBUG("FileStore::read: reader is at EOF immediately, file={}", config.filePath);
        return 0;
    }

    const uint32_t tupleSize = calculateRowWidth(readSchema);
    const uint64_t capacity = buffer.getBufferSize() / tupleSize;
    char* dest = buffer.getAvailableMemoryArea<char>().data();

    const uint64_t tuplesRead = reader->readRows(dest, capacity, tupleSize, readSchema);
    NES_DEBUG("FileStore::read: tuplesRead={}, tupleSize={}, capacity={}, file={}", tuplesRead, tupleSize, capacity, config.filePath);
    buffer.setNumberOfTuples(tuplesRead);

    if (reader->isEof() || reader->peek() == std::char_traits<char>::eof())
    {
        buffer.setLastChunk(true);
    }

    return tuplesRead;
}

bool FileStore::hasMore() const
{
    if (!reader)
    {
        return true; // Haven't started reading yet, assume data exists
    }
    return !reader->isEof();
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
            width += sizeof(uint32_t); // TODO #11: Add Varsized Support
        }
        else
        {
            width += type.getSizeInBytesWithNull();
        }
    }
    return width;
}

} // namespace NES::StoreManager

// Registry registration functions
#include <StoreTypeRegistry.hpp>

namespace NES
{
StoreTypeRegistryReturnType StoreTypeGeneratedRegistrar::RegisterFileStoreStoreType(StoreTypeRegistryArguments args)
{
    auto filePath = args.config.count("file_path") ? args.config.at("file_path") : "";
    auto storeName = args.config.count("store_name") ? args.config.at("store_name") : "";
    auto schemaText = args.config.count("schema_text") ? args.config.at("schema_text") : "";
    return StoreManager::makeStore<StoreManager::FileStore>(
        StoreManager::FileStore::Config{.storeName = storeName, .filePath = filePath, .schemaText = schemaText}, std::move(args.schema));
}
}
