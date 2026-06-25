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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <BinaryStoreWriter.hpp>
#include <FlushPolicy.hpp>
#include <Store.hpp>
#include <StoreTransformation.hpp>
#include <TimeRange.hpp>

namespace NES::StoreManager
{

class ReplayStoreReader;

/// File-backed store with pre-allocated segmented storage.
/// Each segment tracks its own min/max timestamps for efficient time-range skipping.
/// Wraps around circularly when all segments are full.
/// Satisfies StoreConcept. Optionally chains to a next-level store.
class FileStore
{
public:
    struct Config
    {
        std::string storeName;
        std::string storeDir;
        std::string schemaText;
        size_t totalSize = 1 * 1024 * 1024; /// Total pre-allocated size for data (default 1MB)
        size_t segmentSize = 64 * 1024; /// Size per segment (default 64KB)
    };

    /// Standalone constructor (no chaining).
    explicit FileStore(Config config, const Schema& schema);

    /// Chained constructor: this store flushes to nextLevel when the policy triggers.
    FileStore(Config config, const Schema& schema, Store nextLevel, StoreTransformation transformation, FlushPolicy policy);

    ~FileStore();

    FileStore(const FileStore&) = delete;
    FileStore& operator=(const FileStore&) = delete;
    FileStore(FileStore&&) = delete;
    FileStore& operator=(FileStore&&) = delete;

    void open();
    void close(Store& self);
    void flush(Store& self);

    void writeRecord(const uint8_t* recordData, uint32_t recordSize, Timestamp ts, const Schema& writeSchema, Store& self);

    /// Bulk append raw bytes (used by MemoryToFileTransformation).
    void appendRawBytes(const uint8_t* data, size_t len);

    /// Update timestamps for the active segment (used by MemoryToFileTransformation).
    void updateFileTimestamps(Timestamp minTs, Timestamp maxTs);
    uint64_t read(TupleBuffer& buffer, const Schema& readSchema, const TimeRange& range);
    [[nodiscard]] bool hasMore() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] uint64_t size() const;

    [[nodiscard]] static std::string_view typeName() noexcept { return "FileStore"; }

    [[nodiscard]] const std::string& getFilePath() const { return filePath; }

    void removeFile();

    /// Calculate packed row width from schema (no padding, matching binary format).
    static uint32_t calculateRowWidth(const Schema& schema);

private:
    Config config;
    Schema schema;
    std::string filePath;
    BinaryStoreWriter writer;
    std::unique_ptr<ReplayStoreReader> reader;
    bool writerOpened{false};

    /// Segment read state: ordered list of segment indices to read, and current position.
    std::vector<uint32_t> readSegmentOrder;
    size_t readSegmentPos{0};

    /// Chaining support (all optional — empty for standalone/tail stores).
    std::optional<Store> nextLevel;
    std::optional<StoreTransformation> transformation;
    std::optional<FlushPolicy> flushPolicy;
};

}