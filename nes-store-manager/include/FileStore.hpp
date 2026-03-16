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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include <BinaryStoreWriter.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::StoreManager
{

class ReplayStoreReader;

/// File-backed store wrapping BinaryStoreWriter/ReplayStoreReader. Satisfies StoreConcept.
class FileStore
{
public:
    struct Config
    {
        std::string storeName;
        std::string filePath;
        std::string schemaText;
    };

    explicit FileStore(Config config, Schema schema);
    ~FileStore();

    // FileStore is non-copyable, non-movable (BinaryStoreWriter + unique_ptr)
    FileStore(const FileStore&) = delete;
    FileStore& operator=(const FileStore&) = delete;
    FileStore(FileStore&&) = delete;
    FileStore& operator=(FileStore&&) = delete;

    void open();
    void close();
    void flush();

    void write(TupleBuffer buffer, const Schema& schema);
    uint64_t read(TupleBuffer& buffer, const Schema& schema);
    [[nodiscard]] bool hasMore() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] uint64_t size() const;
    [[nodiscard]] std::string_view typeName() const noexcept { return "FileStore"; }

    [[nodiscard]] const std::string& getFilePath() const { return config.filePath; }
    void removeFile();

private:
    /// Calculate packed row width from schema (no padding, matching binary format).
    static uint32_t calculateRowWidth(const Schema& schema);

    Config config;
    Schema schema;
    BinaryStoreWriter writer;
    std::unique_ptr<ReplayStoreReader> reader;
    bool writerOpened{false};
};

} // namespace NES::StoreManager