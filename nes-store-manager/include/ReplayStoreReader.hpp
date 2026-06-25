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

#include <atomic>
#include <cstdint>
#include <fstream>
#include <iosfwd>
#include <string>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <ReplayStoreFormat.hpp>
#include <TimeRange.hpp>

namespace NES::StoreManager
{

/// Reads binary store files produced by the Replay store writer.
/// Supports both legacy (v1) and segmented (v2+) file formats.
class ReplayStoreReader
{
public:
    explicit ReplayStoreReader(std::string filePath);
    ~ReplayStoreReader();

    ReplayStoreReader(const ReplayStoreReader&) = delete;
    ReplayStoreReader& operator=(const ReplayStoreReader&) = delete;
    ReplayStoreReader(ReplayStoreReader&&) = delete;
    ReplayStoreReader& operator=(ReplayStoreReader&&) = delete;

    /// Open the file and parse the header (including segment table for v2+).
    void open();

    /// Close the input stream.
    void close();

    /// Return the schema embedded in the file header.
    [[nodiscard]] Schema readSchema() const;

    /// Verify that the given expected schema matches the schema stored in the file header.
    void verifySchema(const Schema& expectedSchema) const;

    /// Read fixed-size rows from the current segment into a destination buffer.
    /// For v2+ files, reads from the current segment in iteration order.
    /// Returns the number of complete rows successfully read.
    uint64_t readRows(char* dest, uint64_t maxRows, uint32_t tupleSize, const Schema& schema);

    /// Check whether all data has been read.
    [[nodiscard]] bool isEof() const;

    /// Get the byte offset where data begins (after the header/segment table).
    [[nodiscard]] uint64_t getDataStartOffset() const { return dataStartOffset; }

    /// Get the minimum timestamp stored in this file (file-level).
    [[nodiscard]] uint64_t getMinTs() const { return header.minTs; }

    /// Get the maximum timestamp stored in this file (file-level).
    [[nodiscard]] uint64_t getMaxTs() const { return header.maxTs; }

    /// Whether this is a segmented (v2+) file.
    [[nodiscard]] bool isSegmented() const { return header.version >= 2; }

    /// Get the segment descriptors (v2+ only).
    [[nodiscard]] const std::vector<SegmentDescriptor>& getSegments() const { return header.segments; }

    /// Get the segment count.
    [[nodiscard]] uint32_t getSegmentCount() const { return header.segmentCount; }

    /// Get the segment size.
    [[nodiscard]] uint64_t getSegmentSize() const { return header.segmentSize; }

    /// Get the active segment index at the time the file was written.
    [[nodiscard]] uint32_t getActiveSegmentIndex() const { return header.activeSegmentIndex; }

    /// Get the wrap count.
    [[nodiscard]] uint32_t getWrapCount() const { return header.wrapCount; }

    /// Build a list of segment indices to read, ordered from oldest to newest,
    /// filtering to only segments whose time range overlaps the query range.
    [[nodiscard]] std::vector<uint32_t> getSegmentReadOrder(const TimeRange& range) const;

    /// Prepare to read from a specific segment. Seeks to the segment's data area.
    void seekToSegment(uint32_t segmentIndex);

    /// Read rows from a specific segment. Returns the number of rows read.
    /// Reads up to maxRows or until the segment's usedBytes are exhausted.
    uint64_t readSegmentRows(uint32_t segmentIndex, char* dest, uint64_t maxRows, uint32_t tupleSize, const Schema& schema);

    /// Get total bytes read so far.
    [[nodiscard]] uint64_t getTotalBytesRead() const { return totalBytesRead.load(); }

    /// Clear stream error flags.
    void clearErrors();

    /// Seek to a specific position in the file.
    void seekTo(std::streampos pos);

    /// Peek the next character without consuming it.
    [[nodiscard]] int peek();

    /// Get current stream position.
    [[nodiscard]] std::streampos getPosition();

    /// Static helper: read schema from a file without keeping it open.
    static Schema readSchemaFromFile(const std::string& filePath);

private:
    std::string filePath;
    std::ifstream inputFile;
    FileHeader header;
    uint64_t dataStartOffset{0};
    std::atomic<uint64_t> totalBytesRead{0};
    bool opened{false};
};

}