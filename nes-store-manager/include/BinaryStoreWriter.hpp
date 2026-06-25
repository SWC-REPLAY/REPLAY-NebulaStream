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
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include <ReplayStoreFormat.hpp>

namespace NES::StoreManager
{
/// POSIX-based binary file writer for the segmented Replay store format.
/// Pre-allocates a fixed file size partitioned into segments, each with its own min/max timestamps.
/// Wraps around circularly when all segments are full.
class BinaryStoreWriter
{
public:
    struct Config
    {
        std::string storeName;
        std::string filePath;
        std::string schemaText;
        size_t totalSize = 1 * 1024 * 1024; /// Total pre-allocated size for segment data (default 1MB)
        size_t segmentSize = 64 * 1024; /// Size of each segment (default 64KB)
    };

    explicit BinaryStoreWriter(Config cfg);
    ~BinaryStoreWriter();

    BinaryStoreWriter(const BinaryStoreWriter&) = delete;
    BinaryStoreWriter& operator=(const BinaryStoreWriter&) = delete;
    BinaryStoreWriter(BinaryStoreWriter&&) = delete;
    BinaryStoreWriter& operator=(BinaryStoreWriter&&) = delete;

    /// Open the file, write the segmented header, and pre-allocate the full file.
    void open();

    /// Fsync and close the file descriptor.
    void close();

    /// Remove the backing file from disk.
    void removeFile();

    /// Append a record to the active segment. Advances to the next segment (circular) if needed.
    /// Returns the segment index the record was written to.
    uint32_t append(const uint8_t* data, size_t len, uint64_t timestamp);

    /// Append raw bytes to the active segment without timestamp tracking.
    /// Used by MemoryToFileTransformation for bulk writes.
    void appendRaw(const uint8_t* data, size_t len);

    /// Update the overall file-level min/max timestamps in the header.
    void updateFileTimestamps(uint64_t minTs, uint64_t maxTs) const;

    /// Update a specific segment's min/max timestamps and usedBytes in the segment table.
    void updateSegmentTimestamps(uint32_t segmentIndex, uint64_t minTs, uint64_t maxTs);

    [[nodiscard]] const std::string& getStoreName() const { return config.storeName; }

    /// Total bytes of data written across all segments (approximate, for size() reporting).
    [[nodiscard]] uint64_t size() const;

    [[nodiscard]] uint32_t getSegmentCount() const { return segmentCount; }
    [[nodiscard]] uint64_t getSegmentSize() const { return config.segmentSize; }
    [[nodiscard]] uint32_t getActiveSegmentIndex() const { return activeSegmentIndex; }
    [[nodiscard]] uint32_t getWrapCount() const { return wrapCount; }
    [[nodiscard]] const std::vector<SegmentDescriptor>& getSegments() const { return segments; }

    /// Get the file offset where segment data areas begin.
    [[nodiscard]] size_t getDataAreaStart() const { return dataAreaStart; }

private:
    /// Advance to the next segment, wrapping around if necessary. Resets the new segment's descriptor.
    void advanceSegment();

    /// Write the segment table entry for a given segment index to the file.
    void flushSegmentDescriptor(uint32_t segmentIndex) const;

    /// Write the activeSegmentIndex and wrapCount fields in the header.
    void flushSegmentState() const;

    int fd{-1};
    Config config;
    uint32_t segmentCount{0};
    uint32_t activeSegmentIndex{0};
    uint32_t wrapCount{0};
    size_t dataAreaStart{0}; /// File offset where segment data begins
    size_t segmentTableStart{0}; /// File offset where segment table begins
    std::vector<SegmentDescriptor> segments;
    bool opened{false};
};

}