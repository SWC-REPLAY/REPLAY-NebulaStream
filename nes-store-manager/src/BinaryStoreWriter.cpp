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

#include <BinaryStoreWriter.hpp>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <utility>

#include <ErrorHandling.hpp>
#include <ReplayStoreFormat.hpp>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace NES::StoreManager
{

BinaryStoreWriter::BinaryStoreWriter(Config cfg) : config(std::move(cfg))
{
    PRECONDITION(config.segmentSize > 0, "segmentSize must be > 0");
    PRECONDITION(config.totalSize >= config.segmentSize, "totalSize must be >= segmentSize");
    segmentCount = static_cast<uint32_t>(config.totalSize / config.segmentSize);
    PRECONDITION(segmentCount > 0, "Must have at least one segment");
    segments.resize(segmentCount);
}

BinaryStoreWriter::~BinaryStoreWriter()
{
    if (fd >= 0)
    {
        ::close(fd);
        fd = -1;
    }
}

void BinaryStoreWriter::open()
{
    constexpr int flags = O_CREAT | O_RDWR;
    const std::string& filePath = config.filePath;
    fd = ::open(filePath.c_str(), flags, 0644);
    if (fd < 0)
    {
        throw CannotOpenSink("Could not open output file: {} (errno={}, msg={})", filePath, errno, std::strerror(errno));
    }

    /// Write the segmented header
    auto headerBuf = serializeSegmentedHeader(config.schemaText, config.segmentSize, segmentCount);

    segmentTableStart = segmentTableOffset(config.schemaText.size());
    dataAreaStart = segmentDataAreaOffset(config.schemaText.size(), segmentCount);

    const ssize_t written = ::pwrite(fd, headerBuf.data(), headerBuf.size(), 0);
    if (written < 0 || static_cast<size_t>(written) != headerBuf.size())
    {
        ::close(fd);
        fd = -1;
        throw CannotOpenSink("Writing segmented header failed: errno={} {}", errno, std::strerror(errno));
    }

    /// Pre-allocate the full file: header + segment data areas
    const size_t totalFileSize = dataAreaStart + (static_cast<size_t>(segmentCount) * config.segmentSize);
    if (::ftruncate(fd, static_cast<off_t>(totalFileSize)) != 0)
    {
        ::close(fd);
        fd = -1;
        throw CannotOpenSink("ftruncate failed: errno={} {}", errno, std::strerror(errno));
    }

    activeSegmentIndex = 0;
    wrapCount = 0;
    opened = true;
}

void BinaryStoreWriter::close()
{
    if (fd >= 0)
    {
        flushSegmentState();
        ::fsync(fd);
        ::close(fd);
        fd = -1;
    }
    opened = false;
}

void BinaryStoreWriter::removeFile()
{
    close();
    const std::string& filePath = config.filePath;
    auto ec = std::remove(filePath.c_str());
    INVARIANT(ec == 0, "Could not remove file: {}", ec);
}

uint32_t BinaryStoreWriter::append(const uint8_t* data, size_t len, uint64_t timestamp)
{
    PRECONDITION(opened, "BinaryStoreWriter must be opened before writing");
    PRECONDITION(len <= config.segmentSize, "Record size {} exceeds segment size {}", len, config.segmentSize);

    /// Check if the record fits in the active segment
    auto& seg = segments[activeSegmentIndex];
    if (seg.usedBytes + len > config.segmentSize)
    {
        advanceSegment();
    }

    auto& activeSeg = segments[activeSegmentIndex];
    const size_t offset = segmentDataOffset(dataAreaStart, activeSegmentIndex, config.segmentSize) + activeSeg.usedBytes;

    const ssize_t written = ::pwrite(fd, data, len, static_cast<off_t>(offset));
    if (written < 0 || static_cast<size_t>(written) != len)
    {
        throw CannotOpenSink("pwrite failed: errno={} {}", errno, std::strerror(errno));
    }

    activeSeg.usedBytes += len;

    /// Update segment timestamps
    if (timestamp < activeSeg.minTs)
    {
        activeSeg.minTs = timestamp;
    }
    if (timestamp > activeSeg.maxTs)
    {
        activeSeg.maxTs = timestamp;
    }

    flushSegmentDescriptor(activeSegmentIndex);

    return activeSegmentIndex;
}

void BinaryStoreWriter::appendRaw(const uint8_t* data, size_t len)
{
    PRECONDITION(opened, "BinaryStoreWriter must be opened before writing");

    size_t remaining = len;
    const uint8_t* ptr = data;

    while (remaining > 0)
    {
        auto& seg = segments[activeSegmentIndex];
        const size_t available = config.segmentSize - seg.usedBytes;
        if (available == 0)
        {
            advanceSegment();
            continue;
        }

        const size_t toWrite = std::min(remaining, available);
        const size_t offset = segmentDataOffset(dataAreaStart, activeSegmentIndex, config.segmentSize) + seg.usedBytes;

        const ssize_t written = ::pwrite(fd, ptr, toWrite, static_cast<off_t>(offset));
        if (written < 0 || static_cast<size_t>(written) != toWrite)
        {
            throw CannotOpenSink("pwrite failed: errno={} {}", errno, std::strerror(errno));
        }

        seg.usedBytes += toWrite;
        flushSegmentDescriptor(activeSegmentIndex);

        ptr += toWrite;
        remaining -= toWrite;
    }
}

void BinaryStoreWriter::updateFileTimestamps(uint64_t minTs, uint64_t maxTs) const
{
    if (fd < 0)
    {
        return;
    }
    ssize_t written = ::pwrite(fd, &minTs, sizeof(uint64_t), static_cast<off_t>(OFFSET_MIN_TS));
    if (written < 0 || static_cast<size_t>(written) != sizeof(uint64_t))
    {
        throw CannotOpenSink("Failed to update minTs in header: errno={} {}", errno, std::strerror(errno));
    }
    written = ::pwrite(fd, &maxTs, sizeof(uint64_t), static_cast<off_t>(OFFSET_MAX_TS));
    if (written < 0 || static_cast<size_t>(written) != sizeof(uint64_t))
    {
        throw CannotOpenSink("Failed to update maxTs in header: errno={} {}", errno, std::strerror(errno));
    }
}

void BinaryStoreWriter::updateSegmentTimestamps(uint32_t segmentIndex, uint64_t minTs, uint64_t maxTs)
{
    PRECONDITION(segmentIndex < segmentCount, "Segment index out of range");
    auto& seg = segments[segmentIndex];
    if (minTs < seg.minTs)
    {
        seg.minTs = minTs;
    }
    if (maxTs > seg.maxTs)
    {
        seg.maxTs = maxTs;
    }
    flushSegmentDescriptor(segmentIndex);
}

uint64_t BinaryStoreWriter::size() const
{
    uint64_t total = 0;
    for (const auto& seg : segments)
    {
        total += seg.usedBytes;
    }
    return total;
}

void BinaryStoreWriter::advanceSegment()
{
    uint32_t nextIndex = (activeSegmentIndex + 1) % segmentCount;
    if (nextIndex == 0)
    {
        ++wrapCount;
    }

    /// Reset the segment we're about to overwrite
    segments[nextIndex].usedBytes = 0;
    segments[nextIndex].minTs = UINT64_MAX;
    segments[nextIndex].maxTs = 0;
    flushSegmentDescriptor(nextIndex);

    activeSegmentIndex = nextIndex;
    flushSegmentState();
}

void BinaryStoreWriter::flushSegmentDescriptor(uint32_t segmentIndex) const
{
    if (fd < 0)
    {
        return;
    }
    const size_t offset = segmentTableStart + (static_cast<size_t>(segmentIndex) * SEGMENT_DESCRIPTOR_BYTES);
    const auto& seg = segments[segmentIndex];

    /// Write usedBytes, minTs, maxTs as a contiguous 24-byte block
    uint8_t buf[SEGMENT_DESCRIPTOR_BYTES];
    std::memcpy(buf, &seg.usedBytes, sizeof(uint64_t));
    std::memcpy(buf + 8, &seg.minTs, sizeof(uint64_t));
    std::memcpy(buf + 16, &seg.maxTs, sizeof(uint64_t));

    const ssize_t written = ::pwrite(fd, buf, SEGMENT_DESCRIPTOR_BYTES, static_cast<off_t>(offset));
    if (written < 0 || static_cast<size_t>(written) != SEGMENT_DESCRIPTOR_BYTES)
    {
        throw CannotOpenSink("Failed to flush segment descriptor: errno={} {}", errno, std::strerror(errno));
    }
}

void BinaryStoreWriter::flushSegmentState() const
{
    if (fd < 0)
    {
        return;
    }
    /// activeSegmentIndex and wrapCount are stored right after schema text and segmentSize + segmentCount
    const size_t offset = HEADER_FIXED_BYTES + sizeof(uint32_t) + config.schemaText.size() + sizeof(uint64_t) + sizeof(uint32_t);

    uint8_t buf[sizeof(uint32_t) + sizeof(uint32_t)];
    std::memcpy(buf, &activeSegmentIndex, sizeof(uint32_t));
    std::memcpy(buf + sizeof(uint32_t), &wrapCount, sizeof(uint32_t));

    const ssize_t written = ::pwrite(fd, buf, sizeof(buf), static_cast<off_t>(offset));
    if (written < 0 || static_cast<size_t>(written) != sizeof(buf))
    {
        throw CannotOpenSink("Failed to flush segment state: errno={} {}", errno, std::strerror(errno));
    }
}

}