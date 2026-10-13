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

#include <lossy/LossyStore.hpp>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <utility>

#include <ErrorHandling.hpp>
#include <ReplayStoreFormat.hpp>
#include <StoreTypeRegistry.hpp>

namespace NES
{

using namespace Lossy;

namespace
{

// Distinct filename prefix so tiers sharing a storeName/storeDir don't collide on a path.
std::string generateFilePath(const LossyStore::Config& cfg)
{
    const auto now = std::chrono::system_clock::now();
    const auto timeT = std::chrono::system_clock::to_time_t(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::ostringstream ts;
    ts << std::put_time(std::localtime(&timeT), "%Y%m%d_%H%M%S") << '_' << std::setfill('0') << std::setw(3) << ms.count();
    return cfg.storeDir + "/lossy_" + cfg.storeName + "_" + ts.str() + ".bin";
}

// Byte offset of a field's value within a packed row (past the null indicator when present).
uint32_t valueOffsetOf(const Schema& schema, const size_t fieldIndex)
{
    uint32_t offset = 0;
    for (size_t i = 0; i < fieldIndex; ++i)
    {
        offset += schema.getFieldAt(i).dataType.getSizeInBytesWithNull();
    }
    return offset + (schema.getFieldAt(fieldIndex).dataType.nullable ? 1U : 0U);
}

}

LossyStore::LossyStore(Config cfg, const Schema& schemaIn)
    : config(std::move(cfg))
    , schema(schemaIn)
    , filePath(generateFilePath(config))
    , writer(BinaryStoreWriter::Config{.storeName = config.storeName, .filePath = filePath, .schemaText = config.schemaText})
{

    // 100% would let a value move by its whole magnitude -- not a useful bound.
    PRECONDITION(
        config.accuracyBoundPercent >= 0.0 && config.accuracyBoundPercent < 100.0,
        "accuracyBoundPercent is {}%, must be in [0, 100)",
        config.accuracyBoundPercent);

    PRECONDITION(
        getCodec(config.lossyCodec).isLossy(),
        "config.lossyCodec is '{}', which is not a lossy codec",
        codecName(config.lossyCodec));

    rowWidth = schema.getSizeOfSchemaInBytes();
    fields.reserve(schema.getNumberOfFields());

    for (size_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        const auto& field = schema.getFieldAt(i);
        const auto type = field.dataType.type;

        // Reject VARSIZED: the row holds only an index, so columnarizing would capture the index, not the data.
        PRECONDITION(
            isSupportedType(type),
            "LossyStore supports fixed-width types only; field '{}' has DataType::Type {}",
            field.getUnqualifiedName(),
            static_cast<int>(type));

        // Codec fixed from type and bound: bounded float -> lossy (codes inline, losslessCodec 0);
        // otherwise ForBitpack for integer-like, Raw for the rest (including unbounded floats).
        const bool eligibleForLoss = type == DataType::Type::FLOAT32 || type == DataType::Type::FLOAT64;
        const double bound = eligibleForLoss ? config.accuracyBoundPercent : 0.0;

        uint8_t lossyCodec = 0;
        uint8_t losslessCodec = 0;
        if (bound > 0.0)
        {
            lossyCodec = static_cast<uint8_t>(config.lossyCodec);
        }
        else
        {
            losslessCodec = isIntegerLike(type) ? static_cast<uint8_t>(CodecId::ForBitpack) : static_cast<uint8_t>(CodecId::Raw);
        }

        fields.push_back(
            FieldLayout{
                .name = field.getUnqualifiedName(),
                .type = type,
                .valueOffset = valueOffsetOf(schema, i),
                .valueWidth = field.dataType.getSizeInBytesWithoutNull(),
                .nullable = field.dataType.nullable,
                .errorBoundPercent = bound,
                .lossyCodec = lossyCodec,
                .losslessCodec = losslessCodec});
    }
}

LossyStore::~LossyStore()
{
    if (writerOpened)
    {
        writer.close();
    }
}

void LossyStore::open()
{
    const std::unique_lock lock(mutex);
    fileMinTs = Timestamp(Timestamp::INVALID_VALUE);
    fileMaxTs = Timestamp(Timestamp::INITIAL_VALUE);
    writer.open();
    writer.ensureHeader();
    dataStartOffset = HEADER_FIXED_BYTES + sizeof(uint32_t) + config.schemaText.size();
    writerOpened = true;
    opened = true;
}

void LossyStore::close(Store&)
{
    const std::unique_lock lock(mutex);
    sealPending();
    if (writerOpened)
    {
        writer.close();
        writerOpened = false;
    }
    opened = false;
}

void LossyStore::flush(Store&)
{
    const std::unique_lock lock(mutex);
    sealPending();
}

void LossyStore::writeRecord(const uint8_t* recordData, const uint32_t recordSize, const Timestamp ts, Store&)
{
    const std::unique_lock lock(mutex);
    PRECONDITION(opened, "LossyStore must be opened before writing");
    PRECONDITION(
        recordSize == rowWidth, "record is {} bytes, schema row width is {}", recordSize, rowWidth);
    pending.insert(pending.end(), recordData, recordData + recordSize);

    // Range built from the timestamps handed in, not read back from the rows (standalone path).
    pendingMinTs = std::min(pendingMinTs, ts);
    pendingMaxTs = std::max(pendingMaxTs, ts);

    if (pending.size() >= config.groupSize)
    {
        sealPending();
    }
}

void LossyStore::appendSegment(
    const uint8_t* data, const size_t len, const uint64_t rowCount, const Timestamp minTs, const Timestamp maxTs)
{
    const std::unique_lock lock(mutex);
    if (len == 0)
    {
        return;
    }
    PRECONDITION(opened, "LossyStore must be opened before writing");
    PRECONDITION(len % rowWidth == 0, "segment of {} bytes is not a whole number of {}-byte rows", len, rowWidth);
    PRECONDITION(
        len / rowWidth == rowCount, "segment claims {} rows but carries {} bytes at {} per row", rowCount, len, rowWidth);

    // A segment is already one group; seal any pending rows first to keep groups in arrival order.
    sealPending();
    sealGroup(data, rowCount, minTs, maxTs);
}

void LossyStore::sealPending()
{
    // Caller holds the lock.
    if (pending.empty())
    {
        return;
    }
    sealGroup(pending.data(), pending.size() / rowWidth, pendingMinTs, pendingMaxTs);
    pending.clear();
    pendingMinTs = Timestamp(Timestamp::INVALID_VALUE);
    pendingMaxTs = Timestamp(Timestamp::INITIAL_VALUE);
}

void LossyStore::sealGroup(const uint8_t* rows, const uint64_t rowCount, const Timestamp minTs, const Timestamp maxTs)
{
    // Caller holds the lock.
    if (rowCount == 0)
    {
        return;
    }

    const size_t numColumns = fields.size();
    std::vector<std::vector<uint8_t>> encodedValues(numColumns);
    std::vector<uint64_t> uncompressedLengths(numColumns, 0);
    std::vector<std::vector<uint8_t>> nullBits(numColumns);
    std::vector<double> achievedErrors(numColumns, 0.0);

    // One column as it will be written: its bytes plus the two figures ColumnEntry carries.
    struct StoredColumn
    {
        std::vector<uint8_t> bytes;
        // Codec output size before the compressor; equals bytes.size() when the compressor did not win.
        uint64_t uncompressedLength{0};
        double achievedError{0.0};
    };

    // Columnarize and encode one column at a time; scattering into per-column arrays is the transpose.
    std::vector<uint8_t> values;
    for (size_t c = 0; c < numColumns; ++c)
    {
        const auto& field = fields[c];
        values.assign(static_cast<size_t>(rowCount) * field.valueWidth, 0);

        if (field.nullable)
        {
            nullBits[c].assign(nullBitsBytesFor(rowCount), 0);
        }

        {
        // Row-major -> columnar, fused into the column build.
        for (uint64_t r = 0; r < rowCount; ++r)
        {
            const uint8_t* rowPtr = rows + (r * rowWidth);
            std::memcpy(values.data() + (r * field.valueWidth), rowPtr + field.valueOffset, field.valueWidth);

            if (field.nullable)
            {
                // Record what the indicator byte was (0/1); reject anything else rather than guess its meaning.
                const uint8_t indicator = rowPtr[field.valueOffset - 1];
                PRECONDITION(
                    indicator <= 1,
                    "null indicator for field '{}' is {}, expected 0 or 1",
                    field.name,
                    static_cast<int>(indicator));
                if (indicator != 0)
                {
                    nullBits[c][r / 8] |= static_cast<uint8_t>(1U << (r % 8));
                }
            }
        }
        }

        const ColumnValues columnValues{
            .type = field.type, .data = values.data(), .rowCount = rowCount, .valueWidth = field.valueWidth};

        // Encode through the codec, then byte-compress; keep the compressed form only when it is smaller.
        const auto encodeWith = [&](const uint8_t lossyId, const uint8_t losslessId)
        {
            // Either stage goes through the same ColumnCodec interface. A lossless stage over a
            // lossy codec's codes is not implemented.
            INVARIANT(
                lossyId == 0 || losslessId == 0,
                "a second lossless stage ({}) over a lossy codec's codes is not implemented",
                codecName(static_cast<CodecId>(losslessId)));

            const CodecId id = lossyId != 0 ? static_cast<CodecId>(lossyId) : static_cast<CodecId>(losslessId);
            const double bound = lossyId != 0 ? field.errorBoundPercent : 0.0;
            auto encoded = getCodec(id).encode(columnValues, bound);

            StoredColumn stored;
            stored.uncompressedLength = encoded.bytes.size();
            stored.achievedError = encoded.achievedError;
            std::vector<uint8_t> compressed;
            {
                // Paid on every column, kept only when smaller.
                compressed = compressor.compress(encoded.bytes.data(), encoded.bytes.size());
            }
            stored.bytes = compressed.size() < encoded.bytes.size() ? std::move(compressed) : std::move(encoded.bytes);
            return stored;
        };

        /// The codec was fixed at construction from the column's type and the store's bound; no
        /// measurement pass runs here, so every group encodes the column the same way.
        const StoredColumn stored = encodeWith(field.lossyCodec, field.losslessCodec);

        achievedErrors[c] = stored.achievedError;
        uncompressedLengths[c] = stored.uncompressedLength;
        encodedValues[c] = std::move(stored.bytes);
    }

    // The group's time range arrives with its rows; never recovered by reading them back.
    GroupHeader header;
    header.rowCount = rowCount;
    header.numColumns = static_cast<uint32_t>(numColumns);

    header.minTs = minTs.getRawValue();
    header.maxTs = maxTs.getRawValue();

    // Layout: header, column table, column bytes. Table offsets are relative to the data region.
    std::vector<ColumnEntry> entries(numColumns);
    uint64_t dataOffset = 0;
    for (size_t c = 0; c < numColumns; ++c)
    {
        entries[c].valuesOffset = dataOffset;
        entries[c].valuesLength = encodedValues[c].size();
        entries[c].uncompressedLength = uncompressedLengths[c];
        dataOffset += encodedValues[c].size();

        entries[c].nullBitsOffset = dataOffset;
        entries[c].nullBitsLength = nullBits[c].size();
        dataOffset += nullBits[c].size();

        entries[c].lossyCodecId = fields[c].lossyCodec;
        entries[c].losslessCodecId = fields[c].losslessCodec;
        entries[c].achievedError = achievedErrors[c];
    }

    header.groupByteLength = GROUP_HEADER_BYTES + (numColumns * COLUMN_ENTRY_BYTES) + dataOffset;

    std::vector<uint8_t> out;
    out.reserve(header.groupByteLength);
    appendGroupHeader(out, header);
    for (const auto& entry : entries)
    {
        appendColumnEntry(out, entry);
    }
    for (size_t c = 0; c < numColumns; ++c)
    {
        out.insert(out.end(), encodedValues[c].begin(), encodedValues[c].end());
        out.insert(out.end(), nullBits[c].begin(), nullBits[c].end());
    }
    INVARIANT(
        out.size() == header.groupByteLength,
        "serialized group is {} bytes, header claims {}",
        out.size(),
        header.groupByteLength);

    const uint64_t offset = writer.size();
    {
        writer.append(out.data(), out.size());
    }
    groups.push_back(GroupIndexEntry{.offset = offset, .header = header});

    {
        const Timestamp groupMin(header.minTs);
        const Timestamp groupMax(header.maxTs);
        if (groupMin < fileMinTs)
        {
            fileMinTs = groupMin;
        }
        if (groupMax > fileMaxTs)
        {
            fileMaxTs = groupMax;
        }
        writer.updateTimestamps(fileMinTs.getRawValue(), fileMaxTs.getRawValue());
    }
}

uint64_t LossyStore::read(TupleBuffer& buffer, const Schema& readSchema, const TimeRange& range, StoreReadCursor& cursor, size_t depth)
{
    PRECONDITION(depth < StoreReadCursor::MAX_TIERS, "store chain is deeper than a cursor can describe ({} tiers)", depth + 1);

    const std::shared_lock lock(mutex);

    auto& frame = cursor.frames.at(depth);
    bindCursorFrame(frame, readGeneration, typeName());

    const uint32_t readRowWidth = readSchema.getSizeOfSchemaInBytes();
    if (readRowWidth == 0)
    {
        return 0;
    }
    PRECONDITION(
        readRowWidth == rowWidth, "read schema row width {} does not match store's {}", readRowWidth, rowWidth);

    // Every group carries a real range, so the overlap test is unconditional, like MemoryStore's.
    auto destSpan = buffer.getAvailableMemoryArea<uint8_t>();
    const uint64_t maxDestTuples = destSpan.size() / rowWidth;
    // A zero-capacity destination would return 0 forever, read as end of stream.
    PRECONDITION(maxDestTuples > 0, "destination buffer of {} bytes cannot hold a {}-byte row", destSpan.size(), rowWidth);

    uint64_t destTuples = 0;
    size_t unit = frame.unitIndex;
    uint64_t rowOffset = frame.rowOffset;

    // Scratch for raw group bytes; decoded rows live on the cursor so a resume doesn't re-decode.
    std::vector<uint8_t> groupBytes;

    for (; unit < groups.size(); ++unit, rowOffset = 0)
    {
        if (destTuples >= maxDestTuples)
        {
            break;
        }

        const auto& group = groups[unit];
        const Timestamp groupMin(group.header.minTs);
        const Timestamp groupMax(group.header.maxTs);

        if (!range.isUnbounded() && !range.overlaps(groupMin, groupMax))
        {
            continue;
        }

        if (rowOffset >= group.header.rowCount)
        {
            continue;
        }

        const uint64_t rowCount = group.header.rowCount;

        // Decode the group unless the cursor is resuming inside the one it already holds.
        if (!frame.decodedValid || frame.decodedUnit != unit)
        {
            groupBytes.resize(group.header.groupByteLength);
            ssize_t got = 0;
            {
                got = writer.readAt(groupBytes.data(), groupBytes.size(), group.offset);
            }
            PRECONDITION(
                got == static_cast<ssize_t>(groupBytes.size()), "short read of group at offset {}", group.offset);

            const size_t entriesOffset = GROUP_HEADER_BYTES;
            const size_t dataOffset = entriesOffset + (group.header.numColumns * COLUMN_ENTRY_BYTES);
            INVARIANT(
                group.header.numColumns == fields.size(),
                "group has {} columns, schema has {}",
                group.header.numColumns,
                fields.size());

            // Rebuild whole rows, then filter. Decoding only the ts column first is a later refinement.
            frame.decoded.assign(static_cast<size_t>(rowCount) * rowWidth, 0);

            for (size_t c = 0; c < fields.size(); ++c)
            {
                const auto& field = fields[c];
                const auto entry = parseColumnEntry(groupBytes.data() + entriesOffset + (c * COLUMN_ENTRY_BYTES));

                const uint8_t* stored = groupBytes.data() + dataOffset + entry.valuesOffset;

                // Equal lengths mean the column was stored uncompressed; nothing to undo.
                std::vector<uint8_t> inflated;
                if (entry.uncompressedLength != entry.valuesLength)
                {
                    inflated = compressor.decompress(stored, entry.valuesLength, entry.uncompressedLength);
                    stored = inflated.data();
                }

                std::vector<uint8_t> values(static_cast<size_t>(rowCount) * field.valueWidth);
                const MutableColumnValues out{
                    .type = field.type, .data = values.data(), .rowCount = rowCount, .valueWidth = field.valueWidth};
                const std::span<const uint8_t> payload(stored, entry.uncompressedLength);

                // Mirrors encodeWith: whichever stage wrote the bytes decodes them through the same interface.
                INVARIANT(
                    entry.lossyCodecId == 0 || entry.losslessCodecId == 0,
                    "a second lossless stage ({}) over a lossy codec's codes is not implemented",
                    codecName(static_cast<CodecId>(entry.losslessCodecId)));

                const CodecId id = entry.lossyCodecId != 0 ? static_cast<CodecId>(entry.lossyCodecId)
                                                             : static_cast<CodecId>(entry.losslessCodecId);
                {
                    getCodec(id).decode(payload, out);
                }

                const uint8_t* bits = entry.nullBitsLength > 0 ? groupBytes.data() + dataOffset + entry.nullBitsOffset : nullptr;

                {
                // Columnar -> row-major, inverse of the write-side transpose.
                for (uint64_t r = 0; r < rowCount; ++r)
                {
                    uint8_t* rowPtr = frame.decoded.data() + (r * rowWidth);
                    std::memcpy(rowPtr + field.valueOffset, values.data() + (r * field.valueWidth), field.valueWidth);
                    if (bits != nullptr)
                    {
                        const bool set = (bits[r / 8] & (1U << (r % 8))) != 0;
                        rowPtr[field.valueOffset - 1] = set ? 1U : 0U;
                    }
                }
                }
            }
            frame.decodedUnit = unit;
            frame.decodedValid = true;
            decodeCount.fetch_add(1, std::memory_order_relaxed);
        }
        const std::vector<uint8_t>& rowScratch = frame.decoded;

        // Whole group in range (or unbounded): take every row not yet taken.
        if (range.isUnbounded() || (groupMin >= range.start && groupMax < range.end))
        {
            const uint64_t toCopy = std::min(rowCount - rowOffset, maxDestTuples - destTuples);
            std::memcpy(destSpan.data() + (destTuples * rowWidth), rowScratch.data() + (rowOffset * rowWidth), toCopy * rowWidth);
            destTuples += toCopy;
            rowOffset += toCopy;
            if (rowOffset < rowCount)
            {
                break; // destination filled mid-group; resume here
            }
            continue;
        }

        // Otherwise filter rows one by one.
        const auto tsField = std::ranges::find_if(fields, [&](const auto& f) { return f.name == range.fieldName; });
        PRECONDITION(tsField != fields.end(), "TimeRange field '{}' not found in schema", range.fieldName);

        uint64_t row = rowOffset;
        for (; row < rowCount && destTuples < maxDestTuples; ++row)
        {
            const uint8_t* rowPtr = rowScratch.data() + (row * rowWidth);
            uint64_t tsValue = 0;
            std::memcpy(&tsValue, rowPtr + tsField->valueOffset, sizeof(uint64_t));
            if (range.contains(Timestamp(tsValue)))
            {
                std::memcpy(destSpan.data() + (destTuples * rowWidth), rowPtr, rowWidth);
                ++destTuples;
            }
        }
        // Rows LOOKED AT, not rows kept.
        rowOffset = row;
        if (row < rowCount)
        {
            break;
        }
    }

    frame.unitIndex = unit;
    frame.rowOffset = rowOffset;
    if (unit >= groups.size())
    {
        frame.exhausted = true;
    }

    if (destTuples > 0)
    {
        buffer.setNumberOfTuples(destTuples);
    }
    return destTuples;
}

bool LossyStore::hasMore() const
{
    const std::shared_lock lock(mutex);
    return !groups.empty() || !pending.empty();
}

Schema LossyStore::getSchema() const
{
    const std::shared_lock lock(mutex);
    return schema;
}

uint64_t LossyStore::currentSizeLocked() const
{
    uint64_t total = pending.size();
    for (const auto& group : groups)
    {
        total += group.header.groupByteLength;
    }
    return total;
}

uint64_t LossyStore::size() const
{
    const std::shared_lock lock(mutex);
    return currentSizeLocked();
}

void LossyStore::removeFile()
{
    const std::unique_lock lock(mutex);
    ++readGeneration;
    if (writerOpened)
    {
        writer.close();
        writerOpened = false;
    }
    writer.removeFile();
    groups.clear();
    pending.clear();
    opened = false;
}

uint64_t LossyStore::debugGroupCount() const
{
    const std::shared_lock lock(mutex);
    return static_cast<uint64_t>(groups.size());
}

std::vector<std::pair<uint64_t, uint64_t>> LossyStore::debugGroupRanges() const
{
    const std::shared_lock lock(mutex);
    std::vector<std::pair<uint64_t, uint64_t>> out;
    out.reserve(groups.size());
    for (const auto& group : groups)
    {
        out.emplace_back(group.header.minTs, group.header.maxTs);
    }
    return out;
}


}

namespace NES
{
StoreTypeRegistryReturnType StoreTypeGeneratedRegistrar::RegisterLossyStoreStoreType(StoreTypeRegistryArguments args)
{
    LossyStore::Config cfg;
    if (const auto it = args.config.find("storeName"); it != args.config.end())
    {
        cfg.storeName = it->second;
    }
    if (const auto it = args.config.find("storeDir"); it != args.config.end())
    {
        cfg.storeDir = it->second;
    }
    if (const auto it = args.config.find("schemaText"); it != args.config.end())
    {
        cfg.schemaText = it->second;
    }
    if (const auto it = args.config.find("groupSize"); it != args.config.end())
    {
        cfg.groupSize = std::stoull(it->second);
    }
    // Absent means lossless.
    if (const auto it = args.config.find("accuracyBoundPercent"); it != args.config.end())
    {
        cfg.accuracyBoundPercent = std::stod(it->second);
    }
    return makeStore<LossyStore>(std::move(cfg), args.schema);
}
}
