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

#include <LossyStore.hpp>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <utility>

#include <lossy/ColumnStats.hpp>

#include <ErrorHandling.hpp>
#include <ReplayStoreFormat.hpp>
#include <StoreTypeRegistry.hpp>

namespace NES
{

using namespace Lossy;

namespace
{

/// Distinct filename prefix from the other stores': several tiers share a storeName and a
/// storeDir, so they must not collide on a generated path.
std::string generateFilePath(const LossyStore::Config& cfg)
{
    const auto now = std::chrono::system_clock::now();
    const auto timeT = std::chrono::system_clock::to_time_t(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::ostringstream ts;
    ts << std::put_time(std::localtime(&timeT), "%Y%m%d_%H%M%S") << '_' << std::setfill('0') << std::setw(3) << ms.count();
    return cfg.storeDir + "/lossy_" + cfg.storeName + "_" + ts.str() + ".bin";
}

/// Byte offset of a field's VALUE within a packed row, matching the layout the other tiers
/// use: a nullable field carries a one-byte indicator immediately before its value.
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

    /// 100% would let a value move by the column's whole range, which is not an accuracy
    /// bound in any useful sense.
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

        /// Reject rather than silently mishandle: VARSIZED stores only an index in the row
        /// with its payload elsewhere, so columnarizing the row bytes would capture the
        /// index and lose the data.
        PRECONDITION(
            isSupportedType(type),
            "LossyStore supports fixed-width types only; field '{}' has DataType::Type {}",
            field.getUnqualifiedName(),
            static_cast<int>(type));

        const auto name = field.getUnqualifiedName();

        /// The store's budget reaches floating-point columns and nothing else. See
        /// Config::accuracyBoundPercent for why integers are excluded categorically rather
        /// than case by case.
        ///
        /// Applying it here, once, is what lets selection be uniform afterwards. A column that
        /// may not lose anything carries a zero bound, and a zero bound is already enough to
        /// keep every lossy codec out of its candidate set -- so nothing downstream needs to
        /// know which columns are keys, or times, or anything else.
        const bool eligibleForLoss = type == DataType::Type::FLOAT32 || type == DataType::Type::FLOAT64;

        /// The codecs are deliberately left at their defaults here. Which one suits a column is
        /// a question about the data, and no data has arrived yet -- it is answered when the
        /// first group is sealed, then held for the column's lifetime.
        fields.push_back(
            FieldLayout{
                .name = name,
                .type = type,
                .valueOffset = valueOffsetOf(schema, i),
                .valueWidth = field.dataType.getSizeInBytesWithoutNull(),
                .nullable = field.dataType.nullable,
                .policy = CompressionPolicy{.errorBoundPercent = eligibleForLoss ? config.accuracyBoundPercent : 0.0}});

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

    /// The range of what is accumulating, built from the timestamps handed in rather than read
    /// back out of the rows. This is the standalone path; in a chain the tier above has already
    /// done exactly this and hands over whole segments instead.
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

    /// A segment is already the unit this tier stores, so it becomes one group as it stands --
    /// no re-division, and the range it arrived with goes straight into the group header.
    /// Anything the standalone write path left accumulating is sealed first, so groups stay in
    /// arrival order.
    sealPending();
    sealGroup(data, rowCount, minTs, maxTs);
}

void LossyStore::sealPending()
{
    /// Caller holds the unique lock.
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
    /// Caller holds the unique lock.
    if (rowCount == 0)
    {
        return;
    }

    const size_t numColumns = fields.size();
    std::vector<std::vector<uint8_t>> encodedValues(numColumns);
    std::vector<uint64_t> uncompressedLengths(numColumns, 0);
    std::vector<std::vector<uint8_t>> nullBits(numColumns);
    std::vector<double> achievedErrors(numColumns, 0.0);

    /// One column as it will be written: the bytes themselves, plus the two figures the
    /// ColumnEntry carries alongside them. Grouped so that a candidate codec's whole result
    /// can be held and discarded as one thing.
    struct StoredColumn
    {
        std::vector<uint8_t> bytes;
        /// What the codec produced, before the compressor. Equal to `bytes.size()` when the
        /// compressor did not win.
        uint64_t uncompressedLength{0};
        double achievedError{0.0};
    };

    /// Columnarize and encode one column at a time. Scattering row-major bytes into
    /// per-column arrays IS the transpose; there is no separate pass over a staged
    /// row-major copy, which is also how Arrow, DuckDB and ClickHouse build columns.
    std::vector<uint8_t> values;
    for (size_t c = 0; c < numColumns; ++c)
    {
        const auto& field = fields[c];
        values.assign(static_cast<size_t>(rowCount) * field.valueWidth, 0);

        if (field.nullable)
        {
            nullBits[c].assign(nullBitsBytesFor(rowCount), 0);
        }

        for (uint64_t r = 0; r < rowCount; ++r)
        {
            const uint8_t* rowPtr = rows + (r * rowWidth);
            std::memcpy(values.data() + (r * field.valueWidth), rowPtr + field.valueOffset, field.valueWidth);

            if (field.nullable)
            {
                /// The indicator sits immediately before the value. Its two possible meanings
                /// are not pinned down anywhere in the codebase, so the bit records what the
                /// byte was rather than what it means -- and anything but 0 or 1 is refused
                /// instead of being guessed at and silently corrupted.
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

        const ColumnValues columnValues{
            .type = field.type, .data = values.data(), .rowCount = rowCount, .valueWidth = field.valueWidth};

        /// Encode with an explicit two-stage choice, then run the byte compressor over the
        /// result.
        ///
        /// The compressor runs last, over whatever the stages produced. It is looking for the
        /// redundancy they left behind -- repeated headers, patterns across packed values --
        /// which is a different kind from the structure they removed.
        ///
        /// Its output is kept only when it actually wins. A well bit-packed column is already
        /// dense, and zstd over it regularly comes out larger; storing the stages' own bytes
        /// then is both smaller and cheaper to read.
        const auto encodeWith = [&](const uint8_t lossyId, const uint8_t losslessId)
        {
            /// Whichever stage is real, it is asked through the same `ColumnCodec` interface --
            /// nothing here has to know which lossy method LossyGroupFormat's `lossyCodecId`
            /// names, so a new one is a new CodecId and a new class, not a new branch here.
            ///
            /// `losslessId != 0` alongside a lossy stage would mean a second codec re-encoding
            /// the lossy stage's own codes -- a form no lossy codec here still exercises now
            /// that the choice is fixed rather than ranked (see the comment above this lambda's
            /// call site), so it stays unimplemented rather than silently ignored.
            INVARIANT(
                lossyId == 0 || losslessId == 0,
                "a second lossless stage ({}) over a lossy codec's codes is not implemented",
                codecName(static_cast<CodecId>(losslessId)));

            const CodecId id = lossyId != 0 ? static_cast<CodecId>(lossyId) : static_cast<CodecId>(losslessId);
            const double bound = lossyId != 0 ? field.policy.errorBoundPercent : 0.0;
            auto encoded = getCodec(id).encode(columnValues, bound);

            StoredColumn stored;
            stored.uncompressedLength = encoded.bytes.size();
            stored.achievedError = encoded.achievedError;
            auto compressed = compressor.compress(encoded.bytes.data(), encoded.bytes.size());
            stored.bytes = compressed.size() < encoded.bytes.size() ? std::move(compressed) : std::move(encoded.bytes);
            return stored;
        };

        /// Chosen once, from the first group that carries data, and then fixed -- so a column's
        /// encoding does not depend on where group boundaries happened to fall.
        ///
        /// The choice is a fixed, type-only rule, not a measurement: a floating-point column the
        /// policy permits to round gets the lossy stage (the only lossy codec today); every other
        /// column is encoded losslessly with ForBitpack when its type is integer-like, Raw
        /// otherwise. That is what keeps the store general -- the rule is the same for a schema
        /// it has never seen, with nothing to configure per column.
        ///
        /// A statistics-driven ranking across several lossless codecs (delta-of-delta,
        /// dictionary, dict-RLE, ...) used to run here instead, picking whichever measured
        /// smallest per column -- see `ColumnStats.hpp`'s `computeStats`/`filterCandidates`/
        /// `rank`. That code is kept in the tree, tested, and reachable for a benchmark to call
        /// directly; it is deliberately not wired into this pipeline, because a general store
        /// does not need a per-column measurement pass to be *correct*, only to be smaller, and
        /// that ratio-vs-complexity tradeoff is exactly what the benchmarks decide.
        StoredColumn stored;
        if (codecsSelected)
        {
            stored = encodeWith(field.lossyCodec, field.losslessCodec);
        }
        else
        {
            const auto candidates = filterCandidates(field.type, field.policy);
            INVARIANT(!candidates.empty(), "no codec can encode field '{}'", field.name);

            /// Whether to spend accuracy was settled by the policy, so a surviving lossy codec
            /// is used rather than weighed. Which one is `config.lossyCodec` -- see its comment
            /// for why that is a fixed knob and not a measurement across the candidates.
            if (getCodec(candidates.front()).isLossy())
            {
                INVARIANT(
                    std::ranges::find(candidates, config.lossyCodec) != candidates.end(),
                    "field '{}' (type {}) does not support the configured lossy codec '{}'",
                    field.name,
                    static_cast<int>(field.type),
                    codecName(config.lossyCodec));

                fields[c].lossyCodec = static_cast<uint8_t>(config.lossyCodec);
                /// The codes are already zero-based grid indices, so a frame-of-reference codec
                /// on top of them would spend a header subtracting a minimum that is already
                /// ~0. The one-piece form -- plain bit-packing, losslessCodec == 0 -- is the
                /// fixed choice for them.
                fields[c].losslessCodec = 0;
            }
            else
            {
                fields[c].lossyCodec = 0;
                fields[c].losslessCodec
                    = isIntegerLike(field.type) ? static_cast<uint8_t>(CodecId::ForBitpack) : static_cast<uint8_t>(CodecId::Raw);
            }
            stored = encodeWith(fields[c].lossyCodec, fields[c].losslessCodec);
        }

        achievedErrors[c] = stored.achievedError;
        uncompressedLengths[c] = stored.uncompressedLength;
        encodedValues[c] = std::move(stored.bytes);
    }

    codecsSelected = true;

    /// The group's time range arrives with its rows; it is never recovered by reading them
    /// back. MemoryStore measured it from the timestamps handed to writeRecord, FileStore
    /// folded it into the segment, and it lands here unchanged -- which is why this tier needs
    /// no knowledge of which column holds record time.
    GroupHeader header;
    header.rowCount = rowCount;
    header.numColumns = static_cast<uint32_t>(numColumns);

    header.minTs = minTs.getRawValue();
    header.maxTs = maxTs.getRawValue();

    /// Lay the group out: header, then the column table, then the column bytes. Offsets in
    /// the table are relative to the start of the data region, so a reader that has the
    /// table can pread any single column.
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
    writer.append(out.data(), out.size());
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

    /// Group ranges may only be used to skip when they describe the very field being
    /// filtered on. Ranges measured over one column say nothing about another, so skipping
    /// on a mismatch could drop groups that do contain matching rows -- a silently short
    /// answer. Falling back to per-row filtering is slower and always right.
    /// Every group carries a real range now, so there is nothing to gate skipping on -- the
    /// same unconditional overlap test MemoryStore applies to its buffers.

    auto destSpan = buffer.getAvailableMemoryArea<uint8_t>();
    const uint64_t maxDestTuples = destSpan.size() / rowWidth;
    /// A destination that cannot hold one row would return zero forever, and a zero is read as
    /// end of stream -- so the whole replay would come back empty with no error at all.
    PRECONDITION(maxDestTuples > 0, "destination buffer of {} bytes cannot hold a {}-byte row", destSpan.size(), rowWidth);

    uint64_t destTuples = 0;
    size_t unit = frame.unitIndex;
    uint64_t rowOffset = frame.rowOffset;

    std::vector<uint8_t> groupBytes;
    std::vector<uint8_t> rowScratch;

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

        /// A group's columns are decoded as a whole, so resuming inside one decodes it again. That
        /// is the price of a destination smaller than a group; sizing the read buffer above the
        /// group size avoids it entirely.
        groupBytes.resize(group.header.groupByteLength);
        const ssize_t got = writer.readAt(groupBytes.data(), groupBytes.size(), group.offset);
        PRECONDITION(
            got == static_cast<ssize_t>(groupBytes.size()), "short read of group at offset {}", group.offset);

        const uint64_t rowCount = group.header.rowCount;
        const size_t entriesOffset = GROUP_HEADER_BYTES;
        const size_t dataOffset = entriesOffset + (group.header.numColumns * COLUMN_ENTRY_BYTES);
        INVARIANT(
            group.header.numColumns == fields.size(),
            "group has {} columns, schema has {}",
            group.header.numColumns,
            fields.size());

        /// Rebuild whole rows, then filter. Decoding only the timestamp column first and
        /// touching the rest only for the rows that survive is the natural refinement here,
        /// and columnar storage is what makes it possible -- but it only pays on the at most
        /// two groups per query that straddle a range boundary, so it is left for later.
        rowScratch.assign(static_cast<size_t>(rowCount) * rowWidth, 0);

        for (size_t c = 0; c < fields.size(); ++c)
        {
            const auto& field = fields[c];
            const auto entry = parseColumnEntry(groupBytes.data() + entriesOffset + (c * COLUMN_ENTRY_BYTES));

            const uint8_t* stored = groupBytes.data() + dataOffset + entry.valuesOffset;

            /// Equal lengths mean the writer found compression unprofitable and kept the
            /// codec's own bytes, so there is nothing to undo.
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

            /// Mirrors `encodeWith`: whichever stage produced the bytes decodes them back
            /// through the same `ColumnCodec` interface. See its comment for why a lossy codec
            /// paired with a nonzero losslessCodecId is not a form any codec here still writes.
            INVARIANT(
                entry.lossyCodecId == 0 || entry.losslessCodecId == 0,
                "a second lossless stage ({}) over a lossy codec's codes is not implemented",
                codecName(static_cast<CodecId>(entry.losslessCodecId)));

            const CodecId id = entry.lossyCodecId != 0 ? static_cast<CodecId>(entry.lossyCodecId)
                                                         : static_cast<CodecId>(entry.losslessCodecId);
            getCodec(id).decode(payload, out);

            const uint8_t* bits = entry.nullBitsLength > 0 ? groupBytes.data() + dataOffset + entry.nullBitsOffset : nullptr;

            for (uint64_t r = 0; r < rowCount; ++r)
            {
                uint8_t* rowPtr = rowScratch.data() + (r * rowWidth);
                std::memcpy(rowPtr + field.valueOffset, values.data() + (r * field.valueWidth), field.valueWidth);
                if (bits != nullptr)
                {
                    const bool set = (bits[r / 8] & (1U << (r % 8))) != 0;
                    rowPtr[field.valueOffset - 1] = set ? 1U : 0U;
                }
            }
        }

        /// Whole group inside the range (or no range at all): take every row not yet taken.
        if (range.isUnbounded() || (groupMin >= range.start && groupMax < range.end))
        {
            const uint64_t toCopy = std::min(rowCount - rowOffset, maxDestTuples - destTuples);
            std::memcpy(destSpan.data() + (destTuples * rowWidth), rowScratch.data() + (rowOffset * rowWidth), toCopy * rowWidth);
            destTuples += toCopy;
            rowOffset += toCopy;
            if (rowOffset < rowCount)
            {
                break; /// destination filled part-way through this group; resume here
            }
            continue;
        }

        /// Otherwise the rows have to be looked at one by one.
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
        /// Rows LOOKED AT, not rows kept.
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
    /// Absent means lossless. Precision is only ever given up because someone said so here.
    if (const auto it = args.config.find("accuracyBoundPercent"); it != args.config.end())
    {
        cfg.accuracyBoundPercent = std::stod(it->second);
    }
    return makeStore<LossyStore>(std::move(cfg), args.schema);
}
}
