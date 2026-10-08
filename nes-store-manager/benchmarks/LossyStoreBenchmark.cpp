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

/// LossyStore, timed directly. No SQL, no query engine, and the CSV parse outside the measurement.
///
/// The systest benchmarks in compression_benchmarks/ time a whole query, and on this workload about
/// three quarters of a write query is the CSV parse. Subtracting a store-free baseline says how
/// much is the store; it does not say what the store spends it ON.
///
/// The stage breakdown comes from timers INSIDE the store (StoreProfile.hpp), not from this file
/// calling the codecs itself. An earlier version did the latter and it was the wrong instrument: a
/// replica agrees with the real path on the codecs and the order, then quietly disagrees about
/// everything around them -- the transpose the store fuses into its column build, the compressor it
/// runs on every column and keeps only when smaller, the bytes it actually writes. What is reported
/// here is what actually ran.
///
/// Needs -DNES_STORE_PROFILING=ON. Without it the timers compile to nothing and the stage lines
/// read zero, which the program says rather than printing a misleading breakdown.
///
/// WHAT CANNOT BE SPLIT. writeRecord is called once per row -- 42,700,000 times here -- so two
/// clock reads inside it would be about two seconds against a five-second write. Its own cost
/// (the lock, the preconditions, the pending insert, the timestamp min/max) is therefore DERIVED as
/// the write total minus sealGroup, and those four cannot be separated from one another. Everything
/// else is at group or read-call granularity, where a timer is noise.
///
/// Timing is plain steady_clock rather than Google Benchmark: gbenchmark repeats until a
/// measurement stabilises, which on a 42.7M-row store write is many minutes at load.

#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>

#include <LossyStore.hpp>
#include <Store.hpp>
#include <StoreProfile.hpp>
#include <StoreReadCursor.hpp>
#include <TimeRange.hpp>
#include <lossy/ColumnCodec.hpp>
#include <lossy/ColumnStats.hpp>

namespace
{
using Clock = std::chrono::steady_clock;
using NES::DataType;
using NES::Schema;
using NES::Timestamp;
using NES::Lossy::CodecId;

double since(const Clock::time_point start)
{
    return std::chrono::duration<double>(Clock::now() - start).count();
}

double ns(const uint64_t nanoseconds)
{
    return static_cast<double>(nanoseconds) / 1e9;
}

struct Options
{
    /// The release tree stages its own testdata (ENABLE_LARGE_TESTS=ON plus a shared ExternalData
    /// object store), so this does not depend on the Debug tree existing.
    std::string csv = "cmake-build-release/nes-systests/testdata/large/debs/SOLAR_PANELS_TOPIC_1G.csv";
    uint64_t rows = 0; /// 0 = the whole file
    double bound = 1.0;
    size_t groupKb = 256;
    int repeat = 1;
    bool notNull = false;
    bool ranking = false;
    std::string storeDir = "/tmp";
    /// Appended to, not overwritten, so a sweep accumulates one row per run.
    std::string csvOut;
};

/// The DEBS solar schema, matching compression_benchmarks/tests/solar_standalone.test. Nullable by
/// default, because those tests are: four columns each carrying a one-byte indicator, so a stored
/// row is 28 B and not 24. Keeping that makes a number here subtractable from a number there.
Schema makeSchema(const bool notNull)
{
    Schema schema;
    /// Two-argument addField is the NOT NULL form; the three-argument one takes the nullable tag.
    const auto add = [&](const char* name, const DataType::Type type)
    { schema = notNull ? schema.addField(name, type) : schema.addField(name, type, DataType::NULLABLE::IS_NULLABLE); };
    add("producerId", DataType::Type::INT32);
    add("groupId", DataType::Type::INT32);
    add("producedPower", DataType::Type::FLOAT64);
    add("timestamp", DataType::Type::UINT64);
    return schema;
}

/// Offset of a field's VALUE within a stored row. A nullable field carries its indicator byte
/// first, so the value sits one byte in -- the same rule LossyStore itself applies.
uint32_t valueOffset(const Schema& schema, const std::string& name)
{
    uint32_t offset = 0;
    for (size_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        const auto& field = schema.getFieldAt(i);
        if (field.getUnqualifiedName() == name)
        {
            return offset + (field.dataType.nullable ? 1 : 0);
        }
        offset += field.dataType.getSizeInBytesWithNull();
    }
    throw std::runtime_error("no field named " + name);
}

/// Parse the CSV into packed rows. TIMED AND REPORTED, but outside every store measurement: it is
/// the work the systest benchmarks spend three quarters of a write query on, so it is worth seeing
/// even when it is not what is being measured.
std::vector<uint8_t> loadRows(const Options& opts, const Schema& schema, uint64_t& rowsLoaded, double& seconds)
{
    const uint32_t width = schema.getSizeOfSchemaInBytes();
    const uint32_t offProducer = valueOffset(schema, "producerId");
    const uint32_t offGroup = valueOffset(schema, "groupId");
    const uint32_t offPower = valueOffset(schema, "producedPower");
    const uint32_t offTs = valueOffset(schema, "timestamp");

    std::ifstream in(opts.csv);
    if (!in)
    {
        throw std::runtime_error("cannot open " + opts.csv);
    }

    const auto start = Clock::now();
    std::vector<uint8_t> rows;
    if (opts.rows > 0)
    {
        rows.reserve(opts.rows * width);
    }
    std::vector<uint8_t> row(width, 0);
    std::string line;
    rowsLoaded = 0;
    while (std::getline(in, line))
    {
        if (opts.rows > 0 && rowsLoaded >= opts.rows)
        {
            break;
        }
        const char* p = line.data();
        const char* end = p + line.size();
        int32_t producer = 0;
        int32_t group = 0;
        uint64_t ts = 0;
        const auto comma = [&]
        {
            while (p < end && *p != ',')
            {
                ++p;
            }
            if (p < end)
            {
                ++p;
            }
        };

        std::from_chars(p, end, producer);
        comma();
        std::from_chars(p, end, group);
        comma();
        char* after = nullptr;
        const double power = std::strtod(p, &after);
        p = after;
        if (p < end && *p == ',')
        {
            ++p;
        }
        std::from_chars(p, end, ts);

        std::memset(row.data(), 0, width);
        std::memcpy(row.data() + offProducer, &producer, sizeof(producer));
        std::memcpy(row.data() + offGroup, &group, sizeof(group));
        std::memcpy(row.data() + offPower, &power, sizeof(power));
        std::memcpy(row.data() + offTs, &ts, sizeof(ts));
        rows.insert(rows.end(), row.begin(), row.end());
        ++rowsLoaded;
    }
    seconds = since(start);
    return rows;
}

struct Run
{
    double write = 0;
    double flush = 0;
    double read = 0;
    uint64_t rowsRead = 0;
    uint64_t diskBytes = 0;
    /// A plain Snapshot, not the atomic Counters: Run has to be returned by value and kept across
    /// repeats, and std::atomic is neither copyable nor movable.
    NES::StoreProfile::Snapshot profile{};
};

/// One pass: open, write every row, flush, read it all back. The store's own API, nothing around it.
Run measure(const std::vector<uint8_t>& rows, const uint64_t rowCount, const Schema& schema, const Options& opts)
{
    const uint32_t width = schema.getSizeOfSchemaInBytes();
    const uint32_t offTs = valueOffset(schema, "timestamp");

    NES::LossyStore::Config cfg;
    cfg.storeName = "bench";
    cfg.storeDir = opts.storeDir;
    cfg.groupSize = opts.groupKb * 1024UZ;
    cfg.accuracyBoundPercent = opts.bound;

    NES::StoreProfile::reset();
    NES::Store store = NES::makeStore<NES::LossyStore>(cfg, schema);
    store.open();

    Run out;
    auto t = Clock::now();
    for (uint64_t r = 0; r < rowCount; ++r)
    {
        const uint8_t* rec = rows.data() + (r * width);
        uint64_t ts = 0;
        std::memcpy(&ts, rec + offTs, sizeof(ts));
        /// Three arguments, not four: the Store wrapper passes itself down to the implementation,
        /// which is what the `Store& self` parameter on LossyStore::writeRecord receives.
        store.writeRecord(rec, width, Timestamp(ts));
    }
    out.write = since(t);

    t = Clock::now();
    store.flush();
    out.flush = since(t);
    out.diskBytes = store.getAs<NES::LossyStore>().get().size();

    /// 4 KB destination, the size the engine actually hands a store, so the read is paced the way a
    /// replay paces it rather than in one enormous buffer.
    auto buffers = NES::BufferManager::create(4096, 16);
    NES::StoreReadCursor cursor;
    t = Clock::now();
    for (;;)
    {
        auto buffer = buffers->getBufferBlocking();
        const uint64_t got = store.read(buffer, schema, NES::TimeRange{}, cursor);
        if (got == 0)
        {
            break;
        }
        out.rowsRead += got;
    }
    out.read = since(t);
    out.profile = NES::StoreProfile::snapshot();
    return out;
}

/// The path LossyStore does NOT take: computeStats / filterCandidates / rank. The comment at
/// LossyStore.cpp:326-332 keeps that machinery "reachable for a benchmark to call directly" and
/// says the ratio-vs-complexity tradeoff "is exactly what the benchmarks decide". This decides it.
struct Ranking
{
    double decide = 0;
    uint64_t fixedBytes = 0;
    uint64_t rankedBytes = 0;
};

Ranking ranking(const std::vector<uint8_t>& rows, const uint64_t rowCount, const Schema& schema, const Options& opts)
{
    const uint32_t width = schema.getSizeOfSchemaInBytes();
    const uint64_t rowsPerGroup = std::max<uint64_t>(1, (opts.groupKb * 1024UZ) / width);

    struct Col
    {
        DataType::Type type;
        uint32_t valueWidth;
        uint32_t offset;
        CodecId fixed;
    };
    const std::vector<Col> cols{
        {DataType::Type::INT32, 4, valueOffset(schema, "producerId"), CodecId::ForBitpack},
        {DataType::Type::INT32, 4, valueOffset(schema, "groupId"), CodecId::ForBitpack},
        {DataType::Type::UINT64, 8, valueOffset(schema, "timestamp"), CodecId::ForBitpack},
    };

    NES::Lossy::CompressionPolicy policy;
    std::vector<uint8_t> column;
    double decide = 0;
    uint64_t fixedBytes = 0;
    uint64_t rankedBytes = 0;

    for (uint64_t start = 0; start < rowCount; start += rowsPerGroup)
    {
        const uint64_t n = std::min(rowsPerGroup, rowCount - start);
        for (const Col& col : cols)
        {
            column.resize(n * col.valueWidth);
            for (uint64_t r = 0; r < n; ++r)
            {
                std::memcpy(
                    column.data() + (r * col.valueWidth), rows.data() + ((start + r) * width) + col.offset, col.valueWidth);
            }
            const NES::Lossy::ColumnValues in{col.type, column.data(), n, col.valueWidth};

            const auto t = Clock::now();
            const auto stats = NES::Lossy::computeStats(in);
            const auto candidates = NES::Lossy::filterCandidates(col.type, policy);
            const auto ranked = NES::Lossy::rank(candidates, stats, policy);
            decide += since(t);

            fixedBytes += NES::Lossy::getCodec(col.fixed).encode(in, 0.0).bytes.size();
            rankedBytes += NES::Lossy::getCodec(ranked.codecs[0]).encode(in, 0.0).bytes.size();
        }
    }

    std::printf("\nWOULD RANKING PAY?  (the path LossyStore does not take)\n");
    std::printf("  %-44s %10.3f s\n", "computeStats + filterCandidates + rank", decide);
    std::printf("  %-44s %10lu B -> %lu B  (%+.2f%%)\n", "integer columns, fixed rule -> ranked", fixedBytes, rankedBytes,
                fixedBytes > 0 ? 100.0 * (static_cast<double>(rankedBytes) - static_cast<double>(fixedBytes))
                        / static_cast<double>(fixedBytes)
                               : 0.0);
    return Ranking{.decide = decide, .fixedBytes = fixedBytes, .rankedBytes = rankedBytes};
}

void line(const char* label, const double seconds, const double total, const uint64_t bytes = 0)
{
    std::printf("  %-44s %9.3f s %6.1f%%", label, seconds, total > 0 ? 100.0 * seconds / total : 0.0);
    if (bytes > 0 && seconds > 0.0005)
    {
        std::printf("  %9.1f MB/s", static_cast<double>(bytes) / seconds / 1e6);
    }
    std::printf("\n");
}

Options parse(const int argc, char** argv)
{
    Options o;
    for (int i = 1; i < argc; ++i)
    {
        const std::string_view a = argv[i];
        const auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        if (a == "--csv") { o.csv = next(); }
        else if (a == "--rows") { o.rows = std::stoull(next()); }
        else if (a == "--bound") { o.bound = std::stod(next()); }
        else if (a == "--group-kb") { o.groupKb = std::stoull(next()); }
        else if (a == "--repeat") { o.repeat = std::stoi(next()); }
        else if (a == "--store-dir") { o.storeDir = next(); }
        else if (a == "--not-null") { o.notNull = true; }
        else if (a == "--ranking") { o.ranking = true; }
        else if (a == "--csv-out") { o.csvOut = next(); }
        else if (a == "--help")
        {
            std::printf("usage: lossy-store-benchmark [--csv PATH] [--rows N] [--bound PCT]\n"
                        "       [--group-kb KB] [--repeat N] [--not-null] [--ranking]\n"
                        "       [--store-dir DIR] [--csv-out PATH]\n");
            std::exit(0);
        }
        else
        {
            std::fprintf(stderr, "unknown argument: %s\n", std::string(a).c_str());
            std::exit(1);
        }
    }
    return o;
}
}

int main(const int argc, char** argv)
{
    const Options opts = parse(argc, argv);
    const Schema schema = makeSchema(opts.notNull);
    const uint32_t width = schema.getSizeOfSchemaInBytes();

#ifndef NES_STORE_PROFILING
    std::printf("WARNING: built without -DNES_STORE_PROFILING=ON. The stage breakdown will read\n"
                "         zero; only the three wall-clock totals are meaningful.\n\n");
#endif

    uint64_t rowCount = 0;
    double csvSeconds = 0;
    std::printf("loading %s ...\n", opts.csv.c_str());
    const auto rows = loadRows(opts, schema, rowCount, csvSeconds);
    const uint64_t stored = rowCount * width;
    std::printf("  %lu rows x %u B = %lu B stored   bound %.3f%%   group %lu KB   N=%d\n",
                rowCount, width, stored, opts.bound, opts.groupKb, opts.repeat);

    Run best{};
    for (int i = 0; i < opts.repeat; ++i)
    {
        Run r = measure(rows, rowCount, schema, opts);
        /// Keep the FASTEST repeat, not the mean: a slow repeat is interference, and this measures
        /// the code rather than the machine's mood.
        if (i == 0 || r.write + r.flush + r.read < best.write + best.flush + best.read)
        {
            best = std::move(r);
        }
    }

    const auto& p = best.profile;
    const double writeTotal = best.write + best.flush;
    const double seal = ns(p.sealGroupNs);
    const double readTotal = best.read;
    const double readStages = ns(p.readIoNs) + ns(p.decompressNs) + ns(p.decodeNs) + ns(p.transposeInNs);

    std::printf("\nCSV  (outside every measurement below -- this is the engine's cost, not the store's)\n");
    line("parse into packed rows", csvSeconds, csvSeconds + writeTotal + readTotal, stored);

    std::printf("\nWRITE   %.3f s\n", writeTotal);
    line("  writeRecord overhead (lock, checks, buffering)", writeTotal - seal, writeTotal);
    line("  group finalization", seal - ns(p.writeIoNs), writeTotal);
    line("    row -> column", ns(p.transposeOutNs), writeTotal, stored);
    line("    quantization", ns(p.quantizeNs), writeTotal);
    line("    bitpacking", ns(p.bitpackNs), writeTotal);
    line("    compression", ns(p.compressNs), writeTotal);
    line("    (header, serialize, index)", seal - ns(p.writeIoNs) - ns(p.transposeOutNs) - ns(p.quantizeNs)
             - ns(p.bitpackNs) - ns(p.compressNs), writeTotal);
    line("  I/O: writing group to disk", ns(p.writeIoNs), writeTotal, best.diskBytes);

    std::printf("\nREAD    %.3f s\n", readTotal);
    line("  file I/O", ns(p.readIoNs), readTotal, best.diskBytes);
    line("  decompression", ns(p.decompressNs), readTotal);
    line("  decoding", ns(p.decodeNs), readTotal);
    line("  column -> row", ns(p.transposeInNs), readTotal, stored);
    line("  cursor/buffer management", readTotal - readStages, readTotal);

    std::printf("\n  %-44s %9lu groups, %lu read calls\n", "shape",
                p.groupsSealed, p.readCalls);
    std::printf("  %-44s %9lu rows, %lu B on disk, ratio %.3fx\n", "verified", best.rowsRead, best.diskBytes,
                best.diskBytes > 0 ? static_cast<double>(stored) / static_cast<double>(best.diskBytes) : 0.0);

    Ranking rank{};
    if (opts.ranking)
    {
        rank = ranking(rows, rowCount, schema, opts);
    }

    if (!opts.csvOut.empty())
    {
        const bool fresh = !std::ifstream(opts.csvOut).good();
        std::ofstream out(opts.csvOut, std::ios::app);
        if (fresh)
        {
            out << "dataset,rows,row_width_bytes,stored_bytes,group_kb,bound_pct,repeats,"
                   "csv_s,write_s,read_s,"
                   "writerecord_overhead_s,group_finalization_s,transpose_out_s,quantize_s,bitpack_s,"
                   "compress_s,serialize_s,write_io_s,"
                   "read_io_s,decompress_s,decode_s,transpose_in_s,cursor_mgmt_s,"
                   "groups,read_calls,disk_bytes,ratio,rank_decide_s,rank_fixed_bytes,rank_ranked_bytes\n";
        }
        const double finalization = seal - ns(p.writeIoNs);
        const double serialize = finalization - ns(p.transposeOutNs) - ns(p.quantizeNs) - ns(p.bitpackNs) - ns(p.compressNs);
        out << "debs_solar_panels," << rowCount << ',' << width << ',' << stored << ',' << opts.groupKb << ','
            << opts.bound << ',' << opts.repeat << ','
            << csvSeconds << ',' << writeTotal << ',' << readTotal << ','
            << (writeTotal - seal) << ',' << finalization << ',' << ns(p.transposeOutNs) << ','
            << ns(p.quantizeNs) << ',' << ns(p.bitpackNs) << ',' << ns(p.compressNs) << ','
            << serialize << ',' << ns(p.writeIoNs) << ','
            << ns(p.readIoNs) << ',' << ns(p.decompressNs) << ',' << ns(p.decodeNs) << ','
            << ns(p.transposeInNs) << ',' << (readTotal - readStages) << ','
            << p.groupsSealed << ',' << p.readCalls << ',' << best.diskBytes << ','
            << (best.diskBytes > 0 ? static_cast<double>(stored) / static_cast<double>(best.diskBytes) : 0.0) << ','
            << (opts.ranking ? rank.decide : 0.0) << ',' << rank.fixedBytes << ',' << rank.rankedBytes << '\n';
        std::printf("\n  appended a row to %s\n", opts.csvOut.c_str());
    }
    return 0;
}
