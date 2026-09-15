# CompressedStore — Component Notes

Source: `include/CompressedStore.hpp`, `src/CompressedStore.cpp`
(plus `CompressedSegmentFormat.hpp`, `CompressionCodec.{hpp,cpp}`, `FlushPolicy.hpp`,
`Store.hpp`, `StoreTransformation.hpp`, `BinaryStoreWriter.{hpp,cpp}`,
`StoreTransformationRegistry.hpp`, `StoreRegistry.cpp`)

## 1. What it is

`CompressedStore` is a **lossless, segment-based, compressed storage tier**. It is a
full peer of `MemoryStore` / `FileStore` / `ColumnStore` in the store-manager's
tiered-storage chain: it satisfies the same `StoreConcept` (see `Store.hpp`), so it
can sit anywhere in a chain — as a standalone tail store, as a middle tier that
receives data from an upstream store and forwards to a downstream store, or as the
head store that applications write into directly.

On disk it looks like:

```
[ file header ]                              (reused ReplayStoreFormat: MAGIC, VERSION, schemaText, file minTs/maxTs)
[ SegmentHeader ][ compressed bytes ]         <- segment 0
[ SegmentHeader ][ compressed bytes ]         <- segment 1
...
```

Each segment is compressed **as a whole** (not per-row). The `SegmentHeader` is
stored **uncompressed** in front of its payload for two reasons (see
`CompressedSegmentFormat.hpp`):

1. `minTs`/`maxTs` let `read()` skip an entire segment via `TimeRange::overlaps()`
   **without decompressing it** — the main speed win for range reads.
2. `uncompressedBytes` tells the codec how large the output buffer must be;
   `compressedBytes` tells the reader how many bytes to `pread` off disk.

```cpp
struct SegmentHeader
{
    uint64_t minTs;             // smallest timestamp in this segment
    uint64_t maxTs;             // largest timestamp in this segment
    uint64_t rowCount;          // rows packed into this segment
    uint64_t uncompressedBytes; // raw size pre-compression
    uint64_t compressedBytes;   // size of the compressed payload on disk
    uint8_t  codecId;           // which CompressionCodec::Type produced it
};
// fixed 41 bytes on disk (5*u64 + 1*u8)
```

## 2. Internal components (member state)

| Member | Purpose |
|---|---|
| `config` (`Config`) | `storeName`, `storeDir`, `schemaText`, `codec` (default ZSTD), `compressionLevel` (default 3), `segmentSize` (default 1 MB — raw bytes threshold to seal a segment) |
| `schema` | The row schema; can widen itself from an empty schema on first `writeRecord` |
| `codec` (`CompressionCodec`) | The compressor/decompressor (see §4) |
| `activeBuffer` | Uncompressed row-major bytes accumulating before the next seal — the "open" segment, direct analog of `MemoryStore`'s active `TupleBuffer` |
| `activeMinTs` / `activeMaxTs` / `activeRowCount` | Timestamp range and row count of the active buffer |
| `filePath`, `writer` (`BinaryStoreWriter`) | On-disk backing file for **sealed** segments (see §5) |
| `dataStartOffset` | Byte offset right after the file header; `drain()` truncates back to this offset |
| `fileMinTs` / `fileMaxTs` | Timestamp range across everything ever sealed to this file |
| `segments` (`vector<SegmentIndexEntry>`) | Lightweight in-memory index: `{offset, SegmentHeader}` per sealed segment — **payload bytes are not kept in memory**, only headers, so `read()`/pruning stays cheap |
| `mutex` (`shared_mutex`) | Guards all mutable state; readers take a shared lock, writers/sealers/drain take a unique lock |
| `nextLevel` / `transformation` / `flushPolicy` | Optional chaining state — empty for a standalone/tail store (see §6) |

## 3. Core operations

- **`writeRecord`** — append the record's bytes into `activeBuffer`, widen
  `activeMinTs`/`activeMaxTs`, increment `activeRowCount`. If `activeBuffer.size() >=
  config.segmentSize`, call `sealActiveSegment()`. Then, if chained and the flush
  policy trips on the *current total size* (sealed segments' compressed bytes +
  active buffer), call `flush(self)`.
- **`sealActiveSegment()`** (precondition: caller holds the unique lock) — compress
  the whole `activeBuffer` via `codec.compress(...)`, build a `SegmentHeader`,
  `writer.append(header, payload)`, push a `SegmentIndexEntry` (header only) into
  `segments`, widen `fileMinTs`/`fileMaxTs`, persist them via
  `writer.updateTimestamps(...)`, then clear the active buffer/timestamps/row count.
  This is the compression tier's analog of `MemoryStore` sealing its active
  `TupleBuffer` into `buffers`.
- **`read(buffer, readSchema, range)`** — reads the **lower tier first** (older
  data), exactly like `MemoryStore::read`, then reads its own segments:
  - Segments whose `[minTs, maxTs]` don't overlap `range` are skipped **without
    decompressing** (pure header check).
  - A segment fully inside the range is decompressed once and `memcpy`'d wholesale.
  - A segment partially overlapping the range is decompressed, then filtered
    row-by-row using `findFieldValueOffset` to locate the timestamp field's byte
    offset in the packed row layout.
- **`hasMore()`** — true if this store has sealed segments, or the next level does.
- **`appendRawBytes` / `updateTimestamps`** — bulk-ingest API used by an *upstream*
  transformation (e.g. `FileToCompressedTransformation`) to hand this store raw
  row-major bytes without going through `writeRecord` one row at a time. Bytes are
  just appended to `activeBuffer`; nothing is compressed until the next seal.
- **`drain()`** — decompresses **every** sealed segment plus the pending active
  buffer into one row-major byte blob (`DrainedData{bytes, minTs, maxTs}`), then
  resets the store to empty and truncates the file back to `dataStartOffset`. This
  is how a *downstream* transformation (e.g. `CompressedToColumnTransformation`)
  pulls this store's data out to hand to the next tier. Mirrors
  `FileStore::drain()` / `ColumnStore::drain()`.
- **`open()` / `close()`** — open/close the backing file, then propagate to
  `nextLevel` if chained; `close()` also seals any pending active buffer and, if
  chained, runs the transformation once more before closing downstream (so no data
  is silently dropped on shutdown).

## 4. CompressionCodec

`CompressionCodec` (`CompressionCodec.hpp`/`.cpp`) is the **only** place that knows
about the underlying compression library (currently zstd). It's a small
compressor+decompressor pair:

- `Type` enum: `ZSTD = 1` (implemented), `SNAPPY = 2` (reserved, not yet
  implemented) — **the numeric value is persisted on disk** in every
  `SegmentHeader::codecId`, so it must never be renumbered once files exist.
- `compress(src, len)` / `decompress(src, compressedLen, uncompressedLen)` —
  `uncompressedLen` must be supplied (it's read back from the segment header) so the
  decompressor can size its output buffer exactly.
- `level` (1..22 for zstd, default 3) only affects compression, ignored on decompress.

`CompressedStore` owns one `CompressionCodec` instance (`config.codec`,
`config.compressionLevel`), used for every segment it seals.

## 5. BinaryStoreWriter

`BinaryStoreWriter` is the POSIX file I/O layer shared with `FileStore` (and
`ColumnStore`). It gives `CompressedStore`:

- `open()` / `ensureHeader()` — opens the fd, writes the file header exactly once
  (MAGIC/VERSION/schemaText/min-max timestamps), via `ReplayStoreFormat`.
- `append(data, len)` — atomic offset reservation + `pwrite`, so segments can be
  appended from a writer without a global file lock beyond `CompressedStore`'s own
  `mutex`.
- `readAt(dest, len, offset)` — `pread`, used by `read()`/`drain()` to fetch a
  segment's compressed payload directly from disk (payload bytes are never kept
  resident — only the lightweight `segments` header index is).
- `updateTimestamps(minTs, maxTs)` — rewrites the header's timestamp fields in
  place.
- `truncateTo(offset)` — used by `drain()` to shrink the file back to just the
  header once all segments have been read out and handed downstream.

`CompressedStore` uses a filename prefix `compressed_<storeName>_<timestamp>.bin`
(`generateFilePath` in `CompressedStore.cpp`), deliberately distinct from
`FileStore`'s `replay_` prefix, because a `FileStore` and a `CompressedStore`
chained together share the same `storeDir`/`storeName` and must not collide on the
same generated path.

## 6. Store chaining, `Store`/`StoreTransformation` type erasure

`Store` (`Store.hpp`) is a type-erased, `shared_ptr`-backed wrapper around anything
satisfying `StoreConcept` (`open/close/flush/writeRecord/read/hasMore/getSchema/
size/typeName`). `makeStore<CompressedStore>(...)` wraps a `CompressedStore` into a
`Store` handle; `Store::getAs<CompressedStore>()` / `tryGetAs<...>()` recovers the
concrete type (used by transformations to reach `drain()`/`appendRawBytes()`, which
aren't part of `StoreConcept` itself).

`StoreTransformation` (`StoreTransformation.hpp`) is the same type-erasure pattern
for the `execute(Store& source, Store& dest)` + `name()` interface — the glue that
moves data between two adjacent tiers.

`CompressedStore` has two constructors:

- **Standalone/tail**: `CompressedStore(Config, Schema)` — no `nextLevel`, `flush()`
  only seals its own pending buffer; nothing is pushed further down.
- **Chained**: `CompressedStore(Config, Schema, Store nextLevel, FlushPolicy
  policy)` — looks up `"CompressedStore" -> nextLevel->typeName()` in
  `StoreTransformationRegistry::instance()` and asserts it exists
  (`INVARIANT(found.has_value(), ...)`). Today the only registered
  target is `ColumnStore`, via `CompressedToColumnTransformation`
  (registered under the key `CompressedStore_to_ColumnStore`).

### Registry lookup mechanics (`StoreTransformationRegistry.hpp`)

Transformations are registered by a generated plugin registrar
(`add_plugin(CompressedStore_to_ColumnStore StoreTransformation
CompressedToColumnTransformation.cpp)` in `src/CMakeLists.txt`) under the key
`"<sourceTypeName>_to_<destTypeName>"`. `findTransformation(source, dest)` builds
that key and looks it up. `CompressedStore`'s chained constructor and
`StoreRegistry::registerConfiguredStore` both go through this same lookup — there is
no hardcoded reference to `CompressedToColumnTransformation` inside
`CompressedStore` itself, it's fully registry-driven.

## 7. FlushPolicy — how/when CompressedStore pushes data downstream

`FlushPolicy` (`FlushPolicy.hpp`) is a small value type shared by every chained
store (`MemoryStore`, `FileStore`, `CompressedStore`, `ColumnStore`):

```cpp
struct FlushPolicy
{
    enum class Type : uint8_t { SIZE_THRESHOLD };
    Type type = Type::SIZE_THRESHOLD;
    size_t sizeThreshold = 64 MB;   // default

    bool shouldFlush(uint64_t currentSize) const;  // currentSize >= sizeThreshold
};
```

Only one policy kind exists today: **byte-size threshold**. `CompressedStore` checks
it inside `writeRecord`, **after** a potential segment seal:

```cpp
const bool doFlush = flushPolicy && flushPolicy->shouldFlush(currentSizeLocked());
if (doFlush) { lock.unlock(); flush(self); }
```

`currentSizeLocked()` = sum of every sealed segment's **compressed** bytes +
`activeBuffer.size()` (uncompressed, since it isn't sealed yet). So the threshold is
measured against a mix of compressed-on-disk size and raw pending size — it fires
once the store's *approximate footprint* crosses `sizeThreshold`, not once a fixed
number of segments accumulates.

**Important distinction: seal vs. flush.**
- *Sealing* (triggered by `config.segmentSize`, a `CompressedStore::Config` field)
  only compresses the active buffer into a new segment on disk — data stays in
  *this* tier.
- *Flushing* (triggered by `FlushPolicy`, supplied by whichever caller constructs
  this store in a chain) is what actually **moves data to the next tier** — it seals
  first, then runs `transformation->execute(self, *nextLevel)` followed by
  `nextLevel->flush()`, recursively cascading the flush down the whole chain.

So a single `CompressedStore` can seal many small segments internally long before it
ever flushes downstream; segment size and flush threshold are independent knobs.

### `flush(Store& self)` (CompressedStore.cpp:210)

```cpp
void CompressedStore::flush(Store& self)
{
    { unique_lock; sealActiveSegment(); }        // 1. finalize own pending data
    if (nextLevel && transformation) {
        transformation->execute(self, *nextLevel); // 2. drain self -> append to nextLevel
        nextLevel->flush();                         //    cascade the flush recursively
    }
}
```

This is a **direct structural mirror of `MemoryStore::flush`** (seal own pending
data, then hand off + recurse) — the design note at the top of `CompressedStore.hpp`
calls this out explicitly: "flush : seal the active buffer into a compressed
segment (finalize own pending data), THEN transformation->execute + nextLevel->flush."

### `close(Store& self)`

Same idea but for shutdown: seals the active buffer, closes the writer, then — if
chained — runs the transformation **once more** (to push whatever was just sealed)
before recursively closing `nextLevel`. This guarantees no data is stranded in a
closed store.

## 8. Drain — how downstream pulls data out of CompressedStore

`drain()` is the mechanism a **transformation** uses to actually move bytes out of
`CompressedStore` and into the next tier; it is *not* itself the flush trigger — it
is invoked *by* a transformation, which is invoked *by* `flush()`/`close()` once the
policy trips.

```cpp
CompressedStore::DrainedData CompressedStore::drain()
{
    unique_lock;
    // decompress every sealed segment, concatenate into out.bytes
    // append the still-raw activeBuffer as-is (no compress/decompress round trip)
    // widen out.minTs/out.maxTs across both sources
    segments.clear(); activeBuffer.clear(); reset timestamps/row count;
    writer.truncateTo(dataStartOffset);   // shrink file back to just the header
    return out;
}
```

`DrainedData{bytes, minTs, maxTs}` is **row-major bytes matching the schema's
packed layout** — this is the common currency at every tier boundary in the store
chain (`FileStore::DrainedData`, `CompressedStore::DrainedData`,
`ColumnStore::DrainedData` all share this shape), regardless of how each tier
stores data internally (row-major flat / compressed segments / column-major row
groups).

## 9. CompressedToColumnTransformation — the concrete downstream hookup

`CompressedToColumnTransformation::execute(Store& source, Store& dest)`
(`CompressedToColumnTransformation.cpp`) is the only transformation currently
registered *from* `CompressedStore`:

```cpp
void CompressedToColumnTransformation::execute(Store& source, Store& dest)
{
    auto& compressedStore = source.getAs<CompressedStore>().getMutable();
    auto drained = compressedStore.drain();          // decompress everything, reset self
    if (drained.bytes.empty()) return;                // nothing to move

    auto& columnStore = dest.getAs<ColumnStore>().getMutable();
    columnStore.appendRawBytes(drained.bytes.data(), drained.bytes.size());
    columnStore.updateTimestamps(drained.minTs, drained.maxTs);
    // the nextLevel->flush() issued right after by CompressedStore::flush()
    // is what actually seals these bytes into a ColumnStore row group.
}
```

It is registered via the generated plugin registrar under key
`CompressedStore_to_ColumnStore`:

```cpp
StoreTransformationGeneratedRegistrar::RegisterCompressedStore_to_ColumnStoreStoreTransformation(...)
{ return StoreManager::CompressedToColumnTransformation{}; }
```

This is the on-disk-format analog of `FileToCompressedTransformation`
(`FileStore -> CompressedStore`), which does the same drain/append/updateTimestamps
dance one tier up (drains `FileStore`'s flat row-major file, appends its bytes into
`CompressedStore`'s active buffer via `appendRawBytes`).

## 10. Where CompressedStore fits: StoreRegistry chain assembly

`StoreRegistry::registerConfiguredStore` (`StoreRegistry.cpp`) parses a
`storeOrder` string like `"MemoryStore->FileStore->CompressedStore->ColumnStore"`
and builds the object graph **tail-first** (last name in the order = innermost/tail
store, built first; each earlier store is constructed with a `Store` handle to the
one after it). Chains involving `CompressedStore` that are explicitly supported:

- `MemoryStore -> FileStore -> CompressedStore -> ColumnStore` (full 4-tier chain)
- `MemoryStore -> FileStore -> CompressedStore`
- `FileStore -> CompressedStore -> ColumnStore`
- `FileStore -> CompressedStore`
- `CompressedStore -> ColumnStore`
- `CompressedStore` (standalone)

All chains reuse the **same `FlushPolicy` instance** across every tier boundary in
that chain (built once from `config.memoryBufferSize`, type `SIZE_THRESHOLD`) — so
today every hop in a given store's chain flushes at the same byte-size threshold;
there's no per-tier-specific threshold configuration yet.

Example (full 4-tier chain), showing the tail-first construction order:

```cpp
auto columnStore    = makeStore<ColumnStore>(cfg, schema);                         // tail, standalone
auto compressedStore = makeStore<CompressedStore>(cfg, schema, columnStore, policy);// looks up CompressedStore->ColumnStore
auto fileStore       = makeStore<FileStore>(cfg, schema, compressedStore,
                                             *fileToCompressedTransformation, policy);
auto headStore       = makeStore<MemoryStore>(schema, cfg, bufferManager,
                                               fileStore, policy);                  // looks up MemoryStore->FileStore
stores.emplace(storeName, headStore);
```

Writes go into `headStore` (`MemoryStore`); `flush()` cascades down through
`FileStore -> CompressedStore -> ColumnStore` via the drain/append/flush pattern
described in §7–§9. Reads (`read()`) walk the chain in the opposite direction —
`nextLevel->read()` first (oldest tier / most historical data), then the current
tier's own data — so results come back oldest-first across the whole chain.

## 11. Testing surface

- `tests/CompressedStoreTest.cpp` — standalone `CompressedStore` behavior: segment
  sealing at `segmentSize`, compressed round-trip correctness, time-range pruning
  (whole-segment skip vs. per-row filter), `drain()` resetting state + truncating
  the file, debug introspection (`debugActiveRowCount`, `debugSegmentCount`).
- `tests/CompressedToColumnChainTest.cpp` — the chained
  `CompressedStore -> ColumnStore` path: flush policy tripping, transformation
  draining/appending correctly, data surviving the compress -> decompress ->
  transpose round trip.
- `nes-systests/operator/replay/CompressedToColumnStoreConfig.test` — end-to-end
  systest exercising a configured store chain through the query engine.

## 12. Summary of the flush/drain relationship (the part most likely to confuse)

- **Seal** (segment/row-group granularity) = internal housekeeping, governed by
  `Config::segmentSize` (bytes) — keeps `activeBuffer` from growing unbounded and
  gets data onto disk (compressed) sooner. Does **not** move data to another tier.
- **Flush** (tier-to-tier granularity) = governed by `FlushPolicy::sizeThreshold`
  (bytes, checked against sealed+pending size) — seals whatever's pending, then
  **drains + hands off + recursively flushes** the next tier. This is what actually
  advances data through the `MemoryStore -> FileStore -> CompressedStore ->
  ColumnStore` pipeline.
- **Drain** = the *mechanism* a transformation uses inside a flush to pull all data
  (decompressed, row-major) out of a store and reset it to empty — always paired
  with an `appendRawBytes`/`updateTimestamps` call on the destination store.
- **Close** = same cascade as flush, but final: seals, transforms once more, then
  propagates `close()` down the chain so nothing is left stranded when the store
  shuts down.
