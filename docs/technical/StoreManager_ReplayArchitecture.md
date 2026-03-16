# Store Manager and Replay Architecture

The Store Manager provides a composable, extensible storage layer for persisting and replaying streaming data.
Stores can be arranged in a hierarchy (e.g., memory buffer then file) with automatic flush, and the system is
extensible through registries for both store types and transformations between them.

## Overview

The store system has four layers:

1. **Store abstraction** -- C++20 concept with type erasure (`StoreConcept` / `Store`)
2. **Concrete stores** -- `MemoryStore`, `FileStore`, `HierarchicalStore`
3. **Registries** -- factory registries for store types and transformations, plus an instance registry
4. **Pipeline integration** -- physical operators and sources that read/write through the `Store` interface

The default storage pipeline is:

```
MemoryStore (64 MB buffer) --> FileStore (binary file on disk)
```

Data is written to the MemoryStore first. When it exceeds the size threshold, a `MemoryToFileTransformation`
drains the buffers and writes them to the FileStore. Reads happen from the bottom of the hierarchy (FileStore).

## Store Concept and Type Erasure

The store interface is defined as a C++20 concept in `nes-store-manager/include/Store.hpp`:

```cpp
template <typename T>
concept StoreConcept = requires(T& store, const T& constStore,
                                TupleBuffer buffer, TupleBuffer& bufferRef,
                                const Schema& schema) {
    { store.open() };
    { store.close() };
    { store.flush() };
    { store.write(std::move(buffer), schema) };
    { store.read(bufferRef, schema) } -> std::convertible_to<uint64_t>;
    { constStore.hasMore() }          -> std::convertible_to<bool>;
    { constStore.getSchema() }        -> std::convertible_to<Schema>;
    { constStore.size() }             -> std::convertible_to<uint64_t>;
    { constStore.typeName() } noexcept -> std::convertible_to<std::string_view>;
};
```

Any plain class or struct that satisfies this concept can be used as a store -- no virtual inheritance required
from the user's perspective.

### Type erasure pattern

The pattern follows `LogicalOperatorConcept` / `TypedLogicalOperator` from `nes-logical-operators`:

| Component | Role |
|---|---|
| `StoreConcept` | Compile-time interface check |
| `detail::ErasedStore` | Hidden virtual base class |
| `detail::StoreModel<T>` | Bridge: wraps a concrete `T` satisfying `StoreConcept` |
| `TypedStore<>` / `Store` | Public type-erased handle with value semantics |

Concrete stores are non-copyable and non-movable (they hold mutexes, file descriptors, etc.), so they are
constructed in-place through the `makeStore` factory:

```cpp
// Creates a Store wrapping a MemoryStore constructed with the given arguments
Store store = makeStore<MemoryStore>(schema);

// Creates a Store wrapping a FileStore
Store store = makeStore<FileStore>(
    FileStore::Config{.storeName = "S1", .filePath = "/tmp/s1.bin", .schemaText = "..."},
    schema);
```

### Downcasting

The `Store` handle supports safe downcasting for cases where access to the concrete type is needed:

```cpp
// Returns std::optional<TypedStore<FileStore>>
auto maybeFile = store.tryGetAs<FileStore>();
if (maybeFile) {
    const std::string& path = maybeFile->get().getFilePath();
}

// Throws if the cast fails
auto typed = store.getAs<MemoryStore>();
auto& mem = typed.getMutable();
auto buffers = mem.drain();
```

## Concrete Stores

### MemoryStore

Holds TupleBuffers in a `std::deque`. Intended as a fast write buffer that is periodically drained by a
transformation.

```
Header:  nes-store-manager/include/MemoryStore.hpp
Source:  nes-store-manager/src/MemoryStore.cpp
```

| Config field | Type | Default | Description |
|---|---|---|---|
| `maxBufferSize` | `size_t` | 64 MB | Flush threshold for hierarchical use |

Key methods beyond the `StoreConcept` interface:

- `isFull()` -- returns true when `size() >= config.maxBufferSize`
- `drain()` -- returns all stored TupleBuffers and resets internal state (used by transformations)

Thread-safe via `std::shared_mutex`.

### FileStore

Wraps `BinaryStoreWriter` (write path) and `ReplayStoreReader` (read path). Data is stored in a binary
format with a self-describing header containing the schema.

```
Header:  nes-store-manager/include/FileStore.hpp
Source:  nes-store-manager/src/FileStore.cpp
```

| Config field | Type | Description |
|---|---|---|
| `storeName` | `std::string` | Logical store name |
| `filePath` | `std::string` | Path to the binary file on disk |
| `schemaText` | `std::string` | Serialized schema text embedded in the file header |

The reader is created lazily on the first `read()` call. Before reading, the writer is closed.
Non-copyable and non-movable due to file descriptor ownership.

Additional methods:

- `getFilePath()` -- returns the file path (used by `StoreRegistry` for backward compatibility)
- `removeFile()` -- deletes the binary file from disk

#### Binary file format

Defined in `nes-store-manager/include/ReplayStoreFormat.hpp`:

```
[8 bytes magic: "NESSTORE"]
[4 bytes version]
[1 byte endianness]
[4 bytes flags]
[8 bytes fingerprint]
[N bytes schema text (null-terminated)]
[data rows...]
```

Each data row is packed with no padding. The row width is the sum of field sizes from the schema.

### HierarchicalStore

Chains multiple stores into a tiered hierarchy with automatic flush.

```
Header:  nes-store-manager/include/HierarchicalStore.hpp
Source:  nes-store-manager/src/HierarchicalStore.cpp
```

Configured with a vector of `StoreLevel` entries:

```cpp
struct FlushPolicy {
    enum class Type { SIZE_THRESHOLD };
    Type type = Type::SIZE_THRESHOLD;
    size_t sizeThreshold = 64 * 1024 * 1024;  // 64 MB
};

struct StoreLevel {
    Store store;
    FlushPolicy policy;
};
```

Behavior:

- **write**: writes to `levels[0]`, then checks if it should flush
- **flush**: cascades top-down through all levels using registered transformations
- **read**: reads from the last level (the persistent store)
- **maybeFlush(i)**: if `levels[i].store.size() >= policy.sizeThreshold`, looks up the transformation
  `"{sourceTypeName}_to_{destTypeName}"` from the `StoreTransformationRegistry`, executes it, and cascades

Thread-safe via `std::shared_mutex`.

## Store Transformations

A transformation moves data between two stores. Defined as a C++20 concept in
`nes-store-manager/include/StoreTransformation.hpp`:

```cpp
template <typename T>
concept StoreTransformationConcept = requires(T& t, Store& source, Store& dest) {
    { t.execute(source, dest) };
    { t.name() } noexcept -> std::convertible_to<std::string_view>;
};
```

Type erasure follows the same pattern as `Store`: `StoreTransformationModel<T>` wraps a concrete type,
and `StoreTransformation` (alias for `TypedStoreTransformation<>`) is the public handle.

### MemoryToFileTransformation

The built-in transformation for flushing a MemoryStore to a FileStore.

```
Header:  nes-store-manager/include/MemoryToFileTransformation.hpp
Source:  nes-store-manager/src/MemoryToFileTransformation.cpp
```

Implementation:

1. Downcasts the source `Store` to `MemoryStore` via `getAs<MemoryStore>()`
2. Calls `drain()` to extract all TupleBuffers
3. Writes each buffer to the destination store via `dest.write(buffer, schema)`

## Registries

### StoreTypeRegistry

Factory registry that maps a store type name to a factory function.
Uses `BaseRegistry` from `nes-common/include/Util/Registry.hpp`.

```
Header:  nes-store-manager/registry/include/StoreTypeRegistry.hpp
```

```cpp
struct StoreTypeRegistryArguments {
    Schema schema;
    std::unordered_map<std::string, std::string> config;
};

using StoreTypeRegistryReturnType = StoreManager::Store;
```

Built-in entries:

| Key | Factory |
|---|---|
| `"MemoryStore"` | Creates a `MemoryStore` with the provided schema |
| `"FileStore"` | Creates a `FileStore` with `file_path`, `store_name`, `schema_text` from config |

### StoreTransformationRegistry

Factory registry that maps a transformation key to a factory function.

```
Header:  nes-store-manager/registry/include/StoreTransformationRegistry.hpp
```

```cpp
struct StoreTransformationRegistryArguments {};

using StoreTransformationRegistryReturnType = StoreManager::StoreTransformation;
```

Built-in entries:

| Key | Factory |
|---|---|
| `"MemoryStore_to_FileStore"` | Creates a `MemoryToFileTransformation` |

The key format is `"{SourceTypeName}_to_{DestTypeName}"`, matching the `typeName()` of the two stores.
`HierarchicalStore::flushLevel` constructs this key automatically when looking up transformations.

### StoreRegistry (instance manager)

A singleton that maps store names to live `Store` instances. This is separate from the factory registries --
it manages named store instances rather than type factories.

```
Header:  nes-store-manager/include/StoreRegistry.hpp
Source:  nes-store-manager/src/StoreRegistry.cpp
```

Key methods:

| Method | Description |
|---|---|
| `registerStore(name, store)` | Register a pre-built store under a name |
| `registerDefaultStore(name, schema, schemaText)` | Create and register a `HierarchicalStore(MemoryStore -> FileStore)` |
| `getStore(name)` | Look up a store by name |
| `getFilePath(name)` | Get the file path (traverses hierarchy to find the FileStore) |
| `clearAndDeleteFiles()` | Close all stores and delete binary files from disk |

File paths are generated as `{WORKING_DIR}/replay_{storeName}_{YYYYMMDD_HHMMSS_mmm}.bin`.

## Extending the System

### Adding a new store type

1. Create a class satisfying `StoreConcept`:

```cpp
// nes-store-manager/include/MyStore.hpp
class MyStore {
public:
    explicit MyStore(Schema schema);

    void open();
    void close();
    void flush();
    void write(TupleBuffer buffer, const Schema& schema);
    uint64_t read(TupleBuffer& buffer, const Schema& schema);
    bool hasMore() const;
    Schema getSchema() const;
    uint64_t size() const;
    std::string_view typeName() const noexcept { return "MyStore"; }

private:
    Schema schema;
};
```

2. Add a registry factory function at the bottom of the `.cpp` file:

```cpp
// nes-store-manager/src/MyStore.cpp
#include <StoreTypeRegistry.hpp>

namespace NES {
StoreTypeRegistryReturnType
StoreTypeGeneratedRegistrar::RegisterMyStoreStoreType(StoreTypeRegistryArguments args) {
    return StoreManager::makeStore<StoreManager::MyStore>(std::move(args.schema));
}
}
```

3. Register the plugin in CMake:

```cmake
# nes-store-manager/src/CMakeLists.txt
add_source_files(nes-store-manager MyStore.cpp)
add_plugin(MyStore StoreType nes-store-manager MyStore.cpp)
```

The CMake plugin system generates the registrar code that calls `addEntry("MyStore", ...)` on the
`StoreTypeRegistry` at startup.

### Adding a new transformation

1. Create a class satisfying `StoreTransformationConcept`:

```cpp
// nes-store-manager/include/MyTransformation.hpp
class MyTransformation {
public:
    void execute(Store& source, Store& dest);
    std::string_view name() const noexcept { return "MyTransformation"; }
};
```

2. Add a registry factory function:

```cpp
// nes-store-manager/src/MyTransformation.cpp
#include <StoreTransformationRegistry.hpp>

namespace NES {
StoreTransformationRegistryReturnType
StoreTransformationGeneratedRegistrar::RegisterMyStore_to_FileStoreStoreTransformation(
    StoreTransformationRegistryArguments) {
    return StoreManager::MyTransformation{};
}
}
```

3. Register the plugin in CMake:

```cmake
# nes-store-manager/src/CMakeLists.txt
add_source_files(nes-store-manager MyTransformation.cpp)
add_plugin(MyStore_to_FileStore StoreTransformation nes-store-manager MyTransformation.cpp)
```

The plugin name (`MyStore_to_FileStore`) must match the key that `HierarchicalStore::flushLevel` constructs
from the `typeName()` values of the source and destination stores.

## Pipeline Integration

### Write path

```
CSV Source
    |
    v
ScanPhysicalOperator          -- iterates raw input buffer, calls execute() per record
    |
    v
ReplayStorePhysicalOperator   -- accumulates parsed records into staging TupleBuffer
    |
    v
EmitPhysicalOperator           -- emits buffer downstream to sink
```

`ReplayStorePhysicalOperator` follows the same accumulation pattern as `EmitPhysicalOperator`:

- **open()**: allocates a staging TupleBuffer via `ctx.allocateBuffer()`, stores it in an `OperatorState`
- **execute()**: writes each parsed record into the staging buffer via `bufferRef->writeRecord()`;
  flushes and reallocates when full
- **close()**: writes the remaining staging buffer to the store via `ReplayStoreOperatorHandler::writeBuffer()`
- Records are forwarded to the child operator unchanged (pass-through)

The `TupleBufferRef` used by the operator is created in `LowerToPhysicalReplayStore` via
`LowerSchemaProvider::lowerSchema(bufferSize, inputSchema, memoryLayoutType)`, which computes field offsets
from the resolved schema.

### Read path

```
ReplaySource
    |
    v
ScanPhysicalOperator
    |
    v
EmitPhysicalOperator
    |
    v
Sink
```

`ReplaySource` looks up the store by name from the `StoreRegistry` and calls `store.read(tupleBuffer, schema)`
to fill TupleBuffers. For a `HierarchicalStore`, reads go to the last level (FileStore), which uses
`ReplayStoreReader` to read the binary file.

### Lowering

`LowerToPhysicalReplayStore` (in `nes-query-optimizer`) handles the translation from logical to physical:

1. Retrieves the `Store` from `StoreRegistry::getStore(storeName)`
2. Creates a `ReplayStoreOperatorHandler` with the resolved input schema and the store
3. Creates a `TupleBufferRef` via `LowerSchemaProvider::lowerSchema()` for record accumulation
4. Wraps everything in a `ReplayStorePhysicalOperator`

## Schema Lifecycle

Stores may be registered early in the query lifecycle (before type inference), at which point the schema
can have `UNDEFINED` field types with zero byte sizes. The system handles this through lazy schema resolution:

- `MemoryStore::write()` and `FileStore::write()` update their internal schema from the write-time schema
  when the current schema has unresolved types (`getSizeOfSchemaInBytes() == 0`)
- The write-time schema comes from the `ReplayStoreOperatorHandler`, which receives the fully resolved
  schema from the physical lowering phase

This ensures the correct schema propagates through the hierarchy even when stores are registered before
type inference completes.

## File Layout

```
nes-store-manager/
  include/
    Store.hpp                      -- StoreConcept, type erasure, makeStore
    MemoryStore.hpp                -- In-memory TupleBuffer store
    FileStore.hpp                  -- Binary file store
    HierarchicalStore.hpp          -- Composable tiered store
    StoreTransformation.hpp        -- Transformation concept and type erasure
    MemoryToFileTransformation.hpp -- Built-in MemoryStore -> FileStore transformation
    StoreRegistry.hpp              -- Named store instance manager (singleton)
    BinaryStoreWriter.hpp          -- Low-level binary file writer (pwrite-based)
    ReplayStoreReader.hpp          -- Low-level binary file reader
    ReplayStoreFormat.hpp          -- Binary file header format
  registry/
    include/
      StoreTypeRegistry.hpp        -- Factory registry for store types
      StoreTransformationRegistry.hpp -- Factory registry for transformations
  src/
    CMakeLists.txt                 -- Source files and plugin registrations

nes-physical-operators/
  include/
    ReplayStorePhysicalOperator.hpp -- JIT-compiled store write operator
    ReplayStoreOperatorHandler.hpp  -- Runtime handler bridging operator to Store
  src/
    ReplayStorePhysicalOperator.cpp
    ReplayStoreOperatorHandler.cpp

nes-sources/
  include/Sources/
    ReplaySource.hpp               -- Source that reads from a Store
  src/
    ReplaySource.cpp

nes-query-optimizer/
  private/RewriteRules/LowerToPhysical/
    LowerToPhysicalReplayStore.hpp -- Logical-to-physical lowering rule
  src/RewriteRules/LowerToPhysical/
    LowerToPhysicalReplayStore.cpp
```