# Error Log

Exceptions of a worker can be queried like any other stream (discussion #153).

```
Exception constructor ──> ErrorLog (one per worker) <── ErrorSource (one per query, TYPE Error)
```

## Enabling

Off by default. Per worker:

| Option | Default | Meaning |
|---|---|---|
| `enable_error_source` | `false` | Pass the exceptions of this worker to its Error sources. |

- CLI: `nes-single-node-worker -- --enable_error_source=true`
- YAML: `enable_error_source: true`
- Systest file: `GLOBALCONFIGURATION enable_error_source: true` (applies to the queries below the line). The systest stops
  a query on `ERROR_SOURCE` when its expected rows have arrived, because the source never ends. Write the sink
  into the query (`INTO File('JSON' AS "SINK".OUTPUT_FORMAT)`): the systest replaces the options of a declared sink
  (`CREATE SINK ... SET(...)`) by its CSV defaults.

## What is recorded

`Exception(std::string, uint64_t)` records every NES exception it constructs, i.e. every factory of
`ExceptionDefinitions.inc`, on a thread of a worker with `enable_error_source=true`. This includes exceptions used as error values
(e.g. `QueryNotFound` from status polls). Filter in SQL.

Not recorded:
- foreign `std::exception`s that `wrapExternalException` turns into `UnknownException` (other constructor);
- context a catch site appends to the message later (e.g. `" in Query {}"`, `QueryEngine.cpp`); `query_id` has the query;
- exceptions on threads that do not belong to the worker.

| Field | Source |
|---|---|
| `error_code` | `Exception::code()` |
| `message` | factory message at construction, trailing newline removed |
| `query_id`, `distributed_query_id`, `pipeline_id` | parsed from the `LogContext` (spdlog MDC) of the throwing thread: `Task` = `<queryId>-<pipelineId>` in pipeline tasks, `<queryId>` in query tasks, `queryId` during query start (`TaskContext.hpp` formats and parses it); NULL elsewhere, also on source threads |
| `node` | the Error source's `HOST`, i.e. the worker as SQL names it |
| `ts` | `Exception::when()`, unix ms |

## ErrorLog (`nes-common/include/ErrorLog.hpp`)

- Created by `SingleNodeWorker` before the engine, registered under the worker's `host`. Threads find it through
  `Thread::getThisWorkerNodeId()`, which every worker thread carries; `Thread` itself is unchanged.
- Disabled cost per exception: one relaxed atomic load (`ErrorLog::anyRegistered()`).
- No retention: the log keeps no history. Each Error source subscribes when its query starts and gets every error
  recorded after that, in its own queue; an error recorded while no source is subscribed is gone. To keep errors, run
  an error query that writes them into a sink.
- A subscription's queue is unbounded: it holds what the source has not read yet, e.g. while the sink applies
  backpressure.

## `ERROR_SOURCE`

Every `SourceCatalog` has the logical source `ERROR_SOURCE` with all fields below, from its construction on. A worker
gets a physical source of it when it enters the catalogs: `CREATE WORKER` (removed by `DROP WORKER`), the embedded worker
of the REPL, and the workers of the systest cluster. So a query names the source and brings a sink:

```sql
SELECT error_code, message, node FROM ERROR_SOURCE INTO File('JSON' AS "SINK".OUTPUT_FORMAT);
```

The frontend does not know a worker's `enable_error_source` in every case (the systest sets it per file, on the worker
only), so every worker gets a physical source. A query fails with `CannotOpenSource` if any worker lacks the option,
which means that a partly enabled cluster cannot query `ERROR_SOURCE`. Such a cluster declares its own source of
`TYPE Error` on the enabled workers. `ERROR_SOURCE` has no `MAX_RECORDS`: it ends only when its query is stopped.

## ErrorSource (`TYPE Error`)

| Parameter | Default | Meaning |
|---|---|---|
| `MAX_RECORDS` | `0` | End the stream after this many errors (0 = unbounded); counts before `WHERE` |

The source writes NATIVE row-layout tuples (like `NetworkSource`), so the physical source needs
`'NATIVE' AS INPUT_FORMATTER."TYPE"`; the sink chooses the textual format (`OUTPUT_FORMAT` CSV or JSON) and its
formatter does the escaping. The logical source declares any subset of these fields, in any order, matched by name:

| Field | Type |
|---|---|
| `error_code` | `UINT64` |
| `message` | `VARSIZED` (child buffer) |
| `query_id` | `VARSIZED`, nullable: local query id (UUID) |
| `distributed_query_id` | `VARSIZED`, nullable |
| `pipeline_id` | `UINT64`, nullable; pipeline ids are counted per worker process, not per query |
| `node` | `VARSIZED` |
| `ts` | `UINT64` |

`query_id`, `distributed_query_id` and `pipeline_id` must be declared nullable (NULL outside a query task); the other
fields may be. A record never exceeds a buffer: the VARSIZED fields go into child
buffers (`TupleBufferRef::writeVarSized`, unpooled when larger than a buffer).

Constructing the source, which happens while its query is started, fails with `CannotOpenSource` (4002) on a worker
without `enable_error_source=true`, for an input format other than NATIVE, or for an unknown field name or type.

```sql
-- Only needed for a subset of the fields, MAX_RECORDS or a subset of the workers; otherwise read ERROR_SOURCE.
CREATE LOGICAL SOURCE errors(error_code UINT64 NOT NULL, message VARSIZED NOT NULL, node VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR errors TYPE Error SET('NATIVE' AS INPUT_FORMATTER."TYPE", 'worker-1:8080' AS "SOURCE"."HOST", 1000 AS "SOURCE".MAX_RECORDS);

SELECT error_code, message, node FROM errors WHERE error_code >= 3000 INTO File('JSON' AS "SINK".OUTPUT_FORMAT);
```

Running example: `nes-systests/sources/ErrorSource.test`.

```shell
cmake --build cmake-build-debug -j --target systest
cmake-build-debug/nes-systests/systest/systest -t nes-systests/sources/ErrorSource.test
```

## Open points

- Source threads set no `LogContext`, so errors raised in sources have no query or pipeline.
- `MAX_RECORDS` counts before `WHERE`, so noise on the worker (other exceptions) can end a declared source early.
- Errors of an error query itself reach its own source.
- The systest can only check errors of queries that start after the error query, so `ErrorSource.test` needs several
  concurrent queries and cannot pass in a sequential run (`--sequential`, `-b`).
