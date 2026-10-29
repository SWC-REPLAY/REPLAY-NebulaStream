# Error Log

Exceptions of a worker can be queried like any other stream (discussion #153).

```
Exception constructor ──> ErrorLog (one per worker) <── ErrorSource (one per query, TYPE Error)
```

## Enabling

Off by default. Per worker:

| Option | Default | Meaning |
|---|---|---|
| `error_log.enabled` | `false` | Record the exceptions of this worker. |
| `error_log.retention_bytes` | `1048576` | Size of the shared error buffer; must be > 0. |

- CLI: `nes-single-node-worker -- --error_log.enabled=true`
- YAML: `error_log: {enabled: true}`
- Systest file: `GLOBALCONFIGURATION error_log.enabled: true` (applies to the queries below the line). Write the sink
  into the query (`INTO File('JSON' AS "SINK".OUTPUT_FORMAT)`): the systest replaces the options of a declared sink
  (`CREATE SINK ... SET(...)`) by its CSV defaults.

## What is recorded

`Exception(std::string, uint64_t)` records every NES exception it constructs, i.e. every factory of
`ExceptionDefinitions.inc`, on a thread of a worker with an enabled log. This includes exceptions used as error values
(e.g. `QueryNotFound` from status polls). Filter in SQL.

Not recorded:
- foreign `std::exception`s that `wrapExternalException` turns into `UnknownException` (other constructor);
- context a catch site appends to the message later (e.g. `" in Query {}"`, `QueryEngine.cpp`); `ctx` has the query;
- exceptions on threads that do not belong to the worker.

| Field | Source |
|---|---|
| `error_code` | `Exception::code()` |
| `message` | factory message at construction, trailing newline removed |
| `ctx` | `LogContext` (spdlog MDC) of the throwing thread: `Task` = `<queryId>-<pipelineId>` in engine tasks, `queryId` during query start; empty elsewhere (also on source threads) |
| `ts` | `Exception::when()`, unix ms |

## ErrorLog (`nes-common/include/ErrorLog.hpp`)

- Created by `SingleNodeWorker` before the engine, registered under the worker's `host`. Threads find it through
  `Thread::getThisWorkerNodeId()`, which every worker thread carries; `Thread` itself is unchanged.
- Disabled cost per exception: one relaxed atomic load (`ErrorLog::anyRegistered()`).
- One history shared by all readers, bounded by `retention_bytes` (oldest evicted first). Each record has a sequence
  number; a reader keeps its own position, so the log does not track readers. Readers that fall behind miss evicted
  records; the source counts and logs them.

## ErrorSource (`TYPE Error`)

| Parameter | Default | Meaning |
|---|---|---|
| `START_POSITION` | `EARLIEST` | `EARLIEST` replays what is retained, `LATEST` reads only new errors |
| `MAX_RECORDS` | `0` | End the stream after this many errors (0 = unbounded); counts before `WHERE` |

The source writes NATIVE row-layout tuples (like `NetworkSource`), so the physical source needs
`'NATIVE' AS INPUT_FORMATTER."TYPE"`; the sink chooses the textual format (`OUTPUT_FORMAT` CSV or JSON) and its
formatter does the escaping. The logical source declares any subset of these fields, in any order, matched by name:

| Field | Type |
|---|---|
| `error_code` | `UINT64` |
| `message` | `VARSIZED` (child buffer) |
| `ctx` | `VARSIZED`, `key:value key:value` as in the `[%&]` log pattern field |
| `ts` | `UINT64` |

Fields may be nullable; the values are never null. A record never exceeds a buffer: `message` and `ctx` go into child
buffers (`TupleBufferRef::writeVarSized`, unpooled when larger than a buffer).

Opening fails with `CannotOpenSource` (4002) on a worker without a log; constructing it fails with 4002 for an input
format other than NATIVE or an unknown field name or type.

```sql
CREATE LOGICAL SOURCE errors(error_code UINT64 NOT NULL, message VARSIZED NOT NULL, ctx VARSIZED NOT NULL, ts UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR errors TYPE Error SET('NATIVE' AS INPUT_FORMATTER."TYPE");

CREATE LOGICAL SOURCE newErrorCodes(error_code UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR newErrorCodes TYPE Error SET('NATIVE' AS INPUT_FORMATTER."TYPE", 'LATEST' AS "SOURCE".START_POSITION);

SELECT error_code, message, ctx FROM errors WHERE error_code >= 3000 INTO File('JSON' AS "SINK".OUTPUT_FORMAT);
```

Running example: `nes-systests/sources/ErrorSource.test`.

```shell
cmake --build cmake-build-debug -j --target systest
cmake-build-debug/nes-systests/systest/systest -t nes-systests/sources/ErrorSource.test
```

## Open points

- Source threads set no `LogContext`, so errors raised in sources have an empty `ctx`.
- `MAX_RECORDS` is the only end condition; noise on the worker (other exceptions) can end a bounded query early.
- Errors of an error query itself enter the log it reads.
