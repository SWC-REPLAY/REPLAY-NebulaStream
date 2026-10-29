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
- Systest file: `GLOBALCONFIGURATION error_log.enabled: true` (applies to the queries below the line).

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

The source writes the input format of its physical source:
- CSV (default): `error_code, message, ctx, ts` by position; column names are free. The CSV formatter keeps quotes in
  values (`FieldOffsetRawBufferIndex.cpp`, `quoted = false`), so `message` and `ctx` arrive quoted and escaped.
- JSON: keys `ERROR_CODE`, `MESSAGE`, `CTX` (object, keys uppercase, e.g. `CTX/TASK`), `TS`. Matched by name, so a
  schema may declare any subset; values round-trip exactly. Recommended.

On a worker without a log, opening the source fails with `CannotOpenSource` (4002). Non-default delimiters and formats
other than CSV/JSON are rejected.

```sql
CREATE LOGICAL SOURCE errors(error_code UINT64 NOT NULL, message VARSIZED NOT NULL, ctx VARSIZED NOT NULL, ts UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR errors TYPE Error;

CREATE LOGICAL SOURCE errorTasks(error_code UINT64 NOT NULL, "CTX/TASK" VARSIZED);
CREATE PHYSICAL SOURCE FOR errorTasks TYPE Error SET('JSON' AS INPUT_FORMATTER."TYPE", 'LATEST' AS "SOURCE".START_POSITION);

SELECT error_code, message FROM errors WHERE error_code >= 3000 INTO errorSink;
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
