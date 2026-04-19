# MiniKV Getting Started

This document is the best entrypoint if you have never read `minikv/` before.
It follows the current implementation rather than a future target design.

## Summary

`minikv` is a small single-process Redis-like prototype built on RocksDB.

The main request path is:

`main -> MiniKV::Open -> ModuleManager -> NetworkServer -> RespParser -> CreateCmd -> Scheduler -> Worker -> builtin module command -> ModuleSnapshot / ModuleWriteBatch -> StorageEngine -> RocksDB`

Current user-visible scope:

- supported commands:
  `PING`, `TYPE`, `EXISTS`, `DEL`, `EXPIRE`, `TTL`, `PTTL`, `PERSIST`,
  `SET`, `GET`, `STRLEN`, `GETBIT`, `SETBIT`, `BITCOUNT`,
  `HSET`, `HGETALL`, `HDEL`,
  `LPUSH`, `LPOP`, `LRANGE`, `RPUSH`, `RPOP`, `LREM`, `LTRIM`, `LLEN`,
  `SADD`, `SCARD`, `SMEMBERS`, `SISMEMBER`, `SPOP`, `SRANDMEMBER`, `SREM`,
  `ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZLEXCOUNT`, `ZRANGE`,
  `ZRANGEBYLEX`, `ZRANGEBYSCORE`, `ZRANK`, `ZREM`, `ZSCORE`,
  `GEOADD`, `GEOPOS`, `GEOHASH`, `GEODIST`, `GEOSEARCH`,
  `XADD`, `XTRIM`, `XDEL`, `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`
- supported data types: string, hash, list, set, zset, stream
- deployment shape: one POSIX process exposing a TCP server
- module shape: builtin modules only

The most important implementation fact is that `minikv` is network-only.
There is no parallel function-call command surface to keep in sync.

## Architecture And Class Map

### `src/app/main.cc`

Process entrypoint.

- parses command-line flags into `Config`
- opens `MiniKV`
- creates `NetworkServer`
- runs the server until shutdown

### `src/runtime/minikv.h` and `src/runtime/minikv.cc`

Runtime owner.

`MiniKV` owns:

- `StorageEngine`
- `Scheduler`
- `ModuleManager`

Important behavior:

- `MiniKV::Open()` opens RocksDB before publishing the runtime
- builtin modules load through `ModuleManager`
- current builtin load order is `CoreModule`, `StringModule`, `BitmapModule`,
  `HashModule`, `ListModule`, `SetModule`, `ZSetModule`, `GeoModule`, then
  `StreamModule`
- current module support is builtin-only and source-level only
- `MiniKV` exists to share runtime state with `NetworkServer`

### `src/runtime/module/*`

Builtin module SPI and lifecycle management.

This layer provides:

- `Module`
- `ModuleManager`
- `ModuleServices`
- `ModuleCommandRegistry`
- `ModuleExportRegistry`
- `ModuleStorage`
- `ModuleSnapshotService`
- `ModuleBackgroundService`
- `ModuleSchedulerView`
- `ModuleNamespace`
- `ModuleKeyspace`
- `ModuleMetrics`

Notable current exports:

- `core.key_service`
- `core.whole_key_delete_registry`
- `hash.indexing_bridge`
- `zset.bridge`

### `src/network/network_server.h` and `src/network/network_server.cc`

Network and connection-management layer.

`NetworkServer` is responsible for:

- creating the listening socket
- accepting connections
- assigning each connection to one I/O thread
- reading and buffering bytes
- parsing RESP request arrays made of bulk strings
- creating `Cmd` objects from parsed parts
- submitting commands to the shared `Scheduler`
- preserving per-connection response order with request sequence numbers
- writing encoded RESP responses
- enforcing connection count, request size, and idle timeout limits
- exposing in-memory connection and parser metrics

### `src/execution/command/*` and `src/execution/registry/*`

Command creation and registry.

This layer:

- turns parsed RESP parts into initialized `Cmd` objects
- looks command names up in the shared runtime `CommandRegistry`
- performs command-specific validation
- derives a per-command lock plan: none, single-key, or multi-key
- returns a transport-facing `CommandResponse`

Builtin modules register their command families during `OnLoad()`: core,
string, hash, list, set, zset, geo, and stream.

### `src/execution/scheduler/*` and `src/execution/worker/*`

Concurrency core.

`Scheduler` owns:

- one worker thread per configured worker
- one bounded MPSC queue per worker
- a shared striped `KeyLockTable`
- round-robin plus probe-based admission control
- queue depth, inflight, and rejection metrics

Correctness rule:

- acquire the locks described by `cmd->lock_plan()`
- execute `Cmd::Execute()`
- release the locks after completion

Single-key commands such as `HSET`, `ZADD`, and `TYPE` use one route key. Multi-key
commands such as `EXISTS` and `DEL` use a canonicalized multi-key lock plan.

### `src/core/*`

Core builtin commands and key lifecycle services.

This layer provides:

- protocol-level commands such as `PING`, `TYPE`, `EXISTS`, `DEL`, `EXPIRE`,
  `TTL`, `PTTL`, and `PERSIST`
- `CoreKeyService`, which owns metadata encoding, lifecycle lookup, TTL
  calculations, and metadata writes
- `WholeKeyDeleteRegistry`, which lets type modules participate in `DEL` and
  zero-or-negative `EXPIRE`

Key lifecycle states visible in code:

- missing
- live
- expired
- tombstone

### `src/types/string/*` and `src/types/bitmap/*`

String and bitmap semantics share one underlying byte value.

- `StringModule` owns string metadata, string storage, the `string.bridge`
  export, and whole-key delete handling for `ObjectType::kString`
- `BitmapModule` registers `GETBIT`, `SETBIT`, and `BITCOUNT`
- bitmap commands read and write through `string.bridge` rather than through a
  bitmap-private keyspace
- `GET`, `SET`, `STRLEN`, `GETBIT`, `SETBIT`, and `BITCOUNT` therefore all
  operate on the same byte sequence for one string key

### `src/types/hash/*`

Hash semantics layer.

`HashModule` is responsible for:

- registering `HSET`, `HGETALL`, and `HDEL` during `OnLoad()`
- exporting `HashIndexingBridge`
- registering itself as the whole-key delete handler for hash values
- implementing `PutField()`, `ReadAll()`, `DeleteFields()`, and
  `DeleteWholeKey()`
- synchronously notifying `HashObserver` instances before commit

Current hash behavior:

- hash metadata lives in the `meta` column family
- hash fields live in the `hash` column family
- deleting the final field writes a metadata tombstone instead of removing the
  metadata row
- recreating an expired or tombstoned hash bumps its metadata version

### `src/types/zset/*`

Sorted-set semantics layer.

`ZSetModule` is responsible for:

- registering `ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZLEXCOUNT`, `ZRANGE`,
  `ZRANGEBYLEX`, `ZRANGEBYSCORE`, `ZRANK`, `ZREM`, and `ZSCORE`
- registering itself as the whole-key delete handler for zset values
- publishing `zset.bridge` so other builtin modules can observe zset writes or
  request geo-encoded zset inserts without reaching into zset-private storage
- maintaining member-to-score data plus a score-ordered secondary index in
  module-private keyspaces
- bumping metadata versions when recreating expired or tombstoned zsets

### `src/types/geo/*`

Geospatial semantics layer.

`GeoModule` is responsible for:

- registering `GEOADD`, `GEOPOS`, `GEOHASH`, `GEODIST`, and `GEOSEARCH`
- maintaining geo-only sidecar state in geo-owned module keyspaces
- reusing zset storage as the authoritative member/score source through the
  exported `zset.bridge`
- observing zset mutations so geo sidecar rows stay in sync with `ZADD`,
  `ZINCRBY`, `ZREM`, `DEL`, and zero-or-negative `EXPIRE` on geo keys

### `src/types/stream/*`

Stream semantics layer.

`StreamModule` is responsible for:

- registering `XADD`, `XTRIM`, `XDEL`, `XLEN`, `XRANGE`, `XREVRANGE`, and
  `XREAD`
- registering itself as the whole-key delete handler for stream values
- maintaining stream entries plus last-accepted-ID state in module-private
  keyspaces inside the shared `module` column family
- bumping metadata versions when recreating expired or tombstoned streams

### `src/storage/engine/*`

RocksDB integration layer.

`StorageEngine` is responsible for:

- opening RocksDB
- ensuring required column families exist
- exposing primitive `Get`, `Put`, `Delete`, and `Write`
- creating consistent `Snapshot` objects

Current column families:

- `default`
- `meta`
- `hash`
- `module`

### `src/storage/engine/snapshot.*` and `src/storage/engine/write_context.*`

Read and write helpers.

- `Snapshot` pins one RocksDB snapshot and serves consistent reads
- `WriteContext` owns one logical write batch and commits it once
- module code reaches those helpers through `ModuleSnapshot` and
  `ModuleWriteBatch`

### `src/storage/encoding/key_codec.*`

Storage-key encoding layer.

`KeyCodec` defines:

- metadata-key encoding for the `meta` column family
- hash-data-prefix encoding
- hash-data-key encoding
- prefix checks and field extraction helpers for hash scans

Important boundary:

- `KeyCodec` owns the key encodings
- `DefaultCoreKeyService` owns the metadata value encoding and decoding
- `ModuleKeyspace` encoding for the `module` column family lives in
  `src/runtime/module/module_services.cc`

## Thread Model

The runtime currently has three kinds of threads:

- one accept thread
- `io_threads` I/O threads
- `worker_threads` worker threads owned by the shared scheduler

### When threads are created

- `MiniKV::Open()` constructs the shared scheduler and its worker threads
- `ModuleManager::Initialize()` starts the shared background executor
- `NetworkServer::Start()` creates all I/O threads and then the accept thread

### I/O and worker split

- I/O threads own sockets, read buffers, write buffers, connection-local
  request sequence numbers, and response reordering
- worker threads own command execution
- same-key and multi-key serialization is enforced by `KeyLockTable`
- response order is preserved per connection by request sequence numbers even
  when different workers finish out of order

## Reading Route

1. `README.md`
2. `docs/build.md`
3. `docs/getting-started.md`
4. `docs/module-lifecycle.md`
5. `docs/module-services.md`
6. `docs/hash-module-integration.md`
7. `docs/architecture.md`
8. `docs/layers/runtime.md`
9. `docs/layers/network.md`
10. `docs/layers/command.md`
11. `docs/layers/worker.md`
12. `docs/layers/codec.md`
