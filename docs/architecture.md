# MiniKV Architecture Audit

## Background And Scope

This document captures the current implementation state of `minikv/` in this
repository. It summarizes how the code is structured today, how requests flow
through the system, what storage model is in use, and which architectural
tensions are already visible from the implementation.

Related documents:

- [README.md](../README.md)
- [getting-started.md](./getting-started.md)
- [build.md](./build.md)
- [module-lifecycle.md](./module-lifecycle.md)
- [module-services.md](./module-services.md)
- [hash-module-integration.md](./hash-module-integration.md)

## Layering Summary

The current implementation is organized as:

`main -> MiniKV::Open -> ModuleManager -> NetworkServer -> RespParser -> CommandRegistry/CreateCmd -> Scheduler -> Worker -> builtin module command -> ModuleSnapshot / ModuleWriteBatch -> StorageEngine -> RocksDB`

The main responsibilities are:

- `src/app/main.cc`: parses process flags, opens `MiniKV`, starts
  `NetworkServer`
- `src/runtime/minikv.h` and `src/runtime/minikv.cc`: runtime owner for
  storage, shared scheduler state, and builtin module loading
- `src/network/`: TCP accept loop, per-I/O-thread connection management, RESP
  parsing, response encoding, response reordering, and connection metrics
- `src/runtime/module/`: builtin module SPI, lifecycle, shared export
  registry, module service facades, module keyspace helpers, and background
  executor
- `src/execution/command/`, `src/execution/registry/`,
  `src/execution/reply/`, `src/execution/scheduler/`, and
  `src/execution/worker/`: command creation, command registry, reply tree,
  scheduler, keyed workers, and execution coordination
- `src/core/`: protocol-level builtin commands and key lifecycle services
- `src/types/string/`: string builtin module, shared string storage, and the
  exported string bridge
- `src/types/bitmap/`: bitmap builtin module layered on shared string storage
- `src/types/hash/`: hash builtin module, exported indexing bridge, observer
  interface, whole-key delete handling, and command registrations
- `src/types/list/`: list builtin module and list command handling
- `src/types/set/`: set builtin module and set command handling
- `src/types/geo/`: geo builtin module, zset bridge consumer, and geo sidecar
  command handling
- `src/types/zset/`: zset builtin module and sorted-set command handling
- `src/types/stream/`: stream builtin module and stream command handling
- `src/storage/engine/` and `src/storage/encoding/`: RocksDB integration,
  snapshots, write batching, and key encoding rules

Public behavior is intentionally narrow today:

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
- supported deployment shape: single process, POSIX server path
- supported module shape: builtin modules only, with no external ABI
- search wiring is still limited to prep infrastructure; there is no builtin
  `SearchModule` and no `FT.*` command family

## Request Lifecycle

The request path is split into network I/O, keyed execution, and typed storage
semantics:

1. `MiniKV::Open()` opens RocksDB, constructs the shared `Scheduler`, creates
   `ModuleManager`, and initializes builtin modules.
2. `NetworkServer::Start()` creates the listening socket, one accept thread,
   and one or more I/O threads.
3. `NetworkServer::AcceptLoop()` accepts client sockets and assigns them to an
   I/O thread in round-robin fashion.
4. Each I/O thread polls its own connections, reads bytes into the
   per-connection read buffer, and uses `RespParser` to extract one or more
   RESP arrays.
5. Parsed parts are converted into a concrete `Cmd` through the shared runtime
   `CommandRegistry`.
6. The I/O thread assigns a per-connection request sequence and submits the
   task into the shared `Scheduler`.
7. `Scheduler` picks a worker queue with round-robin plus ring probing.
8. The worker thread acquires the striped lock set described by the command's
   lock plan and executes `Cmd::Execute()`.
9. Core commands use `CoreKeyService` for metadata lookup, TTL handling, and
   whole-key delete dispatch. Hash commands use `HashModule`, which reads
   through `ModuleSnapshot` and stages writes through `ModuleWriteBatch`.
10. Before commit, type modules may synchronously notify same-batch observers:
    `HashModule` uses `HashIndexingBridge`, while `ZSetModule` exposes
    `zset.bridge` for `GeoModule` sidecar updates.
11. `StorageEngine` translates the committed write batch onto RocksDB column
    families.
12. The completion callback pushes the `CommandResponse` back into the owning
    I/O thread's completed queue.
13. The I/O thread reorders completions by request sequence, encodes the
    response as RESP, and appends it to the connection's write buffer.
14. The I/O thread flushes buffered responses back to the client socket.

## Module And Command Surface

Builtin module load order is fixed:

1. `CoreModule`
2. `StringModule`
3. `BitmapModule`
4. `HashModule`
5. `ListModule`
6. `SetModule`
7. `ZSetModule`
8. `GeoModule`
9. `StreamModule`

Current command ownership:

- `CoreModule`: `PING`, `TYPE`, `EXISTS`, `DEL`, `EXPIRE`, `TTL`, `PTTL`,
  `PERSIST`
- `StringModule`: `SET`, `GET`, `STRLEN`
- `BitmapModule`: `GETBIT`, `SETBIT`, `BITCOUNT`
- `HashModule`: `HSET`, `HGETALL`, `HDEL`
- `ListModule`: `LPUSH`, `LPOP`, `LRANGE`, `RPUSH`, `RPOP`, `LREM`, `LTRIM`,
  `LLEN`
- `SetModule`: `SADD`, `SCARD`, `SMEMBERS`, `SISMEMBER`, `SPOP`,
  `SRANDMEMBER`, `SREM`
- `ZSetModule`: `ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZLEXCOUNT`, `ZRANGE`,
  `ZRANGEBYLEX`, `ZRANGEBYSCORE`, `ZRANK`, `ZREM`, `ZSCORE`
- `GeoModule`: `GEOADD`, `GEOPOS`, `GEOHASH`, `GEODIST`, `GEOSEARCH`
- `StreamModule`: `XADD`, `XTRIM`, `XDEL`, `XLEN`, `XRANGE`, `XREVRANGE`,
  `XREAD`

Important current boundaries:

- commands register only during `OnLoad()`
- typed module exports may publish during `OnLoad()` and `OnStart()`
- `CoreModule` exports `CoreKeyService` and `WholeKeyDeleteRegistry`
- `HashModule` exports `HashIndexingBridge` and registers itself as the
  `WholeKeyDeleteHandler` for hash keys
- `StreamModule` registers itself as the `WholeKeyDeleteHandler` for stream
  keys and keeps its stream-private entry and state keyspaces inside the shared
  `module` column family

## Storage Model

`StorageEngine` opens RocksDB with four column families:

- `default`
- `meta`
- `hash`
- `module`

The active encodings are:

- meta key: `m| + uint32(key_length) + user_key`
- hash data prefix:
  `h| + uint32(key_length) + user_key + uint64(version)`
- hash data key: `hash_prefix + field`
- module keyspace prefix:
  `uint32(module_name_length) + module_name + uint32(local_name_length) + local_name`

Module-private storage uses `ModuleKeyspace` on top of the shared `module`
column family. Hash user data still lives in `meta` and `hash`.

The current metadata payload contains:

- `type`
- `encoding`
- `version`
- `size`
- `expire_at_ms`

Those fields are active today:

- `expire_at_ms = 0` means no TTL
- `expire_at_ms = 1` is the logical delete tombstone sentinel
- expired and tombstoned keys are treated as non-existent by user-visible
  lookup commands
- recreating an expired or tombstoned hash bumps its metadata version

## Concurrency Model

The concurrency design is based on keyed serialization:

- connections are owned by I/O threads
- command execution is owned by worker threads
- requests are load-balanced across worker queues
- lock acquisition is driven by a command lock plan rather than by the network
  layer

Current lock-plan shapes:

- `kNone`: `PING`
- `kSingle`: most single-key commands such as `TYPE`, `EXPIRE`, `TTL`, `HSET`
- `kMulti`: multi-key commands such as `EXISTS` and `DEL`

Benefits of this model:

- same-key updates avoid explicit coordination inside the hash module
- multi-column-family reads use one snapshot inside a single command
- network progress is decoupled from RocksDB calls
- different keys can execute in parallel across workers
- same-connection response order is preserved by the network reorder buffer

Module-owned asynchronous maintenance runs on one minimal background executor
thread exposed through `ModuleBackgroundService`. This is intentionally
separate from request execution and is not a replacement for the keyed worker
model.

## Architecture Audit

### P1: Results Are Still RESP-Shaped

`CommandResponse` is still close to RESP instead of being a transport-neutral
domain result.

Impact:

- the command layer is still biased toward the current RESP transport
- a future non-RESP transport would likely need a second result-shaping layer

### P1: Execution Metrics Still Depend On A Network-Layer Type

`Scheduler` and `ModuleSchedulerView` currently reuse `MetricsSnapshot`, which
is declared in `src/network/network_server.h`.

Impact:

- execution code depends on a transport-layer header for metric shape reuse
- the current layering is workable, but not fully clean

### P2: Module SPI Is Still Builtin-Only

`minikv` now has a builtin module SPI, but it is intentionally limited to
source-compiled modules loaded by `ModuleManager`.

Impact:

- command registration is unified under one runtime registry
- typed module exports support narrow cross-module collaboration without
  leaking private module pointers
- there is still no external ABI, dynamic loading path, or third-party module
  contract

### P2: Search Prep Exists But Search Is Still Not Wired In

The current code exposes a hash-only indexing bridge plus synchronous hash
observer callbacks, but the actual search feature surface is still absent.

Impact:

- hash writes can carry observer-side index updates in the same write batch
- module-private search state can live in the shared `module` column family
  behind `ModuleKeyspace`
- there is still no `SearchModule`
- there are still no `FT.CREATE`, `FT.SEARCH`, or other `FT.*` commands

### P2: Module Storage Still Has A Compatibility Escape Hatch

`ModuleKeyspace`, `ModuleIterator`, and keyspace-aware storage helpers now
exist, but `ModuleWriteBatch` and `ModuleSnapshot` still expose raw
`StorageColumnFamily` helpers for the existing hash path.

Impact:

- new module-private state should use `ModuleKeyspace` rather than raw
  column-family access
- `HashModule` and current hash observers remain compatible without changing
  their data layout
- a future cleanup pass can retire the raw-CF entrypoints after remaining
  callers migrate

### P3: Module Background Execution Is Intentionally Minimal

`BackgroundExecutor` behind `ModuleBackgroundService` is a small single-thread
FIFO facility rather than a general-purpose thread pool.

Impact:

- it is sufficient for lightweight module maintenance
- there is no cancellation, prioritization, per-module quota, or structured
  background task error reporting

### P3: Network Operability Is Still Minimal

The network layer still uses a hand-rolled `poll` loop plus wakeup pipes.
That is acceptable for a compact prototype, but the operational surface is
still thin:

- no built-in metrics endpoint
- wakeup writes are ignored
- no structured shutdown reporting beyond thread termination
