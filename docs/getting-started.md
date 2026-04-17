# MiniKV Getting Started

This document is the best entrypoint if you have never read `minikv/` before.
It follows the current implementation rather than a future target design.

## Summary

`minikv` is a small single-process Redis-like prototype built on top of
RocksDB.

The main request path is now:

`main -> Server -> RESP parser -> CmdFactory -> MiniKV -> Scheduler -> Worker -> CommandContext -> HashModule -> Snapshot / WriteContext -> StorageEngine -> RocksDB`

Current scope is intentionally narrow:

- supported commands: `PING`, `HSET`, `HGETALL`, `HDEL`
- supported data type: hash only
- deployment shape: one POSIX process exposing a TCP server

The most important implementation fact is that two consistency concerns are now
separated explicitly:

- same-key write serialization comes from `Scheduler` plus `KeyLockTable`
- multi-column-family consistent reads come from `Snapshot`

## Architecture And Class Map

### `src/main.cc`

Process entrypoint.

- parses command-line flags into `Config`
- opens `MiniKV`
- creates `Server`
- runs the server until shutdown

Read this first to understand the top-level object graph.

### `include/minikv/minikv.h` and `src/minikv.cc`

Public facade.

`MiniKV` owns an internal `Impl`, and `Impl` owns:

- `StorageEngine`
- `NoopMutationHook`
- `HashModule`
- `CommandContext`
- `Scheduler`

Important behavior:

- `MiniKV::Open()` opens the RocksDB-backed storage engine before publishing the
  facade
- `Execute()` runs through `Scheduler::ExecuteInline()`
- `Submit()` runs through the same shared `Scheduler`
- typed helpers `HSet()`, `HGetAll()`, and `HDel()` remain thin wrappers around
  the unified command path

### `src/server/server.h` and `src/server/server.cc`

Network and connection-management layer.

`Server` is responsible for:

- creating the listening socket
- accepting connections
- assigning each connection to one I/O thread
- reading and buffering bytes
- parsing RESP requests
- writing encoded RESP responses
- idle timeout and orderly shutdown

Important current boundary:

- `Server` no longer owns a separate worker runtime
- it submits parsed commands back into `MiniKV::Submit()`
- response ordering remains connection-local through request sequence numbers

Key internal types:

- `Connection`: per-client socket state, including read/write buffers and
  request / response sequence tracking
- `CompletedResponse`: worker result routed back to one connection with the
  original request sequence
- `IOThreadState`: per-I/O-thread ownership bundle for connections, completion
  queue, wakeup pipe, and thread object

### `src/command/cmd.*`, `cmd_create.*`, `cmd_factory.*`, `t_*.*`

Turns parsed RESP parts or compatibility `CommandRequest` values into concrete
`Cmd` objects.

This layer:

- registers each supported command once
- separates registration lookup from command creation
- maps command names and `CommandType` values onto concrete `Cmd` classes
- performs command-specific `DoInitial()` validation and parameter extraction
- executes against `CommandContext`

Current file split:

- `cmd_factory.*`: registration and lookup only
- `cmd_create.*`: build a `Cmd` from RESP parts or `CommandRequest`
- `t_kv.*`: `PING`
- `t_hash.*`: `HSET`, `HGETALL`, `HDEL`

Current execution split:

- `PING` stays protocol-only
- hash commands delegate to `HashModule`

### `src/kernel/scheduler.*` and `src/worker/*`

Concurrency core of the current design.

`Scheduler` owns:

- one worker thread per configured worker
- one bounded MPSC queue per worker
- a shared `KeyLockTable`
- round-robin plus probe admission control
- queue depth / inflight / rejection metrics
- both async submission and inline execution

`Worker` owns:

- one consumer thread
- one bounded queue
- exception isolation around command execution

Correctness rule:

- acquire the striped key lock for `cmd->RouteKey()`
- execute `Cmd::Execute(context)`
- release the key lock after completion

This means same-key correctness no longer depends on any duplicated runtime
structure. Both embedded and server paths use the same scheduler.

### `src/kernel/storage_engine.*`

RocksDB integration layer.

`StorageEngine` is responsible for:

- opening RocksDB
- ensuring required column families exist
- exposing primitive `Get`, `Put`, `Delete`, `Write`
- creating consistent `Snapshot` objects
- providing iterator construction only to kernel helpers such as `Snapshot`

Current column families:

- `default`
- `meta`
- `hash`

`src/engine/db_engine.h` is now only a compatibility alias to `StorageEngine`.

### `src/kernel/snapshot.*`

Read-consistency helper.

`Snapshot` is responsible for:

- pinning one RocksDB snapshot
- serving `Get()` across one column family
- serving `ScanPrefix()` across one column family
- giving `HashModule` one consistent read view across `meta` and `hash`

Modules should not reach directly for raw RocksDB iterators.

### `src/kernel/write_context.*`

Write batching helper.

`WriteContext` is responsible for:

- owning one `rocksdb::WriteBatch`
- collecting `Put` and `Delete` operations for one logical mutation
- committing that batch once

It is the write-side companion to `Snapshot`.

### `src/kernel/mutation_hook.h`

Future extension hook.

`MutationHook` is responsible for:

- defining the hook interface for logical hash mutations
- receiving both the logical mutation description and the active
  `WriteContext`

Current behavior:

- `NoopMutationHook` is the active implementation
- no Search or `FT.*` logic is attached yet

### `src/types/hash/hash_module.*`

Hash semantics layer.

`HashModule` is responsible for:

- `PutField()`
- `ReadAll()`
- `DeleteFields()`

Current design rules:

- reads always use `Snapshot`
- writes always use `WriteContext`
- hook call sites are present for future index or side-effect updates
- the module does not expose old `DBEngine::HSet/HGetAll/HDel` style APIs

### `src/engine/key_codec.*`

Storage-key encoding layer.

It defines:

- `ValueType`
- `ValueEncoding`
- `KeyMetadata`
- binary encoding helpers for metadata and hash keys

Current logical encodings:

- meta key: `m| + key_length + user_key`
- hash prefix: `h| + key_length + user_key + version`
- hash data key: `hash_prefix + field`

This encoding still makes `HGETALL` possible through a prefix scan, but the
scan now runs through `Snapshot`.

## Thread Model

The runtime currently has three kinds of threads:

- one accept thread
- `io_threads` I/O threads
- `worker_threads` worker threads owned by the shared scheduler

### When threads are created

- `MiniKV::Open()` constructs the shared scheduler and its worker threads
- `Server::Start()` creates all I/O threads and then the accept thread

There is no longer one scheduler inside `MiniKV` and another inside `Server`.

### Accept thread

The accept thread runs `Server::AcceptLoop()`.

Its job is only:

- accept a client socket
- make it nonblocking
- assign it to one I/O thread in round-robin order

It does not parse requests and does not touch RocksDB.

### I/O threads

Each I/O thread runs `Server::RunIOThread()`.

Each I/O thread exclusively owns:

- its `connections` vector
- each connection's socket fd
- per-connection read buffer
- per-connection write buffer
- per-connection pending request count
- per-connection request / response sequence numbers
- per-connection last-activity timestamp

I/O threads use:

- `poll()` for socket readiness
- a wakeup `pipe()` for cross-thread notification

The wakeup pipe is used when:

- a new accepted connection is assigned to that I/O thread
- a completed response is pushed back to that I/O thread
- shutdown is requested

### Worker threads

Each worker thread runs `Worker::Run()`.

Its loop is:

1. wait on `condition_variable`
2. pop one task from its local queue
3. acquire the striped key lock for `RouteKey()`
4. execute the command with `Cmd::Execute(context)`
5. invoke the completion callback

## Read This Next

After this file, the best next documents are:

1. [architecture.md](./architecture.md)
2. [layers/facade.md](./layers/facade.md)
3. [layers/server.md](./layers/server.md)
4. [layers/command.md](./layers/command.md)
5. [layers/worker.md](./layers/worker.md)
6. [layers/engine.md](./layers/engine.md)
