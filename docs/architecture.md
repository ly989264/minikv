# MiniKV Architecture Audit

## Background And Scope

This document captures the current implementation state of `minikv/` in this
repository. It summarizes how the code is structured today, how requests flow
through the system, what storage model is in use, and which architectural risks
are already visible from the implementation.

Related documents:

- [README.md](../README.md)
- [getting-started.md](./getting-started.md)
- [build.md](./build.md)
- [layers/runtime.md](./layers/runtime.md)
- [layers/network.md](./layers/network.md)
- [layers/command.md](./layers/command.md)
- [layers/worker.md](./layers/worker.md)
- [layers/codec.md](./layers/codec.md)

## Module Layering

The current implementation is organized as:

`main -> NetworkServer -> RESP parser / runtime CommandRegistry -> Scheduler -> Worker -> builtin module command -> HashModule -> HashIndexingBridge / HashObserver -> ModuleSnapshot / ModuleIterator / ModuleWriteBatch -> StorageEngine -> RocksDB`

The responsibilities are:

- `src/app/main.cc`: parses process flags, opens `MiniKV`, starts
  `NetworkServer`
- `src/runtime/minikv.h` and `src/runtime/minikv.cc`: runtime owner for
  storage, shared scheduler state, and builtin module loading
- `src/network/network_server.h` and `src/network/`: TCP accept loop,
  per-I/O-thread connection management, RESP parsing, response encoding, and
  response reordering
- `src/runtime/module/`: builtin module SPI, lifecycle, shared export
  registry, module service facades, module keyspace helpers, and background
  executor
- `src/execution/command/`: converts parsed RESP parts into registered `Cmd`
  objects
- `src/execution/registry/`, `src/execution/reply/`,
  `src/execution/scheduler/`, and `src/execution/worker/`: command registry,
  reply helpers, scheduler, keyed workers, and execution coordination
- `src/core/`: protocol-level builtin commands and current core key lifecycle
  services
- `src/types/hash/`: hash builtin module, exported indexing bridge, observer
  interface, and command registrations
- `src/storage/engine/` and `src/storage/encoding/`: RocksDB integration,
  snapshots, write batching, and key/metadata encoding rules

Public behavior is intentionally narrow today:

- supported commands: `PING`, `HSET`, `HGETALL`, `HDEL`
- supported data type: hash only
- supported deployment shape: single process, POSIX server path
- supported module shape: builtin modules only, with no external ABI
- search wiring is still limited to prep infrastructure only; there is no
  builtin `SearchModule` and no `FT.*` command family

## Request Lifecycle

The request path is split into network I/O, keyed execution, and typed storage
semantics:

1. `NetworkServer::AcceptLoop()` accepts client sockets and assigns them to an
   I/O thread in round-robin fashion.
2. Each I/O thread polls its own connections, reads bytes, appends into the
   per-connection read buffer, and uses `RespParser` to extract one or more
   RESP arrays.
3. Parsed parts are converted into a concrete `Cmd` through the runtime
   `CommandRegistry` owned by `ModuleManager`.
4. `NetworkServer` assigns a per-connection request sequence and submits the
   task into the shared `Scheduler`.
5. `Scheduler` picks a worker queue with round-robin plus ring probing.
6. The worker thread acquires the striped key lock for `cmd->RouteKey()` and
   executes `Cmd::Execute()`.
7. Hash commands use `HashModule`, which reads through `ModuleSnapshot` and
   stages writes through `ModuleWriteBatch`.
8. Before commit, `HashModule` synchronously notifies any registered
   `HashObserver` instances through the exported `HashIndexingBridge`.
9. `StorageEngine` translates the committed write batch onto RocksDB column
   families.
10. The completion callback pushes the `CommandResponse` back into the owning
   I/O thread's completed queue.
11. The I/O thread reorders completions by request sequence, encodes the
    response as RESP, and appends it to the connection's write buffer.
12. The I/O thread flushes buffered responses back to the client socket.

## Storage Model

`StorageEngine` opens RocksDB with four column families:

- `default`
- `meta`
- `hash`
- `module`

The data layout is driven by `KeyCodec`:

- meta key: `m| + key_length + user_key`
- hash data key prefix: `h| + key_length + user_key + version`
- hash data key: `hash_prefix + field`

Module-private storage uses `ModuleKeyspace` on top of the shared `module`
column family:

- keyspace identity is `module_name + local_keyspace_name`
- the encoded key prefix is length-prefixed binary data rather than a dedicated
  search-specific column family
- modules read and write that state through keyspace-aware `ModuleSnapshot`,
  `ModuleIterator`, and `ModuleWriteBatch` helpers instead of hard-coding
  global `StorageColumnFamily` choices

Current hash behavior is intentionally unchanged. `HashModule` still stores its
user-visible data in `meta` and `hash`, while the `module` column family exists
for module-private infrastructure such as future search metadata.

The metadata payload currently contains:

- `type`
- `encoding`
- `version`
- `size`
- `expire_at_ms`

## Concurrency Model

The concurrency design is based on keyed serialization:

- connections are owned by I/O threads
- command execution is owned by worker threads
- requests are load-balanced across worker queues
- requests for the same key serialize on one striped mutex derived from
  `RouteKey()`

Benefits of this model:

- same-key updates avoid explicit coordination inside the hash module
- multi-column-family reads no longer depend on ad hoc read ordering
- network progress is decoupled from RocksDB calls
- different keys can execute in parallel across workers
- same-connection response order is preserved by the network reorder buffer

Module-owned asynchronous maintenance now runs on one minimal background
executor thread exposed through `ModuleBackgroundService`. This is intentionally
separate from request execution and is not a replacement for the keyed worker
model.

## Architecture Audit

### P1: Core Results Are Still RESP-Shaped

`CommandResponse` is still close to RESP instead of being a transport-neutral
domain result.

Impact:

- the command layer is still biased toward the current RESP transport
- a future non-RESP transport would likely need a second result-shaping layer

### P2: Module SPI Is Still Builtin-Only

`minikv` now has a builtin module SPI, but it is intentionally limited to
source-compiled modules loaded by `ModuleManager`.

Impact:

- command registration is now unified under one runtime registry
- typed module exports now support narrow cross-module collaboration without
  leaking private module pointers
- there is still no external ABI, dynamic loading path, or third-party module
  contract

### P2: Search Prep Exists But Search Is Still Not Wired In

The current code now exposes a hash-only indexing bridge plus synchronous hash
observer callbacks, but the actual search feature surface is still absent.

Impact:

- hash writes can now carry observer-side index updates in the same write batch
- module-private search state can now live in one shared `module` column family
  behind `ModuleKeyspace`
- future search modules can iterate that state through `ModuleIterator` and use
  one minimal `BackgroundExecutor` without learning the kernel's column-family
  layout
- there is still no `SearchModule`
- there are still no `FT.CREATE`, `FT.SEARCH`, or other `FT.*` commands

### P2: Metadata Schema Still Signals Future Features That Are Not Active

The metadata contains `version` and `expire_at_ms`, but the current hash
implementation still uses `version = 1` and does not enforce TTL.

### P3: Module Storage Boundary Still Has A Compatibility Escape Hatch

PR-B added `ModuleKeyspace` plus keyspace-aware `ModuleSnapshot`,
`ModuleIterator`, and `ModuleWriteBatch` helpers, but the module service layer
still keeps raw `StorageColumnFamily` read/write entrypoints for the existing
hash path.

Impact:

- new module-private state should use `ModuleKeyspace` rather than raw
  column-family access
- `HashModule` and current hash observers remain compatible without changing
  their data layout
- the kernel-to-module boundary is not fully narrowed yet, so a future cleanup
  pass should retire the raw-CF helpers once remaining callers migrate

### P3: Module Background Execution Is Intentionally Minimal

PR-B introduced one shared `BackgroundExecutor` behind
`ModuleBackgroundService`, but it is deliberately a small single-thread FIFO
facility rather than a general-purpose thread pool.

Impact:

- this is enough for lightweight module maintenance and search-prep plumbing
- there is no cancellation, prioritization, per-module quota, or structured
  background task error reporting
- future work should treat this as a narrow module helper, not as a second
  request scheduler

### P3: Network Operability Is Still Minimal

The network layer still uses a hand-rolled `poll` loop plus wakeup pipes.
That is acceptable for a compact prototype, but the operational surface is
still thin:

- no built-in metrics endpoint
- wakeup writes are ignored
- no structured shutdown reporting beyond thread termination
