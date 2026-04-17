# MiniKV Getting Started

This document is the best entrypoint if you have never read `minikv/` before.
It follows the current implementation rather than a future target design.

## Summary

`minikv` is a small single-process Redis-like prototype built on RocksDB.

The main request path is:

`main -> NetworkServer -> RESP parser -> CmdFactory -> Scheduler -> Worker -> CommandServices -> HashModule -> Snapshot / WriteContext -> StorageEngine -> RocksDB`

Current scope is intentionally narrow:

- supported commands: `PING`, `HSET`, `HGETALL`, `HDEL`
- supported data type: hash only
- deployment shape: one POSIX process exposing a TCP server

The most important implementation fact is that `minikv` is now network-only.
There is no parallel function-call command surface to keep in sync.

## Architecture And Class Map

### `src/main.cc`

Process entrypoint.

- parses command-line flags into `Config`
- opens `MiniKV`
- creates `NetworkServer`
- runs the server until shutdown

### `src/minikv.h` and `src/minikv.cc`

Runtime owner.

`MiniKV` owns:

- `StorageEngine`
- `NoopMutationHook`
- `HashModule`
- `CommandServices`
- `Scheduler`

Important behavior:

- `MiniKV::Open()` opens the RocksDB-backed storage engine before publishing
  the runtime
- `MiniKV` does not expose command execution helpers
- `MiniKV` exists to share runtime state with `NetworkServer`

### `src/network/network_server.h` and `src/network/network_server.cc`

Network and connection-management layer.

`NetworkServer` is responsible for:

- creating the listening socket
- accepting connections
- assigning each connection to one I/O thread
- reading and buffering bytes
- parsing RESP requests
- creating `Cmd` objects from RESP parts
- submitting commands to the shared `Scheduler`
- writing encoded RESP responses
- idle timeout and orderly shutdown

### `src/command/*`

Command creation and execution.

This layer:

- registers each supported command once
- separates registration lookup from command creation
- maps command names onto concrete `Cmd` classes
- performs command-specific validation and route-key derivation
- executes against `CommandServices`

### `src/kernel/scheduler.*` and `src/worker/*`

Concurrency core.

`Scheduler` owns:

- one worker thread per configured worker
- one bounded MPSC queue per worker
- a shared `KeyLockTable`
- round-robin plus probe admission control
- queue depth, inflight, and rejection metrics

Correctness rule:

- acquire the striped key lock for `cmd->RouteKey()`
- execute `Cmd::Execute(services)`
- release the key lock after completion

### `src/kernel/storage_engine.*`

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

### `src/kernel/snapshot.*` and `src/kernel/write_context.*`

Read and write helpers.

- `Snapshot` pins one RocksDB snapshot and serves consistent reads
- `WriteContext` owns one logical write batch and commits it once

### `src/types/hash/hash_module.*`

Hash semantics layer.

`HashModule` is responsible for:

- `PutField()`
- `ReadAll()`
- `DeleteFields()`

Current design rules:

- reads always use `Snapshot`
- writes always use `WriteContext`
- hook call sites exist for future secondary effects

### `src/codec/key_codec.*`

Storage-key encoding layer.

It defines:

- `ValueType`
- `ValueEncoding`
- `KeyMetadata`
- binary encoding helpers for metadata and hash keys

## Thread Model

The runtime currently has three kinds of threads:

- one accept thread
- `io_threads` I/O threads
- `worker_threads` worker threads owned by the shared scheduler

### When threads are created

- `MiniKV::Open()` constructs the shared scheduler and its worker threads
- `NetworkServer::Start()` creates all I/O threads and then the accept thread

### I/O and worker split

- I/O threads own sockets, read buffers, write buffers, and response reordering
- worker threads own command execution
- same-key serialization is enforced by `KeyLockTable`
- response order is preserved per connection by request sequence numbers

## Reading Route

1. `README.md`
2. `docs/architecture.md`
3. `docs/layers/runtime.md`
4. `docs/layers/network.md`
5. `docs/layers/command.md`
6. `docs/layers/worker.md`
7. `docs/layers/codec.md`
