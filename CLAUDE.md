# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and test commands

Authoritative compilation and validation must run in a Linux Docker container, not directly on the macOS host. On macOS, first locate or start a Linux Docker container for this workspace; a known container mount path is `/workspace/projects/OpenSource/minikv`.

Preferred all-in-one workflow from the macOS host:

```bash
docker exec <container> sh -lc 'cd /workspace/projects/OpenSource/minikv && ./tools/build_linux.sh'
```

If already inside the Linux container at the repository root:

```bash
./tools/build_linux.sh
```

Useful variants:

```bash
./tools/build_linux.sh --skip-tests
./tools/build_linux.sh --build-dir build --build-type Debug --jobs 8
python3 tools/export_compile_commands.py
```

Manual CMake workflow, run inside the Linux container:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build --parallel 8
ctest --test-dir build --output-on-failure
```

Run a single CTest target:

```bash
ctest --test-dir build -R minikv_hash_module_test --output-on-failure
```

Run a single test binary directly:

```bash
./build/minikv_hash_module_test
```

Run the server and exercise it manually:

```bash
./build/minikv_server --db_path /tmp/minikv --bind 127.0.0.1 --port 6379
python3 tools/resp_cli.py 127.0.0.1 6379 PING
python3 tools/baseline_smoke.py --host 127.0.0.1 --port 6379
```

If refreshing the committed RocksDB bundle from a local checkout:

```bash
./tools/build_linux.sh \
  --rocksdb-source-dir /path/to/rocksdb \
  --rocksdb-reuse-build-dir /path/to/rocksdb/build-minikv
```

## Project architecture

MiniKV is a C++17, single-process Redis-like prototype built on RocksDB. It exposes RESP over TCP, loads builtin modules only, and has no external module ABI or dynamic module loading.

The main request path is:

```text
main -> MiniKV::Open -> ModuleManager -> NetworkServer -> RespParser -> CreateCmd -> Scheduler -> Worker -> builtin module command -> ModuleSnapshot / ModuleWriteBatch -> StorageEngine -> RocksDB
```

Key layers:

- `src/app/main.cc` parses process flags, opens `MiniKV`, creates `NetworkServer`, and runs until shutdown.
- `src/runtime/minikv.*` owns `StorageEngine`, `Scheduler`, and `ModuleManager`; `MiniKV::Open()` opens RocksDB before loading builtin modules.
- `src/runtime/module/` contains the builtin module SPI, lifecycle manager, exports registry, module service facades, module keyspaces, and background executor.
- `src/network/` owns the TCP accept loop, I/O threads, RESP parsing, command submission, per-connection response ordering, and response encoding.
- `src/execution/command/`, `registry/`, `reply/`, `scheduler/`, and `worker/` define command creation, command registration, reply representation, worker queues, and keyed lock serialization.
- `src/core/` implements protocol-level commands and key lifecycle services: metadata lookup, TTL handling, tombstones, and whole-key delete dispatch.
- `src/types/*/` contains builtin data-type modules for string, bitmap, hash, json, list, set, zset, geo, and stream commands.
- `src/storage/engine/` wraps RocksDB open/get/put/delete/write/snapshot behavior; `src/storage/encoding/` owns storage-facing key encodings.

Builtin module load order is fixed in the runtime: core, string, bitmap, hash, json, list, set, zset, geo, then stream. Modules register commands during `OnLoad()` and collaborate through narrow exports such as `core.key_service`, `core.whole_key_delete_registry`, `string.bridge`, `hash.indexing_bridge`, and `zset.bridge` rather than by reaching into each other's private storage.

Concurrency is keyed: I/O threads own sockets and parsing, worker threads execute commands, and `KeyLockTable` serializes commands according to each command's lock plan (`none`, `single`, or `multi`). Same-connection response order is restored by request sequence numbers even when workers complete out of order.

Storage uses RocksDB column families for metadata and type-specific data. User-visible key lifecycle states include missing, live, expired, and tombstone; expired and tombstoned keys are treated as non-existent by lookup commands. New module-private state should generally use `ModuleKeyspace` rather than raw column-family access.

Important current boundaries and caveats:

- The command surface is network-only; there is no parallel in-process command API to keep synchronized.
- `CommandResponse` is still RESP-shaped rather than fully transport-neutral.
- The module SPI is builtin/source-level only; do not assume support for third-party dynamic modules.
- Search prep infrastructure exists through hash observers/indexing bridges, but there is no `SearchModule` and no `FT.*` command family.
- Public runtime/server headers still live under `src/`; there is no `include/minikv/` public header tree yet.

## Documentation map

Start with `README.md`, then `docs/build.md` for validation workflow and `docs/getting-started.md` for the implementation walkthrough. Use `docs/architecture.md`, `docs/architecture/current-layering.md`, and `docs/layers/*.md` for deeper architectural details before making cross-layer changes.
