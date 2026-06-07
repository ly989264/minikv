# Current Layering

## Scope

This document records the current source-tree layering for `minikv/` as it
exists today.

## Current Directory Layering

The source layout is:

- `src/app/`: process entrypoint and argument parsing
- `src/common/`: small shared utilities such as thread naming
- `src/network/`: RESP transport, connection lifecycle, request submission, and
  response flush and reorder
- `src/execution/command/`: `Cmd` abstractions, command creation, command flags,
  and command input and response types
- `src/execution/registry/`: runtime command registration and lookup
- `src/execution/reply/`: reply tree representation used by execution and
  network encoding
- `src/execution/scheduler/`: shared scheduler, queue metrics, and worker
  dispatch
- `src/execution/worker/`: worker threads and keyed lock serialization
- `src/core/`: current core builtin commands and key lifecycle service
- `src/types/string/`: string data-type module, command registration, and
  string bridge
- `src/types/bitmap/`: bitmap command module layered on string storage
- `src/types/hash/`: hash data-type module, command registration, bridge,
  observer contract, and hash value types
- `src/types/json/`: JSON data-type module, command registration, document
  helpers, and path parsing
- `src/types/list/`: list data-type module and command registration
- `src/types/set/`: set data-type module, command registration, and member-key
  helpers
- `src/types/zset/`: sorted-set data-type module, command registration, and
  zset bridge
- `src/types/geo/`: geo command module, zset observer integration, and geo
  sidecar helpers
- `src/types/stream/`: stream data-type module, command registration, and
  shared stream encoding helpers
- `src/runtime/`: process configuration and the `MiniKV` runtime owner
- `src/runtime/module/`: builtin module SPI, manager, services, exports, and
  background executor wiring
- `src/storage/engine/`: RocksDB engine, snapshots, and write-batch commit
  helpers
- `src/storage/encoding/`: storage-facing key encoding helpers

## Current File Inventory

### App

- `src/app/main.cc`

### Common

- `src/common/thread_name.h`
- `src/common/thread_name.cc`

### Network

- `src/network/network_server.h`
- `src/network/network_server.cc`
- `src/network/resp_parser.h`
- `src/network/resp_parser.cc`

### Execution

- `src/execution/command/cmd.h`
- `src/execution/command/cmd.cc`
- `src/execution/command/cmd_create.h`
- `src/execution/command/cmd_create.cc`
- `src/execution/command/command_types.h`
- `src/execution/registry/command_registry.h`
- `src/execution/registry/command_registry.cc`
- `src/execution/reply/reply.h`
- `src/execution/reply/reply.cc`
- `src/execution/reply/reply_node.h`
- `src/execution/scheduler/scheduler.h`
- `src/execution/scheduler/scheduler.cc`
- `src/execution/worker/worker.h`
- `src/execution/worker/worker.cc`
- `src/execution/worker/key_lock_table.h`
- `src/execution/worker/key_lock_table.cc`

### Core

- `src/core/core_module.h`
- `src/core/core_module.cc`
- `src/core/key_service.h`
- `src/core/key_service.cc`
- `src/core/whole_key_delete_handler.h`

### Type Modules

String and bitmap:

- `src/types/string/string_bridge.h`
- `src/types/string/string_commands.h`
- `src/types/string/string_commands.cc`
- `src/types/string/string_module.h`
- `src/types/string/string_module.cc`
- `src/types/bitmap/bitmap_commands.h`
- `src/types/bitmap/bitmap_commands.cc`
- `src/types/bitmap/bitmap_module.h`
- `src/types/bitmap/bitmap_module.cc`

Hash:

- `src/types/hash/hash_commands.h`
- `src/types/hash/hash_commands.cc`
- `src/types/hash/hash_module.h`
- `src/types/hash/hash_module.cc`
- `src/types/hash/hash_types.h`
- `src/types/hash/hash_observer.h`
- `src/types/hash/hash_indexing_bridge.h`

JSON:

- `src/types/json/json_commands.h`
- `src/types/json/json_commands.cc`
- `src/types/json/json_document_utils.h`
- `src/types/json/json_document_utils.cc`
- `src/types/json/json_module.h`
- `src/types/json/json_module.cc`
- `src/types/json/json_path.h`
- `src/types/json/json_path.cc`

List:

- `src/types/list/list_commands.h`
- `src/types/list/list_commands.cc`
- `src/types/list/list_module.h`
- `src/types/list/list_module.cc`

Set:

- `src/types/set/set_commands.h`
- `src/types/set/set_commands.cc`
- `src/types/set/set_internal.h`
- `src/types/set/set_internal.cc`
- `src/types/set/set_module.h`
- `src/types/set/set_module.cc`

Sorted set and geo:

- `src/types/zset/zset_bridge.h`
- `src/types/zset/zset_commands.h`
- `src/types/zset/zset_commands.cc`
- `src/types/zset/zset_module.h`
- `src/types/zset/zset_module.cc`
- `src/types/geo/geo_commands.h`
- `src/types/geo/geo_commands.cc`
- `src/types/geo/geo_internal.h`
- `src/types/geo/geo_internal.cc`
- `src/types/geo/geo_module.h`
- `src/types/geo/geo_module.cc`

Stream:

- `src/types/stream/stream_commands.h`
- `src/types/stream/stream_commands.cc`
- `src/types/stream/stream_common.h`
- `src/types/stream/stream_common.cc`
- `src/types/stream/stream_module.h`
- `src/types/stream/stream_module.cc`

### Runtime

- `src/runtime/config.h`
- `src/runtime/minikv.h`
- `src/runtime/minikv.cc`
- `src/runtime/module/module.h`
- `src/runtime/module/module_manager.h`
- `src/runtime/module/module_manager.cc`
- `src/runtime/module/module_services.h`
- `src/runtime/module/module_services.cc`
- `src/runtime/module/background_executor.h`
- `src/runtime/module/background_executor.cc`

### Storage

- `src/storage/engine/storage_engine.h`
- `src/storage/engine/storage_engine.cc`
- `src/storage/engine/snapshot.h`
- `src/storage/engine/snapshot.cc`
- `src/storage/engine/write_context.h`
- `src/storage/engine/write_context.cc`
- `src/storage/encoding/key_codec.h`
- `src/storage/encoding/key_codec.cc`

## Intentional Cross-Layer Couplings

The current layout is cleaner than the older mixed tree, but a few deliberate
cross-layer couplings still exist:

- `src/execution/scheduler/scheduler.h` includes
  `src/network/network_server.h` only to reuse `MetricsSnapshot`
- `src/runtime/module/module_services.h` bridges runtime, execution, and
  storage concerns by design and also depends on the network-shaped
  `MetricsSnapshot`
- `src/core/key_service.cc` owns metadata encoding and directly depends on
  `KeyCodec` plus module storage helpers
- type modules depend on core lifecycle services for metadata lookup,
  wrong-type checks, TTL/tombstone handling, and whole-key delete registration
- `src/types/bitmap/bitmap_module.*` depends on the exported string bridge
  rather than owning bitmap-private storage
- `src/types/hash/hash_observer.h` exposes `KeyMetadata` in its mutation
  payload, which intentionally crosses the type-module and core-lifecycle
  boundary
- `src/types/geo/geo_module.*` depends on the exported zset bridge and observes
  zset mutations to maintain geo sidecar rows

## Public Header Note

The current runtime and server entry headers still live under `src/`:

- `src/runtime/config.h`
- `src/runtime/minikv.h`
- `src/network/network_server.h`

There is no `include/minikv/` public header tree yet in this standalone
project.
