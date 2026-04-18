# Current Layering

## Scope And Non-Goals

This document records the current structural layering cleanup for `minikv/`.

This pass is intentionally limited to:

- directory cleanup
- file re-homing
- include and build path fixes
- minimal mechanical structure cleanup needed to keep the build working

This pass does not do semantic refactoring of core lifecycle, observer,
indexing bridge, expire, tombstone, or command behavior.

## Step 1. Pre-Move Layering Inventory

The following inventory assigns every current `src/` `.h` / `.cc` file to the
layer it primarily belongs to today. When a file still spans multiple concerns,
the assignment follows its dominant responsibility so the directory layout can
match the current implementation without forcing semantic extraction in this PR.

### Network

- `src/network/network_server.h`
- `src/network/network_server.cc`
- `src/network/resp_parser.h`
- `src/network/resp_parser.cc`

### Execution

- `src/command/cmd.h`
- `src/command/cmd.cc`
- `src/command/cmd_create.h`
- `src/command/cmd_create.cc`
- `src/command/command_types.h`
- `src/kernel/command_registry.h`
- `src/kernel/command_registry.cc`
- `src/kernel/reply.h`
- `src/kernel/reply.cc`
- `src/kernel/reply_node.h`
- `src/kernel/scheduler.h`
- `src/kernel/scheduler.cc`
- `src/worker/worker.h`
- `src/worker/worker.cc`
- `src/worker/key_lock_table.h`
- `src/worker/key_lock_table.cc`

### Core

- `src/modules/core/core_module.h`
- `src/modules/core/core_module.cc`
- `src/modules/core/key_service.h`
- `src/modules/core/key_service.cc`
- `src/modules/core/whole_key_delete_handler.h`

### Type Module

- `src/modules/hash/hash_module.h`
- `src/modules/hash/hash_module.cc`
- `src/modules/hash/hash_types.h`
- `src/modules/hash/hash_observer.h`
- `src/modules/hash/hash_indexing_bridge.h`

### Runtime

- `src/config.h`
- `src/minikv.h`
- `src/minikv.cc`
- `src/module/module.h`
- `src/module/module_manager.h`
- `src/module/module_manager.cc`
- `src/module/module_services.h`
- `src/module/module_services.cc`
- `src/module/background_executor.h`
- `src/module/background_executor.cc`
- `src/main.cc`

### Storage

- `src/kernel/storage_engine.h`
- `src/kernel/storage_engine.cc`
- `src/kernel/snapshot.h`
- `src/kernel/snapshot.cc`
- `src/kernel/write_context.h`
- `src/kernel/write_context.cc`
- `src/codec/key_codec.h`
- `src/codec/key_codec.cc`

### Common

- `src/common/thread_name.h`
- `src/common/thread_name.cc`

## Mixed-Layer Files Found During Inventory

- `src/codec/key_codec.h` and `src/codec/key_codec.cc` are assigned to
  storage because their main job is storage key encoding, but they currently
  mix general metadata encoding with hash-specific data-key encoding.
- `src/modules/hash/hash_module.h` and `src/modules/hash/hash_module.cc` are
  assigned to the hash type module, but they still depend on core key lifecycle
  services and core whole-key delete wiring.
- `src/modules/hash/hash_observer.h` is assigned to the hash type module, but
  its mutation payload includes `KeyMetadata`, so it currently crosses into
  core lifecycle state.
- `src/modules/core/key_service.h` and `src/modules/core/key_service.cc` are
  assigned to core because they define the current core key lifecycle service,
  but the implementation directly uses storage metadata encoding and runtime
  module storage services.
- `src/module/module_services.h` and `src/module/module_services.cc` are
  assigned to runtime because they are module SPI wiring, but they expose
  execution and storage facilities and currently also depend on the
  network-layer `MetricsSnapshot` shape through `ModuleSchedulerView`, so they
  remain a deliberate cross-layer support surface.
- `src/kernel/scheduler.h` is assigned to execution, but it currently includes
  `network/network_server.h` only to reuse `MetricsSnapshot`, which is a
  layering mismatch that remains visible today.
- `src/main.cc` is temporarily grouped under runtime for the pre-move
  inventory because it is pure process bootstrap, but its target location in
  this cleanup is `src/app/`.

## Current Directory Layering

The post-move source layout is:

- `src/app/`: process entrypoint and argument parsing
- `src/common/`: small shared utilities
- `src/network/`: RESP transport, connection lifecycle, request submission, and
  response flush/reorder
- `src/execution/command/`: `Cmd` abstractions, command creation, and command
  result types
- `src/execution/registry/`: runtime command registration and lookup
- `src/execution/reply/`: reply tree representation used by execution and
  network encoding
- `src/execution/scheduler/`: shared scheduler, queue metrics, and worker
  dispatch
- `src/execution/worker/`: worker threads and keyed lock serialization
- `src/core/`: current core builtin commands and core key lifecycle service
- `src/types/hash/`: hash data-type module, bridge, observer contract, and hash
  value types
- `src/runtime/`: process configuration and the `MiniKV` runtime owner
- `src/runtime/module/`: builtin module SPI, manager, services, exports, and
  background executor wiring
- `src/storage/engine/`: RocksDB engine, snapshots, and write-batch commit
  helpers
- `src/storage/encoding/`: storage-facing key and metadata encoding helpers

## Old Path To New Path Mapping

| Old path | New path |
| --- | --- |
| `src/main.cc` | `src/app/main.cc` |
| `src/common/thread_name.h` | `src/common/thread_name.h` |
| `src/common/thread_name.cc` | `src/common/thread_name.cc` |
| `src/network/network_server.h` | `src/network/network_server.h` |
| `src/network/network_server.cc` | `src/network/network_server.cc` |
| `src/network/resp_parser.h` | `src/network/resp_parser.h` |
| `src/network/resp_parser.cc` | `src/network/resp_parser.cc` |
| `src/command/cmd.h` | `src/execution/command/cmd.h` |
| `src/command/cmd.cc` | `src/execution/command/cmd.cc` |
| `src/command/cmd_create.h` | `src/execution/command/cmd_create.h` |
| `src/command/cmd_create.cc` | `src/execution/command/cmd_create.cc` |
| `src/command/command_types.h` | `src/execution/command/command_types.h` |
| `src/kernel/command_registry.h` | `src/execution/registry/command_registry.h` |
| `src/kernel/command_registry.cc` | `src/execution/registry/command_registry.cc` |
| `src/kernel/reply.h` | `src/execution/reply/reply.h` |
| `src/kernel/reply.cc` | `src/execution/reply/reply.cc` |
| `src/kernel/reply_node.h` | `src/execution/reply/reply_node.h` |
| `src/kernel/scheduler.h` | `src/execution/scheduler/scheduler.h` |
| `src/kernel/scheduler.cc` | `src/execution/scheduler/scheduler.cc` |
| `src/worker/worker.h` | `src/execution/worker/worker.h` |
| `src/worker/worker.cc` | `src/execution/worker/worker.cc` |
| `src/worker/key_lock_table.h` | `src/execution/worker/key_lock_table.h` |
| `src/worker/key_lock_table.cc` | `src/execution/worker/key_lock_table.cc` |
| `src/modules/core/core_module.h` | `src/core/core_module.h` |
| `src/modules/core/core_module.cc` | `src/core/core_module.cc` |
| `src/modules/core/key_service.h` | `src/core/key_service.h` |
| `src/modules/core/key_service.cc` | `src/core/key_service.cc` |
| `src/modules/core/whole_key_delete_handler.h` | `src/core/whole_key_delete_handler.h` |
| `src/modules/hash/hash_module.h` | `src/types/hash/hash_module.h` |
| `src/modules/hash/hash_module.cc` | `src/types/hash/hash_module.cc` |
| `src/modules/hash/hash_types.h` | `src/types/hash/hash_types.h` |
| `src/modules/hash/hash_observer.h` | `src/types/hash/hash_observer.h` |
| `src/modules/hash/hash_indexing_bridge.h` | `src/types/hash/hash_indexing_bridge.h` |
| `src/config.h` | `src/runtime/config.h` |
| `src/minikv.h` | `src/runtime/minikv.h` |
| `src/minikv.cc` | `src/runtime/minikv.cc` |
| `src/module/module.h` | `src/runtime/module/module.h` |
| `src/module/module_manager.h` | `src/runtime/module/module_manager.h` |
| `src/module/module_manager.cc` | `src/runtime/module/module_manager.cc` |
| `src/module/module_services.h` | `src/runtime/module/module_services.h` |
| `src/module/module_services.cc` | `src/runtime/module/module_services.cc` |
| `src/module/background_executor.h` | `src/runtime/module/background_executor.h` |
| `src/module/background_executor.cc` | `src/runtime/module/background_executor.cc` |
| `src/kernel/storage_engine.h` | `src/storage/engine/storage_engine.h` |
| `src/kernel/storage_engine.cc` | `src/storage/engine/storage_engine.cc` |
| `src/kernel/snapshot.h` | `src/storage/engine/snapshot.h` |
| `src/kernel/snapshot.cc` | `src/storage/engine/snapshot.cc` |
| `src/kernel/write_context.h` | `src/storage/engine/write_context.h` |
| `src/kernel/write_context.cc` | `src/storage/engine/write_context.cc` |
| `src/codec/key_codec.h` | `src/storage/encoding/key_codec.h` |
| `src/codec/key_codec.cc` | `src/storage/encoding/key_codec.cc` |

## Remaining Mismatches

- `src/storage/encoding/key_codec.h` and `src/storage/encoding/key_codec.cc`
  stay in storage because their main responsibility is storage encoding, but
  they still mix generic metadata encoding with hash-specific key encoding.
- `src/types/hash/hash_module.h` and `src/types/hash/hash_module.cc` stay under
  the hash type module, but they still contain dependencies on core key
  lifecycle service APIs and whole-key delete registration.
- `src/types/hash/hash_observer.h` stays under the hash type module, but its
  mutation payload still exposes core lifecycle metadata.
- `src/core/key_service.h` and `src/core/key_service.cc` stay under core, but
  the implementation still directly touches storage encoding and runtime module
  storage services.
- `src/runtime/module/module_services.h` and
  `src/runtime/module/module_services.cc` stay under runtime because they are
  module SPI wiring, but they still intentionally bridge runtime with execution
  and storage primitives and also depend on the network-layer
  `MetricsSnapshot` shape through `ModuleSchedulerView`.
- `src/execution/scheduler/scheduler.h` stays under execution, but it still
  includes `src/network/network_server.h` only to reuse `MetricsSnapshot`.

## Explicit Non-Goals For This PR

This PR does not extract a new semantic core lifecycle layer, does not redesign
observer or bridge behavior, does not change DEL / EXPIRE / TTL / tombstone /
version behavior, and does not introduce new service abstractions such as
`CoreKeyService` replacements. It only reorganizes files to better match the
current implementation layering.
