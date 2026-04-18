# Hash Builtin Module Integration

`HashModule` is the first builtin data module in `minikv`.

It lives under `src/types/hash/` and is loaded by `ModuleManager` during
`MiniKV::Open()` after `CoreModule`.

## What It Owns

`HashModule` owns:

- builtin command registration for `HSET`
- builtin command registration for `HGETALL`
- builtin command registration for `HDEL`
- the exported `HashIndexingBridge`
- whole-key delete handling for hash values
- hash read, write, delete, tombstone, and recreate semantics on top of
  `ModuleSnapshot` and `ModuleWriteBatch`

The hash module no longer depends on a special-case command path in
`src/execution/command/`.

## Registration Path

The registration flow is:

1. `MiniKV` constructs `ModuleManager`
2. `ModuleManager` calls `HashModule::OnLoad()`
3. `HashModule` publishes `hash.indexing_bridge`
4. `HashModule` registers `HSET`, `HGETALL`, and `HDEL` through
   `ModuleServices::command_registry()`
5. `ModuleManager` later calls `HashModule::OnStart()`
6. `HashModule` resolves `core.key_service` and
   `core.whole_key_delete_registry`
7. `HashModule` registers itself as the whole-key delete handler for
   `ObjectType::kHash`
8. `CreateCmd()` resolves RESP command names against the shared runtime
   registry

This means hash commands use the same registry path as every other builtin
module command.

## Storage Boundary

`HashModule` performs reads and writes through module services only:

- reads go through `ModuleServices::snapshot()`
- writes go through `ModuleServices::storage()`

The module does not receive raw kernel pointers for storage, snapshots, or the
scheduler.

Current storage layout:

- metadata row in the `meta` column family
- hash fields in the `hash` column family under the current metadata version
- no user-visible hash data in the `module` column family

## Current Behavior

Current hash behavior in code is:

- `HSET` inserts or overwrites one field and returns `1` on insert, `0` on
  overwrite
- `HGETALL` scans all visible fields for the current metadata version and
  returns a flat field/value array
- `HDEL` removes existing fields and returns the number of removed fields
- deleting the final field writes a metadata tombstone instead of deleting the
  metadata row
- whole-key delete through `DEL` or `EXPIRE key 0` removes all current-version
  field rows and writes a metadata tombstone
- recreating an expired or tombstoned hash bumps the metadata version so stale
  field rows from an earlier incarnation stay unreachable

## Observer Integration

`HashModule` also implements the producer side of the current indexing bridge.

Current observer flow:

1. another builtin module resolves `hash.indexing_bridge`
2. it calls `AddObserver()` with a `HashObserver`
3. `HashModule` builds a `HashMutation` describing the logical change
4. `HashModule` passes that mutation and the shared `ModuleWriteBatch` to each
   observer before commit
5. if every observer succeeds, the combined batch is committed once

This gives observers an atomic extension point for hash writes without
exposing the kernel write path directly.

## Current Compatibility Scope

The current module integration is source-level SPI only:

- builtin modules are compiled into the binary
- no external ABI exists
- no `.so` loading exists
- no third-party module contract exists yet

That boundary is intentional for this phase. The goal is to stabilize the
in-process module interfaces before any external ABI work begins.
