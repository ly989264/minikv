# Hash Builtin Module Integration

`HashModule` is now the first builtin data module in `minikv`.

It lives under `src/modules/hash/` and is loaded by `ModuleManager` during
`MiniKV::Open()` after `CoreModule`.

## What It Owns

`HashModule` owns:

- builtin command registration for `HSET`
- builtin command registration for `HGETALL`
- builtin command registration for `HDEL`
- hash read/write semantics on top of `ModuleSnapshot` and `ModuleWriteBatch`

The hash module no longer depends on a special-case command path in
`src/command/`.

## Registration Path

The registration flow is now:

1. `MiniKV` constructs `ModuleManager`
2. `ModuleManager` calls `HashModule::OnLoad()`
3. `HashModule` registers `HSET`, `HGETALL`, and `HDEL` through
   `ModuleServices::command_registry()`
4. `CreateCmd()` resolves RESP command names against that shared runtime
   registry

This means hash commands use the same registry path as every other builtin
module command.

## Storage Boundary

`HashModule` performs reads and writes through module services only:

- reads go through `ModuleServices::snapshot()`
- writes go through `ModuleServices::storage()`

The module does not receive raw kernel pointers for storage, snapshots, or the
scheduler.

## Current Compatibility Scope

The current module integration is source-level SPI only:

- builtin modules are compiled into the binary
- no external ABI exists
- no `.so` loading exists
- no third-party module contract exists yet

That boundary is intentional for this phase. The goal is to stabilize the
in-process module interfaces before any external ABI work begins.
