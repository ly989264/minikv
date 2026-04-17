# MiniKV Module Lifecycle

`minikv` now has a builtin-only module platform. Modules are compiled into the
binary and loaded by `ModuleManager`. There is no dynamic `.so` loading, no
filesystem discovery, and no external ABI in the current implementation.

## Lifecycle

Each module follows exactly three hooks:

1. `OnLoad(ModuleServices&)`
2. `OnStart(ModuleServices&)`
3. `OnStop(ModuleServices&)`

`OnLoad()` and `OnStart()` return `rocksdb::Status`. `OnStop()` returns `void`
and must be idempotent.

## Call Order

Startup order is fixed:

1. `MiniKV::Open()` opens `StorageEngine`
2. `MiniKV` constructs `Scheduler`
3. `MiniKV` constructs `ModuleManager`
4. `ModuleManager` calls `OnLoad()` for every builtin module in order
5. `ModuleManager` calls `OnStart()` for every builtin module in order
6. only after all hooks succeed does `MiniKV::Open()` publish the runtime

Shutdown order is the reverse:

1. `ModuleManager::StopAll()` calls `OnStop()` in reverse module order
2. module teardown completes before `Scheduler` and `StorageEngine` are
   destroyed

Current builtin load order is:

1. `CoreModule`
2. `HashModule`

## Registration Window

Command registration is only allowed during `OnLoad()`.

`ModuleCommandRegistry::Register()` rejects calls made outside that window.
This keeps command names stable before the server starts accepting requests and
makes conflicts fail during `MiniKV::Open()`.

## Failure Rollback

If any `OnLoad()` fails:

- `MiniKV::Open()` fails
- previously loaded modules receive `OnStop()` in reverse order
- the runtime is not published

If any `OnStart()` fails:

- `MiniKV::Open()` fails
- all loaded modules, including the one whose `OnStart()` failed, receive
  `OnStop()` in reverse order
- the runtime is not published

## Current Scope

The module platform is intentionally narrow in this phase:

- builtin modules only
- source-level SPI only
- no external ABI
- no runtime module loading or unloading
- no module-to-module private header includes
