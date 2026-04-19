# MiniKV Module Lifecycle

`minikv` has a builtin-only module platform. Modules are compiled into the
binary and loaded by `ModuleManager`. There is no dynamic `.so` loading, no
filesystem discovery, and no external ABI in the current implementation.

## Lifecycle Hooks

Each module follows exactly three hooks:

1. `OnLoad(ModuleServices&)`
2. `OnStart(ModuleServices&)`
3. `OnStop(ModuleServices&)`

`OnLoad()` and `OnStart()` return `rocksdb::Status`. `OnStop()` returns `void`.

## Call Order

Startup order is fixed:

1. `MiniKV::Open()` creates the runtime owner
2. `MiniKV::Open()` opens `StorageEngine`
3. `MiniKV::Open()` constructs `Scheduler`
4. `MiniKV::Open()` constructs `ModuleManager`
5. `ModuleManager::Initialize()` starts the shared `BackgroundExecutor`
6. `ModuleManager::Initialize()` calls `OnLoad()` for every builtin module in
   order
7. `ModuleManager::Initialize()` calls `OnStart()` for every builtin module in
   order
8. only after all hooks succeed does `MiniKV::Open()` publish the runtime

Shutdown order is the reverse:

1. `ModuleManager::StopAll()` closes the registration and export-publish
   windows
2. `ModuleManager::StopAll()` stops the shared `BackgroundExecutor`
3. `ModuleManager::StopAll()` calls `OnStop()` for loaded modules in reverse
   order
4. each module's owned exports are cleared after its `OnStop()`
5. module teardown completes before `Scheduler` and `StorageEngine` are
   destroyed

Current builtin load order is:

1. `CoreModule`
2. `StringModule`
3. `HashModule`
4. `ListModule`
5. `SetModule`
6. `ZSetModule`

## Startup Windows

Command registration is only allowed during `OnLoad()`.

`ModuleCommandRegistry::Register()` rejects calls made outside that window.
This keeps command names stable before the server starts accepting requests and
makes conflicts fail during `MiniKV::Open()`.

Module export publication uses a slightly wider startup window:

- `ModuleExportRegistry::Publish()` is allowed during `OnLoad()` and `OnStart()`
- the current core exports publish during `CoreModule::OnLoad()`
- the current hash bridge publishes during `HashModule::OnLoad()`
- consumer modules should normally resolve and bind exports during `OnStart()`

Export lookup itself is not restricted to the startup window. Only publication
is.

## Failure Rollback

If any `OnLoad()` fails:

- `MiniKV::Open()` fails
- the failing module is not marked as loaded, so it does not receive `OnStop()`
- exports published by the failing module are cleared immediately
- previously loaded modules receive `OnStop()` in reverse order
- the runtime is not published

If any `OnStart()` fails:

- `MiniKV::Open()` fails
- all modules whose `OnLoad()` succeeded are treated as loaded
- loaded modules, including the module whose `OnStart()` failed, receive
  `OnStop()` in reverse order
- each module's published exports are cleared after its `OnStop()`
- the runtime is not published

## Current Scope

The module platform is intentionally narrow in this phase:

- builtin modules only
- source-level SPI only
- no external ABI
- no runtime module loading or unloading
- no module-to-module private header includes
