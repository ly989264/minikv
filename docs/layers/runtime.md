# MiniKV Runtime Layer

## Scope

This layer is defined by:

- `src/runtime/config.h`
- `src/runtime/minikv.h`
- `src/runtime/minikv.cc`

It is the runtime owner for the current process. It is not a public command
API.

## Responsibilities

`MiniKV` owns the long-lived core subsystems:

- `StorageEngine`
- `Scheduler`
- `ModuleManager`

The ownership graph is:

`MiniKV -> Impl -> { Config, StorageEngine, Scheduler, ModuleManager }`

`MiniKV::Open()`:

1. allocates the implementation object
2. opens RocksDB through `StorageEngine`
3. constructs `Scheduler`
4. constructs `ModuleManager`
5. loads builtin modules
6. publishes the runtime only after all of the above succeed

Current builtin module loading behavior:

- builtin modules only
- fixed source-compiled module list
- current load order is `CoreModule`, `StringModule`, `HashModule`,
  `ListModule`, `SetModule`, then `ZSetModule`
- no external ABI
- no runtime `.so` loading

## Current Boundary

- `MiniKV` does not expose command execution helpers
- `MiniKV` does not expose typed hash helpers
- `MiniKV` exists to own shared runtime state used by the network layer
- module lifecycle is centralized under `ModuleManager`
- `NetworkServer` is the only supported external request entrypoint

The current intentional coupling between runtime and network is:

- `NetworkServer` can access the shared `Scheduler`
- `NetworkServer` can read the shared `CommandRegistry` owned by
  `ModuleManager`

## Current Design Conclusion

The runtime layer is narrow and explicit:

- configuration defaults live here
- storage lifecycle lives here
- builtin module lifecycle lives here
- keyed command execution is owned by the scheduler
- transport-specific behavior stays outside this layer
