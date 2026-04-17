# MiniKV Runtime Layer

## Scope

This layer is defined by:

- `src/minikv.h`
- `src/minikv.cc`

It is the runtime owner for the current process. It is not a command API.

## Responsibilities

`MiniKV` owns the long-lived core subsystems:

- `StorageEngine`
- `Scheduler`
- `ModuleManager`

The ownership graph is:

`MiniKV -> Impl -> { StorageEngine, Scheduler, ModuleManager }`

`Open()` initializes RocksDB first and only publishes the `MiniKV` instance
after the storage open path succeeds.

Current module loading behavior:

- builtin modules only
- fixed source-compiled module list
- no external ABI
- no runtime `.so` loading

## Current Boundary

- `MiniKV` no longer exposes command execution helpers.
- `MiniKV` no longer exposes typed hash helpers.
- `MiniKV` exists to own shared runtime state used by the network layer.
- module lifecycle is centralized under `ModuleManager`.
- `NetworkServer` is the only supported external request entrypoint.

The only intentional coupling between runtime and network is that
`NetworkServer` can submit `Cmd` objects into the shared `Scheduler`.

## Current Design Conclusion

The runtime layer is now narrow and explicit:

- storage lifecycle lives here
- builtin module lifecycle lives here
- keyed command execution is owned by the scheduler
- transport-specific behavior stays outside this layer
