# MiniKV Runtime Layer

## Scope

This layer is defined by:

- `src/minikv.h`
- `src/minikv.cc`

It is the runtime owner for the current process. It is not a command API.

## Responsibilities

`MiniKV` owns the long-lived core subsystems:

- `StorageEngine`
- `NoopMutationHook`
- `HashModule`
- `CommandServices`
- `Scheduler`

The ownership graph is:

`MiniKV -> Impl -> { StorageEngine, NoopMutationHook, HashModule, CommandServices, Scheduler }`

`Open()` initializes RocksDB first and only publishes the `MiniKV` instance
after the storage open path succeeds.

## Current Boundary

- `MiniKV` no longer exposes command execution helpers.
- `MiniKV` no longer exposes typed hash helpers.
- `MiniKV` exists to own shared runtime state used by the network layer.
- `NetworkServer` is the only supported external request entrypoint.

The only intentional coupling between runtime and network is that
`NetworkServer` can submit `Cmd` objects into the shared `Scheduler`.

## Current Design Conclusion

The runtime layer is now narrow and explicit:

- storage lifecycle lives here
- hash semantics are owned by typed modules
- keyed command execution is owned by the scheduler
- transport-specific behavior stays outside this layer
