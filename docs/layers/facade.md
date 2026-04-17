# MiniKV Facade Layer

## Scope

This layer is defined by:

- `include/minikv/minikv.h`
- `include/minikv/command.h`
- `include/minikv/config.h`
- `src/minikv.cc`

It is the public entrypoint for embedding `minikv` as a library.

## Responsibilities

`MiniKV` currently owns five internal subsystems:

- `StorageEngine`
- `NoopMutationHook`
- `HashModule`
- `CommandContext`
- `Scheduler`

The facade exposes:

- lifecycle: `MiniKV::Open()`
- generic command path: `Execute()` and `Submit()`
- typed helpers: `HSet()`, `HGetAll()`, `HDel()`

The ownership graph is now:

`MiniKV -> Impl -> { StorageEngine, NoopMutationHook, HashModule, CommandContext, Scheduler }`

`Open()` initializes the storage engine first and only publishes the `MiniKV`
instance after the RocksDB open path succeeds.

## Current Design Characteristics

- The facade is intentionally thin. It does not contain command semantics.
- The synchronous path runs `Cmd` objects through `Scheduler::ExecuteInline()`.
- The asynchronous path runs through that same shared `Scheduler`.
- Typed helpers are implemented by constructing `CommandRequest` values and
  then invoking the unified command path.
- `CommandRequest` is still a compatibility surface; the internal path converts
  it into a concrete `Cmd` via `CmdFactory` before execution or async
  submission.

This gives the code one execution path instead of duplicating business logic
between direct API calls and server requests.

## Current Design Tension

The facade still mixes two roles:

- an embedded storage API
- a command-oriented execution interface shaped around server commands

This is visible in:

- `CommandType`
- `CommandRequest`
- `CommandResponse`
- typed helpers such as `HSet()`

The current public API is therefore still not fully transport-neutral.

## Current Design Conclusion

For the current prototype, the facade is small and coherent enough. The main
cleanup gained by the current split is that scheduling, storage primitives, and
hash semantics are no longer hidden inside one DB-oriented runtime object.
