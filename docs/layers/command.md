# MiniKV Command Layer

## Scope

This layer is defined by:

- `src/command/command_types.h`
- `src/command/cmd.h`
- `src/command/cmd.cc`
- `src/command/cmd_create.h`
- `src/command/cmd_create.cc`

It maps parsed RESP command parts into executable operations.

## Responsibilities

The command layer owns three steps:

- turn parsed RESP parts into `Cmd`
- validate arguments and derive `RouteKey()`
- execute command semantics through the module-bound `Cmd`

This keeps the network layer unaware of individual command rules.

## Current Command Surface

Supported commands:

- `PING`
- `HSET`
- `HGETALL`
- `HDEL`

Those commands are registered by builtin modules during startup:

- `CoreModule`: `PING`
- `HashModule`: `HSET`, `HGETALL`, `HDEL`

`CreateCmd()` now resolves names from the runtime `CommandRegistry` owned by
`ModuleManager`.

## Current Design Characteristics

- `PING` remains protocol-level and storage-independent.
- Hash commands are created from registrations owned by `HashModule`.
- `CommandResponse` normalizes execution output before the network encoder sees
  it.
- The command layer now has one input path: parsed RESP command parts.

## Current Design Conclusion

Today the command layer is small and direct. The main cleanup is that command
creation is now fully network-driven, with no parallel function-call or
compatibility request path to maintain.
