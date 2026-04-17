# MiniKV Command Layer

## Scope

This layer is defined by:

- `src/command/command_types.h`
- `src/command/cmd.h`
- `src/command/cmd.cc`
- `src/command/cmd_create.h`
- `src/command/cmd_create.cc`
- `src/command/cmd_factory.h`
- `src/command/cmd_factory.cc`
- `src/command/t_kv.h`
- `src/command/t_kv.cc`
- `src/command/t_hash.h`
- `src/command/t_hash.cc`

It maps parsed RESP command parts into executable operations.

## Responsibilities

The command layer owns three steps:

- turn parsed RESP parts into `Cmd`
- validate arguments and derive `RouteKey()`
- execute command semantics against `CommandServices`

This keeps the network layer unaware of individual command rules.

## Current Command Surface

Supported commands:

- `PING`
- `HSET`
- `HGETALL`
- `HDEL`

`CmdFactory` owns registration for each supported command, and `CreateCmd()`
uses that registration to instantiate concrete `Cmd` implementations from RESP
parts.

Current command implementations are grouped by family:

- `t_kv.*`: `PING`
- `t_hash.*`: `HSET`, `HGETALL`, `HDEL`

## Current Design Characteristics

- `PING` remains protocol-level and storage-independent.
- Hash commands delegate to `HashModule` through `CommandServices`.
- `CommandResponse` normalizes execution output before the network encoder sees
  it.
- The command layer now has one input path: parsed RESP command parts.

## Current Design Conclusion

Today the command layer is small and direct. The main cleanup is that command
creation is now fully network-driven, with no parallel function-call or
compatibility request path to maintain.
