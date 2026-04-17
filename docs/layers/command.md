# MiniKV Command Layer

## Scope

This layer is defined by:

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

- turn parsed RESP parts or compatibility `CommandRequest` values into `Cmd`
- validate arguments and derive `RouteKey()`
- execute command semantics against `CommandContext`

This keeps the server layer unaware of individual command rules.

## Current Command Surface

Supported commands:

- `PING`
- `HSET`
- `HGETALL`
- `HDEL`

`CmdFactory` owns registration for each supported command.

`CreateCmd()` uses that registration to instantiate concrete `Cmd`
implementations from:

- RESP parts
- compatibility `CommandRequest` values

Current command implementations are grouped by family:

- `t_kv.*`: `PING`
- `t_hash.*`: `HSET`, `HGETALL`, `HDEL`

Each command validates its own input in `DoInitial()`, extracts the parameters
it needs, and runs the actual operation in `Do(context)`.

Current execution split:

- `PING` does not depend on storage
- hash commands delegate to `HashModule` through `CommandContext`

Each registration also carries static flags:

- `PING`: `read | fast`
- `HSET`: `write | fast`
- `HGETALL`: `read | slow`
- `HDEL`: `write | slow`

## Current Design Characteristics

- The command layer is small and easy to extend for additional single-key
  commands.
- Validation and execution live in the same command object.
- `CommandContext` keeps command execution decoupled from any one storage or
  type implementation class.
- Command output is normalized into one internal response shape before reaching
  the server encoder.

## Current Design Risks

### Protocol-Shaped Core Types

`CommandResponse` is still close to RESP instead of being a transport-neutral
result type. That keeps the server simple but couples command execution to the
wire format.

### Limited Growth Path For Richer Semantics

The current command path is well suited to straightforward single-key commands.
It still does not define how to handle:

- cross-key ordering
- multi-step atomicity
- richer result types
- conditional updates

As command coverage grows, the main open design question is how much command
metadata should influence scheduling beyond the current same-key lock model.

## Current Design Conclusion

Today, the command layer is small and direct. The main improvement of the
current split is that command objects no longer call storage-oriented DB
semantics directly; they execute against a context that can route to typed
modules and storage primitives separately.
