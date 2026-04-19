# MiniKV Command Layer

## Scope

This layer is defined by:

- `src/execution/command/command_types.h`
- `src/execution/command/cmd.h`
- `src/execution/command/cmd.cc`
- `src/execution/command/cmd_create.h`
- `src/execution/command/cmd_create.cc`
- `src/execution/registry/command_registry.h`
- `src/execution/registry/command_registry.cc`
- `src/execution/reply/reply.h`
- `src/execution/reply/reply.cc`
- `src/execution/reply/reply_node.h`

It maps parsed RESP command parts into executable operations.

## Responsibilities

The command layer owns four steps:

- register command names and creators in the shared runtime registry
- turn parsed RESP parts into one initialized `Cmd`
- validate arguments and derive a lock plan
- execute command semantics through the module-bound `Cmd`

This keeps the network layer unaware of individual command rules.

## Current Command Surface

Supported commands:

- `PING`
- `TYPE`
- `EXISTS`
- `DEL`
- `EXPIRE`
- `TTL`
- `PTTL`
- `PERSIST`
- `SET`
- `GET`
- `STRLEN`
- `HSET`
- `HGETALL`
- `HDEL`
- `LPUSH`
- `LPOP`
- `LRANGE`
- `RPUSH`
- `RPOP`
- `LREM`
- `LTRIM`
- `LLEN`
- `SADD`
- `SCARD`
- `SMEMBERS`
- `SISMEMBER`
- `SPOP`
- `SRANDMEMBER`
- `SREM`
- `ZADD`
- `ZCARD`
- `ZCOUNT`
- `ZINCRBY`
- `ZLEXCOUNT`
- `ZRANGE`
- `ZRANGEBYLEX`
- `ZRANGEBYSCORE`
- `ZRANK`
- `ZREM`
- `ZSCORE`
- `XADD`
- `XTRIM`
- `XDEL`
- `XLEN`
- `XRANGE`
- `XREVRANGE`
- `XREAD`

Those commands are registered by builtin modules during startup:

- `CoreModule`: `PING`, `TYPE`, `EXISTS`, `DEL`, `EXPIRE`, `TTL`, `PTTL`,
  `PERSIST`
- `StringModule`: `SET`, `GET`, `STRLEN`
- `HashModule`: `HSET`, `HGETALL`, `HDEL`
- `ListModule`: `LPUSH`, `LPOP`, `LRANGE`, `RPUSH`, `RPOP`, `LREM`, `LTRIM`,
  `LLEN`
- `SetModule`: `SADD`, `SCARD`, `SMEMBERS`, `SISMEMBER`, `SPOP`,
  `SRANDMEMBER`, `SREM`
- `ZSetModule`: `ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZLEXCOUNT`, `ZRANGE`,
  `ZRANGEBYLEX`, `ZRANGEBYSCORE`, `ZRANK`, `ZREM`, `ZSCORE`
- `StreamModule`: `XADD`, `XTRIM`, `XDEL`, `XLEN`, `XRANGE`, `XREVRANGE`,
  `XREAD`

`CreateCmd()` resolves names from the shared `CommandRegistry` owned by
`ModuleManager`.

## Registration And Lookup Rules

Current registry behavior:

- command names are normalized to uppercase on registration
- lookup is case-insensitive because callers are expected to normalize before
  lookup
- duplicate command names fail during startup
- each registration keeps its owner module and command flags

## Lock Plans

Each `Cmd` derives one lock plan during `Init()`:

- `kNone`: no key-based locking
- `kSingle`: one route key
- `kMulti`: multiple keys, canonicalized by sorting and deduplicating for lock
  acquisition

Important nuance:

- lock-plan deduplication exists only for locking
- command semantics may still choose to preserve duplicates from the original
  input, such as `EXISTS` counting duplicate keys and `DEL` deleting each key
  at most once

## Reply Shapes

`CommandResponse` is the command-to-network handoff object.

Current helpers can build:

- simple string
- error
- integer
- bulk string
- array
- map
- null

Current builtin commands use:

- simple string: `PING`
- bulk string: `TYPE`, `GET`, `LPOP`, `RPOP`, `SRANDMEMBER`, `SPOP`,
  `ZINCRBY`, `ZSCORE`, `XADD`
- integer: `EXISTS`, `DEL`, `EXPIRE`, `TTL`, `PTTL`, `PERSIST`, `STRLEN`,
  `HSET`, `HDEL`, `LLEN`, `LPUSH`, `RPUSH`, `LREM`, `SADD`, `SCARD`,
  `SISMEMBER`, `SREM`, `ZADD`, `ZCARD`, `ZCOUNT`, `ZLEXCOUNT`, `ZRANK`,
  `ZREM`, `XTRIM`, `XDEL`, `XLEN`
- flat bulk-string array: `HGETALL`, `LRANGE`, `SMEMBERS`, `ZRANGE`,
  `ZRANGEBYLEX`, `ZRANGEBYSCORE`
- nested array: `XRANGE`, `XREVRANGE`, `XREAD`
- null: missing-value reads such as `GET`, `LPOP`, and `RPOP`, plus `XREAD`
  when every requested stream has no newer entries

## Current Design Conclusion

Today the command layer is small and direct. The important cleanup is that
command creation is fully registry-driven and network-driven, with no parallel
function-call compatibility path to maintain.
