# ADR 0001: Current MiniKV Boundaries

## Status

Accepted on 2026-04-18.

## Context

This ADR freezes the current `minikv` implementation boundaries as they exist
in the codebase today. It is intentionally descriptive. It does not propose
future behavior.

## Current Supported Commands

The current command registry is populated by builtin modules during startup:

- `CoreModule`: `PING`, `TYPE`, `EXISTS`, `DEL`, `EXPIRE`, `TTL`, `PTTL`,
  `PERSIST`
- `StringModule`: `SET`, `GET`, `STRLEN`
- `BitmapModule`: `GETBIT`, `SETBIT`, `BITCOUNT`
- `HashModule`: `HSET`, `HGET`, `HMGET`, `HLEN`, `HEXISTS`, `HGETALL`,
  `HKEYS`, `HVALS`, `HDEL`
- `JsonModule`: `JSON.SET`, `JSON.GET`, `JSON.MGET`, `JSON.DEL`,
  `JSON.FORGET`, `JSON.TYPE`, `JSON.CLEAR`, `JSON.TOGGLE`,
  `JSON.NUMINCRBY`
- `ListModule`: `LPUSH`, `LPOP`, `LRANGE`, `RPUSH`, `RPOP`, `LREM`, `LTRIM`,
  `LLEN`
- `SetModule`: `SADD`, `SCARD`, `SDIFF`, `SDIFFSTORE`, `SINTER`,
  `SINTERSTORE`, `SISMEMBER`, `SMEMBERS`, `SMISMEMBER`, `SMOVE`, `SPOP`,
  `SRANDMEMBER`, `SREM`, `SUNION`, `SUNIONSTORE`
- `ZSetModule`: `ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZLEXCOUNT`, `ZRANGE`,
  `ZRANGEBYLEX`, `ZRANGEBYSCORE`, `ZRANK`, `ZREM`, `ZSCORE`
- `GeoModule`: `GEOADD`, `GEOPOS`, `GEOHASH`, `GEODIST`, `GEOSEARCH`
- `StreamModule`: `XADD`, `XTRIM`, `XDEL`, `XLEN`, `XRANGE`, `XREVRANGE`,
  `XREAD`

Current command behavior is intentionally Redis-like for the implemented subset:

- core lifecycle commands operate on live keys and treat expired or tombstoned
  keys as missing
- string and bitmap commands share the same underlying string byte storage
- hash, json, list, set, zset, and stream commands maintain type-specific
  storage semantics and reject wrong-type keys
- geo commands use zset storage as the authoritative member/score source and
  maintain geo sidecar state

No `FT.*` search commands are registered in the shared runtime
`CommandRegistry` loaded by `ModuleManager`.

## Current Thread Model

The current runtime is split into four thread roles:

- one accept thread owns `accept()` on the listening socket
- `io_threads` I/O threads own client sockets, parse RESP requests, buffer
  writes, and preserve per-connection response order
- `worker_threads` worker threads execute commands through the shared
  `Scheduler`
- one module background executor thread runs lightweight module-owned
  maintenance work submitted through `ModuleBackgroundService`

Execution routing rules today:

- each accepted connection is assigned to one I/O thread
- parsed requests are converted into `Cmd` instances on that I/O thread
- the network path submits work into one shared `Scheduler`
- worker selection is queue-oriented round-robin with probing for a queue that
  still has capacity
- locking is driven by a command lock plan: none, single-key, or multi-key
- responses are shipped back to the owning I/O thread and reordered by request
  sequence before writing to the socket

This means socket progress and command execution remain separated.

## Current Response Model

`CommandResponse` currently maps builtin command results onto the active RESP
surface:

- simple string
- integer
- bulk string
- array, including nested arrays
- null
- error

The reply tree and encoder can also represent maps. `CommandResponse` is still
close to RESP rather than being a transport-neutral domain result.

## Current Storage Model

The active kernel split is:

- `StorageEngine`: RocksDB open path, column-family handles, primitive
  `Get/Put/Delete/Write`, and snapshot creation
- `ModuleSnapshot`: consistent read view used by logical multi-column-family
  reads
- `ModuleWriteBatch`: one logical write batch per mutation
- `CoreModule`: protocol-level builtin commands plus key lifecycle services
- builtin type modules: command registration, type-specific storage semantics,
  and whole-key delete handling where applicable

Builtin module scope today:

- builtin modules are compiled into the binary and loaded by `ModuleManager`
- current load order is `CoreModule`, `StringModule`, `BitmapModule`,
  `HashModule`, `JsonModule`, `ListModule`, `SetModule`, `ZSetModule`,
  `GeoModule`, then `StreamModule`
- `CoreModule` exports `CoreKeyService` and `WholeKeyDeleteRegistry`
- `StringModule` exports `string.bridge`
- `HashModule` exports `HashIndexingBridge`
- `ZSetModule` exports `zset.bridge`
- no external ABI or dynamic module loading exists

`StorageEngine` currently opens these RocksDB column families:

- `default`
- `meta`
- `string`
- `hash`
- `list`
- `set`
- `zset`
- `stream`
- `json`
- `timeseries`
- `vectorset`
- `module`

The logical data model is:

- the `meta` column family stores per-key metadata
- type-specific column families store string, hash, json, list, set, zset, and
  stream entity data
- bitmap commands operate on string storage rather than a bitmap-private column
  family
- geo commands use zset storage plus geo-owned sidecar state
- the `module` column family stores auxiliary module-private keyspaces
- the `default` column family is present because RocksDB requires it

Current metadata fields are:

- `type`
- `encoding`
- `version`
- `size`
- `expire_at_ms`

Current active lifecycle behavior:

- live keys are visible to user commands
- expired keys are hidden from user commands
- tombstoned keys are hidden from user commands
- tombstones use the sentinel `expire_at_ms = 1`
- recreating an expired or tombstoned typed value bumps its version where stale
  type-specific rows must remain unreachable

## Current Non-Supported Items

The following are explicitly not supported in the current baseline:

- external module loading, external ABI support, or third-party module-defined
  commands
- search functionality, including any `FT.*` command family
- transaction interfaces such as `MULTI`/`EXEC`
- replication, clustering, or persistence modes beyond local RocksDB storage
- public installed headers under `include/minikv/`

## Consequences

This ADR defines the current compatibility boundary for documentation,
verification, and baseline tooling. Any later feature work should update or
supersede this document before claiming broader semantics than the current code
actually provides.
