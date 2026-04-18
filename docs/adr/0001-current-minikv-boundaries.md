# ADR 0001: Current MiniKV Boundaries

## Status

Accepted on 2026-04-18.

## Context

This ADR freezes the current `minikv` implementation boundaries as they exist
in the codebase today. It is intentionally descriptive. It does not propose
future behavior.

## Current Supported Commands

The current command registry contains exactly eleven commands:

- `PING`
- `TYPE`
- `EXISTS`
- `DEL`
- `EXPIRE`
- `TTL`
- `PTTL`
- `PERSIST`
- `HSET`
- `HGETALL`
- `HDEL`

Current command behavior:

- `PING` takes no arguments and returns `PONG`
- `TYPE key` returns the current logical type name or `none`
- `EXISTS key [key ...]` counts the number of live keys and preserves duplicate
  keys in the count
- `DEL key [key ...]` deletes each live key at most once and returns the number
  of deleted keys
- `EXPIRE key seconds` sets a TTL on a live key and returns `1` on success, `0`
  when the key is not live; zero or negative seconds route through whole-key
  delete
- `TTL key` returns `-2` for missing, expired, or tombstoned keys, `-1` for
  live keys without TTL, or the remaining TTL in whole seconds
- `PTTL key` returns the same state codes as `TTL`, but uses milliseconds for
  positive TTL values
- `PERSIST key` clears a live key's TTL and returns `1` on success, `0`
  otherwise
- `HSET key field value` stores one hash field and returns integer `1` when the
  field is inserted, `0` when the field already exists and is overwritten
- `HGETALL key` returns a flat RESP array of alternating field and value bulk
  strings for the visible incarnation of the hash
- `HDEL key field [field ...]` removes existing fields and returns the integer
  count of removed fields

No other command names are registered in the shared runtime `CommandRegistry`
loaded by `ModuleManager`.

## Current Thread Model

The current runtime is split into three thread roles:

- one accept thread owns `accept()` on the listening socket
- `io_threads` I/O threads own client sockets, parse RESP requests, buffer
  writes, and preserve per-connection response order
- `worker_threads` worker threads execute commands through the shared
  `Scheduler`

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

`CommandResponse` currently maps builtin command results onto this active RESP
surface:

- simple string
- integer
- bulk string
- array of bulk strings
- error

Current command-to-response mapping:

- `PING` -> simple string
- `TYPE` -> bulk string
- `EXISTS`, `DEL`, `EXPIRE`, `TTL`, `PTTL`, `PERSIST`, `HSET`, `HDEL` ->
  integer
- `HGETALL` -> flat array of bulk strings
- command or execution failures -> RESP error

The reply tree and encoder can also represent maps and nulls, but no current
builtin command uses those reply shapes.

## Current Storage Model

The active kernel split is:

- `StorageEngine`: RocksDB open path, column-family handles, primitive
  `Get/Put/Delete/Write`, and snapshot creation
- `ModuleSnapshot`: consistent read view used by logical multi-column-family
  reads
- `ModuleWriteBatch`: one logical write batch per mutation
- `CoreModule`: protocol-level builtin commands plus key lifecycle services
- `HashModule`: hash semantics plus builtin command registration for hash
  commands

Builtin module scope today:

- `CoreModule`: exports `CoreKeyService` and `WholeKeyDeleteRegistry`
- `HashModule`: exports `HashIndexingBridge`
- no external ABI or dynamic module loading

`StorageEngine` currently opens four RocksDB column families:

- `default`
- `meta`
- `hash`
- `module`

The logical model is still hash-only for user-visible data:

- the `meta` column family stores per-key metadata
- the `hash` column family stores hash field/value entries
- the `module` column family stores module-private keyspaces
- the `default` column family is present because RocksDB requires it, but it is
  not part of the active `minikv` data model

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
- recreating an expired or tombstoned hash bumps its version

## Current Non-Supported Items

The following are explicitly not supported in the current baseline:

- non-hash user-visible data types such as string, list, set, zset, or stream
- external module loading, external ABI support, or third-party module-defined
  commands
- search functionality, including any `FT.*` command family
- transaction interfaces such as `MULTI`/`EXEC`
- replication, clustering, or persistence modes beyond local RocksDB storage

## Consequences

This ADR defines the current compatibility boundary for documentation,
verification, and baseline tooling. Any later feature work should update or
supersede this document before claiming broader semantics than the current code
actually provides.
