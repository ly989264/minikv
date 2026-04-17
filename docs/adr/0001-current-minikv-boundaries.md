# ADR 0001: Current MiniKV Boundaries

## Status

Accepted on 2026-04-17.

## Context

This ADR freezes the current `minikv` implementation boundaries as they exist in
the codebase today after the search-prep kernel split. It is intentionally
descriptive. It does not propose future behavior.

## Current Supported Commands

The current command registry contains exactly four commands:

- `PING`
- `HSET`
- `HGETALL`
- `HDEL`

Current command behavior:

- `PING` takes no arguments and returns `PONG`.
- `HSET key field value` stores one hash field and returns integer `1` when the
  field is inserted, `0` when the field already exists and is overwritten.
- `HGETALL key` returns a flat RESP array of alternating field and value bulk
  strings.
- `HDEL key field [field ...]` removes existing fields and returns the integer
  count of removed fields.

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
- same-key serialization depends on `KeyLockTable`, which locks by a striped
  hash of `cmd->RouteKey()`
- responses are shipped back to the owning I/O thread and reordered by request
  sequence before writing to the socket

This means socket progress and command execution remain separated, but there is
no longer a duplicated runtime structure between `MiniKV` and
`NetworkServer`.

## Current Response Model

`CommandResponse` currently maps to a narrow RESP surface:

- simple string
- integer
- array of bulk strings
- error

Current command-to-response mapping:

- `PING` -> simple string
- `HSET` -> integer
- `HGETALL` -> array of bulk strings
- `HDEL` -> integer
- command or execution failures -> RESP error

There is no current support for richer reply shapes such as maps, sets,
attribute replies, pushed messages, streaming replies, cursor replies, or
module-defined custom reply types.

## Current Storage Model

The active kernel split is:

- `StorageEngine`: RocksDB open path, column-family handles, primitive
  `Get/Put/Delete/Write`, and snapshot creation
- `ModuleSnapshot`: consistent read view used by logical multi-column-family
  reads
- `ModuleWriteBatch`: one logical write batch per mutation
- `HashModule`: hash semantics plus builtin command registration for hash
  commands

Builtin module scope today:

- `CoreModule`: protocol-level builtin commands such as `PING`
- `HashModule`: builtin hash commands and semantics
- no external ABI or dynamic module loading

`StorageEngine` currently opens three RocksDB column families:

- `default`
- `meta`
- `hash`

The logical model is still hash-only:

- the `meta` column family stores per-key metadata
- the `hash` column family stores hash field/value entries
- the `default` column family is present because RocksDB requires it, but it is
  not part of the active `minikv` data model

Current metadata fields are:

- `type`
- `encoding`
- `version`
- `size`
- `expire_at_ms`

Current behavior is limited to hash operations:

- `HSET` reads metadata and field existence through one `ModuleSnapshot` and
  writes metadata plus field value through one `ModuleWriteBatch`
- `HGETALL` reads metadata and scans the `hash` column family through one
  `ModuleSnapshot`
- `HDEL` reads metadata and field existence through one `ModuleSnapshot`,
  deletes existing field keys, and updates or removes metadata through one
  `ModuleWriteBatch`

## Current Non-Supported Items

The following are explicitly not supported in the current baseline:

- non-hash data types such as string, list, set, zset, stream, or generic KV
- complex reply shapes beyond simple string, integer, flat bulk-string array,
  and error
- external module loading, external ABI support, or third-party module-defined
  commands
- search functionality, including any `FT.*` command family

Additional current limitations:

- no TTL or expiration behavior is enforced
- no cross-key atomicity exists
- no transaction interface exists
- no replication, clustering, or persistence modes beyond local RocksDB exist

## Consequences

This ADR defines the current compatibility boundary for documentation,
verification, and baseline tooling. Any later feature work should update or
supersede this document before claiming broader semantics than the current code
actually provides.
