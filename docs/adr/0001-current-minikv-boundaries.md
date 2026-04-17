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

No other command names are registered in `CmdFactory`.

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
- both embedded and server paths submit work into one shared `Scheduler`
- worker selection is queue-oriented round-robin with probing for a queue that
  still has capacity
- same-key serialization depends on `KeyLockTable`, which locks by a striped
  hash of `cmd->RouteKey()`
- responses are shipped back to the owning I/O thread and reordered by request
  sequence before writing to the socket

This means socket progress and command execution remain separated, but there is
no longer a duplicated runtime structure between `MiniKV` and `Server`.

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
- `Snapshot`: consistent read view used by logical multi-column-family reads
- `WriteContext`: one logical write batch per mutation
- `HashModule`: hash semantics on top of storage primitives
- `MutationHook`: hook interface for future secondary effects, currently no-op

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

- `HSET` reads metadata and field existence through one `Snapshot`, writes
  metadata plus field value through one `WriteContext`, and passes through the
  mutation hook call site before commit
- `HGETALL` reads metadata and scans the `hash` column family through one
  `Snapshot`
- `HDEL` reads metadata and field existence through one `Snapshot`, deletes
  existing field keys and updates or removes metadata through one
  `WriteContext`, and passes through the mutation hook call site before commit

`src/engine/db_engine.h` remains only as a compatibility alias to
`StorageEngine`.

## Current Non-Supported Items

The following are explicitly not supported in the current baseline:

- non-hash data types such as string, list, set, zset, stream, or generic KV
- complex reply shapes beyond simple string, integer, flat bulk-string array,
  and error
- module platform behavior, module loading, or module-defined commands
- search functionality, including any `FT.*` command family

Additional current limitations:

- no TTL or expiration behavior is enforced
- no cross-key atomicity exists
- no transaction interface exists
- no replication, clustering, or persistence modes beyond local RocksDB exist
- mutation hooks do not yet have any non-noop implementation

## Consequences

This ADR defines the current compatibility boundary for documentation,
verification, and baseline tooling. Any later feature work should update or
supersede this document before claiming broader semantics than the current code
actually provides.
