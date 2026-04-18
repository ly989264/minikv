# ADR 0002: Consistency Model V1

## Status

Accepted on 2026-04-18.

## Context

This ADR documents the consistency model that the current `minikv`
implementation actually provides. It is scoped to the current command set:
`PING`, `TYPE`, `EXISTS`, `DEL`, `EXPIRE`, `TTL`, `PTTL`, `PERSIST`,
`HSET`, `HGETALL`, and `HDEL`.

## Decision

The current consistency model is intentionally narrow and should be read as a
baseline contract, not as a general Redis-compatible transaction model.

### Keyed Consistency Depends On `Scheduler` And `KeyLockTable`

For commands with a lock plan, correctness depends on the shared `Scheduler`
plus `KeyLockTable`:

- single-key commands such as `TYPE`, `EXPIRE`, `TTL`, `PTTL`, `PERSIST`,
  `HSET`, `HGETALL`, and `HDEL` acquire one logical key lock
- multi-key commands such as `EXISTS` and `DEL` acquire a canonicalized set of
  stripe locks in stable order
- requests that overlap on the same protected stripes therefore serialize even
  when different workers pick them up

This means current consistency still comes from keyed execution serialization,
not from RocksDB transactions.

### Logical Reads Use One Snapshot Per Command

Current logical reads use RocksDB snapshots:

- core lookup-based commands create one `ModuleSnapshot`
- `HashModule::ReadAll()` acquires one `ModuleSnapshot`
- metadata lookup and hash-prefix scan share that same snapshot handle

This gives current commands a consistent multi-column-family view for one
logical operation.

This is still not general snapshot isolation:

- no snapshot is shared across multiple commands
- the public API does not expose long-lived snapshots
- there is no cross-connection transaction model

### Writes Use One Write Batch Per Logical Mutation

Current write paths are grouped by one logical `ModuleWriteBatch`:

- `HSET` and `HDEL` build one `rocksdb::WriteBatch` per logical mutation
- `DEL` builds one shared batch across the targeted keys in that command
- `EXPIRE`, `PERSIST`, and whole-key delete flows also stage writes through one
  logical batch
- the batch is committed once after the logical mutation is fully prepared

This does not remove the need for keyed serialization:

- read-modify-write flows still rely on scheduler locking to avoid conflicting
  updates

### Multi-Key Support Is Limited But Real

The current command set does contain limited multi-key operations:

- `EXISTS key [key ...]` acquires a multi-key lock plan and reads the targeted
  keys from one snapshot while holding those locks
- `DEL key [key ...]` acquires a multi-key lock plan, reads the targeted keys
  from one snapshot, stages all deletes and tombstone writes in one batch, and
  commits once

Current caveats:

- duplicate keys are preserved by `EXISTS` counting semantics
- duplicate keys are deduplicated by `DEL` deletion semantics
- there is still no general user-facing transaction interface
- there is still no arbitrary multi-command atomicity

### TTL, Tombstones, And Versioning Are Active Semantics

The current metadata schema fields `version` and `expire_at_ms` are active:

- `EXPIRE` writes TTL metadata for live keys
- `TTL` and `PTTL` interpret missing, live, expired, and tombstoned states
- zero-or-negative `EXPIRE` routes through whole-key delete
- deleting the final hash field writes a tombstone metadata row
- `DEL` and whole-key delete also write tombstones
- recreating an expired or tombstoned hash increments the metadata version so
  stale field rows remain unreachable

This means those fields must be documented as implemented behavior, not as
reserved placeholders.

### Module SPI Is Builtin-Only

The current module SPI is intentionally narrow:

- `CoreModule` and `HashModule` are builtin modules loaded by `ModuleManager`
- commands are registered during `OnLoad()` into one runtime registry
- there is no external ABI or dynamic module loading

## Consequences

The current system is safe for its small builtin command set, but its
consistency boundary remains intentionally narrow:

- correctness relies on scheduler-layer keyed serialization
- logical reads use one snapshot per command
- writes use one write batch per logical mutation
- limited multi-key semantics exist for `EXISTS` and `DEL`
- there is still no general transaction interface
- modules are builtin-only in the current implementation

Any future expansion beyond the current builtin surface should update this ADR
before claiming stronger guarantees.
