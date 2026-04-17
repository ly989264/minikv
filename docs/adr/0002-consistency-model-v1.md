# ADR 0002: Consistency Model V1

## Status

Accepted on 2026-04-17.

## Context

This ADR documents the consistency model that the current `minikv`
implementation actually provides. It is scoped to the current command set:
`PING`, `HSET`, `HGETALL`, and `HDEL`.

## Decision

The current consistency model is intentionally narrow and should be read as a
baseline contract, not as a general Redis-compatible transaction model.

### Same-Key Consistency Depends On `Scheduler` And `KeyLockTable`

For commands with a route key, correctness for same-key concurrency depends on
the shared `Scheduler` plus `KeyLockTable`:

- `HSET`, `HGETALL`, and `HDEL` all route on the user key
- worker threads acquire a striped mutex derived from that key before executing
  the command
- same-key requests therefore execute serially even when different workers pick
  them up

This means current same-key consistency is still provided by keyed execution
serialization, not by RocksDB transactions or multi-key locking.

### Logical Hash Reads Use One Snapshot Across `meta` And `hash`

Current hash reads now use RocksDB snapshots:

- `HashModule::ReadAll()` acquires one `ModuleSnapshot`
- metadata lookup and `hash` prefix scan share that same snapshot handle
- modules do not reach directly for raw iterators

This gives current hash reads a consistent multi-column-family view for one
logical hash operation.

This is still not general snapshot isolation:

- no snapshot is shared across multiple commands
- no cross-key read transaction exists
- the public API does not expose long-lived snapshots

### Writes Use `ModuleWriteBatch` But Still Depend On Same-Key Serialization

Current writes are grouped by one logical `ModuleWriteBatch`:

- `HSET` and `HDEL` build one `rocksdb::WriteBatch` per logical mutation
- the batch is committed once after the logical mutation is fully prepared

This does not remove the need for keyed serialization:

- read-modify-write flows still rely on same-key scheduler locking to avoid
  conflicting same-key updates
- there is still no multi-key atomicity

### Multi-Key Operations Have No Atomicity Guarantee

The current command set does not define any multi-key atomic operation.

Operationally:

- each command routes to at most one key
- different keys may run in parallel on different workers
- there is no mechanism for atomically reading or updating multiple keys
- any future multi-key command would need an explicit new execution contract

### Module SPI Is Builtin-Only

The current module SPI is intentionally narrow:

- `CoreModule` and `HashModule` are builtin modules loaded by `ModuleManager`
- commands are registered during `OnLoad()` into one runtime registry
- there is no external ABI or dynamic module loading

### `version` And `expire_at_ms` Are Still Reserved Fields Only

The current metadata schema contains `version` and `expire_at_ms`, but they do
not currently provide active semantics:

- new hashes are created with `version = 1`
- current write paths do not roll the version forward
- current read and write paths do not enforce TTL or expiration behavior
- current scans use whatever version is already stored in metadata

These fields should therefore be treated as reserved metadata fields in the
baseline, not as implemented versioning or expiration features.

## Consequences

The current system is safe for its small single-key hash command set, but its
consistency boundary remains intentionally narrow:

- same-key behavior relies on scheduler-layer keyed serialization
- logical hash reads use one snapshot across `meta` and `hash`
- writes use one write batch per logical mutation
- multi-key atomicity does not exist
- modules are builtin-only in the current implementation
- reserved metadata fields must not be documented as active semantics

Any future expansion beyond the current single-key hash path should update this
ADR before claiming stronger guarantees.
