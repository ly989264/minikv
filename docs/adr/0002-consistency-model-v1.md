# ADR 0002: Consistency Model V1

## Status

Accepted on 2026-04-18. Updated to match the current builtin-module command
surface.

## Context

This ADR documents the consistency model that the current `minikv`
implementation actually provides. It is scoped to the current builtin command
surface loaded by `ModuleManager`: core lifecycle commands plus string, bitmap,
hash, JSON, list, set, sorted-set, geo, and stream commands.

## Decision

The current consistency model is intentionally narrow and should be read as a
baseline contract, not as a general Redis-compatible transaction model.

### Keyed Consistency Depends On `Scheduler` And `KeyLockTable`

For commands with a lock plan, correctness depends on the shared `Scheduler`
plus `KeyLockTable`:

- `PING` has no key lock.
- most single-key commands acquire one logical key lock, including `TYPE`,
  `EXPIRE`, `TTL`, `PTTL`, `PERSIST`, string/bitmap commands, hash commands,
  JSON single-key commands, list commands, set commands, zset commands, geo
  commands, and single-key stream commands.
- multi-key commands acquire a canonicalized set of stripe locks in stable
  order. Current multi-key commands include `EXISTS`, `DEL`, `MGET`, `MSET`,
  `JSON.MGET`, and `XREAD`.

Requests that overlap on the same protected stripes therefore serialize even
when different workers pick them up. This is the current consistency mechanism;
it does not rely on RocksDB transactions.

### Lock Plans Do Not Rewrite Command Semantics

`Cmd::LockPlan` sorts and deduplicates route keys only for lock acquisition.
Command implementations keep their own argument semantics:

- `EXISTS key [key ...]` preserves duplicate-key counting semantics.
- `DEL key [key ...]` deduplicates deletion work so one key is deleted at most
  once per command.
- `MGET` preserves requested key order in its reply.
- `MSET` applies key/value pairs atomically and the last repeated key wins.
- `JSON.MGET` preserves requested key order in its reply.
- `XREAD` preserves requested stream order in its reply.

### Logical Reads Use Command-Local Snapshots

Logical reads use `ModuleSnapshot` objects backed by RocksDB snapshots:

- core lookup-based commands create a `ModuleSnapshot` for metadata reads.
- type-module reads create snapshots that cover metadata and type-specific
  data reads for that logical module operation.
- `XREAD` uses one snapshot while reading all requested stream keys.
- some multi-key reads, such as `JSON.MGET`, call a per-key module read while
  the command holds the relevant key locks.

This gives each logical read operation a stable view of the rows it reads, and
key locks prevent overlapping writes from changing the protected keys during
the command.

This is still not general snapshot isolation:

- no snapshot is shared across multiple client commands.
- the public API does not expose long-lived snapshots.
- there is no cross-connection transaction model.

### Writes Use One Write Batch Per Logical Mutation

Current write paths are grouped through `ModuleWriteBatch`:

- single-key mutations such as `SET`, `HSET`, `JSON.SET`, `LPUSH`, `SADD`,
  `ZADD`, `GEOADD`, and `XADD` stage metadata and data changes in one logical
  batch.
- `DEL` builds one shared batch across the targeted keys in that command.
- `EXPIRE`, `PERSIST`, zero-or-negative `EXPIRE`, and whole-key delete flows
  stage their metadata and type-data changes through one logical batch.
- zset writes notify zset observers before commit, so geo sidecar updates can
  join the same batch.
- hash writes notify hash observers before commit, so observer-side changes can
  join the same batch.

The batch is committed once after the logical mutation is fully prepared. This
does not remove the need for keyed serialization: read-modify-write flows still
rely on scheduler locking to avoid conflicting updates.

### Multi-Key Support Is Limited But Real

The current command set contains limited multi-key operations:

- `EXISTS key [key ...]` locks the targeted keys and reads metadata for each
  key.
- `DEL key [key ...]` locks the targeted keys, reads live metadata, dispatches
  whole-key delete handlers, stages all deletes and tombstone writes in one
  batch, and commits once.
- `JSON.MGET key [key ...] path` locks the targeted keys and returns per-key
  JSON reads in request order.
- `XREAD STREAMS key [key ...] id [id ...]` locks the targeted stream keys and
  reads entries newer than the supplied IDs.

Current caveats:

- there is still no general user-facing transaction interface.
- there is still no arbitrary multi-command atomicity.
- multi-key consistency is limited to commands that derive a multi-key lock
  plan during `Cmd::Init()`.

### TTL, Tombstones, And Versioning Are Active Semantics

The current metadata schema fields `version` and `expire_at_ms` are active:

- `EXPIRE` writes TTL metadata for live keys.
- `TTL` and `PTTL` interpret missing, live, expired, and tombstoned states.
- zero-or-negative `EXPIRE` routes through whole-key delete.
- `DEL` and whole-key delete write tombstones for live typed values.
- deleting the final field/member/entry in type modules that support shrinking
  to empty writes tombstone metadata where that module's semantics require it.
- recreating an expired or tombstoned typed value bumps the metadata version.
- versioned row layouts, including hash, list, set, zset, stream, and geo
  sidecar storage, use the metadata version to keep stale rows from earlier
  incarnations unreachable.

These fields are implemented behavior, not reserved placeholders.

### Module SPI Is Builtin-Only

The current module SPI is intentionally narrow:

- builtin modules are compiled into the binary and loaded by `ModuleManager`.
- commands are registered during `OnLoad()` into one runtime registry.
- modules publish typed source-level exports such as `core.key_service`,
  `core.whole_key_delete_registry`, `string.bridge`, `hash.indexing_bridge`,
  and `zset.bridge`.
- there is no external ABI or dynamic module loading.

## Consequences

The current system is safe for its builtin command set, but its consistency
boundary remains intentionally narrow:

- correctness relies on scheduler-layer keyed serialization.
- logical reads use command-local snapshots.
- writes use one write batch per logical mutation.
- limited multi-key semantics exist for `EXISTS`, `DEL`, `MGET`, `MSET`,
  `JSON.MGET`, and `XREAD`.
- there is still no general transaction interface.
- modules are builtin-only in the current implementation.

Any future expansion beyond the current builtin surface should update or
supersede this ADR before claiming stronger guarantees.
