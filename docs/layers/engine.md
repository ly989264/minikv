# MiniKV Storage And Type Kernel

## Scope

This layer is currently defined by:

- `src/kernel/storage_engine.h`
- `src/kernel/storage_engine.cc`
- `src/kernel/snapshot.h`
- `src/kernel/snapshot.cc`
- `src/kernel/write_context.h`
- `src/kernel/write_context.cc`
- `src/kernel/mutation_hook.h`
- `src/types/hash/hash_module.h`
- `src/types/hash/hash_module.cc`
- `src/engine/key_codec.h`
- `src/engine/key_codec.cc`
- `src/engine/db_engine.h`
- `src/engine/db_engine.cc`

It owns RocksDB integration, read/write helper primitives, and the current
typed hash model.

## Responsibilities

### `StorageEngine`

Owns:

- opening RocksDB
- ensuring required column families exist
- exposing primitive `Get`, `Put`, `Delete`, `Write`
- creating RocksDB-backed `Snapshot` objects

It does not own hash semantics.

### `Snapshot`

Owns:

- one pinned RocksDB snapshot
- `Get()` across one column family
- `ScanPrefix()` across one column family

It exists so logical hash reads can share one consistent view across `meta` and
`hash`.

### `WriteContext`

Owns:

- one `rocksdb::WriteBatch`
- `Put` / `Delete` collection for one logical mutation
- one final `Commit()`

It exists so logical writes and future hooks share one batch boundary.

### `MutationHook`

Owns:

- the interface for future logical mutation side effects
- the current `HashMutation` description type

Current behavior:

- only `NoopMutationHook` exists
- no Search or `FT.*` behavior is attached

### `HashModule`

Owns current hash semantics:

- `PutField()`
- `ReadAll()`
- `DeleteFields()`

Design rules:

- reads use `Snapshot`
- writes use `WriteContext`
- hook call sites run before commit

### `DBEngine`

`src/engine/db_engine.h` now exists only as a compatibility alias:

- `using DBEngine = StorageEngine`

The active implementation path should be read through `StorageEngine`, not as a
separate semantic layer.

## Column Family Model

The storage engine opens:

- `default`
- `meta`
- `hash`

The effective data model is carried by `meta` and `hash`.

`meta` stores one metadata record per logical user key.

`hash` stores field/value records for one logical hash object, using a prefix
that includes the user key and metadata version.

## Key Encoding Model

`KeyCodec` provides three important encodings:

- meta key
- hash data prefix
- hash data key

The encoding shape remains prefix-oriented so the hash module can:

- fetch metadata by exact key
- scan all fields for one hash by prefix iteration

Metadata currently stores:

- `type`
- `encoding`
- `version`
- `size`
- `expire_at_ms`

## Current Hash Semantics

### `PutField`

- read metadata through `Snapshot`
- reject type mismatch
- probe field existence through the same `Snapshot`
- adjust `size` only when the field is newly inserted
- write metadata and field value in one `WriteContext`
- invoke the mutation hook before commit

### `ReadAll`

- read metadata through `Snapshot`
- reject type mismatch
- derive the hash prefix from user key and version
- scan all matching field/value entries through the same `Snapshot`

### `DeleteFields`

- read metadata through `Snapshot`
- reject type mismatch
- probe each requested field through the same `Snapshot`
- delete matching fields in one `WriteContext`
- update `size` or remove metadata in that same `WriteContext`
- invoke the mutation hook before commit

## Current Design Strengths

- Typed metadata exists from the start instead of retrofitting later.
- The hash prefix layout works naturally with RocksDB iteration.
- `Snapshot` separates read consistency from scheduler locking.
- `WriteContext` gives future side effects one shared batch boundary.
- `HashModule` keeps hash semantics out of the storage primitive layer.

## Current Design Risks

### Future Fields Without Current Semantics

`version` and `expire_at_ms` are stored, but current command execution does not
advance version or enforce expiration.

### Single-Key Assumption

Hash module methods are still designed around one logical key at a time. That
matches the current scheduler model but becomes restrictive if the command
surface grows toward cross-key behavior.

### Hook Surface Is Ahead Of Hook Behavior

The mutation hook interface exists, but the active implementation is a no-op.
That is deliberate for this phase, but the interface should stay minimal until
real secondary-write behavior exists.

## Current Design Conclusion

The old combined engine boundary is now split into a smaller storage primitive
layer and a separate typed hash semantics layer. That split is the main kernel
preparation work for future features such as Search.
