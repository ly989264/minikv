# MiniKV Module Services

`ModuleServices` is the only supported kernel-facing surface for builtin
modules. Modules must not reach around it for private runtime objects such as
`StorageEngine`, `Snapshot`, `WriteContext`, `Scheduler`, or
`BackgroundExecutor`.

Current service accessors are:

- `command_registry()`
- `exports()`
- `storage()`
- `snapshot()`
- `background()`
- `scheduler()`
- `name_space()`
- `metrics()`

## `command_registry()`

Type: `ModuleCommandRegistry`

Responsibilities:

- register module commands into the shared runtime registry
- stamp each registration with the owning module name
- force `CommandSource::kBuiltin`
- reject registration outside `OnLoad()`
- surface command-name conflicts before startup succeeds

Conflicts are case-insensitive because command names are normalized before
insertion.

## `exports()`

Type: `ModuleExportRegistry`

Responsibilities:

- publish typed module-owned exports into the shared module registry
- qualify local export names with the owning module namespace
- allow typed lookup by other modules without sharing private implementation
  pointers
- reject publish calls made outside the module startup window
- clear published exports during rollback and module shutdown

Current rules:

- providers publish during `OnLoad()` or `OnStart()`
- the current core exports publish `core.key_service` and
  `core.whole_key_delete_registry`
- the current hash bridge publishes `hash.indexing_bridge`
- consumers usually resolve and bind exports during `OnStart()`
- lookup is typed, so providers must publish the interface type they intend
  consumers to request

## `storage()`

Type: `ModuleStorage`

Responsibilities:

- create explicit `ModuleKeyspace` values such as `search.docs`
- create module-scoped write batches through `ModuleWriteBatch`
- expose keyspace-aware `Put()`, `Delete()`, and `Commit()` without leaking
  `WriteContext`

Modules can mutate storage through this service, but they cannot access the raw
kernel write path directly. Keyspace-aware writes go through the module's
default `StorageColumnFamily` rather than hard-coding one global target.
Builtin type modules bind their keyspaces to dedicated type-specific column
families, while auxiliary module state stays in the shared RocksDB `module`
column family.

`ModuleKeyspace` is the storage-side companion to `ModuleNamespace`:

- `ModuleNamespace` identifies the owning module for commands, exports, and
  metrics
- `ModuleKeyspace` identifies one module-owned storage subspace inside the
  owning module's default column family
- one module may own multiple keyspaces such as `search.docs` and
  `search.postings`
- keyspace-aware APIs encode the module name plus the local keyspace name into
  one storage prefix, so modules do not need to know how the kernel maps that
  onto RocksDB

Current hash module behavior is intentionally unchanged. Hash data still uses
the existing `meta` and `hash` column families, while keyspace-aware APIs are
used by string/list/set/zset/stream/json/geo storage plus new auxiliary
module-private state.

Compatibility note:

- `ModuleWriteBatch` and `ModuleSnapshot` still expose raw
  `StorageColumnFamily` helpers for the existing hash path
- this is a compatibility bridge for `HashModule` and current hash observers
- new module-private code should prefer the keyspace-aware APIs

Current hash observer behavior also depends on this boundary:

- `HashObserver::OnHashMutation()` receives the same `ModuleWriteBatch` as the
  base hash write
- observers may append `Put()` and `Delete()` operations to that batch
- `HashModule` still owns `Commit()`
- if any observer returns an error, the shared batch is never committed and the
  base write fails as a whole

## `snapshot()`

Type: `ModuleSnapshotService`

Responsibilities:

- create read-only `ModuleSnapshot` views
- expose consistent keyspace-aware `Get()` and `ScanPrefix()` helpers without
  leaking raw `Snapshot`
- create `ModuleIterator` objects for `Seek`, `Next`, `Valid`, `key`, `value`,
  and `status` iteration inside one `ModuleKeyspace`

This is the supported way for modules to perform multi-read logical operations.

`ModuleIterator` only surfaces keys from the requested keyspace. Its `key()`
value is the decoded local module key, not the internal encoded RocksDB key.

## `background()`

Type: `ModuleBackgroundService`

Responsibilities:

- submit module-owned maintenance work onto the shared background executor
- keep modules from depending on private runtime thread ownership
- provide a minimal async hook for future maintenance or indexing work without
  exposing a general-purpose thread-pool API

Current execution model:

- one process-wide `BackgroundExecutor`
- one background thread
- FIFO task execution
- no module-visible queue tuning, worker pools, or scheduler bypass

Known limitation:

- there is still no cancellation, prioritization, per-module quota, or
  structured task-failure reporting

## `scheduler()`

Type: `ModuleSchedulerView`

Responsibilities:

- expose worker count
- expose in-memory scheduler metrics

Current scheduler service is intentionally read-only. Modules cannot use it to
submit background tasks or bypass the normal request execution path. Background
work goes through the separate `background` service instead.

## `name_space()`

Type: `ModuleNamespace`

Responsibilities:

- describe module identity
- qualify local names such as export names and metric keys

Current namespace semantics are about module identity only. They do not rewrite
user keys and do not add storage prefixes. Module storage prefixes live in
`ModuleKeyspace`, not in `ModuleNamespace`.

## `metrics()`

Type: `ModuleMetrics`

Responsibilities:

- store module-local in-memory counters
- qualify keys with the module namespace

There is no metrics export endpoint yet. These counters currently exist to give
modules a safe internal metrics surface without exposing the whole runtime.

## Current Boundary

In this phase, `ModuleServices` is the stability boundary for builtin modules.
Anything outside it is treated as private kernel implementation detail.
