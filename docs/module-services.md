# MiniKV Module Services

`ModuleServices` is the only supported kernel-facing surface for builtin
modules. Modules must not reach around it for private runtime objects such as
`StorageEngine`, `Snapshot`, `WriteContext`, `Scheduler`, or
`BackgroundExecutor`.

Current services are:

- `command_registry`
- `exports`
- `storage`
- `snapshot`
- `background`
- `scheduler`
- `namespace`
- `metrics`

## `command_registry`

Type: `ModuleCommandRegistry`

Responsibilities:

- register module commands into the shared runtime registry
- stamp each registration with the owning module name
- enforce builtin command source metadata
- reject registration outside `OnLoad()`
- surface command-name conflicts before startup succeeds

Conflicts are case-insensitive because command names are normalized before
insertion.

## `exports`

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
- the current hash bridge publishes `hash.indexing_bridge` during `OnLoad()`
- consumers resolve and bind exports during `OnStart()`
- lookup is typed, so providers must publish the interface type they intend
  consumers to request

## `storage`

Type: `ModuleStorage`

Responsibilities:

- create explicit `ModuleKeyspace` values such as `search.docs`
- create module-scoped write batches through `ModuleWriteBatch`
- expose keyspace-aware `Put()`, `Delete()`, and `Commit()` without leaking
  `WriteContext`

Modules can mutate storage through this service, but they cannot access the raw
kernel write path directly. Module-private state now goes through the shared
RocksDB `module` column family rather than having modules bind themselves to the
global `StorageColumnFamily` layout.

`ModuleKeyspace` is the storage-side companion to `ModuleNamespace`:

- `ModuleNamespace` identifies the owning module for commands, exports, and
  metrics
- `ModuleKeyspace` identifies one module-owned storage subspace inside the
  shared `module` column family
- one module may own multiple keyspaces such as `search.docs` and
  `search.postings`
- keyspace-aware APIs encode the module name plus the local keyspace name into
  one storage prefix, so modules do not need to know how the kernel maps that
  onto RocksDB

Current hash module behavior is intentionally unchanged. Hash data still uses
the existing `meta` and `hash` column families, while new module-private state
should use `ModuleKeyspace`.

Compatibility note:

- `ModuleWriteBatch` and `ModuleSnapshot` still expose raw
  `StorageColumnFamily` helpers for the existing hash path
- this is a temporary compatibility bridge for `HashModule` and current hash
  observers
- follow-up work should migrate remaining module callers onto
  keyspace-aware-only storage APIs before the raw-CF entrypoints are retired

Current hash observer behavior also depends on this boundary:

- `HashObserver::OnHashMutation()` receives the same `ModuleWriteBatch` as the
  base hash write
- observers may append `Put()` and `Delete()` operations to that batch
- `HashModule` still owns `Commit()`
- if any observer returns an error, the shared batch is never committed and the
  base write fails as a whole

## `snapshot`

Type: `ModuleSnapshotService`

Responsibilities:

- create read-only `ModuleSnapshot` views
- expose consistent keyspace-aware `Get()` and `ScanPrefix()` helpers without
  leaking raw `Snapshot`
- create `ModuleIterator` objects for `seek`, `next`, `valid`, `key`, `value`,
  and `status` iteration inside one `ModuleKeyspace`

This is the supported way for modules to perform multi-read logical operations.

`ModuleIterator` only surfaces keys from the requested keyspace. Its `key()`
value is the local module key, not the internal encoded RocksDB key.

## `background`

Type: `ModuleBackgroundService`

Responsibilities:

- submit module-owned maintenance work onto the shared background executor
- keep modules from depending on private runtime thread ownership
- provide a minimal async hook for future search indexing work without exposing
  a general-purpose thread-pool API

Current execution model:

- one process-wide `BackgroundExecutor`
- one background thread
- FIFO task execution
- no module-visible queue tuning, worker pools, or scheduler bypass

Known limitation:

- this executor is intentionally minimal for PR-B
- there is still no cancellation, prioritization, per-module quota, or
  structured task-failure reporting
- future sessions should only expand it when a concrete module use case proves
  the need

## `scheduler`

Type: `ModuleSchedulerView`

Responsibilities:

- expose worker count
- expose in-memory scheduler metrics

Current scheduler service is intentionally read-only. Modules cannot use it to
submit background tasks or bypass the normal request execution path. Background
work now goes through the separate `background` service instead.

## `namespace`

Type: `ModuleNamespace`

Responsibilities:

- describe module identity
- qualify local names such as metric keys

Current namespace semantics are about module identity only. They do not rewrite
user keys and do not add storage prefixes. Module storage prefixes now live in
`ModuleKeyspace`, not in `ModuleNamespace`.

## `metrics`

Type: `ModuleMetrics`

Responsibilities:

- store module-local in-memory counters
- qualify keys with the module namespace

There is no metrics export endpoint yet. These counters currently exist to give
modules a safe internal metrics surface without exposing the whole runtime.

## Current Boundary

In this phase, `ModuleServices` is the stability boundary for builtin modules.
Anything outside it is treated as private kernel implementation detail.
