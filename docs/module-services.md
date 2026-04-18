# MiniKV Module Services

`ModuleServices` is the only supported kernel-facing surface for builtin
modules. Modules must not reach around it for private runtime objects such as
`StorageEngine`, `Snapshot`, `WriteContext`, or `Scheduler`.

Current services are:

- `command_registry`
- `exports`
- `storage`
- `snapshot`
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

- create module-scoped write batches through `ModuleWriteBatch`
- expose `Put`, `Delete`, and `Commit` without leaking `WriteContext`

Modules can mutate storage through this service, but they cannot access the raw
kernel write path directly.

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
- expose consistent `Get` and `ScanPrefix` helpers without leaking raw
  `Snapshot`

This is the supported way for modules to perform multi-read logical operations.

## `scheduler`

Type: `ModuleSchedulerView`

Responsibilities:

- expose worker count
- expose in-memory scheduler metrics

Current scheduler service is intentionally read-only. Modules cannot use it to
submit background tasks or bypass the normal request execution path.

## `namespace`

Type: `ModuleNamespace`

Responsibilities:

- describe module identity
- qualify local names such as metric keys

Current namespace semantics are about module identity only. They do not rewrite
user keys and do not add storage prefixes.

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
