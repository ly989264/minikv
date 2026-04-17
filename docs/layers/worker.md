# MiniKV Scheduler And Worker Layer

## Scope

This layer is defined by:

- `src/kernel/scheduler.h`
- `src/kernel/scheduler.cc`
- `src/worker/key_lock_table.h`
- `src/worker/key_lock_table.cc`
- `src/worker/worker.h`
- `src/worker/worker.cc`

It provides keyed execution for command requests.

## Responsibilities

The scheduler/worker split owns:

- worker thread creation and teardown
- per-worker bounded MPSC queues
- round-robin worker selection with ring probing
- striped key locking for same-key serialization
- queue-depth backpressure
- exception isolation around command execution
- scheduler metrics
- one shared execution path for both embedded and server callers

It does not own socket I/O or RocksDB schema decisions.

## Execution Model

Request admission is decoupled from correctness:

- `Scheduler` chooses a starting worker with round-robin
- if that queue is full, it probes the remaining workers once
- the first worker that accepts the task owns execution
- before `Cmd::Execute()` runs, the worker acquires one striped lock derived
  from `cmd->RouteKey()`

This gives the current prototype its main safety property:

- requests for the same key cannot execute concurrently, even if they are
  enqueued onto different workers

The shared scheduler also owns `ExecuteInline()`:

- synchronous embedded execution still goes through the same key-lock contract
- there is no separate compatibility runtime anymore

## Queueing Model

Each worker has:

- one bounded lock-free MPSC ring queue
- one consumer thread
- one condition variable used only for sleep / wakeup
- one `stopping` flag

Admission control is local to a worker queue via
`max_pending_requests_per_worker`.

If the initially selected worker queue is full:

- the scheduler probes the remaining workers once
- if every queue is full, the request is rejected with
  `Busy("worker queue full")`

## Key Lock Table

`KeyLockTable` is a fixed striped mutex table:

- stripe count is `max(64, worker_threads * 64)`
- empty route keys do not acquire a lock
- non-empty route keys hash to one stripe and hold that mutex for the full
  `Cmd::Execute()` call

## Current Design Strengths

- Same-key serialization is shared by both embedded and server execution paths.
- Different keys can scale across workers.
- The execution model is easy to reason about.
- Worker failures are converted into error responses instead of crashing the
  request path directly.
- Scheduler metrics are exposed without duplicating runtime ownership in the
  server.

## Current Design Risks

### Hot-Key Saturation

Hot keys still serialize on one striped lock and can dominate one stripe.

### Stripe Collisions

Different keys can hash to the same stripe. That is acceptable for correctness,
but it introduces avoidable contention under unlucky key distributions.

### No Cross-Key Ordering Or Atomicity

The scheduler layer makes no attempt to coordinate related requests that span
multiple keys. That is acceptable today, but it is still a hard boundary for
future command growth.

## Current Design Conclusion

The current concurrency boundary is no longer a standalone worker runtime type.
It is a shared scheduler plus worker pool, with keyed serialization still living
in explicit `KeyLockTable` locking.
