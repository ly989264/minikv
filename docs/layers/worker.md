# MiniKV Worker Layer

## Scope

This layer is defined by:

- `src/execution/scheduler/scheduler.h`
- `src/execution/scheduler/scheduler.cc`
- `src/execution/worker/key_lock_table.h`
- `src/execution/worker/key_lock_table.cc`
- `src/execution/worker/worker.h`
- `src/execution/worker/worker.cc`

It owns queueing, worker fan-out, keyed serialization, and backpressure.

## Responsibilities

`Scheduler` owns:

- one worker thread per configured worker
- one bounded MPSC queue per worker
- a shared `KeyLockTable`
- round-robin plus probe admission control
- queue depth, inflight, and rejection metrics

`Worker` owns:

- one consumer thread
- one bounded queue
- lock-plan-driven command execution
- exception isolation around command execution

## Queueing Model

Each worker owns one `BoundedMPSCQueue`:

- producers are the network I/O threads calling `Scheduler::Submit()`
- the single consumer is the worker thread
- queue capacity is normalized to a power of two
- `Scheduler` probes worker queues until it finds one with capacity

If every worker queue is full, submission fails with `Busy("worker queue full")`
and the scheduler increments its rejection counter.

## Keyed Locking

`KeyLockTable` owns a striped set of mutexes.

Current behavior:

- stripe count defaults to `max(64, worker_count * 64)`
- single-key commands lock one stripe derived from the route key
- multi-key commands compute all stripe indexes, sort them, deduplicate them,
  and lock them in stable order

The stable multi-key order avoids deadlock when different workers process
different multi-key commands concurrently.

## Correctness Rule

The worker path is:

1. dequeue one `Cmd`
2. acquire the locks described by `cmd->lock_plan()`
3. execute `Cmd::Execute()`
4. release the locks
5. publish the `CommandResponse`

This means:

- same-key commands serialize
- different keys may run in parallel
- current multi-key commands such as `EXISTS` and `DEL` hold all required
  stripes for the duration of one command
- connection-local response order is preserved later by the network layer

`Worker` also catches synchronous exceptions and converts them into
`rocksdb::Status::Aborted(...)` responses instead of crashing the process.

## Current Design Conclusion

The worker layer is shared by the network path and only by the network path.
There is no second execution path to keep behavior in sync with.
