# MiniKV Worker Layer

## Scope

This layer is defined by:

- `src/kernel/scheduler.h`
- `src/kernel/scheduler.cc`
- `src/worker/key_lock_table.h`
- `src/worker/key_lock_table.cc`
- `src/worker/worker.h`
- `src/worker/worker.cc`

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
- exception isolation around command execution

## Correctness Rule

The worker path is:

1. dequeue one `Cmd`
2. acquire the striped key lock for `cmd->RouteKey()`
3. execute `Cmd::Execute(services)`
4. release the key lock
5. publish the `CommandResponse`

This means:

- same-key commands serialize
- different keys may run in parallel
- connection-local response order is preserved later by the network layer

## Current Design Conclusion

The worker layer is now exclusively shared by the network path. There is no
second execution path to keep behavior in sync with.
