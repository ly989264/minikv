# MiniKV Docs

This directory stores implementation-facing documentation for the standalone
`minikv/` project under `cancer_redis`.

Current documents:

- [build.md](./build.md): standalone build, dependency pinning, container
  verification flow, and test targets.
- [rocksdb-bundle.md](./rocksdb-bundle.md): committed RocksDB bundle layout,
  refresh flow, and commit-detection rules.
- [getting-started.md](./getting-started.md): newcomer-oriented map of the
  codebase, class responsibilities, thread model, and a step-by-step reading
  route.
- [architecture.md](./architecture.md): overall architecture audit, current
  boundaries, and major design risks.
- [layers/facade.md](./layers/facade.md): `MiniKV` public facade and ownership
  model.
- [layers/server.md](./layers/server.md): TCP server, I/O threading, RESP path,
  and connection lifecycle.
- [layers/command.md](./layers/command.md): command parsing, dispatch, and
  execution path.
- [layers/worker.md](./layers/worker.md): shared scheduler, keyed worker
  execution, queueing, and backpressure model.
- [layers/engine.md](./layers/engine.md): storage primitives, snapshots, write
  contexts, hooks, key encoding, and hash data semantics.
- [perf/baseline.md](./perf/baseline.md): baseline validation and smoke test
  workflow for the standalone project.

Suggested reading order:

1. `build.md`
2. `rocksdb-bundle.md`
3. `getting-started.md`
4. `architecture.md`
5. `layers/server.md`
6. `layers/command.md`
7. `layers/worker.md`
8. `layers/engine.md`

The docs are intentionally based on the current code in `minikv/`, not on a
future target architecture.
