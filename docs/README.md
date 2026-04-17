# MiniKV Docs

This directory stores implementation-facing documentation for the standalone
`minikv/` project under `cancer_redis`.

Current documents:

- [build.md](./build.md): standalone build, dependency pinning, container
  verification flow, and test targets.
- [rocksdb-bundle.md](./rocksdb-bundle.md): committed RocksDB bundle layout,
  refresh flow, and commit-detection rules.
- [getting-started.md](./getting-started.md): newcomer-oriented map of the
  current network-only codebase and reading order.
- [module-lifecycle.md](./module-lifecycle.md): builtin module lifecycle,
  ordering, and rollback rules.
- [module-services.md](./module-services.md): builtin module service facade
  boundary.
- [hash-module-integration.md](./hash-module-integration.md): how hash commands
  now register and execute as a builtin module.
- [architecture.md](./architecture.md): overall architecture, current
  boundaries, and implementation risks.
- [layers/runtime.md](./layers/runtime.md): `MiniKV` runtime ownership model.
- [layers/network.md](./layers/network.md): TCP networking, RESP parsing,
  connection lifecycle, and metrics.
- [layers/command.md](./layers/command.md): command creation, validation, and
  execution path.
- [layers/worker.md](./layers/worker.md): scheduler, keyed worker execution,
  queueing, and backpressure model.
- [layers/codec.md](./layers/codec.md): key encoding, metadata layout, and hash
  storage representation.
- [perf/baseline.md](./perf/baseline.md): baseline validation and smoke test
  workflow for the standalone project.

Suggested reading order:

1. `build.md`
2. `rocksdb-bundle.md`
3. `getting-started.md`
4. `module-lifecycle.md`
5. `module-services.md`
6. `hash-module-integration.md`
7. `architecture.md`
8. `layers/runtime.md`
9. `layers/network.md`
10. `layers/command.md`
11. `layers/worker.md`
12. `layers/codec.md`

The docs are intentionally based on the current code in `minikv/`, not on a
future target architecture.
