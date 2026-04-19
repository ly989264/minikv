# MiniKV Docs

This directory stores implementation-facing documentation for the standalone
`minikv/` project in this workspace. These documents are descriptive: they
track the current builtin-module, RESP-server implementation in `src/`, rather
than a future target architecture.

Current documents:

- [build.md](./build.md): standalone build, dependency selection, container
  workflow, and validation entrypoints
- [rocksdb-bundle.md](./rocksdb-bundle.md): committed RocksDB bundle layout,
  refresh flow, and bundle-vs-fetch behavior
- [getting-started.md](./getting-started.md): newcomer-oriented map of the
  current server, runtime, and module layers
- [module-lifecycle.md](./module-lifecycle.md): builtin module lifecycle,
  startup windows, rollback, and shutdown ordering
- [module-services.md](./module-services.md): builtin module service facade and
  storage/export boundaries
- [hash-module-integration.md](./hash-module-integration.md): how the hash
  module registers commands, handles delete/tombstone flows, and notifies
  observers
- [architecture.md](./architecture.md): overall architecture, request flow,
  storage model, and current design tensions
- [architecture/current-layering.md](./architecture/current-layering.md):
  current source-tree layering and intentional cross-layer couplings
- [layers/runtime.md](./layers/runtime.md): `MiniKV` runtime ownership model
- [layers/network.md](./layers/network.md): TCP networking, RESP parsing,
  connection lifecycle, response ordering, and metrics
- [layers/command.md](./layers/command.md): command registration, creation,
  validation, and lock-plan derivation
- [layers/worker.md](./layers/worker.md): scheduler, worker queues, keyed
  locking, and backpressure
- [layers/codec.md](./layers/codec.md): key encodings, metadata ownership, and
  hash/type-specific/module storage prefixes
- [perf/baseline.md](./perf/baseline.md): baseline validation and smoke-test
  workflow for the standalone project
- [adr/0001-current-minikv-boundaries.md](./adr/0001-current-minikv-boundaries.md):
  current compatibility boundary
- [adr/0002-consistency-model-v1.md](./adr/0002-consistency-model-v1.md):
  current consistency contract

Suggested reading order:

1. `build.md`
2. `rocksdb-bundle.md`
3. `getting-started.md`
4. `module-lifecycle.md`
5. `module-services.md`
6. `hash-module-integration.md`
7. `architecture.md`
8. `architecture/current-layering.md`
9. `layers/runtime.md`
10. `layers/network.md`
11. `layers/command.md`
12. `layers/worker.md`
13. `layers/codec.md`
14. `perf/baseline.md`
15. `adr/0001-current-minikv-boundaries.md`
16. `adr/0002-consistency-model-v1.md`
