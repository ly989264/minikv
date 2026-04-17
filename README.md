# MiniKV

`minikv` is a small Redis-like prototype built on RocksDB. It currently focuses
on a narrow hash-only command surface, exposes a network-only runtime, and is
maintained here as a standalone project under `cancer_redis/minikv`.

## Quick Start

Build with the committed RocksDB bundle and vendored googletest:

```bash
./tools/build_linux.sh
```

If you have a local RocksDB checkout and want to refresh the committed bundle
first:

```bash
./tools/build_linux.sh \
  --rocksdb-source-dir /path/to/rocksdb \
  --rocksdb-reuse-build-dir /path/to/rocksdb/build-minikv
```

The first form avoids downloading and recompiling RocksDB. The second form
checks the local RocksDB commit against the committed bundle metadata and only
refreshes the bundle when the source commit changed.

## Docs

- [docs/README.md](./docs/README.md): documentation index
- [docs/build.md](./docs/build.md): build and test workflow
- [docs/rocksdb-bundle.md](./docs/rocksdb-bundle.md): committed RocksDB bundle
  layout, update flow, and commit-detection rules
- [docs/getting-started.md](./docs/getting-started.md): codebase walkthrough

## Project Layout

- `src/config.h`, `src/minikv.h`, `src/network/network_server.h`: runtime and
  network entry headers
- `src/`: implementation
- `tests/`: unit and integration tests
- `tools/`: build, smoke, and maintenance scripts
- `third_party/rocksdb/linux-x86_64/`: committed RocksDB headers, shared
  library, and bundle metadata for Linux container builds
