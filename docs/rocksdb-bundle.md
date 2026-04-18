# RocksDB Bundle

## Purpose

`minikv` keeps a committed Linux RocksDB bundle under
`third_party/rocksdb/linux-x86_64/` so the normal Linux-container workflow does
not need to fetch RocksDB source or rebuild RocksDB from scratch every session.

The bundle contains only the pieces `minikv` needs to compile and run:

- public headers under `include/rocksdb/`
- a shared library plus soname symlinks under `lib/`
- commit and build metadata in `BUNDLE_INFO.env`

## Bundle Layout

Expected files:

- `third_party/rocksdb/linux-x86_64/include/rocksdb/...`
- `third_party/rocksdb/linux-x86_64/lib/librocksdb.so`
- `third_party/rocksdb/linux-x86_64/lib/librocksdb.so.<soname>`
- `third_party/rocksdb/linux-x86_64/lib/librocksdb.so.<full-version>`
- `third_party/rocksdb/linux-x86_64/BUNDLE_INFO.env`

`BUNDLE_INFO.env` records:

- `ROCKSDB_SOURCE_COMMIT`
- `ROCKSDB_SOURCE_DESCRIBE`
- `ROCKSDB_LIBRARY_REALNAME`
- `ROCKSDB_LIBRARY_SONAME`
- `ROCKSDB_BUNDLE_KIND`
- `ROCKSDB_BUNDLE_PLATFORM`
- `ROCKSDB_BUNDLE_CREATED_AT`

The manifest is the source of truth for the committed bundle. It is separate
from the fallback `MINIKV_ROCKSDB_TAG` used by `FetchContent`.

## Status And Refresh

Check bundle status:

```bash
./tools/sync_rocksdb_bundle.sh --status
./tools/sync_rocksdb_bundle.sh \
  --status \
  --rocksdb-source-dir /path/to/rocksdb
```

Refresh the bundle from a local RocksDB checkout:

```bash
./tools/sync_rocksdb_bundle.sh \
  --rocksdb-source-dir /path/to/rocksdb
```

If you already have an up-to-date RocksDB shared build, reuse it instead of
rebuilding:

```bash
./tools/sync_rocksdb_bundle.sh \
  --rocksdb-source-dir /path/to/rocksdb \
  --reuse-build-dir /path/to/rocksdb/build-minikv
```

The script compares `git rev-parse HEAD` from the supplied RocksDB checkout
with `ROCKSDB_SOURCE_COMMIT` in `BUNDLE_INFO.env`.

- if the commit matches and the bundle files exist, the script exits without
  rebuilding
- if the commit differs, the script refreshes the committed headers, shared
  library, symlinks, and manifest

Do not edit bundled RocksDB headers or libraries by hand. Refresh them through
`tools/sync_rocksdb_bundle.sh`.

## Build Behavior

`cmake/Dependencies.cmake` prefers the committed bundle when:

- `MINIKV_USE_BUNDLED_ROCKSDB=ON`
- the current build is Linux on `x86_64` or `amd64`
- `MINIKV_ROCKSDB_BUNDLE_DIR` contains the expected headers and shared library

When that path is available, CMake creates an imported shared-library target
and sets the runtime rpath so `minikv_server` and the tests can find the
bundled `librocksdb.so` inside the repository.

`tools/build_linux.sh` follows this rule:

- if `--rocksdb-source-dir` is not provided and the bundle exists, build
  against the committed bundle
- if `--rocksdb-source-dir` is provided, refresh the bundle only when the
  source commit changed, then build against the bundle
- if the bundle is incomplete and no source dir is provided, fail fast instead
  of silently downloading dependencies
- if the bundle is missing but a source dir is provided, fall back to the
  source-based path

## Commit Policy

When you intentionally change the RocksDB dependency that `minikv` should
track:

1. Refresh the bundle.
2. Commit the updated headers, shared library, symlinks, and `BUNDLE_INFO.env`.
3. Mention the source commit or describe string in the commit message or PR
   summary.
