# RocksDB Bundle

## Purpose

`minikv` keeps a committed Linux RocksDB bundle under
`third_party/rocksdb/linux-x86_64/` so normal builds do not need to fetch
RocksDB source or rebuild RocksDB from scratch every session.

The bundle contains only the pieces `minikv` needs to compile and run:

- public headers under `include/rocksdb/`
- a stripped shared library under `lib/`
- commit and build metadata in `BUNDLE_INFO.env`

## Bundle Layout

Expected files:

- `third_party/rocksdb/linux-x86_64/include/rocksdb/...`
- `third_party/rocksdb/linux-x86_64/lib/librocksdb.so`
- `third_party/rocksdb/linux-x86_64/lib/librocksdb.so.9`
- `third_party/rocksdb/linux-x86_64/lib/librocksdb.so.<version>`
- `third_party/rocksdb/linux-x86_64/BUNDLE_INFO.env`

`BUNDLE_INFO.env` records:

- `ROCKSDB_SOURCE_COMMIT`
- `ROCKSDB_SOURCE_DESCRIBE`
- `ROCKSDB_LIBRARY_REALNAME`
- `ROCKSDB_LIBRARY_SONAME`
- `ROCKSDB_BUNDLE_KIND`
- `ROCKSDB_BUNDLE_PLATFORM`
- `ROCKSDB_BUNDLE_CREATED_AT`

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
- if the commit differs, it refreshes the committed headers and shared library

## Build Behavior

`CMakeLists.txt` prefers the committed bundle when:

- `MINIKV_USE_BUNDLED_ROCKSDB=ON`
- `MINIKV_ROCKSDB_BUNDLE_DIR` contains the expected headers and shared library

`tools/build_linux.sh` follows this rule:

- if `--rocksdb-source-dir` is not provided and the bundle exists, build against
  the committed bundle
- if `--rocksdb-source-dir` is provided, refresh the bundle only when the source
  commit changed, then build against the bundle
- if the bundle does not exist, fall back to the older source/fetch path

## Commit Policy

When you intentionally change the RocksDB dependency that `minikv` should track:

1. Refresh the bundle.
2. Commit the updated headers, shared library, symlinks, and `BUNDLE_INFO.env`.
3. Mention the source commit or describe string in the commit message or PR
   summary.
