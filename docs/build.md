# MiniKV Build Notes

## Scope

This document records how the standalone `minikv` project is configured, built,
and verified under `cancer_redis/minikv`.

## Standalone Build Layout

`minikv/CMakeLists.txt` defines an independent CMake project with these
targets:

- `minikv_core`: static library containing public headers, internal headers,
  and implementation sources under `src/`
- `minikv_server`
- `minikv_cmd_test`
- `minikv_command_registry_test`
- `minikv_hash_test`
- `minikv_hash_module_test`
- `minikv_reply_encode_test`
- `minikv_scheduler_test`
- `minikv_server_test`
- `minikv_snapshot_test`

All targets build as C++17.

## Dependency Strategy

Default dependency mode prefers the committed RocksDB bundle:

- `MINIKV_USE_BUNDLED_ROCKSDB=ON`
- `MINIKV_ROCKSDB_BUNDLE_DIR=third_party/rocksdb/linux-x86_64`
- `MINIKV_FETCH_DEPS=OFF` when the bundle is complete
- `MINIKV_GTEST_SOURCE_DIR=third_party/googletest`
- `MINIKV_FETCH_GTEST=OFF` when the vendored googletest tree is present
- `MINIKV_ROCKSDB_TAG=v11.0.4`
- `MINIKV_GTEST_TAG=v1.14.0`

When the bundle is present, `minikv` links against the committed
`third_party/rocksdb/linux-x86_64/lib/librocksdb.so` and uses the committed
headers under `third_party/rocksdb/linux-x86_64/include/rocksdb`. Tests prefer
the vendored googletest tree under `third_party/googletest/` and only fall back
to `FetchContent` when that tree is unavailable.

If the bundle is missing, CMake falls back to either:

- `MINIKV_ROCKSDB_SOURCE_DIR`
- `FetchContent` using `MINIKV_ROCKSDB_TAG`

Pinned RocksDB fallback build settings:

- `ROCKSDB_BUILD_SHARED=OFF`
- `WITH_TESTS=OFF`
- `WITH_TOOLS=OFF`
- `WITH_CORE_TOOLS=OFF`
- `WITH_BENCHMARK_TOOLS=OFF`
- `WITH_GFLAGS=OFF`
- `WITH_LIBURING=OFF`
- `WITH_SNAPPY=OFF`
- `WITH_LZ4=OFF`
- `WITH_ZLIB=OFF`
- `WITH_ZSTD=OFF`
- `WITH_BZ2=OFF`
- `WITH_JEMALLOC=OFF`

Optional local source override:

- `MINIKV_ROCKSDB_SOURCE_DIR=/path/to/rocksdb`

Bundle maintenance is documented separately in
[rocksdb-bundle.md](./rocksdb-bundle.md).

## Common Commands

Default standalone configure/build/test flow using the committed bundle:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel 8
ctest --test-dir build --output-on-failure
```

Refreshing the bundle from a local RocksDB checkout, then building against the
bundle:

```bash
./tools/sync_rocksdb_bundle.sh --rocksdb-source-dir /path/to/rocksdb
./tools/build_linux.sh
```

Helper script:

```bash
./tools/build_linux.sh
./tools/build_linux.sh --rocksdb-source-dir /path/to/rocksdb
./tools/build_linux.sh \
  --rocksdb-source-dir /path/to/rocksdb \
  --rocksdb-reuse-build-dir /path/to/rocksdb/build-minikv
```

## Container Workflow

In this workspace, the authoritative verification path is still the Linux Docker
container rather than the macOS host.

Representative commands:

```bash
docker exec <container> sh -lc '
  cd /workspace/projects/OpenSource/cancer_redis/minikv &&
  cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug \
    -DMINIKV_USE_BUNDLED_ROCKSDB=ON
'

docker exec <container> sh -lc '
  cd /workspace/projects/OpenSource/cancer_redis/minikv &&
  cmake --build build --parallel 8
'

docker exec <container> sh -lc '
  cd /workspace/projects/OpenSource/cancer_redis/minikv &&
  ctest --test-dir build --output-on-failure
'
```

If the workspace already has a local RocksDB checkout mounted in the same
container, you can refresh the committed bundle when its source commit changes:

```bash
docker exec <container> sh -lc '
  cd /workspace/projects/OpenSource/cancer_redis/minikv &&
  ./tools/build_linux.sh \
    --rocksdb-source-dir /path/to/rocksdb \
    --rocksdb-reuse-build-dir /path/to/rocksdb/build-minikv
'
```

## Runtime Configuration Surface

`src/main.cc` accepts:

- `--db_path`
- `--bind`
- `--port`
- `--io_threads`
- `--workers`
- `--max_pending`
- `--max_connections`
- `--max_request_bytes`
- `--idle_timeout_ms`

Defaults are defined in `src/config.h`.

## Validation Notes

The standalone validation standard is:

- all 8 test targets build successfully
- `ctest --test-dir build --output-on-failure` works from the standalone build
  root
- the committed RocksDB bundle can be used without re-fetching or recompiling
  RocksDB source
- `minikv_server` can be launched and exercised with `tools/resp_cli.py` or
  `tools/baseline_smoke.py`

`tools/build_linux.sh` also remains a supported developer entrypoint. It runs
the eight test binaries directly after building, which is useful when you want
a single command that both builds and validates the project.
