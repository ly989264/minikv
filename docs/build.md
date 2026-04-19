# MiniKV Build Notes

## Scope

This document records how the standalone `minikv` project is configured, built,
and verified in this standalone `minikv/` workspace.

## Standalone Build Layout

`CMakeLists.txt` defines one independent CMake project with these targets:

- `minikv_core`: static library containing the runtime, module, storage,
  execution, and transport implementation under `src/`
- `minikv_server`: server executable from `src/app/main.cc`
- `minikv_cmd_test`
- `minikv_command_registry_test`
- `minikv_hash_module_test`
- `minikv_json_module_test`
- `minikv_list_module_test`
- `minikv_string_module_test`
- `minikv_bitmap_module_test`
- `minikv_set_module_test`
- `minikv_stream_module_test`
- `minikv_geo_module_test`
- `minikv_zset_module_test`
- `minikv_network_test`
- `minikv_reply_encode_test`
- `minikv_scheduler_test`
- `minikv_snapshot_test`
- `minikv_module_manager_test`
- `minikv_module_exports_test`
- `minikv_module_keyspace_test`
- `minikv_module_iterator_test`
- `minikv_background_executor_test`
- `minikv_hash_bridge_test`
- `minikv_zset_bridge_test`
- `minikv_hash_observer_test`

All targets build as C++17.

## Dependency Strategy

Default dependency mode prefers the committed RocksDB bundle and the vendored
googletest tree plus the checked-in header-only JSON library:

- `MINIKV_USE_BUNDLED_ROCKSDB=ON`
- `MINIKV_ROCKSDB_BUNDLE_DIR=third_party/rocksdb/linux-x86_64`
- `MINIKV_FETCH_DEPS=OFF` when the bundle is complete
- `MINIKV_GTEST_SOURCE_DIR=third_party/googletest`
- `MINIKV_FETCH_GTEST=OFF` when the vendored googletest tree is present
- `third_party/minijson/minijson.h` is always available locally and never
  fetched during configure
- `MINIKV_ROCKSDB_TAG=v11.0.4` for `FetchContent` fallback only
- `MINIKV_GTEST_TAG=v1.14.0` for `FetchContent` fallback only

When the bundle is present, `minikv` links against the committed
`third_party/rocksdb/linux-x86_64/lib/librocksdb.so` and uses the committed
headers under `third_party/rocksdb/linux-x86_64/include/rocksdb`.

Important distinction:

- `MINIKV_ROCKSDB_TAG` is the fallback fetch pin used when the bundle is absent
- the committed bundle itself is described by
  `third_party/rocksdb/linux-x86_64/BUNDLE_INFO.env`
- those two values may differ because the bundle can be refreshed from a local
  checkout rather than from the fallback tag

If the bundle is missing, CMake falls back to either:

- `MINIKV_ROCKSDB_SOURCE_DIR`
- `FetchContent` using `MINIKV_ROCKSDB_TAG`

Pinned RocksDB fallback build settings:

- `ROCKSDB_BUILD_SHARED=OFF`
- `WITH_TESTS=OFF`
- `WITH_MINIKV=OFF`
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

Bundle maintenance is documented separately in
[rocksdb-bundle.md](./rocksdb-bundle.md).

## Common Commands

Default configure, build, and test flow:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build --parallel 8
ctest --test-dir build --output-on-failure
```

`compile_commands.json` is enabled by default for fresh CMake configure runs in
this repository, but the explicit flag above is still useful when refreshing an
existing build directory that was configured before that default was added.

Preferred helper script:

```bash
./tools/build_linux.sh
./tools/build_linux.sh --skip-tests
./tools/build_linux.sh --rocksdb-source-dir /path/to/rocksdb
./tools/build_linux.sh \
  --rocksdb-source-dir /path/to/rocksdb \
  --rocksdb-reuse-build-dir /path/to/rocksdb/build-minikv
```

`tools/build_linux.sh`:

- refreshes the committed bundle first when `--rocksdb-source-dir` is provided
- configures CMake to prefer the committed bundle when it is complete
- exports `build/compile_commands.json` by default
- refreshes a top-level `compile_commands.json` symlink when the build
  directory lives inside the repository
- builds the project
- runs a fixed list of 22 test binaries directly unless `--skip-tests` is used

If your authoritative build ran inside the Linux container but your editor is a
host-side VS Code window on macOS, rewrite the recorded container paths for the
host workspace after the build:

```bash
python3 tools/export_compile_commands.py
```

That command reads `build/compile_commands.json` and `build/CMakeCache.txt`,
then writes a host-friendly top-level `compile_commands.json`.

## Container Workflow

In this workspace, the authoritative verification path is the Linux Docker
container rather than the macOS host.

The current workspace is mounted inside the container at:

- `/workspace/projects/OpenSource/minikv`

Representative commands:

```bash
docker exec <container> sh -lc '
  cd /workspace/projects/OpenSource/minikv &&
  ./tools/build_linux.sh
'

docker exec <container> sh -lc '
  cd /workspace/projects/OpenSource/minikv &&
  ctest --test-dir build --output-on-failure
'
```

If the same container also mounts a local RocksDB checkout, you can refresh the
committed bundle when its source commit changes:

```bash
docker exec <container> sh -lc '
  cd /workspace/projects/OpenSource/minikv &&
  ./tools/build_linux.sh \
    --rocksdb-source-dir /path/to/rocksdb \
    --rocksdb-reuse-build-dir /path/to/rocksdb/build-minikv
'
```

## Runtime Configuration Surface

`src/app/main.cc` accepts:

- `--db_path`
- `--bind`
- `--port`
- `--io_threads`
- `--workers`
- `--max_pending`
- `--max_connections`
- `--max_request_bytes`
- `--idle_timeout_ms`

Defaults are defined in `src/runtime/config.h`.

## Validation Notes

The standalone validation standard is:

- all 22 test targets build successfully
- `ctest --test-dir build --output-on-failure` works from the standalone build
  root
- the committed RocksDB bundle can be used without re-fetching or recompiling
  RocksDB source
- `minikv_server` can be launched and exercised with `tools/resp_cli.py` or
  `tools/baseline_smoke.py`

`ctest --test-dir <build-dir> --output-on-failure` is the authoritative CMake
test entrypoint.

`tools/build_linux.sh` also remains a supported developer entrypoint. It runs a
fixed list of 22 test binaries directly after building, which is useful when
you want a single command that both builds and validates the project.
