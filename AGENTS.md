# Minikv Agent Notes

This directory is a standalone C++17 project rooted at `minikv/`. It is a small Redis-like prototype built on RocksDB, exposes RESP over TCP, and loads builtin modules only.

## Platform Rules

- Authoritative compilation and validation must run in a Linux Docker container, not directly on the macOS host.
- If the detected host system is macOS, first locate an existing Linux Docker container or start one for this workspace before running configure, build, or `ctest`.
- Only discuss or attempt a macOS host build when the user explicitly asks to investigate host compatibility.
- Inside the Linux container, prefer `./tools/build_linux.sh` and the committed RocksDB bundle under `third_party/rocksdb/linux-x86_64`.
- A known container mount path for this workspace is `/workspace/projects/OpenSource/minikv`; verify it exists before using it.

## Build Rules

- Prefer the all-in-one Linux workflow from inside the container:

  ```bash
  docker exec <container> sh -lc 'cd /workspace/projects/OpenSource/minikv && ./tools/build_linux.sh'
  ```

- If already inside the container at the repository root, run:

  ```bash
  ./tools/build_linux.sh
  ```

- Useful variants:

  ```bash
  ./tools/build_linux.sh --skip-tests
  ./tools/build_linux.sh --build-dir build --build-type Debug --jobs 8
  ```

- Manual CMake workflow:

  ```bash
  cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
  cmake --build build --parallel 8
  ctest --test-dir build --output-on-failure
  ```

- Default development flow should use the committed RocksDB bundle when it exists.
- If refreshing the committed RocksDB bundle from a local checkout, use the supported tool flow rather than editing bundled headers or libraries by hand:

  ```bash
  ./tools/build_linux.sh \
    --rocksdb-source-dir /path/to/rocksdb \
    --rocksdb-reuse-build-dir /path/to/rocksdb/build-minikv
  ```

- Commit refreshed bundle outputs together with `third_party/rocksdb/linux-x86_64/BUNDLE_INFO.env` when bundle contents change.

## Validation Rules

- `ctest --test-dir <build-dir> --output-on-failure` is the authoritative CMake test entrypoint.
- `./tools/build_linux.sh` builds the project and runs the current fixed set of 23 test binaries directly unless `--skip-tests` is used.
- Run a single CTest target with:

  ```bash
  ctest --test-dir build -R minikv_hash_module_test --output-on-failure
  ```

- Run a single test binary directly with:

  ```bash
  ./build/minikv_hash_module_test
  ```

- If a test becomes flaky, fix the test or synchronization issue rather than documenting the flake as expected behavior.

## Architecture Notes

The main request path is:

```text
main -> MiniKV::Open -> ModuleManager -> NetworkServer -> RespParser -> CreateCmd -> Scheduler -> Worker -> builtin module command -> ModuleSnapshot / ModuleWriteBatch -> StorageEngine -> RocksDB
```

Key current boundaries:

- The command surface is network-only; there is no parallel in-process command API.
- The module SPI is builtin/source-level only; there is no external ABI or dynamic module loading.
- Builtin module load order is core, string, bitmap, hash, json, list, set, zset, geo, then stream.
- I/O threads own sockets and parsing; worker threads execute commands under keyed lock plans.
- New module-private state should generally use `ModuleKeyspace` instead of raw column-family access.
- Public runtime/server entry headers still live under `src/` (`src/runtime/config.h`, `src/runtime/minikv.h`, `src/network/network_server.h`); there is no `include/minikv/` public header tree yet.
- Search prep infrastructure exists, but there is no `SearchModule` and no `FT.*` command family.

## Docs Chain

- Start at `README.md`.
- Then read `docs/README.md`.
- Build and dependency details live in `docs/build.md` and `docs/rocksdb-bundle.md`.
- Use `docs/getting-started.md`, `docs/architecture.md`, `docs/architecture/current-layering.md`, and `docs/layers/*.md` before making cross-layer changes.
