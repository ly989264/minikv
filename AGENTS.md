# Minikv Agent Notes

This directory is a standalone project rooted at `minikv/`.

## Build Rules

- Prefer `./tools/build_linux.sh` for Linux container validation.
- Default development flow should use the committed RocksDB bundle under
  `third_party/rocksdb/linux-x86_64` when it exists.
- If you need to refresh that bundle from a local RocksDB checkout, use
  `./tools/sync_rocksdb_bundle.sh --rocksdb-source-dir /path/to/rocksdb`.
- Do not edit bundled RocksDB headers or shared libraries by hand. Refresh them
  through `tools/sync_rocksdb_bundle.sh` and commit the resulting files plus
  `BUNDLE_INFO.env`.

## Validation Rules

- `ctest --test-dir <build-dir> --output-on-failure` is the authoritative CMake
  test entrypoint.
- `./tools/build_linux.sh` intentionally runs the eleven test binaries directly
  after building. Keep that path working because it is the most robust
  end-to-end developer workflow.
- If a test becomes flaky, fix the test or its synchronization rather than
  documenting the flake as expected behavior.

## Docs Chain

- Start at `README.md`.
- Then read `docs/README.md`.
- Build and dependency details live in `docs/build.md` and
  `docs/rocksdb-bundle.md`.

## Scope

- Public interface headers live under `include/minikv/`.
- Internal implementation stays under `src/`.
- Keep runtime behavior stable unless the task explicitly asks for a semantic
  change.
