# MiniKV

`minikv` is a small Redis-like prototype built on RocksDB. The current
implementation runs as a single POSIX server process, accepts RESP requests
over TCP, and loads all commands from builtin modules only. There is currently
no external module ABI and no dynamic module loading.

Current builtin modules:

- `CoreModule`: `PING`, `TYPE`, `EXISTS`, `DEL`, `EXPIRE`, `TTL`, `PTTL`,
  `PERSIST`
- `StringModule`: `SET`, `GET`, `STRLEN`
- `HashModule`: `HSET`, `HGETALL`, `HDEL`
- `ListModule`: `LPUSH`, `LPOP`, `LRANGE`, `RPUSH`, `RPOP`, `LREM`, `LTRIM`,
  `LLEN`
- `SetModule`: `SADD`, `SCARD`, `SMEMBERS`, `SISMEMBER`, `SPOP`,
  `SRANDMEMBER`, `SREM`
- `ZSetModule`: `ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZLEXCOUNT`, `ZRANGE`,
  `ZRANGEBYLEX`, `ZRANGEBYSCORE`, `ZRANK`, `ZREM`, `ZSCORE`

Current user-visible data model:

- string, hash, list, set, and zset keys
- per-key metadata with live, expired, and tombstone lifecycle states
- module-private storage keyspaces in a shared RocksDB `module` column family

## Platform Note

- If your current system is Linux, use the normal `./tools/build_linux.sh`
  workflow below.
- If your current system is macOS, do not treat the macOS host as the default
  build or validation environment.
- On macOS, first locate or start a Linux Docker container, then run
  configure, build, and test inside that container.
- For representative container commands, see
  [docs/build.md#container-workflow](./docs/build.md#container-workflow).

## Quick Start

Build with the committed RocksDB bundle and vendored googletest:

```bash
./tools/build_linux.sh
```

That flow also exports `build/compile_commands.json` for clangd-compatible
tooling. When the build directory lives under the repository root, the script
refreshes a top-level `compile_commands.json` symlink so editors such as VS
Code can discover it more easily.

If you build inside the Linux container but run VS Code + clangd on the macOS
host, rewrite the recorded container paths for the host workspace with:

```bash
python3 tools/export_compile_commands.py
```

If you have a local RocksDB checkout and want to refresh the committed bundle
first:

```bash
./tools/build_linux.sh \
  --rocksdb-source-dir /path/to/rocksdb \
  --rocksdb-reuse-build-dir /path/to/rocksdb/build-minikv
```

The first form keeps the normal offline bundle-based workflow. The second form
refreshes the committed bundle only when the source checkout commit differs
from the metadata recorded in `third_party/rocksdb/linux-x86_64/BUNDLE_INFO.env`.

## Docs

- [docs/README.md](./docs/README.md): documentation index and reading order
- [docs/build.md](./docs/build.md): build, container, and validation workflow
- [docs/rocksdb-bundle.md](./docs/rocksdb-bundle.md): committed RocksDB bundle
  layout and refresh policy
- [docs/getting-started.md](./docs/getting-started.md): codebase walkthrough
- [docs/architecture.md](./docs/architecture.md): current architecture and
  known design tensions

## Project Layout

- `src/runtime/config.h`, `src/runtime/minikv.h`,
  `src/network/network_server.h`: current runtime and transport entry headers
- `src/runtime/module/`: builtin module SPI, lifecycle manager, exports, and
  module services
- `src/core/`: protocol-level builtin commands and key lifecycle services
- `src/types/string/`: string commands and string storage semantics
- `src/types/hash/`: hash commands, hash storage semantics, and observer bridge
- `src/types/list/`: list commands and list storage semantics
- `src/types/set/`: set commands and set storage semantics
- `src/types/zset/`: sorted-set commands and zset storage semantics
- `src/`: implementation sources. There is no `include/minikv/` public header
  tree yet in this standalone project
- `tests/`: unit and integration tests
- `tools/`: build, smoke, and maintenance scripts
- `third_party/rocksdb/linux-x86_64/`: committed RocksDB headers, shared
  library, symlinks, and bundle metadata for Linux container builds
