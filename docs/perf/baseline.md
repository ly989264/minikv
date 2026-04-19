# MiniKV Baseline

This document records the standalone `minikv` validation baseline and the smoke
workflow used after splitting the project out of the RocksDB repository tree.

## Authoritative Environment

The default acceptance environment for this workspace is the Linux Docker
container with the current workspace mounted at:

- `/workspace/projects/OpenSource/minikv`

## Build And Test Baseline

Recommended standalone build:

```bash
docker exec <container> sh -lc '
  cd /workspace/projects/OpenSource/minikv &&
  ./tools/build_linux.sh
'
```

If you want to reuse an existing local RocksDB checkout instead of relying on
the committed bundle alone:

```bash
docker exec <container> sh -lc '
  cd /workspace/projects/OpenSource/minikv &&
  ./tools/build_linux.sh \
    --rocksdb-source-dir /path/to/rocksdb
'
```

The baseline test matrix is:

- `minikv_cmd_test`
- `minikv_command_registry_test`
- `minikv_hash_module_test`
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
- `minikv_hash_observer_test`

All of these are expected to run through:

```bash
docker exec <container> sh -lc '
  cd /workspace/projects/OpenSource/minikv &&
  ctest --test-dir build --output-on-failure
'
```

## Basic RESP Smoke

Start the server:

```bash
docker exec <container> sh -lc '
  cd /workspace/projects/OpenSource/minikv &&
  rm -rf /tmp/minikv-baseline-db &&
  ./build/minikv_server \
    --db_path /tmp/minikv-baseline-db \
    --bind 127.0.0.1 \
    --port 6390 \
    --io_threads 2 \
    --workers 4
'
```

Run the smoke check:

```bash
docker exec <container> sh -lc '
  cd /workspace/projects/OpenSource/minikv &&
  python3 tools/baseline_smoke.py --host 127.0.0.1 --port 6390 --iterations 20
'
```

The smoke runner samples:

- `PING`
- `HSET`
- `HGETALL`
- `HDEL`

That is a deliberate subset. The broader command surface is still covered by
the unit and integration tests:

- `TYPE`
- `EXISTS`
- `DEL`
- `EXPIRE`
- `TTL`
- `PTTL`
- `PERSIST`

## Scope Boundary

The current baseline applies to the current implementation surface:

- supported user-visible data types: string, hash, list, set, zset
- supported server replies from builtin commands: simple string, integer, bulk
  string, flat array, and error
- module SPI: builtin-only

Explicitly out of scope for this baseline:

- unimplemented data types such as stream
- external module loading or external ABI behavior
- search commands including `FT.*`
- benchmarking claims beyond the smoke script's simple latency samples
