# RocksDB Bundle Storage

This directory stores committed RocksDB artifacts used by `minikv`.

Current layout:

- `linux-x86_64/`: Linux container bundle used by the documented build flow

Do not edit files under the platform bundle manually. Refresh them with
`tools/sync_rocksdb_bundle.sh` and commit the resulting changes together with
`BUNDLE_INFO.env`.

See [docs/rocksdb-bundle.md](../../docs/rocksdb-bundle.md) for the workflow.
