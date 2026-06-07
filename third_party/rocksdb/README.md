# RocksDB Bundle Storage

This directory stores committed RocksDB artifacts used by `minikv`.

Current layout:

- `bundles/current/linux-x86_64/`: default Linux container bundle used by the
  documented build flow
- `bundles/5.18.3/linux-x86_64/`: RocksDB 5.18.3 Linux container bundle

Do not edit files under a platform bundle manually. Refresh them with
`tools/sync_rocksdb_bundle.sh --rocksdb-version <version>` and commit the
resulting changes together with `BUNDLE_INFO.env`.

See [docs/rocksdb-bundle.md](../../docs/rocksdb-bundle.md) for the workflow.
