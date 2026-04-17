#include "kernel/snapshot.h"

#include <memory>

#include "engine/key_codec.h"

namespace minikv {

Snapshot::Snapshot(const StorageEngine* storage_engine,
                   const rocksdb::Snapshot* snapshot)
    : storage_engine_(storage_engine), snapshot_(snapshot) {}

Snapshot::~Snapshot() { storage_engine_->ReleaseSnapshot(snapshot_); }

rocksdb::Status Snapshot::Get(StorageColumnFamily column_family,
                              const rocksdb::Slice& key,
                              std::string* value) const {
  rocksdb::ReadOptions options;
  options.snapshot = snapshot_;
  return storage_engine_->Get(options, column_family, key, value);
}

rocksdb::Status Snapshot::ScanPrefix(StorageColumnFamily column_family,
                                     const rocksdb::Slice& prefix,
                                     const ScanVisitor& visitor) const {
  rocksdb::ReadOptions options;
  options.snapshot = snapshot_;
  std::unique_ptr<rocksdb::Iterator> iter =
      storage_engine_->NewIterator(options, column_family);
  for (iter->Seek(prefix);
       iter->Valid() && KeyCodec::StartsWith(iter->key(), prefix);
       iter->Next()) {
    if (!visitor(iter->key(), iter->value())) {
      break;
    }
  }
  return iter->status();
}

}  // namespace minikv
