#include "storage/engine/snapshot.h"

#include <cstring>
#include <memory>

namespace minikv {

namespace {

bool StartsWith(const rocksdb::Slice& value, const rocksdb::Slice& prefix) {
  return value.size() >= prefix.size() &&
         std::memcmp(value.data(), prefix.data(), prefix.size()) == 0;
}

}  // namespace

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

std::unique_ptr<rocksdb::Iterator> Snapshot::NewIterator(
    StorageColumnFamily column_family) const {
  rocksdb::ReadOptions options;
  options.snapshot = snapshot_;
  return storage_engine_->NewIterator(options, column_family);
}

rocksdb::Status Snapshot::ScanPrefix(StorageColumnFamily column_family,
                                     const rocksdb::Slice& prefix,
                                     const ScanVisitor& visitor) const {
  std::unique_ptr<rocksdb::Iterator> iter = NewIterator(column_family);
  for (iter->Seek(prefix);
       iter->Valid() && StartsWith(iter->key(), prefix);
       iter->Next()) {
    if (!visitor(iter->key(), iter->value())) {
      break;
    }
  }
  return iter->status();
}

}  // namespace minikv
