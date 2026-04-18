#pragma once

#include <functional>
#include <string>

#include "kernel/storage_engine.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace minikv {

class Snapshot {
 public:
  using ScanVisitor =
      std::function<bool(const rocksdb::Slice& key, const rocksdb::Slice& value)>;

  ~Snapshot();

  Snapshot(const Snapshot&) = delete;
  Snapshot& operator=(const Snapshot&) = delete;

  rocksdb::Status Get(StorageColumnFamily column_family,
                      const rocksdb::Slice& key, std::string* value) const;
  std::unique_ptr<rocksdb::Iterator> NewIterator(
      StorageColumnFamily column_family) const;
  rocksdb::Status ScanPrefix(StorageColumnFamily column_family,
                             const rocksdb::Slice& prefix,
                             const ScanVisitor& visitor) const;

 private:
  friend class StorageEngine;

  Snapshot(const StorageEngine* storage_engine,
           const rocksdb::Snapshot* snapshot);

  const StorageEngine* storage_engine_;
  const rocksdb::Snapshot* snapshot_;
};

}  // namespace minikv
