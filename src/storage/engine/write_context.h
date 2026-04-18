#pragma once

#include "storage/engine/storage_engine.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"

namespace minikv {

class WriteContext {
 public:
  explicit WriteContext(StorageEngine* storage_engine);

  rocksdb::Status Put(StorageColumnFamily column_family,
                      const rocksdb::Slice& key,
                      const rocksdb::Slice& value);
  rocksdb::Status Delete(StorageColumnFamily column_family,
                         const rocksdb::Slice& key);
  rocksdb::Status Commit();

 private:
  StorageEngine* storage_engine_;
  rocksdb::WriteBatch batch_;
  bool committed_ = false;
};

}  // namespace minikv
