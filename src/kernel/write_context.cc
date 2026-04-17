#include "kernel/write_context.h"

namespace minikv {

WriteContext::WriteContext(StorageEngine* storage_engine)
    : storage_engine_(storage_engine) {}

rocksdb::Status WriteContext::Put(StorageColumnFamily column_family,
                                  const rocksdb::Slice& key,
                                  const rocksdb::Slice& value) {
  if (committed_) {
    return rocksdb::Status::InvalidArgument("write context already committed");
  }
  batch_.Put(storage_engine_->Handle(column_family), key, value);
  return rocksdb::Status::OK();
}

rocksdb::Status WriteContext::Delete(StorageColumnFamily column_family,
                                     const rocksdb::Slice& key) {
  if (committed_) {
    return rocksdb::Status::InvalidArgument("write context already committed");
  }
  batch_.Delete(storage_engine_->Handle(column_family), key);
  return rocksdb::Status::OK();
}

rocksdb::Status WriteContext::Commit() {
  if (committed_) {
    return rocksdb::Status::InvalidArgument("write context already committed");
  }
  committed_ = true;
  return storage_engine_->Write(&batch_);
}

}  // namespace minikv
