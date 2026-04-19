#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "runtime/config.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"

namespace minikv {

class Snapshot;
class WriteContext;

enum class StorageColumnFamily {
  kDefault,
  kMeta,
  kString,
  kHash,
  kList,
  kSet,
  kZSet,
  kStream,
  kJson,
  kTimeseries,
  kVectorSet,
  kModule,
};

class StorageEngine {
 public:
  StorageEngine();
  ~StorageEngine();

  StorageEngine(const StorageEngine&) = delete;
  StorageEngine& operator=(const StorageEngine&) = delete;

  rocksdb::Status Open(const Config& config);

  rocksdb::Status Get(StorageColumnFamily column_family,
                      const rocksdb::Slice& key, std::string* value) const;
  rocksdb::Status Put(StorageColumnFamily column_family,
                      const rocksdb::Slice& key,
                      const rocksdb::Slice& value);
  rocksdb::Status Delete(StorageColumnFamily column_family,
                         const rocksdb::Slice& key);
  rocksdb::Status Write(rocksdb::WriteBatch* batch);

  std::unique_ptr<Snapshot> CreateSnapshot() const;

 private:
  friend class Snapshot;
  friend class WriteContext;

  rocksdb::Status OpenWithColumnFamilies(const Config& config);
  rocksdb::Status Get(const rocksdb::ReadOptions& options,
                      StorageColumnFamily column_family,
                      const rocksdb::Slice& key, std::string* value) const;
  rocksdb::ColumnFamilyHandle* FindHandle(const std::string& name) const;
  rocksdb::ColumnFamilyHandle* Handle(StorageColumnFamily column_family) const;
  std::unique_ptr<rocksdb::Iterator> NewIterator(
      const rocksdb::ReadOptions& options,
      StorageColumnFamily column_family) const;
  void ReleaseSnapshot(const rocksdb::Snapshot* snapshot) const;

  rocksdb::DB* db_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
  rocksdb::ColumnFamilyHandle* default_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* meta_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* string_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* hash_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* list_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* set_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* zset_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* stream_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* json_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* timeseries_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* vectorset_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* module_cf_ = nullptr;
};

}  // namespace minikv
