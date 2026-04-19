#include "storage/engine/storage_engine.h"

#include <memory>
#include <utility>
#include <vector>

#include "storage/engine/snapshot.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/write_batch.h"

namespace minikv {

namespace {

constexpr char kMetaCF[] = "meta";
constexpr char kStringCF[] = "string";
constexpr char kHashCF[] = "hash";
constexpr char kListCF[] = "list";
constexpr char kSetCF[] = "set";
constexpr char kZSetCF[] = "zset";
constexpr char kStreamCF[] = "stream";
constexpr char kJsonCF[] = "json";
constexpr char kTimeseriesCF[] = "timeseries";
constexpr char kVectorSetCF[] = "vectorset";
constexpr char kModuleCF[] = "module";

std::vector<std::string> RequiredColumnFamilyNames() {
  return {
      rocksdb::kDefaultColumnFamilyName, kMetaCF,       kStringCF,   kHashCF,
      kListCF,                     kSetCF,       kZSetCF,    kStreamCF,
      kJsonCF,                     kTimeseriesCF, kVectorSetCF, kModuleCF,
  };
}

rocksdb::Options BaseOptions() {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  return options;
}

rocksdb::ColumnFamilyOptions HashCFOptions() {
  rocksdb::ColumnFamilyOptions options;
  options.optimize_filters_for_hits = true;
  return options;
}

}  // namespace

StorageEngine::StorageEngine() = default;

StorageEngine::~StorageEngine() {
  for (auto* handle : handles_) {
    delete handle;
  }
  delete db_;
}

rocksdb::ColumnFamilyHandle* StorageEngine::FindHandle(
    const std::string& name) const {
  for (auto* handle : handles_) {
    if (handle->GetName() == name) {
      return handle;
    }
  }
  return nullptr;
}

rocksdb::ColumnFamilyHandle* StorageEngine::Handle(
    StorageColumnFamily column_family) const {
  switch (column_family) {
    case StorageColumnFamily::kDefault:
      return default_cf_;
    case StorageColumnFamily::kMeta:
      return meta_cf_;
    case StorageColumnFamily::kString:
      return string_cf_;
    case StorageColumnFamily::kHash:
      return hash_cf_;
    case StorageColumnFamily::kList:
      return list_cf_;
    case StorageColumnFamily::kSet:
      return set_cf_;
    case StorageColumnFamily::kZSet:
      return zset_cf_;
    case StorageColumnFamily::kStream:
      return stream_cf_;
    case StorageColumnFamily::kJson:
      return json_cf_;
    case StorageColumnFamily::kTimeseries:
      return timeseries_cf_;
    case StorageColumnFamily::kVectorSet:
      return vectorset_cf_;
    case StorageColumnFamily::kModule:
      return module_cf_;
  }
  return nullptr;
}

rocksdb::Status StorageEngine::Open(const Config& config) {
  return OpenWithColumnFamilies(config);
}

rocksdb::Status StorageEngine::OpenWithColumnFamilies(const Config& config) {
  rocksdb::Options options = BaseOptions();
  const std::vector<std::string> required_cf_names = RequiredColumnFamilyNames();
  std::vector<std::string> cf_names;
  rocksdb::Status status =
      rocksdb::DB::ListColumnFamilies(options, config.db_path, &cf_names);
  if (!status.ok() && !status.IsIOError() && !status.IsNotFound()) {
    return status;
  }

  if (!status.ok()) {
    cf_names = required_cf_names;
  } else {
    for (const auto& required_name : required_cf_names) {
      bool has_name = false;
      for (const auto& existing_name : cf_names) {
        if (existing_name == required_name) {
          has_name = true;
          break;
        }
      }
      if (!has_name) {
        cf_names.push_back(required_name);
      }
    }
  }

  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  descriptors.reserve(cf_names.size());
  for (const auto& name : cf_names) {
    if (name == kHashCF) {
      descriptors.emplace_back(name, HashCFOptions());
    } else {
      descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
    }
  }

  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::DB* db = nullptr;
  status = rocksdb::DB::Open(rocksdb::DBOptions(options), config.db_path,
                             descriptors, &handles, &db);
  if (!status.ok()) {
    for (auto* handle : handles) {
      delete handle;
    }
    return status;
  }

  db_ = db;
  handles_ = std::move(handles);
  default_cf_ = FindHandle(rocksdb::kDefaultColumnFamilyName);
  meta_cf_ = FindHandle(kMetaCF);
  string_cf_ = FindHandle(kStringCF);
  hash_cf_ = FindHandle(kHashCF);
  list_cf_ = FindHandle(kListCF);
  set_cf_ = FindHandle(kSetCF);
  zset_cf_ = FindHandle(kZSetCF);
  stream_cf_ = FindHandle(kStreamCF);
  json_cf_ = FindHandle(kJsonCF);
  timeseries_cf_ = FindHandle(kTimeseriesCF);
  vectorset_cf_ = FindHandle(kVectorSetCF);
  module_cf_ = FindHandle(kModuleCF);
  if (default_cf_ == nullptr || meta_cf_ == nullptr || string_cf_ == nullptr ||
      hash_cf_ == nullptr || list_cf_ == nullptr || set_cf_ == nullptr ||
      zset_cf_ == nullptr || stream_cf_ == nullptr || json_cf_ == nullptr ||
      timeseries_cf_ == nullptr || vectorset_cf_ == nullptr ||
      module_cf_ == nullptr) {
    return rocksdb::Status::Corruption("required column families missing");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status StorageEngine::Get(StorageColumnFamily column_family,
                                   const rocksdb::Slice& key,
                                   std::string* value) const {
  return Get(rocksdb::ReadOptions(), column_family, key, value);
}

rocksdb::Status StorageEngine::Get(const rocksdb::ReadOptions& options,
                                   StorageColumnFamily column_family,
                                   const rocksdb::Slice& key,
                                   std::string* value) const {
  return db_->Get(options, Handle(column_family), key, value);
}

rocksdb::Status StorageEngine::Put(StorageColumnFamily column_family,
                                   const rocksdb::Slice& key,
                                   const rocksdb::Slice& value) {
  return db_->Put(rocksdb::WriteOptions(), Handle(column_family), key, value);
}

rocksdb::Status StorageEngine::Delete(StorageColumnFamily column_family,
                                      const rocksdb::Slice& key) {
  return db_->Delete(rocksdb::WriteOptions(), Handle(column_family), key);
}

rocksdb::Status StorageEngine::Write(rocksdb::WriteBatch* batch) {
  return db_->Write(rocksdb::WriteOptions(), batch);
}

std::unique_ptr<Snapshot> StorageEngine::CreateSnapshot() const {
  return std::unique_ptr<Snapshot>(new Snapshot(this, db_->GetSnapshot()));
}

std::unique_ptr<rocksdb::Iterator> StorageEngine::NewIterator(
    const rocksdb::ReadOptions& options,
    StorageColumnFamily column_family) const {
  return std::unique_ptr<rocksdb::Iterator>(
      db_->NewIterator(options, Handle(column_family)));
}

void StorageEngine::ReleaseSnapshot(const rocksdb::Snapshot* snapshot) const {
  if (snapshot != nullptr && db_ != nullptr) {
    db_->ReleaseSnapshot(snapshot);
  }
}

}  // namespace minikv
