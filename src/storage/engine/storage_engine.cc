#include "storage/engine/storage_engine.h"

#include <memory>
#include <utility>

#include "storage/engine/snapshot.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/write_batch.h"

namespace minikv {

namespace {

constexpr char kMetaCF[] = "meta";
constexpr char kHashCF[] = "hash";
constexpr char kModuleCF[] = "module";

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
    case StorageColumnFamily::kHash:
      return hash_cf_;
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
  std::vector<std::string> cf_names;
  rocksdb::Status status =
      rocksdb::DB::ListColumnFamilies(options, config.db_path, &cf_names);
  if (!status.ok() && !status.IsIOError() && !status.IsNotFound()) {
    return status;
  }

  if (!status.ok()) {
    cf_names = {rocksdb::kDefaultColumnFamilyName, kMetaCF, kHashCF,
                kModuleCF};
  } else {
    bool has_meta = false;
    bool has_hash = false;
    bool has_module = false;
    for (const auto& name : cf_names) {
      has_meta = has_meta || name == kMetaCF;
      has_hash = has_hash || name == kHashCF;
      has_module = has_module || name == kModuleCF;
    }
    if (!has_meta) {
      cf_names.push_back(kMetaCF);
    }
    if (!has_hash) {
      cf_names.push_back(kHashCF);
    }
    if (!has_module) {
      cf_names.push_back(kModuleCF);
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
  hash_cf_ = FindHandle(kHashCF);
  module_cf_ = FindHandle(kModuleCF);
  if (meta_cf_ == nullptr || hash_cf_ == nullptr || module_cf_ == nullptr) {
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
