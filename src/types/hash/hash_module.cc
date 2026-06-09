#include "types/hash/hash_module.h"

#include <algorithm>
#include <memory>

#include "storage/encoding/key_codec.h"
#include "runtime/module/module_services.h"
#include "core/key_service.h"
#include "types/hash/hash_commands.h"
#include "types/hash/hash_observer.h"

namespace minikv {

namespace {

rocksdb::Status RequireHashEncoding(const KeyLookup& lookup) {
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  if (lookup.metadata.type != ObjectType::kHash ||
      lookup.metadata.encoding != ObjectEncoding::kHashPlain) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }
  return rocksdb::Status::OK();
}

KeyMetadata BuildHashMetadata(const CoreKeyService* key_service,
                              const KeyLookup& lookup) {
  if (lookup.exists) {
    return lookup.metadata;
  }

  KeyMetadata metadata =
      key_service->MakeMetadata(ObjectType::kHash, ObjectEncoding::kHashPlain,
                                lookup);
  metadata.size = 0;
  return metadata;
}

KeyMetadata BuildHashTombstoneMetadata(const CoreKeyService* key_service,
                                       const KeyLookup& lookup) {
  KeyMetadata metadata = key_service->MakeTombstoneMetadata(lookup);
  metadata.size = 0;
  return metadata;
}

std::vector<FieldValue> DeduplicateFieldValues(
    const std::vector<FieldValue>& values) {
  std::vector<FieldValue> unique_values;
  unique_values.reserve(values.size());
  for (const auto& value : values) {
    auto it = std::find_if(
        unique_values.begin(), unique_values.end(),
        [&value](const FieldValue& existing) {
          return existing.field == value.field;
        });
    if (it == unique_values.end()) {
      unique_values.push_back(value);
    } else {
      it->value = value.value;
    }
  }
  return unique_values;
}

std::vector<std::string> DeduplicateFields(
    const std::vector<std::string>& fields) {
  std::vector<std::string> unique_fields;
  unique_fields.reserve(fields.size());
  for (const auto& field : fields) {
    if (std::find(unique_fields.begin(), unique_fields.end(), field) ==
        unique_fields.end()) {
      unique_fields.push_back(field);
    }
  }
  return unique_fields;
}

}  // namespace

rocksdb::Status HashModule::OnLoad(ModuleServices& services) {
  observers_.clear();
  services_ = &services;

  rocksdb::Status status = services.exports().Publish<HashIndexingBridge>(
      kHashIndexingBridgeExportName,
      static_cast<HashIndexingBridge*>(this));
  if (!status.ok()) {
    return status;
  }

  return RegisterHashCommands(services, this);
}

rocksdb::Status HashModule::OnStart(ModuleServices& services) {
  key_service_ = services.exports().Find<CoreKeyService>(
      kCoreKeyServiceQualifiedExportName);
  if (key_service_ == nullptr) {
    return rocksdb::Status::InvalidArgument("core key service is unavailable");
  }
  delete_registry_ = services.exports().Find<WholeKeyDeleteRegistry>(
      kWholeKeyDeleteRegistryQualifiedExportName);
  if (delete_registry_ == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "whole-key delete registry is unavailable");
  }
  rocksdb::Status status = delete_registry_->RegisterHandler(this);
  if (!status.ok()) {
    return status;
  }

  started_ = true;
  services.metrics().SetCounter("worker_count",
                                services.scheduler().worker_count());
  return rocksdb::Status::OK();
}

void HashModule::OnStop(ModuleServices& /*services*/) {
  observers_.clear();
  started_ = false;
  delete_registry_ = nullptr;
  key_service_ = nullptr;
  services_ = nullptr;
}

rocksdb::Status HashModule::AddObserver(HashObserver* observer) {
  if (services_ == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "hash indexing bridge is unavailable");
  }
  if (observer == nullptr) {
    return rocksdb::Status::InvalidArgument("hash observer is required");
  }
  if (std::find(observers_.begin(), observers_.end(), observer) !=
      observers_.end()) {
    return rocksdb::Status::InvalidArgument("hash observer already registered");
  }
  observers_.push_back(observer);
  return rocksdb::Status::OK();
}

rocksdb::Status HashModule::RemoveObserver(HashObserver* observer) {
  if (services_ == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "hash indexing bridge is unavailable");
  }
  if (observer == nullptr) {
    return rocksdb::Status::InvalidArgument("hash observer is required");
  }
  auto it = std::find(observers_.begin(), observers_.end(), observer);
  if (it == observers_.end()) {
    return rocksdb::Status::InvalidArgument("hash observer is not registered");
  }
  observers_.erase(it);
  return rocksdb::Status::OK();
}

rocksdb::Status HashModule::PutField(const std::string& key,
                                     const std::string& field,
                                     const std::string& value,
                                     bool* inserted) {
  if (inserted != nullptr) {
    *inserted = false;
  }

  uint64_t inserted_count = 0;
  rocksdb::Status status =
      PutFields(key, {FieldValue{field, value}}, &inserted_count);
  if (inserted != nullptr) {
    *inserted = inserted_count != 0 && status.ok();
  }
  return status;
}

rocksdb::Status HashModule::PutFields(const std::string& key,
                                      const std::vector<FieldValue>& values,
                                      uint64_t* inserted) {
  if (inserted != nullptr) {
    *inserted = 0;
  }

  const std::vector<FieldValue> unique_values = DeduplicateFieldValues(values);
  if (unique_values.empty()) {
    return rocksdb::Status::OK();
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  status = RequireHashEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const bool existed_before = lookup.exists;
  KeyMetadata before = BuildHashMetadata(key_service_, lookup);

  uint64_t inserted_count = 0;
  for (const auto& value : unique_values) {
    const std::string data_key =
        KeyCodec::EncodeHashDataKey(key, before.version, value.field);
    std::string existing_value;
    status =
        snapshot->Get(StorageColumnFamily::kHash, data_key, &existing_value);
    if (status.ok()) {
      continue;
    }
    if (!status.IsNotFound()) {
      return status;
    }
    ++inserted_count;
  }

  KeyMetadata after = before;
  after.size += inserted_count;

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }
  for (const auto& value : unique_values) {
    const std::string data_key =
        KeyCodec::EncodeHashDataKey(key, before.version, value.field);
    status = write_batch->Put(StorageColumnFamily::kHash, data_key,
                              value.value);
    if (!status.ok()) {
      return status;
    }
  }

  HashMutation mutation;
  mutation.type = HashMutation::Type::kPutField;
  mutation.key = key;
  mutation.values = unique_values;
  mutation.before = before;
  mutation.after = after;
  mutation.existed_before = existed_before;
  mutation.exists_after = true;
  status = NotifyObservers(mutation, write_batch.get());
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (inserted != nullptr) {
    *inserted = status.ok() ? inserted_count : 0;
  }
  return status;
}

rocksdb::Status HashModule::ReadField(const std::string& key,
                                      const std::string& field,
                                      FieldLookup* out) {
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("field lookup output is required");
  }
  std::vector<FieldLookup> values;
  rocksdb::Status status = ReadFields(key, {field}, &values);
  if (!status.ok()) {
    return status;
  }
  *out = values.empty() ? FieldLookup{} : std::move(values.front());
  return rocksdb::Status::OK();
}

rocksdb::Status HashModule::ReadFields(
    const std::string& key, const std::vector<std::string>& fields,
    std::vector<FieldLookup>* out) {
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("field lookup output is required");
  }
  out->clear();
  out->resize(fields.size());

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireHashEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  for (size_t i = 0; i < fields.size(); ++i) {
    const std::string data_key =
        KeyCodec::EncodeHashDataKey(key, lookup.metadata.version, fields[i]);
    std::string value;
    status = snapshot->Get(StorageColumnFamily::kHash, data_key, &value);
    if (status.ok()) {
      (*out)[i].found = true;
      (*out)[i].value = std::move(value);
    } else if (!status.IsNotFound()) {
      return status;
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status HashModule::ReadAll(const std::string& key,
                                    std::vector<FieldValue>* out) {
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("field values output is required");
  }
  out->clear();

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireHashEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const std::string prefix =
      KeyCodec::EncodeHashDataPrefix(key, lookup.metadata.version);
  return snapshot->ScanPrefix(
      StorageColumnFamily::kHash, prefix,
      [out, &prefix](const rocksdb::Slice& encoded_key,
                     const rocksdb::Slice& value_slice) {
        std::string field;
        if (!KeyCodec::ExtractFieldFromHashDataKey(encoded_key, prefix, &field)) {
          return false;
        }
        out->push_back(FieldValue{std::move(field), value_slice.ToString()});
        return true;
      });
}

rocksdb::Status HashModule::Length(const std::string& key, uint64_t* length) {
  if (length == nullptr) {
    return rocksdb::Status::InvalidArgument("hash length output is required");
  }
  *length = 0;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireHashEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  *length = lookup.metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status HashModule::FieldExists(const std::string& key,
                                        const std::string& field,
                                        bool* exists) {
  if (exists == nullptr) {
    return rocksdb::Status::InvalidArgument("field exists output is required");
  }
  *exists = false;
  FieldLookup lookup;
  rocksdb::Status status = ReadField(key, field, &lookup);
  if (!status.ok()) {
    return status;
  }
  *exists = lookup.found;
  return rocksdb::Status::OK();
}

rocksdb::Status HashModule::DeleteFields(const std::string& key,
                                         const std::vector<std::string>& fields,
                                         uint64_t* deleted) {
  if (deleted != nullptr) {
    *deleted = 0;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireHashEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  KeyMetadata before = lookup.metadata;
  uint64_t removed = 0;
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  std::vector<std::string> removed_fields;
  const std::vector<std::string> unique_fields = DeduplicateFields(fields);
  std::string scratch;
  for (const auto& field : unique_fields) {
    const std::string data_key =
        KeyCodec::EncodeHashDataKey(key, before.version, field);
    status = snapshot->Get(StorageColumnFamily::kHash, data_key, &scratch);
    if (status.ok()) {
      status = write_batch->Delete(StorageColumnFamily::kHash, data_key);
      if (!status.ok()) {
        return status;
      }
      ++removed;
      removed_fields.push_back(field);
    } else if (!status.IsNotFound()) {
      return status;
    }
  }

  if (removed == 0) {
    return rocksdb::Status::OK();
  }

  KeyMetadata after = before;
  if (removed >= before.size) {
    after = BuildHashTombstoneMetadata(key_service_, lookup);
    status = key_service_->PutMetadata(write_batch.get(), key, after);
  } else {
    after.size -= removed;
    status = key_service_->PutMetadata(write_batch.get(), key, after);
  }
  if (!status.ok()) {
    return status;
  }

  HashMutation mutation;
  mutation.type = HashMutation::Type::kDeleteFields;
  mutation.key = key;
  mutation.deleted_fields = removed_fields;
  mutation.before = before;
  mutation.after = after;
  mutation.existed_before = true;
  mutation.exists_after = removed < before.size;
  status = NotifyObservers(mutation, write_batch.get());
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (deleted != nullptr) {
    *deleted = status.ok() ? removed : 0;
  }
  return status;
}

rocksdb::Status HashModule::DeleteWholeKey(ModuleSnapshot* snapshot,
                                           ModuleWriteBatch* write_batch,
                                           const std::string& key,
                                           const KeyLookup& lookup) {
  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (write_batch == nullptr) {
    return rocksdb::Status::InvalidArgument("module write batch is unavailable");
  }

  rocksdb::Status status = RequireHashEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  KeyMetadata before = lookup.metadata;
  std::vector<std::string> removed_fields;
  const std::string prefix =
      KeyCodec::EncodeHashDataPrefix(key, lookup.metadata.version);
  status = snapshot->ScanPrefix(
      StorageColumnFamily::kHash, prefix,
      [&removed_fields, &prefix](const rocksdb::Slice& encoded_key,
                                 const rocksdb::Slice& /*value_slice*/) {
        std::string field;
        if (!KeyCodec::ExtractFieldFromHashDataKey(encoded_key, prefix, &field)) {
          return false;
        }
        removed_fields.push_back(std::move(field));
        return true;
      });
  if (!status.ok()) {
    return status;
  }

  for (const auto& field : removed_fields) {
    status = write_batch->Delete(StorageColumnFamily::kHash,
                                 KeyCodec::EncodeHashDataKey(
                                     key, before.version, field));
    if (!status.ok()) {
      return status;
    }
  }
  KeyMetadata after = BuildHashTombstoneMetadata(key_service_, lookup);
  status = key_service_->PutMetadata(write_batch, key, after);
  if (!status.ok()) {
    return status;
  }

  HashMutation mutation;
  mutation.type = HashMutation::Type::kDeleteKey;
  mutation.key = key;
  mutation.deleted_fields = removed_fields;
  mutation.before = before;
  mutation.after = after;
  mutation.existed_before = true;
  mutation.exists_after = false;
  return NotifyObservers(mutation, write_batch);
}

rocksdb::Status HashModule::EnsureReady() const {
  if (services_ == nullptr || key_service_ == nullptr || !started_) {
    return rocksdb::Status::InvalidArgument("hash module is unavailable");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status HashModule::NotifyObservers(const HashMutation& mutation,
                                            ModuleWriteBatch* write_batch) const {
  if (write_batch == nullptr) {
    return rocksdb::Status::InvalidArgument("module write batch is unavailable");
  }
  for (HashObserver* observer : observers_) {
    rocksdb::Status status = observer->OnHashMutation(mutation, write_batch);
    if (!status.ok()) {
      return status;
    }
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
