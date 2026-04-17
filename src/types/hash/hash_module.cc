#include "types/hash/hash_module.h"

#include <memory>
#include <utility>

#include "engine/key_codec.h"
#include "kernel/mutation_hook.h"
#include "kernel/snapshot.h"
#include "kernel/storage_engine.h"
#include "kernel/write_context.h"

namespace minikv {

namespace {

rocksdb::Status DecodeHashMetadata(const std::string& key,
                                   const std::string& raw_meta,
                                   KeyMetadata* metadata) {
  if (!KeyCodec::DecodeMetaValue(raw_meta, metadata) ||
      metadata->type != ValueType::kHash) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }
  return rocksdb::Status::OK();
}

}  // namespace

HashModule::HashModule(StorageEngine* storage_engine, MutationHook* mutation_hook)
    : storage_engine_(storage_engine), mutation_hook_(mutation_hook) {}

rocksdb::Status HashModule::PutField(const std::string& key,
                                     const std::string& field,
                                     const std::string& value,
                                     bool* inserted) {
  if (inserted != nullptr) {
    *inserted = false;
  }

  std::unique_ptr<Snapshot> snapshot = storage_engine_->CreateSnapshot();
  std::string raw_meta;
  KeyMetadata before;
  bool existed_before = false;
  rocksdb::Status status =
      snapshot->Get(StorageColumnFamily::kMeta, KeyCodec::EncodeMetaKey(key),
                    &raw_meta);
  if (status.ok()) {
    existed_before = true;
    status = DecodeHashMetadata(key, raw_meta, &before);
    if (!status.ok()) {
      return status;
    }
  } else if (!status.IsNotFound()) {
    return status;
  }

  if (!existed_before) {
    before.type = ValueType::kHash;
    before.encoding = ValueEncoding::kHashPlain;
    before.version = 1;
    before.size = 0;
    before.expire_at_ms = 0;
  }

  const std::string data_key =
      KeyCodec::EncodeHashDataKey(key, before.version, field);
  std::string existing_value;
  status = snapshot->Get(StorageColumnFamily::kHash, data_key, &existing_value);
  bool field_exists = false;
  if (status.ok()) {
    field_exists = true;
  } else if (!status.IsNotFound()) {
    return status;
  }

  KeyMetadata after = before;
  if (!field_exists) {
    ++after.size;
  }

  WriteContext write_context(storage_engine_);
  status = write_context.Put(StorageColumnFamily::kMeta,
                             KeyCodec::EncodeMetaKey(key),
                             KeyCodec::EncodeMetaValue(after));
  if (!status.ok()) {
    return status;
  }
  status = write_context.Put(StorageColumnFamily::kHash, data_key, value);
  if (!status.ok()) {
    return status;
  }

  HashMutation mutation;
  mutation.type = HashMutation::Type::kPutField;
  mutation.key = key;
  mutation.values.push_back(FieldValue{field, value});
  mutation.before = before;
  mutation.after = after;
  mutation.existed_before = existed_before;
  mutation.exists_after = true;
  status = mutation_hook_->OnHashMutation(mutation, &write_context);
  if (!status.ok()) {
    return status;
  }

  status = write_context.Commit();
  if (inserted != nullptr) {
    *inserted = !field_exists && status.ok();
  }
  return status;
}

rocksdb::Status HashModule::ReadAll(const std::string& key,
                                    std::vector<FieldValue>* out) {
  out->clear();

  std::unique_ptr<Snapshot> snapshot = storage_engine_->CreateSnapshot();
  std::string raw_meta;
  rocksdb::Status status =
      snapshot->Get(StorageColumnFamily::kMeta, KeyCodec::EncodeMetaKey(key),
                    &raw_meta);
  if (status.IsNotFound()) {
    return rocksdb::Status::OK();
  }
  if (!status.ok()) {
    return status;
  }

  KeyMetadata metadata;
  status = DecodeHashMetadata(key, raw_meta, &metadata);
  if (!status.ok()) {
    return status;
  }

  const std::string prefix = KeyCodec::EncodeHashDataPrefix(key, metadata.version);
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

rocksdb::Status HashModule::DeleteFields(const std::string& key,
                                         const std::vector<std::string>& fields,
                                         uint64_t* deleted) {
  if (deleted != nullptr) {
    *deleted = 0;
  }

  std::unique_ptr<Snapshot> snapshot = storage_engine_->CreateSnapshot();
  std::string raw_meta;
  KeyMetadata before;
  rocksdb::Status status =
      snapshot->Get(StorageColumnFamily::kMeta, KeyCodec::EncodeMetaKey(key),
                    &raw_meta);
  if (status.IsNotFound()) {
    return rocksdb::Status::OK();
  }
  if (!status.ok()) {
    return status;
  }
  status = DecodeHashMetadata(key, raw_meta, &before);
  if (!status.ok()) {
    return status;
  }

  uint64_t removed = 0;
  std::vector<std::string> removed_fields;
  removed_fields.reserve(fields.size());
  WriteContext write_context(storage_engine_);
  std::string scratch;
  for (const auto& field : fields) {
    const std::string data_key =
        KeyCodec::EncodeHashDataKey(key, before.version, field);
    status = snapshot->Get(StorageColumnFamily::kHash, data_key, &scratch);
    if (status.ok()) {
      status = write_context.Delete(StorageColumnFamily::kHash, data_key);
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
  bool exists_after = true;
  if (removed >= before.size) {
    exists_after = false;
    after.size = 0;
    status = write_context.Delete(StorageColumnFamily::kMeta,
                                  KeyCodec::EncodeMetaKey(key));
  } else {
    after.size -= removed;
    status = write_context.Put(StorageColumnFamily::kMeta,
                               KeyCodec::EncodeMetaKey(key),
                               KeyCodec::EncodeMetaValue(after));
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
  mutation.exists_after = exists_after;
  status = mutation_hook_->OnHashMutation(mutation, &write_context);
  if (!status.ok()) {
    return status;
  }

  status = write_context.Commit();
  if (deleted != nullptr) {
    *deleted = status.ok() ? removed : 0;
  }
  return status;
}

}  // namespace minikv
