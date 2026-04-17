#include "modules/hash/hash_module.h"

#include <memory>
#include <utility>

#include "codec/key_codec.h"
#include "command/cmd.h"
#include "module/module_services.h"

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

class HSetCmd : public Cmd {
 public:
  HSetCmd(const CmdRegistration& registration, HashModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument("HSET requires field and value");
    }
    key_ = input.key;
    field_ = input.args[0];
    value_ = input.args[1];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("hash module is unavailable"));
    }
    bool inserted = false;
    rocksdb::Status status =
        module_->PutField(key_, field_, value_, &inserted);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(inserted ? 1 : 0);
  }

  HashModule* module_ = nullptr;
  std::string key_;
  std::string field_;
  std::string value_;
};

class HGetAllCmd : public Cmd {
 public:
  HGetAllCmd(const CmdRegistration& registration, HashModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "HGETALL takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("hash module is unavailable"));
    }
    std::vector<FieldValue> values;
    rocksdb::Status status = module_->ReadAll(key_, &values);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }

    std::vector<std::string> flattened;
    flattened.reserve(values.size() * 2);
    for (const auto& item : values) {
      flattened.push_back(item.field);
      flattened.push_back(item.value);
    }
    return MakeArray(std::move(flattened));
  }

  HashModule* module_ = nullptr;
  std::string key_;
};

class HDelCmd : public Cmd {
 public:
  HDelCmd(const CmdRegistration& registration, HashModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument("HDEL requires at least one field");
    }
    key_ = input.key;
    fields_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("hash module is unavailable"));
    }
    uint64_t deleted = 0;
    rocksdb::Status status =
        module_->DeleteFields(key_, fields_, &deleted);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(deleted));
  }

  HashModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> fields_;
};

}  // namespace

rocksdb::Status HashModule::OnLoad(ModuleServices& services) {
  services_ = &services;

  rocksdb::Status status = services.command_registry().Register(
      {"HSET", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<HSetCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"HGETALL", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin,
       "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<HGetAllCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"HDEL", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<HDelCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");
  return rocksdb::Status::OK();
}

rocksdb::Status HashModule::OnStart(ModuleServices& services) {
  started_ = true;
  services.metrics().SetCounter("worker_count",
                                services.scheduler().worker_count());
  return rocksdb::Status::OK();
}

void HashModule::OnStop(ModuleServices& /*services*/) {
  started_ = false;
  services_ = nullptr;
}

rocksdb::Status HashModule::PutField(const std::string& key,
                                     const std::string& field,
                                     const std::string& value,
                                     bool* inserted) {
  if (inserted != nullptr) {
    *inserted = false;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
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

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = write_batch->Put(StorageColumnFamily::kMeta,
                            KeyCodec::EncodeMetaKey(key),
                            KeyCodec::EncodeMetaValue(after));
  if (!status.ok()) {
    return status;
  }
  status = write_batch->Put(StorageColumnFamily::kHash, data_key, value);
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (inserted != nullptr) {
    *inserted = !field_exists && status.ok();
  }
  return status;
}

rocksdb::Status HashModule::ReadAll(const std::string& key,
                                    std::vector<FieldValue>* out) {
  out->clear();

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
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

  const std::string prefix =
      KeyCodec::EncodeHashDataPrefix(key, metadata.version);
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

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
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
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  std::string scratch;
  for (const auto& field : fields) {
    const std::string data_key =
        KeyCodec::EncodeHashDataKey(key, before.version, field);
    status = snapshot->Get(StorageColumnFamily::kHash, data_key, &scratch);
    if (status.ok()) {
      status = write_batch->Delete(StorageColumnFamily::kHash, data_key);
      if (!status.ok()) {
        return status;
      }
      ++removed;
    } else if (!status.IsNotFound()) {
      return status;
    }
  }

  if (removed == 0) {
    return rocksdb::Status::OK();
  }

  KeyMetadata after = before;
  if (removed >= before.size) {
    after.size = 0;
    status =
        write_batch->Delete(StorageColumnFamily::kMeta,
                            KeyCodec::EncodeMetaKey(key));
  } else {
    after.size -= removed;
    status = write_batch->Put(StorageColumnFamily::kMeta,
                              KeyCodec::EncodeMetaKey(key),
                              KeyCodec::EncodeMetaValue(after));
  }
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (deleted != nullptr) {
    *deleted = status.ok() ? removed : 0;
  }
  return status;
}

rocksdb::Status HashModule::EnsureReady() const {
  if (services_ == nullptr || !started_) {
    return rocksdb::Status::InvalidArgument("hash module is unavailable");
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
