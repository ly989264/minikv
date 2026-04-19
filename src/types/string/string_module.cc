#include "types/string/string_module.h"

#include <memory>
#include <utility>

#include "core/key_service.h"
#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"

namespace minikv {

namespace {

rocksdb::Status RequireStringEncoding(const KeyLookup& lookup) {
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  if (lookup.metadata.type != ObjectType::kString ||
      lookup.metadata.encoding != ObjectEncoding::kRaw) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }
  return rocksdb::Status::OK();
}

KeyMetadata BuildStringMetadata(const CoreKeyService* key_service,
                                const KeyLookup& lookup) {
  if (lookup.exists) {
    return lookup.metadata;
  }
  return key_service->MakeMetadata(ObjectType::kString, ObjectEncoding::kRaw,
                                   lookup);
}

KeyMetadata BuildStringTombstoneMetadata(const CoreKeyService* key_service,
                                         const KeyLookup& lookup) {
  KeyMetadata metadata = key_service->MakeTombstoneMetadata(lookup);
  metadata.size = 0;
  return metadata;
}

class SetCmd : public Cmd {
 public:
  SetCmd(const CmdRegistration& registration, StringModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("SET requires value");
    }
    key_ = input.key;
    value_ = input.args[0];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("string module is unavailable"));
    }
    rocksdb::Status status = module_->SetValue(key_, value_);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeSimpleString("OK");
  }

  StringModule* module_ = nullptr;
  std::string key_;
  std::string value_;
};

class GetCmd : public Cmd {
 public:
  GetCmd(const CmdRegistration& registration, StringModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("GET takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("string module is unavailable"));
    }
    std::string value;
    bool found = false;
    rocksdb::Status status = module_->GetValue(key_, &value, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeBulkString(std::move(value));
  }

  StringModule* module_ = nullptr;
  std::string key_;
};

class StrlenCmd : public Cmd {
 public:
  StrlenCmd(const CmdRegistration& registration, StringModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "STRLEN takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("string module is unavailable"));
    }
    uint64_t length = 0;
    rocksdb::Status status = module_->Length(key_, &length);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(length));
  }

  StringModule* module_ = nullptr;
  std::string key_;
};

}  // namespace

rocksdb::Status StringModule::OnLoad(ModuleServices& services) {
  services_ = &services;

  rocksdb::Status status = services.exports().Publish<StringBridge>(
      kStringBridgeExportName, static_cast<StringBridge*>(this));
  if (!status.ok()) {
    return status;
  }

  status = services.command_registry().Register(
      {"SET", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<SetCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"GET", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<GetCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"STRLEN", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [this](const CmdRegistration& registration) {
         return std::make_unique<StrlenCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

rocksdb::Status StringModule::OnStart(ModuleServices& services) {
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

void StringModule::OnStop(ModuleServices& /*services*/) {
  started_ = false;
  delete_registry_ = nullptr;
  key_service_ = nullptr;
  services_ = nullptr;
}

rocksdb::Status StringModule::SetValue(const std::string& key,
                                       const std::string& value) {
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
  status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  KeyMetadata after = BuildStringMetadata(key_service_, lookup);
  after.size = value.size();

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = write_batch->Put(data_keyspace, key, value);
  if (!status.ok()) {
    return status;
  }
  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }
  return write_batch->Commit();
}

rocksdb::Status StringModule::GetValue(const std::string& key, std::string* value,
                                       bool* found) {
  if (value == nullptr) {
    return rocksdb::Status::InvalidArgument("string value output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("string found output is required");
  }
  value->clear();
  *found = false;

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
  status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  status = snapshot->Get(data_keyspace, key, value);
  if (status.ok()) {
    *found = true;
    return rocksdb::Status::OK();
  }
  if (status.IsNotFound()) {
    return rocksdb::Status::Corruption("string value is missing");
  }
  return status;
}

rocksdb::Status StringModule::Length(const std::string& key, uint64_t* length) {
  if (length == nullptr) {
    return rocksdb::Status::InvalidArgument("string length output is required");
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
  status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  *length = lookup.metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status StringModule::DeleteWholeKey(ModuleSnapshot* snapshot,
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

  rocksdb::Status status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  status = write_batch->Delete(data_keyspace, key);
  if (!status.ok()) {
    return status;
  }

  const KeyMetadata after = BuildStringTombstoneMetadata(key_service_, lookup);
  return key_service_->PutMetadata(write_batch, key, after);
}

rocksdb::Status StringModule::EnsureReady() const {
  if (services_ == nullptr || key_service_ == nullptr || !started_) {
    return rocksdb::Status::InvalidArgument("string module is unavailable");
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
