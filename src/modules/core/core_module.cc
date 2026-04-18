#include "modules/core/core_module.h"

#include <algorithm>
#include <memory>
#include <unordered_set>
#include <vector>

#include "command/cmd.h"
#include "module/module_services.h"

namespace minikv {
namespace {

std::vector<std::string> CollectKeys(const CmdInput& input) {
  std::vector<std::string> keys;
  if (!input.has_key) {
    return keys;
  }
  keys.reserve(1 + input.args.size());
  keys.push_back(input.key);
  keys.insert(keys.end(), input.args.begin(), input.args.end());
  return keys;
}

class PingCmd : public Cmd {
 public:
  explicit PingCmd(const CmdRegistration& registration)
      : Cmd(registration.name, registration.flags) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (input.has_key || !input.args.empty()) {
      return rocksdb::Status::InvalidArgument("PING takes no arguments");
    }
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override { return MakeSimpleString("PONG"); }
};

class TypeCmd : public Cmd {
 public:
  TypeCmd(const CmdRegistration& registration, ModuleServices* services,
          const CoreKeyService* key_service)
      : Cmd(registration.name, registration.flags),
        services_(services),
        key_service_(key_service) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("TYPE takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (services_ == nullptr || key_service_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("core key service is unavailable"));
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    KeyLookup lookup;
    rocksdb::Status status = key_service_->Lookup(snapshot.get(), key_, &lookup);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!lookup.exists) {
      return MakeBulkString("none");
    }
    return MakeBulkString(key_service_->ObjectTypeName(lookup.metadata.type));
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  std::string key_;
};

class ExistsCmd : public Cmd {
 public:
  ExistsCmd(const CmdRegistration& registration, ModuleServices* services,
            const CoreKeyService* key_service)
      : Cmd(registration.name, registration.flags),
        services_(services),
        key_service_(key_service) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    keys_ = CollectKeys(input);
    SetRouteKeys(keys_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (services_ == nullptr || key_service_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("core key service is unavailable"));
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    long long exists = 0;
    for (const auto& key : keys_) {
      KeyLookup lookup;
      rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      if (lookup.exists) {
        ++exists;
      }
    }
    return MakeInteger(exists);
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  std::vector<std::string> keys_;
};

class DelCmd : public Cmd {
 public:
  DelCmd(const CmdRegistration& registration, ModuleServices* services,
         const CoreKeyService* key_service, CoreModule* module)
      : Cmd(registration.name, registration.flags),
        services_(services),
        key_service_(key_service),
        module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    keys_ = CollectKeys(input);
    SetRouteKeys(keys_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (services_ == nullptr || key_service_ == nullptr || module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("core delete services are unavailable"));
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    std::unique_ptr<ModuleWriteBatch> write_batch =
        services_->storage().CreateWriteBatch();
    std::unordered_set<std::string> processed;
    processed.reserve(keys_.size());

    long long deleted = 0;
    for (const auto& key : keys_) {
      if (!processed.insert(key).second) {
        continue;
      }

      KeyLookup lookup;
      rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      if (!lookup.exists) {
        continue;
      }

      WholeKeyDeleteHandler* handler = module_->FindHandler(lookup.metadata.type);
      if (handler == nullptr) {
        return MakeStatus(
            rocksdb::Status::InvalidArgument("DEL is unsupported for key type"));
      }
      status = handler->DeleteWholeKey(snapshot.get(), write_batch.get(), key,
                                      lookup);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      ++deleted;
    }

    if (deleted == 0) {
      return MakeInteger(0);
    }

    rocksdb::Status status = write_batch->Commit();
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(deleted);
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  CoreModule* module_ = nullptr;
  std::vector<std::string> keys_;
};

}  // namespace

rocksdb::Status CoreModule::OnLoad(ModuleServices& services) {
  rocksdb::Status status = services.exports().Publish<CoreKeyService>(
      kCoreKeyServiceExportName, &key_service_);
  if (!status.ok()) {
    return status;
  }
  status = services.exports().Publish<WholeKeyDeleteRegistry>(
      kWholeKeyDeleteRegistryExportName,
      static_cast<WholeKeyDeleteRegistry*>(this));
  if (!status.ok()) {
    return status;
  }

  ModuleServices* services_ptr = &services;
  status = services.command_registry().Register(
      {"PING", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [](const CmdRegistration& registration) {
         return std::make_unique<PingCmd>(registration);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"TYPE", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<TypeCmd>(registration, services_ptr,
                                          &key_service_);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"EXISTS", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<ExistsCmd>(registration, services_ptr,
                                            &key_service_);
       }});
  if (status.ok()) {
    services.metrics().IncrementCounter("commands.registered");
  }

  status = services.command_registry().Register(
      {"DEL", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<DelCmd>(registration, services_ptr, &key_service_,
                                         this);
       }});
  if (status.ok()) {
    services.metrics().IncrementCounter("commands.registered");
  }
  return status;
}

rocksdb::Status CoreModule::OnStart(ModuleServices& services) {
  started_ = true;
  services.metrics().SetCounter("worker_count",
                                services.scheduler().worker_count());
  return rocksdb::Status::OK();
}

void CoreModule::OnStop(ModuleServices& /*services*/) {
  delete_handlers_.clear();
  started_ = false;
}

rocksdb::Status CoreModule::RegisterHandler(WholeKeyDeleteHandler* handler) {
  if (handler == nullptr) {
    return rocksdb::Status::InvalidArgument("whole-key delete handler is required");
  }

  auto [it, inserted] =
      delete_handlers_.emplace(handler->HandledType(), handler);
  if (!inserted) {
    return rocksdb::Status::InvalidArgument(
        "whole-key delete handler already registered");
  }
  return rocksdb::Status::OK();
}

WholeKeyDeleteHandler* CoreModule::FindHandler(ObjectType type) const {
  auto it = delete_handlers_.find(type);
  if (it == delete_handlers_.end()) {
    return nullptr;
  }
  return it->second;
}

}  // namespace minikv
