#include "modules/core/core_module.h"

#include <memory>

#include "command/cmd.h"
#include "module/module_services.h"

namespace minikv {
namespace {

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
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "EXISTS takes no extra arguments");
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
    return MakeInteger(lookup.exists ? 1 : 0);
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  std::string key_;
};

}  // namespace

rocksdb::Status CoreModule::OnLoad(ModuleServices& services) {
  rocksdb::Status status = services.exports().Publish<CoreKeyService>(
      kCoreKeyServiceExportName, &key_service_);
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
  return status;
}

rocksdb::Status CoreModule::OnStart(ModuleServices& services) {
  started_ = true;
  services.metrics().SetCounter("worker_count",
                                services.scheduler().worker_count());
  return rocksdb::Status::OK();
}

void CoreModule::OnStop(ModuleServices& /*services*/) { started_ = false; }

}  // namespace minikv
