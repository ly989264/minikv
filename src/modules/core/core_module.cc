#include "modules/core/core_module.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <limits>
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

bool IsLive(const KeyLookup& lookup) {
  return lookup.state == KeyLifecycleState::kLive;
}

bool ParseInt64(const std::string& input, int64_t* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  errno = 0;
  char* parse_end = nullptr;
  const long long parsed = std::strtoll(input.c_str(), &parse_end, 10);
  if (parse_end == nullptr || *parse_end != '\0' || errno == ERANGE) {
    return false;
  }
  *value = static_cast<int64_t>(parsed);
  return true;
}

uint64_t ComputeExpireAtMs(uint64_t now_ms, int64_t ttl_seconds) {
  const uint64_t max_value = std::numeric_limits<uint64_t>::max();
  const uint64_t ttl_seconds_u = static_cast<uint64_t>(ttl_seconds);
  if (ttl_seconds_u > max_value / 1000) {
    return max_value;
  }
  const uint64_t ttl_ms = ttl_seconds_u * 1000;
  if (ttl_ms > max_value - now_ms) {
    return max_value;
  }
  return now_ms + ttl_ms;
}

rocksdb::Status DeleteLiveKey(CoreModule* module, ModuleSnapshot* snapshot,
                              ModuleWriteBatch* write_batch,
                              const std::string& key,
                              const KeyLookup& lookup) {
  if (module == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "core delete services are unavailable");
  }

  WholeKeyDeleteHandler* handler = module->FindHandler(lookup.metadata.type);
  if (handler == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "DEL is unsupported for key type");
  }
  return handler->DeleteWholeKey(snapshot, write_batch, key, lookup);
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
    if (!IsLive(lookup)) {
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
      if (IsLive(lookup)) {
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
      if (!IsLive(lookup)) {
        continue;
      }

      status =
          DeleteLiveKey(module_, snapshot.get(), write_batch.get(), key, lookup);
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

class ExpireCmd : public Cmd {
 public:
  ExpireCmd(const CmdRegistration& registration, ModuleServices* services,
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
    if (input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("EXPIRE requires seconds");
    }
    if (!ParseInt64(input.args[0], &ttl_seconds_)) {
      return rocksdb::Status::InvalidArgument(
          "EXPIRE requires integer seconds");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (services_ == nullptr || key_service_ == nullptr || module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("core expire services are unavailable"));
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    KeyLookup lookup;
    rocksdb::Status status = key_service_->Lookup(snapshot.get(), key_, &lookup);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!IsLive(lookup)) {
      return MakeInteger(0);
    }

    std::unique_ptr<ModuleWriteBatch> write_batch =
        services_->storage().CreateWriteBatch();
    if (ttl_seconds_ <= 0) {
      status = DeleteLiveKey(module_, snapshot.get(), write_batch.get(), key_,
                             lookup);
    } else {
      KeyMetadata metadata = lookup.metadata;
      metadata.expire_at_ms =
          ComputeExpireAtMs(key_service_->CurrentTimeMs(), ttl_seconds_);
      status = key_service_->PutMetadata(write_batch.get(), key_, metadata);
    }
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }

    status = write_batch->Commit();
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(1);
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  CoreModule* module_ = nullptr;
  std::string key_;
  int64_t ttl_seconds_ = 0;
};

class TtlCmd : public Cmd {
 public:
  TtlCmd(const CmdRegistration& registration, ModuleServices* services,
         const CoreKeyService* key_service, bool return_millis)
      : Cmd(registration.name, registration.flags),
        services_(services),
        key_service_(key_service),
        return_millis_(return_millis) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(Name() +
                                              " takes no extra arguments");
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

    const int64_t ttl_ms = key_service_->GetRemainingTtlMs(lookup);
    if (return_millis_ || ttl_ms < 0) {
      return MakeInteger(ttl_ms);
    }
    return MakeInteger(ttl_ms / 1000);
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  bool return_millis_ = false;
  std::string key_;
};

class PersistCmd : public Cmd {
 public:
  PersistCmd(const CmdRegistration& registration, ModuleServices* services,
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
          "PERSIST takes no extra arguments");
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
    if (!IsLive(lookup) || lookup.metadata.expire_at_ms == 0) {
      return MakeInteger(0);
    }

    KeyMetadata metadata = lookup.metadata;
    metadata.expire_at_ms = 0;
    std::unique_ptr<ModuleWriteBatch> write_batch =
        services_->storage().CreateWriteBatch();
    status = key_service_->PutMetadata(write_batch.get(), key_, metadata);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    status = write_batch->Commit();
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(1);
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  std::string key_;
};

}  // namespace

CoreModule::CoreModule(TimeSource time_source)
    : key_service_(std::move(time_source)) {}

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
      {"EXPIRE", CmdFlags::kWrite | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<ExpireCmd>(registration, services_ptr,
                                            &key_service_, this);
       }});
  if (status.ok()) {
    services.metrics().IncrementCounter("commands.registered");
  }

  status = services.command_registry().Register(
      {"TTL", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<TtlCmd>(registration, services_ptr,
                                         &key_service_, false);
       }});
  if (status.ok()) {
    services.metrics().IncrementCounter("commands.registered");
  }

  status = services.command_registry().Register(
      {"PTTL", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<TtlCmd>(registration, services_ptr,
                                         &key_service_, true);
       }});
  if (status.ok()) {
    services.metrics().IncrementCounter("commands.registered");
  }

  status = services.command_registry().Register(
      {"PERSIST", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<PersistCmd>(registration, services_ptr,
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
