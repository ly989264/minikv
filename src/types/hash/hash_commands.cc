#include "types/hash/hash_commands.h"

#include <memory>
#include <utility>
#include <vector>

#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "types/hash/hash_module.h"

namespace minikv {

namespace {

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
    rocksdb::Status status = module_->PutField(key_, field_, value_, &inserted);
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
      return rocksdb::Status::InvalidArgument(
          "HDEL requires at least one field");
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
    rocksdb::Status status = module_->DeleteFields(key_, fields_, &deleted);
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

rocksdb::Status RegisterHashCommands(ModuleServices& services,
                                     HashModule* module) {
  rocksdb::Status status = services.command_registry().Register(
      {"HSET", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<HSetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"HGETALL", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<HGetAllCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"HDEL", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<HDelCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

}  // namespace minikv
