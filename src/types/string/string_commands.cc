#include "types/string/string_commands.h"

#include <memory>
#include <utility>

#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "types/string/string_module.h"

namespace minikv {

namespace {

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

rocksdb::Status RegisterStringCommands(ModuleServices& services,
                                       StringModule* module) {
  rocksdb::Status status = services.command_registry().Register(
      {"SET", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<SetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"GET", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<GetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"STRLEN", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<StrlenCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

}  // namespace minikv
