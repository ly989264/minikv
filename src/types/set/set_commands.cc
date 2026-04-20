#include "types/set/set_commands.h"

#include <memory>
#include <utility>
#include <vector>

#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "types/set/set_module.h"

namespace minikv {

namespace {

class SAddCmd : public Cmd {
 public:
  SAddCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SADD requires at least one member");
    }
    key_ = input.key;
    members_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    uint64_t added = 0;
    rocksdb::Status status = module_->AddMembers(key_, members_, &added);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(added));
  }

  SetModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> members_;
};

class SCardCmd : public Cmd {
 public:
  SCardCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SCARD takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    uint64_t size = 0;
    rocksdb::Status status = module_->Cardinality(key_, &size);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(size));
  }

  SetModule* module_ = nullptr;
  std::string key_;
};

class SMembersCmd : public Cmd {
 public:
  SMembersCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SMEMBERS takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    std::vector<std::string> members;
    rocksdb::Status status = module_->ReadMembers(key_, &members);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeArray(std::move(members));
  }

  SetModule* module_ = nullptr;
  std::string key_;
};

class SIsMemberCmd : public Cmd {
 public:
  SIsMemberCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("SISMEMBER requires member");
    }
    key_ = input.key;
    member_ = input.args[0];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    bool found = false;
    rocksdb::Status status = module_->IsMember(key_, member_, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(found ? 1 : 0);
  }

  SetModule* module_ = nullptr;
  std::string key_;
  std::string member_;
};

class SPopCmd : public Cmd {
 public:
  SPopCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SPOP takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    std::string member;
    bool found = false;
    rocksdb::Status status = module_->PopRandomMember(key_, &member, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeBulkString(std::move(member));
  }

  SetModule* module_ = nullptr;
  std::string key_;
};

class SRandMemberCmd : public Cmd {
 public:
  SRandMemberCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SRANDMEMBER takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    std::string member;
    bool found = false;
    rocksdb::Status status = module_->RandomMember(key_, &member, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeBulkString(std::move(member));
  }

  SetModule* module_ = nullptr;
  std::string key_;
};

class SRemCmd : public Cmd {
 public:
  SRemCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SREM requires at least one member");
    }
    key_ = input.key;
    members_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    uint64_t removed = 0;
    rocksdb::Status status = module_->RemoveMembers(key_, members_, &removed);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(removed));
  }

  SetModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> members_;
};

}  // namespace

rocksdb::Status RegisterSetCommands(ModuleServices& services,
                                    SetModule* module) {
  rocksdb::Status status = services.command_registry().Register(
      {"SADD", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SAddCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SCARD", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SCardCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SMEMBERS", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SMembersCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SISMEMBER", CmdFlags::kRead | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SIsMemberCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SPOP", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SPopCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SRANDMEMBER", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SRandMemberCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SREM", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SRemCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

}  // namespace minikv
