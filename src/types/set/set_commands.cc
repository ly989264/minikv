#include "types/set/set_commands.h"

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>

#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "types/set/set_module.h"

namespace minikv {

namespace {

enum class SetCombineKind {
  kUnion,
  kIntersection,
  kDifference,
};

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

CommandResponse MakeIntegerArrayResponse(const std::vector<bool>& values) {
  CommandResponse response;
  response.status = rocksdb::Status::OK();
  std::vector<ReplyNode> replies;
  replies.reserve(values.size());
  for (bool value : values) {
    replies.push_back(ReplyNode::Integer(value ? 1 : 0));
  }
  response.reply = ReplyNode::Array(std::move(replies));
  return response;
}

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

class SMIsMemberCmd : public Cmd {
 public:
  SMIsMemberCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SMISMEMBER requires at least one member");
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
    std::vector<bool> found;
    rocksdb::Status status = module_->IsMembers(key_, members_, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeIntegerArrayResponse(found);
  }

  SetModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> members_;
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
    if (input.args.size() > 1) {
      return rocksdb::Status::InvalidArgument("SPOP takes at most count");
    }
    if (input.args.size() == 1) {
      int64_t parsed_count = 0;
      if (!ParseInt64(input.args[0], &parsed_count) || parsed_count < 0) {
        return rocksdb::Status::InvalidArgument(
            "SPOP requires non-negative integer count");
      }
      has_count_ = true;
      count_ = static_cast<uint64_t>(parsed_count);
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
    if (has_count_) {
      std::vector<std::string> members;
      rocksdb::Status status =
          module_->PopRandomMembers(key_, count_, &members);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      return MakeArray(std::move(members));
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
  bool has_count_ = false;
  uint64_t count_ = 0;
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
    if (input.args.size() > 1) {
      return rocksdb::Status::InvalidArgument(
          "SRANDMEMBER takes at most count");
    }
    if (input.args.size() == 1) {
      int64_t parsed_count = 0;
      if (!ParseInt64(input.args[0], &parsed_count)) {
        return rocksdb::Status::InvalidArgument(
            "SRANDMEMBER requires integer count");
      }
      has_count_ = true;
      count_ = parsed_count;
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
    if (has_count_) {
      std::vector<std::string> members;
      rocksdb::Status status = module_->RandomMembers(key_, count_, &members);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      return MakeArray(std::move(members));
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
  bool has_count_ = false;
  int64_t count_ = 0;
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

class SMoveCmd : public Cmd {
 public:
  SMoveCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing source key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument(
          "SMOVE requires destination and member");
    }
    source_ = input.key;
    destination_ = input.args[0];
    member_ = input.args[1];
    SetRouteKeys({source_, destination_});
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    bool moved = false;
    rocksdb::Status status =
        module_->MoveMember(source_, destination_, member_, &moved);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(moved ? 1 : 0);
  }

  SetModule* module_ = nullptr;
  std::string source_;
  std::string destination_;
  std::string member_;
};

class SetCombineCmd : public Cmd {
 public:
  SetCombineCmd(const CmdRegistration& registration, SetModule* module,
                SetCombineKind kind, bool store_result)
      : Cmd(registration.name, registration.flags),
        module_(module),
        kind_(kind),
        store_result_(store_result) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }

    if (store_result_) {
      if (input.args.empty()) {
        return rocksdb::Status::InvalidArgument(Name() +
                                                " requires at least one key");
      }
      destination_ = input.key;
      keys_ = input.args;

      std::vector<std::string> route_keys;
      route_keys.reserve(1 + keys_.size());
      route_keys.push_back(destination_);
      route_keys.insert(route_keys.end(), keys_.begin(), keys_.end());
      SetRouteKeys(std::move(route_keys));
      return rocksdb::Status::OK();
    }

    keys_ = CollectKeys(input);
    SetRouteKeys(keys_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }

    if (store_result_) {
      uint64_t stored = 0;
      rocksdb::Status status;
      switch (kind_) {
        case SetCombineKind::kUnion:
          status = module_->StoreUnion(destination_, keys_, &stored);
          break;
        case SetCombineKind::kIntersection:
          status = module_->StoreIntersection(destination_, keys_, &stored);
          break;
        case SetCombineKind::kDifference:
          status = module_->StoreDifference(destination_, keys_, &stored);
          break;
      }
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      return MakeInteger(static_cast<long long>(stored));
    }

    std::vector<std::string> members;
    rocksdb::Status status;
    switch (kind_) {
      case SetCombineKind::kUnion:
        status = module_->Union(keys_, &members);
        break;
      case SetCombineKind::kIntersection:
        status = module_->Intersection(keys_, &members);
        break;
      case SetCombineKind::kDifference:
        status = module_->Difference(keys_, &members);
        break;
    }
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeArray(std::move(members));
  }

  SetModule* module_ = nullptr;
  SetCombineKind kind_;
  bool store_result_ = false;
  std::string destination_;
  std::vector<std::string> keys_;
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
      {"SMISMEMBER", CmdFlags::kRead | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SMIsMemberCmd>(registration, module);
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

  status = services.command_registry().Register(
      {"SMOVE", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SMoveCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SUNION", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SetCombineCmd>(
             registration, module, SetCombineKind::kUnion, false);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SINTER", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SetCombineCmd>(
             registration, module, SetCombineKind::kIntersection, false);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SDIFF", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SetCombineCmd>(
             registration, module, SetCombineKind::kDifference, false);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SUNIONSTORE", CmdFlags::kWrite | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SetCombineCmd>(
             registration, module, SetCombineKind::kUnion, true);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SINTERSTORE", CmdFlags::kWrite | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SetCombineCmd>(
             registration, module, SetCombineKind::kIntersection, true);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SDIFFSTORE", CmdFlags::kWrite | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SetCombineCmd>(
             registration, module, SetCombineKind::kDifference, true);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

}  // namespace minikv
