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
    if (input.args.empty() || input.args.size() % 2 != 0) {
      return rocksdb::Status::InvalidArgument(
          "HSET requires one or more field/value pairs");
    }
    key_ = input.key;
    values_.clear();
    values_.reserve(input.args.size() / 2);
    for (size_t i = 0; i < input.args.size(); i += 2) {
      values_.push_back(FieldValue{input.args[i], input.args[i + 1]});
    }
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("hash module is unavailable"));
    }
    uint64_t inserted = 0;
    rocksdb::Status status = module_->PutFields(key_, values_, &inserted);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(inserted));
  }

  HashModule* module_ = nullptr;
  std::string key_;
  std::vector<FieldValue> values_;
};

class HGetCmd : public Cmd {
 public:
  HGetCmd(const CmdRegistration& registration, HashModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("HGET requires field");
    }
    key_ = input.key;
    field_ = input.args[0];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("hash module is unavailable"));
    }
    FieldLookup lookup;
    rocksdb::Status status = module_->ReadField(key_, field_, &lookup);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!lookup.found) {
      return MakeNull();
    }
    return MakeBulkString(std::move(lookup.value));
  }

  HashModule* module_ = nullptr;
  std::string key_;
  std::string field_;
};

class HMGetCmd : public Cmd {
 public:
  HMGetCmd(const CmdRegistration& registration, HashModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "HMGET requires at least one field");
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
    std::vector<FieldLookup> values;
    rocksdb::Status status = module_->ReadFields(key_, fields_, &values);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }

    std::vector<ReplyNode> replies;
    replies.reserve(values.size());
    for (auto& value : values) {
      if (value.found) {
        replies.push_back(ReplyNode::BulkString(std::move(value.value)));
      } else {
        replies.push_back(ReplyNode::Null());
      }
    }
    return MakeArray(std::move(replies));
  }

  HashModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> fields_;
};

class HLenCmd : public Cmd {
 public:
  HLenCmd(const CmdRegistration& registration, HashModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("HLEN takes no extra arguments");
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
    uint64_t length = 0;
    rocksdb::Status status = module_->Length(key_, &length);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(length));
  }

  HashModule* module_ = nullptr;
  std::string key_;
};

class HExistsCmd : public Cmd {
 public:
  HExistsCmd(const CmdRegistration& registration, HashModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("HEXISTS requires field");
    }
    key_ = input.key;
    field_ = input.args[0];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("hash module is unavailable"));
    }
    bool exists = false;
    rocksdb::Status status = module_->FieldExists(key_, field_, &exists);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(exists ? 1 : 0);
  }

  HashModule* module_ = nullptr;
  std::string key_;
  std::string field_;
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

class HKeysCmd : public Cmd {
 public:
  HKeysCmd(const CmdRegistration& registration, HashModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("HKEYS takes no extra arguments");
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

    std::vector<std::string> keys;
    keys.reserve(values.size());
    for (auto& value : values) {
      keys.push_back(std::move(value.field));
    }
    return MakeArray(std::move(keys));
  }

  HashModule* module_ = nullptr;
  std::string key_;
};

class HValsCmd : public Cmd {
 public:
  HValsCmd(const CmdRegistration& registration, HashModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("HVALS takes no extra arguments");
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

    std::vector<std::string> replies;
    replies.reserve(values.size());
    for (auto& value : values) {
      replies.push_back(std::move(value.value));
    }
    return MakeArray(std::move(replies));
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
  auto register_command = [&services](CmdRegistration registration) {
    rocksdb::Status status =
        services.command_registry().Register(std::move(registration));
    if (status.ok()) {
      services.metrics().IncrementCounter("commands.registered");
    }
    return status;
  };

  rocksdb::Status status = register_command(
      {"HSET", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<HSetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"HGET", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<HGetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"HMGET", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<HMGetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"HLEN", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<HLenCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"HEXISTS", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<HExistsCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"HGETALL", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<HGetAllCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"HKEYS", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<HKeysCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"HVALS", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<HValsCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"HDEL", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<HDelCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  return rocksdb::Status::OK();
}

}  // namespace minikv
