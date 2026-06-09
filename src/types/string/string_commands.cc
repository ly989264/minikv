#include "types/string/string_commands.h"

#include <cctype>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "types/string/string_module.h"

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

bool ParseInt64Strict(const std::string& input, int64_t* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  const bool negative = input.front() == '-';
  size_t index = negative ? 1 : 0;
  if (index == input.size()) {
    return false;
  }

  const uint64_t limit =
      negative ? static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1
               : static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
  uint64_t parsed = 0;
  for (; index < input.size(); ++index) {
    const unsigned char ch = static_cast<unsigned char>(input[index]);
    if (!std::isdigit(ch)) {
      return false;
    }
    const uint64_t digit = static_cast<uint64_t>(ch - '0');
    if (parsed > (limit - digit) / 10) {
      return false;
    }
    parsed = parsed * 10 + digit;
  }

  if (negative) {
    if (parsed == limit) {
      *value = std::numeric_limits<int64_t>::min();
    } else {
      *value = -static_cast<int64_t>(parsed);
    }
  } else {
    *value = static_cast<int64_t>(parsed);
  }
  return true;
}

bool ParseUint64Strict(const std::string& input, uint64_t* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  uint64_t parsed = 0;
  for (char c : input) {
    const unsigned char ch = static_cast<unsigned char>(c);
    if (!std::isdigit(ch)) {
      return false;
    }
    const uint64_t digit = static_cast<uint64_t>(ch - '0');
    if (parsed > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
      return false;
    }
    parsed = parsed * 10 + digit;
  }
  *value = parsed;
  return true;
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
    rocksdb::Status status = module_->ReplaceValue(key_, value_);
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

class MGetCmd : public Cmd {
 public:
  MGetCmd(const CmdRegistration& registration, StringModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

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
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("string module is unavailable"));
    }
    std::vector<StringValue> values;
    rocksdb::Status status = module_->GetValues(keys_, &values);
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

  StringModule* module_ = nullptr;
  std::vector<std::string> keys_;
};

class MSetCmd : public Cmd {
 public:
  MSetCmd(const CmdRegistration& registration, StringModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty() || input.args.size() % 2 == 0) {
      return rocksdb::Status::InvalidArgument(
          "MSET requires one or more key/value pairs");
    }

    values_.reserve((1 + input.args.size()) / 2);
    values_.push_back({input.key, input.args[0]});
    std::vector<std::string> keys;
    keys.reserve(values_.capacity());
    keys.push_back(input.key);
    for (size_t index = 1; index < input.args.size(); index += 2) {
      values_.push_back({input.args[index], input.args[index + 1]});
      keys.push_back(input.args[index]);
    }
    SetRouteKeys(std::move(keys));
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("string module is unavailable"));
    }
    rocksdb::Status status = module_->SetValues(values_);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeSimpleString("OK");
  }

  StringModule* module_ = nullptr;
  std::vector<std::pair<std::string, std::string>> values_;
};

class AppendCmd : public Cmd {
 public:
  AppendCmd(const CmdRegistration& registration, StringModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("APPEND requires value");
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
    uint64_t length = 0;
    rocksdb::Status status = module_->AppendValue(key_, value_, &length);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(length));
  }

  StringModule* module_ = nullptr;
  std::string key_;
  std::string value_;
};

class GetRangeCmd : public Cmd {
 public:
  GetRangeCmd(const CmdRegistration& registration, StringModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument("GETRANGE requires start and end");
    }
    if (!ParseInt64Strict(input.args[0], &start_) ||
        !ParseInt64Strict(input.args[1], &end_)) {
      return rocksdb::Status::InvalidArgument(
          "GETRANGE requires integer start and end");
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
    rocksdb::Status status = module_->GetRange(key_, start_, end_, &value);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeBulkString(std::move(value));
  }

  StringModule* module_ = nullptr;
  std::string key_;
  int64_t start_ = 0;
  int64_t end_ = 0;
};

class SetRangeCmd : public Cmd {
 public:
  SetRangeCmd(const CmdRegistration& registration, StringModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument(
          "SETRANGE requires offset and value");
    }
    if (!ParseUint64Strict(input.args[0], &offset_)) {
      return rocksdb::Status::InvalidArgument(
          "SETRANGE requires non-negative integer offset");
    }
    key_ = input.key;
    value_ = input.args[1];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("string module is unavailable"));
    }
    uint64_t length = 0;
    rocksdb::Status status =
        module_->SetRange(key_, offset_, value_, &length);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(length));
  }

  StringModule* module_ = nullptr;
  std::string key_;
  std::string value_;
  uint64_t offset_ = 0;
};

class GetSetCmd : public Cmd {
 public:
  GetSetCmd(const CmdRegistration& registration, StringModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("GETSET requires value");
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
    std::string old_value;
    bool found = false;
    rocksdb::Status status =
        module_->GetSetValue(key_, value_, &old_value, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeBulkString(std::move(old_value));
  }

  StringModule* module_ = nullptr;
  std::string key_;
  std::string value_;
};

class IntegerMutationCmd : public Cmd {
 public:
  enum class Kind {
    kIncrement,
    kDecrement,
  };

  IntegerMutationCmd(const CmdRegistration& registration, StringModule* module,
                     Kind kind, bool explicit_amount)
      : Cmd(registration.name, registration.flags),
        module_(module),
        kind_(kind),
        explicit_amount_(explicit_amount) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (explicit_amount_) {
      if (input.args.size() != 1) {
        return rocksdb::Status::InvalidArgument(Name() +
                                                " requires increment");
      }
      if (!ParseInt64Strict(input.args[0], &amount_)) {
        return rocksdb::Status::InvalidArgument(Name() +
                                                " requires integer increment");
      }
    } else if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(Name() +
                                              " takes no extra arguments");
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
    int64_t value = 0;
    rocksdb::Status status =
        kind_ == Kind::kIncrement
            ? module_->IncrementBy(key_, amount_, &value)
            : module_->DecrementBy(key_, amount_, &value);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(value);
  }

  StringModule* module_ = nullptr;
  Kind kind_ = Kind::kIncrement;
  bool explicit_amount_ = false;
  std::string key_;
  int64_t amount_ = 1;
};

}  // namespace

rocksdb::Status RegisterStringCommands(ModuleServices& services,
                                       StringModule* module) {
  auto register_command = [&services](CmdRegistration registration) {
    rocksdb::Status status =
        services.command_registry().Register(std::move(registration));
    if (status.ok()) {
      services.metrics().IncrementCounter("commands.registered");
    }
    return status;
  };

  rocksdb::Status status = register_command(
      {"SET", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<SetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"GET", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<GetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"STRLEN", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<StrlenCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"MGET", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<MGetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"MSET", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<MSetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"APPEND", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<AppendCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"GETRANGE", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<GetRangeCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"SETRANGE", CmdFlags::kWrite | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<SetRangeCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"GETSET", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<GetSetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"INCR", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<IntegerMutationCmd>(
             registration, module, IntegerMutationCmd::Kind::kIncrement,
             false);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"DECR", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<IntegerMutationCmd>(
             registration, module, IntegerMutationCmd::Kind::kDecrement,
             false);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"INCRBY", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<IntegerMutationCmd>(
             registration, module, IntegerMutationCmd::Kind::kIncrement, true);
       }});
  if (!status.ok()) {
    return status;
  }

  status = register_command(
      {"DECRBY", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<IntegerMutationCmd>(
             registration, module, IntegerMutationCmd::Kind::kDecrement, true);
       }});
  if (!status.ok()) {
    return status;
  }

  return rocksdb::Status::OK();
}

}  // namespace minikv
