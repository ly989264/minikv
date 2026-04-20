#include "types/json/json_commands.h"

#include <cctype>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "types/json/json_module.h"

namespace minikv {
namespace {

std::string ToUpper(std::string text) {
  for (char& ch : text) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return text;
}

rocksdb::Status ParseCommandJson(const std::string& text, minijson::Value* value) {
  std::string error;
  if (minijson::Parse(text, value, &error)) {
    return rocksdb::Status::OK();
  }
  return rocksdb::Status::InvalidArgument("invalid JSON value: " + error);
}

minijson::Value MakeJsonArray(const std::vector<minijson::Value>& values) {
  return minijson::Value::ArrayValue(values);
}

std::string SerializeForGet(const minijson::Value& value,
                            const minijson::SerializeOptions& options) {
  return minijson::Serialize(value, options);
}

std::string SerializeMatchesForJsonPath(
    const std::vector<minijson::Value>& matches,
    const minijson::SerializeOptions& options) {
  return minijson::Serialize(MakeJsonArray(matches), options);
}

bool FormatLegacyGet(const JsonGetResult& result, const JsonPath& path,
                     const minijson::SerializeOptions& options,
                     std::string* out) {
  if (out == nullptr) {
    return false;
  }
  if (path.dialect == JsonPathDialect::kJsonPath) {
    *out = SerializeMatchesForJsonPath(result.matches, options);
    return true;
  }
  if (result.matches.empty()) {
    return false;
  }
  if (result.matches.size() == 1) {
    *out = SerializeForGet(result.matches.front(), options);
    return true;
  }
  *out = SerializeMatchesForJsonPath(result.matches, options);
  return true;
}

class JsonSetCmd : public Cmd {
 public:
  JsonSetCmd(const CmdRegistration& registration, JsonModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() < 2 || input.args.size() > 3) {
      return rocksdb::Status::InvalidArgument(
          "JSON.SET requires path, value, and optional NX or XX");
    }
    key_ = input.key;
    SetRouteKey(key_);
    rocksdb::Status status = ParseJsonPath(input.args[0], &path_);
    if (!status.ok()) {
      return status;
    }
    status = ParseCommandJson(input.args[1], &value_);
    if (!status.ok()) {
      return status;
    }
    if (input.args.size() == 3) {
      const std::string option = ToUpper(input.args[2]);
      if (option == "NX") {
        condition_ = JsonSetCondition::kNx;
      } else if (option == "XX") {
        condition_ = JsonSetCondition::kXx;
      } else {
        return rocksdb::Status::InvalidArgument(
            "JSON.SET option must be NX or XX");
      }
    }
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("json module is unavailable"));
    }
    bool applied = false;
    rocksdb::Status status =
        module_->Set(key_, path_, value_, condition_, &applied);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!applied) {
      return MakeNull();
    }
    return MakeSimpleString("OK");
  }

  JsonModule* module_ = nullptr;
  std::string key_;
  JsonPath path_;
  minijson::Value value_;
  JsonSetCondition condition_ = JsonSetCondition::kNone;
};

class JsonGetCmd : public Cmd {
 public:
  JsonGetCmd(const CmdRegistration& registration, JsonModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    key_ = input.key;
    SetRouteKey(key_);

    std::vector<std::string> path_texts;
    size_t index = 0;
    while (index < input.args.size()) {
      const std::string option = ToUpper(input.args[index]);
      if (option == "INDENT" || option == "NEWLINE" || option == "SPACE") {
        if (index + 1 >= input.args.size()) {
          return rocksdb::Status::InvalidArgument(
              "JSON.GET formatting option requires a value");
        }
        if (option == "INDENT") {
          format_.indent = input.args[index + 1];
        } else if (option == "NEWLINE") {
          format_.newline = input.args[index + 1];
        } else {
          format_.space = input.args[index + 1];
        }
        index += 2;
        continue;
      }
      path_texts.assign(input.args.begin() + static_cast<long>(index),
                        input.args.end());
      break;
    }

    if (path_texts.empty()) {
      path_texts.push_back("$");
    }
    for (const auto& text : path_texts) {
      JsonPath path;
      rocksdb::Status status = ParseJsonPath(text, &path);
      if (!status.ok()) {
        return status;
      }
      paths_.push_back(std::move(path));
    }
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("json module is unavailable"));
    }

    if (paths_.size() == 1) {
      JsonGetResult result;
      rocksdb::Status status = module_->Get(key_, paths_[0], &result);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      if (!result.key_exists) {
        return MakeNull();
      }
      std::string encoded;
      if (!FormatLegacyGet(result, paths_[0], format_, &encoded)) {
        if (paths_[0].dialect == JsonPathDialect::kJsonPath) {
          return MakeBulkString(
              SerializeMatchesForJsonPath(result.matches, format_));
        }
        return MakeNull();
      }
      return MakeBulkString(std::move(encoded));
    }

    bool key_exists = false;
    minijson::Value::Object object_entries;
    bool use_json_collections = false;
    for (const auto& path : paths_) {
      if (path.dialect == JsonPathDialect::kJsonPath) {
        use_json_collections = true;
      }
    }
    for (const auto& path : paths_) {
      JsonGetResult result;
      rocksdb::Status status = module_->Get(key_, path, &result);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      if (!result.key_exists) {
        return MakeNull();
      }
      key_exists = true;
      if (use_json_collections || path.dialect == JsonPathDialect::kJsonPath ||
          result.matches.size() != 1) {
        object_entries.emplace_back(path.text, MakeJsonArray(result.matches));
      } else if (result.matches.empty()) {
        object_entries.emplace_back(path.text, minijson::Value::Null());
      } else {
        object_entries.emplace_back(path.text, result.matches.front());
      }
    }
    if (!key_exists) {
      return MakeNull();
    }
    return MakeBulkString(minijson::Serialize(
        minijson::Value::ObjectValue(std::move(object_entries)), format_));
  }

  JsonModule* module_ = nullptr;
  std::string key_;
  std::vector<JsonPath> paths_;
  minijson::SerializeOptions format_;
};

class JsonMGetCmd : public Cmd {
 public:
  JsonMGetCmd(const CmdRegistration& registration, JsonModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key || input.args.size() < 2) {
      return rocksdb::Status::InvalidArgument(
          "JSON.MGET requires one or more keys and a path");
    }
    keys_.push_back(input.key);
    keys_.insert(keys_.end(), input.args.begin(), input.args.end() - 1);
    SetRouteKeys(keys_);
    return ParseJsonPath(input.args.back(), &path_);
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("json module is unavailable"));
    }

    std::vector<ReplyNode> replies;
    replies.reserve(keys_.size());
    for (const auto& key : keys_) {
      JsonGetResult result;
      rocksdb::Status status = module_->Get(key, path_, &result);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      if (!result.key_exists) {
        replies.push_back(ReplyNode::Null());
        continue;
      }
      std::string encoded;
      if (!FormatLegacyGet(result, path_, minijson::SerializeOptions(), &encoded)) {
        if (path_.dialect == JsonPathDialect::kJsonPath) {
          replies.push_back(
              ReplyNode::BulkString(SerializeMatchesForJsonPath(
                  result.matches, minijson::SerializeOptions())));
        } else {
          replies.push_back(ReplyNode::Null());
        }
        continue;
      }
      replies.push_back(ReplyNode::BulkString(std::move(encoded)));
    }
    return MakeArray(std::move(replies));
  }

  JsonModule* module_ = nullptr;
  std::vector<std::string> keys_;
  JsonPath path_;
};

class JsonCountCmd : public Cmd {
 public:
  enum class Mode : uint8_t {
    kDelete = 0,
    kForget = 1,
    kClear = 2,
  };

  JsonCountCmd(const CmdRegistration& registration, JsonModule* module, Mode mode)
      : Cmd(registration.name, registration.flags), module_(module), mode_(mode) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() > 1) {
      return rocksdb::Status::InvalidArgument(
          "JSON path command takes at most one path");
    }
    key_ = input.key;
    SetRouteKey(key_);
    if (!input.args.empty()) {
      return ParseJsonPath(input.args[0], &path_);
    }
    return ParseJsonPath("$", &path_);
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("json module is unavailable"));
    }
    uint64_t count = 0;
    rocksdb::Status status;
    if (mode_ == Mode::kClear) {
      status = module_->ClearPath(key_, path_, &count);
    } else {
      status = module_->DeletePath(key_, path_, &count);
    }
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(count));
  }

  JsonModule* module_ = nullptr;
  Mode mode_ = Mode::kDelete;
  std::string key_;
  JsonPath path_;
};

class JsonTypeCmd : public Cmd {
 public:
  JsonTypeCmd(const CmdRegistration& registration, JsonModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() > 1) {
      return rocksdb::Status::InvalidArgument("JSON.TYPE takes at most one path");
    }
    key_ = input.key;
    SetRouteKey(key_);
    if (!input.args.empty()) {
      return ParseJsonPath(input.args[0], &path_);
    }
    return ParseJsonPath("$", &path_);
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("json module is unavailable"));
    }
    bool key_exists = false;
    std::vector<std::string> types;
    rocksdb::Status status = module_->TypePath(key_, path_, &key_exists, &types);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!key_exists) {
      return MakeNull();
    }
    if (path_.dialect == JsonPathDialect::kJsonPath || types.size() != 1) {
      std::vector<ReplyNode> replies;
      replies.reserve(types.size());
      for (const auto& type : types) {
        replies.push_back(ReplyNode::BulkString(type));
      }
      return MakeArray(std::move(replies));
    }
    if (types.empty()) {
      return MakeNull();
    }
    return MakeBulkString(types.front());
  }

  JsonModule* module_ = nullptr;
  std::string key_;
  JsonPath path_;
};

class JsonToggleCmd : public Cmd {
 public:
  JsonToggleCmd(const CmdRegistration& registration, JsonModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key || input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("JSON.TOGGLE requires one path");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return ParseJsonPath(input.args[0], &path_);
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("json module is unavailable"));
    }
    bool key_exists = false;
    std::vector<JsonToggleResult> results;
    rocksdb::Status status =
        module_->TogglePath(key_, path_, &key_exists, &results);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!key_exists) {
      return MakeNull();
    }
    if (path_.dialect == JsonPathDialect::kJsonPath || results.size() != 1) {
      std::vector<ReplyNode> replies;
      replies.reserve(results.size());
      for (const auto& result : results) {
        if (!result.is_boolean) {
          replies.push_back(ReplyNode::Null());
        } else {
          replies.push_back(ReplyNode::Integer(result.value ? 1 : 0));
        }
      }
      return MakeArray(std::move(replies));
    }
    if (results.empty() || !results.front().is_boolean) {
      return MakeNull();
    }
    return MakeInteger(results.front().value ? 1 : 0);
  }

  JsonModule* module_ = nullptr;
  std::string key_;
  JsonPath path_;
};

class JsonNumIncrByCmd : public Cmd {
 public:
  JsonNumIncrByCmd(const CmdRegistration& registration, JsonModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key || input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument(
          "JSON.NUMINCRBY requires path and increment");
    }
    key_ = input.key;
    SetRouteKey(key_);
    rocksdb::Status status = ParseJsonPath(input.args[0], &path_);
    if (!status.ok()) {
      return status;
    }
    minijson::Value value;
    status = ParseCommandJson(input.args[1], &value);
    if (!status.ok()) {
      return rocksdb::Status::InvalidArgument("increment must be a JSON number");
    }
    if (!value.IsNumber()) {
      return rocksdb::Status::InvalidArgument("increment must be a JSON number");
    }
    increment_ = value.number().value;
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("json module is unavailable"));
    }
    bool key_exists = false;
    std::vector<JsonNumberResult> results;
    rocksdb::Status status =
        module_->NumIncrByPath(key_, path_, increment_, &key_exists, &results);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!key_exists) {
      return MakeNull();
    }
    if (path_.dialect == JsonPathDialect::kJsonPath || results.size() != 1) {
      std::vector<minijson::Value> values;
      values.reserve(results.size());
      for (const auto& result : results) {
        if (result.is_number) {
          values.push_back(result.value);
        } else {
          values.push_back(minijson::Value::Null());
        }
      }
      return MakeBulkString(minijson::Serialize(MakeJsonArray(values)));
    }
    if (results.empty() || !results.front().is_number) {
      return MakeNull();
    }
    return MakeBulkString(minijson::Serialize(results.front().value));
  }

  JsonModule* module_ = nullptr;
  std::string key_;
  JsonPath path_;
  long double increment_ = 0.0L;
};

}  // namespace

rocksdb::Status RegisterJsonCommands(ModuleServices& services,
                                     JsonModule* module) {
  rocksdb::Status status = services.command_registry().Register(
      {"JSON.SET", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<JsonSetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.GET", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<JsonGetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.MGET", CmdFlags::kRead | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<JsonMGetCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.DEL", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<JsonCountCmd>(registration, module,
                                               JsonCountCmd::Mode::kDelete);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.FORGET", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<JsonCountCmd>(registration, module,
                                               JsonCountCmd::Mode::kForget);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.TYPE", CmdFlags::kRead | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<JsonTypeCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.CLEAR", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<JsonCountCmd>(registration, module,
                                               JsonCountCmd::Mode::kClear);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.TOGGLE", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<JsonToggleCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.NUMINCRBY", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<JsonNumIncrByCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

}  // namespace minikv
