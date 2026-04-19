#include "types/json/json_module.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "core/key_service.h"
#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"

namespace minikv {
namespace {

std::string ToUpper(std::string text) {
  for (char& ch : text) {
    ch = static_cast<char>(
        std::toupper(static_cast<unsigned char>(ch)));
  }
  return text;
}

rocksdb::Status RequireJsonEncoding(const KeyLookup& lookup) {
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  if (lookup.metadata.type != ObjectType::kJson ||
      lookup.metadata.encoding != ObjectEncoding::kJsonDocument) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }
  return rocksdb::Status::OK();
}

KeyMetadata BuildJsonMetadata(const CoreKeyService* key_service,
                              const KeyLookup& lookup) {
  if (lookup.exists) {
    return lookup.metadata;
  }
  return key_service->MakeMetadata(ObjectType::kJson,
                                   ObjectEncoding::kJsonDocument, lookup);
}

KeyMetadata BuildJsonTombstoneMetadata(const CoreKeyService* key_service,
                                       const KeyLookup& lookup) {
  KeyMetadata metadata = key_service->MakeTombstoneMetadata(lookup);
  metadata.size = 0;
  return metadata;
}

std::string JsonTypeName(const minijson::Value& value) {
  switch (value.type()) {
    case minijson::Value::Type::kNull:
      return "null";
    case minijson::Value::Type::kBool:
      return "boolean";
    case minijson::Value::Type::kNumber:
      return value.number().integral ? "integer" : "number";
    case minijson::Value::Type::kString:
      return "string";
    case minijson::Value::Type::kArray:
      return "array";
    case minijson::Value::Type::kObject:
      return "object";
  }
  return "unknown";
}

std::string SerializeResolvedPath(const JsonResolvedPath& path) {
  std::string out;
  for (const auto& segment : path.segments) {
    out.push_back('/');
    if (segment.kind == JsonResolvedPathSegment::Kind::kField) {
      out.append(segment.field);
      continue;
    }
    out.append(std::to_string(segment.index));
  }
  return out;
}

bool IsZeroNumber(const minijson::Value& value) {
  return value.IsNumber() &&
         std::fabsl(value.number().value) < 0.5e-18L;
}

minijson::Value MakeJsonArray(const std::vector<minijson::Value>& values) {
  return minijson::Value::ArrayValue(values);
}

struct LoadedJsonDocument {
  KeyLookup lookup;
  minijson::Value root;
};

rocksdb::Status LoadJsonDocument(ModuleServices* services,
                                 const CoreKeyService* key_service,
                                 const std::string& key,
                                 LoadedJsonDocument* out) {
  if (services == nullptr || key_service == nullptr || out == nullptr) {
    return rocksdb::Status::InvalidArgument("JSON document services are unavailable");
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services->snapshot().Create();
  rocksdb::Status status =
      key_service->Lookup(snapshot.get(), key, &out->lookup);
  if (!status.ok()) {
    return status;
  }
  status = RequireJsonEncoding(out->lookup);
  if (!status.ok()) {
    return status;
  }
  if (!out->lookup.exists) {
    out->root = minijson::Value::Null();
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace data_keyspace = services->storage().Keyspace("data");
  std::string encoded;
  status = snapshot->Get(data_keyspace, key, &encoded);
  if (status.IsNotFound()) {
    return rocksdb::Status::Corruption("JSON document is missing data");
  }
  if (!status.ok()) {
    return status;
  }

  std::string error;
  if (!minijson::Parse(encoded, &out->root, &error)) {
    return rocksdb::Status::Corruption("invalid stored JSON: " + error);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status PersistJsonDocument(ModuleServices* services,
                                    const CoreKeyService* key_service,
                                    const std::string& key,
                                    const KeyLookup& lookup,
                                    const minijson::Value& root) {
  if (services == nullptr || key_service == nullptr) {
    return rocksdb::Status::InvalidArgument("JSON document services are unavailable");
  }

  const std::string encoded = minijson::Serialize(root);
  KeyMetadata metadata = BuildJsonMetadata(key_service, lookup);
  metadata.size = encoded.size();

  const ModuleKeyspace data_keyspace = services->storage().Keyspace("data");
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services->storage().CreateWriteBatch();
  rocksdb::Status status = write_batch->Put(data_keyspace, key, encoded);
  if (!status.ok()) {
    return status;
  }
  status = key_service->PutMetadata(write_batch.get(), key, metadata);
  if (!status.ok()) {
    return status;
  }
  return write_batch->Commit();
}

minijson::Value* ResolveMutableMatch(minijson::Value* root,
                                     const JsonResolvedPath& path) {
  minijson::Value* value = nullptr;
  if (!ResolveMutableJsonPath(root, path, &value)) {
    return nullptr;
  }
  return value;
}

const minijson::Value* ResolveMatch(const minijson::Value& root,
                                    const JsonResolvedPath& path) {
  const minijson::Value* value = nullptr;
  if (!ResolveJsonPath(root, path, &value)) {
    return nullptr;
  }
  return value;
}

void DeduplicateMatches(std::vector<JsonResolvedPath>* matches) {
  if (matches == nullptr) {
    return;
  }
  std::unordered_set<std::string> seen;
  std::vector<JsonResolvedPath> unique;
  unique.reserve(matches->size());
  for (const auto& match : *matches) {
    const std::string signature = SerializeResolvedPath(match);
    if (seen.insert(signature).second) {
      unique.push_back(match);
    }
  }
  *matches = std::move(unique);
}

rocksdb::Status ParseCommandJson(const std::string& text, minijson::Value* value) {
  std::string error;
  if (minijson::Parse(text, value, &error)) {
    return rocksdb::Status::OK();
  }
  return rocksdb::Status::InvalidArgument("invalid JSON value: " + error);
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

rocksdb::Status JsonModule::OnLoad(ModuleServices& services) {
  services_ = &services;

  rocksdb::Status status = services.command_registry().Register(
      {"JSON.SET", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [this](const CmdRegistration& registration) {
         return std::make_unique<JsonSetCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.GET", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [this](const CmdRegistration& registration) {
         return std::make_unique<JsonGetCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.MGET", CmdFlags::kRead | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<JsonMGetCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.DEL", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<JsonCountCmd>(
             registration, this, JsonCountCmd::Mode::kDelete);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.FORGET", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<JsonCountCmd>(
             registration, this, JsonCountCmd::Mode::kForget);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.TYPE", CmdFlags::kRead | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<JsonTypeCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.CLEAR", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<JsonCountCmd>(
             registration, this, JsonCountCmd::Mode::kClear);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.TOGGLE", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<JsonToggleCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"JSON.NUMINCRBY", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<JsonNumIncrByCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

rocksdb::Status JsonModule::OnStart(ModuleServices& services) {
  key_service_ = services.exports().Find<CoreKeyService>(
      kCoreKeyServiceQualifiedExportName);
  if (key_service_ == nullptr) {
    return rocksdb::Status::InvalidArgument("core key service is unavailable");
  }
  delete_registry_ = services.exports().Find<WholeKeyDeleteRegistry>(
      kWholeKeyDeleteRegistryQualifiedExportName);
  if (delete_registry_ == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "whole-key delete registry is unavailable");
  }
  rocksdb::Status status = delete_registry_->RegisterHandler(this);
  if (!status.ok()) {
    return status;
  }
  started_ = true;
  return rocksdb::Status::OK();
}

void JsonModule::OnStop(ModuleServices& /*services*/) {
  started_ = false;
  delete_registry_ = nullptr;
  key_service_ = nullptr;
  services_ = nullptr;
}

rocksdb::Status JsonModule::DeleteWholeKey(ModuleSnapshot* snapshot,
                                           ModuleWriteBatch* write_batch,
                                           const std::string& key,
                                           const KeyLookup& lookup) {
  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (write_batch == nullptr) {
    return rocksdb::Status::InvalidArgument("module write batch is unavailable");
  }
  rocksdb::Status status = RequireJsonEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  status = write_batch->Delete(data_keyspace, key);
  if (!status.ok()) {
    return status;
  }
  const KeyMetadata metadata = BuildJsonTombstoneMetadata(key_service_, lookup);
  return key_service_->PutMetadata(write_batch, key, metadata);
}

rocksdb::Status JsonModule::Set(const std::string& key, const JsonPath& path,
                                const minijson::Value& value,
                                JsonSetCondition condition, bool* applied) {
  if (applied == nullptr) {
    return rocksdb::Status::InvalidArgument("JSON.SET applied output is required");
  }
  *applied = false;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  LoadedJsonDocument loaded;
  rocksdb::Status status = LoadJsonDocument(services_, key_service_, key, &loaded);
  if (!status.ok()) {
    return status;
  }

  if (!loaded.lookup.exists) {
    if (!path.is_root() || condition == JsonSetCondition::kXx) {
      return rocksdb::Status::OK();
    }
    *applied = true;
    return PersistJsonDocument(services_, key_service_, key, loaded.lookup, value);
  }

  if (path.is_root()) {
    if (condition == JsonSetCondition::kNx) {
      return rocksdb::Status::OK();
    }
    *applied = true;
    return PersistJsonDocument(services_, key_service_, key, loaded.lookup, value);
  }

  std::vector<JsonResolvedPath> matches;
  CollectJsonPathMatches(loaded.root, path, &matches);
  DeduplicateMatches(&matches);
  if (!matches.empty()) {
    if (condition == JsonSetCondition::kNx) {
      return rocksdb::Status::OK();
    }
    for (const auto& match : matches) {
      minijson::Value* target = ResolveMutableMatch(&loaded.root, match);
      if (target != nullptr) {
        *target = value;
      }
    }
    *applied = true;
    return PersistJsonDocument(services_, key_service_, key, loaded.lookup,
                               loaded.root);
  }

  if (condition == JsonSetCondition::kXx) {
    return rocksdb::Status::OK();
  }

  JsonPath parent_path;
  std::string member;
  if (!SplitJsonPathForObjectMemberCreate(path, &parent_path, &member)) {
    return rocksdb::Status::OK();
  }

  std::vector<JsonResolvedPath> parents;
  CollectJsonPathMatches(loaded.root, parent_path, &parents);
  DeduplicateMatches(&parents);
  if (parents.size() != 1) {
    return rocksdb::Status::OK();
  }
  minijson::Value* parent = ResolveMutableMatch(&loaded.root, parents.front());
  if (parent == nullptr || !parent->IsObject() || parent->FindMember(member) != nullptr) {
    return rocksdb::Status::OK();
  }
  parent->object_items().emplace_back(member, value);
  *applied = true;
  return PersistJsonDocument(services_, key_service_, key, loaded.lookup,
                             loaded.root);
}

rocksdb::Status JsonModule::Get(const std::string& key, const JsonPath& path,
                                JsonGetResult* result) {
  if (result == nullptr) {
    return rocksdb::Status::InvalidArgument("JSON.GET result output is required");
  }
  result->key_exists = false;
  result->matches.clear();

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  LoadedJsonDocument loaded;
  rocksdb::Status status = LoadJsonDocument(services_, key_service_, key, &loaded);
  if (!status.ok()) {
    return status;
  }
  if (!loaded.lookup.exists) {
    return rocksdb::Status::OK();
  }
  result->key_exists = true;

  std::vector<JsonResolvedPath> matches;
  CollectJsonPathMatches(loaded.root, path, &matches);
  DeduplicateMatches(&matches);
  result->matches.reserve(matches.size());
  for (const auto& match : matches) {
    const minijson::Value* resolved = ResolveMatch(loaded.root, match);
    if (resolved != nullptr) {
      result->matches.push_back(*resolved);
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status JsonModule::DeletePath(const std::string& key,
                                       const JsonPath& path,
                                       uint64_t* deleted) {
  if (deleted == nullptr) {
    return rocksdb::Status::InvalidArgument("JSON.DEL output is required");
  }
  *deleted = 0;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  LoadedJsonDocument loaded;
  rocksdb::Status status = LoadJsonDocument(services_, key_service_, key, &loaded);
  if (!status.ok()) {
    return status;
  }
  if (!loaded.lookup.exists) {
    return rocksdb::Status::OK();
  }

  std::vector<JsonResolvedPath> matches;
  CollectJsonPathMatches(loaded.root, path, &matches);
  DeduplicateMatches(&matches);
  if (matches.empty()) {
    return rocksdb::Status::OK();
  }

  for (const auto& match : matches) {
    if (match.segments.empty()) {
      std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
      std::unique_ptr<ModuleWriteBatch> write_batch =
          services_->storage().CreateWriteBatch();
      status = DeleteWholeKey(snapshot.get(), write_batch.get(), key, loaded.lookup);
      if (!status.ok()) {
        return status;
      }
      status = write_batch->Commit();
      if (!status.ok()) {
        return status;
      }
      *deleted = 1;
      return rocksdb::Status::OK();
    }
  }

  std::unordered_set<std::string> seen;
  for (auto it = matches.rbegin(); it != matches.rend(); ++it) {
    const std::string signature = SerializeResolvedPath(*it);
    if (!seen.insert(signature).second) {
      continue;
    }
    minijson::Value* parent = nullptr;
    JsonResolvedPathSegment leaf;
    if (!ResolveMutableJsonParent(&loaded.root, *it, &parent, &leaf) ||
        parent == nullptr) {
      continue;
    }
    if (leaf.kind == JsonResolvedPathSegment::Kind::kField) {
      if (parent->IsObject() && parent->EraseMember(leaf.field)) {
        ++(*deleted);
      }
      continue;
    }
    if (parent->IsArray() && leaf.index < parent->array_items().size()) {
      parent->array_items().erase(parent->array_items().begin() +
                                  static_cast<long>(leaf.index));
      ++(*deleted);
    }
  }

  if (*deleted == 0) {
    return rocksdb::Status::OK();
  }

  if (loaded.root.IsObject() && loaded.root.object_items().empty()) {
    return PersistJsonDocument(services_, key_service_, key, loaded.lookup,
                               loaded.root);
  }
  if (loaded.root.IsArray() && loaded.root.array_items().empty()) {
    return PersistJsonDocument(services_, key_service_, key, loaded.lookup,
                               loaded.root);
  }
  return PersistJsonDocument(services_, key_service_, key, loaded.lookup,
                             loaded.root);
}

rocksdb::Status JsonModule::TypePath(const std::string& key, const JsonPath& path,
                                     bool* key_exists,
                                     std::vector<std::string>* types) {
  if (key_exists == nullptr || types == nullptr) {
    return rocksdb::Status::InvalidArgument("JSON.TYPE outputs are required");
  }
  *key_exists = false;
  types->clear();

  JsonGetResult result;
  rocksdb::Status status = Get(key, path, &result);
  if (!status.ok()) {
    return status;
  }
  *key_exists = result.key_exists;
  for (const auto& match : result.matches) {
    types->push_back(JsonTypeName(match));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status JsonModule::ClearPath(const std::string& key,
                                      const JsonPath& path,
                                      uint64_t* cleared) {
  if (cleared == nullptr) {
    return rocksdb::Status::InvalidArgument("JSON.CLEAR output is required");
  }
  *cleared = 0;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  LoadedJsonDocument loaded;
  rocksdb::Status status = LoadJsonDocument(services_, key_service_, key, &loaded);
  if (!status.ok()) {
    return status;
  }
  if (!loaded.lookup.exists) {
    return rocksdb::Status::OK();
  }

  std::vector<JsonResolvedPath> matches;
  CollectJsonPathMatches(loaded.root, path, &matches);
  DeduplicateMatches(&matches);
  for (const auto& match : matches) {
    minijson::Value* target = ResolveMutableMatch(&loaded.root, match);
    if (target == nullptr) {
      continue;
    }
    if (target->IsObject()) {
      if (!target->object_items().empty()) {
        target->object_items().clear();
        ++(*cleared);
      }
      continue;
    }
    if (target->IsArray()) {
      if (!target->array_items().empty()) {
        target->array_items().clear();
        ++(*cleared);
      }
      continue;
    }
    if (target->IsNumber() && !IsZeroNumber(*target)) {
      *target = minijson::MakeNumber(0.0L);
      ++(*cleared);
    }
  }

  if (*cleared == 0) {
    return rocksdb::Status::OK();
  }
  return PersistJsonDocument(services_, key_service_, key, loaded.lookup,
                             loaded.root);
}

rocksdb::Status JsonModule::TogglePath(const std::string& key,
                                       const JsonPath& path,
                                       bool* key_exists,
                                       std::vector<JsonToggleResult>* results) {
  if (key_exists == nullptr || results == nullptr) {
    return rocksdb::Status::InvalidArgument("JSON.TOGGLE outputs are required");
  }
  *key_exists = false;
  results->clear();

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  LoadedJsonDocument loaded;
  rocksdb::Status status = LoadJsonDocument(services_, key_service_, key, &loaded);
  if (!status.ok()) {
    return status;
  }
  if (!loaded.lookup.exists) {
    return rocksdb::Status::OK();
  }
  *key_exists = true;

  std::vector<JsonResolvedPath> matches;
  CollectJsonPathMatches(loaded.root, path, &matches);
  DeduplicateMatches(&matches);
  bool changed = false;
  for (const auto& match : matches) {
    JsonToggleResult result;
    minijson::Value* target = ResolveMutableMatch(&loaded.root, match);
    if (target != nullptr && target->IsBool()) {
      result.is_boolean = true;
      result.value = !target->bool_value();
      *target = minijson::Value::Bool(result.value);
      changed = true;
    }
    results->push_back(result);
  }

  if (!changed) {
    return rocksdb::Status::OK();
  }
  return PersistJsonDocument(services_, key_service_, key, loaded.lookup,
                             loaded.root);
}

rocksdb::Status JsonModule::NumIncrByPath(
    const std::string& key, const JsonPath& path, long double increment,
    bool* key_exists, std::vector<JsonNumberResult>* results) {
  if (key_exists == nullptr || results == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "JSON.NUMINCRBY outputs are required");
  }
  *key_exists = false;
  results->clear();

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  LoadedJsonDocument loaded;
  rocksdb::Status status = LoadJsonDocument(services_, key_service_, key, &loaded);
  if (!status.ok()) {
    return status;
  }
  if (!loaded.lookup.exists) {
    return rocksdb::Status::OK();
  }
  *key_exists = true;

  std::vector<JsonResolvedPath> matches;
  CollectJsonPathMatches(loaded.root, path, &matches);
  DeduplicateMatches(&matches);

  bool changed = false;
  for (const auto& match : matches) {
    JsonNumberResult result;
    minijson::Value* target = ResolveMutableMatch(&loaded.root, match);
    if (target != nullptr && target->IsNumber()) {
      const long double next = target->number().value + increment;
      if (!std::isfinite(static_cast<double>(next))) {
        return rocksdb::Status::InvalidArgument(
            "JSON.NUMINCRBY result must be finite");
      }
      result.is_number = true;
      result.value = minijson::MakeNumber(next);
      *target = result.value;
      changed = true;
    }
    results->push_back(result);
  }

  if (!changed) {
    return rocksdb::Status::OK();
  }
  return PersistJsonDocument(services_, key_service_, key, loaded.lookup,
                             loaded.root);
}

rocksdb::Status JsonModule::EnsureReady() const {
  if (services_ == nullptr || key_service_ == nullptr || !started_) {
    return rocksdb::Status::InvalidArgument("json module is unavailable");
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
