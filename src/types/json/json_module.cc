#include "types/json/json_module.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "types/json/json_document_utils.h"
#include "types/json/json_commands.h"
#include "runtime/module/module_services.h"

namespace minikv {
rocksdb::Status JsonModule::OnLoad(ModuleServices& services) {
  services_ = &services;
  return RegisterJsonCommands(services, this);
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
  rocksdb::Status status = json_internal::RequireJsonEncoding(lookup);
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
  const KeyMetadata metadata = json_internal::BuildJsonTombstoneMetadata(key_service_, lookup);
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

  json_internal::LoadedJsonDocument loaded;
  rocksdb::Status status = json_internal::LoadJsonDocument(services_, key_service_, key, &loaded);
  if (!status.ok()) {
    return status;
  }

  if (!loaded.lookup.exists) {
    if (!path.is_root() || condition == JsonSetCondition::kXx) {
      return rocksdb::Status::OK();
    }
    *applied = true;
    return json_internal::PersistJsonDocument(services_, key_service_, key, loaded.lookup, value);
  }

  if (path.is_root()) {
    if (condition == JsonSetCondition::kNx) {
      return rocksdb::Status::OK();
    }
    *applied = true;
    return json_internal::PersistJsonDocument(services_, key_service_, key, loaded.lookup, value);
  }

  std::vector<JsonResolvedPath> matches;
  CollectJsonPathMatches(loaded.root, path, &matches);
  json_internal::DeduplicateMatches(&matches);
  if (!matches.empty()) {
    if (condition == JsonSetCondition::kNx) {
      return rocksdb::Status::OK();
    }
    for (const auto& match : matches) {
      minijson::Value* target = json_internal::ResolveMutableMatch(&loaded.root, match);
      if (target != nullptr) {
        *target = value;
      }
    }
    *applied = true;
    return json_internal::PersistJsonDocument(services_, key_service_, key, loaded.lookup,
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
  json_internal::DeduplicateMatches(&parents);
  if (parents.size() != 1) {
    return rocksdb::Status::OK();
  }
  minijson::Value* parent = json_internal::ResolveMutableMatch(&loaded.root, parents.front());
  if (parent == nullptr || !parent->IsObject() || parent->FindMember(member) != nullptr) {
    return rocksdb::Status::OK();
  }
  parent->object_items().emplace_back(member, value);
  *applied = true;
  return json_internal::PersistJsonDocument(services_, key_service_, key, loaded.lookup,
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

  json_internal::LoadedJsonDocument loaded;
  rocksdb::Status status = json_internal::LoadJsonDocument(services_, key_service_, key, &loaded);
  if (!status.ok()) {
    return status;
  }
  if (!loaded.lookup.exists) {
    return rocksdb::Status::OK();
  }
  result->key_exists = true;

  std::vector<JsonResolvedPath> matches;
  CollectJsonPathMatches(loaded.root, path, &matches);
  json_internal::DeduplicateMatches(&matches);
  result->matches.reserve(matches.size());
  for (const auto& match : matches) {
    const minijson::Value* resolved = json_internal::ResolveMatch(loaded.root, match);
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

  json_internal::LoadedJsonDocument loaded;
  rocksdb::Status status = json_internal::LoadJsonDocument(services_, key_service_, key, &loaded);
  if (!status.ok()) {
    return status;
  }
  if (!loaded.lookup.exists) {
    return rocksdb::Status::OK();
  }

  std::vector<JsonResolvedPath> matches;
  CollectJsonPathMatches(loaded.root, path, &matches);
  json_internal::DeduplicateMatches(&matches);
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
    const std::string signature = json_internal::SerializeResolvedPath(*it);
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
    return json_internal::PersistJsonDocument(services_, key_service_, key, loaded.lookup,
                               loaded.root);
  }
  if (loaded.root.IsArray() && loaded.root.array_items().empty()) {
    return json_internal::PersistJsonDocument(services_, key_service_, key, loaded.lookup,
                               loaded.root);
  }
  return json_internal::PersistJsonDocument(services_, key_service_, key, loaded.lookup,
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
    types->push_back(json_internal::JsonTypeName(match));
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

  json_internal::LoadedJsonDocument loaded;
  rocksdb::Status status = json_internal::LoadJsonDocument(services_, key_service_, key, &loaded);
  if (!status.ok()) {
    return status;
  }
  if (!loaded.lookup.exists) {
    return rocksdb::Status::OK();
  }

  std::vector<JsonResolvedPath> matches;
  CollectJsonPathMatches(loaded.root, path, &matches);
  json_internal::DeduplicateMatches(&matches);
  for (const auto& match : matches) {
    minijson::Value* target = json_internal::ResolveMutableMatch(&loaded.root, match);
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
    if (target->IsNumber() && !json_internal::IsZeroNumber(*target)) {
      *target = minijson::MakeNumber(0.0L);
      ++(*cleared);
    }
  }

  if (*cleared == 0) {
    return rocksdb::Status::OK();
  }
  return json_internal::PersistJsonDocument(services_, key_service_, key, loaded.lookup,
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

  json_internal::LoadedJsonDocument loaded;
  rocksdb::Status status = json_internal::LoadJsonDocument(services_, key_service_, key, &loaded);
  if (!status.ok()) {
    return status;
  }
  if (!loaded.lookup.exists) {
    return rocksdb::Status::OK();
  }
  *key_exists = true;

  std::vector<JsonResolvedPath> matches;
  CollectJsonPathMatches(loaded.root, path, &matches);
  json_internal::DeduplicateMatches(&matches);
  bool changed = false;
  for (const auto& match : matches) {
    JsonToggleResult result;
    minijson::Value* target = json_internal::ResolveMutableMatch(&loaded.root, match);
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
  return json_internal::PersistJsonDocument(services_, key_service_, key, loaded.lookup,
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

  json_internal::LoadedJsonDocument loaded;
  rocksdb::Status status = json_internal::LoadJsonDocument(services_, key_service_, key, &loaded);
  if (!status.ok()) {
    return status;
  }
  if (!loaded.lookup.exists) {
    return rocksdb::Status::OK();
  }
  *key_exists = true;

  std::vector<JsonResolvedPath> matches;
  CollectJsonPathMatches(loaded.root, path, &matches);
  json_internal::DeduplicateMatches(&matches);

  bool changed = false;
  for (const auto& match : matches) {
    JsonNumberResult result;
    minijson::Value* target = json_internal::ResolveMutableMatch(&loaded.root, match);
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
  return json_internal::PersistJsonDocument(services_, key_service_, key, loaded.lookup,
                             loaded.root);
}

rocksdb::Status JsonModule::EnsureReady() const {
  if (services_ == nullptr || key_service_ == nullptr || !started_) {
    return rocksdb::Status::InvalidArgument("json module is unavailable");
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
