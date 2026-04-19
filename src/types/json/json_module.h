#pragma once

#include <string>
#include <vector>

#include "core/whole_key_delete_handler.h"
#include "runtime/module/module.h"
#include "third_party/minijson/minijson.h"
#include "types/json/json_path.h"

namespace minikv {

class CoreKeyService;
class ModuleServices;
class ModuleSnapshot;
class ModuleWriteBatch;

enum class JsonSetCondition : uint8_t {
  kNone = 0,
  kNx = 1,
  kXx = 2,
};

struct JsonGetResult {
  bool key_exists = false;
  std::vector<minijson::Value> matches;
};

struct JsonToggleResult {
  bool is_boolean = false;
  bool value = false;
};

struct JsonNumberResult {
  bool is_number = false;
  minijson::Value value;
};

class JsonModule : public Module, public WholeKeyDeleteHandler {
 public:
  std::string_view Name() const override { return "json"; }
  StorageColumnFamily DefaultStorageColumnFamily() const override {
    return StorageColumnFamily::kJson;
  }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

  ObjectType HandledType() const override { return ObjectType::kJson; }
  rocksdb::Status DeleteWholeKey(ModuleSnapshot* snapshot,
                                 ModuleWriteBatch* write_batch,
                                 const std::string& key,
                                 const KeyLookup& lookup) override;

  rocksdb::Status Set(const std::string& key, const JsonPath& path,
                      const minijson::Value& value,
                      JsonSetCondition condition, bool* applied);
  rocksdb::Status Get(const std::string& key, const JsonPath& path,
                      JsonGetResult* result);
  rocksdb::Status DeletePath(const std::string& key, const JsonPath& path,
                             uint64_t* deleted);
  rocksdb::Status TypePath(const std::string& key, const JsonPath& path,
                           bool* key_exists, std::vector<std::string>* types);
  rocksdb::Status ClearPath(const std::string& key, const JsonPath& path,
                            uint64_t* cleared);
  rocksdb::Status TogglePath(const std::string& key, const JsonPath& path,
                             bool* key_exists,
                             std::vector<JsonToggleResult>* results);
  rocksdb::Status NumIncrByPath(const std::string& key, const JsonPath& path,
                                long double increment, bool* key_exists,
                                std::vector<JsonNumberResult>* results);

 private:
  rocksdb::Status EnsureReady() const;

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  WholeKeyDeleteRegistry* delete_registry_ = nullptr;
  bool started_ = false;
};

}  // namespace minikv
