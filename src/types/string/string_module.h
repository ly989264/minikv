#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "core/whole_key_delete_handler.h"
#include "runtime/module/module.h"
#include "types/string/string_bridge.h"

namespace minikv {

class CoreKeyService;
class ModuleServices;
class ModuleSnapshot;
class ModuleWriteBatch;

struct StringValue {
  bool found = false;
  std::string value;
};

class StringModule : public Module,
                     public WholeKeyDeleteHandler,
                     public StringBridge {
 public:
  std::string_view Name() const override { return "string"; }
  StorageColumnFamily DefaultStorageColumnFamily() const override {
    return StorageColumnFamily::kString;
  }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

  ObjectType HandledType() const override { return ObjectType::kString; }
  rocksdb::Status DeleteWholeKey(ModuleSnapshot* snapshot,
                                 ModuleWriteBatch* write_batch,
                                 const std::string& key,
                                 const KeyLookup& lookup) override;

  rocksdb::Status SetValue(const std::string& key,
                           const std::string& value) override;
  rocksdb::Status ReplaceValue(const std::string& key,
                               const std::string& value);
  rocksdb::Status SetValues(
      const std::vector<std::pair<std::string, std::string>>& values);
  rocksdb::Status GetValue(const std::string& key, std::string* value,
                           bool* found) override;
  rocksdb::Status GetValues(const std::vector<std::string>& keys,
                            std::vector<StringValue>* values);
  rocksdb::Status AppendValue(const std::string& key,
                              const std::string& suffix,
                              uint64_t* new_length);
  rocksdb::Status GetRange(const std::string& key, int64_t start, int64_t end,
                           std::string* value);
  rocksdb::Status SetRange(const std::string& key, uint64_t offset,
                           const std::string& value, uint64_t* new_length);
  rocksdb::Status GetSetValue(const std::string& key,
                              const std::string& value,
                              std::string* old_value, bool* found);
  rocksdb::Status IncrementBy(const std::string& key, int64_t increment,
                              int64_t* new_value);
  rocksdb::Status DecrementBy(const std::string& key, int64_t decrement,
                              int64_t* new_value);
  rocksdb::Status Length(const std::string& key, uint64_t* length) override;

 private:
  rocksdb::Status EnsureReady() const;
  rocksdb::Status StoreValue(const std::string& key,
                             const std::string& value, bool clear_ttl);

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  WholeKeyDeleteRegistry* delete_registry_ = nullptr;
  bool started_ = false;
};

}  // namespace minikv
