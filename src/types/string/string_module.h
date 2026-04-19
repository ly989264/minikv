#pragma once

#include <cstdint>
#include <string>

#include "core/whole_key_delete_handler.h"
#include "runtime/module/module.h"
#include "types/string/string_bridge.h"

namespace minikv {

class CoreKeyService;
class ModuleServices;
class ModuleSnapshot;
class ModuleWriteBatch;

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
  rocksdb::Status GetValue(const std::string& key, std::string* value,
                           bool* found) override;
  rocksdb::Status Length(const std::string& key, uint64_t* length) override;

 private:
  rocksdb::Status EnsureReady() const;

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  WholeKeyDeleteRegistry* delete_registry_ = nullptr;
  bool started_ = false;
};

}  // namespace minikv
