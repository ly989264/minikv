#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "runtime/module/module.h"
#include "core/whole_key_delete_handler.h"

namespace minikv {

class CoreKeyService;
class ModuleServices;
class ModuleSnapshot;
class ModuleWriteBatch;

class ListModule : public Module, public WholeKeyDeleteHandler {
 public:
  std::string_view Name() const override { return "list"; }
  StorageColumnFamily DefaultStorageColumnFamily() const override {
    return StorageColumnFamily::kList;
  }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

  ObjectType HandledType() const override { return ObjectType::kList; }
  rocksdb::Status DeleteWholeKey(ModuleSnapshot* snapshot,
                                 ModuleWriteBatch* write_batch,
                                 const std::string& key,
                                 const KeyLookup& lookup) override;

  rocksdb::Status PushLeft(const std::string& key,
                           const std::vector<std::string>& elements,
                           uint64_t* new_length);
  rocksdb::Status PushRight(const std::string& key,
                            const std::vector<std::string>& elements,
                            uint64_t* new_length);
  rocksdb::Status PopLeft(const std::string& key, std::string* element,
                          bool* found);
  rocksdb::Status PopRight(const std::string& key, std::string* element,
                           bool* found);
  rocksdb::Status ReadRange(const std::string& key, int64_t start, int64_t stop,
                            std::vector<std::string>* out);
  rocksdb::Status RemoveElements(const std::string& key, int64_t count,
                                 const std::string& element,
                                 uint64_t* removed_count);
  rocksdb::Status Trim(const std::string& key, int64_t start, int64_t stop);
  rocksdb::Status Length(const std::string& key, uint64_t* length);

 private:
  rocksdb::Status EnsureReady() const;

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  WholeKeyDeleteRegistry* delete_registry_ = nullptr;
  bool started_ = false;
};

}  // namespace minikv
