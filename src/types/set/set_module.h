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

class SetModule : public Module, public WholeKeyDeleteHandler {
 public:
  std::string_view Name() const override { return "set"; }
  StorageColumnFamily DefaultStorageColumnFamily() const override {
    return StorageColumnFamily::kSet;
  }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

  ObjectType HandledType() const override { return ObjectType::kSet; }
  rocksdb::Status DeleteWholeKey(ModuleSnapshot* snapshot,
                                 ModuleWriteBatch* write_batch,
                                 const std::string& key,
                                 const KeyLookup& lookup) override;

  rocksdb::Status AddMembers(const std::string& key,
                             const std::vector<std::string>& members,
                             uint64_t* added_count);
  rocksdb::Status Cardinality(const std::string& key, uint64_t* size);
  rocksdb::Status ReadMembers(const std::string& key,
                              std::vector<std::string>* out);
  rocksdb::Status IsMember(const std::string& key, const std::string& member,
                           bool* found);
  rocksdb::Status RemoveMembers(const std::string& key,
                                const std::vector<std::string>& members,
                                uint64_t* removed_count);
  rocksdb::Status RandomMember(const std::string& key, std::string* member,
                               bool* found);
  rocksdb::Status PopRandomMember(const std::string& key, std::string* member,
                                  bool* found);

 private:
  rocksdb::Status EnsureReady() const;

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  WholeKeyDeleteRegistry* delete_registry_ = nullptr;
  bool started_ = false;
};

}  // namespace minikv
