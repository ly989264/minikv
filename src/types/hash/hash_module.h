#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "runtime/module/module.h"
#include "core/whole_key_delete_handler.h"
#include "types/hash/hash_indexing_bridge.h"
#include "types/hash/hash_types.h"

namespace minikv {

class CoreKeyService;
class ModuleServices;
class ModuleSnapshot;
class ModuleWriteBatch;
struct HashMutation;

class HashModule : public Module,
                   public HashIndexingBridge,
                   public WholeKeyDeleteHandler {
 public:
  std::string_view Name() const override { return "hash"; }
  StorageColumnFamily DefaultStorageColumnFamily() const override {
    return StorageColumnFamily::kHash;
  }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

  rocksdb::Status AddObserver(HashObserver* observer) override;
  rocksdb::Status RemoveObserver(HashObserver* observer) override;

  ObjectType HandledType() const override { return ObjectType::kHash; }
  rocksdb::Status DeleteWholeKey(ModuleSnapshot* snapshot,
                                 ModuleWriteBatch* write_batch,
                                 const std::string& key,
                                 const KeyLookup& lookup) override;

  rocksdb::Status PutField(const std::string& key, const std::string& field,
                           const std::string& value, bool* inserted);
  rocksdb::Status ReadAll(const std::string& key,
                          std::vector<FieldValue>* out);
  rocksdb::Status DeleteFields(const std::string& key,
                               const std::vector<std::string>& fields,
                               uint64_t* deleted);

 private:
  rocksdb::Status EnsureReady() const;
  rocksdb::Status NotifyObservers(const HashMutation& mutation,
                                  ModuleWriteBatch* write_batch) const;

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  WholeKeyDeleteRegistry* delete_registry_ = nullptr;
  bool started_ = false;
  std::vector<HashObserver*> observers_;
};

}  // namespace minikv
