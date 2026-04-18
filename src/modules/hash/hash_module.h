#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "module/module.h"
#include "modules/hash/hash_indexing_bridge.h"
#include "modules/hash/hash_types.h"

namespace minikv {

class CoreKeyService;
class ModuleServices;
class ModuleWriteBatch;
struct HashMutation;

class HashModule : public Module, public HashIndexingBridge {
 public:
  std::string_view Name() const override { return "hash"; }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

  rocksdb::Status AddObserver(HashObserver* observer) override;
  rocksdb::Status RemoveObserver(HashObserver* observer) override;

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
  bool started_ = false;
  std::vector<HashObserver*> observers_;
};

}  // namespace minikv
