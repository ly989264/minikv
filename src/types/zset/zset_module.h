#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "core/whole_key_delete_handler.h"
#include "runtime/module/module.h"

namespace minikv {

class CoreKeyService;
class ModuleServices;
class ModuleSnapshot;
class ModuleWriteBatch;

struct ZSetEntry {
  std::string member;
  double score = 0;
};

class ZSetModule : public Module, public WholeKeyDeleteHandler {
 public:
  std::string_view Name() const override { return "zset"; }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

  ObjectType HandledType() const override { return ObjectType::kZSet; }
  rocksdb::Status DeleteWholeKey(ModuleSnapshot* snapshot,
                                 ModuleWriteBatch* write_batch,
                                 const std::string& key,
                                 const KeyLookup& lookup) override;

  rocksdb::Status AddMembers(const std::string& key,
                             const std::vector<ZSetEntry>& entries,
                             uint64_t* added_count);
  rocksdb::Status Cardinality(const std::string& key, uint64_t* size);
  rocksdb::Status CountByScore(const std::string& key, const std::string& min,
                               const std::string& max, uint64_t* count);
  rocksdb::Status IncrementBy(const std::string& key, double increment,
                              const std::string& member, double* new_score);
  rocksdb::Status CountByLex(const std::string& key, const std::string& min,
                             const std::string& max, uint64_t* count);
  rocksdb::Status RangeByRank(const std::string& key, int64_t start,
                              int64_t stop, std::vector<std::string>* out);
  rocksdb::Status RangeByLex(const std::string& key, const std::string& min,
                             const std::string& max,
                             std::vector<std::string>* out);
  rocksdb::Status RangeByScore(const std::string& key, const std::string& min,
                               const std::string& max,
                               std::vector<std::string>* out);
  rocksdb::Status Rank(const std::string& key, const std::string& member,
                       uint64_t* rank, bool* found);
  rocksdb::Status RemoveMembers(const std::string& key,
                                const std::vector<std::string>& members,
                                uint64_t* removed_count);
  rocksdb::Status Score(const std::string& key, const std::string& member,
                        double* score, bool* found);

 private:
  rocksdb::Status EnsureReady() const;

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  WholeKeyDeleteRegistry* delete_registry_ = nullptr;
  bool started_ = false;
};

}  // namespace minikv
