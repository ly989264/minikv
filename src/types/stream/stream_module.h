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

struct StreamFieldValue {
  std::string field;
  std::string value;
};

struct StreamEntry {
  std::string id;
  std::vector<StreamFieldValue> values;
};

struct StreamReadSpec {
  std::string key;
  std::string last_seen_id;
};

struct StreamReadResult {
  std::string key;
  std::vector<StreamEntry> entries;
};

class StreamModule : public Module, public WholeKeyDeleteHandler {
 public:
  std::string_view Name() const override { return "stream"; }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

  ObjectType HandledType() const override { return ObjectType::kStream; }
  rocksdb::Status DeleteWholeKey(ModuleSnapshot* snapshot,
                                 ModuleWriteBatch* write_batch,
                                 const std::string& key,
                                 const KeyLookup& lookup) override;

  rocksdb::Status AddEntry(const std::string& key, const std::string& id_spec,
                           const std::vector<StreamFieldValue>& values,
                           std::string* added_id);
  rocksdb::Status TrimByMaxLen(const std::string& key, uint64_t max_len,
                               uint64_t* removed_count);
  rocksdb::Status DeleteEntries(const std::string& key,
                                const std::vector<std::string>& ids,
                                uint64_t* removed_count);
  rocksdb::Status Length(const std::string& key, uint64_t* length);
  rocksdb::Status Range(const std::string& key, const std::string& start,
                        const std::string& end,
                        std::vector<StreamEntry>* out);
  rocksdb::Status ReverseRange(const std::string& key, const std::string& end,
                               const std::string& start,
                               std::vector<StreamEntry>* out);
  rocksdb::Status Read(const std::vector<StreamReadSpec>& requests,
                       std::vector<StreamReadResult>* out);

 private:
  rocksdb::Status EnsureReady() const;

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  WholeKeyDeleteRegistry* delete_registry_ = nullptr;
  bool started_ = false;
};

}  // namespace minikv
