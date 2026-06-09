#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>

#include "runtime/module/module.h"
#include "core/key_service.h"
#include "core/whole_key_delete_handler.h"

namespace minikv {

class CoreModule : public Module, public WholeKeyDeleteRegistry {
 public:
  using TimeSource = DefaultCoreKeyService::TimeSource;

  struct ActiveExpireOptions {
    bool enabled = false;
    uint64_t interval_ms = 100;
    size_t batch_size = 64;
    size_t backfill_batch_size = 512;
  };

  explicit CoreModule(TimeSource time_source = {});
  CoreModule(TimeSource time_source, ActiveExpireOptions active_expire_options);
  ~CoreModule() override;

  std::string_view Name() const override { return "core"; }
  StorageColumnFamily DefaultStorageColumnFamily() const override {
    return StorageColumnFamily::kModule;
  }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  rocksdb::Status OnAfterStart(ModuleServices& services) override;
  void OnPrepareStop(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

  rocksdb::Status RegisterHandler(WholeKeyDeleteHandler* handler) override;
  rocksdb::Status DeleteWholeKey(ModuleSnapshot* snapshot,
                                 ModuleWriteBatch* write_batch,
                                 const std::string& key,
                                 const KeyLookup& lookup) override;
  WholeKeyDeleteHandler* FindHandler(ObjectType type) const;
  rocksdb::Status DeleteExpiredWholeKey(ModuleSnapshot* snapshot,
                                        ModuleWriteBatch* write_batch,
                                        const std::string& key,
                                        const KeyLookup& lookup);

 private:
  class ActiveExpireManager;

  DefaultCoreKeyService key_service_;
  ActiveExpireOptions active_expire_options_;
  std::unique_ptr<ActiveExpireManager> active_expire_manager_;
  std::map<ObjectType, WholeKeyDeleteHandler*> delete_handlers_;
  bool started_ = false;
};

}  // namespace minikv
