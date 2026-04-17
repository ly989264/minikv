#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "kernel/command_registry.h"
#include "kernel/storage_engine.h"
#include "network/network_server.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace minikv {

class Scheduler;

class ModuleNamespace {
 public:
  explicit ModuleNamespace(std::string module_name);

  const std::string& module_name() const { return module_name_; }
  std::string Qualify(const std::string& local_name) const;

 private:
  std::string module_name_;
};

class ModuleWriteBatch {
 public:
  ~ModuleWriteBatch();
  ModuleWriteBatch(ModuleWriteBatch&&) noexcept;
  ModuleWriteBatch& operator=(ModuleWriteBatch&&) noexcept;

  ModuleWriteBatch(const ModuleWriteBatch&) = delete;
  ModuleWriteBatch& operator=(const ModuleWriteBatch&) = delete;

  rocksdb::Status Put(StorageColumnFamily column_family,
                      const rocksdb::Slice& key,
                      const rocksdb::Slice& value);
  rocksdb::Status Delete(StorageColumnFamily column_family,
                         const rocksdb::Slice& key);
  rocksdb::Status Commit();

 private:
  class Impl;

  explicit ModuleWriteBatch(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;

  friend class ModuleStorage;
};

class ModuleSnapshot {
 public:
  using ScanVisitor =
      std::function<bool(const rocksdb::Slice& key, const rocksdb::Slice& value)>;

  ~ModuleSnapshot();
  ModuleSnapshot(ModuleSnapshot&&) noexcept;
  ModuleSnapshot& operator=(ModuleSnapshot&&) noexcept;

  ModuleSnapshot(const ModuleSnapshot&) = delete;
  ModuleSnapshot& operator=(const ModuleSnapshot&) = delete;

  rocksdb::Status Get(StorageColumnFamily column_family,
                      const rocksdb::Slice& key, std::string* value) const;
  rocksdb::Status ScanPrefix(StorageColumnFamily column_family,
                             const rocksdb::Slice& prefix,
                             const ScanVisitor& visitor) const;

 private:
  class Impl;

  explicit ModuleSnapshot(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;

  friend class ModuleSnapshotService;
};

class ModuleCommandRegistry {
 public:
  ModuleCommandRegistry(CommandRegistry* registry, ModuleNamespace module_namespace,
                        const bool* registration_open);

  rocksdb::Status Register(CmdRegistration registration);

 private:
  CommandRegistry* registry_ = nullptr;
  ModuleNamespace module_namespace_;
  const bool* registration_open_ = nullptr;
};

class ModuleStorage {
 public:
  explicit ModuleStorage(StorageEngine* storage_engine);

  std::unique_ptr<ModuleWriteBatch> CreateWriteBatch() const;

 private:
  StorageEngine* storage_engine_ = nullptr;
};

class ModuleSnapshotService {
 public:
  explicit ModuleSnapshotService(const StorageEngine* storage_engine);

  std::unique_ptr<ModuleSnapshot> Create() const;

 private:
  const StorageEngine* storage_engine_ = nullptr;
};

class ModuleSchedulerView {
 public:
  explicit ModuleSchedulerView(const Scheduler* scheduler);

  size_t worker_count() const;
  MetricsSnapshot GetMetricsSnapshot() const;

 private:
  const Scheduler* scheduler_ = nullptr;
};

struct ModuleMetricsStore {
  mutable std::mutex mutex;
  std::map<std::string, uint64_t> counters;
};

class ModuleMetrics {
 public:
  ModuleMetrics(ModuleNamespace module_namespace,
                std::shared_ptr<ModuleMetricsStore> store);

  uint64_t IncrementCounter(const std::string& local_name, uint64_t delta = 1);
  void SetCounter(const std::string& local_name, uint64_t value);
  uint64_t GetCounter(const std::string& local_name) const;

 private:
  ModuleNamespace module_namespace_;
  std::shared_ptr<ModuleMetricsStore> store_;
};

class ModuleServices {
 public:
  ModuleServices(ModuleCommandRegistry command_registry, ModuleStorage storage,
                 ModuleSnapshotService snapshot, ModuleSchedulerView scheduler,
                 ModuleNamespace name_space, ModuleMetrics metrics);

  ModuleCommandRegistry& command_registry() { return command_registry_; }
  const ModuleCommandRegistry& command_registry() const {
    return command_registry_;
  }

  ModuleStorage& storage() { return storage_; }
  const ModuleStorage& storage() const { return storage_; }

  ModuleSnapshotService& snapshot() { return snapshot_; }
  const ModuleSnapshotService& snapshot() const { return snapshot_; }

  ModuleSchedulerView& scheduler() { return scheduler_; }
  const ModuleSchedulerView& scheduler() const { return scheduler_; }

  ModuleNamespace& name_space() { return name_space_; }
  const ModuleNamespace& name_space() const { return name_space_; }

  ModuleMetrics& metrics() { return metrics_; }
  const ModuleMetrics& metrics() const { return metrics_; }

 private:
  ModuleCommandRegistry command_registry_;
  ModuleStorage storage_;
  ModuleSnapshotService snapshot_;
  ModuleSchedulerView scheduler_;
  ModuleNamespace name_space_;
  ModuleMetrics metrics_;
};

}  // namespace minikv
