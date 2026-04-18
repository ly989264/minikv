#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <typeindex>

#include "kernel/command_registry.h"
#include "kernel/storage_engine.h"
#include "network/network_server.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace minikv {

class BackgroundExecutor;
class Scheduler;

class ModuleNamespace {
 public:
  explicit ModuleNamespace(std::string module_name);

  const std::string& module_name() const { return module_name_; }
  std::string Qualify(const std::string& local_name) const;

 private:
  std::string module_name_;
};

class ModuleKeyspace {
 public:
  ModuleKeyspace();
  ModuleKeyspace(std::string module_name, std::string local_name);

  const std::string& module_name() const { return module_name_; }
  const std::string& local_name() const { return local_name_; }
  bool valid() const;
  std::string QualifiedName() const;
  const std::string& Prefix() const { return encoded_prefix_; }
  std::string EncodeKey(const rocksdb::Slice& key) const;
  bool DecodeKey(const rocksdb::Slice& storage_key, std::string* key) const;

 private:
  std::string module_name_;
  std::string local_name_;
  std::string encoded_prefix_;
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
  rocksdb::Status Put(const ModuleKeyspace& keyspace, const rocksdb::Slice& key,
                      const rocksdb::Slice& value);
  rocksdb::Status Delete(StorageColumnFamily column_family,
                         const rocksdb::Slice& key);
  rocksdb::Status Delete(const ModuleKeyspace& keyspace,
                         const rocksdb::Slice& key);
  rocksdb::Status Commit();

 private:
  class Impl;

  explicit ModuleWriteBatch(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;

  friend class ModuleStorage;
};

class ModuleIterator {
 public:
  ~ModuleIterator();
  ModuleIterator(ModuleIterator&&) noexcept;
  ModuleIterator& operator=(ModuleIterator&&) noexcept;

  ModuleIterator(const ModuleIterator&) = delete;
  ModuleIterator& operator=(const ModuleIterator&) = delete;

  void Seek(const rocksdb::Slice& key);
  void Next();
  bool Valid() const;
  rocksdb::Slice key() const;
  rocksdb::Slice value() const;
  rocksdb::Status status() const;

 private:
  class Impl;

  explicit ModuleIterator(std::unique_ptr<Impl> impl);
  void Refresh();

  std::unique_ptr<Impl> impl_;

  friend class ModuleSnapshot;
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
  rocksdb::Status Get(const ModuleKeyspace& keyspace, const rocksdb::Slice& key,
                      std::string* value) const;
  rocksdb::Status ScanPrefix(StorageColumnFamily column_family,
                             const rocksdb::Slice& prefix,
                             const ScanVisitor& visitor) const;
  rocksdb::Status ScanPrefix(const ModuleKeyspace& keyspace,
                             const rocksdb::Slice& prefix,
                             const ScanVisitor& visitor) const;
  std::unique_ptr<ModuleIterator> NewIterator(
      const ModuleKeyspace& keyspace) const;

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

class ModuleExportRegistry {
 public:
  class SharedState;

  static std::shared_ptr<SharedState> CreateSharedState();

  ModuleExportRegistry(std::shared_ptr<SharedState> shared_state,
                       ModuleNamespace module_namespace,
                       const bool* publish_open);

  template <typename T>
  rocksdb::Status Publish(const std::string& local_name, T* value) {
    return PublishType(local_name, std::type_index(typeid(T)), value);
  }

  template <typename T>
  T* Find(const std::string& qualified_name) const {
    return static_cast<T*>(
        FindType(qualified_name, std::type_index(typeid(T))));
  }

  void ClearOwnedExports();

 private:
  rocksdb::Status PublishType(const std::string& local_name,
                              std::type_index export_type, void* value);
  void* FindType(const std::string& qualified_name,
                 std::type_index export_type) const;

  std::shared_ptr<SharedState> shared_state_;
  ModuleNamespace module_namespace_;
  const bool* publish_open_ = nullptr;
};

class ModuleStorage {
 public:
  ModuleStorage(ModuleNamespace module_namespace, StorageEngine* storage_engine);

  ModuleKeyspace Keyspace(const std::string& local_name) const;

  std::unique_ptr<ModuleWriteBatch> CreateWriteBatch() const;

 private:
  ModuleNamespace module_namespace_;
  StorageEngine* storage_engine_ = nullptr;
};

class ModuleSnapshotService {
 public:
  ModuleSnapshotService(ModuleNamespace module_namespace,
                        const StorageEngine* storage_engine);

  ModuleKeyspace Keyspace(const std::string& local_name) const;

  std::unique_ptr<ModuleSnapshot> Create() const;

 private:
  ModuleNamespace module_namespace_;
  const StorageEngine* storage_engine_ = nullptr;
};

class ModuleBackgroundService {
 public:
  using Task = std::function<void()>;

  ModuleBackgroundService(BackgroundExecutor* executor,
                          ModuleNamespace module_namespace);

  rocksdb::Status Submit(Task task) const;

 private:
  BackgroundExecutor* executor_ = nullptr;
  ModuleNamespace module_namespace_;
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
  ModuleServices(ModuleCommandRegistry command_registry,
                 ModuleExportRegistry exports, ModuleStorage storage,
                 ModuleSnapshotService snapshot,
                 ModuleBackgroundService background,
                 ModuleSchedulerView scheduler, ModuleNamespace name_space,
                 ModuleMetrics metrics);

  ModuleCommandRegistry& command_registry() { return command_registry_; }
  const ModuleCommandRegistry& command_registry() const {
    return command_registry_;
  }

  ModuleExportRegistry& exports() { return exports_; }
  const ModuleExportRegistry& exports() const { return exports_; }

  ModuleStorage& storage() { return storage_; }
  const ModuleStorage& storage() const { return storage_; }

  ModuleSnapshotService& snapshot() { return snapshot_; }
  const ModuleSnapshotService& snapshot() const { return snapshot_; }

  ModuleBackgroundService& background() { return background_; }
  const ModuleBackgroundService& background() const { return background_; }

  ModuleSchedulerView& scheduler() { return scheduler_; }
  const ModuleSchedulerView& scheduler() const { return scheduler_; }

  ModuleNamespace& name_space() { return name_space_; }
  const ModuleNamespace& name_space() const { return name_space_; }

  ModuleMetrics& metrics() { return metrics_; }
  const ModuleMetrics& metrics() const { return metrics_; }

 private:
  ModuleCommandRegistry command_registry_;
  ModuleExportRegistry exports_;
  ModuleStorage storage_;
  ModuleSnapshotService snapshot_;
  ModuleBackgroundService background_;
  ModuleSchedulerView scheduler_;
  ModuleNamespace name_space_;
  ModuleMetrics metrics_;
};

}  // namespace minikv
