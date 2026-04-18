#include "module/module_services.h"

#include <utility>

#include "kernel/scheduler.h"
#include "kernel/snapshot.h"
#include "kernel/write_context.h"

namespace minikv {

ModuleNamespace::ModuleNamespace(std::string module_name)
    : module_name_(std::move(module_name)) {}

std::string ModuleNamespace::Qualify(const std::string& local_name) const {
  if (local_name.empty()) {
    return module_name_;
  }
  return module_name_ + "." + local_name;
}

class ModuleWriteBatch::Impl {
 public:
  explicit Impl(StorageEngine* storage_engine) : write_context(storage_engine) {}

  WriteContext write_context;
};

ModuleWriteBatch::ModuleWriteBatch(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

ModuleWriteBatch::~ModuleWriteBatch() = default;

ModuleWriteBatch::ModuleWriteBatch(ModuleWriteBatch&&) noexcept = default;

ModuleWriteBatch& ModuleWriteBatch::operator=(ModuleWriteBatch&&) noexcept =
    default;

rocksdb::Status ModuleWriteBatch::Put(StorageColumnFamily column_family,
                                      const rocksdb::Slice& key,
                                      const rocksdb::Slice& value) {
  if (impl_ == nullptr) {
    return rocksdb::Status::InvalidArgument("module storage is unavailable");
  }
  return impl_->write_context.Put(column_family, key, value);
}

rocksdb::Status ModuleWriteBatch::Delete(StorageColumnFamily column_family,
                                         const rocksdb::Slice& key) {
  if (impl_ == nullptr) {
    return rocksdb::Status::InvalidArgument("module storage is unavailable");
  }
  return impl_->write_context.Delete(column_family, key);
}

rocksdb::Status ModuleWriteBatch::Commit() {
  if (impl_ == nullptr) {
    return rocksdb::Status::InvalidArgument("module storage is unavailable");
  }
  return impl_->write_context.Commit();
}

class ModuleSnapshot::Impl {
 public:
  explicit Impl(std::unique_ptr<Snapshot> snapshot_value)
      : snapshot(std::move(snapshot_value)) {}

  std::unique_ptr<Snapshot> snapshot;
};

ModuleSnapshot::ModuleSnapshot(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

ModuleSnapshot::~ModuleSnapshot() = default;

ModuleSnapshot::ModuleSnapshot(ModuleSnapshot&&) noexcept = default;

ModuleSnapshot& ModuleSnapshot::operator=(ModuleSnapshot&&) noexcept = default;

rocksdb::Status ModuleSnapshot::Get(StorageColumnFamily column_family,
                                    const rocksdb::Slice& key,
                                    std::string* value) const {
  if (impl_ == nullptr || impl_->snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  return impl_->snapshot->Get(column_family, key, value);
}

rocksdb::Status ModuleSnapshot::ScanPrefix(StorageColumnFamily column_family,
                                           const rocksdb::Slice& prefix,
                                           const ScanVisitor& visitor) const {
  if (impl_ == nullptr || impl_->snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  return impl_->snapshot->ScanPrefix(column_family, prefix, visitor);
}

ModuleCommandRegistry::ModuleCommandRegistry(CommandRegistry* registry,
                                             ModuleNamespace module_namespace,
                                             const bool* registration_open)
    : registry_(registry),
      module_namespace_(std::move(module_namespace)),
      registration_open_(registration_open) {}

rocksdb::Status ModuleCommandRegistry::Register(CmdRegistration registration) {
  if (registry_ == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "module command registry is unavailable");
  }
  if (registration_open_ == nullptr || !*registration_open_) {
    return rocksdb::Status::InvalidArgument(
        "module commands may only register during OnLoad");
  }

  registration.source = CommandSource::kBuiltin;
  registration.owner_module = module_namespace_.module_name();
  return registry_->Register(std::move(registration));
}

class ModuleExportRegistry::SharedState {
 public:
  struct Entry {
    std::string owner_module;
    std::type_index export_type = std::type_index(typeid(void));
    void* value = nullptr;
  };

  rocksdb::Status Publish(const std::string& qualified_name,
                          const std::string& owner_module,
                          std::type_index export_type, void* value) {
    auto [it, inserted] =
        entries_.emplace(qualified_name,
                         Entry{owner_module, export_type, value});
    if (!inserted) {
      std::string message = "module export already published: " + qualified_name;
      if (!it->second.owner_module.empty()) {
        message += " existing module=" + it->second.owner_module;
      }
      if (!owner_module.empty()) {
        message += " new module=" + owner_module;
      }
      return rocksdb::Status::InvalidArgument(message);
    }
    return rocksdb::Status::OK();
  }

  void* Find(const std::string& qualified_name,
             std::type_index export_type) const {
    auto it = entries_.find(qualified_name);
    if (it == entries_.end() || it->second.export_type != export_type) {
      return nullptr;
    }
    return it->second.value;
  }

  void ClearOwnedExports(const std::string& owner_module) {
    for (auto it = entries_.begin(); it != entries_.end();) {
      if (it->second.owner_module == owner_module) {
        it = entries_.erase(it);
      } else {
        ++it;
      }
    }
  }

 private:
  std::map<std::string, Entry> entries_;
};

std::shared_ptr<ModuleExportRegistry::SharedState>
ModuleExportRegistry::CreateSharedState() {
  return std::make_shared<SharedState>();
}

ModuleExportRegistry::ModuleExportRegistry(
    std::shared_ptr<SharedState> shared_state, ModuleNamespace module_namespace,
    const bool* publish_open)
    : shared_state_(std::move(shared_state)),
      module_namespace_(std::move(module_namespace)),
      publish_open_(publish_open) {}

rocksdb::Status ModuleExportRegistry::PublishType(const std::string& local_name,
                                                  std::type_index export_type,
                                                  void* value) {
  if (shared_state_ == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "module export registry is unavailable");
  }
  if (local_name.empty()) {
    return rocksdb::Status::InvalidArgument("module export name is required");
  }
  if (value == nullptr) {
    return rocksdb::Status::InvalidArgument("module export value is required");
  }
  if (publish_open_ == nullptr || !*publish_open_) {
    return rocksdb::Status::InvalidArgument(
        "module exports may only publish during startup");
  }
  return shared_state_->Publish(module_namespace_.Qualify(local_name),
                                module_namespace_.module_name(), export_type,
                                value);
}

void* ModuleExportRegistry::FindType(const std::string& qualified_name,
                                     std::type_index export_type) const {
  if (shared_state_ == nullptr || qualified_name.empty()) {
    return nullptr;
  }
  return shared_state_->Find(qualified_name, export_type);
}

void ModuleExportRegistry::ClearOwnedExports() {
  if (shared_state_ == nullptr) {
    return;
  }
  shared_state_->ClearOwnedExports(module_namespace_.module_name());
}

ModuleStorage::ModuleStorage(StorageEngine* storage_engine)
    : storage_engine_(storage_engine) {}

std::unique_ptr<ModuleWriteBatch> ModuleStorage::CreateWriteBatch() const {
  if (storage_engine_ == nullptr) {
    return std::unique_ptr<ModuleWriteBatch>(
        new ModuleWriteBatch(std::unique_ptr<ModuleWriteBatch::Impl>()));
  }
  return std::unique_ptr<ModuleWriteBatch>(
      new ModuleWriteBatch(std::make_unique<ModuleWriteBatch::Impl>(
          storage_engine_)));
}

ModuleSnapshotService::ModuleSnapshotService(const StorageEngine* storage_engine)
    : storage_engine_(storage_engine) {}

std::unique_ptr<ModuleSnapshot> ModuleSnapshotService::Create() const {
  if (storage_engine_ == nullptr) {
    return std::unique_ptr<ModuleSnapshot>(
        new ModuleSnapshot(std::unique_ptr<ModuleSnapshot::Impl>()));
  }
  return std::unique_ptr<ModuleSnapshot>(
      new ModuleSnapshot(std::make_unique<ModuleSnapshot::Impl>(
          storage_engine_->CreateSnapshot())));
}

ModuleSchedulerView::ModuleSchedulerView(const Scheduler* scheduler)
    : scheduler_(scheduler) {}

size_t ModuleSchedulerView::worker_count() const {
  return scheduler_ != nullptr ? scheduler_->worker_count() : 0;
}

MetricsSnapshot ModuleSchedulerView::GetMetricsSnapshot() const {
  return scheduler_ != nullptr ? scheduler_->GetMetricsSnapshot()
                               : MetricsSnapshot{};
}

ModuleMetrics::ModuleMetrics(ModuleNamespace module_namespace,
                             std::shared_ptr<ModuleMetricsStore> store)
    : module_namespace_(std::move(module_namespace)), store_(std::move(store)) {}

uint64_t ModuleMetrics::IncrementCounter(const std::string& local_name,
                                         uint64_t delta) {
  if (store_ == nullptr) {
    return 0;
  }
  const std::string key = module_namespace_.Qualify(local_name);
  std::lock_guard<std::mutex> lock(store_->mutex);
  uint64_t& value = store_->counters[key];
  value += delta;
  return value;
}

void ModuleMetrics::SetCounter(const std::string& local_name, uint64_t value) {
  if (store_ == nullptr) {
    return;
  }
  const std::string key = module_namespace_.Qualify(local_name);
  std::lock_guard<std::mutex> lock(store_->mutex);
  store_->counters[key] = value;
}

uint64_t ModuleMetrics::GetCounter(const std::string& local_name) const {
  if (store_ == nullptr) {
    return 0;
  }
  const std::string key = module_namespace_.Qualify(local_name);
  std::lock_guard<std::mutex> lock(store_->mutex);
  auto it = store_->counters.find(key);
  return it != store_->counters.end() ? it->second : 0;
}

ModuleServices::ModuleServices(ModuleCommandRegistry command_registry,
                               ModuleExportRegistry exports,
                               ModuleStorage storage,
                               ModuleSnapshotService snapshot,
                               ModuleSchedulerView scheduler,
                               ModuleNamespace name_space,
                               ModuleMetrics metrics)
    : command_registry_(std::move(command_registry)),
      exports_(std::move(exports)),
      storage_(std::move(storage)),
      snapshot_(std::move(snapshot)),
      scheduler_(std::move(scheduler)),
      name_space_(std::move(name_space)),
      metrics_(std::move(metrics)) {}

}  // namespace minikv
