#include "runtime/module/module_manager.h"

#include <array>
#include <string>
#include <utility>

#include "storage/engine/snapshot.h"

namespace minikv {

namespace {

ModuleServices MakeServices(StorageEngine* storage_engine, Scheduler* scheduler,
                            BackgroundExecutor* background_executor,
                            CommandRegistry* registry,
                            std::shared_ptr<ModuleExportRegistry::SharedState>
                                export_store,
                            std::shared_ptr<ModuleMetricsStore> metrics_store,
                            std::string module_name,
                            StorageColumnFamily default_column_family,
                            const bool* registration_open,
                            const bool* export_publish_open) {
  ModuleNamespace command_namespace(module_name);
  ModuleNamespace export_namespace(module_name);
  ModuleNamespace storage_namespace(module_name);
  ModuleNamespace snapshot_namespace(module_name);
  ModuleNamespace background_namespace(module_name);
  ModuleNamespace services_namespace(module_name);
  ModuleNamespace metrics_namespace(module_name);
  return ModuleServices(ModuleCommandRegistry(registry,
                                             std::move(command_namespace),
                                             registration_open),
                        ModuleExportRegistry(std::move(export_store),
                                             std::move(export_namespace),
                                             export_publish_open),
                        ModuleStorage(std::move(storage_namespace),
                                      storage_engine,
                                      default_column_family),
                        ModuleSnapshotService(std::move(snapshot_namespace),
                                              storage_engine,
                                              default_column_family),
                        ModuleBackgroundService(background_executor,
                                                std::move(background_namespace)),
                        ModuleSchedulerView(scheduler),
                        std::move(services_namespace),
                        ModuleMetrics(std::move(metrics_namespace),
                                      std::move(metrics_store)));
}

}  // namespace

ModuleManager::ModuleManager(StorageEngine* storage_engine, Scheduler* scheduler,
                             std::vector<std::unique_ptr<Module>> modules)
    : storage_engine_(storage_engine),
      background_executor_("module-bg"),
      metrics_store_(std::make_shared<ModuleMetricsStore>()),
      export_store_(ModuleExportRegistry::CreateSharedState()) {
  modules_.reserve(modules.size());
  for (auto& module : modules) {
    const std::string module_name =
        module != nullptr ? std::string(module->Name()) : std::string();
    const StorageColumnFamily default_column_family =
        module != nullptr ? module->DefaultStorageColumnFamily()
                          : StorageColumnFamily::kModule;
    modules_.emplace_back(std::move(module),
                          MakeServices(storage_engine, scheduler,
                                       &background_executor_,
                                       &command_registry_, export_store_,
                                       metrics_store_, module_name,
                                       default_column_family,
                                       &registration_open_,
                                       &export_publish_open_));
  }
}

ModuleManager::~ModuleManager() { StopAll(); }

rocksdb::Status ModuleManager::Initialize() {
  if (initialized_) {
    return rocksdb::Status::InvalidArgument("modules already initialized");
  }

  rocksdb::Status layout_status = ValidateStorageLayout();
  if (!layout_status.ok()) {
    return layout_status;
  }

  rocksdb::Status background_status = background_executor_.Start();
  if (!background_status.ok()) {
    return background_status;
  }

  for (auto& slot : modules_) {
    registration_open_ = true;
    export_publish_open_ = true;
    rocksdb::Status status = slot.module->OnLoad(slot.services);
    registration_open_ = false;
    export_publish_open_ = false;
    if (!status.ok()) {
      slot.services.exports().ClearOwnedExports();
      StopLoadedModules();
      return status;
    }
    slot.loaded = true;
  }

  for (auto& slot : modules_) {
    export_publish_open_ = true;
    rocksdb::Status status = slot.module->OnStart(slot.services);
    export_publish_open_ = false;
    if (!status.ok()) {
      StopLoadedModules();
      return status;
    }
    slot.started = true;
  }

  initialized_ = true;
  return rocksdb::Status::OK();
}

void ModuleManager::StopAll() {
  if (!initialized_) {
    StopLoadedModules();
    return;
  }
  StopLoadedModules();
  initialized_ = false;
}

rocksdb::Status ModuleManager::ValidateStorageLayout() const {
  if (storage_engine_ == nullptr) {
    return rocksdb::Status::OK();
  }

  const std::array<ModuleKeyspace, 10> legacy_keyspaces = {
      ModuleKeyspace("string", "data"),
      ModuleKeyspace("json", "data"),
      ModuleKeyspace("list", "entries"),
      ModuleKeyspace("list", "state"),
      ModuleKeyspace("set", "members"),
      ModuleKeyspace("zset", "members"),
      ModuleKeyspace("zset", "score_index"),
      ModuleKeyspace("stream", "entries"),
      ModuleKeyspace("stream", "state"),
      ModuleKeyspace("geo", "members"),
  };

  std::unique_ptr<Snapshot> snapshot = storage_engine_->CreateSnapshot();
  for (const ModuleKeyspace& keyspace : legacy_keyspaces) {
    bool found = false;
    rocksdb::Status status = snapshot->ScanPrefix(
        StorageColumnFamily::kModule, keyspace.Prefix(),
        [&found](const rocksdb::Slice&, const rocksdb::Slice&) {
          found = true;
          return false;
        });
    if (!status.ok()) {
      return status;
    }
    if (found) {
      return rocksdb::Status::InvalidArgument(
          "legacy typed data found in module column family for keyspace " +
          keyspace.QualifiedName() +
          "; this database uses the unsupported pre-type-cf layout");
    }
  }

  return rocksdb::Status::OK();
}

void ModuleManager::StopLoadedModules() {
  registration_open_ = false;
  export_publish_open_ = false;
  background_executor_.Stop();
  for (auto it = modules_.rbegin(); it != modules_.rend(); ++it) {
    if (!it->loaded) {
      continue;
    }
    it->module->OnStop(it->services);
    it->services.exports().ClearOwnedExports();
    it->started = false;
    it->loaded = false;
  }
}

}  // namespace minikv
