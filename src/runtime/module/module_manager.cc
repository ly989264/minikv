#include "runtime/module/module_manager.h"

#include <utility>

namespace minikv {

namespace {

ModuleServices MakeServices(StorageEngine* storage_engine, Scheduler* scheduler,
                            BackgroundExecutor* background_executor,
                            CommandRegistry* registry,
                            std::shared_ptr<ModuleExportRegistry::SharedState>
                                export_store,
                            std::shared_ptr<ModuleMetricsStore> metrics_store,
                            std::string module_name,
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
                                      storage_engine),
                        ModuleSnapshotService(std::move(snapshot_namespace),
                                              storage_engine),
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
    : background_executor_("module-bg"),
      metrics_store_(std::make_shared<ModuleMetricsStore>()),
      export_store_(ModuleExportRegistry::CreateSharedState()) {
  modules_.reserve(modules.size());
  for (auto& module : modules) {
    const std::string module_name =
        module != nullptr ? std::string(module->Name()) : std::string();
    modules_.emplace_back(std::move(module),
                          MakeServices(storage_engine, scheduler,
                                       &background_executor_,
                                       &command_registry_, export_store_,
                                       metrics_store_, module_name,
                                       &registration_open_,
                                       &export_publish_open_));
  }
}

ModuleManager::~ModuleManager() { StopAll(); }

rocksdb::Status ModuleManager::Initialize() {
  if (initialized_) {
    return rocksdb::Status::InvalidArgument("modules already initialized");
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
