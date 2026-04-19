#pragma once

#include <memory>
#include <vector>

#include "runtime/module/background_executor.h"
#include "runtime/module/module.h"
#include "runtime/module/module_services.h"

namespace minikv {

class Scheduler;
class StorageEngine;

class ModuleManager {
 public:
  ModuleManager(StorageEngine* storage_engine, Scheduler* scheduler,
                std::vector<std::unique_ptr<Module>> modules);
  ~ModuleManager();

  ModuleManager(const ModuleManager&) = delete;
  ModuleManager& operator=(const ModuleManager&) = delete;

  rocksdb::Status Initialize();
  void StopAll();

  const CommandRegistry& command_registry() const { return command_registry_; }
  size_t module_count() const { return modules_.size(); }

 private:
  struct ModuleSlot {
    std::unique_ptr<Module> module;
    ModuleServices services;
    bool loaded = false;
    bool started = false;

    ModuleSlot(std::unique_ptr<Module> module_value, ModuleServices services_value)
        : module(std::move(module_value)), services(std::move(services_value)) {}
  };

  rocksdb::Status ValidateStorageLayout() const;
  void StopLoadedModules();

  StorageEngine* storage_engine_ = nullptr;
  CommandRegistry command_registry_;
  std::shared_ptr<ModuleMetricsStore> metrics_store_;
  std::shared_ptr<ModuleExportRegistry::SharedState> export_store_;
  BackgroundExecutor background_executor_;
  std::vector<ModuleSlot> modules_;
  bool registration_open_ = false;
  bool export_publish_open_ = false;
  bool initialized_ = false;
};

}  // namespace minikv
