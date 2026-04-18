#include "runtime/minikv.h"

#include <memory>
#include <utility>
#include <vector>

#include "runtime/module/module_manager.h"
#include "execution/scheduler/scheduler.h"
#include "storage/engine/storage_engine.h"
#include "core/core_module.h"
#include "types/hash/hash_module.h"

namespace minikv {
namespace {

std::vector<std::unique_ptr<Module>> CreateBuiltinModules() {
  std::vector<std::unique_ptr<Module>> modules;
  modules.push_back(std::make_unique<CoreModule>());
  modules.push_back(std::make_unique<HashModule>());
  return modules;
}

}  // namespace

class MiniKV::Impl {
 public:
  explicit Impl(const Config& config_value) : config(config_value) {}

  ~Impl() {
    if (module_manager != nullptr) {
      module_manager->StopAll();
    }
  }

  Config config;
  StorageEngine storage_engine;
  std::unique_ptr<Scheduler> scheduler;
  std::unique_ptr<ModuleManager> module_manager;
};

MiniKV::MiniKV(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

MiniKV::~MiniKV() = default;

rocksdb::Status MiniKV::Open(const Config& config,
                             std::unique_ptr<MiniKV>* minikv) {
  auto impl = std::unique_ptr<Impl>(new Impl(config));
  rocksdb::Status status = impl->storage_engine.Open(config);
  if (!status.ok()) {
    return status;
  }
  impl->scheduler = std::make_unique<Scheduler>(
      config.worker_threads, config.max_pending_requests_per_worker);
  impl->module_manager = std::make_unique<ModuleManager>(
      &impl->storage_engine, impl->scheduler.get(), CreateBuiltinModules());
  status = impl->module_manager->Initialize();
  if (!status.ok()) {
    return status;
  }
  minikv->reset(new MiniKV(std::move(impl)));
  return rocksdb::Status::OK();
}

Scheduler* MiniKV::scheduler() { return impl_->scheduler.get(); }

const CommandRegistry& MiniKV::command_registry() const {
  return impl_->module_manager->command_registry();
}

}  // namespace minikv
