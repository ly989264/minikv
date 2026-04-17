#include "minikv.h"

#include <memory>
#include <utility>

#include "kernel/mutation_hook.h"
#include "kernel/scheduler.h"
#include "kernel/storage_engine.h"
#include "types/hash/hash_module.h"

namespace minikv {

class MiniKV::Impl {
 public:
  explicit Impl(const Config& config_value)
      : config(config_value),
        hash_module(&storage_engine, &mutation_hook),
        command_services{&storage_engine, &hash_module},
        scheduler(&command_services, config_value.worker_threads,
                  config_value.max_pending_requests_per_worker) {}

  Config config;
  StorageEngine storage_engine;
  NoopMutationHook mutation_hook;
  HashModule hash_module;
  CommandServices command_services;
  Scheduler scheduler;
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
  minikv->reset(new MiniKV(std::move(impl)));
  return rocksdb::Status::OK();
}

Scheduler* MiniKV::scheduler() { return &impl_->scheduler; }

}  // namespace minikv
