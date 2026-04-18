#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "network/network_server.h"
#include "execution/worker/key_lock_table.h"
#include "execution/worker/worker.h"

namespace minikv {

class Scheduler {
 public:
  using Completion = WorkerTask::Completion;

  Scheduler(size_t worker_count, size_t max_queue_depth);
  ~Scheduler() = default;

  Scheduler(const Scheduler&) = delete;
  Scheduler& operator=(const Scheduler&) = delete;

  rocksdb::Status Submit(std::unique_ptr<Cmd> cmd, Completion completion);

  uint64_t rejected_requests() const {
    return rejected_requests_.load(std::memory_order_relaxed);
  }
  uint64_t inflight_requests() const {
    return inflight_requests_.load(std::memory_order_relaxed);
  }
  size_t worker_count() const { return workers_.size(); }
  std::vector<size_t> worker_queue_depth() const;
  MetricsSnapshot GetMetricsSnapshot() const;

 private:
  KeyLockTable key_lock_table_;
  std::vector<std::unique_ptr<Worker>> workers_;
  std::atomic<size_t> next_worker_{0};
  std::atomic<uint64_t> rejected_requests_{0};
  std::atomic<uint64_t> inflight_requests_{0};
};

}  // namespace minikv
