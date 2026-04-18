#include "execution/scheduler/scheduler.h"

#include <algorithm>
#include <utility>

namespace minikv {

Scheduler::Scheduler(size_t worker_count, size_t max_queue_depth)
    : key_lock_table_(KeyLockTable::DefaultStripeCount(worker_count)) {
  const size_t normalized_worker_count = std::max<size_t>(1, worker_count);
  workers_.reserve(normalized_worker_count);
  for (size_t i = 0; i < normalized_worker_count; ++i) {
    workers_.push_back(
        std::make_unique<Worker>(&key_lock_table_, max_queue_depth, i));
  }
}

rocksdb::Status Scheduler::Submit(std::unique_ptr<Cmd> cmd,
                                  Completion completion) {
  if (cmd == nullptr) {
    return rocksdb::Status::InvalidArgument("cmd is required");
  }

  auto task = std::make_unique<WorkerTask>();
  task->cmd = std::move(cmd);
  task->completion = [this, completion = std::move(completion)](
                         CommandResponse response) mutable {
    inflight_requests_.fetch_sub(1, std::memory_order_relaxed);
    completion(std::move(response));
  };

  const size_t start = next_worker_.fetch_add(1, std::memory_order_relaxed);
  for (size_t offset = 0; offset < workers_.size(); ++offset) {
    Worker* worker = workers_[(start + offset) % workers_.size()].get();
    if (worker->Enqueue(task.get())) {
      inflight_requests_.fetch_add(1, std::memory_order_relaxed);
      task.release();
      return rocksdb::Status::OK();
    }
  }

  rejected_requests_.fetch_add(1, std::memory_order_relaxed);
  return rocksdb::Status::Busy("worker queue full");
}

std::vector<size_t> Scheduler::worker_queue_depth() const {
  std::vector<size_t> queue_depth;
  queue_depth.reserve(workers_.size());
  for (const auto& worker : workers_) {
    queue_depth.push_back(worker->backlog());
  }
  return queue_depth;
}

MetricsSnapshot Scheduler::GetMetricsSnapshot() const {
  MetricsSnapshot snapshot;
  snapshot.worker_queue_depth = worker_queue_depth();
  snapshot.worker_rejections = rejected_requests();
  snapshot.worker_inflight = inflight_requests();
  return snapshot;
}

}  // namespace minikv
