#include "worker/worker.h"

#include <algorithm>
#include <exception>
#include <limits>
#include <utility>

#include "common/thread_name.h"

namespace minikv {

Worker::BoundedMPSCQueue::BoundedMPSCQueue(size_t capacity)
    : capacity_(NormalizeCapacity(capacity)),
      mask_(capacity_ - 1),
      buffer_(std::make_unique<Cell[]>(capacity_)) {}

size_t Worker::BoundedMPSCQueue::NormalizeCapacity(size_t capacity) {
  size_t normalized = std::max<size_t>(1, capacity);
  size_t power_of_two = 1;
  while (power_of_two < normalized &&
         power_of_two < (std::numeric_limits<size_t>::max() >> 1)) {
    power_of_two <<= 1;
  }
  return std::max<size_t>(1, power_of_two);
}

bool Worker::BoundedMPSCQueue::TryEnqueue(WorkerTask* task) {
  size_t pos = head_.load(std::memory_order_relaxed);
  while (true) {
    const size_t tail = tail_.load(std::memory_order_acquire);
    if (pos - tail >= capacity_) {
      return false;
    }
    if (!head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
      continue;
    }

    Cell& cell = buffer_[pos & mask_];
    cell.task = task;
    cell.ready.store(true, std::memory_order_release);
    return true;
  }
}

bool Worker::BoundedMPSCQueue::TryDequeue(WorkerTask** task) {
  const size_t pos = tail_.load(std::memory_order_relaxed);
  Cell& cell = buffer_[pos & mask_];
  if (!cell.ready.load(std::memory_order_acquire)) {
    return false;
  }

  *task = cell.task;
  cell.task = nullptr;
  cell.ready.store(false, std::memory_order_release);
  tail_.store(pos + 1, std::memory_order_release);
  return true;
}

bool Worker::BoundedMPSCQueue::HasPending() const {
  return head_.load(std::memory_order_acquire) !=
         tail_.load(std::memory_order_acquire);
}

size_t Worker::BoundedMPSCQueue::Backlog() const {
  const size_t head = head_.load(std::memory_order_acquire);
  const size_t tail = tail_.load(std::memory_order_acquire);
  return head - tail;
}

Worker::Worker(KeyLockTable* key_lock_table, size_t queue_depth, size_t worker_id)
    : key_lock_table_(key_lock_table),
      queue_(queue_depth),
      worker_id_(worker_id),
      thread_([this] { Run(); }) {}

Worker::~Worker() {
  stopping_.store(true, std::memory_order_release);
  wait_cv_.notify_one();
  if (thread_.joinable()) {
    thread_.join();
  }
}

bool Worker::Enqueue(WorkerTask* task) {
  const bool was_empty = !queue_.HasPending();
  if (!queue_.TryEnqueue(task)) {
    return false;
  }
  if (was_empty) {
    wait_cv_.notify_one();
  }
  return true;
}

size_t Worker::backlog() const { return queue_.Backlog(); }

CommandResponse ExecuteCommand(KeyLockTable* key_lock_table, Cmd* cmd) {
  KeyLockTable::Guard guard;
  KeyLockTable::MultiGuard multi_guard;
  if (key_lock_table != nullptr) {
    switch (cmd->lock_plan().kind()) {
      case Cmd::LockPlan::Kind::kNone:
        break;
      case Cmd::LockPlan::Kind::kSingle:
        guard = key_lock_table->Acquire(cmd->lock_plan().single_key());
        break;
      case Cmd::LockPlan::Kind::kMulti:
        multi_guard =
            key_lock_table->AcquireMulti(cmd->lock_plan().multi_keys());
        break;
    }
  }

  try {
    return cmd->Execute();
  } catch (const std::exception& e) {
    return CommandResponse{rocksdb::Status::Aborted(e.what()), {}};
  } catch (...) {
    return CommandResponse{
        rocksdb::Status::Aborted("unknown worker failure"), {}};
  }
}

CommandResponse Worker::ExecuteTask(WorkerTask* task) {
  return ExecuteCommand(key_lock_table_, task->cmd.get());
}

void Worker::Run() {
  SetCurrentThreadName("minikv-w" + std::to_string(worker_id_));

  while (true) {
    WorkerTask* raw_task = nullptr;
    if (!queue_.TryDequeue(&raw_task)) {
      std::unique_lock<std::mutex> lock(wait_mutex_);
      wait_cv_.wait(lock, [&] {
        return stopping_.load(std::memory_order_acquire) || queue_.HasPending();
      });
      if (stopping_.load(std::memory_order_acquire) && !queue_.HasPending()) {
        return;
      }
      continue;
    }

    std::unique_ptr<WorkerTask> task(raw_task);
    CommandResponse response = ExecuteTask(task.get());
    task->completion(std::move(response));
  }
}

}  // namespace minikv
