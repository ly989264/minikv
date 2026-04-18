#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "execution/command/cmd.h"
#include "execution/worker/key_lock_table.h"

namespace minikv {

struct WorkerTask {
  using Completion = std::function<void(CommandResponse)>;

  std::unique_ptr<Cmd> cmd;
  Completion completion;
};

class Worker {
 public:
  Worker(KeyLockTable* key_lock_table, size_t queue_depth, size_t worker_id);
  ~Worker();

  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  bool Enqueue(WorkerTask* task);
  size_t backlog() const;

 private:
  class BoundedMPSCQueue {
   public:
    explicit BoundedMPSCQueue(size_t capacity);

    bool TryEnqueue(WorkerTask* task);
    bool TryDequeue(WorkerTask** task);
    bool HasPending() const;
    size_t Backlog() const;

   private:
    struct Cell {
      std::atomic<bool> ready{false};
      WorkerTask* task = nullptr;
    };

    static size_t NormalizeCapacity(size_t capacity);

    const size_t capacity_;
    const size_t mask_;
    std::unique_ptr<Cell[]> buffer_;
    std::atomic<size_t> head_{0};
    std::atomic<size_t> tail_{0};
  };

  CommandResponse ExecuteTask(WorkerTask* task);
  void Run();

  KeyLockTable* key_lock_table_;
  BoundedMPSCQueue queue_;
  size_t worker_id_;
  std::mutex wait_mutex_;
  std::condition_variable wait_cv_;
  std::atomic<bool> stopping_{false};
  std::thread thread_;
};

CommandResponse ExecuteCommand(KeyLockTable* key_lock_table, Cmd* cmd);

}  // namespace minikv
