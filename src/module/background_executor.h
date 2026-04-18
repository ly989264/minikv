#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/status.h"

namespace minikv {

class BackgroundExecutor {
 public:
  using Task = std::function<void()>;

  explicit BackgroundExecutor(std::string thread_name = "module-bg");
  ~BackgroundExecutor();

  BackgroundExecutor(const BackgroundExecutor&) = delete;
  BackgroundExecutor& operator=(const BackgroundExecutor&) = delete;

  rocksdb::Status Start();
  rocksdb::Status Submit(Task task);
  void Stop();

 private:
  void Run();

  std::string thread_name_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::vector<Task> tasks_;
  std::thread worker_;
  bool started_ = false;
  bool stopping_ = false;
};

}  // namespace minikv
