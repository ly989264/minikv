#include "runtime/module/background_executor.h"

#include <condition_variable>
#include <system_error>
#include <utility>

#include "common/thread_name.h"

namespace minikv {

BackgroundExecutor::BackgroundExecutor(std::string thread_name)
    : thread_name_(std::move(thread_name)) {}

BackgroundExecutor::~BackgroundExecutor() { Stop(); }

rocksdb::Status BackgroundExecutor::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (started_) {
    return rocksdb::Status::InvalidArgument(
        "background executor already started");
  }

  tasks_.clear();
  stopping_ = false;
  started_ = true;
  try {
    worker_ = std::thread([this]() { Run(); });
  } catch (const std::system_error& error) {
    started_ = false;
    return rocksdb::Status::Aborted(error.what());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status BackgroundExecutor::Submit(Task task) {
  if (!task) {
    return rocksdb::Status::InvalidArgument("background task is required");
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!started_) {
      return rocksdb::Status::InvalidArgument(
          "background executor is unavailable");
    }
    if (stopping_) {
      return rocksdb::Status::Aborted("background executor is stopping");
    }
    tasks_.push_back(std::move(task));
  }
  cv_.notify_one();
  return rocksdb::Status::OK();
}

void BackgroundExecutor::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!started_) {
      return;
    }
    stopping_ = true;
  }
  cv_.notify_all();

  if (worker_.joinable()) {
    worker_.join();
  }

  std::lock_guard<std::mutex> lock(mutex_);
  tasks_.clear();
  started_ = false;
  stopping_ = false;
}

void BackgroundExecutor::Run() {
  SetCurrentThreadName(thread_name_);

  std::unique_lock<std::mutex> lock(mutex_);
  while (true) {
    cv_.wait(lock, [this]() { return stopping_ || !tasks_.empty(); });
    if (tasks_.empty()) {
      if (stopping_) {
        break;
      }
      continue;
    }

    Task task = std::move(tasks_.front());
    tasks_.erase(tasks_.begin());
    lock.unlock();
    task();
    lock.lock();
  }
}

}  // namespace minikv
