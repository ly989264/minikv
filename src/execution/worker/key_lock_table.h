#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace minikv {

class KeyLockTable {
 public:
  class Guard {
   public:
    Guard() = default;
    explicit Guard(std::unique_lock<std::mutex> lock)
        : lock_(std::move(lock)) {}

    Guard(Guard&&) = default;
    Guard& operator=(Guard&&) = default;

   private:
    std::unique_lock<std::mutex> lock_;
  };

  class MultiGuard {
   public:
    MultiGuard() = default;
    explicit MultiGuard(std::vector<std::unique_lock<std::mutex>> locks)
        : locks_(std::move(locks)) {}

    MultiGuard(MultiGuard&&) = default;
    MultiGuard& operator=(MultiGuard&&) = default;

   private:
    std::vector<std::unique_lock<std::mutex>> locks_;
  };

  explicit KeyLockTable(size_t stripe_count);

  KeyLockTable(const KeyLockTable&) = delete;
  KeyLockTable& operator=(const KeyLockTable&) = delete;

  Guard Acquire(const std::string& key);
  MultiGuard AcquireMulti(const std::vector<std::string>& keys);
  size_t stripe_count() const { return stripes_.size(); }

  static size_t DefaultStripeCount(size_t worker_count) {
    const size_t normalized_worker_count = std::max<size_t>(1, worker_count);
    return std::max<size_t>(64, normalized_worker_count * 64);
  }

 private:
  size_t StripeIndex(const std::string& key) const;

  std::vector<std::unique_ptr<std::mutex>> stripes_;
};

}  // namespace minikv
