#include "worker/key_lock_table.h"

#include <functional>

namespace minikv {

KeyLockTable::KeyLockTable(size_t stripe_count) {
  const size_t normalized_stripe_count = std::max<size_t>(1, stripe_count);
  stripes_.reserve(normalized_stripe_count);
  for (size_t i = 0; i < normalized_stripe_count; ++i) {
    stripes_.push_back(std::make_unique<std::mutex>());
  }
}

KeyLockTable::Guard KeyLockTable::Acquire(const std::string& key) {
  if (key.empty()) {
    return Guard();
  }
  const size_t index = std::hash<std::string>{}(key) % stripes_.size();
  return Guard(std::unique_lock<std::mutex>(*stripes_[index]));
}

}  // namespace minikv
