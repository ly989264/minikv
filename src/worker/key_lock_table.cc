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

size_t KeyLockTable::StripeIndex(const std::string& key) const {
  return std::hash<std::string>{}(key) % stripes_.size();
}

KeyLockTable::Guard KeyLockTable::Acquire(const std::string& key) {
  return Guard(std::unique_lock<std::mutex>(*stripes_[StripeIndex(key)]));
}

KeyLockTable::MultiGuard KeyLockTable::AcquireMulti(
    const std::vector<std::string>& keys) {
  if (keys.empty()) {
    return MultiGuard();
  }

  std::vector<size_t> stripe_indexes;
  stripe_indexes.reserve(keys.size());
  for (const auto& key : keys) {
    stripe_indexes.push_back(StripeIndex(key));
  }
  std::sort(stripe_indexes.begin(), stripe_indexes.end());
  stripe_indexes.erase(
      std::unique(stripe_indexes.begin(), stripe_indexes.end()),
      stripe_indexes.end());

  std::vector<std::unique_lock<std::mutex>> locks;
  locks.reserve(stripe_indexes.size());
  for (size_t index : stripe_indexes) {
    locks.emplace_back(*stripes_[index]);
  }
  return MultiGuard(std::move(locks));
}

}  // namespace minikv
