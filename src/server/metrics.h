#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace minikv {

struct MetricsSnapshot {
  std::vector<size_t> worker_queue_depth;
  uint64_t worker_rejections = 0;
  uint64_t worker_inflight = 0;

  uint64_t active_connections = 0;
  uint64_t accepted_connections = 0;
  uint64_t closed_connections = 0;
  uint64_t idle_timeout_connections = 0;
  uint64_t errored_connections = 0;
  uint64_t parse_errors = 0;
};

}  // namespace minikv
