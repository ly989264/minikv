#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "runtime/config.h"
#include "rocksdb/status.h"

namespace minikv {

class MiniKV;
class Scheduler;

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

class NetworkServer {
 public:
  NetworkServer(const Config& config, MiniKV* minikv);
  ~NetworkServer();

  NetworkServer(const NetworkServer&) = delete;
  NetworkServer& operator=(const NetworkServer&) = delete;

  rocksdb::Status Start();
  void Stop();
  void Wait();
  rocksdb::Status Run();
  uint16_t port() const;
  MetricsSnapshot GetMetricsSnapshot() const;

 private:
  struct Impl;
  static Scheduler* GetScheduler(MiniKV* minikv);

  std::unique_ptr<Impl> impl_;
};

}  // namespace minikv
