#pragma once

#include <cstdint>
#include <string>

namespace minikv {

struct Config {
  std::string db_path = "/tmp/minikv-db";
  std::string bind_host = "0.0.0.0";
  uint16_t port = 6389;
  size_t io_threads = 2;
  size_t worker_threads = 4;
  size_t max_pending_requests_per_worker = 1024;
  size_t max_connections = 1024;
  size_t max_request_bytes = 64 * 1024;
  uint64_t idle_connection_timeout_ms = 30000;
};

}  // namespace minikv
