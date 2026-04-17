#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "minikv/config.h"
#include "minikv/minikv.h"
#include "server/server.h"

namespace {

minikv::Config ParseConfig(int argc, char** argv) {
  minikv::Config config;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--db_path" && i + 1 < argc) {
      config.db_path = argv[++i];
    } else if (arg == "--bind" && i + 1 < argc) {
      config.bind_host = argv[++i];
    } else if (arg == "--port" && i + 1 < argc) {
      config.port = static_cast<uint16_t>(std::strtoul(argv[++i], nullptr, 10));
    } else if (arg == "--io_threads" && i + 1 < argc) {
      config.io_threads = std::strtoul(argv[++i], nullptr, 10);
    } else if (arg == "--workers" && i + 1 < argc) {
      config.worker_threads = std::strtoul(argv[++i], nullptr, 10);
    } else if (arg == "--max_pending" && i + 1 < argc) {
      config.max_pending_requests_per_worker =
          std::strtoul(argv[++i], nullptr, 10);
    } else if (arg == "--max_connections" && i + 1 < argc) {
      config.max_connections = std::strtoul(argv[++i], nullptr, 10);
    } else if (arg == "--max_request_bytes" && i + 1 < argc) {
      config.max_request_bytes = std::strtoul(argv[++i], nullptr, 10);
    } else if (arg == "--idle_timeout_ms" && i + 1 < argc) {
      config.idle_connection_timeout_ms =
          std::strtoull(argv[++i], nullptr, 10);
    }
  }
  return config;
}

}  // namespace

int main(int argc, char** argv) {
  const minikv::Config config = ParseConfig(argc, argv);

  std::unique_ptr<minikv::MiniKV> minikv;
  rocksdb::Status status = minikv::MiniKV::Open(config, &minikv);
  if (!status.ok()) {
    std::cerr << "failed to open minikv: " << status.ToString() << "\n";
    return 1;
  }

  minikv::Server server(config, minikv.get());
  status = server.Run();
  if (!status.ok()) {
    std::cerr << "server stopped with error: " << status.ToString() << "\n";
    return 1;
  }
  return 0;
}
