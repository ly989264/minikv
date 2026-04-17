#pragma once

#include <memory>

#include "config.h"
#include "rocksdb/status.h"

namespace minikv {

class Scheduler;
class NetworkServer;

class MiniKV {
 public:
  ~MiniKV();

  MiniKV(const MiniKV&) = delete;
  MiniKV& operator=(const MiniKV&) = delete;

  static rocksdb::Status Open(const Config& config,
                              std::unique_ptr<MiniKV>* minikv);

 private:
  friend class NetworkServer;
  class Impl;

  explicit MiniKV(std::unique_ptr<Impl> impl);
  Scheduler* scheduler();

  std::unique_ptr<Impl> impl_;
};

}  // namespace minikv
