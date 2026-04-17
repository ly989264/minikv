#pragma once

#include "kernel/reply.h"
#include "rocksdb/status.h"

namespace minikv {

struct CommandResponse {
  rocksdb::Status status;
  ReplyNode reply;
};

}  // namespace minikv
