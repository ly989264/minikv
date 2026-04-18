#pragma once

#include "execution/reply/reply.h"
#include "rocksdb/status.h"

namespace minikv {

struct CommandResponse {
  rocksdb::Status status;
  ReplyNode reply;
};

}  // namespace minikv
