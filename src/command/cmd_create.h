#pragma once

#include <memory>
#include <vector>

#include "command/cmd.h"

namespace minikv {

rocksdb::Status CreateCmd(const std::vector<std::string>& parts,
                          std::unique_ptr<Cmd>* cmd);

}  // namespace minikv
