#pragma once

#include <memory>
#include <vector>

#include "command/cmd.h"
#include "kernel/command_registry.h"

namespace minikv {

rocksdb::Status CreateCmd(const CommandRegistry& registry,
                          const std::vector<std::string>& parts,
                          std::unique_ptr<Cmd>* cmd);

}  // namespace minikv
