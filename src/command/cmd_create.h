#pragma once

#include <memory>
#include <vector>

#include "command/cmd.h"

namespace minikv {

rocksdb::Status CreateCmd(const std::vector<std::string>& parts,
                          std::unique_ptr<Cmd>* cmd);
// CommandRequest may come from the legacy CommandType shim or direct
// string-based command names. Both resolve through the same registry path.
rocksdb::Status CreateCmd(const CommandRequest& request,
                          std::unique_ptr<Cmd>* cmd);

}  // namespace minikv
