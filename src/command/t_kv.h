#pragma once

#include <memory>

#include "command/cmd_factory.h"

namespace minikv {

std::unique_ptr<Cmd> CreatePingCmd(const CmdRegistration& registration);

}  // namespace minikv
