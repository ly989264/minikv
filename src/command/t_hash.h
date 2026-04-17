#pragma once

#include <memory>

#include "command/cmd_factory.h"

namespace minikv {

std::unique_ptr<Cmd> CreateHSetCmd(const CmdRegistration& registration);
std::unique_ptr<Cmd> CreateHGetAllCmd(const CmdRegistration& registration);
std::unique_ptr<Cmd> CreateHDelCmd(const CmdRegistration& registration);

}  // namespace minikv
