#pragma once

#include <string>

#include "kernel/command_registry.h"

namespace minikv {

class CmdFactory {
 public:
  static const CommandRegistry& Registry();
  static const CmdRegistration* FindByName(const std::string& name);
};

}  // namespace minikv
