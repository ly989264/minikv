#pragma once

#include <cstdint>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <string>

#include "command/cmd.h"

namespace minikv {

enum class CommandSource : uint8_t {
  kBuiltin,
  kModule,
};

struct CmdRegistration {
  std::string name;
  CmdFlags flags = CmdFlags::kNone;
  CommandSource source = CommandSource::kBuiltin;
  std::string owner_module;
  std::function<std::unique_ptr<Cmd>(const CmdRegistration&)> creator;
};

std::string NormalizeCommandName(const std::string& name);

class CommandRegistry {
 public:
  rocksdb::Status Register(CmdRegistration registration);
  const CmdRegistration* Find(const std::string& name) const;
  size_t size() const { return registrations_.size(); }

 private:
  std::map<std::string, CmdRegistration> registrations_;
};

}  // namespace minikv
