#include "kernel/command_registry.h"

#include <cctype>

namespace minikv {

std::string NormalizeCommandName(const std::string& name) {
  std::string normalized = name;
  for (char& c : normalized) {
    c = static_cast<char>(::toupper(static_cast<unsigned char>(c)));
  }
  return normalized;
}

rocksdb::Status CommandRegistry::Register(CmdRegistration registration) {
  if (registration.name.empty()) {
    return rocksdb::Status::InvalidArgument("command name is required");
  }
  if (!registration.creator) {
    return rocksdb::Status::InvalidArgument("command creator is required");
  }

  registration.name = NormalizeCommandName(registration.name);
  auto [it, inserted] =
      registrations_.emplace(registration.name, std::move(registration));
  if (!inserted) {
    std::string message = "command already registered: " + it->first;
    if (!it->second.owner_module.empty()) {
      message += " existing module=" + it->second.owner_module;
    }
    if (!registration.owner_module.empty()) {
      message += " new module=" + registration.owner_module;
    }
    return rocksdb::Status::InvalidArgument(message);
  }
  return rocksdb::Status::OK();
}

const CmdRegistration* CommandRegistry::Find(const std::string& name) const {
  auto it = registrations_.find(name);
  if (it == registrations_.end()) {
    return nullptr;
  }
  return &it->second;
}

}  // namespace minikv
