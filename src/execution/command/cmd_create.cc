#include "execution/command/cmd_create.h"

#include <memory>
#include <utility>
#include <vector>

namespace minikv {
namespace {

rocksdb::Status CreateCmdFromRegistration(const CmdRegistration& registration,
                                          const CmdInput& input,
                                          std::unique_ptr<Cmd>* cmd) {
  std::unique_ptr<Cmd> created = registration.creator(registration);
  if (created == nullptr) {
    return rocksdb::Status::Corruption("command creator returned null");
  }
  rocksdb::Status status = created->Init(input);
  if (!status.ok()) {
    return status;
  }
  *cmd = std::move(created);
  return rocksdb::Status::OK();
}

CmdInput MakeInput(const std::vector<std::string>& parts) {
  CmdInput input;
  if (parts.size() >= 2) {
    input.has_key = true;
    input.key = parts[1];
  }
  if (parts.size() >= 3) {
    input.args.assign(parts.begin() + 2, parts.end());
  }
  return input;
}

}  // namespace

rocksdb::Status CreateCmd(const CommandRegistry& registry,
                          const std::vector<std::string>& parts,
                          std::unique_ptr<Cmd>* cmd) {
  if (cmd == nullptr) {
    return rocksdb::Status::InvalidArgument("cmd output is required");
  }
  cmd->reset();
  if (parts.empty()) {
    return rocksdb::Status::InvalidArgument("empty command");
  }

  const CmdRegistration* registration =
      registry.Find(NormalizeCommandName(parts[0]));
  if (registration == nullptr) {
    return rocksdb::Status::InvalidArgument("unsupported command");
  }
  return CreateCmdFromRegistration(*registration, MakeInput(parts), cmd);
}

}  // namespace minikv
