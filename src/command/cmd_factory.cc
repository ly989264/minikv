#include "command/cmd_factory.h"

#include <cassert>
#include <utility>

#include "command/t_hash.h"
#include "command/t_kv.h"

namespace minikv {
namespace {

void MustRegister(CommandRegistry* registry, CmdRegistration registration) {
  rocksdb::Status status = registry->Register(std::move(registration));
  assert(status.ok());
}

CommandRegistry BuildRegistry() {
  CommandRegistry registry;
  MustRegister(&registry, {"PING", CmdFlags::kRead | CmdFlags::kFast,
                           CommandSource::kBuiltin, &CreatePingCmd});
  MustRegister(&registry, {"HSET", CmdFlags::kWrite | CmdFlags::kFast,
                           CommandSource::kBuiltin, &CreateHSetCmd});
  MustRegister(&registry, {"HGETALL", CmdFlags::kRead | CmdFlags::kSlow,
                           CommandSource::kBuiltin, &CreateHGetAllCmd});
  MustRegister(&registry, {"HDEL", CmdFlags::kWrite | CmdFlags::kSlow,
                           CommandSource::kBuiltin, &CreateHDelCmd});
  return registry;
}

}  // namespace

const CommandRegistry& CmdFactory::Registry() {
  static const CommandRegistry registry = BuildRegistry();
  return registry;
}

const CmdRegistration* CmdFactory::FindByName(const std::string& name) {
  return Registry().Find(name);
}

}  // namespace minikv
