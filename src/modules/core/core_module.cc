#include "modules/core/core_module.h"

#include <memory>

#include "command/cmd.h"
#include "module/module_services.h"

namespace minikv {
namespace {

class PingCmd : public Cmd {
 public:
  explicit PingCmd(const CmdRegistration& registration)
      : Cmd(registration.name, registration.flags) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (input.has_key || !input.args.empty()) {
      return rocksdb::Status::InvalidArgument("PING takes no arguments");
    }
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override { return MakeSimpleString("PONG"); }
};

}  // namespace

rocksdb::Status CoreModule::OnLoad(ModuleServices& services) {
  rocksdb::Status status = services.command_registry().Register(
      {"PING", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [](const CmdRegistration& registration) {
         return std::make_unique<PingCmd>(registration);
       }});
  if (status.ok()) {
    services.metrics().IncrementCounter("commands.registered");
  }
  return status;
}

rocksdb::Status CoreModule::OnStart(ModuleServices& services) {
  started_ = true;
  services.metrics().SetCounter("worker_count",
                                services.scheduler().worker_count());
  return rocksdb::Status::OK();
}

void CoreModule::OnStop(ModuleServices& /*services*/) { started_ = false; }

}  // namespace minikv
