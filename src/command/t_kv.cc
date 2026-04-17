#include "command/t_kv.h"

#include <memory>

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

  CommandResponse Do(CommandContext* /*context*/) override {
    return MakeSimpleString("PONG");
  }
};

}  // namespace

std::unique_ptr<Cmd> CreatePingCmd(const CmdRegistration& registration) {
  return std::make_unique<PingCmd>(registration);
}

}  // namespace minikv
