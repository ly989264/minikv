#include "types/list/list_commands.h"

#include <cerrno>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>

#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "types/list/list_module.h"

namespace minikv {

namespace {

bool ParseInt64(const std::string& input, int64_t* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  errno = 0;
  char* parse_end = nullptr;
  const long long parsed = std::strtoll(input.c_str(), &parse_end, 10);
  if (parse_end == nullptr || *parse_end != '\0' || errno == ERANGE) {
    return false;
  }
  *value = static_cast<int64_t>(parsed);
  return true;
}

class LPushCmd : public Cmd {
 public:
  LPushCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "LPUSH requires at least one element");
    }
    key_ = input.key;
    elements_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    uint64_t length = 0;
    rocksdb::Status status = module_->PushLeft(key_, elements_, &length);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(length));
  }

  ListModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> elements_;
};

class LPopCmd : public Cmd {
 public:
  LPopCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("LPOP takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    std::string element;
    bool found = false;
    rocksdb::Status status = module_->PopLeft(key_, &element, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeBulkString(std::move(element));
  }

  ListModule* module_ = nullptr;
  std::string key_;
};

class LRangeCmd : public Cmd {
 public:
  LRangeCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument(
          "LRANGE requires start and stop");
    }
    if (!ParseInt64(input.args[0], &start_)) {
      return rocksdb::Status::InvalidArgument("LRANGE requires integer start");
    }
    if (!ParseInt64(input.args[1], &stop_)) {
      return rocksdb::Status::InvalidArgument("LRANGE requires integer stop");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    std::vector<std::string> values;
    rocksdb::Status status = module_->ReadRange(key_, start_, stop_, &values);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeArray(std::move(values));
  }

  ListModule* module_ = nullptr;
  std::string key_;
  int64_t start_ = 0;
  int64_t stop_ = 0;
};

class RPushCmd : public Cmd {
 public:
  RPushCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "RPUSH requires at least one element");
    }
    key_ = input.key;
    elements_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    uint64_t length = 0;
    rocksdb::Status status = module_->PushRight(key_, elements_, &length);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(length));
  }

  ListModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> elements_;
};

class RPopCmd : public Cmd {
 public:
  RPopCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("RPOP takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    std::string element;
    bool found = false;
    rocksdb::Status status = module_->PopRight(key_, &element, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeBulkString(std::move(element));
  }

  ListModule* module_ = nullptr;
  std::string key_;
};

class LRemCmd : public Cmd {
 public:
  LRemCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument("LREM requires count and element");
    }
    if (!ParseInt64(input.args[0], &count_)) {
      return rocksdb::Status::InvalidArgument("LREM requires integer count");
    }
    key_ = input.key;
    element_ = input.args[1];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    uint64_t removed = 0;
    rocksdb::Status status =
        module_->RemoveElements(key_, count_, element_, &removed);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(removed));
  }

  ListModule* module_ = nullptr;
  std::string key_;
  std::string element_;
  int64_t count_ = 0;
};

class LTrimCmd : public Cmd {
 public:
  LTrimCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument(
          "LTRIM requires start and stop");
    }
    if (!ParseInt64(input.args[0], &start_)) {
      return rocksdb::Status::InvalidArgument("LTRIM requires integer start");
    }
    if (!ParseInt64(input.args[1], &stop_)) {
      return rocksdb::Status::InvalidArgument("LTRIM requires integer stop");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    rocksdb::Status status = module_->Trim(key_, start_, stop_);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeSimpleString("OK");
  }

  ListModule* module_ = nullptr;
  std::string key_;
  int64_t start_ = 0;
  int64_t stop_ = 0;
};

class LLenCmd : public Cmd {
 public:
  LLenCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("LLEN takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    uint64_t length = 0;
    rocksdb::Status status = module_->Length(key_, &length);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(length));
  }

  ListModule* module_ = nullptr;
  std::string key_;
};

}  // namespace

rocksdb::Status RegisterListCommands(ModuleServices& services,
                                     ListModule* module) {
  rocksdb::Status status = services.command_registry().Register(
      {"LPUSH", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<LPushCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"LPOP", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<LPopCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"LRANGE", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<LRangeCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"RPUSH", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<RPushCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"RPOP", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<RPopCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"LREM", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<LRemCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"LTRIM", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<LTrimCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"LLEN", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<LLenCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

}  // namespace minikv
