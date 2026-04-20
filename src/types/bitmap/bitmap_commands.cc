#include "types/bitmap/bitmap_commands.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "types/bitmap/bitmap_module.h"

namespace minikv {

namespace {

bool ParseUint64Strict(const std::string& input, uint64_t* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  uint64_t parsed = 0;
  for (char c : input) {
    if (c < '0' || c > '9') {
      return false;
    }
    const uint64_t digit = static_cast<uint64_t>(c - '0');
    if (parsed > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
      return false;
    }
    parsed = parsed * 10 + digit;
  }

  *value = parsed;
  return true;
}

class GetBitCmd : public Cmd {
 public:
  GetBitCmd(const CmdRegistration& registration, BitmapModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("GETBIT requires offset");
    }
    if (!ParseUint64Strict(input.args[0], &offset_)) {
      return rocksdb::Status::InvalidArgument(
          "GETBIT requires non-negative integer offset");
    }

    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("bitmap module is unavailable"));
    }

    int bit = 0;
    rocksdb::Status status = module_->GetBit(key_, offset_, &bit);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(bit);
  }

  BitmapModule* module_ = nullptr;
  std::string key_;
  uint64_t offset_ = 0;
};

class SetBitCmd : public Cmd {
 public:
  SetBitCmd(const CmdRegistration& registration, BitmapModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument("SETBIT requires offset and bit");
    }
    if (!ParseUint64Strict(input.args[0], &offset_)) {
      return rocksdb::Status::InvalidArgument(
          "SETBIT requires non-negative integer offset");
    }
    if (input.args[1] == "0") {
      bit_ = 0;
    } else if (input.args[1] == "1") {
      bit_ = 1;
    } else {
      return rocksdb::Status::InvalidArgument("SETBIT bit must be 0 or 1");
    }

    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("bitmap module is unavailable"));
    }

    int old_bit = 0;
    rocksdb::Status status = module_->SetBit(key_, offset_, bit_, &old_bit);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(old_bit);
  }

  BitmapModule* module_ = nullptr;
  std::string key_;
  uint64_t offset_ = 0;
  int bit_ = 0;
};

class BitCountCmd : public Cmd {
 public:
  BitCountCmd(const CmdRegistration& registration, BitmapModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "BITCOUNT takes no extra arguments");
    }

    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("bitmap module is unavailable"));
    }

    uint64_t count = 0;
    rocksdb::Status status = module_->CountBits(key_, &count);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(count));
  }

  BitmapModule* module_ = nullptr;
  std::string key_;
};

}  // namespace

rocksdb::Status RegisterBitmapCommands(ModuleServices& services,
                                       BitmapModule* module) {
  rocksdb::Status status = services.command_registry().Register(
      {"GETBIT", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<GetBitCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SETBIT", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<SetBitCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"BITCOUNT", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<BitCountCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

}  // namespace minikv
