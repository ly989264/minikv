#include "command/t_hash.h"

#include <memory>
#include <utility>
#include <vector>

#include "types/hash/hash_module.h"

namespace minikv {
namespace {

class HSetCmd : public Cmd {
 public:
  explicit HSetCmd(const CmdRegistration& registration)
      : Cmd(registration.name, registration.flags) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument("HSET requires field and value");
    }
    key_ = input.key;
    field_ = input.args[0];
    value_ = input.args[1];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do(CommandServices* context) override {
    if (context == nullptr || context->hash_module == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("hash module is unavailable"));
    }
    bool inserted = false;
    rocksdb::Status status =
        context->hash_module->PutField(key_, field_, value_, &inserted);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(inserted ? 1 : 0);
  }

  std::string key_;
  std::string field_;
  std::string value_;
};

class HGetAllCmd : public Cmd {
 public:
  explicit HGetAllCmd(const CmdRegistration& registration)
      : Cmd(registration.name, registration.flags) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "HGETALL takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do(CommandServices* context) override {
    if (context == nullptr || context->hash_module == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("hash module is unavailable"));
    }
    std::vector<FieldValue> values;
    rocksdb::Status status = context->hash_module->ReadAll(key_, &values);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }

    std::vector<std::string> flattened;
    flattened.reserve(values.size() * 2);
    for (const auto& item : values) {
      flattened.push_back(item.field);
      flattened.push_back(item.value);
    }
    return MakeArray(std::move(flattened));
  }

  std::string key_;
};

class HDelCmd : public Cmd {
 public:
  explicit HDelCmd(const CmdRegistration& registration)
      : Cmd(registration.name, registration.flags) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument("HDEL requires at least one field");
    }
    key_ = input.key;
    fields_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do(CommandServices* context) override {
    if (context == nullptr || context->hash_module == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("hash module is unavailable"));
    }
    uint64_t deleted = 0;
    rocksdb::Status status =
        context->hash_module->DeleteFields(key_, fields_, &deleted);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(deleted));
  }

  std::string key_;
  std::vector<std::string> fields_;
};

}  // namespace

std::unique_ptr<Cmd> CreateHSetCmd(const CmdRegistration& registration) {
  return std::make_unique<HSetCmd>(registration);
}

std::unique_ptr<Cmd> CreateHGetAllCmd(const CmdRegistration& registration) {
  return std::make_unique<HGetAllCmd>(registration);
}

std::unique_ptr<Cmd> CreateHDelCmd(const CmdRegistration& registration) {
  return std::make_unique<HDelCmd>(registration);
}

}  // namespace minikv
