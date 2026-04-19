#include "types/bitmap/bitmap_module.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>

#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "types/string/string_bridge.h"

namespace minikv {

namespace {

// Guard the bitmap auto-expansion path from attempting pathological allocations.
// This keeps one oversized SETBIT offset from forcing multi-gigabyte materialization.
constexpr size_t kMaxBitmapMaterializedBytes = 512ULL * 1024ULL * 1024ULL;

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

rocksdb::Status GetRequiredByteCount(uint64_t offset, size_t max_size,
                                     size_t* required_bytes) {
  if (required_bytes == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "required bitmap byte count output is required");
  }

  const uint64_t byte_index = offset / 8;
  if (byte_index >= static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
    return rocksdb::Status::InvalidArgument("bitmap offset is too large");
  }

  const size_t bytes = static_cast<size_t>(byte_index + 1);
  if (bytes > kMaxBitmapMaterializedBytes) {
    return rocksdb::Status::InvalidArgument("bitmap offset is too large");
  }
  if (bytes > max_size) {
    return rocksdb::Status::InvalidArgument("bitmap offset is too large");
  }

  *required_bytes = bytes;
  return rocksdb::Status::OK();
}

rocksdb::Status GetByteIndexForOffset(uint64_t offset, size_t* byte_index) {
  if (byte_index == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "bitmap byte index output is required");
  }

  const uint64_t index = offset / 8;
  if (index > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
    return rocksdb::Status::InvalidArgument("bitmap offset is too large");
  }

  *byte_index = static_cast<size_t>(index);
  return rocksdb::Status::OK();
}

unsigned char BitMaskForOffset(uint64_t offset) {
  return static_cast<unsigned char>(1u << (7 - (offset % 8)));
}

int ReadBitAtByteIndex(const std::string& value, size_t byte_index,
                       uint64_t offset) {
  if (byte_index >= value.size()) {
    return 0;
  }

  const unsigned char byte =
      static_cast<unsigned char>(value[byte_index]);
  return (byte & BitMaskForOffset(offset)) != 0 ? 1 : 0;
}

uint64_t CountSetBits(const std::string& value) {
  uint64_t count = 0;
  for (unsigned char byte : value) {
    count += static_cast<uint64_t>(__builtin_popcount(byte));
  }
  return count;
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

rocksdb::Status BitmapModule::OnLoad(ModuleServices& services) {
  services_ = &services;

  rocksdb::Status status = services.command_registry().Register(
      {"GETBIT", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [this](const CmdRegistration& registration) {
         return std::make_unique<GetBitCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SETBIT", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [this](const CmdRegistration& registration) {
         return std::make_unique<SetBitCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"BITCOUNT", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<BitCountCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

rocksdb::Status BitmapModule::OnStart(ModuleServices& services) {
  string_bridge_ = services.exports().Find<StringBridge>(
      kStringBridgeQualifiedExportName);
  if (string_bridge_ == nullptr) {
    return rocksdb::Status::InvalidArgument("string bridge is unavailable");
  }

  started_ = true;
  services.metrics().SetCounter("worker_count",
                                services.scheduler().worker_count());
  return rocksdb::Status::OK();
}

void BitmapModule::OnStop(ModuleServices& /*services*/) {
  started_ = false;
  string_bridge_ = nullptr;
  services_ = nullptr;
}

rocksdb::Status BitmapModule::GetBit(const std::string& key, uint64_t offset,
                                     int* bit) {
  if (bit == nullptr) {
    return rocksdb::Status::InvalidArgument("bitmap bit output is required");
  }
  *bit = 0;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::string value;
  bool found = false;
  rocksdb::Status status = string_bridge_->GetValue(key, &value, &found);
  if (!status.ok()) {
    return status;
  }
  if (!found) {
    return rocksdb::Status::OK();
  }

  size_t byte_index = 0;
  status = GetByteIndexForOffset(offset, &byte_index);
  if (!status.ok()) {
    return status;
  }

  *bit = ReadBitAtByteIndex(value, byte_index, offset);
  return rocksdb::Status::OK();
}

rocksdb::Status BitmapModule::SetBit(const std::string& key, uint64_t offset,
                                     int bit, int* old_bit) {
  if (old_bit == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "bitmap old bit output is required");
  }
  if (bit != 0 && bit != 1) {
    return rocksdb::Status::InvalidArgument("SETBIT bit must be 0 or 1");
  }
  *old_bit = 0;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::string value;
  bool found = false;
  rocksdb::Status status = string_bridge_->GetValue(key, &value, &found);
  if (!status.ok()) {
    return status;
  }

  size_t byte_index = 0;
  status = GetByteIndexForOffset(offset, &byte_index);
  if (!status.ok()) {
    return status;
  }

  *old_bit = ReadBitAtByteIndex(value, byte_index, offset);

  size_t required_bytes = 0;
  status = GetRequiredByteCount(offset, value.max_size(), &required_bytes);
  if (!status.ok()) {
    return status;
  }
  if (required_bytes > value.size()) {
    try {
      value.resize(required_bytes, '\0');
    } catch (const std::bad_alloc&) {
      return rocksdb::Status::InvalidArgument("bitmap offset is too large");
    } catch (const std::length_error&) {
      return rocksdb::Status::InvalidArgument("bitmap offset is too large");
    }
  }

  unsigned char byte = static_cast<unsigned char>(value[byte_index]);
  const unsigned char mask = BitMaskForOffset(offset);
  if (bit == 1) {
    byte = static_cast<unsigned char>(byte | mask);
  } else {
    byte = static_cast<unsigned char>(byte & static_cast<unsigned char>(~mask));
  }
  value[byte_index] = static_cast<char>(byte);
  return string_bridge_->SetValue(key, value);
}

rocksdb::Status BitmapModule::CountBits(const std::string& key,
                                        uint64_t* count) {
  if (count == nullptr) {
    return rocksdb::Status::InvalidArgument("bitmap count output is required");
  }
  *count = 0;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::string value;
  bool found = false;
  rocksdb::Status status = string_bridge_->GetValue(key, &value, &found);
  if (!status.ok()) {
    return status;
  }
  if (!found) {
    return rocksdb::Status::OK();
  }

  *count = CountSetBits(value);
  return rocksdb::Status::OK();
}

rocksdb::Status BitmapModule::EnsureReady() const {
  if (services_ == nullptr || string_bridge_ == nullptr || !started_) {
    return rocksdb::Status::InvalidArgument("bitmap module is unavailable");
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
