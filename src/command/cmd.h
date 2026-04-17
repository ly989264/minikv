#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "minikv/command.h"
#include "rocksdb/status.h"

namespace minikv {

class HashModule;
class StorageEngine;

struct CommandContext {
  StorageEngine* storage_engine = nullptr;
  HashModule* hash_module = nullptr;
};

enum class CmdFlags : uint32_t {
  kNone = 0,
  kRead = 1u << 0,
  kWrite = 1u << 1,
  kFast = 1u << 2,
  kSlow = 1u << 3,
};

inline CmdFlags operator|(CmdFlags lhs, CmdFlags rhs) {
  return static_cast<CmdFlags>(static_cast<uint32_t>(lhs) |
                               static_cast<uint32_t>(rhs));
}

inline CmdFlags operator&(CmdFlags lhs, CmdFlags rhs) {
  return static_cast<CmdFlags>(static_cast<uint32_t>(lhs) &
                               static_cast<uint32_t>(rhs));
}

inline CmdFlags& operator|=(CmdFlags& lhs, CmdFlags rhs) {
  lhs = lhs | rhs;
  return lhs;
}

inline bool HasFlag(CmdFlags flags, CmdFlags flag) {
  return static_cast<uint32_t>(flags & flag) != 0;
}

struct CmdInput {
  bool has_key = false;
  std::string key;
  std::vector<std::string> args;
};

class Cmd {
 public:
  virtual ~Cmd() = default;

  rocksdb::Status Init(const CmdInput& input);
  CommandResponse Execute(CommandContext* context);

  const std::string& Name() const { return name_; }
  CmdFlags Flags() const { return flags_; }
  const std::string& RouteKey() const { return route_key_; }

 protected:
  Cmd(std::string name, CmdFlags flags);

  void SetRouteKey(std::string key) { route_key_ = std::move(key); }

  static CommandResponse MakeStatus(rocksdb::Status status);
  static CommandResponse MakeSimpleString(std::string text);
  static CommandResponse MakeError(std::string text);
  static CommandResponse MakeInteger(long long value);
  static CommandResponse MakeBulkString(std::string value);
  static CommandResponse MakeNull();
  static CommandResponse MakeArray(std::vector<std::string> values);
  static CommandResponse MakeArray(std::vector<ReplyNode> values);
  static CommandResponse MakeMap(std::vector<ReplyNode::MapEntry> entries);

 private:
  virtual rocksdb::Status DoInitial(const CmdInput& input) = 0;
  virtual CommandResponse Do(CommandContext* context) = 0;

  std::string name_;
  CmdFlags flags_;
  std::string route_key_;
  bool initialized_ = false;
};

}  // namespace minikv
