#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "command/command_types.h"
#include "rocksdb/status.h"

namespace minikv {

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
  class LockPlan {
   public:
    enum class Kind {
      kNone,
      kSingle,
      kMulti,
    };

    Kind kind() const { return kind_; }
    const std::string& single_key() const { return single_key_; }
    const std::vector<std::string>& multi_keys() const { return multi_keys_; }

   private:
    void Clear();
    void SetSingle(std::string key);
    void SetCanonicalized(std::vector<std::string> keys);

    Kind kind_ = Kind::kNone;
    std::string single_key_;
    std::vector<std::string> multi_keys_;

    friend class Cmd;
  };

  virtual ~Cmd() = default;

  rocksdb::Status Init(const CmdInput& input);
  CommandResponse Execute();

  const std::string& Name() const { return name_; }
  CmdFlags Flags() const { return flags_; }
  const std::string& RouteKey() const { return lock_plan_.single_key(); }
  const LockPlan& lock_plan() const { return lock_plan_; }

 protected:
  Cmd(std::string name, CmdFlags flags);

  void SetRouteKey(std::string key) { lock_plan_.SetSingle(std::move(key)); }
  void SetRouteKeys(std::vector<std::string> keys) {
    lock_plan_.SetCanonicalized(std::move(keys));
  }

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
  virtual CommandResponse Do() = 0;

  std::string name_;
  CmdFlags flags_;
  LockPlan lock_plan_;
  bool initialized_ = false;
};

}  // namespace minikv
