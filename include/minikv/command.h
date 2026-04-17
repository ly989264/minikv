#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "minikv/reply.h"
#include "rocksdb/status.h"

namespace minikv {

enum class CommandType {
  kPing,
  kHSet,
  kHGetAll,
  kHDel,
};

inline const char* CommandTypeName(CommandType type) {
  switch (type) {
    case CommandType::kPing:
      return "PING";
    case CommandType::kHSet:
      return "HSET";
    case CommandType::kHGetAll:
      return "HGETALL";
    case CommandType::kHDel:
      return "HDEL";
  }
  return "";
}

struct FieldValue {
  std::string field;
  std::string value;
};

struct CommandRequest {
  CommandRequest() = default;

  CommandRequest(CommandType type_value, std::string key_value,
                 std::vector<std::string> args_value)
      : name(CommandTypeName(type_value)),
        has_key(type_value == CommandType::kPing ? !key_value.empty() : true),
        key(std::move(key_value)),
        args(std::move(args_value)) {}

  CommandRequest(std::string name_value, std::string key_value,
                 std::vector<std::string> args_value,
                 bool has_key_value = true)
      : name(std::move(name_value)),
        has_key(has_key_value),
        key(std::move(key_value)),
        args(std::move(args_value)) {}

  CommandRequest(std::string name_value, std::vector<std::string> args_value)
      : name(std::move(name_value)),
        has_key(false),
        args(std::move(args_value)) {}

  std::string name;
  bool has_key = false;
  std::string key;
  std::vector<std::string> args;
};

struct CommandResponse {
  rocksdb::Status status;
  ReplyNode reply;
};

}  // namespace minikv
