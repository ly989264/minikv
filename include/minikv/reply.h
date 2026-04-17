#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace minikv {

class ReplyNode {
 public:
  enum class Type : uint8_t {
    kSimpleString,
    kError,
    kInteger,
    kBulkString,
    kArray,
    kMap,
    kNull,
  };

  using MapEntry = std::pair<ReplyNode, ReplyNode>;

  ReplyNode();

  static ReplyNode SimpleString(std::string value);
  static ReplyNode Error(std::string value);
  static ReplyNode Integer(long long value);
  static ReplyNode BulkString(std::string value);
  static ReplyNode Array(std::vector<ReplyNode> values);
  static ReplyNode Map(std::vector<MapEntry> entries);
  static ReplyNode Null();
  static ReplyNode BulkStringArray(std::vector<std::string> values);

  Type type() const { return type_; }
  const std::string& string() const { return string_; }
  long long integer() const { return integer_; }
  const std::vector<ReplyNode>& array() const { return array_; }
  const std::vector<MapEntry>& map() const { return map_; }

  bool IsSimpleString() const { return type_ == Type::kSimpleString; }
  bool IsError() const { return type_ == Type::kError; }
  bool IsInteger() const { return type_ == Type::kInteger; }
  bool IsBulkString() const { return type_ == Type::kBulkString; }
  bool IsArray() const { return type_ == Type::kArray; }
  bool IsMap() const { return type_ == Type::kMap; }
  bool IsNull() const { return type_ == Type::kNull; }

 private:
  explicit ReplyNode(Type type);

  Type type_ = Type::kNull;
  std::string string_;
  long long integer_ = 0;
  std::vector<ReplyNode> array_;
  std::vector<MapEntry> map_;
};

}  // namespace minikv
