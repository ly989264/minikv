#include "kernel/reply.h"

#include <utility>

namespace minikv {

ReplyNode::ReplyNode() = default;

ReplyNode::ReplyNode(Type type) : type_(type) {}

ReplyNode ReplyNode::SimpleString(std::string value) {
  ReplyNode reply(Type::kSimpleString);
  reply.string_ = std::move(value);
  return reply;
}

ReplyNode ReplyNode::Error(std::string value) {
  ReplyNode reply(Type::kError);
  reply.string_ = std::move(value);
  return reply;
}

ReplyNode ReplyNode::Integer(long long value) {
  ReplyNode reply(Type::kInteger);
  reply.integer_ = value;
  return reply;
}

ReplyNode ReplyNode::BulkString(std::string value) {
  ReplyNode reply(Type::kBulkString);
  reply.string_ = std::move(value);
  return reply;
}

ReplyNode ReplyNode::Array(std::vector<ReplyNode> values) {
  ReplyNode reply(Type::kArray);
  reply.array_ = std::move(values);
  return reply;
}

ReplyNode ReplyNode::Map(std::vector<MapEntry> entries) {
  ReplyNode reply(Type::kMap);
  reply.map_ = std::move(entries);
  return reply;
}

ReplyNode ReplyNode::Null() { return ReplyNode(Type::kNull); }

ReplyNode ReplyNode::BulkStringArray(std::vector<std::string> values) {
  std::vector<ReplyNode> nodes;
  nodes.reserve(values.size());
  for (auto& value : values) {
    nodes.push_back(BulkString(std::move(value)));
  }
  return Array(std::move(nodes));
}

}  // namespace minikv
