#include "command/cmd.h"

namespace minikv {

Cmd::Cmd(std::string name, CmdFlags flags)
    : name_(std::move(name)), flags_(flags) {}

rocksdb::Status Cmd::Init(const CmdInput& input) {
  initialized_ = false;
  route_key_.clear();

  rocksdb::Status status = DoInitial(input);
  if (status.ok()) {
    initialized_ = true;
  }
  return status;
}

CommandResponse Cmd::Execute() {
  if (!initialized_) {
    return MakeStatus(rocksdb::Status::InvalidArgument(
        "command must be initialized before execution"));
  }
  return Do();
}

CommandResponse Cmd::MakeStatus(rocksdb::Status status) {
  CommandResponse response;
  response.status = std::move(status);
  return response;
}

CommandResponse Cmd::MakeSimpleString(std::string text) {
  CommandResponse response;
  response.status = rocksdb::Status::OK();
  response.reply = ReplyNode::SimpleString(std::move(text));
  return response;
}

CommandResponse Cmd::MakeError(std::string text) {
  CommandResponse response;
  response.status = rocksdb::Status::OK();
  response.reply = ReplyNode::Error(std::move(text));
  return response;
}

CommandResponse Cmd::MakeInteger(long long value) {
  CommandResponse response;
  response.status = rocksdb::Status::OK();
  response.reply = ReplyNode::Integer(value);
  return response;
}

CommandResponse Cmd::MakeBulkString(std::string value) {
  CommandResponse response;
  response.status = rocksdb::Status::OK();
  response.reply = ReplyNode::BulkString(std::move(value));
  return response;
}

CommandResponse Cmd::MakeNull() {
  CommandResponse response;
  response.status = rocksdb::Status::OK();
  response.reply = ReplyNode::Null();
  return response;
}

CommandResponse Cmd::MakeArray(std::vector<std::string> values) {
  CommandResponse response;
  response.status = rocksdb::Status::OK();
  response.reply = ReplyNode::BulkStringArray(std::move(values));
  return response;
}

CommandResponse Cmd::MakeArray(std::vector<ReplyNode> values) {
  CommandResponse response;
  response.status = rocksdb::Status::OK();
  response.reply = ReplyNode::Array(std::move(values));
  return response;
}

CommandResponse Cmd::MakeMap(std::vector<ReplyNode::MapEntry> entries) {
  CommandResponse response;
  response.status = rocksdb::Status::OK();
  response.reply = ReplyNode::Map(std::move(entries));
  return response;
}

}  // namespace minikv
