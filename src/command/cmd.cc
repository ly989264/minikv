#include "command/cmd.h"

#include <algorithm>

namespace minikv {

void Cmd::LockPlan::Clear() {
  kind_ = Kind::kNone;
  single_key_.clear();
  multi_keys_.clear();
}

void Cmd::LockPlan::SetSingle(std::string key) {
  kind_ = Kind::kSingle;
  single_key_ = std::move(key);
  multi_keys_.clear();
}

void Cmd::LockPlan::SetCanonicalized(std::vector<std::string> keys) {
  if (keys.empty()) {
    Clear();
    return;
  }

  if (keys.size() == 1) {
    SetSingle(std::move(keys.front()));
    return;
  }

  std::sort(keys.begin(), keys.end());
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  if (keys.empty()) {
    Clear();
    return;
  }
  if (keys.size() == 1) {
    SetSingle(std::move(keys.front()));
    return;
  }

  kind_ = Kind::kMulti;
  single_key_.clear();
  multi_keys_ = std::move(keys);
}

Cmd::Cmd(std::string name, CmdFlags flags)
    : name_(std::move(name)), flags_(flags) {}

rocksdb::Status Cmd::Init(const CmdInput& input) {
  initialized_ = false;
  lock_plan_.Clear();

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
