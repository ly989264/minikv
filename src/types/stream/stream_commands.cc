#include "types/stream/stream_commands.h"

#include <memory>
#include <utility>
#include <vector>

#include "execution/command/cmd.h"
#include "types/stream/stream_common.h"
#include "types/stream/stream_module.h"

namespace minikv {
namespace {

using stream_internal::MakeStreamEntryReply;
using stream_internal::MakeStreamReadResultReply;
using stream_internal::NormalizeKeyword;
using stream_internal::ParseRangeId;
using stream_internal::ParseStreamId;
using stream_internal::ParseUint64;
using stream_internal::StreamId;

class XAddCmd : public Cmd {
 public:
  XAddCmd(const CmdRegistration& registration, StreamModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() < 3 || input.args.size() % 2 == 0) {
      return rocksdb::Status::InvalidArgument(
          "XADD requires id and field/value pairs");
    }
    key_ = input.key;
    id_spec_ = input.args[0];
    if (id_spec_ != "*") {
      StreamId parsed;
      if (!ParseStreamId(id_spec_, &parsed)) {
        return rocksdb::Status::InvalidArgument("XADD requires valid id");
      }
      if (parsed.ms == 0 && parsed.seq == 0) {
        return rocksdb::Status::InvalidArgument(
            "XADD ID must be greater than 0-0");
      }
    }
    values_.clear();
    values_.reserve((input.args.size() - 1) / 2);
    for (size_t index = 1; index < input.args.size(); index += 2) {
      values_.push_back(StreamFieldValue{input.args[index], input.args[index + 1]});
    }
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("stream module is unavailable"));
    }
    std::string added_id;
    rocksdb::Status status = module_->AddEntry(key_, id_spec_, values_, &added_id);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeBulkString(std::move(added_id));
  }

  StreamModule* module_ = nullptr;
  std::string key_;
  std::string id_spec_;
  std::vector<StreamFieldValue> values_;
};

class XTrimCmd : public Cmd {
 public:
  XTrimCmd(const CmdRegistration& registration, StreamModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument(
          "XTRIM requires MAXLEN and threshold");
    }
    if (NormalizeKeyword(input.args[0]) != "MAXLEN") {
      return rocksdb::Status::InvalidArgument(
          "XTRIM requires MAXLEN and threshold");
    }
    if (!ParseUint64(input.args[1], &max_len_)) {
      return rocksdb::Status::InvalidArgument(
          "XTRIM requires integer threshold");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("stream module is unavailable"));
    }
    uint64_t removed = 0;
    rocksdb::Status status = module_->TrimByMaxLen(key_, max_len_, &removed);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(removed));
  }

  StreamModule* module_ = nullptr;
  std::string key_;
  uint64_t max_len_ = 0;
};

class XDelCmd : public Cmd {
 public:
  XDelCmd(const CmdRegistration& registration, StreamModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument("XDEL requires at least one id");
    }
    for (const auto& id : input.args) {
      StreamId parsed;
      if (!ParseStreamId(id, &parsed)) {
        return rocksdb::Status::InvalidArgument("XDEL requires valid id");
      }
    }
    key_ = input.key;
    ids_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("stream module is unavailable"));
    }
    uint64_t removed = 0;
    rocksdb::Status status = module_->DeleteEntries(key_, ids_, &removed);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(removed));
  }

  StreamModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> ids_;
};

class XLenCmd : public Cmd {
 public:
  XLenCmd(const CmdRegistration& registration, StreamModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("XLEN takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("stream module is unavailable"));
    }
    uint64_t length = 0;
    rocksdb::Status status = module_->Length(key_, &length);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(length));
  }

  StreamModule* module_ = nullptr;
  std::string key_;
};

class XRangeCmd : public Cmd {
 public:
  XRangeCmd(const CmdRegistration& registration, StreamModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument("XRANGE requires start and end");
    }
    StreamId start;
    if (!ParseRangeId(input.args[0], &start)) {
      return rocksdb::Status::InvalidArgument("XRANGE requires valid start id");
    }
    StreamId end;
    if (!ParseRangeId(input.args[1], &end)) {
      return rocksdb::Status::InvalidArgument("XRANGE requires valid end id");
    }
    key_ = input.key;
    start_ = input.args[0];
    end_ = input.args[1];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("stream module is unavailable"));
    }
    std::vector<StreamEntry> entries;
    rocksdb::Status status = module_->Range(key_, start_, end_, &entries);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    std::vector<ReplyNode> nodes;
    nodes.reserve(entries.size());
    for (const auto& entry : entries) {
      nodes.push_back(MakeStreamEntryReply(entry));
    }
    return MakeArray(std::move(nodes));
  }

  StreamModule* module_ = nullptr;
  std::string key_;
  std::string start_;
  std::string end_;
};

class XRevRangeCmd : public Cmd {
 public:
  XRevRangeCmd(const CmdRegistration& registration, StreamModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument(
          "XREVRANGE requires end and start");
    }
    StreamId end;
    if (!ParseRangeId(input.args[0], &end)) {
      return rocksdb::Status::InvalidArgument(
          "XREVRANGE requires valid end id");
    }
    StreamId start;
    if (!ParseRangeId(input.args[1], &start)) {
      return rocksdb::Status::InvalidArgument(
          "XREVRANGE requires valid start id");
    }
    key_ = input.key;
    end_ = input.args[0];
    start_ = input.args[1];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("stream module is unavailable"));
    }
    std::vector<StreamEntry> entries;
    rocksdb::Status status = module_->ReverseRange(key_, end_, start_, &entries);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    std::vector<ReplyNode> nodes;
    nodes.reserve(entries.size());
    for (const auto& entry : entries) {
      nodes.push_back(MakeStreamEntryReply(entry));
    }
    return MakeArray(std::move(nodes));
  }

  StreamModule* module_ = nullptr;
  std::string key_;
  std::string end_;
  std::string start_;
};

class XReadCmd : public Cmd {
 public:
  XReadCmd(const CmdRegistration& registration, StreamModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key || NormalizeKeyword(input.key) != "STREAMS") {
      return rocksdb::Status::InvalidArgument("XREAD requires STREAMS keyword");
    }
    if (input.args.size() < 2 || input.args.size() % 2 != 0) {
      return rocksdb::Status::InvalidArgument(
          "XREAD requires matching stream keys and ids");
    }

    const size_t stream_count = input.args.size() / 2;
    requests_.clear();
    requests_.reserve(stream_count);
    std::vector<std::string> route_keys;
    route_keys.reserve(stream_count);
    for (size_t index = 0; index < stream_count; ++index) {
      StreamId parsed;
      if (!ParseStreamId(input.args[stream_count + index], &parsed)) {
        return rocksdb::Status::InvalidArgument("XREAD requires valid id");
      }
      requests_.push_back(
          StreamReadSpec{input.args[index], input.args[stream_count + index]});
      route_keys.push_back(input.args[index]);
    }
    SetRouteKeys(std::move(route_keys));
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("stream module is unavailable"));
    }
    std::vector<StreamReadResult> results;
    rocksdb::Status status = module_->Read(requests_, &results);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (results.empty()) {
      return MakeNull();
    }
    std::vector<ReplyNode> nodes;
    nodes.reserve(results.size());
    for (const auto& result : results) {
      nodes.push_back(MakeStreamReadResultReply(result));
    }
    return MakeArray(std::move(nodes));
  }

  StreamModule* module_ = nullptr;
  std::vector<StreamReadSpec> requests_;
};

}  // namespace

rocksdb::Status RegisterStreamCommands(ModuleServices& services,
                                       StreamModule* module) {
  rocksdb::Status status = services.command_registry().Register(
      {"XADD", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<XAddCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"XTRIM", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<XTrimCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"XDEL", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<XDelCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"XLEN", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<XLenCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"XRANGE", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<XRangeCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"XREVRANGE", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<XRevRangeCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"XREAD", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<XReadCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

}  // namespace minikv
