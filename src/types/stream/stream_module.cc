#include "types/stream/stream_module.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "core/key_service.h"
#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "storage/encoding/key_codec.h"

namespace minikv {

namespace {

struct StreamId {
  uint64_t ms = 0;
  uint64_t seq = 0;
};

bool operator==(const StreamId& lhs, const StreamId& rhs) {
  return lhs.ms == rhs.ms && lhs.seq == rhs.seq;
}

bool operator<(const StreamId& lhs, const StreamId& rhs) {
  if (lhs.ms != rhs.ms) {
    return lhs.ms < rhs.ms;
  }
  return lhs.seq < rhs.seq;
}

bool operator>(const StreamId& lhs, const StreamId& rhs) { return rhs < lhs; }
bool operator<=(const StreamId& lhs, const StreamId& rhs) {
  return !(rhs < lhs);
}

void AppendUint32(std::string* out, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

void AppendUint64(std::string* out, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

uint32_t DecodeUint32(const char* input) {
  uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value = (value << 8) | static_cast<unsigned char>(input[i]);
  }
  return value;
}

uint64_t DecodeUint64(const char* input) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value = (value << 8) | static_cast<unsigned char>(input[i]);
  }
  return value;
}

bool ParseUint64(const std::string& input, uint64_t* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  errno = 0;
  char* parse_end = nullptr;
  const unsigned long long parsed =
      std::strtoull(input.c_str(), &parse_end, 10);
  if (parse_end == nullptr || *parse_end != '\0' || errno == ERANGE) {
    return false;
  }
  *value = static_cast<uint64_t>(parsed);
  return true;
}

std::string NormalizeKeyword(const std::string& input) {
  std::string normalized = input;
  for (char& ch : normalized) {
    if (ch >= 'a' && ch <= 'z') {
      ch = static_cast<char>(ch - 'a' + 'A');
    }
  }
  return normalized;
}

bool ParseStreamId(const std::string& input, StreamId* id) {
  if (id == nullptr) {
    return false;
  }

  const size_t dash = input.find('-');
  if (dash == std::string::npos || dash == 0 || dash + 1 >= input.size() ||
      input.find('-', dash + 1) != std::string::npos) {
    return false;
  }

  StreamId parsed;
  if (!ParseUint64(input.substr(0, dash), &parsed.ms) ||
      !ParseUint64(input.substr(dash + 1), &parsed.seq)) {
    return false;
  }
  *id = parsed;
  return true;
}

bool ParseRangeId(const std::string& input, StreamId* id) {
  if (input == "-") {
    *id = StreamId{};
    return true;
  }
  if (input == "+") {
    *id = StreamId{std::numeric_limits<uint64_t>::max(),
                   std::numeric_limits<uint64_t>::max()};
    return true;
  }
  return ParseStreamId(input, id);
}

std::string FormatStreamId(const StreamId& id) {
  return std::to_string(id.ms) + "-" + std::to_string(id.seq);
}

std::string EncodeStreamLocalPrefix(const std::string& key, uint64_t version) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  return out;
}

std::string EncodeStreamEntryLocalKey(const std::string& key, uint64_t version,
                                      const StreamId& id) {
  std::string out = EncodeStreamLocalPrefix(key, version);
  AppendUint64(&out, id.ms);
  AppendUint64(&out, id.seq);
  return out;
}

std::string EncodeStreamStateLocalKey(const std::string& key,
                                      uint64_t version) {
  return EncodeStreamLocalPrefix(key, version);
}

bool DecodeStreamEntryId(const rocksdb::Slice& encoded_key,
                         const rocksdb::Slice& prefix, StreamId* id) {
  if (!KeyCodec::StartsWith(encoded_key, prefix) ||
      encoded_key.size() != prefix.size() + sizeof(uint64_t) * 2) {
    return false;
  }
  if (id != nullptr) {
    id->ms = DecodeUint64(encoded_key.data() + prefix.size());
    id->seq = DecodeUint64(encoded_key.data() + prefix.size() + sizeof(uint64_t));
  }
  return true;
}

std::string EncodeStreamStateValue(const StreamId& id) {
  std::string out;
  AppendUint64(&out, id.ms);
  AppendUint64(&out, id.seq);
  return out;
}

bool DecodeStreamStateValue(const rocksdb::Slice& value, StreamId* id) {
  if (id == nullptr || value.size() != sizeof(uint64_t) * 2) {
    return false;
  }
  id->ms = DecodeUint64(value.data());
  id->seq = DecodeUint64(value.data() + sizeof(uint64_t));
  return true;
}

std::string EncodeStreamEntryValue(
    const std::vector<StreamFieldValue>& values) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(values.size()));
  for (const auto& item : values) {
    AppendUint32(&out, static_cast<uint32_t>(item.field.size()));
    out.append(item.field);
    AppendUint32(&out, static_cast<uint32_t>(item.value.size()));
    out.append(item.value);
  }
  return out;
}

bool DecodeStreamEntryValue(const rocksdb::Slice& value,
                            std::vector<StreamFieldValue>* out) {
  if (out == nullptr || value.size() < sizeof(uint32_t)) {
    return false;
  }

  size_t offset = 0;
  const auto require_bytes = [&value, &offset](size_t count) {
    return offset + count <= value.size();
  };

  out->clear();
  const uint32_t count = DecodeUint32(value.data());
  offset += sizeof(uint32_t);
  out->reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    if (!require_bytes(sizeof(uint32_t))) {
      return false;
    }
    const uint32_t field_len = DecodeUint32(value.data() + offset);
    offset += sizeof(uint32_t);
    if (!require_bytes(field_len + sizeof(uint32_t))) {
      return false;
    }
    std::string field(value.data() + offset, field_len);
    offset += field_len;
    const uint32_t value_len = DecodeUint32(value.data() + offset);
    offset += sizeof(uint32_t);
    if (!require_bytes(value_len)) {
      return false;
    }
    std::string entry_value(value.data() + offset, value_len);
    offset += value_len;
    out->push_back(StreamFieldValue{std::move(field), std::move(entry_value)});
  }
  return offset == value.size();
}

rocksdb::Status RequireStreamEncoding(const KeyLookup& lookup) {
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  if (lookup.metadata.type != ObjectType::kStream ||
      lookup.metadata.encoding != ObjectEncoding::kStreamRadixTree) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }
  return rocksdb::Status::OK();
}

KeyMetadata BuildStreamMetadata(const CoreKeyService* key_service,
                                const KeyLookup& lookup) {
  if (lookup.exists) {
    return lookup.metadata;
  }

  KeyMetadata metadata =
      key_service->MakeMetadata(ObjectType::kStream,
                                ObjectEncoding::kStreamRadixTree, lookup);
  metadata.size = 0;
  return metadata;
}

KeyMetadata BuildStreamTombstoneMetadata(const CoreKeyService* key_service,
                                         const KeyLookup& lookup) {
  KeyMetadata metadata = key_service->MakeTombstoneMetadata(lookup);
  metadata.size = 0;
  return metadata;
}

rocksdb::Status LoadStreamState(ModuleSnapshot* snapshot,
                                const ModuleKeyspace& state_keyspace,
                                const std::string& key, uint64_t version,
                                StreamId* id) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (id == nullptr) {
    return rocksdb::Status::InvalidArgument("stream state output is required");
  }

  std::string raw_state;
  rocksdb::Status status =
      snapshot->Get(state_keyspace, EncodeStreamStateLocalKey(key, version),
                    &raw_state);
  if (!status.ok()) {
    return status;
  }
  if (!DecodeStreamStateValue(raw_state, id)) {
    return rocksdb::Status::Corruption("invalid stream state");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status PutStreamState(ModuleWriteBatch* write_batch,
                               const ModuleKeyspace& state_keyspace,
                               const std::string& key, uint64_t version,
                               const StreamId& id) {
  if (write_batch == nullptr) {
    return rocksdb::Status::InvalidArgument("module write batch is unavailable");
  }
  return write_batch->Put(state_keyspace, EncodeStreamStateLocalKey(key, version),
                          EncodeStreamStateValue(id));
}

rocksdb::Status DeleteStreamState(ModuleWriteBatch* write_batch,
                                  const ModuleKeyspace& state_keyspace,
                                  const std::string& key, uint64_t version) {
  if (write_batch == nullptr) {
    return rocksdb::Status::InvalidArgument("module write batch is unavailable");
  }
  return write_batch->Delete(state_keyspace,
                             EncodeStreamStateLocalKey(key, version));
}

rocksdb::Status ResolveLastStreamId(ModuleSnapshot* snapshot,
                                    const ModuleKeyspace& entries_keyspace,
                                    const ModuleKeyspace& state_keyspace,
                                    const std::string& key, uint64_t version,
                                    StreamId* id, bool* found) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (id == nullptr || found == nullptr) {
    return rocksdb::Status::InvalidArgument("stream id output is required");
  }

  *found = false;
  rocksdb::Status status =
      LoadStreamState(snapshot, state_keyspace, key, version, id);
  if (status.ok()) {
    *found = true;
    return status;
  }
  if (!status.IsNotFound()) {
    return status;
  }

  const std::string prefix = EncodeStreamLocalPrefix(key, version);
  std::unique_ptr<ModuleIterator> iter = snapshot->NewIterator(entries_keyspace);
  iter->Seek(prefix);
  StreamId last_id;
  bool saw_entry = false;
  while (iter->Valid()) {
    if (!KeyCodec::StartsWith(iter->key(), prefix)) {
      break;
    }
    StreamId current;
    if (!DecodeStreamEntryId(iter->key(), prefix, &current)) {
      return rocksdb::Status::Corruption("invalid stream entry key");
    }
    last_id = current;
    saw_entry = true;
    iter->Next();
  }
  if (!iter->status().ok()) {
    return iter->status();
  }
  if (!saw_entry) {
    return rocksdb::Status::NotFound("stream is empty");
  }
  *id = last_id;
  *found = true;
  return rocksdb::Status::OK();
}

rocksdb::Status CollectEntryIds(ModuleSnapshot* snapshot,
                                const ModuleKeyspace& entries_keyspace,
                                const std::string& key, uint64_t version,
                                size_t limit, std::vector<StreamId>* out) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("stream id output is required");
  }

  out->clear();
  const std::string prefix = EncodeStreamLocalPrefix(key, version);
  std::unique_ptr<ModuleIterator> iter = snapshot->NewIterator(entries_keyspace);
  iter->Seek(prefix);
  while (iter->Valid()) {
    if (!KeyCodec::StartsWith(iter->key(), prefix)) {
      break;
    }
    StreamId id;
    if (!DecodeStreamEntryId(iter->key(), prefix, &id)) {
      return rocksdb::Status::Corruption("invalid stream entry key");
    }
    out->push_back(id);
    if (limit != 0 && out->size() >= limit) {
      break;
    }
    iter->Next();
  }
  return iter->status();
}

rocksdb::Status CollectEntriesInRange(ModuleSnapshot* snapshot,
                                      const ModuleKeyspace& entries_keyspace,
                                      const std::string& key, uint64_t version,
                                      const StreamId& start,
                                      const StreamId& end,
                                      std::vector<StreamEntry>* out) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("stream range output is required");
  }

  out->clear();
  if (end < start) {
    return rocksdb::Status::OK();
  }

  const std::string prefix = EncodeStreamLocalPrefix(key, version);
  const std::string seek_key =
      (start.ms == 0 && start.seq == 0)
          ? prefix
          : EncodeStreamEntryLocalKey(key, version, start);
  std::unique_ptr<ModuleIterator> iter = snapshot->NewIterator(entries_keyspace);
  iter->Seek(seek_key);
  while (iter->Valid()) {
    if (!KeyCodec::StartsWith(iter->key(), prefix)) {
      break;
    }
    StreamId id;
    if (!DecodeStreamEntryId(iter->key(), prefix, &id)) {
      return rocksdb::Status::Corruption("invalid stream entry key");
    }
    if (id < start) {
      iter->Next();
      continue;
    }
    if (end < id) {
      break;
    }
    std::vector<StreamFieldValue> values;
    if (!DecodeStreamEntryValue(iter->value(), &values)) {
      return rocksdb::Status::Corruption("invalid stream entry value");
    }
    out->push_back(StreamEntry{FormatStreamId(id), std::move(values)});
    iter->Next();
  }
  return iter->status();
}

rocksdb::Status CollectEntriesAfterId(ModuleSnapshot* snapshot,
                                      const ModuleKeyspace& entries_keyspace,
                                      const std::string& key, uint64_t version,
                                      const StreamId& last_seen,
                                      std::vector<StreamEntry>* out) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("stream read output is required");
  }

  out->clear();
  const std::string prefix = EncodeStreamLocalPrefix(key, version);
  std::unique_ptr<ModuleIterator> iter = snapshot->NewIterator(entries_keyspace);
  iter->Seek(EncodeStreamEntryLocalKey(key, version, last_seen));
  while (iter->Valid()) {
    if (!KeyCodec::StartsWith(iter->key(), prefix)) {
      break;
    }
    StreamId id;
    if (!DecodeStreamEntryId(iter->key(), prefix, &id)) {
      return rocksdb::Status::Corruption("invalid stream entry key");
    }
    if (id <= last_seen) {
      iter->Next();
      continue;
    }
    std::vector<StreamFieldValue> values;
    if (!DecodeStreamEntryValue(iter->value(), &values)) {
      return rocksdb::Status::Corruption("invalid stream entry value");
    }
    out->push_back(StreamEntry{FormatStreamId(id), std::move(values)});
    iter->Next();
  }
  return iter->status();
}

std::vector<StreamId> DeduplicateIds(const std::vector<StreamId>& ids) {
  std::vector<StreamId> unique_ids = ids;
  std::sort(unique_ids.begin(), unique_ids.end(),
            [](const StreamId& lhs, const StreamId& rhs) { return lhs < rhs; });
  unique_ids.erase(std::unique(unique_ids.begin(), unique_ids.end()),
                   unique_ids.end());
  return unique_ids;
}

ReplyNode MakeStreamEntryReply(const StreamEntry& entry) {
  std::vector<std::string> flattened;
  flattened.reserve(entry.values.size() * 2);
  for (const auto& item : entry.values) {
    flattened.push_back(item.field);
    flattened.push_back(item.value);
  }
  std::vector<ReplyNode> nodes;
  nodes.reserve(2);
  nodes.push_back(ReplyNode::BulkString(entry.id));
  nodes.push_back(ReplyNode::BulkStringArray(std::move(flattened)));
  return ReplyNode::Array(std::move(nodes));
}

ReplyNode MakeStreamReadResultReply(const StreamReadResult& result) {
  std::vector<ReplyNode> entries;
  entries.reserve(result.entries.size());
  for (const auto& entry : result.entries) {
    entries.push_back(MakeStreamEntryReply(entry));
  }
  std::vector<ReplyNode> nodes;
  nodes.reserve(2);
  nodes.push_back(ReplyNode::BulkString(result.key));
  nodes.push_back(ReplyNode::Array(std::move(entries)));
  return ReplyNode::Array(std::move(nodes));
}

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

rocksdb::Status StreamModule::OnLoad(ModuleServices& services) {
  services_ = &services;

  rocksdb::Status status = services.command_registry().Register(
      {"XADD", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<XAddCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"XTRIM", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<XTrimCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"XDEL", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<XDelCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"XLEN", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<XLenCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"XRANGE", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<XRangeCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"XREVRANGE", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<XRevRangeCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"XREAD", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<XReadCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

rocksdb::Status StreamModule::OnStart(ModuleServices& services) {
  key_service_ = services.exports().Find<CoreKeyService>(
      kCoreKeyServiceQualifiedExportName);
  if (key_service_ == nullptr) {
    return rocksdb::Status::InvalidArgument("core key service is unavailable");
  }
  delete_registry_ = services.exports().Find<WholeKeyDeleteRegistry>(
      kWholeKeyDeleteRegistryQualifiedExportName);
  if (delete_registry_ == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "whole-key delete registry is unavailable");
  }
  rocksdb::Status status = delete_registry_->RegisterHandler(this);
  if (!status.ok()) {
    return status;
  }

  started_ = true;
  return rocksdb::Status::OK();
}

void StreamModule::OnStop(ModuleServices& /*services*/) {
  started_ = false;
  delete_registry_ = nullptr;
  key_service_ = nullptr;
  services_ = nullptr;
}

rocksdb::Status StreamModule::AddEntry(
    const std::string& key, const std::string& id_spec,
    const std::vector<StreamFieldValue>& values, std::string* added_id) {
  if (added_id != nullptr) {
    added_id->clear();
  }
  if (values.empty()) {
    return rocksdb::Status::InvalidArgument("stream entry values are required");
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  status = RequireStreamEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  const ModuleKeyspace state_keyspace = services_->storage().Keyspace("state");
  KeyMetadata after = BuildStreamMetadata(key_service_, lookup);
  StreamId last_id;
  bool last_found = false;
  status = ResolveLastStreamId(snapshot.get(), entries_keyspace, state_keyspace,
                               key, after.version, &last_id, &last_found);
  if (!status.ok() && !status.IsNotFound()) {
    return status;
  }

  StreamId new_id;
  if (id_spec == "*") {
    const uint64_t now_ms = key_service_->CurrentTimeMs();
    if (!last_found || now_ms > last_id.ms) {
      new_id = StreamId{now_ms, 0};
    } else {
      if (last_id.seq == std::numeric_limits<uint64_t>::max()) {
        return rocksdb::Status::InvalidArgument("stream sequence overflow");
      }
      new_id = StreamId{last_id.ms, last_id.seq + 1};
    }
  } else {
    if (!ParseStreamId(id_spec, &new_id)) {
      return rocksdb::Status::InvalidArgument("XADD requires valid id");
    }
    if (new_id.ms == 0 && new_id.seq == 0) {
      return rocksdb::Status::InvalidArgument(
          "XADD ID must be greater than 0-0");
    }
    if (last_found && !(last_id < new_id)) {
      return rocksdb::Status::InvalidArgument(
          "XADD ID must be greater than current stream top item");
    }
  }

  after.size += 1;
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }
  status = write_batch->Put(entries_keyspace,
                            EncodeStreamEntryLocalKey(key, after.version, new_id),
                            EncodeStreamEntryValue(values));
  if (!status.ok()) {
    return status;
  }
  status = PutStreamState(write_batch.get(), state_keyspace, key, after.version,
                          new_id);
  if (!status.ok()) {
    return status;
  }
  status = write_batch->Commit();
  if (status.ok() && added_id != nullptr) {
    *added_id = FormatStreamId(new_id);
  }
  return status;
}

rocksdb::Status StreamModule::TrimByMaxLen(const std::string& key,
                                           uint64_t max_len,
                                           uint64_t* removed_count) {
  if (removed_count != nullptr) {
    *removed_count = 0;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireStreamEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (lookup.metadata.size <= max_len) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  const ModuleKeyspace state_keyspace = services_->storage().Keyspace("state");
  const size_t trim_count =
      static_cast<size_t>(lookup.metadata.size - max_len);
  std::vector<StreamId> ids;
  status = CollectEntryIds(snapshot.get(), entries_keyspace, key,
                           lookup.metadata.version, trim_count, &ids);
  if (!status.ok()) {
    return status;
  }
  if (ids.empty()) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  for (const auto& id : ids) {
    status = write_batch->Delete(entries_keyspace,
                                 EncodeStreamEntryLocalKey(key,
                                                           lookup.metadata.version,
                                                           id));
    if (!status.ok()) {
      return status;
    }
  }

  if (ids.size() >= lookup.metadata.size) {
    const KeyMetadata tombstone =
        BuildStreamTombstoneMetadata(key_service_, lookup);
    status = key_service_->PutMetadata(write_batch.get(), key, tombstone);
    if (!status.ok()) {
      return status;
    }
    status = DeleteStreamState(write_batch.get(), state_keyspace, key,
                               lookup.metadata.version);
    if (!status.ok()) {
      return status;
    }
  } else {
    KeyMetadata after = lookup.metadata;
    after.size -= ids.size();
    status = key_service_->PutMetadata(write_batch.get(), key, after);
    if (!status.ok()) {
      return status;
    }
  }

  status = write_batch->Commit();
  if (status.ok() && removed_count != nullptr) {
    *removed_count = ids.size();
  }
  return status;
}

rocksdb::Status StreamModule::DeleteEntries(const std::string& key,
                                            const std::vector<std::string>& ids,
                                            uint64_t* removed_count) {
  if (removed_count != nullptr) {
    *removed_count = 0;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::vector<StreamId> parsed_ids;
  parsed_ids.reserve(ids.size());
  for (const auto& id : ids) {
    StreamId parsed;
    if (!ParseStreamId(id, &parsed)) {
      return rocksdb::Status::InvalidArgument("XDEL requires valid id");
    }
    parsed_ids.push_back(parsed);
  }
  const std::vector<StreamId> unique_ids = DeduplicateIds(parsed_ids);

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireStreamEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  const ModuleKeyspace state_keyspace = services_->storage().Keyspace("state");
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  uint64_t removed = 0;
  std::string scratch;
  for (const auto& id : unique_ids) {
    status = snapshot->Get(entries_keyspace,
                           EncodeStreamEntryLocalKey(key, lookup.metadata.version,
                                                     id),
                           &scratch);
    if (status.ok()) {
      status = write_batch->Delete(
          entries_keyspace,
          EncodeStreamEntryLocalKey(key, lookup.metadata.version, id));
      if (!status.ok()) {
        return status;
      }
      ++removed;
    } else if (!status.IsNotFound()) {
      return status;
    }
  }
  if (removed == 0) {
    return rocksdb::Status::OK();
  }

  if (removed >= lookup.metadata.size) {
    const KeyMetadata tombstone =
        BuildStreamTombstoneMetadata(key_service_, lookup);
    status = key_service_->PutMetadata(write_batch.get(), key, tombstone);
    if (!status.ok()) {
      return status;
    }
    status = DeleteStreamState(write_batch.get(), state_keyspace, key,
                               lookup.metadata.version);
    if (!status.ok()) {
      return status;
    }
  } else {
    KeyMetadata after = lookup.metadata;
    after.size -= removed;
    status = key_service_->PutMetadata(write_batch.get(), key, after);
    if (!status.ok()) {
      return status;
    }
  }

  status = write_batch->Commit();
  if (status.ok() && removed_count != nullptr) {
    *removed_count = removed;
  }
  return status;
}

rocksdb::Status StreamModule::Length(const std::string& key, uint64_t* length) {
  if (length == nullptr) {
    return rocksdb::Status::InvalidArgument("stream length output is required");
  }
  *length = 0;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireStreamEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  *length = lookup.metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status StreamModule::Range(const std::string& key,
                                    const std::string& start,
                                    const std::string& end,
                                    std::vector<StreamEntry>* out) {
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("stream range output is required");
  }
  out->clear();

  StreamId start_id;
  if (!ParseRangeId(start, &start_id)) {
    return rocksdb::Status::InvalidArgument("XRANGE requires valid start id");
  }
  StreamId end_id;
  if (!ParseRangeId(end, &end_id)) {
    return rocksdb::Status::InvalidArgument("XRANGE requires valid end id");
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireStreamEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  return CollectEntriesInRange(snapshot.get(), entries_keyspace, key,
                               lookup.metadata.version, start_id, end_id, out);
}

rocksdb::Status StreamModule::ReverseRange(const std::string& key,
                                           const std::string& end,
                                           const std::string& start,
                                           std::vector<StreamEntry>* out) {
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("stream range output is required");
  }
  out->clear();

  StreamId end_id;
  if (!ParseRangeId(end, &end_id)) {
    return rocksdb::Status::InvalidArgument(
        "XREVRANGE requires valid end id");
  }
  StreamId start_id;
  if (!ParseRangeId(start, &start_id)) {
    return rocksdb::Status::InvalidArgument(
        "XREVRANGE requires valid start id");
  }

  std::vector<StreamEntry> entries;
  rocksdb::Status status = Range(key, start, end, &entries);
  if (!status.ok()) {
    return status;
  }
  (void)end_id;
  (void)start_id;
  std::reverse(entries.begin(), entries.end());
  *out = std::move(entries);
  return rocksdb::Status::OK();
}

rocksdb::Status StreamModule::Read(const std::vector<StreamReadSpec>& requests,
                                   std::vector<StreamReadResult>* out) {
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("stream read output is required");
  }
  out->clear();

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  for (const auto& request : requests) {
    StreamId last_seen;
    if (!ParseStreamId(request.last_seen_id, &last_seen)) {
      return rocksdb::Status::InvalidArgument("XREAD requires valid id");
    }

    KeyLookup lookup;
    rocksdb::Status status =
        key_service_->Lookup(snapshot.get(), request.key, &lookup);
    if (!status.ok()) {
      return status;
    }
    if (!lookup.exists) {
      continue;
    }
    status = RequireStreamEncoding(lookup);
    if (!status.ok()) {
      return status;
    }

    std::vector<StreamEntry> entries;
    status = CollectEntriesAfterId(snapshot.get(), entries_keyspace, request.key,
                                   lookup.metadata.version, last_seen, &entries);
    if (!status.ok()) {
      return status;
    }
    if (!entries.empty()) {
      out->push_back(StreamReadResult{request.key, std::move(entries)});
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status StreamModule::DeleteWholeKey(ModuleSnapshot* snapshot,
                                             ModuleWriteBatch* write_batch,
                                             const std::string& key,
                                             const KeyLookup& lookup) {
  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (write_batch == nullptr) {
    return rocksdb::Status::InvalidArgument("module write batch is unavailable");
  }
  rocksdb::Status status = RequireStreamEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  const ModuleKeyspace state_keyspace = services_->storage().Keyspace("state");
  std::vector<StreamId> ids;
  status = CollectEntryIds(snapshot, entries_keyspace, key,
                           lookup.metadata.version, 0, &ids);
  if (!status.ok()) {
    return status;
  }
  for (const auto& id : ids) {
    status = write_batch->Delete(entries_keyspace,
                                 EncodeStreamEntryLocalKey(key,
                                                           lookup.metadata.version,
                                                           id));
    if (!status.ok()) {
      return status;
    }
  }
  status = DeleteStreamState(write_batch, state_keyspace, key,
                             lookup.metadata.version);
  if (!status.ok()) {
    return status;
  }

  const KeyMetadata after = BuildStreamTombstoneMetadata(key_service_, lookup);
  return key_service_->PutMetadata(write_batch, key, after);
}

rocksdb::Status StreamModule::EnsureReady() const {
  if (services_ == nullptr || key_service_ == nullptr || !started_) {
    return rocksdb::Status::InvalidArgument("stream module is unavailable");
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
