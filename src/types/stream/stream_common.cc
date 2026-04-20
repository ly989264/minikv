#include "types/stream/stream_common.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <memory>
#include <utility>

#include "core/key_service.h"
#include "storage/encoding/key_codec.h"

namespace minikv::stream_internal {
namespace {

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

std::string EncodeStreamLocalPrefix(const std::string& key, uint64_t version) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  return out;
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

}  // namespace

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

std::string EncodeStreamEntryLocalKey(const std::string& key, uint64_t version,
                                      const StreamId& id) {
  std::string out = EncodeStreamLocalPrefix(key, version);
  AppendUint64(&out, id.ms);
  AppendUint64(&out, id.seq);
  return out;
}

std::string EncodeStreamStateLocalKey(const std::string& key, uint64_t version) {
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

std::string EncodeStreamEntryValue(const std::vector<StreamFieldValue>& values) {
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

}  // namespace minikv::stream_internal
