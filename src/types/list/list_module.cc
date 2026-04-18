#include "types/list/list_module.h"

#include <cerrno>
#include <cstdlib>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "storage/encoding/key_codec.h"
#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "core/key_service.h"

namespace minikv {

namespace {

constexpr uint64_t kInitialSequence = 1ull << 63;

struct ListState {
  uint64_t head_seq = 0;
  uint64_t tail_seq = 0;
};

struct ListEntry {
  uint64_t seq = 0;
  std::string value;
};

struct NormalizedRange {
  size_t begin = 0;
  size_t end = 0;
  bool empty = true;
};

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

uint64_t DecodeUint64(const char* input) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value = (value << 8) | static_cast<unsigned char>(input[i]);
  }
  return value;
}

std::string EncodeListLocalPrefix(const std::string& key, uint64_t version) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  return out;
}

std::string EncodeListEntryLocalKey(const std::string& key, uint64_t version,
                                    uint64_t sequence) {
  std::string out = EncodeListLocalPrefix(key, version);
  AppendUint64(&out, sequence);
  return out;
}

std::string EncodeListStateLocalKey(const std::string& key, uint64_t version) {
  return EncodeListLocalPrefix(key, version);
}

bool DecodeListEntrySequence(const rocksdb::Slice& encoded_key,
                             const rocksdb::Slice& prefix,
                             uint64_t* sequence) {
  if (!KeyCodec::StartsWith(encoded_key, prefix) ||
      encoded_key.size() != prefix.size() + sizeof(uint64_t)) {
    return false;
  }
  if (sequence != nullptr) {
    *sequence = DecodeUint64(encoded_key.data() + prefix.size());
  }
  return true;
}

std::string EncodeListStateValue(const ListState& state) {
  std::string out;
  AppendUint64(&out, state.head_seq);
  AppendUint64(&out, state.tail_seq);
  return out;
}

bool DecodeListStateValue(const rocksdb::Slice& value, ListState* state) {
  if (state == nullptr || value.size() != sizeof(uint64_t) * 2) {
    return false;
  }
  state->head_seq = DecodeUint64(value.data());
  state->tail_seq = DecodeUint64(value.data() + sizeof(uint64_t));
  return true;
}

bool ParseInt64(const std::string& input, int64_t* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  errno = 0;
  char* parse_end = nullptr;
  const long long parsed = std::strtoll(input.c_str(), &parse_end, 10);
  if (parse_end == nullptr || *parse_end != '\0' || errno == ERANGE) {
    return false;
  }
  *value = static_cast<int64_t>(parsed);
  return true;
}

rocksdb::Status RequireListEncoding(const KeyLookup& lookup) {
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  if (lookup.metadata.type != ObjectType::kList ||
      lookup.metadata.encoding != ObjectEncoding::kListQuicklist) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }
  return rocksdb::Status::OK();
}

KeyMetadata BuildListMetadata(const CoreKeyService* key_service,
                              const KeyLookup& lookup) {
  if (lookup.exists) {
    return lookup.metadata;
  }

  KeyMetadata metadata =
      key_service->MakeMetadata(ObjectType::kList,
                                ObjectEncoding::kListQuicklist, lookup);
  metadata.size = 0;
  return metadata;
}

KeyMetadata BuildListTombstoneMetadata(const CoreKeyService* key_service,
                                       const KeyLookup& lookup) {
  KeyMetadata metadata = key_service->MakeTombstoneMetadata(lookup);
  metadata.size = 0;
  return metadata;
}

rocksdb::Status CollectEntries(ModuleSnapshot* snapshot,
                               const ModuleKeyspace& entries_keyspace,
                               const std::string& key, uint64_t version,
                               std::vector<ListEntry>* out) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("list entry output is required");
  }

  out->clear();
  const std::string prefix = EncodeListLocalPrefix(key, version);
  return snapshot->ScanPrefix(
      entries_keyspace, prefix,
      [out, &prefix](const rocksdb::Slice& encoded_key,
                     const rocksdb::Slice& value_slice) {
        uint64_t sequence = 0;
        if (!DecodeListEntrySequence(encoded_key, prefix, &sequence)) {
          return false;
        }
        out->push_back(ListEntry{sequence, value_slice.ToString()});
        return true;
      });
}

bool DeriveStateFromEntries(const std::vector<ListEntry>& entries,
                            ListState* state) {
  if (state == nullptr || entries.empty()) {
    return false;
  }
  state->head_seq = entries.front().seq;
  state->tail_seq = entries.back().seq;
  return true;
}

rocksdb::Status LoadListState(ModuleSnapshot* snapshot,
                              const ModuleKeyspace& state_keyspace,
                              const std::string& key, uint64_t version,
                              ListState* state) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (state == nullptr) {
    return rocksdb::Status::InvalidArgument("list state output is required");
  }

  std::string raw_state;
  rocksdb::Status status =
      snapshot->Get(state_keyspace, EncodeListStateLocalKey(key, version),
                    &raw_state);
  if (!status.ok()) {
    return status;
  }
  if (!DecodeListStateValue(raw_state, state)) {
    return rocksdb::Status::Corruption("invalid list state");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ResolveListState(ModuleSnapshot* snapshot,
                                 const ModuleKeyspace& entries_keyspace,
                                 const ModuleKeyspace& state_keyspace,
                                 const std::string& key, uint64_t version,
                                 ListState* state) {
  rocksdb::Status status =
      LoadListState(snapshot, state_keyspace, key, version, state);
  if (status.ok()) {
    return status;
  }
  if (!status.IsNotFound()) {
    return status;
  }

  std::vector<ListEntry> entries;
  status = CollectEntries(snapshot, entries_keyspace, key, version, &entries);
  if (!status.ok()) {
    return status;
  }
  if (!DeriveStateFromEntries(entries, state)) {
    return rocksdb::Status::NotFound("list state is missing");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status PutListState(ModuleWriteBatch* write_batch,
                             const ModuleKeyspace& state_keyspace,
                             const std::string& key, uint64_t version,
                             const ListState& state) {
  if (write_batch == nullptr) {
    return rocksdb::Status::InvalidArgument("module write batch is unavailable");
  }
  return write_batch->Put(state_keyspace, EncodeListStateLocalKey(key, version),
                          EncodeListStateValue(state));
}

rocksdb::Status DeleteListState(ModuleWriteBatch* write_batch,
                                const ModuleKeyspace& state_keyspace,
                                const std::string& key, uint64_t version) {
  if (write_batch == nullptr) {
    return rocksdb::Status::InvalidArgument("module write batch is unavailable");
  }
  return write_batch->Delete(state_keyspace,
                             EncodeListStateLocalKey(key, version));
}

NormalizedRange NormalizeRange(size_t size, int64_t start, int64_t stop) {
  NormalizedRange range;
  if (size == 0) {
    return range;
  }

  const int64_t size_i64 = static_cast<int64_t>(size);
  int64_t normalized_start = start < 0 ? size_i64 + start : start;
  int64_t normalized_stop = stop < 0 ? size_i64 + stop : stop;

  if (normalized_start < 0) {
    normalized_start = 0;
  }
  if (normalized_stop < 0) {
    return range;
  }
  if (normalized_start >= size_i64) {
    return range;
  }
  if (normalized_stop >= size_i64) {
    normalized_stop = size_i64 - 1;
  }
  if (normalized_start > normalized_stop) {
    return range;
  }

  range.begin = static_cast<size_t>(normalized_start);
  range.end = static_cast<size_t>(normalized_stop);
  range.empty = false;
  return range;
}

class LPushCmd : public Cmd {
 public:
  LPushCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "LPUSH requires at least one element");
    }
    key_ = input.key;
    elements_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    uint64_t length = 0;
    rocksdb::Status status = module_->PushLeft(key_, elements_, &length);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(length));
  }

  ListModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> elements_;
};

class LPopCmd : public Cmd {
 public:
  LPopCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("LPOP takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    std::string element;
    bool found = false;
    rocksdb::Status status = module_->PopLeft(key_, &element, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeBulkString(std::move(element));
  }

  ListModule* module_ = nullptr;
  std::string key_;
};

class LRangeCmd : public Cmd {
 public:
  LRangeCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument(
          "LRANGE requires start and stop");
    }
    if (!ParseInt64(input.args[0], &start_)) {
      return rocksdb::Status::InvalidArgument("LRANGE requires integer start");
    }
    if (!ParseInt64(input.args[1], &stop_)) {
      return rocksdb::Status::InvalidArgument("LRANGE requires integer stop");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    std::vector<std::string> values;
    rocksdb::Status status = module_->ReadRange(key_, start_, stop_, &values);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeArray(std::move(values));
  }

  ListModule* module_ = nullptr;
  std::string key_;
  int64_t start_ = 0;
  int64_t stop_ = 0;
};

class RPushCmd : public Cmd {
 public:
  RPushCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "RPUSH requires at least one element");
    }
    key_ = input.key;
    elements_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    uint64_t length = 0;
    rocksdb::Status status = module_->PushRight(key_, elements_, &length);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(length));
  }

  ListModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> elements_;
};

class RPopCmd : public Cmd {
 public:
  RPopCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("RPOP takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    std::string element;
    bool found = false;
    rocksdb::Status status = module_->PopRight(key_, &element, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeBulkString(std::move(element));
  }

  ListModule* module_ = nullptr;
  std::string key_;
};

class LRemCmd : public Cmd {
 public:
  LRemCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument("LREM requires count and element");
    }
    if (!ParseInt64(input.args[0], &count_)) {
      return rocksdb::Status::InvalidArgument("LREM requires integer count");
    }
    key_ = input.key;
    element_ = input.args[1];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    uint64_t removed = 0;
    rocksdb::Status status =
        module_->RemoveElements(key_, count_, element_, &removed);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(removed));
  }

  ListModule* module_ = nullptr;
  std::string key_;
  std::string element_;
  int64_t count_ = 0;
};

class LTrimCmd : public Cmd {
 public:
  LTrimCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument(
          "LTRIM requires start and stop");
    }
    if (!ParseInt64(input.args[0], &start_)) {
      return rocksdb::Status::InvalidArgument("LTRIM requires integer start");
    }
    if (!ParseInt64(input.args[1], &stop_)) {
      return rocksdb::Status::InvalidArgument("LTRIM requires integer stop");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    rocksdb::Status status = module_->Trim(key_, start_, stop_);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeSimpleString("OK");
  }

  ListModule* module_ = nullptr;
  std::string key_;
  int64_t start_ = 0;
  int64_t stop_ = 0;
};

class LLenCmd : public Cmd {
 public:
  LLenCmd(const CmdRegistration& registration, ListModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("LLEN takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("list module is unavailable"));
    }
    uint64_t length = 0;
    rocksdb::Status status = module_->Length(key_, &length);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(length));
  }

  ListModule* module_ = nullptr;
  std::string key_;
};

}  // namespace

rocksdb::Status ListModule::OnLoad(ModuleServices& services) {
  services_ = &services;

  rocksdb::Status status = services.command_registry().Register(
      {"LPUSH", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<LPushCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"LPOP", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<LPopCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"LRANGE", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<LRangeCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"RPUSH", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<RPushCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"RPOP", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<RPopCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"LREM", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<LRemCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"LTRIM", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<LTrimCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"LLEN", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<LLenCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

rocksdb::Status ListModule::OnStart(ModuleServices& services) {
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
  services.metrics().SetCounter("worker_count",
                                services.scheduler().worker_count());
  return rocksdb::Status::OK();
}

void ListModule::OnStop(ModuleServices& /*services*/) {
  started_ = false;
  delete_registry_ = nullptr;
  key_service_ = nullptr;
  services_ = nullptr;
}

rocksdb::Status ListModule::PushLeft(const std::string& key,
                                     const std::vector<std::string>& elements,
                                     uint64_t* new_length) {
  if (new_length != nullptr) {
    *new_length = 0;
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
  status = RequireListEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  const ModuleKeyspace state_keyspace = services_->storage().Keyspace("state");
  KeyMetadata after = BuildListMetadata(key_service_, lookup);
  ListState state;
  if (lookup.exists && after.size > 0) {
    status = ResolveListState(snapshot.get(), entries_keyspace, state_keyspace,
                              key, after.version, &state);
    if (!status.ok()) {
      return status;
    }
  } else {
    state.head_seq = kInitialSequence;
    state.tail_seq = kInitialSequence;
  }

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  for (const auto& element : elements) {
    uint64_t sequence = kInitialSequence;
    if (after.size == 0) {
      state.head_seq = kInitialSequence;
      state.tail_seq = kInitialSequence;
    } else {
      if (state.head_seq == 0) {
        return rocksdb::Status::InvalidArgument("LPUSH exceeds list sequence range");
      }
      state.head_seq -= 1;
    }
    sequence = state.head_seq;
    status =
        write_batch->Put(entries_keyspace,
                         EncodeListEntryLocalKey(key, after.version, sequence),
                         element);
    if (!status.ok()) {
      return status;
    }
    ++after.size;
  }

  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }
  status = PutListState(write_batch.get(), state_keyspace, key, after.version,
                        state);
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (new_length != nullptr) {
    *new_length = status.ok() ? after.size : 0;
  }
  return status;
}

rocksdb::Status ListModule::PushRight(const std::string& key,
                                      const std::vector<std::string>& elements,
                                      uint64_t* new_length) {
  if (new_length != nullptr) {
    *new_length = 0;
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
  status = RequireListEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  const ModuleKeyspace state_keyspace = services_->storage().Keyspace("state");
  KeyMetadata after = BuildListMetadata(key_service_, lookup);
  ListState state;
  if (lookup.exists && after.size > 0) {
    status = ResolveListState(snapshot.get(), entries_keyspace, state_keyspace,
                              key, after.version, &state);
    if (!status.ok()) {
      return status;
    }
  } else {
    state.head_seq = kInitialSequence;
    state.tail_seq = kInitialSequence;
  }

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  for (const auto& element : elements) {
    uint64_t sequence = kInitialSequence;
    if (after.size == 0) {
      state.head_seq = kInitialSequence;
      state.tail_seq = kInitialSequence;
    } else {
      if (state.tail_seq == std::numeric_limits<uint64_t>::max()) {
        return rocksdb::Status::InvalidArgument(
            "RPUSH exceeds list sequence range");
      }
      state.tail_seq += 1;
    }
    sequence = state.tail_seq;
    status =
        write_batch->Put(entries_keyspace,
                         EncodeListEntryLocalKey(key, after.version, sequence),
                         element);
    if (!status.ok()) {
      return status;
    }
    ++after.size;
  }

  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }
  status = PutListState(write_batch.get(), state_keyspace, key, after.version,
                        state);
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (new_length != nullptr) {
    *new_length = status.ok() ? after.size : 0;
  }
  return status;
}

rocksdb::Status ListModule::PopLeft(const std::string& key, std::string* element,
                                    bool* found) {
  if (element == nullptr) {
    return rocksdb::Status::InvalidArgument("list pop output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("list found output is required");
  }
  element->clear();
  *found = false;

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
  status = RequireListEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  const ModuleKeyspace state_keyspace = services_->storage().Keyspace("state");
  std::vector<ListEntry> entries;
  status = CollectEntries(snapshot.get(), entries_keyspace, key,
                          lookup.metadata.version, &entries);
  if (!status.ok()) {
    return status;
  }
  if (entries.empty()) {
    return rocksdb::Status::OK();
  }

  const ListEntry popped = entries.front();
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = write_batch->Delete(
      entries_keyspace,
      EncodeListEntryLocalKey(key, lookup.metadata.version, popped.seq));
  if (!status.ok()) {
    return status;
  }

  if (entries.size() == 1) {
    status = DeleteListState(write_batch.get(), state_keyspace, key,
                             lookup.metadata.version);
    if (!status.ok()) {
      return status;
    }
    const KeyMetadata after = BuildListTombstoneMetadata(key_service_, lookup);
    status = key_service_->PutMetadata(write_batch.get(), key, after);
  } else {
    KeyMetadata after = lookup.metadata;
    after.size = entries.size() - 1;
    const ListState state{entries[1].seq, entries.back().seq};
    status = key_service_->PutMetadata(write_batch.get(), key, after);
    if (!status.ok()) {
      return status;
    }
    status = PutListState(write_batch.get(), state_keyspace, key,
                          lookup.metadata.version, state);
  }
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (!status.ok()) {
    return status;
  }

  *element = popped.value;
  *found = true;
  return rocksdb::Status::OK();
}

rocksdb::Status ListModule::PopRight(const std::string& key, std::string* element,
                                     bool* found) {
  if (element == nullptr) {
    return rocksdb::Status::InvalidArgument("list pop output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("list found output is required");
  }
  element->clear();
  *found = false;

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
  status = RequireListEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  const ModuleKeyspace state_keyspace = services_->storage().Keyspace("state");
  std::vector<ListEntry> entries;
  status = CollectEntries(snapshot.get(), entries_keyspace, key,
                          lookup.metadata.version, &entries);
  if (!status.ok()) {
    return status;
  }
  if (entries.empty()) {
    return rocksdb::Status::OK();
  }

  const ListEntry popped = entries.back();
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = write_batch->Delete(
      entries_keyspace,
      EncodeListEntryLocalKey(key, lookup.metadata.version, popped.seq));
  if (!status.ok()) {
    return status;
  }

  if (entries.size() == 1) {
    status = DeleteListState(write_batch.get(), state_keyspace, key,
                             lookup.metadata.version);
    if (!status.ok()) {
      return status;
    }
    const KeyMetadata after = BuildListTombstoneMetadata(key_service_, lookup);
    status = key_service_->PutMetadata(write_batch.get(), key, after);
  } else {
    KeyMetadata after = lookup.metadata;
    after.size = entries.size() - 1;
    const ListState state{entries.front().seq, entries[entries.size() - 2].seq};
    status = key_service_->PutMetadata(write_batch.get(), key, after);
    if (!status.ok()) {
      return status;
    }
    status = PutListState(write_batch.get(), state_keyspace, key,
                          lookup.metadata.version, state);
  }
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (!status.ok()) {
    return status;
  }

  *element = popped.value;
  *found = true;
  return rocksdb::Status::OK();
}

rocksdb::Status ListModule::ReadRange(const std::string& key, int64_t start,
                                      int64_t stop,
                                      std::vector<std::string>* out) {
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("list range output is required");
  }
  out->clear();

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
  status = RequireListEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  std::vector<ListEntry> entries;
  status = CollectEntries(snapshot.get(), entries_keyspace, key,
                          lookup.metadata.version, &entries);
  if (!status.ok()) {
    return status;
  }
  const NormalizedRange range = NormalizeRange(entries.size(), start, stop);
  if (range.empty) {
    return rocksdb::Status::OK();
  }

  out->reserve(range.end - range.begin + 1);
  for (size_t index = range.begin; index <= range.end; ++index) {
    out->push_back(entries[index].value);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ListModule::RemoveElements(const std::string& key, int64_t count,
                                           const std::string& element,
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
  status = RequireListEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  const ModuleKeyspace state_keyspace = services_->storage().Keyspace("state");
  std::vector<ListEntry> entries;
  status = CollectEntries(snapshot.get(), entries_keyspace, key,
                          lookup.metadata.version, &entries);
  if (!status.ok()) {
    return status;
  }
  if (entries.empty()) {
    return rocksdb::Status::OK();
  }

  std::vector<bool> remove(entries.size(), false);
  size_t removed = 0;
  if (count >= 0) {
    const size_t limit =
        count == 0 ? std::numeric_limits<size_t>::max() : static_cast<size_t>(count);
    for (size_t index = 0; index < entries.size() && removed < limit; ++index) {
      if (entries[index].value == element) {
        remove[index] = true;
        ++removed;
      }
    }
  } else {
    const size_t limit =
        count == std::numeric_limits<int64_t>::min()
            ? std::numeric_limits<size_t>::max()
            : static_cast<size_t>(-count);
    for (size_t index = entries.size(); index > 0 && removed < limit; --index) {
      if (entries[index - 1].value == element) {
        remove[index - 1] = true;
        ++removed;
      }
    }
  }

  if (removed == 0) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  std::vector<ListEntry> kept_entries;
  kept_entries.reserve(entries.size() - removed);
  for (size_t index = 0; index < entries.size(); ++index) {
    if (remove[index]) {
      status = write_batch->Delete(
          entries_keyspace,
          EncodeListEntryLocalKey(key, lookup.metadata.version, entries[index].seq));
      if (!status.ok()) {
        return status;
      }
      continue;
    }
    kept_entries.push_back(entries[index]);
  }

  if (kept_entries.empty()) {
    status = DeleteListState(write_batch.get(), state_keyspace, key,
                             lookup.metadata.version);
    if (!status.ok()) {
      return status;
    }
    const KeyMetadata after = BuildListTombstoneMetadata(key_service_, lookup);
    status = key_service_->PutMetadata(write_batch.get(), key, after);
  } else {
    KeyMetadata after = lookup.metadata;
    after.size = kept_entries.size();
    ListState state;
    DeriveStateFromEntries(kept_entries, &state);
    status = key_service_->PutMetadata(write_batch.get(), key, after);
    if (!status.ok()) {
      return status;
    }
    status = PutListState(write_batch.get(), state_keyspace, key,
                          lookup.metadata.version, state);
  }
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (removed_count != nullptr) {
    *removed_count = status.ok() ? removed : 0;
  }
  return status;
}

rocksdb::Status ListModule::Trim(const std::string& key, int64_t start,
                                 int64_t stop) {
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
  status = RequireListEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  const ModuleKeyspace state_keyspace = services_->storage().Keyspace("state");
  std::vector<ListEntry> entries;
  status = CollectEntries(snapshot.get(), entries_keyspace, key,
                          lookup.metadata.version, &entries);
  if (!status.ok()) {
    return status;
  }
  if (entries.empty()) {
    return rocksdb::Status::OK();
  }

  const NormalizedRange range = NormalizeRange(entries.size(), start, stop);
  if (!range.empty && range.begin == 0 && range.end + 1 == entries.size()) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  std::vector<ListEntry> kept_entries;
  if (!range.empty) {
    kept_entries.reserve(range.end - range.begin + 1);
  }
  for (size_t index = 0; index < entries.size(); ++index) {
    const bool keep = !range.empty && index >= range.begin && index <= range.end;
    if (keep) {
      kept_entries.push_back(entries[index]);
      continue;
    }
    status = write_batch->Delete(
        entries_keyspace,
        EncodeListEntryLocalKey(key, lookup.metadata.version, entries[index].seq));
    if (!status.ok()) {
      return status;
    }
  }

  if (kept_entries.empty()) {
    status = DeleteListState(write_batch.get(), state_keyspace, key,
                             lookup.metadata.version);
    if (!status.ok()) {
      return status;
    }
    const KeyMetadata after = BuildListTombstoneMetadata(key_service_, lookup);
    status = key_service_->PutMetadata(write_batch.get(), key, after);
  } else {
    KeyMetadata after = lookup.metadata;
    after.size = kept_entries.size();
    ListState state;
    DeriveStateFromEntries(kept_entries, &state);
    status = key_service_->PutMetadata(write_batch.get(), key, after);
    if (!status.ok()) {
      return status;
    }
    status = PutListState(write_batch.get(), state_keyspace, key,
                          lookup.metadata.version, state);
  }
  if (!status.ok()) {
    return status;
  }
  return write_batch->Commit();
}

rocksdb::Status ListModule::Length(const std::string& key, uint64_t* length) {
  if (length == nullptr) {
    return rocksdb::Status::InvalidArgument("list length output is required");
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
  status = RequireListEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  *length = lookup.metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status ListModule::DeleteWholeKey(ModuleSnapshot* snapshot,
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

  rocksdb::Status status = RequireListEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace entries_keyspace = services_->storage().Keyspace("entries");
  const ModuleKeyspace state_keyspace = services_->storage().Keyspace("state");
  std::vector<ListEntry> entries;
  status = CollectEntries(snapshot, entries_keyspace, key, lookup.metadata.version,
                          &entries);
  if (!status.ok()) {
    return status;
  }

  for (const auto& entry : entries) {
    status = write_batch->Delete(
        entries_keyspace,
        EncodeListEntryLocalKey(key, lookup.metadata.version, entry.seq));
    if (!status.ok()) {
      return status;
    }
  }
  status =
      DeleteListState(write_batch, state_keyspace, key, lookup.metadata.version);
  if (!status.ok()) {
    return status;
  }

  const KeyMetadata after = BuildListTombstoneMetadata(key_service_, lookup);
  return key_service_->PutMetadata(write_batch, key, after);
}

rocksdb::Status ListModule::EnsureReady() const {
  if (services_ == nullptr || key_service_ == nullptr || !started_) {
    return rocksdb::Status::InvalidArgument("list module is unavailable");
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
