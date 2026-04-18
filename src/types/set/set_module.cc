#include "types/set/set_module.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <random>
#include <unordered_set>
#include <utility>
#include <vector>

#include "storage/encoding/key_codec.h"
#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "core/key_service.h"

namespace minikv {

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

std::string EncodeSetMemberPrefix(const std::string& key, uint64_t version) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  return out;
}

std::string EncodeSetMemberKey(const std::string& key, uint64_t version,
                               const std::string& member) {
  std::string out = EncodeSetMemberPrefix(key, version);
  out.append(member);
  return out;
}

bool ExtractMemberFromSetMemberKey(const rocksdb::Slice& encoded_key,
                                   const rocksdb::Slice& prefix,
                                   std::string* member) {
  if (!KeyCodec::StartsWith(encoded_key, prefix)) {
    return false;
  }
  if (member != nullptr) {
    member->assign(encoded_key.data() + prefix.size(),
                   encoded_key.size() - prefix.size());
  }
  return true;
}

rocksdb::Status RequireSetEncoding(const KeyLookup& lookup) {
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  if (lookup.metadata.type != ObjectType::kSet ||
      lookup.metadata.encoding != ObjectEncoding::kSetHashtable) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }
  return rocksdb::Status::OK();
}

KeyMetadata BuildSetMetadata(const CoreKeyService* key_service,
                             const KeyLookup& lookup) {
  if (lookup.exists) {
    return lookup.metadata;
  }

  KeyMetadata metadata =
      key_service->MakeMetadata(ObjectType::kSet,
                                ObjectEncoding::kSetHashtable, lookup);
  metadata.size = 0;
  return metadata;
}

KeyMetadata BuildSetTombstoneMetadata(const CoreKeyService* key_service,
                                      const KeyLookup& lookup) {
  KeyMetadata metadata = key_service->MakeTombstoneMetadata(lookup);
  metadata.size = 0;
  return metadata;
}

std::vector<std::string> DeduplicateMembers(
    const std::vector<std::string>& members) {
  std::vector<std::string> unique_members;
  unique_members.reserve(members.size());

  std::unordered_set<std::string> seen;
  seen.reserve(members.size());
  for (const auto& member : members) {
    if (seen.insert(member).second) {
      unique_members.push_back(member);
    }
  }
  return unique_members;
}

rocksdb::Status CollectMembers(ModuleSnapshot* snapshot,
                               const ModuleKeyspace& members_keyspace,
                               const std::string& key, uint64_t version,
                               std::vector<std::string>* out) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("members output is required");
  }

  out->clear();
  const std::string prefix = EncodeSetMemberPrefix(key, version);
  return snapshot->ScanPrefix(
      members_keyspace, prefix,
      [out, &prefix](const rocksdb::Slice& encoded_key,
                     const rocksdb::Slice& /*value_slice*/) {
        std::string member;
        if (!ExtractMemberFromSetMemberKey(encoded_key, prefix, &member)) {
          return false;
        }
        out->push_back(std::move(member));
        return true;
      });
}

size_t SelectRandomIndex(size_t count) {
  std::random_device device;
  std::mt19937_64 generator(
      (static_cast<uint64_t>(device()) << 32) ^ device());
  std::uniform_int_distribution<size_t> distribution(0, count - 1);
  return distribution(generator);
}

class SAddCmd : public Cmd {
 public:
  SAddCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SADD requires at least one member");
    }
    key_ = input.key;
    members_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    uint64_t added = 0;
    rocksdb::Status status = module_->AddMembers(key_, members_, &added);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(added));
  }

  SetModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> members_;
};

class SCardCmd : public Cmd {
 public:
  SCardCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SCARD takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    uint64_t size = 0;
    rocksdb::Status status = module_->Cardinality(key_, &size);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(size));
  }

  SetModule* module_ = nullptr;
  std::string key_;
};

class SMembersCmd : public Cmd {
 public:
  SMembersCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SMEMBERS takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    std::vector<std::string> members;
    rocksdb::Status status = module_->ReadMembers(key_, &members);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeArray(std::move(members));
  }

  SetModule* module_ = nullptr;
  std::string key_;
};

class SIsMemberCmd : public Cmd {
 public:
  SIsMemberCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("SISMEMBER requires member");
    }
    key_ = input.key;
    member_ = input.args[0];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    bool found = false;
    rocksdb::Status status = module_->IsMember(key_, member_, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(found ? 1 : 0);
  }

  SetModule* module_ = nullptr;
  std::string key_;
  std::string member_;
};

class SPopCmd : public Cmd {
 public:
  SPopCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SPOP takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    std::string member;
    bool found = false;
    rocksdb::Status status = module_->PopRandomMember(key_, &member, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeBulkString(std::move(member));
  }

  SetModule* module_ = nullptr;
  std::string key_;
};

class SRandMemberCmd : public Cmd {
 public:
  SRandMemberCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SRANDMEMBER takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    std::string member;
    bool found = false;
    rocksdb::Status status = module_->RandomMember(key_, &member, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeBulkString(std::move(member));
  }

  SetModule* module_ = nullptr;
  std::string key_;
};

class SRemCmd : public Cmd {
 public:
  SRemCmd(const CmdRegistration& registration, SetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "SREM requires at least one member");
    }
    key_ = input.key;
    members_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("set module is unavailable"));
    }
    uint64_t removed = 0;
    rocksdb::Status status = module_->RemoveMembers(key_, members_, &removed);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(removed));
  }

  SetModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> members_;
};

}  // namespace

rocksdb::Status SetModule::OnLoad(ModuleServices& services) {
  services_ = &services;

  rocksdb::Status status = services.command_registry().Register(
      {"SADD", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<SAddCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SCARD", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<SCardCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SMEMBERS", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<SMembersCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SISMEMBER", CmdFlags::kRead | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<SIsMemberCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SPOP", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<SPopCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SRANDMEMBER", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<SRandMemberCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"SREM", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<SRemCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

rocksdb::Status SetModule::OnStart(ModuleServices& services) {
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

void SetModule::OnStop(ModuleServices& /*services*/) {
  started_ = false;
  delete_registry_ = nullptr;
  key_service_ = nullptr;
  services_ = nullptr;
}

rocksdb::Status SetModule::AddMembers(const std::string& key,
                                      const std::vector<std::string>& members,
                                      uint64_t* added_count) {
  if (added_count != nullptr) {
    *added_count = 0;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  const std::vector<std::string> unique_members = DeduplicateMembers(members);
  if (unique_members.empty()) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  status = RequireSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  KeyMetadata before = BuildSetMetadata(key_service_, lookup);
  KeyMetadata after = before;
  uint64_t added = 0;
  std::string scratch;

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  for (const auto& member : unique_members) {
    const std::string member_key =
        EncodeSetMemberKey(key, before.version, member);
    status = snapshot->Get(members_keyspace, member_key, &scratch);
    if (status.ok()) {
      continue;
    }
    if (!status.IsNotFound()) {
      return status;
    }
    status = write_batch->Put(members_keyspace, member_key, "");
    if (!status.ok()) {
      return status;
    }
    ++added;
  }

  if (added == 0) {
    return rocksdb::Status::OK();
  }

  after.size += added;
  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (added_count != nullptr) {
    *added_count = status.ok() ? added : 0;
  }
  return status;
}

rocksdb::Status SetModule::Cardinality(const std::string& key, uint64_t* size) {
  if (size == nullptr) {
    return rocksdb::Status::InvalidArgument("set size output is required");
  }
  *size = 0;

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
  status = RequireSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  *size = lookup.metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status SetModule::ReadMembers(const std::string& key,
                                       std::vector<std::string>* out) {
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("members output is required");
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
  status = RequireSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  return CollectMembers(snapshot.get(), members_keyspace, key,
                        lookup.metadata.version, out);
}

rocksdb::Status SetModule::IsMember(const std::string& key,
                                    const std::string& member, bool* found) {
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("membership output is required");
  }
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
  status = RequireSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  std::string scratch;
  status = snapshot->Get(
      members_keyspace, EncodeSetMemberKey(key, lookup.metadata.version, member),
      &scratch);
  if (status.ok()) {
    *found = true;
    return rocksdb::Status::OK();
  }
  if (status.IsNotFound()) {
    return rocksdb::Status::OK();
  }
  return status;
}

rocksdb::Status SetModule::RemoveMembers(const std::string& key,
                                         const std::vector<std::string>& members,
                                         uint64_t* removed_count) {
  if (removed_count != nullptr) {
    *removed_count = 0;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  const std::vector<std::string> unique_members = DeduplicateMembers(members);
  if (unique_members.empty()) {
    return rocksdb::Status::OK();
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
  status = RequireSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  KeyMetadata before = lookup.metadata;
  uint64_t removed = 0;
  std::string scratch;
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  for (const auto& member : unique_members) {
    const std::string member_key =
        EncodeSetMemberKey(key, before.version, member);
    status = snapshot->Get(members_keyspace, member_key, &scratch);
    if (status.ok()) {
      status = write_batch->Delete(members_keyspace, member_key);
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

  KeyMetadata after = before;
  if (removed >= before.size) {
    after = BuildSetTombstoneMetadata(key_service_, lookup);
  } else {
    after.size -= removed;
  }

  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (removed_count != nullptr) {
    *removed_count = status.ok() ? removed : 0;
  }
  return status;
}

rocksdb::Status SetModule::RandomMember(const std::string& key,
                                        std::string* member, bool* found) {
  if (member == nullptr) {
    return rocksdb::Status::InvalidArgument("random member output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("member found output is required");
  }
  member->clear();
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
  status = RequireSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  std::vector<std::string> members;
  status = CollectMembers(snapshot.get(), members_keyspace, key,
                          lookup.metadata.version, &members);
  if (!status.ok()) {
    return status;
  }
  if (members.empty()) {
    return rocksdb::Status::OK();
  }

  *member = members[SelectRandomIndex(members.size())];
  *found = true;
  return rocksdb::Status::OK();
}

rocksdb::Status SetModule::PopRandomMember(const std::string& key,
                                           std::string* member, bool* found) {
  if (member == nullptr) {
    return rocksdb::Status::InvalidArgument("random member output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("member found output is required");
  }
  member->clear();
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
  status = RequireSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  std::vector<std::string> members;
  status = CollectMembers(snapshot.get(), members_keyspace, key,
                          lookup.metadata.version, &members);
  if (!status.ok()) {
    return status;
  }
  if (members.empty()) {
    return rocksdb::Status::OK();
  }

  const std::string selected_member = members[SelectRandomIndex(members.size())];
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = write_batch->Delete(
      members_keyspace, EncodeSetMemberKey(key, lookup.metadata.version,
                                           selected_member));
  if (!status.ok()) {
    return status;
  }

  KeyMetadata after = lookup.metadata;
  if (lookup.metadata.size <= 1 || members.size() <= 1) {
    after = BuildSetTombstoneMetadata(key_service_, lookup);
  } else {
    --after.size;
  }
  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (!status.ok()) {
    return status;
  }

  *member = selected_member;
  *found = true;
  return rocksdb::Status::OK();
}

rocksdb::Status SetModule::DeleteWholeKey(ModuleSnapshot* snapshot,
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

  rocksdb::Status status = RequireSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  std::vector<std::string> members;
  status = CollectMembers(snapshot, members_keyspace, key, lookup.metadata.version,
                          &members);
  if (!status.ok()) {
    return status;
  }

  for (const auto& member : members) {
    status = write_batch->Delete(
        members_keyspace, EncodeSetMemberKey(key, lookup.metadata.version, member));
    if (!status.ok()) {
      return status;
    }
  }

  const KeyMetadata after = BuildSetTombstoneMetadata(key_service_, lookup);
  return key_service_->PutMetadata(write_batch, key, after);
}

rocksdb::Status SetModule::EnsureReady() const {
  if (services_ == nullptr || key_service_ == nullptr || !started_) {
    return rocksdb::Status::InvalidArgument("set module is unavailable");
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
