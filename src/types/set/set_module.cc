#include "types/set/set_module.h"

#include <algorithm>
#include <memory>
#include <vector>

#include "types/set/set_commands.h"
#include "types/set/set_internal.h"
#include "runtime/module/module_services.h"
#include "core/key_service.h"

namespace minikv {

rocksdb::Status SetModule::OnLoad(ModuleServices& services) {
  services_ = &services;
  return RegisterSetCommands(services, this);
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
  return CollectSetMembers(snapshot.get(), members_keyspace, key,
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
  status = CollectSetMembers(snapshot.get(), members_keyspace, key,
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
  status = CollectSetMembers(snapshot.get(), members_keyspace, key,
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
  status = CollectSetMembers(snapshot, members_keyspace, key,
                             lookup.metadata.version, &members);
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
