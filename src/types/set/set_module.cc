#include "types/set/set_module.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <unordered_set>
#include <vector>

#include "types/set/set_commands.h"
#include "types/set/set_internal.h"
#include "runtime/module/module_services.h"
#include "core/key_service.h"

namespace minikv {

namespace {

enum class SetCombineKind {
  kUnion,
  kIntersection,
  kDifference,
};

uint64_t AbsoluteCount(int64_t count) {
  if (count >= 0) {
    return static_cast<uint64_t>(count);
  }
  return static_cast<uint64_t>(-(count + 1)) + 1;
}

rocksdb::Status CountFitsSizeT(uint64_t count) {
  if (count > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
    return rocksdb::Status::InvalidArgument("count is out of range");
  }
  return rocksdb::Status::OK();
}

KeyMetadata BuildStoredSetMetadata(const CoreKeyService* key_service,
                                   const KeyLookup& lookup, uint64_t size) {
  KeyMetadata metadata;
  if (lookup.exists) {
    metadata = lookup.metadata;
  } else {
    metadata = key_service->MakeMetadata(ObjectType::kSet,
                                         ObjectEncoding::kSetHashtable,
                                         lookup);
  }
  metadata.type = ObjectType::kSet;
  metadata.encoding = ObjectEncoding::kSetHashtable;
  metadata.size = size;
  metadata.expire_at_ms = 0;
  return metadata;
}

rocksdb::Status LoadSetMembers(ModuleSnapshot* snapshot,
                               const ModuleKeyspace& members_keyspace,
                               const CoreKeyService* key_service,
                               const std::string& key,
                               std::vector<std::string>* members) {
  if (members == nullptr) {
    return rocksdb::Status::InvalidArgument("members output is required");
  }
  members->clear();

  KeyLookup lookup;
  rocksdb::Status status = key_service->Lookup(snapshot, key, &lookup);
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
  return CollectSetMembers(snapshot, members_keyspace, key,
                           lookup.metadata.version, members);
}

rocksdb::Status ComputeSetCombination(ModuleSnapshot* snapshot,
                                      const ModuleKeyspace& members_keyspace,
                                      const CoreKeyService* key_service,
                                      SetCombineKind kind,
                                      const std::vector<std::string>& keys,
                                      std::vector<std::string>* out) {
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("members output is required");
  }
  out->clear();
  if (keys.empty()) {
    return rocksdb::Status::OK();
  }

  std::vector<std::string> members;
  rocksdb::Status status;
  switch (kind) {
    case SetCombineKind::kUnion: {
      std::unordered_set<std::string> seen;
      for (const auto& key : keys) {
        status =
            LoadSetMembers(snapshot, members_keyspace, key_service, key, &members);
        if (!status.ok()) {
          return status;
        }
        for (const auto& member : members) {
          if (seen.insert(member).second) {
            out->push_back(member);
          }
        }
      }
      return rocksdb::Status::OK();
    }
    case SetCombineKind::kIntersection: {
      status = LoadSetMembers(snapshot, members_keyspace, key_service, keys[0],
                              out);
      if (!status.ok() || out->empty()) {
        return status;
      }

      for (size_t i = 1; i < keys.size(); ++i) {
        status = LoadSetMembers(snapshot, members_keyspace, key_service,
                                keys[i], &members);
        if (!status.ok()) {
          return status;
        }
        if (members.empty()) {
          out->clear();
          return rocksdb::Status::OK();
        }

        std::unordered_set<std::string> rhs(members.begin(), members.end());
        std::vector<std::string> filtered;
        filtered.reserve(out->size());
        for (const auto& member : *out) {
          if (rhs.find(member) != rhs.end()) {
            filtered.push_back(member);
          }
        }
        *out = std::move(filtered);
        if (out->empty()) {
          return rocksdb::Status::OK();
        }
      }
      return rocksdb::Status::OK();
    }
    case SetCombineKind::kDifference: {
      status = LoadSetMembers(snapshot, members_keyspace, key_service, keys[0],
                              out);
      if (!status.ok() || out->empty()) {
        return status;
      }

      for (size_t i = 1; i < keys.size(); ++i) {
        status = LoadSetMembers(snapshot, members_keyspace, key_service,
                                keys[i], &members);
        if (!status.ok()) {
          return status;
        }
        if (members.empty()) {
          continue;
        }

        std::unordered_set<std::string> rhs(members.begin(), members.end());
        std::vector<std::string> filtered;
        filtered.reserve(out->size());
        for (const auto& member : *out) {
          if (rhs.find(member) == rhs.end()) {
            filtered.push_back(member);
          }
        }
        *out = std::move(filtered);
        if (out->empty()) {
          return rocksdb::Status::OK();
        }
      }
      return rocksdb::Status::OK();
    }
  }

  return rocksdb::Status::InvalidArgument("unsupported set operation");
}

rocksdb::Status StoreSetResult(ModuleServices* services,
                               const CoreKeyService* key_service,
                               WholeKeyDeleteRegistry* delete_registry,
                               ModuleSnapshot* snapshot,
                               const std::string& destination,
                               std::vector<std::string> members,
                               uint64_t* stored_count) {
  if (stored_count != nullptr) {
    *stored_count = 0;
  }
  if (services == nullptr || key_service == nullptr ||
      delete_registry == nullptr) {
    return rocksdb::Status::InvalidArgument("set module is unavailable");
  }

  members = DeduplicateMembers(members);

  KeyLookup destination_lookup;
  rocksdb::Status status =
      key_service->Lookup(snapshot, destination, &destination_lookup);
  if (!status.ok()) {
    return status;
  }

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services->storage().CreateWriteBatch();
  bool has_writes = false;
  if (destination_lookup.exists) {
    status = delete_registry->DeleteWholeKey(
        snapshot, write_batch.get(), destination, destination_lookup);
    if (!status.ok()) {
      return status;
    }
    has_writes = true;
  }

  if (members.empty()) {
    if (!has_writes) {
      return rocksdb::Status::OK();
    }
    return write_batch->Commit();
  }

  const KeyMetadata metadata = BuildStoredSetMetadata(
      key_service, destination_lookup, static_cast<uint64_t>(members.size()));
  const ModuleKeyspace members_keyspace = services->storage().Keyspace("members");
  for (const auto& member : members) {
    status = write_batch->Put(
        members_keyspace,
        EncodeSetMemberKey(destination, metadata.version, member), "");
    if (!status.ok()) {
      return status;
    }
  }

  status = key_service->PutMetadata(write_batch.get(), destination, metadata);
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (stored_count != nullptr) {
    *stored_count = status.ok() ? static_cast<uint64_t>(members.size()) : 0;
  }
  return status;
}

}  // namespace

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

rocksdb::Status SetModule::IsMembers(
    const std::string& key, const std::vector<std::string>& members,
    std::vector<bool>* found) {
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("membership output is required");
  }
  found->assign(members.size(), false);

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
  if (!lookup.exists || members.empty()) {
    return rocksdb::Status::OK();
  }
  status = RequireSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace =
      services_->storage().Keyspace("members");
  std::string scratch;
  for (size_t i = 0; i < members.size(); ++i) {
    status = snapshot->Get(
        members_keyspace,
        EncodeSetMemberKey(key, lookup.metadata.version, members[i]),
        &scratch);
    if (status.ok()) {
      (*found)[i] = true;
    } else if (!status.IsNotFound()) {
      return status;
    }
  }
  return rocksdb::Status::OK();
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

rocksdb::Status SetModule::MoveMember(const std::string& source,
                                      const std::string& destination,
                                      const std::string& member, bool* moved) {
  if (moved == nullptr) {
    return rocksdb::Status::InvalidArgument("move output is required");
  }
  *moved = false;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup source_lookup;
  rocksdb::Status status =
      key_service_->Lookup(snapshot.get(), source, &source_lookup);
  if (!status.ok()) {
    return status;
  }
  if (!source_lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireSetEncoding(source_lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace =
      services_->storage().Keyspace("members");
  std::string scratch;
  const std::string source_member_key =
      EncodeSetMemberKey(source, source_lookup.metadata.version, member);
  status = snapshot->Get(members_keyspace, source_member_key, &scratch);
  if (status.IsNotFound()) {
    return rocksdb::Status::OK();
  }
  if (!status.ok()) {
    return status;
  }

  if (source == destination) {
    *moved = true;
    return rocksdb::Status::OK();
  }

  KeyLookup destination_lookup;
  status = key_service_->Lookup(snapshot.get(), destination, &destination_lookup);
  if (!status.ok()) {
    return status;
  }
  status = RequireSetEncoding(destination_lookup);
  if (!status.ok()) {
    return status;
  }

  bool destination_has_member = false;
  if (destination_lookup.exists) {
    status = snapshot->Get(
        members_keyspace,
        EncodeSetMemberKey(destination, destination_lookup.metadata.version,
                           member),
        &scratch);
    if (status.ok()) {
      destination_has_member = true;
    } else if (!status.IsNotFound()) {
      return status;
    }
  }

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = write_batch->Delete(members_keyspace, source_member_key);
  if (!status.ok()) {
    return status;
  }

  KeyMetadata source_after = source_lookup.metadata;
  if (source_lookup.metadata.size <= 1) {
    source_after = BuildSetTombstoneMetadata(key_service_, source_lookup);
  } else {
    --source_after.size;
  }
  status = key_service_->PutMetadata(write_batch.get(), source, source_after);
  if (!status.ok()) {
    return status;
  }

  if (!destination_has_member) {
    KeyMetadata destination_after = BuildSetMetadata(key_service_,
                                                    destination_lookup);
    ++destination_after.size;
    status = write_batch->Put(
        members_keyspace,
        EncodeSetMemberKey(destination, destination_after.version, member), "");
    if (!status.ok()) {
      return status;
    }
    status = key_service_->PutMetadata(write_batch.get(), destination,
                                       destination_after);
    if (!status.ok()) {
      return status;
    }
  }

  status = write_batch->Commit();
  if (!status.ok()) {
    return status;
  }

  *moved = true;
  return rocksdb::Status::OK();
}

rocksdb::Status SetModule::Union(const std::vector<std::string>& keys,
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
  const ModuleKeyspace members_keyspace =
      services_->storage().Keyspace("members");
  return ComputeSetCombination(snapshot.get(), members_keyspace, key_service_,
                               SetCombineKind::kUnion, keys, out);
}

rocksdb::Status SetModule::Intersection(const std::vector<std::string>& keys,
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
  const ModuleKeyspace members_keyspace =
      services_->storage().Keyspace("members");
  return ComputeSetCombination(snapshot.get(), members_keyspace, key_service_,
                               SetCombineKind::kIntersection, keys, out);
}

rocksdb::Status SetModule::Difference(const std::vector<std::string>& keys,
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
  const ModuleKeyspace members_keyspace =
      services_->storage().Keyspace("members");
  return ComputeSetCombination(snapshot.get(), members_keyspace, key_service_,
                               SetCombineKind::kDifference, keys, out);
}

rocksdb::Status SetModule::StoreUnion(const std::string& destination,
                                      const std::vector<std::string>& keys,
                                      uint64_t* stored_count) {
  if (stored_count != nullptr) {
    *stored_count = 0;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  const ModuleKeyspace members_keyspace =
      services_->storage().Keyspace("members");
  std::vector<std::string> members;
  rocksdb::Status status = ComputeSetCombination(
      snapshot.get(), members_keyspace, key_service_, SetCombineKind::kUnion,
      keys, &members);
  if (!status.ok()) {
    return status;
  }
  return StoreSetResult(services_, key_service_, delete_registry_,
                        snapshot.get(), destination, std::move(members),
                        stored_count);
}

rocksdb::Status SetModule::StoreIntersection(
    const std::string& destination, const std::vector<std::string>& keys,
    uint64_t* stored_count) {
  if (stored_count != nullptr) {
    *stored_count = 0;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  const ModuleKeyspace members_keyspace =
      services_->storage().Keyspace("members");
  std::vector<std::string> members;
  rocksdb::Status status = ComputeSetCombination(
      snapshot.get(), members_keyspace, key_service_,
      SetCombineKind::kIntersection, keys, &members);
  if (!status.ok()) {
    return status;
  }
  return StoreSetResult(services_, key_service_, delete_registry_,
                        snapshot.get(), destination, std::move(members),
                        stored_count);
}

rocksdb::Status SetModule::StoreDifference(
    const std::string& destination, const std::vector<std::string>& keys,
    uint64_t* stored_count) {
  if (stored_count != nullptr) {
    *stored_count = 0;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  const ModuleKeyspace members_keyspace =
      services_->storage().Keyspace("members");
  std::vector<std::string> members;
  rocksdb::Status status = ComputeSetCombination(
      snapshot.get(), members_keyspace, key_service_,
      SetCombineKind::kDifference, keys, &members);
  if (!status.ok()) {
    return status;
  }
  return StoreSetResult(services_, key_service_, delete_registry_,
                        snapshot.get(), destination, std::move(members),
                        stored_count);
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

rocksdb::Status SetModule::RandomMembers(const std::string& key, int64_t count,
                                         std::vector<std::string>* members) {
  if (members == nullptr) {
    return rocksdb::Status::InvalidArgument("random members output is required");
  }
  members->clear();

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  const uint64_t requested_count = AbsoluteCount(count);
  rocksdb::Status status = CountFitsSizeT(requested_count);
  if (!status.ok()) {
    return status;
  }
  if (requested_count == 0) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  status = key_service_->Lookup(snapshot.get(), key, &lookup);
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

  const ModuleKeyspace members_keyspace =
      services_->storage().Keyspace("members");
  std::vector<std::string> all_members;
  status = CollectSetMembers(snapshot.get(), members_keyspace, key,
                             lookup.metadata.version, &all_members);
  if (!status.ok()) {
    return status;
  }
  if (all_members.empty()) {
    return rocksdb::Status::OK();
  }

  if (count < 0) {
    members->reserve(static_cast<size_t>(requested_count));
    for (uint64_t i = 0; i < requested_count; ++i) {
      members->push_back(all_members[SelectRandomIndex(all_members.size())]);
    }
    return rocksdb::Status::OK();
  }

  const size_t unique_count = std::min(
      static_cast<size_t>(requested_count), all_members.size());
  members->reserve(unique_count);
  for (size_t i = 0; i < unique_count; ++i) {
    const size_t index = SelectRandomIndex(all_members.size());
    members->push_back(all_members[index]);
    all_members[index] = std::move(all_members.back());
    all_members.pop_back();
  }
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

rocksdb::Status SetModule::PopRandomMembers(const std::string& key,
                                            uint64_t count,
                                            std::vector<std::string>* members) {
  if (members == nullptr) {
    return rocksdb::Status::InvalidArgument("random members output is required");
  }
  members->clear();

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  rocksdb::Status status = CountFitsSizeT(count);
  if (!status.ok()) {
    return status;
  }
  if (count == 0) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  status = key_service_->Lookup(snapshot.get(), key, &lookup);
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

  const ModuleKeyspace members_keyspace =
      services_->storage().Keyspace("members");
  std::vector<std::string> all_members;
  status = CollectSetMembers(snapshot.get(), members_keyspace, key,
                             lookup.metadata.version, &all_members);
  if (!status.ok()) {
    return status;
  }
  if (all_members.empty()) {
    return rocksdb::Status::OK();
  }

  const size_t pop_count =
      std::min(static_cast<size_t>(count), all_members.size());
  members->reserve(pop_count);
  for (size_t i = 0; i < pop_count; ++i) {
    const size_t index = SelectRandomIndex(all_members.size());
    members->push_back(all_members[index]);
    all_members[index] = std::move(all_members.back());
    all_members.pop_back();
  }

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  for (const auto& member : *members) {
    status = write_batch->Delete(
        members_keyspace,
        EncodeSetMemberKey(key, lookup.metadata.version, member));
    if (!status.ok()) {
      return status;
    }
  }

  KeyMetadata after = lookup.metadata;
  if (pop_count >= lookup.metadata.size || all_members.empty()) {
    after = BuildSetTombstoneMetadata(key_service_, lookup);
  } else {
    after.size -= pop_count;
  }
  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (!status.ok()) {
    members->clear();
  }
  return status;
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
  if (services_ == nullptr || key_service_ == nullptr ||
      delete_registry_ == nullptr || !started_) {
    return rocksdb::Status::InvalidArgument("set module is unavailable");
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
