#include "types/stream/stream_module.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "core/key_service.h"
#include "runtime/module/module_services.h"
#include "types/stream/stream_commands.h"
#include "types/stream/stream_common.h"

namespace minikv {

rocksdb::Status StreamModule::OnLoad(ModuleServices& services) {
  services_ = &services;
  return RegisterStreamCommands(services, this);
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
  using namespace stream_internal;

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
  using namespace stream_internal;

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
  using namespace stream_internal;

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
  using namespace stream_internal;

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
  using namespace stream_internal;

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
  using namespace stream_internal;

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
  using namespace stream_internal;

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
  using namespace stream_internal;

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
