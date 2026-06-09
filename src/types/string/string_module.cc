#include "types/string/string_module.h"

#include <algorithm>
#include <cctype>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>

#include "core/key_service.h"
#include "runtime/module/module_services.h"
#include "types/string/string_commands.h"

namespace minikv {

namespace {

constexpr size_t kMaxSetRangeMaterializedBytes = 512ULL * 1024ULL * 1024ULL;

rocksdb::Status RequireStringEncoding(const KeyLookup& lookup) {
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  if (lookup.metadata.type != ObjectType::kString ||
      lookup.metadata.encoding != ObjectEncoding::kRaw) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }
  return rocksdb::Status::OK();
}

KeyMetadata BuildStringMetadata(const CoreKeyService* key_service,
                                const KeyLookup& lookup, bool clear_ttl) {
  KeyMetadata metadata;
  if (lookup.exists) {
    metadata = lookup.metadata;
  } else {
    metadata = key_service->MakeMetadata(ObjectType::kString,
                                         ObjectEncoding::kRaw, lookup);
  }
  if (clear_ttl) {
    metadata.expire_at_ms = 0;
  }
  return metadata;
}

KeyMetadata BuildStringTombstoneMetadata(const CoreKeyService* key_service,
                                         const KeyLookup& lookup) {
  KeyMetadata metadata = key_service->MakeTombstoneMetadata(lookup);
  metadata.size = 0;
  return metadata;
}

rocksdb::Status ReadStoredValue(ModuleSnapshot* snapshot,
                                const ModuleKeyspace& data_keyspace,
                                const std::string& key, std::string* value) {
  rocksdb::Status status = snapshot->Get(data_keyspace, key, value);
  if (status.ok()) {
    return rocksdb::Status::OK();
  }
  if (status.IsNotFound()) {
    return rocksdb::Status::Corruption("string value is missing");
  }
  return status;
}

bool ParseInt64Strict(const std::string& input, int64_t* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  const bool negative = input.front() == '-';
  size_t index = negative ? 1 : 0;
  if (index == input.size()) {
    return false;
  }

  const uint64_t limit =
      negative ? static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1
               : static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
  uint64_t parsed = 0;
  for (; index < input.size(); ++index) {
    const unsigned char ch = static_cast<unsigned char>(input[index]);
    if (!std::isdigit(ch)) {
      return false;
    }
    const uint64_t digit = static_cast<uint64_t>(ch - '0');
    if (parsed > (limit - digit) / 10) {
      return false;
    }
    parsed = parsed * 10 + digit;
  }

  if (negative) {
    if (parsed == limit) {
      *value = std::numeric_limits<int64_t>::min();
    } else {
      *value = -static_cast<int64_t>(parsed);
    }
  } else {
    *value = static_cast<int64_t>(parsed);
  }
  return true;
}

rocksdb::Status CheckSetRangeSize(uint64_t offset, size_t value_size,
                                  size_t* required_size) {
  if (required_size == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "string range size output is required");
  }
  if (offset > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
    return rocksdb::Status::InvalidArgument("string offset is too large");
  }
  const size_t offset_size = static_cast<size_t>(offset);
  if (value_size > std::numeric_limits<size_t>::max() - offset_size) {
    return rocksdb::Status::InvalidArgument("string size is too large");
  }
  const size_t size = offset_size + value_size;
  if (size > kMaxSetRangeMaterializedBytes) {
    return rocksdb::Status::InvalidArgument("string size is too large");
  }
  *required_size = size;
  return rocksdb::Status::OK();
}

std::string SliceRange(const std::string& value, int64_t start, int64_t end) {
  if (value.empty()) {
    return {};
  }

  const int64_t length = static_cast<int64_t>(value.size());
  if (start < 0) {
    start = length + start;
  }
  if (end < 0) {
    end = length + end;
  }

  if (start < 0) {
    start = 0;
  }
  if (end < 0 || start >= length || start > end) {
    return {};
  }
  if (end >= length) {
    end = length - 1;
  }

  return value.substr(static_cast<size_t>(start),
                      static_cast<size_t>(end - start + 1));
}

rocksdb::Status CheckIntegerResult(__int128 value) {
  if (value < static_cast<__int128>(std::numeric_limits<int64_t>::min()) ||
      value > static_cast<__int128>(std::numeric_limits<int64_t>::max())) {
    return rocksdb::Status::InvalidArgument(
        "increment or decrement would overflow");
  }
  return rocksdb::Status::OK();
}

}  // namespace

rocksdb::Status StringModule::OnLoad(ModuleServices& services) {
  services_ = &services;

  rocksdb::Status status = services.exports().Publish<StringBridge>(
      kStringBridgeExportName, static_cast<StringBridge*>(this));
  if (!status.ok()) {
    return status;
  }

  return RegisterStringCommands(services, this);
}

rocksdb::Status StringModule::OnStart(ModuleServices& services) {
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

void StringModule::OnStop(ModuleServices& /*services*/) {
  started_ = false;
  delete_registry_ = nullptr;
  key_service_ = nullptr;
  services_ = nullptr;
}

rocksdb::Status StringModule::SetValue(const std::string& key,
                                       const std::string& value) {
  return StoreValue(key, value, false);
}

rocksdb::Status StringModule::ReplaceValue(const std::string& key,
                                           const std::string& value) {
  return StoreValue(key, value, true);
}

rocksdb::Status StringModule::StoreValue(const std::string& key,
                                         const std::string& value,
                                         bool clear_ttl) {
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
  status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  KeyMetadata after = BuildStringMetadata(key_service_, lookup, clear_ttl);
  after.size = value.size();

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = write_batch->Put(data_keyspace, key, value);
  if (!status.ok()) {
    return status;
  }
  status = key_service_->PutMetadata(write_batch.get(), key, lookup, after);
  if (!status.ok()) {
    return status;
  }
  return write_batch->Commit();
}

rocksdb::Status StringModule::SetValues(
    const std::vector<std::pair<std::string, std::string>>& values) {
  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::vector<std::pair<std::string, std::string>> deduplicated;
  deduplicated.reserve(values.size());
  for (const auto& value : values) {
    auto it = std::find_if(
        deduplicated.begin(), deduplicated.end(),
        [&value](const std::pair<std::string, std::string>& existing) {
          return existing.first == value.first;
        });
    if (it == deduplicated.end()) {
      deduplicated.push_back(value);
    } else {
      it->second = value.second;
    }
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");

  for (const auto& value : deduplicated) {
    KeyLookup lookup;
    rocksdb::Status status =
        key_service_->Lookup(snapshot.get(), value.first, &lookup);
    if (!status.ok()) {
      return status;
    }
    status = RequireStringEncoding(lookup);
    if (!status.ok()) {
      return status;
    }

    KeyMetadata after = BuildStringMetadata(key_service_, lookup, true);
    after.size = value.second.size();
    status = write_batch->Put(data_keyspace, value.first, value.second);
    if (!status.ok()) {
      return status;
    }
    status =
        key_service_->PutMetadata(write_batch.get(), value.first, lookup, after);
    if (!status.ok()) {
      return status;
    }
  }

  return write_batch->Commit();
}

rocksdb::Status StringModule::GetValue(const std::string& key, std::string* value,
                                       bool* found) {
  if (value == nullptr) {
    return rocksdb::Status::InvalidArgument("string value output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("string found output is required");
  }
  value->clear();
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
  status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  status = ReadStoredValue(snapshot.get(), data_keyspace, key, value);
  if (!status.ok()) {
    return status;
  }
  *found = true;
  return rocksdb::Status::OK();
}

rocksdb::Status StringModule::GetValues(const std::vector<std::string>& keys,
                                        std::vector<StringValue>* values) {
  if (values == nullptr) {
    return rocksdb::Status::InvalidArgument("string values output is required");
  }
  values->clear();
  values->reserve(keys.size());

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  for (const auto& key : keys) {
    KeyLookup lookup;
    rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
    if (!status.ok()) {
      return status;
    }
    status = RequireStringEncoding(lookup);
    if (!status.ok()) {
      return status;
    }
    if (!lookup.exists) {
      values->push_back(StringValue{false, ""});
      continue;
    }

    std::string value;
    status = ReadStoredValue(snapshot.get(), data_keyspace, key, &value);
    if (!status.ok()) {
      return status;
    }
    values->push_back(StringValue{true, std::move(value)});
  }
  return rocksdb::Status::OK();
}

rocksdb::Status StringModule::AppendValue(const std::string& key,
                                          const std::string& suffix,
                                          uint64_t* new_length) {
  if (new_length == nullptr) {
    return rocksdb::Status::InvalidArgument("string length output is required");
  }
  *new_length = 0;

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
  status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  std::string value;
  if (lookup.exists) {
    status = ReadStoredValue(snapshot.get(), data_keyspace, key, &value);
    if (!status.ok()) {
      return status;
    }
  }
  if (suffix.size() > value.max_size() - value.size()) {
    return rocksdb::Status::InvalidArgument("string size is too large");
  }
  try {
    value.append(suffix);
  } catch (const std::bad_alloc&) {
    return rocksdb::Status::InvalidArgument("string size is too large");
  } catch (const std::length_error&) {
    return rocksdb::Status::InvalidArgument("string size is too large");
  }

  KeyMetadata after = BuildStringMetadata(key_service_, lookup, false);
  after.size = value.size();
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = write_batch->Put(data_keyspace, key, value);
  if (!status.ok()) {
    return status;
  }
  status = key_service_->PutMetadata(write_batch.get(), key, lookup, after);
  if (!status.ok()) {
    return status;
  }
  status = write_batch->Commit();
  if (!status.ok()) {
    return status;
  }
  *new_length = static_cast<uint64_t>(value.size());
  return rocksdb::Status::OK();
}

rocksdb::Status StringModule::GetRange(const std::string& key, int64_t start,
                                       int64_t end, std::string* value) {
  if (value == nullptr) {
    return rocksdb::Status::InvalidArgument("string value output is required");
  }
  value->clear();

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
  status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  std::string stored;
  status = ReadStoredValue(snapshot.get(), data_keyspace, key, &stored);
  if (!status.ok()) {
    return status;
  }
  *value = SliceRange(stored, start, end);
  return rocksdb::Status::OK();
}

rocksdb::Status StringModule::SetRange(const std::string& key, uint64_t offset,
                                       const std::string& range_value,
                                       uint64_t* new_length) {
  if (new_length == nullptr) {
    return rocksdb::Status::InvalidArgument("string length output is required");
  }
  *new_length = 0;

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
  status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  std::string value;
  if (lookup.exists) {
    status = ReadStoredValue(snapshot.get(), data_keyspace, key, &value);
    if (!status.ok()) {
      return status;
    }
  }

  if (range_value.empty()) {
    *new_length = static_cast<uint64_t>(value.size());
    return rocksdb::Status::OK();
  }

  size_t required_size = 0;
  status = CheckSetRangeSize(offset, range_value.size(), &required_size);
  if (!status.ok()) {
    return status;
  }
  if (required_size > value.max_size()) {
    return rocksdb::Status::InvalidArgument("string size is too large");
  }

  try {
    if (required_size > value.size()) {
      value.resize(required_size, '\0');
    }
    value.replace(static_cast<size_t>(offset), range_value.size(), range_value);
  } catch (const std::bad_alloc&) {
    return rocksdb::Status::InvalidArgument("string size is too large");
  } catch (const std::length_error&) {
    return rocksdb::Status::InvalidArgument("string size is too large");
  }

  KeyMetadata after = BuildStringMetadata(key_service_, lookup, false);
  after.size = value.size();
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = write_batch->Put(data_keyspace, key, value);
  if (!status.ok()) {
    return status;
  }
  status = key_service_->PutMetadata(write_batch.get(), key, lookup, after);
  if (!status.ok()) {
    return status;
  }
  status = write_batch->Commit();
  if (!status.ok()) {
    return status;
  }
  *new_length = static_cast<uint64_t>(value.size());
  return rocksdb::Status::OK();
}

rocksdb::Status StringModule::GetSetValue(const std::string& key,
                                          const std::string& value,
                                          std::string* old_value,
                                          bool* found) {
  if (old_value == nullptr) {
    return rocksdb::Status::InvalidArgument("string value output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("string found output is required");
  }
  old_value->clear();
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
  status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  if (lookup.exists) {
    status = ReadStoredValue(snapshot.get(), data_keyspace, key, old_value);
    if (!status.ok()) {
      return status;
    }
    *found = true;
  }

  KeyMetadata after = BuildStringMetadata(key_service_, lookup, true);
  after.size = value.size();
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = write_batch->Put(data_keyspace, key, value);
  if (!status.ok()) {
    return status;
  }
  status = key_service_->PutMetadata(write_batch.get(), key, lookup, after);
  if (!status.ok()) {
    return status;
  }
  return write_batch->Commit();
}

rocksdb::Status StringModule::IncrementBy(const std::string& key,
                                          int64_t increment,
                                          int64_t* new_value) {
  if (new_value == nullptr) {
    return rocksdb::Status::InvalidArgument("string integer output is required");
  }
  *new_value = 0;

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
  status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  int64_t current = 0;
  if (lookup.exists) {
    std::string value;
    status = ReadStoredValue(snapshot.get(), data_keyspace, key, &value);
    if (!status.ok()) {
      return status;
    }
    if (!ParseInt64Strict(value, &current)) {
      return rocksdb::Status::InvalidArgument(
          "value is not an integer or out of range");
    }
  }

  const __int128 candidate =
      static_cast<__int128>(current) + static_cast<__int128>(increment);
  status = CheckIntegerResult(candidate);
  if (!status.ok()) {
    return status;
  }
  const int64_t result = static_cast<int64_t>(candidate);
  const std::string stored = std::to_string(result);

  KeyMetadata after = BuildStringMetadata(key_service_, lookup, false);
  after.size = stored.size();
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = write_batch->Put(data_keyspace, key, stored);
  if (!status.ok()) {
    return status;
  }
  status = key_service_->PutMetadata(write_batch.get(), key, lookup, after);
  if (!status.ok()) {
    return status;
  }
  status = write_batch->Commit();
  if (!status.ok()) {
    return status;
  }
  *new_value = result;
  return rocksdb::Status::OK();
}

rocksdb::Status StringModule::DecrementBy(const std::string& key,
                                          int64_t decrement,
                                          int64_t* new_value) {
  if (new_value == nullptr) {
    return rocksdb::Status::InvalidArgument("string integer output is required");
  }
  *new_value = 0;

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
  status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  int64_t current = 0;
  if (lookup.exists) {
    std::string value;
    status = ReadStoredValue(snapshot.get(), data_keyspace, key, &value);
    if (!status.ok()) {
      return status;
    }
    if (!ParseInt64Strict(value, &current)) {
      return rocksdb::Status::InvalidArgument(
          "value is not an integer or out of range");
    }
  }

  const __int128 candidate =
      static_cast<__int128>(current) - static_cast<__int128>(decrement);
  status = CheckIntegerResult(candidate);
  if (!status.ok()) {
    return status;
  }
  const int64_t result = static_cast<int64_t>(candidate);
  const std::string stored = std::to_string(result);

  KeyMetadata after = BuildStringMetadata(key_service_, lookup, false);
  after.size = stored.size();
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  status = write_batch->Put(data_keyspace, key, stored);
  if (!status.ok()) {
    return status;
  }
  status = key_service_->PutMetadata(write_batch.get(), key, lookup, after);
  if (!status.ok()) {
    return status;
  }
  status = write_batch->Commit();
  if (!status.ok()) {
    return status;
  }
  *new_value = result;
  return rocksdb::Status::OK();
}

rocksdb::Status StringModule::Length(const std::string& key, uint64_t* length) {
  if (length == nullptr) {
    return rocksdb::Status::InvalidArgument("string length output is required");
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
  status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  *length = lookup.metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status StringModule::DeleteWholeKey(ModuleSnapshot* snapshot,
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

  rocksdb::Status status = RequireStringEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace data_keyspace = services_->storage().Keyspace("data");
  status = write_batch->Delete(data_keyspace, key);
  if (!status.ok()) {
    return status;
  }

  const KeyMetadata after = BuildStringTombstoneMetadata(key_service_, lookup);
  return key_service_->PutMetadata(write_batch, key, lookup, after);
}

rocksdb::Status StringModule::EnsureReady() const {
  if (services_ == nullptr || key_service_ == nullptr || !started_) {
    return rocksdb::Status::InvalidArgument("string module is unavailable");
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
