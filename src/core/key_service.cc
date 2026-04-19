#include "core/key_service.h"

#include <chrono>

#include "storage/encoding/key_codec.h"
#include "runtime/module/module_services.h"

namespace minikv {

namespace {

constexpr size_t kEncodedMetadataSize = 26;

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

bool IsKnownObjectType(uint8_t value) {
  switch (static_cast<ObjectType>(value)) {
    case ObjectType::kString:
    case ObjectType::kHash:
    case ObjectType::kList:
    case ObjectType::kSet:
    case ObjectType::kZSet:
    case ObjectType::kStream:
      return true;
  }
  return false;
}

bool IsKnownObjectEncoding(uint8_t value) {
  switch (static_cast<ObjectEncoding>(value)) {
    case ObjectEncoding::kRaw:
    case ObjectEncoding::kHashPlain:
    case ObjectEncoding::kListQuicklist:
    case ObjectEncoding::kSetHashtable:
    case ObjectEncoding::kZSetSkiplist:
    case ObjectEncoding::kStreamRadixTree:
    case ObjectEncoding::kZSetGeo:
      return true;
  }
  return false;
}

rocksdb::Status InvalidMetadataStatus() {
  return rocksdb::Status::Corruption("invalid key metadata");
}

}  // namespace

DefaultCoreKeyService::DefaultCoreKeyService(TimeSource time_source)
    : time_source_(std::move(time_source)) {}

rocksdb::Status DefaultCoreKeyService::Lookup(ModuleSnapshot* snapshot,
                                              const std::string& key,
                                              KeyLookup* lookup) const {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (lookup == nullptr) {
    return rocksdb::Status::InvalidArgument("key lookup output is required");
  }

  *lookup = KeyLookup{};

  std::string raw_meta;
  rocksdb::Status status =
      snapshot->Get(StorageColumnFamily::kMeta, KeyCodec::EncodeMetaKey(key),
                    &raw_meta);
  if (status.IsNotFound()) {
    return rocksdb::Status::OK();
  }
  if (!status.ok()) {
    return status;
  }

  if (!DecodeMetadataValue(raw_meta, &lookup->metadata)) {
    return InvalidMetadataStatus();
  }

  lookup->found = true;
  if (IsLogicalDeleteExpireAt(lookup->metadata.expire_at_ms)) {
    lookup->state = KeyLifecycleState::kTombstone;
  } else if (IsExpiredAt(lookup->metadata.expire_at_ms, CurrentTimeMs())) {
    lookup->state = KeyLifecycleState::kExpired;
  } else {
    lookup->state = KeyLifecycleState::kLive;
  }
  lookup->expired = lookup->state == KeyLifecycleState::kExpired ||
                    lookup->state == KeyLifecycleState::kTombstone;
  lookup->exists = lookup->state == KeyLifecycleState::kLive;
  return rocksdb::Status::OK();
}

KeyMetadata DefaultCoreKeyService::MakeMetadata(ObjectType type,
                                                ObjectEncoding encoding,
                                                const KeyLookup& lookup) const {
  KeyMetadata metadata;
  metadata.type = type;
  metadata.encoding = encoding;
  metadata.version = (lookup.found && lookup.state != KeyLifecycleState::kLive)
                         ? lookup.metadata.version + 1
                         : 1;
  metadata.size = 0;
  metadata.expire_at_ms = 0;
  return metadata;
}

KeyMetadata DefaultCoreKeyService::MakeTombstoneMetadata(
    const KeyLookup& lookup) const {
  KeyMetadata metadata = lookup.metadata;
  metadata.expire_at_ms = kLogicalDeleteExpireAtMs;
  return metadata;
}

int64_t DefaultCoreKeyService::GetRemainingTtlMs(
    const KeyLookup& lookup) const {
  if (lookup.state != KeyLifecycleState::kLive) {
    return -2;
  }
  if (lookup.metadata.expire_at_ms == 0) {
    return -1;
  }

  const uint64_t now_ms = CurrentTimeMs();
  if (lookup.metadata.expire_at_ms <= now_ms) {
    return -2;
  }
  const uint64_t remaining_ms = lookup.metadata.expire_at_ms - now_ms;
  if (remaining_ms > static_cast<uint64_t>(INT64_MAX)) {
    return INT64_MAX;
  }
  return static_cast<int64_t>(remaining_ms);
}

uint64_t DefaultCoreKeyService::CurrentTimeMs() const {
  if (time_source_) {
    return time_source_();
  }
  return SystemCurrentTimeMs();
}

rocksdb::Status DefaultCoreKeyService::PutMetadata(
    ModuleWriteBatch* write_batch, const std::string& key,
    const KeyMetadata& metadata) const {
  if (write_batch == nullptr) {
    return rocksdb::Status::InvalidArgument("module write batch is unavailable");
  }
  return write_batch->Put(StorageColumnFamily::kMeta,
                          KeyCodec::EncodeMetaKey(key),
                          EncodeMetadataValue(metadata));
}

rocksdb::Status DefaultCoreKeyService::DeleteMetadata(
    ModuleWriteBatch* write_batch, const std::string& key) const {
  if (write_batch == nullptr) {
    return rocksdb::Status::InvalidArgument("module write batch is unavailable");
  }
  return write_batch->Delete(StorageColumnFamily::kMeta,
                             KeyCodec::EncodeMetaKey(key));
}

std::string DefaultCoreKeyService::ObjectTypeName(ObjectType type) const {
  switch (type) {
    case ObjectType::kString:
      return "string";
    case ObjectType::kHash:
      return "hash";
    case ObjectType::kList:
      return "list";
    case ObjectType::kSet:
      return "set";
    case ObjectType::kZSet:
      return "zset";
    case ObjectType::kStream:
      return "stream";
  }
  return "unknown";
}

std::string DefaultCoreKeyService::EncodeMetadataValue(
    const KeyMetadata& metadata) {
  std::string out;
  out.push_back(static_cast<char>(metadata.type));
  out.push_back(static_cast<char>(metadata.encoding));
  AppendUint64(&out, metadata.version);
  AppendUint64(&out, metadata.size);
  AppendUint64(&out, metadata.expire_at_ms);
  return out;
}

bool DefaultCoreKeyService::DecodeMetadataValue(const rocksdb::Slice& value,
                                                KeyMetadata* metadata) {
  if (metadata == nullptr || value.size() != kEncodedMetadataSize) {
    return false;
  }

  const auto type = static_cast<uint8_t>(value.data()[0]);
  const auto encoding = static_cast<uint8_t>(value.data()[1]);
  if (!IsKnownObjectType(type) || !IsKnownObjectEncoding(encoding)) {
    return false;
  }

  metadata->type = static_cast<ObjectType>(type);
  metadata->encoding = static_cast<ObjectEncoding>(encoding);
  metadata->version = DecodeUint64(value.data() + 2);
  metadata->size = DecodeUint64(value.data() + 10);
  metadata->expire_at_ms = DecodeUint64(value.data() + 18);
  return true;
}

bool DefaultCoreKeyService::IsLogicalDeleteExpireAt(uint64_t expire_at_ms) {
  return expire_at_ms == kLogicalDeleteExpireAtMs;
}

uint64_t DefaultCoreKeyService::SystemCurrentTimeMs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

bool DefaultCoreKeyService::IsExpiredAt(uint64_t expire_at_ms,
                                        uint64_t now_ms) {
  return expire_at_ms != 0 && expire_at_ms <= now_ms;
}

}  // namespace minikv
