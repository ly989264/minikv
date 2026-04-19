#pragma once

#include <cstdint>
#include <functional>
#include <string>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace minikv {

class ModuleSnapshot;
class ModuleWriteBatch;

inline constexpr char kCoreKeyServiceExportName[] = "key_service";
inline constexpr char kCoreKeyServiceQualifiedExportName[] = "core.key_service";

enum class ObjectType : uint8_t {
  kString = 1,
  kHash = 2,
  kList = 3,
  kSet = 4,
  kZSet = 5,
  kStream = 6,
};

enum class ObjectEncoding : uint8_t {
  kRaw = 1,
  kHashPlain = 2,
  kListQuicklist = 3,
  kSetHashtable = 4,
  kZSetSkiplist = 5,
  kStreamRadixTree = 6,
  kZSetGeo = 7,
};

struct KeyMetadata {
  ObjectType type = ObjectType::kString;
  ObjectEncoding encoding = ObjectEncoding::kRaw;
  uint64_t version = 1;
  uint64_t size = 0;
  uint64_t expire_at_ms = 0;
};

inline constexpr uint64_t kLogicalDeleteExpireAtMs = 1;

enum class KeyLifecycleState : uint8_t {
  kMissing = 0,
  kLive = 1,
  kExpired = 2,
  kTombstone = 3,
};

struct KeyLookup {
  KeyLifecycleState state = KeyLifecycleState::kMissing;
  bool found = false;
  bool expired = false;
  bool exists = false;
  KeyMetadata metadata;
};

class CoreKeyService {
 public:
  virtual ~CoreKeyService() = default;

  virtual rocksdb::Status Lookup(ModuleSnapshot* snapshot,
                                 const std::string& key,
                                 KeyLookup* lookup) const = 0;
  virtual KeyMetadata MakeMetadata(ObjectType type, ObjectEncoding encoding,
                                   const KeyLookup& lookup) const = 0;
  virtual KeyMetadata MakeTombstoneMetadata(
      const KeyLookup& lookup) const = 0;
  virtual int64_t GetRemainingTtlMs(const KeyLookup& lookup) const = 0;
  virtual uint64_t CurrentTimeMs() const = 0;
  virtual rocksdb::Status PutMetadata(ModuleWriteBatch* write_batch,
                                      const std::string& key,
                                      const KeyMetadata& metadata) const = 0;
  virtual rocksdb::Status DeleteMetadata(ModuleWriteBatch* write_batch,
                                         const std::string& key) const = 0;
  virtual std::string ObjectTypeName(ObjectType type) const = 0;
};

class DefaultCoreKeyService final : public CoreKeyService {
 public:
  using TimeSource = std::function<uint64_t()>;

  explicit DefaultCoreKeyService(TimeSource time_source = {});

  rocksdb::Status Lookup(ModuleSnapshot* snapshot, const std::string& key,
                         KeyLookup* lookup) const override;
  KeyMetadata MakeMetadata(ObjectType type, ObjectEncoding encoding,
                           const KeyLookup& lookup) const override;
  KeyMetadata MakeTombstoneMetadata(const KeyLookup& lookup) const override;
  int64_t GetRemainingTtlMs(const KeyLookup& lookup) const override;
  uint64_t CurrentTimeMs() const override;
  rocksdb::Status PutMetadata(ModuleWriteBatch* write_batch,
                              const std::string& key,
                              const KeyMetadata& metadata) const override;
  rocksdb::Status DeleteMetadata(ModuleWriteBatch* write_batch,
                                 const std::string& key) const override;
  std::string ObjectTypeName(ObjectType type) const override;

  static std::string EncodeMetadataValue(const KeyMetadata& metadata);
  static bool DecodeMetadataValue(const rocksdb::Slice& value,
                                  KeyMetadata* metadata);
  static bool IsLogicalDeleteExpireAt(uint64_t expire_at_ms);

 private:
  static uint64_t SystemCurrentTimeMs();
  static bool IsExpiredAt(uint64_t expire_at_ms, uint64_t now_ms);

  TimeSource time_source_;
};

}  // namespace minikv
