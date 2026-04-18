#pragma once

#include <cstdint>
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
};

struct KeyMetadata {
  ObjectType type = ObjectType::kString;
  ObjectEncoding encoding = ObjectEncoding::kRaw;
  uint64_t version = 1;
  uint64_t size = 0;
  uint64_t expire_at_ms = 0;
};

struct KeyLookup {
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
  virtual rocksdb::Status PutMetadata(ModuleWriteBatch* write_batch,
                                      const std::string& key,
                                      const KeyMetadata& metadata) const = 0;
  virtual rocksdb::Status DeleteMetadata(ModuleWriteBatch* write_batch,
                                         const std::string& key) const = 0;
  virtual std::string ObjectTypeName(ObjectType type) const = 0;
};

class DefaultCoreKeyService final : public CoreKeyService {
 public:
  rocksdb::Status Lookup(ModuleSnapshot* snapshot, const std::string& key,
                         KeyLookup* lookup) const override;
  KeyMetadata MakeMetadata(ObjectType type, ObjectEncoding encoding,
                           const KeyLookup& lookup) const override;
  rocksdb::Status PutMetadata(ModuleWriteBatch* write_batch,
                              const std::string& key,
                              const KeyMetadata& metadata) const override;
  rocksdb::Status DeleteMetadata(ModuleWriteBatch* write_batch,
                                 const std::string& key) const override;
  std::string ObjectTypeName(ObjectType type) const override;

  static std::string EncodeMetadataValue(const KeyMetadata& metadata);
  static bool DecodeMetadataValue(const rocksdb::Slice& value,
                                  KeyMetadata* metadata);

 private:
  static uint64_t CurrentTimeMs();
  static bool IsExpiredAt(uint64_t expire_at_ms, uint64_t now_ms);
};

}  // namespace minikv
