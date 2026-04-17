#pragma once

#include <cstdint>
#include <string>

#include "rocksdb/slice.h"

namespace minikv {

enum class ValueType : uint8_t {
  kHash = 1,
};

enum class ValueEncoding : uint8_t {
  kHashPlain = 1,
};

struct KeyMetadata {
  ValueType type = ValueType::kHash;
  ValueEncoding encoding = ValueEncoding::kHashPlain;
  uint64_t version = 1;
  uint64_t size = 0;
  uint64_t expire_at_ms = 0;
};

class KeyCodec {
 public:
  static std::string EncodeMetaKey(const std::string& user_key);
  static std::string EncodeHashDataPrefix(const std::string& user_key,
                                          uint64_t version);
  static std::string EncodeHashDataKey(const std::string& user_key,
                                       uint64_t version,
                                       const std::string& field);

  static std::string EncodeMetaValue(const KeyMetadata& metadata);
  static bool DecodeMetaValue(const rocksdb::Slice& value,
                              KeyMetadata* metadata);

  static bool StartsWith(const rocksdb::Slice& value,
                         const rocksdb::Slice& prefix);
  static bool ExtractFieldFromHashDataKey(const rocksdb::Slice& encoded_key,
                                          const rocksdb::Slice& prefix,
                                          std::string* field);

 private:
  static void AppendUint32(std::string* out, uint32_t value);
  static void AppendUint64(std::string* out, uint64_t value);
  static uint32_t DecodeUint32(const char* input);
  static uint64_t DecodeUint64(const char* input);
};

}  // namespace minikv
