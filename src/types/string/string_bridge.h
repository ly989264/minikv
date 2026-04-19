#pragma once

#include <cstdint>
#include <string>

#include "rocksdb/status.h"

namespace minikv {

inline constexpr char kStringBridgeExportName[] = "bridge";
inline constexpr char kStringBridgeQualifiedExportName[] = "string.bridge";

class StringBridge {
 public:
  virtual ~StringBridge() = default;

  virtual rocksdb::Status GetValue(const std::string& key, std::string* value,
                                   bool* found) = 0;
  virtual rocksdb::Status SetValue(const std::string& key,
                                   const std::string& value) = 0;
  virtual rocksdb::Status Length(const std::string& key, uint64_t* length) = 0;
};

}  // namespace minikv
