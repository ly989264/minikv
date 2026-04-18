#pragma once

#include "rocksdb/status.h"

namespace minikv {

class HashObserver;

inline constexpr char kHashIndexingBridgeExportName[] = "indexing_bridge";
inline constexpr char kHashIndexingBridgeQualifiedExportName[] =
    "hash.indexing_bridge";

class HashIndexingBridge {
 public:
  virtual ~HashIndexingBridge() = default;

  virtual rocksdb::Status AddObserver(HashObserver* observer) = 0;
  virtual rocksdb::Status RemoveObserver(HashObserver* observer) = 0;
};

}  // namespace minikv
