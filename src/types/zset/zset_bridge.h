#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "core/key_service.h"
#include "rocksdb/status.h"

namespace minikv {

class ModuleSnapshot;
class ModuleWriteBatch;

struct ZSetEntry {
  std::string member;
  double score = 0;
};

struct ZSetMutation {
  enum class Type {
    kUpsertMembers,
    kRemoveMembers,
    kDeleteKey,
  };

  Type type = Type::kUpsertMembers;
  std::string key;
  std::vector<ZSetEntry> upserted_entries;
  std::vector<std::string> removed_members;
  KeyMetadata before;
  KeyMetadata after;
  bool existed_before = false;
  bool exists_after = false;
};

class ZSetObserver {
 public:
  virtual ~ZSetObserver() = default;

  virtual rocksdb::Status OnZSetMutation(const ZSetMutation& mutation,
                                         ModuleSnapshot* snapshot,
                                         ModuleWriteBatch* write_batch) = 0;
};

inline constexpr char kZSetBridgeExportName[] = "bridge";
inline constexpr char kZSetBridgeQualifiedExportName[] = "zset.bridge";

class ZSetBridge {
 public:
  virtual ~ZSetBridge() = default;

  virtual rocksdb::Status AddMembersWithEncoding(
      const std::string& key, const std::vector<ZSetEntry>& entries,
      ObjectEncoding encoding, uint64_t* added_count) = 0;
  virtual rocksdb::Status AddObserver(ZSetObserver* observer) = 0;
  virtual rocksdb::Status RemoveObserver(ZSetObserver* observer) = 0;
};

}  // namespace minikv
