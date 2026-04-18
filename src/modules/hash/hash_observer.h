#pragma once

#include <string>
#include <vector>

#include "modules/core/key_service.h"
#include "modules/hash/hash_types.h"
#include "rocksdb/status.h"

namespace minikv {

class ModuleWriteBatch;

struct HashMutation {
  enum class Type {
    kPutField,
    kDeleteFields,
    kDeleteKey,
  };

  Type type = Type::kPutField;
  std::string key;
  std::vector<FieldValue> values;
  std::vector<std::string> deleted_fields;
  KeyMetadata before;
  KeyMetadata after;
  bool existed_before = false;
  bool exists_after = false;
};

class HashObserver {
 public:
  virtual ~HashObserver() = default;

  virtual rocksdb::Status OnHashMutation(const HashMutation& mutation,
                                         ModuleWriteBatch* write_batch) = 0;
};

}  // namespace minikv
