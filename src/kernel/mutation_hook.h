#pragma once

#include <string>
#include <vector>

#include "codec/key_codec.h"
#include "modules/hash/hash_types.h"
#include "rocksdb/status.h"

namespace minikv {

class WriteContext;

struct HashMutation {
  enum class Type {
    kPutField,
    kDeleteFields,
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

class MutationHook {
 public:
  virtual ~MutationHook() = default;

  virtual rocksdb::Status OnHashMutation(const HashMutation& mutation,
                                         WriteContext* write_context) = 0;
};

class NoopMutationHook : public MutationHook {
 public:
  rocksdb::Status OnHashMutation(const HashMutation&,
                                 WriteContext*) override {
    return rocksdb::Status::OK();
  }
};

}  // namespace minikv
