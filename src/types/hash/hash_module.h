#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/status.h"

namespace minikv {

class MutationHook;
class StorageEngine;

struct FieldValue {
  std::string field;
  std::string value;
};

class HashModule {
 public:
  HashModule(StorageEngine* storage_engine, MutationHook* mutation_hook);

  rocksdb::Status PutField(const std::string& key, const std::string& field,
                           const std::string& value, bool* inserted);
  rocksdb::Status ReadAll(const std::string& key,
                          std::vector<FieldValue>* out);
  rocksdb::Status DeleteFields(const std::string& key,
                               const std::vector<std::string>& fields,
                               uint64_t* deleted);

 private:
  StorageEngine* storage_engine_;
  MutationHook* mutation_hook_;
};

}  // namespace minikv
