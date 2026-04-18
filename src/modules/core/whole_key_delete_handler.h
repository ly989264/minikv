#pragma once

#include <string>

#include "modules/core/key_service.h"
#include "rocksdb/status.h"

namespace minikv {

class ModuleSnapshot;
class ModuleWriteBatch;

inline constexpr char kWholeKeyDeleteRegistryExportName[] =
    "whole_key_delete_registry";
inline constexpr char kWholeKeyDeleteRegistryQualifiedExportName[] =
    "core.whole_key_delete_registry";

class WholeKeyDeleteHandler {
 public:
  virtual ~WholeKeyDeleteHandler() = default;

  virtual ObjectType HandledType() const = 0;
  virtual rocksdb::Status DeleteWholeKey(ModuleSnapshot* snapshot,
                                         ModuleWriteBatch* write_batch,
                                         const std::string& key,
                                         const KeyLookup& lookup) = 0;
};

class WholeKeyDeleteRegistry {
 public:
  virtual ~WholeKeyDeleteRegistry() = default;

  virtual rocksdb::Status RegisterHandler(WholeKeyDeleteHandler* handler) = 0;
};

}  // namespace minikv
