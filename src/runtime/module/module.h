#pragma once

#include <string_view>

#include "storage/engine/storage_engine.h"
#include "rocksdb/status.h"

namespace minikv {

class ModuleServices;

class Module {
 public:
  virtual ~Module() = default;

  virtual std::string_view Name() const = 0;
  virtual StorageColumnFamily DefaultStorageColumnFamily() const {
    return StorageColumnFamily::kModule;
  }
  virtual rocksdb::Status OnLoad(ModuleServices& services) = 0;
  virtual rocksdb::Status OnStart(ModuleServices& services) = 0;
  virtual void OnStop(ModuleServices& services) = 0;
};

}  // namespace minikv
