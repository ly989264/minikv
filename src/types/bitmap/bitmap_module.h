#pragma once

#include <cstdint>
#include <string>

#include "runtime/module/module.h"

namespace minikv {

class ModuleServices;
class StringBridge;

class BitmapModule : public Module {
 public:
  std::string_view Name() const override { return "bitmap"; }
  StorageColumnFamily DefaultStorageColumnFamily() const override {
    return StorageColumnFamily::kString;
  }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

  rocksdb::Status GetBit(const std::string& key, uint64_t offset, int* bit);
  rocksdb::Status SetBit(const std::string& key, uint64_t offset, int bit,
                         int* old_bit);
  rocksdb::Status CountBits(const std::string& key, uint64_t* count);

 private:
  rocksdb::Status EnsureReady() const;

  ModuleServices* services_ = nullptr;
  StringBridge* string_bridge_ = nullptr;
  bool started_ = false;
};

}  // namespace minikv
