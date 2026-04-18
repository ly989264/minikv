#pragma once

#include "module/module.h"
#include "modules/core/key_service.h"

namespace minikv {

class CoreModule : public Module {
 public:
  std::string_view Name() const override { return "core"; }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
 void OnStop(ModuleServices& services) override;

 private:
  DefaultCoreKeyService key_service_;
  bool started_ = false;
};

}  // namespace minikv
