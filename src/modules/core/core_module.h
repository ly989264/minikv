#pragma once

#include "module/module.h"

namespace minikv {

class CoreModule : public Module {
 public:
  std::string_view Name() const override { return "core"; }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

 private:
  bool started_ = false;
};

}  // namespace minikv
