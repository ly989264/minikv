#pragma once

#include <map>

#include "module/module.h"
#include "modules/core/key_service.h"
#include "modules/core/whole_key_delete_handler.h"

namespace minikv {

class CoreModule : public Module, public WholeKeyDeleteRegistry {
 public:
  std::string_view Name() const override { return "core"; }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

  rocksdb::Status RegisterHandler(WholeKeyDeleteHandler* handler) override;
  WholeKeyDeleteHandler* FindHandler(ObjectType type) const;

 private:
  DefaultCoreKeyService key_service_;
  std::map<ObjectType, WholeKeyDeleteHandler*> delete_handlers_;
  bool started_ = false;
};

}  // namespace minikv
