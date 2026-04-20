#pragma once

namespace rocksdb {
class Status;
}

namespace minikv {

class ModuleServices;
class SetModule;

rocksdb::Status RegisterSetCommands(ModuleServices& services,
                                    SetModule* module);

}  // namespace minikv
