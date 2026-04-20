#pragma once

namespace rocksdb {
class Status;
}

namespace minikv {

class ModuleServices;
class ZSetModule;

rocksdb::Status RegisterZSetCommands(ModuleServices& services,
                                     ZSetModule* module);

}  // namespace minikv
