#pragma once

namespace rocksdb {
class Status;
}

namespace minikv {

class ListModule;
class ModuleServices;

rocksdb::Status RegisterListCommands(ModuleServices& services,
                                     ListModule* module);

}  // namespace minikv
