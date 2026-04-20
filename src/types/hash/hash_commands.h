#pragma once

namespace rocksdb {
class Status;
}

namespace minikv {

class HashModule;
class ModuleServices;

rocksdb::Status RegisterHashCommands(ModuleServices& services, HashModule* module);

}  // namespace minikv
