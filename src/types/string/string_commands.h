#pragma once

namespace rocksdb {
class Status;
}

namespace minikv {

class ModuleServices;
class StringModule;

rocksdb::Status RegisterStringCommands(ModuleServices& services,
                                       StringModule* module);

}  // namespace minikv
