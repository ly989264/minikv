#pragma once

namespace rocksdb {
class Status;
}

namespace minikv {

class JsonModule;
class ModuleServices;

rocksdb::Status RegisterJsonCommands(ModuleServices& services,
                                     JsonModule* module);

}  // namespace minikv
