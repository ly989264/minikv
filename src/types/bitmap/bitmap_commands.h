#pragma once

namespace rocksdb {
class Status;
}

namespace minikv {

class BitmapModule;
class ModuleServices;

rocksdb::Status RegisterBitmapCommands(ModuleServices& services,
                                       BitmapModule* module);

}  // namespace minikv
