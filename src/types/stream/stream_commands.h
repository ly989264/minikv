#pragma once

#include "runtime/module/module_services.h"

namespace minikv {

class StreamModule;

rocksdb::Status RegisterStreamCommands(ModuleServices& services,
                                       StreamModule* module);

}  // namespace minikv
