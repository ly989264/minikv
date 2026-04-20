#pragma once

namespace rocksdb {
class Status;
}

namespace minikv {

class GeoModule;
class ModuleServices;

rocksdb::Status RegisterGeoCommands(ModuleServices& services, GeoModule* module);

}  // namespace minikv
