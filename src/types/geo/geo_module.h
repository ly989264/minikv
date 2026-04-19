#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "runtime/module/module.h"
#include "types/zset/zset_bridge.h"

namespace minikv {

class CoreKeyService;

struct GeoLocation {
  std::string member;
  double longitude = 0;
  double latitude = 0;
};

struct GeoCoordinates {
  double longitude = 0;
  double latitude = 0;
};

enum class GeoSearchSort {
  kNone,
  kAsc,
  kDesc,
};

struct GeoSearchQuery {
  enum class OriginType {
    kFromLonLat,
    kFromMember,
  };

  enum class ShapeType {
    kByRadius,
    kByBox,
  };

  OriginType origin_type = OriginType::kFromLonLat;
  ShapeType shape_type = ShapeType::kByRadius;
  GeoCoordinates origin;
  std::string from_member;
  double radius = 0;
  double box_width = 0;
  double box_height = 0;
  double unit_to_meters = 1;
  GeoSearchSort sort = GeoSearchSort::kNone;
  uint64_t count = 0;
  bool with_dist = false;
  bool with_hash = false;
  bool with_coord = false;
};

struct GeoSearchMatch {
  std::string member;
  GeoCoordinates coordinates;
  double distance_meters = 0;
  uint64_t raw_hash = 0;
};

class GeoModule : public Module, public ZSetObserver {
 public:
  std::string_view Name() const override { return "geo"; }
  rocksdb::Status OnLoad(ModuleServices& services) override;
  rocksdb::Status OnStart(ModuleServices& services) override;
  void OnStop(ModuleServices& services) override;

  rocksdb::Status AddLocations(const std::string& key,
                               const std::vector<GeoLocation>& locations,
                               uint64_t* added_count);
  rocksdb::Status Position(const std::string& key, const std::string& member,
                           GeoCoordinates* coordinates, bool* found);
  rocksdb::Status Hash(const std::string& key, const std::string& member,
                       std::string* geohash, bool* found);
  rocksdb::Status Distance(const std::string& key, const std::string& member_a,
                           const std::string& member_b, double unit_to_meters,
                           double* distance, bool* found);
  rocksdb::Status Search(const std::string& key, const GeoSearchQuery& query,
                         std::vector<GeoSearchMatch>* matches);

  rocksdb::Status OnZSetMutation(const ZSetMutation& mutation,
                                 ModuleSnapshot* snapshot,
                                 ModuleWriteBatch* write_batch) override;

 private:
  rocksdb::Status EnsureReady() const;

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  ZSetBridge* zset_bridge_ = nullptr;
  bool started_ = false;
};

}  // namespace minikv
