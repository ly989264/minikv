#include "types/geo/geo_module.h"

#include <memory>
#include <utility>
#include <vector>

#include "core/key_service.h"
#include "runtime/module/module_services.h"
#include "types/geo/geo_commands.h"
#include "types/geo/geo_internal.h"

namespace minikv {

rocksdb::Status GeoModule::OnLoad(ModuleServices& services) {
  services_ = &services;
  return RegisterGeoCommands(services, this);
}

rocksdb::Status GeoModule::OnStart(ModuleServices& services) {
  key_service_ = services.exports().Find<CoreKeyService>(
      kCoreKeyServiceQualifiedExportName);
  if (key_service_ == nullptr) {
    return rocksdb::Status::InvalidArgument("core key service is unavailable");
  }

  zset_bridge_ = services.exports().Find<ZSetBridge>(
      kZSetBridgeQualifiedExportName);
  if (zset_bridge_ == nullptr) {
    return rocksdb::Status::InvalidArgument("zset bridge is unavailable");
  }

  rocksdb::Status status = zset_bridge_->AddObserver(this);
  if (!status.ok()) {
    return status;
  }

  started_ = true;
  services.metrics().SetCounter("worker_count",
                                services.scheduler().worker_count());
  return rocksdb::Status::OK();
}

void GeoModule::OnStop(ModuleServices& /*services*/) {
  if (zset_bridge_ != nullptr) {
    zset_bridge_->RemoveObserver(this);
  }
  started_ = false;
  zset_bridge_ = nullptr;
  key_service_ = nullptr;
  services_ = nullptr;
}

rocksdb::Status GeoModule::AddLocations(const std::string& key,
                                        const std::vector<GeoLocation>& locations,
                                        uint64_t* added_count) {
  if (added_count != nullptr) {
    *added_count = 0;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  const std::vector<GeoLocation> unique_locations =
      CollapseLocationsByMember(locations);
  if (unique_locations.empty()) {
    return rocksdb::Status::OK();
  }

  std::vector<ZSetEntry> entries;
  entries.reserve(unique_locations.size());
  for (const auto& location : unique_locations) {
    double score = 0;
    if (!EncodeGeoScore(location.longitude, location.latitude, nullptr, &score)) {
      return rocksdb::Status::InvalidArgument(
          "GEOADD requires valid longitude/latitude pairs");
    }
    entries.push_back(ZSetEntry{location.member, score});
  }

  return zset_bridge_->AddMembersWithEncoding(key, entries, ObjectEncoding::kZSetGeo,
                                              added_count);
}

rocksdb::Status GeoModule::Position(const std::string& key,
                                    const std::string& member,
                                    GeoCoordinates* coordinates, bool* found) {
  if (coordinates == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "geo coordinates output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("geo found output is required");
  }
  *coordinates = GeoCoordinates{};
  *found = false;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = LookupGeoKey(key_service_, snapshot.get(), key, &lookup);
  if (!status.ok()) {
    if (status.IsInvalidArgument()) {
      return status;
    }
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace geo_keyspace = services_->storage().Keyspace("members");
  GeoStoredPoint point;
  status = ReadGeoPoint(snapshot.get(), geo_keyspace, key, lookup.metadata.version,
                        member, &point, found);
  if (!status.ok() || !*found) {
    return status;
  }
  *coordinates = point.coordinates;
  return rocksdb::Status::OK();
}

rocksdb::Status GeoModule::Hash(const std::string& key, const std::string& member,
                                std::string* geohash, bool* found) {
  if (geohash == nullptr) {
    return rocksdb::Status::InvalidArgument("geo hash output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("geo found output is required");
  }
  geohash->clear();
  *found = false;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = LookupGeoKey(key_service_, snapshot.get(), key, &lookup);
  if (!status.ok()) {
    if (status.IsInvalidArgument()) {
      return status;
    }
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace geo_keyspace = services_->storage().Keyspace("members");
  GeoStoredPoint point;
  status = ReadGeoPoint(snapshot.get(), geo_keyspace, key, lookup.metadata.version,
                        member, &point, found);
  if (!status.ok() || !*found) {
    return status;
  }
  *geohash = EncodeStandardGeohashString(point.coordinates);
  *found = !geohash->empty();
  return rocksdb::Status::OK();
}

rocksdb::Status GeoModule::Distance(const std::string& key,
                                    const std::string& member_a,
                                    const std::string& member_b,
                                    double unit_to_meters, double* distance,
                                    bool* found) {
  if (distance == nullptr) {
    return rocksdb::Status::InvalidArgument("geo distance output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("geo found output is required");
  }
  *distance = 0;
  *found = false;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }
  if (unit_to_meters <= 0) {
    return rocksdb::Status::InvalidArgument("geo unit conversion is invalid");
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = LookupGeoKey(key_service_, snapshot.get(), key, &lookup);
  if (!status.ok()) {
    if (status.IsInvalidArgument()) {
      return status;
    }
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace geo_keyspace = services_->storage().Keyspace("members");
  GeoStoredPoint point_a;
  GeoStoredPoint point_b;
  bool found_a = false;
  bool found_b = false;
  status = ReadGeoPoint(snapshot.get(), geo_keyspace, key, lookup.metadata.version,
                        member_a, &point_a, &found_a);
  if (!status.ok()) {
    return status;
  }
  status = ReadGeoPoint(snapshot.get(), geo_keyspace, key, lookup.metadata.version,
                        member_b, &point_b, &found_b);
  if (!status.ok()) {
    return status;
  }
  if (!found_a || !found_b) {
    return rocksdb::Status::OK();
  }

  *distance = GeoDistanceMeters(point_a.coordinates, point_b.coordinates) /
              unit_to_meters;
  *found = true;
  return rocksdb::Status::OK();
}

rocksdb::Status GeoModule::Search(const std::string& key,
                                  const GeoSearchQuery& query,
                                  std::vector<GeoSearchMatch>* matches) {
  if (matches == nullptr) {
    return rocksdb::Status::InvalidArgument("geo matches output is required");
  }
  matches->clear();

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = LookupGeoKey(key_service_, snapshot.get(), key, &lookup);
  if (!status.ok()) {
    if (status.IsInvalidArgument()) {
      return status;
    }
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  GeoCoordinates origin = query.origin;
  if (query.origin_type == GeoSearchQuery::OriginType::kFromMember) {
    const ModuleKeyspace geo_keyspace = services_->storage().Keyspace("members");
    GeoStoredPoint origin_point;
    bool found = false;
    status = ReadGeoPoint(snapshot.get(), geo_keyspace, key, lookup.metadata.version,
                          query.from_member, &origin_point, &found);
    if (!status.ok()) {
      return status;
    }
    if (!found) {
      return rocksdb::Status::InvalidArgument(
          "could not decode requested zset member");
    }
    origin = origin_point.coordinates;
  }

  std::vector<GeoStoredPoint> points;
  status = CollectGeoPoints(snapshot.get(), services_->storage().Keyspace("members"),
                            key, lookup.metadata.version, &points);
  if (!status.ok()) {
    return status;
  }

  matches->reserve(points.size());
  const double radius_meters = query.radius * query.unit_to_meters;
  const double width_meters = query.box_width * query.unit_to_meters;
  const double height_meters = query.box_height * query.unit_to_meters;
  for (const auto& point : points) {
    double distance_meters = 0;
    bool in_shape = false;
    if (query.shape_type == GeoSearchQuery::ShapeType::kByRadius) {
      in_shape = PointInRadius(origin, point.coordinates, radius_meters,
                               &distance_meters);
    } else {
      in_shape = PointInBox(origin, point.coordinates, width_meters,
                            height_meters, &distance_meters);
    }
    if (!in_shape) {
      continue;
    }

    matches->push_back(
        GeoSearchMatch{point.member, point.coordinates, distance_meters,
                       point.raw_hash});
    if (query.sort == GeoSearchSort::kNone && query.count != 0 &&
        matches->size() >= query.count) {
      break;
    }
  }

  if (query.sort != GeoSearchSort::kNone) {
    std::sort(matches->begin(), matches->end(),
              [sort = query.sort](const GeoSearchMatch& lhs,
                                  const GeoSearchMatch& rhs) {
                if (lhs.distance_meters == rhs.distance_meters) {
                  return lhs.member < rhs.member;
                }
                if (sort == GeoSearchSort::kAsc) {
                  return lhs.distance_meters < rhs.distance_meters;
                }
                return lhs.distance_meters > rhs.distance_meters;
              });
    if (query.count != 0 && matches->size() > query.count) {
      matches->resize(query.count);
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status GeoModule::OnZSetMutation(const ZSetMutation& mutation,
                                          ModuleSnapshot* /*snapshot*/,
                                          ModuleWriteBatch* write_batch) {
  if (write_batch == nullptr) {
    return rocksdb::Status::InvalidArgument("module write batch is unavailable");
  }
  if (services_ == nullptr) {
    return rocksdb::Status::InvalidArgument("geo module is unavailable");
  }

  const ModuleKeyspace geo_keyspace = services_->storage().Keyspace("members");
  if ((mutation.type == ZSetMutation::Type::kRemoveMembers ||
       mutation.type == ZSetMutation::Type::kDeleteKey) &&
      mutation.existed_before &&
      mutation.before.encoding == ObjectEncoding::kZSetGeo) {
    for (const std::string& member : mutation.removed_members) {
      rocksdb::Status status = write_batch->Delete(
          geo_keyspace,
          EncodeGeoMemberKey(mutation.key, mutation.before.version, member));
      if (!status.ok()) {
        return status;
      }
    }
  }

  if (mutation.type != ZSetMutation::Type::kUpsertMembers ||
      !mutation.exists_after ||
      mutation.after.encoding != ObjectEncoding::kZSetGeo) {
    return rocksdb::Status::OK();
  }

  for (const auto& entry : mutation.upserted_entries) {
    GeoCoordinates coordinates;
    uint64_t raw_hash = 0;
    const std::string local_key =
        EncodeGeoMemberKey(mutation.key, mutation.after.version, entry.member);
    if (!DecodeGeoScore(entry.score, &raw_hash, &coordinates)) {
      rocksdb::Status status = write_batch->Delete(geo_keyspace, local_key);
      if (!status.ok()) {
        return status;
      }
      continue;
    }

    GeoStoredPoint point;
    point.coordinates = coordinates;
    point.raw_hash = raw_hash;
    rocksdb::Status status =
        write_batch->Put(geo_keyspace, local_key, EncodeGeoValue(point));
    if (!status.ok()) {
      return status;
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status GeoModule::EnsureReady() const {
  if (services_ == nullptr || key_service_ == nullptr || zset_bridge_ == nullptr ||
      !started_) {
    return rocksdb::Status::InvalidArgument("geo module is unavailable");
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
