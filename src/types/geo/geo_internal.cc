#include "types/geo/geo_internal.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <utility>

#include "core/key_service.h"
#include "storage/encoding/key_codec.h"

namespace minikv {

namespace {

constexpr uint8_t kGeoStepMax = 26;
constexpr double kGeoLongitudeMin = -180.0;
constexpr double kGeoLongitudeMax = 180.0;
constexpr double kGeoLatitudeMin = -85.05112878;
constexpr double kGeoLatitudeMax = 85.05112878;
constexpr double kStandardLatitudeMin = -90.0;
constexpr double kStandardLatitudeMax = 90.0;
constexpr double kEarthRadiusMeters = 6372797.560856;
constexpr char kGeoAlphabet[] = "0123456789bcdefghjkmnpqrstuvwxyz";

double DegreesToRadians(double degrees) {
  return degrees * (3.14159265358979323846 / 180.0);
}

double LatitudeDistanceMeters(double latitude_a, double latitude_b) {
  return kEarthRadiusMeters *
         std::fabs(DegreesToRadians(latitude_b) - DegreesToRadians(latitude_a));
}

}  // namespace

void AppendUint32(std::string* out, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

void AppendUint64(std::string* out, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

uint64_t DecodeUint64(const char* input) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value = (value << 8) | static_cast<unsigned char>(input[i]);
  }
  return value;
}

double CanonicalizeZero(double value) { return value == 0 ? 0.0 : value; }

uint64_t DoubleToBits(double value) {
  uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

double BitsToDouble(uint64_t bits) {
  double value = 0;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

bool IsValidLongitudeLatitude(double longitude, double latitude) {
  return longitude >= kGeoLongitudeMin && longitude <= kGeoLongitudeMax &&
         latitude >= kGeoLatitudeMin && latitude <= kGeoLatitudeMax;
}

uint64_t Interleave64(uint32_t xlo, uint32_t ylo) {
  static const uint64_t kMasks[] = {
      0x5555555555555555ULL, 0x3333333333333333ULL, 0x0F0F0F0F0F0F0F0FULL,
      0x00FF00FF00FF00FFULL, 0x0000FFFF0000FFFFULL};
  static const unsigned int kShifts[] = {1, 2, 4, 8, 16};

  uint64_t x = xlo;
  uint64_t y = ylo;
  x = (x | (x << kShifts[4])) & kMasks[4];
  y = (y | (y << kShifts[4])) & kMasks[4];
  x = (x | (x << kShifts[3])) & kMasks[3];
  y = (y | (y << kShifts[3])) & kMasks[3];
  x = (x | (x << kShifts[2])) & kMasks[2];
  y = (y | (y << kShifts[2])) & kMasks[2];
  x = (x | (x << kShifts[1])) & kMasks[1];
  y = (y | (y << kShifts[1])) & kMasks[1];
  x = (x | (x << kShifts[0])) & kMasks[0];
  y = (y | (y << kShifts[0])) & kMasks[0];
  return x | (y << 1);
}

uint64_t Deinterleave64(uint64_t interleaved) {
  static const uint64_t kMasks[] = {
      0x5555555555555555ULL, 0x3333333333333333ULL, 0x0F0F0F0F0F0F0F0FULL,
      0x00FF00FF00FF00FFULL, 0x0000FFFF0000FFFFULL,
      0x00000000FFFFFFFFULL};
  static const unsigned int kShifts[] = {0, 1, 2, 4, 8, 16};

  uint64_t x = interleaved;
  uint64_t y = interleaved >> 1;
  x = (x | (x >> kShifts[0])) & kMasks[0];
  y = (y | (y >> kShifts[0])) & kMasks[0];
  x = (x | (x >> kShifts[1])) & kMasks[1];
  y = (y | (y >> kShifts[1])) & kMasks[1];
  x = (x | (x >> kShifts[2])) & kMasks[2];
  y = (y | (y >> kShifts[2])) & kMasks[2];
  x = (x | (x >> kShifts[3])) & kMasks[3];
  y = (y | (y >> kShifts[3])) & kMasks[3];
  x = (x | (x >> kShifts[4])) & kMasks[4];
  y = (y | (y >> kShifts[4])) & kMasks[4];
  x = (x | (x >> kShifts[5])) & kMasks[5];
  y = (y | (y >> kShifts[5])) & kMasks[5];
  return x | (y << 32);
}

bool EncodeGeoHashBits(double longitude, double latitude, double longitude_min,
                       double longitude_max, double latitude_min,
                       double latitude_max, uint8_t step, uint64_t* bits) {
  if (bits == nullptr || step == 0 || step > 32) {
    return false;
  }
  if (longitude < longitude_min || longitude > longitude_max ||
      latitude < latitude_min || latitude > latitude_max) {
    return false;
  }

  const double lat_offset =
      (latitude - latitude_min) / (latitude_max - latitude_min);
  const double lon_offset =
      (longitude - longitude_min) / (longitude_max - longitude_min);
  const uint32_t ilat =
      static_cast<uint32_t>(lat_offset * static_cast<double>(1ULL << step));
  const uint32_t ilon =
      static_cast<uint32_t>(lon_offset * static_cast<double>(1ULL << step));
  *bits = Interleave64(ilat, ilon);
  return true;
}

bool DecodeGeoHashBits(uint64_t bits, double longitude_min,
                       double longitude_max, double latitude_min,
                       double latitude_max, uint8_t step,
                       GeoCoordinates* coordinates) {
  if (coordinates == nullptr || step == 0 || step > 32) {
    return false;
  }

  const uint64_t separated = Deinterleave64(bits);
  const uint32_t ilat = static_cast<uint32_t>(separated);
  const uint32_t ilon = static_cast<uint32_t>(separated >> 32);
  const double lat_scale = latitude_max - latitude_min;
  const double lon_scale = longitude_max - longitude_min;
  const double latitude_floor =
      latitude_min +
      (static_cast<double>(ilat) / static_cast<double>(1ULL << step)) * lat_scale;
  const double latitude_ceil =
      latitude_min +
      (static_cast<double>(ilat + 1) / static_cast<double>(1ULL << step)) *
          lat_scale;
  const double longitude_floor =
      longitude_min +
      (static_cast<double>(ilon) / static_cast<double>(1ULL << step)) * lon_scale;
  const double longitude_ceil =
      longitude_min +
      (static_cast<double>(ilon + 1) / static_cast<double>(1ULL << step)) *
          lon_scale;

  coordinates->longitude =
      std::clamp((longitude_floor + longitude_ceil) / 2.0, kGeoLongitudeMin,
                 kGeoLongitudeMax);
  coordinates->latitude =
      std::clamp((latitude_floor + latitude_ceil) / 2.0, kGeoLatitudeMin,
                 kGeoLatitudeMax);
  return true;
}

bool EncodeGeoScore(double longitude, double latitude, uint64_t* raw_hash,
                    double* score) {
  if (!IsValidLongitudeLatitude(longitude, latitude)) {
    return false;
  }
  uint64_t bits = 0;
  if (!EncodeGeoHashBits(longitude, latitude, kGeoLongitudeMin,
                         kGeoLongitudeMax, kGeoLatitudeMin, kGeoLatitudeMax,
                         kGeoStepMax, &bits)) {
    return false;
  }
  if (raw_hash != nullptr) {
    *raw_hash = bits;
  }
  if (score != nullptr) {
    *score = static_cast<double>(bits);
  }
  return true;
}

bool DecodeGeoScore(double score, uint64_t* raw_hash,
                    GeoCoordinates* coordinates) {
  if (!std::isfinite(score) || score < 0) {
    return false;
  }
  if (score > static_cast<double>(std::numeric_limits<uint64_t>::max())) {
    return false;
  }

  const uint64_t bits =
      static_cast<uint64_t>(CanonicalizeZero(score)) &
      ((1ULL << (kGeoStepMax * 2)) - 1);
  if (!DecodeGeoHashBits(bits, kGeoLongitudeMin, kGeoLongitudeMax,
                         kGeoLatitudeMin, kGeoLatitudeMax, kGeoStepMax,
                         coordinates)) {
    return false;
  }
  if (raw_hash != nullptr) {
    *raw_hash = bits;
  }
  return true;
}

std::string EncodeStandardGeohashString(const GeoCoordinates& coordinates) {
  uint64_t bits = 0;
  if (!EncodeGeoHashBits(coordinates.longitude, coordinates.latitude,
                         kGeoLongitudeMin, kGeoLongitudeMax,
                         kStandardLatitudeMin, kStandardLatitudeMax,
                         kGeoStepMax, &bits)) {
    return std::string();
  }

  std::string out;
  out.reserve(11);
  for (int index = 0; index < 11; ++index) {
    const uint64_t group =
        index == 10 ? 0 : ((bits >> (52 - ((index + 1) * 5))) & 0x1f);
    out.push_back(kGeoAlphabet[group]);
  }
  return out;
}

double GeoDistanceMeters(const GeoCoordinates& a, const GeoCoordinates& b) {
  const double lon_a = DegreesToRadians(a.longitude);
  const double lon_b = DegreesToRadians(b.longitude);
  const double v = std::sin((lon_b - lon_a) / 2.0);
  if (v == 0.0) {
    return LatitudeDistanceMeters(a.latitude, b.latitude);
  }

  const double lat_a = DegreesToRadians(a.latitude);
  const double lat_b = DegreesToRadians(b.latitude);
  const double u = std::sin((lat_b - lat_a) / 2.0);
  const double h = u * u + std::cos(lat_a) * std::cos(lat_b) * v * v;
  return 2.0 * kEarthRadiusMeters * std::asin(std::sqrt(h));
}

bool PointInRadius(const GeoCoordinates& origin, const GeoCoordinates& point,
                   double radius_meters, double* distance_meters) {
  if (distance_meters == nullptr) {
    return false;
  }
  *distance_meters = GeoDistanceMeters(origin, point);
  return *distance_meters <= radius_meters;
}

bool PointInBox(const GeoCoordinates& origin, const GeoCoordinates& point,
                double width_meters, double height_meters,
                double* distance_meters) {
  if (distance_meters == nullptr) {
    return false;
  }

  const double lat_distance =
      LatitudeDistanceMeters(point.latitude, origin.latitude);
  if (lat_distance > height_meters / 2.0) {
    return false;
  }
  const GeoCoordinates same_latitude_origin{origin.longitude, point.latitude};
  const double lon_distance = GeoDistanceMeters(point, same_latitude_origin);
  if (lon_distance > width_meters / 2.0) {
    return false;
  }

  *distance_meters = GeoDistanceMeters(origin, point);
  return true;
}

std::string EncodeGeoMemberPrefix(const std::string& key, uint64_t version) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  return out;
}

std::string EncodeGeoMemberKey(const std::string& key, uint64_t version,
                               const std::string& member) {
  std::string out = EncodeGeoMemberPrefix(key, version);
  out.append(member);
  return out;
}

bool ExtractMemberFromGeoMemberKey(const rocksdb::Slice& encoded_key,
                                   const rocksdb::Slice& prefix,
                                   std::string* member) {
  if (!KeyCodec::StartsWith(encoded_key, prefix)) {
    return false;
  }
  if (member != nullptr) {
    member->assign(encoded_key.data() + prefix.size(),
                   encoded_key.size() - prefix.size());
  }
  return true;
}

std::string EncodeGeoValue(const GeoStoredPoint& point) {
  std::string out;
  AppendUint64(&out, point.raw_hash);
  AppendUint64(&out, DoubleToBits(CanonicalizeZero(point.coordinates.longitude)));
  AppendUint64(&out, DoubleToBits(CanonicalizeZero(point.coordinates.latitude)));
  return out;
}

bool DecodeGeoValue(const rocksdb::Slice& value, GeoStoredPoint* point) {
  if (point == nullptr || value.size() != sizeof(uint64_t) * 3) {
    return false;
  }
  point->raw_hash = DecodeUint64(value.data());
  point->coordinates.longitude =
      CanonicalizeZero(BitsToDouble(DecodeUint64(value.data() + 8)));
  point->coordinates.latitude =
      CanonicalizeZero(BitsToDouble(DecodeUint64(value.data() + 16)));
  return !std::isnan(point->coordinates.longitude) &&
         !std::isnan(point->coordinates.latitude);
}

rocksdb::Status LookupGeoKey(const CoreKeyService* key_service,
                             ModuleSnapshot* snapshot, const std::string& key,
                             KeyLookup* lookup) {
  if (key_service == nullptr) {
    return rocksdb::Status::InvalidArgument("core key service is unavailable");
  }
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (lookup == nullptr) {
    return rocksdb::Status::InvalidArgument("key lookup output is required");
  }

  rocksdb::Status status = key_service->Lookup(snapshot, key, lookup);
  if (!status.ok() || !lookup->exists) {
    return status;
  }
  if (lookup->metadata.type != ObjectType::kZSet ||
      lookup->metadata.encoding != ObjectEncoding::kZSetGeo) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ReadGeoPoint(ModuleSnapshot* snapshot,
                             const ModuleKeyspace& keyspace,
                             const std::string& key, uint64_t version,
                             const std::string& member, GeoStoredPoint* point,
                             bool* found) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (point == nullptr) {
    return rocksdb::Status::InvalidArgument("geo point output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("geo point found output is required");
  }

  *point = GeoStoredPoint{};
  *found = false;

  std::string raw_value;
  rocksdb::Status status =
      snapshot->Get(keyspace, EncodeGeoMemberKey(key, version, member), &raw_value);
  if (status.IsNotFound()) {
    return rocksdb::Status::OK();
  }
  if (!status.ok()) {
    return status;
  }
  if (!DecodeGeoValue(raw_value, point)) {
    return rocksdb::Status::Corruption("invalid geo sidecar value");
  }
  point->member = member;
  *found = true;
  return rocksdb::Status::OK();
}

rocksdb::Status CollectGeoPoints(ModuleSnapshot* snapshot,
                                 const ModuleKeyspace& keyspace,
                                 const std::string& key, uint64_t version,
                                 std::vector<GeoStoredPoint>* out) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("geo result output is required");
  }

  out->clear();
  const std::string prefix = EncodeGeoMemberPrefix(key, version);
  std::unique_ptr<ModuleIterator> iter = snapshot->NewIterator(keyspace);
  for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
    if (!KeyCodec::StartsWith(iter->key(), prefix)) {
      break;
    }

    GeoStoredPoint point;
    if (!ExtractMemberFromGeoMemberKey(iter->key(), prefix, &point.member)) {
      break;
    }
    if (!DecodeGeoValue(iter->value(), &point)) {
      return rocksdb::Status::Corruption("invalid geo sidecar value");
    }
    out->push_back(std::move(point));
  }
  return iter->status();
}

std::vector<GeoLocation> CollapseLocationsByMember(
    const std::vector<GeoLocation>& locations) {
  std::map<std::string, GeoCoordinates> unique_locations;
  for (const auto& location : locations) {
    unique_locations[location.member] =
        GeoCoordinates{CanonicalizeZero(location.longitude),
                       CanonicalizeZero(location.latitude)};
  }

  std::vector<GeoLocation> result;
  result.reserve(unique_locations.size());
  for (const auto& entry : unique_locations) {
    result.push_back(
        GeoLocation{entry.first, entry.second.longitude, entry.second.latitude});
  }
  return result;
}

}  // namespace minikv
