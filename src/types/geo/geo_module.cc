#include "types/geo/geo_module.h"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "core/key_service.h"
#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
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

struct GeoStoredPoint {
  std::string member;
  GeoCoordinates coordinates;
  uint64_t raw_hash = 0;
};

struct GeoHashArea {
  double longitude_min = 0;
  double longitude_max = 0;
  double latitude_min = 0;
  double latitude_max = 0;
};

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

double CanonicalizeZero(double value) {
  return value == 0 ? 0.0 : value;
}

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

std::string LowercaseAscii(std::string input) {
  for (char& ch : input) {
    if (ch >= 'A' && ch <= 'Z') {
      ch = static_cast<char>(ch - 'A' + 'a');
    }
  }
  return input;
}

bool ParseRawDouble(const std::string& input, double* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  errno = 0;
  char* parse_end = nullptr;
  const double parsed = std::strtod(input.c_str(), &parse_end);
  if (parse_end == nullptr || *parse_end != '\0' || std::isnan(parsed)) {
    return false;
  }
  if (errno == ERANGE && !std::isinf(parsed)) {
    return false;
  }

  *value = CanonicalizeZero(parsed);
  return true;
}

bool ParseInt64(const std::string& input, int64_t* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  errno = 0;
  char* parse_end = nullptr;
  const long long parsed = std::strtoll(input.c_str(), &parse_end, 10);
  if (parse_end == nullptr || *parse_end != '\0' || errno == ERANGE) {
    return false;
  }
  *value = static_cast<int64_t>(parsed);
  return true;
}

bool IsValidLongitudeLatitude(double longitude, double latitude) {
  return longitude >= kGeoLongitudeMin && longitude <= kGeoLongitudeMax &&
         latitude >= kGeoLatitudeMin && latitude <= kGeoLatitudeMax;
}

std::string FormatCoordinate(double value) {
  value = CanonicalizeZero(value);
  char buffer[64];
  const int length = std::snprintf(buffer, sizeof(buffer), "%.17g", value);
  if (length <= 0) {
    return "0";
  }
  return std::string(buffer, static_cast<size_t>(length));
}

std::string FormatDistance(double distance) {
  distance = CanonicalizeZero(distance);
  char buffer[64];
  const int length = std::snprintf(buffer, sizeof(buffer), "%.4f", distance);
  if (length <= 0) {
    return "0.0000";
  }
  return std::string(buffer, static_cast<size_t>(length));
}

rocksdb::Status ParseUnit(const std::string& token, double* to_meters) {
  if (to_meters == nullptr) {
    return rocksdb::Status::InvalidArgument("unit output is required");
  }

  const std::string lowered = LowercaseAscii(token);
  if (lowered == "m") {
    *to_meters = 1.0;
    return rocksdb::Status::OK();
  }
  if (lowered == "km") {
    *to_meters = 1000.0;
    return rocksdb::Status::OK();
  }
  if (lowered == "ft") {
    *to_meters = 0.3048;
    return rocksdb::Status::OK();
  }
  if (lowered == "mi") {
    *to_meters = 1609.34;
    return rocksdb::Status::OK();
  }
  return rocksdb::Status::InvalidArgument(
      "unsupported unit provided. please use M, KM, FT, MI");
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

double DegreesToRadians(double degrees) {
  return degrees * (3.14159265358979323846 / 180.0);
}

double LatitudeDistanceMeters(double latitude_a, double latitude_b) {
  return kEarthRadiusMeters *
         std::fabs(DegreesToRadians(latitude_b) - DegreesToRadians(latitude_a));
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

class GeoAddCmd : public Cmd {
 public:
  GeoAddCmd(const CmdRegistration& registration, GeoModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty() || (input.args.size() % 3) != 0) {
      return rocksdb::Status::InvalidArgument(
          "GEOADD requires longitude/latitude/member triplets");
    }

    key_ = input.key;
    locations_.clear();
    locations_.reserve(input.args.size() / 3);
    for (size_t index = 0; index < input.args.size(); index += 3) {
      double longitude = 0;
      double latitude = 0;
      if (!ParseRawDouble(input.args[index], &longitude) ||
          !ParseRawDouble(input.args[index + 1], &latitude) ||
          !IsValidLongitudeLatitude(longitude, latitude)) {
        return rocksdb::Status::InvalidArgument(
            "GEOADD requires valid longitude/latitude pairs");
      }
      locations_.push_back(
          GeoLocation{input.args[index + 2], longitude, latitude});
    }
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("geo module is unavailable"));
    }

    uint64_t added = 0;
    rocksdb::Status status = module_->AddLocations(key_, locations_, &added);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(added));
  }

  GeoModule* module_ = nullptr;
  std::string key_;
  std::vector<GeoLocation> locations_;
};

class GeoPosCmd : public Cmd {
 public:
  GeoPosCmd(const CmdRegistration& registration, GeoModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "GEOPOS requires at least one member");
    }
    key_ = input.key;
    members_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("geo module is unavailable"));
    }

    std::vector<ReplyNode> replies;
    replies.reserve(members_.size());
    for (const std::string& member : members_) {
      GeoCoordinates coordinates;
      bool found = false;
      rocksdb::Status status =
          module_->Position(key_, member, &coordinates, &found);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      if (!found) {
        replies.push_back(ReplyNode::Null());
        continue;
      }
      std::vector<ReplyNode> point;
      point.push_back(ReplyNode::BulkString(FormatCoordinate(coordinates.longitude)));
      point.push_back(ReplyNode::BulkString(FormatCoordinate(coordinates.latitude)));
      replies.push_back(ReplyNode::Array(std::move(point)));
    }
    return MakeArray(std::move(replies));
  }

  GeoModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> members_;
};

class GeoHashCmd : public Cmd {
 public:
  GeoHashCmd(const CmdRegistration& registration, GeoModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "GEOHASH requires at least one member");
    }
    key_ = input.key;
    members_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("geo module is unavailable"));
    }

    std::vector<ReplyNode> replies;
    replies.reserve(members_.size());
    for (const std::string& member : members_) {
      std::string geohash;
      bool found = false;
      rocksdb::Status status = module_->Hash(key_, member, &geohash, &found);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      if (!found) {
        replies.push_back(ReplyNode::Null());
      } else {
        replies.push_back(ReplyNode::BulkString(std::move(geohash)));
      }
    }
    return MakeArray(std::move(replies));
  }

  GeoModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> members_;
};

class GeoDistCmd : public Cmd {
 public:
  GeoDistCmd(const CmdRegistration& registration, GeoModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2 && input.args.size() != 3) {
      return rocksdb::Status::InvalidArgument(
          "GEODIST requires two members and an optional unit");
    }

    key_ = input.key;
    member_a_ = input.args[0];
    member_b_ = input.args[1];
    if (input.args.size() == 3) {
      rocksdb::Status status = ParseUnit(input.args[2], &unit_to_meters_);
      if (!status.ok()) {
        return status;
      }
    }
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("geo module is unavailable"));
    }

    double distance = 0;
    bool found = false;
    rocksdb::Status status = module_->Distance(key_, member_a_, member_b_,
                                               unit_to_meters_, &distance, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeBulkString(FormatDistance(distance));
  }

  GeoModule* module_ = nullptr;
  std::string key_;
  std::string member_a_;
  std::string member_b_;
  double unit_to_meters_ = 1.0;
};

class GeoSearchCmd : public Cmd {
 public:
  GeoSearchCmd(const CmdRegistration& registration, GeoModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }

    key_ = input.key;
    query_ = GeoSearchQuery{};

    bool have_origin = false;
    bool have_shape = false;
    bool have_count = false;

    size_t index = 0;
    while (index < input.args.size()) {
      const std::string token = LowercaseAscii(input.args[index]);
      if (token == "frommember" && index + 1 < input.args.size() &&
          !have_origin) {
        query_.origin_type = GeoSearchQuery::OriginType::kFromMember;
        query_.from_member = input.args[index + 1];
        have_origin = true;
        index += 2;
        continue;
      }
      if (token == "fromlonlat" && index + 2 < input.args.size() &&
          !have_origin) {
        double longitude = 0;
        double latitude = 0;
        if (!ParseRawDouble(input.args[index + 1], &longitude) ||
            !ParseRawDouble(input.args[index + 2], &latitude) ||
            !IsValidLongitudeLatitude(longitude, latitude)) {
          return rocksdb::Status::InvalidArgument(
              "GEOSEARCH requires a valid FROMLONLAT pair");
        }
        query_.origin_type = GeoSearchQuery::OriginType::kFromLonLat;
        query_.origin = GeoCoordinates{longitude, latitude};
        have_origin = true;
        index += 3;
        continue;
      }
      if (token == "byradius" && index + 2 < input.args.size() &&
          !have_shape) {
        if (!ParseRawDouble(input.args[index + 1], &query_.radius) ||
            query_.radius < 0) {
          return rocksdb::Status::InvalidArgument(
              "GEOSEARCH requires a non-negative radius");
        }
        rocksdb::Status status =
            ParseUnit(input.args[index + 2], &query_.unit_to_meters);
        if (!status.ok()) {
          return status;
        }
        query_.shape_type = GeoSearchQuery::ShapeType::kByRadius;
        have_shape = true;
        index += 3;
        continue;
      }
      if (token == "bybox" && index + 3 < input.args.size() && !have_shape) {
        if (!ParseRawDouble(input.args[index + 1], &query_.box_width) ||
            !ParseRawDouble(input.args[index + 2], &query_.box_height) ||
            query_.box_width < 0 || query_.box_height < 0) {
          return rocksdb::Status::InvalidArgument(
              "GEOSEARCH requires non-negative box dimensions");
        }
        rocksdb::Status status =
            ParseUnit(input.args[index + 3], &query_.unit_to_meters);
        if (!status.ok()) {
          return status;
        }
        query_.shape_type = GeoSearchQuery::ShapeType::kByBox;
        have_shape = true;
        index += 4;
        continue;
      }
      if (token == "withdist") {
        query_.with_dist = true;
        ++index;
        continue;
      }
      if (token == "withhash") {
        query_.with_hash = true;
        ++index;
        continue;
      }
      if (token == "withcoord") {
        query_.with_coord = true;
        ++index;
        continue;
      }
      if (token == "asc" && query_.sort == GeoSearchSort::kNone) {
        query_.sort = GeoSearchSort::kAsc;
        ++index;
        continue;
      }
      if (token == "desc" && query_.sort == GeoSearchSort::kNone) {
        query_.sort = GeoSearchSort::kDesc;
        ++index;
        continue;
      }
      if (token == "count" && index + 1 < input.args.size() && !have_count) {
        int64_t count = 0;
        if (!ParseInt64(input.args[index + 1], &count) || count <= 0) {
          return rocksdb::Status::InvalidArgument("COUNT must be > 0");
        }
        query_.count = static_cast<uint64_t>(count);
        have_count = true;
        index += 2;
        continue;
      }
      if (token == "any") {
        return rocksdb::Status::InvalidArgument(
            "GEOSEARCH COUNT ANY is unsupported");
      }
      return rocksdb::Status::InvalidArgument("invalid GEOSEARCH syntax");
    }

    if (!have_origin) {
      return rocksdb::Status::InvalidArgument(
          "GEOSEARCH requires exactly one center");
    }
    if (!have_shape) {
      return rocksdb::Status::InvalidArgument(
          "GEOSEARCH requires exactly one search shape");
    }

    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("geo module is unavailable"));
    }

    std::vector<GeoSearchMatch> matches;
    rocksdb::Status status = module_->Search(key_, query_, &matches);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }

    if (!query_.with_dist && !query_.with_hash && !query_.with_coord) {
      std::vector<std::string> members;
      members.reserve(matches.size());
      for (const auto& match : matches) {
        members.push_back(match.member);
      }
      return MakeArray(std::move(members));
    }

    std::vector<ReplyNode> reply_matches;
    reply_matches.reserve(matches.size());
    for (const auto& match : matches) {
      std::vector<ReplyNode> parts;
      parts.push_back(ReplyNode::BulkString(match.member));
      if (query_.with_dist) {
        parts.push_back(ReplyNode::BulkString(
            FormatDistance(match.distance_meters / query_.unit_to_meters)));
      }
      if (query_.with_hash) {
        parts.push_back(
            ReplyNode::Integer(static_cast<long long>(match.raw_hash)));
      }
      if (query_.with_coord) {
        std::vector<ReplyNode> coordinates;
        coordinates.push_back(
            ReplyNode::BulkString(FormatCoordinate(match.coordinates.longitude)));
        coordinates.push_back(
            ReplyNode::BulkString(FormatCoordinate(match.coordinates.latitude)));
        parts.push_back(ReplyNode::Array(std::move(coordinates)));
      }
      reply_matches.push_back(ReplyNode::Array(std::move(parts)));
    }
    return MakeArray(std::move(reply_matches));
  }

  GeoModule* module_ = nullptr;
  std::string key_;
  GeoSearchQuery query_;
};

}  // namespace

rocksdb::Status GeoModule::OnLoad(ModuleServices& services) {
  services_ = &services;

  rocksdb::Status status = services.command_registry().Register(
      {"GEOADD", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<GeoAddCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"GEOPOS", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<GeoPosCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"GEOHASH", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<GeoHashCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"GEODIST", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<GeoDistCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"GEOSEARCH", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [this](const CmdRegistration& registration) {
         return std::make_unique<GeoSearchCmd>(registration, this);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");
  return rocksdb::Status::OK();
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
