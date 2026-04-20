#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "runtime/module/module_services.h"
#include "types/geo/geo_module.h"

namespace minikv {

class CoreKeyService;

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

void AppendUint32(std::string* out, uint32_t value);
void AppendUint64(std::string* out, uint64_t value);
uint64_t DecodeUint64(const char* input);
double CanonicalizeZero(double value);
uint64_t DoubleToBits(double value);
double BitsToDouble(uint64_t bits);
bool IsValidLongitudeLatitude(double longitude, double latitude);

uint64_t Interleave64(uint32_t xlo, uint32_t ylo);
uint64_t Deinterleave64(uint64_t interleaved);
bool EncodeGeoHashBits(double longitude, double latitude, double longitude_min,
                       double longitude_max, double latitude_min,
                       double latitude_max, uint8_t step, uint64_t* bits);
bool DecodeGeoHashBits(uint64_t bits, double longitude_min,
                       double longitude_max, double latitude_min,
                       double latitude_max, uint8_t step,
                       GeoCoordinates* coordinates);
bool EncodeGeoScore(double longitude, double latitude, uint64_t* raw_hash,
                    double* score);
bool DecodeGeoScore(double score, uint64_t* raw_hash,
                    GeoCoordinates* coordinates);
std::string EncodeStandardGeohashString(const GeoCoordinates& coordinates);
double GeoDistanceMeters(const GeoCoordinates& a, const GeoCoordinates& b);
bool PointInRadius(const GeoCoordinates& origin, const GeoCoordinates& point,
                   double radius_meters, double* distance_meters);
bool PointInBox(const GeoCoordinates& origin, const GeoCoordinates& point,
                double width_meters, double height_meters,
                double* distance_meters);

std::string EncodeGeoMemberPrefix(const std::string& key, uint64_t version);
std::string EncodeGeoMemberKey(const std::string& key, uint64_t version,
                               const std::string& member);
bool ExtractMemberFromGeoMemberKey(const rocksdb::Slice& encoded_key,
                                   const rocksdb::Slice& prefix,
                                   std::string* member);
std::string EncodeGeoValue(const GeoStoredPoint& point);
bool DecodeGeoValue(const rocksdb::Slice& value, GeoStoredPoint* point);
rocksdb::Status LookupGeoKey(const CoreKeyService* key_service,
                             ModuleSnapshot* snapshot, const std::string& key,
                             KeyLookup* lookup);
rocksdb::Status ReadGeoPoint(ModuleSnapshot* snapshot,
                             const ModuleKeyspace& keyspace,
                             const std::string& key, uint64_t version,
                             const std::string& member, GeoStoredPoint* point,
                             bool* found);
rocksdb::Status CollectGeoPoints(ModuleSnapshot* snapshot,
                                 const ModuleKeyspace& keyspace,
                                 const std::string& key, uint64_t version,
                                 std::vector<GeoStoredPoint>* out);
std::vector<GeoLocation> CollapseLocationsByMember(
    const std::vector<GeoLocation>& locations);

}  // namespace minikv
