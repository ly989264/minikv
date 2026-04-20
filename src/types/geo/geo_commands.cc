#include "types/geo/geo_commands.h"

#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>

#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "types/geo/geo_module.h"

namespace minikv {

namespace {

double CanonicalizeZero(double value) { return value == 0 ? 0.0 : value; }

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
  constexpr double kGeoLongitudeMin = -180.0;
  constexpr double kGeoLongitudeMax = 180.0;
  constexpr double kGeoLatitudeMin = -85.05112878;
  constexpr double kGeoLatitudeMax = 85.05112878;
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
      point.push_back(
          ReplyNode::BulkString(FormatCoordinate(coordinates.longitude)));
      point.push_back(
          ReplyNode::BulkString(FormatCoordinate(coordinates.latitude)));
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
                                               unit_to_meters_, &distance,
                                               &found);
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
        parts.push_back(ReplyNode::Integer(
            static_cast<long long>(match.raw_hash)));
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

rocksdb::Status RegisterGeoCommands(ModuleServices& services, GeoModule* module) {
  rocksdb::Status status = services.command_registry().Register(
      {"GEOADD", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<GeoAddCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"GEOPOS", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<GeoPosCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"GEOHASH", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<GeoHashCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"GEODIST", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "", [module](const CmdRegistration& registration) {
         return std::make_unique<GeoDistCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"GEOSEARCH", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<GeoSearchCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");
  return rocksdb::Status::OK();
}

}  // namespace minikv
