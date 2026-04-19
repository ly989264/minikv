#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/status.h"
#include "third_party/minijson/minijson.h"

namespace minikv {

enum class JsonPathDialect : uint8_t {
  kLegacy = 0,
  kJsonPath = 1,
};

struct JsonPathStep {
  enum class Kind : uint8_t {
    kField = 0,
    kIndex = 1,
    kWildcard = 2,
    kRecursiveField = 3,
    kRecursiveWildcard = 4,
  };

  Kind kind = Kind::kField;
  std::string field;
  int64_t index = 0;
};

struct JsonPath {
  JsonPathDialect dialect = JsonPathDialect::kLegacy;
  std::string text;
  std::vector<JsonPathStep> steps;

  bool is_root() const { return steps.empty(); }

  bool is_dynamic() const;
};

struct JsonResolvedPathSegment {
  enum class Kind : uint8_t {
    kField = 0,
    kIndex = 1,
  };

  Kind kind = Kind::kField;
  std::string field;
  size_t index = 0;
};

struct JsonResolvedPath {
  std::vector<JsonResolvedPathSegment> segments;
};

rocksdb::Status ParseJsonPath(const std::string& text, JsonPath* path);

void CollectJsonPathMatches(const minijson::Value& root, const JsonPath& path,
                            std::vector<JsonResolvedPath>* matches);

bool ResolveJsonPath(const minijson::Value& root, const JsonResolvedPath& path,
                     const minijson::Value** value);
bool ResolveMutableJsonPath(minijson::Value* root, const JsonResolvedPath& path,
                            minijson::Value** value);
bool ResolveMutableJsonParent(minijson::Value* root,
                              const JsonResolvedPath& path,
                              minijson::Value** parent,
                              JsonResolvedPathSegment* leaf);

bool SplitJsonPathForObjectMemberCreate(const JsonPath& path,
                                        JsonPath* parent_path,
                                        std::string* member);

}  // namespace minikv
