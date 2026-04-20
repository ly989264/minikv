#pragma once

#include <string>
#include <vector>

#include "core/key_service.h"
#include "runtime/module/module_services.h"
#include "types/json/json_path.h"

namespace minikv {
namespace json_internal {

struct LoadedJsonDocument {
  KeyLookup lookup;
  minijson::Value root;
};

rocksdb::Status RequireJsonEncoding(const KeyLookup& lookup);

KeyMetadata BuildJsonMetadata(const CoreKeyService* key_service,
                              const KeyLookup& lookup);

KeyMetadata BuildJsonTombstoneMetadata(const CoreKeyService* key_service,
                                       const KeyLookup& lookup);

std::string SerializeResolvedPath(const JsonResolvedPath& path);

std::string JsonTypeName(const minijson::Value& value);

bool IsZeroNumber(const minijson::Value& value);

rocksdb::Status LoadJsonDocument(ModuleServices* services,
                                 const CoreKeyService* key_service,
                                 const std::string& key,
                                 LoadedJsonDocument* out);

rocksdb::Status PersistJsonDocument(ModuleServices* services,
                                    const CoreKeyService* key_service,
                                    const std::string& key,
                                    const KeyLookup& lookup,
                                    const minijson::Value& root);

minijson::Value* ResolveMutableMatch(minijson::Value* root,
                                     const JsonResolvedPath& path);

const minijson::Value* ResolveMatch(const minijson::Value& root,
                                    const JsonResolvedPath& path);

void DeduplicateMatches(std::vector<JsonResolvedPath>* matches);

}  // namespace json_internal
}  // namespace minikv
