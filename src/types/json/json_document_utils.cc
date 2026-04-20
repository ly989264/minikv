#include "types/json/json_document_utils.h"

#include <cmath>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace minikv {
namespace json_internal {

rocksdb::Status RequireJsonEncoding(const KeyLookup& lookup) {
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  if (lookup.metadata.type != ObjectType::kJson ||
      lookup.metadata.encoding != ObjectEncoding::kJsonDocument) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }
  return rocksdb::Status::OK();
}

KeyMetadata BuildJsonMetadata(const CoreKeyService* key_service,
                              const KeyLookup& lookup) {
  if (lookup.exists) {
    return lookup.metadata;
  }
  return key_service->MakeMetadata(ObjectType::kJson,
                                   ObjectEncoding::kJsonDocument, lookup);
}

KeyMetadata BuildJsonTombstoneMetadata(const CoreKeyService* key_service,
                                       const KeyLookup& lookup) {
  KeyMetadata metadata = key_service->MakeTombstoneMetadata(lookup);
  metadata.size = 0;
  return metadata;
}

std::string JsonTypeName(const minijson::Value& value) {
  switch (value.type()) {
    case minijson::Value::Type::kNull:
      return "null";
    case minijson::Value::Type::kBool:
      return "boolean";
    case minijson::Value::Type::kNumber:
      return value.number().integral ? "integer" : "number";
    case minijson::Value::Type::kString:
      return "string";
    case minijson::Value::Type::kArray:
      return "array";
    case minijson::Value::Type::kObject:
      return "object";
  }
  return "unknown";
}

std::string SerializeResolvedPath(const JsonResolvedPath& path) {
  std::string out;
  for (const auto& segment : path.segments) {
    out.push_back('/');
    if (segment.kind == JsonResolvedPathSegment::Kind::kField) {
      out.append(segment.field);
      continue;
    }
    out.append(std::to_string(segment.index));
  }
  return out;
}

bool IsZeroNumber(const minijson::Value& value) {
  return value.IsNumber() && std::fabs(value.number().value) < 0.5e-18L;
}

rocksdb::Status LoadJsonDocument(ModuleServices* services,
                                 const CoreKeyService* key_service,
                                 const std::string& key,
                                 LoadedJsonDocument* out) {
  if (services == nullptr || key_service == nullptr || out == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "JSON document services are unavailable");
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services->snapshot().Create();
  rocksdb::Status status = key_service->Lookup(snapshot.get(), key, &out->lookup);
  if (!status.ok()) {
    return status;
  }
  status = RequireJsonEncoding(out->lookup);
  if (!status.ok()) {
    return status;
  }
  if (!out->lookup.exists) {
    out->root = minijson::Value::Null();
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace data_keyspace = services->storage().Keyspace("data");
  std::string encoded;
  status = snapshot->Get(data_keyspace, key, &encoded);
  if (status.IsNotFound()) {
    return rocksdb::Status::Corruption("JSON document is missing data");
  }
  if (!status.ok()) {
    return status;
  }

  std::string error;
  if (!minijson::Parse(encoded, &out->root, &error)) {
    return rocksdb::Status::Corruption("invalid stored JSON: " + error);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status PersistJsonDocument(ModuleServices* services,
                                    const CoreKeyService* key_service,
                                    const std::string& key,
                                    const KeyLookup& lookup,
                                    const minijson::Value& root) {
  if (services == nullptr || key_service == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "JSON document services are unavailable");
  }

  const std::string encoded = minijson::Serialize(root);
  KeyMetadata metadata = BuildJsonMetadata(key_service, lookup);
  metadata.size = encoded.size();

  const ModuleKeyspace data_keyspace = services->storage().Keyspace("data");
  std::unique_ptr<ModuleWriteBatch> write_batch =
      services->storage().CreateWriteBatch();
  rocksdb::Status status = write_batch->Put(data_keyspace, key, encoded);
  if (!status.ok()) {
    return status;
  }
  status = key_service->PutMetadata(write_batch.get(), key, metadata);
  if (!status.ok()) {
    return status;
  }
  return write_batch->Commit();
}

minijson::Value* ResolveMutableMatch(minijson::Value* root,
                                     const JsonResolvedPath& path) {
  minijson::Value* value = nullptr;
  if (!ResolveMutableJsonPath(root, path, &value)) {
    return nullptr;
  }
  return value;
}

const minijson::Value* ResolveMatch(const minijson::Value& root,
                                    const JsonResolvedPath& path) {
  const minijson::Value* value = nullptr;
  if (!ResolveJsonPath(root, path, &value)) {
    return nullptr;
  }
  return value;
}

void DeduplicateMatches(std::vector<JsonResolvedPath>* matches) {
  if (matches == nullptr) {
    return;
  }
  std::unordered_set<std::string> seen;
  std::vector<JsonResolvedPath> unique;
  unique.reserve(matches->size());
  for (const auto& match : *matches) {
    const std::string signature = SerializeResolvedPath(match);
    if (seen.insert(signature).second) {
      unique.push_back(match);
    }
  }
  *matches = std::move(unique);
}

}  // namespace json_internal
}  // namespace minikv
