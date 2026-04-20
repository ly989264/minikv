#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "core/key_service.h"
#include "runtime/module/module_services.h"

namespace minikv {

std::string EncodeSetMemberPrefix(const std::string& key, uint64_t version);
std::string EncodeSetMemberKey(const std::string& key, uint64_t version,
                               const std::string& member);
bool ExtractMemberFromSetMemberKey(const rocksdb::Slice& encoded_key,
                                   const rocksdb::Slice& prefix,
                                   std::string* member);
rocksdb::Status CollectSetMembers(ModuleSnapshot* snapshot,
                                  const ModuleKeyspace& members_keyspace,
                                  const std::string& key, uint64_t version,
                                  std::vector<std::string>* out);
size_t SelectRandomIndex(size_t count);
rocksdb::Status RequireSetEncoding(const KeyLookup& lookup);
KeyMetadata BuildSetMetadata(const CoreKeyService* key_service,
                             const KeyLookup& lookup);
KeyMetadata BuildSetTombstoneMetadata(const CoreKeyService* key_service,
                                      const KeyLookup& lookup);
std::vector<std::string> DeduplicateMembers(
    const std::vector<std::string>& members);

}  // namespace minikv
