#include "types/set/set_internal.h"

#include <random>
#include <unordered_set>
#include <utility>

#include "storage/encoding/key_codec.h"

namespace minikv {

namespace {

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

}  // namespace

std::string EncodeSetMemberPrefix(const std::string& key, uint64_t version) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  return out;
}

std::string EncodeSetMemberKey(const std::string& key, uint64_t version,
                               const std::string& member) {
  std::string out = EncodeSetMemberPrefix(key, version);
  out.append(member);
  return out;
}

bool ExtractMemberFromSetMemberKey(const rocksdb::Slice& encoded_key,
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

rocksdb::Status CollectSetMembers(ModuleSnapshot* snapshot,
                                  const ModuleKeyspace& members_keyspace,
                                  const std::string& key, uint64_t version,
                                  std::vector<std::string>* out) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("members output is required");
  }

  out->clear();
  const std::string prefix = EncodeSetMemberPrefix(key, version);
  return snapshot->ScanPrefix(
      members_keyspace, prefix,
      [out, &prefix](const rocksdb::Slice& encoded_key,
                     const rocksdb::Slice& /*value_slice*/) {
        std::string member;
        if (!ExtractMemberFromSetMemberKey(encoded_key, prefix, &member)) {
          return false;
        }
        out->push_back(std::move(member));
        return true;
      });
}

size_t SelectRandomIndex(size_t count) {
  std::random_device device;
  std::mt19937_64 generator(
      (static_cast<uint64_t>(device()) << 32) ^ device());
  std::uniform_int_distribution<size_t> distribution(0, count - 1);
  return distribution(generator);
}

rocksdb::Status RequireSetEncoding(const KeyLookup& lookup) {
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  if (lookup.metadata.type != ObjectType::kSet ||
      lookup.metadata.encoding != ObjectEncoding::kSetHashtable) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }
  return rocksdb::Status::OK();
}

KeyMetadata BuildSetMetadata(const CoreKeyService* key_service,
                             const KeyLookup& lookup) {
  if (lookup.exists) {
    return lookup.metadata;
  }

  KeyMetadata metadata =
      key_service->MakeMetadata(ObjectType::kSet,
                                ObjectEncoding::kSetHashtable, lookup);
  metadata.size = 0;
  return metadata;
}

KeyMetadata BuildSetTombstoneMetadata(const CoreKeyService* key_service,
                                      const KeyLookup& lookup) {
  KeyMetadata metadata = key_service->MakeTombstoneMetadata(lookup);
  metadata.size = 0;
  return metadata;
}

std::vector<std::string> DeduplicateMembers(
    const std::vector<std::string>& members) {
  std::vector<std::string> unique_members;
  unique_members.reserve(members.size());

  std::unordered_set<std::string> seen;
  seen.reserve(members.size());
  for (const auto& member : members) {
    if (seen.insert(member).second) {
      unique_members.push_back(member);
    }
  }
  return unique_members;
}

}  // namespace minikv
