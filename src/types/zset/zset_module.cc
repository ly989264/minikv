#include "types/zset/zset_module.h"

#include "types/zset/zset_commands.h"

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
#include "runtime/module/module_services.h"
#include "storage/encoding/key_codec.h"

namespace minikv {

namespace {

enum class AddMembersMode {
  kPreserveLiveEncoding,
  kRequireRequestedEncoding,
};

struct NormalizedRange {
  size_t begin = 0;
  size_t end = 0;
  bool empty = true;
};

struct ScoreBound {
  double score = 0;
  bool inclusive = true;
  bool negative_infinity = false;
  bool positive_infinity = false;
};

struct ScoreRange {
  ScoreBound min;
  ScoreBound max;
  bool empty = false;
};

struct LexBound {
  std::string member;
  bool inclusive = true;
  bool negative_infinity = false;
  bool positive_infinity = false;
};

struct LexRange {
  LexBound min;
  LexBound max;
  bool empty = false;
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

uint64_t EncodeSortableScore(double score) {
  const uint64_t bits = DoubleToBits(CanonicalizeZero(score));
  if ((bits & (1ull << 63)) != 0) {
    return ~bits;
  }
  return bits ^ (1ull << 63);
}

double DecodeSortableScore(uint64_t encoded) {
  uint64_t bits = 0;
  if ((encoded & (1ull << 63)) != 0) {
    bits = encoded ^ (1ull << 63);
  } else {
    bits = ~encoded;
  }
  return CanonicalizeZero(BitsToDouble(bits));
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

NormalizedRange NormalizeRange(size_t size, int64_t start, int64_t stop) {
  NormalizedRange range;
  if (size == 0) {
    return range;
  }

  const int64_t size_i64 = static_cast<int64_t>(size);
  int64_t normalized_start = start < 0 ? size_i64 + start : start;
  int64_t normalized_stop = stop < 0 ? size_i64 + stop : stop;

  if (normalized_start < 0) {
    normalized_start = 0;
  }
  if (normalized_stop < 0) {
    return range;
  }
  if (normalized_start >= size_i64) {
    return range;
  }
  if (normalized_stop >= size_i64) {
    normalized_stop = size_i64 - 1;
  }
  if (normalized_start > normalized_stop) {
    return range;
  }

  range.begin = static_cast<size_t>(normalized_start);
  range.end = static_cast<size_t>(normalized_stop);
  range.empty = false;
  return range;
}

int CompareScoreBoundValues(const ScoreBound& lhs, const ScoreBound& rhs) {
  if (lhs.negative_infinity) {
    return rhs.negative_infinity ? 0 : -1;
  }
  if (lhs.positive_infinity) {
    return rhs.positive_infinity ? 0 : 1;
  }
  if (rhs.negative_infinity) {
    return 1;
  }
  if (rhs.positive_infinity) {
    return -1;
  }
  if (lhs.score < rhs.score) {
    return -1;
  }
  if (lhs.score > rhs.score) {
    return 1;
  }
  return 0;
}

int CompareLexBoundValues(const LexBound& lhs, const LexBound& rhs) {
  if (lhs.negative_infinity) {
    return rhs.negative_infinity ? 0 : -1;
  }
  if (lhs.positive_infinity) {
    return rhs.positive_infinity ? 0 : 1;
  }
  if (rhs.negative_infinity) {
    return 1;
  }
  if (rhs.positive_infinity) {
    return -1;
  }
  if (lhs.member < rhs.member) {
    return -1;
  }
  if (lhs.member > rhs.member) {
    return 1;
  }
  return 0;
}

bool ParseScoreBound(const std::string& input, ScoreBound* bound) {
  if (bound == nullptr || input.empty()) {
    return false;
  }

  *bound = ScoreBound{};
  std::string token = input;
  if (!token.empty() && token[0] == '(') {
    bound->inclusive = false;
    token.erase(0, 1);
    if (token.empty()) {
      return false;
    }
  }

  const std::string lowered = LowercaseAscii(token);
  if (lowered == "-inf") {
    bound->negative_infinity = true;
    return true;
  }
  if (lowered == "inf" || lowered == "+inf") {
    bound->positive_infinity = true;
    return true;
  }

  double score = 0;
  if (!ParseRawDouble(token, &score) || std::isinf(score)) {
    return false;
  }
  bound->score = score;
  return true;
}

bool ParseLexBound(const std::string& input, LexBound* bound) {
  if (bound == nullptr || input.empty()) {
    return false;
  }

  *bound = LexBound{};
  if (input == "-") {
    bound->negative_infinity = true;
    return true;
  }
  if (input == "+") {
    bound->positive_infinity = true;
    return true;
  }
  if (input[0] != '[' && input[0] != '(') {
    return false;
  }

  bound->inclusive = input[0] == '[';
  bound->member = input.substr(1);
  return true;
}

rocksdb::Status ParseScoreRange(const std::string& min, const std::string& max,
                                ScoreRange* range) {
  if (range == nullptr) {
    return rocksdb::Status::InvalidArgument("score range output is required");
  }

  ScoreRange parsed;
  if (!ParseScoreBound(min, &parsed.min)) {
    return rocksdb::Status::InvalidArgument("invalid score range min");
  }
  if (!ParseScoreBound(max, &parsed.max)) {
    return rocksdb::Status::InvalidArgument("invalid score range max");
  }

  const int comparison = CompareScoreBoundValues(parsed.min, parsed.max);
  parsed.empty =
      comparison > 0 || (comparison == 0 &&
                         (!parsed.min.inclusive || !parsed.max.inclusive));
  *range = std::move(parsed);
  return rocksdb::Status::OK();
}

rocksdb::Status ParseLexRange(const std::string& min, const std::string& max,
                              LexRange* range) {
  if (range == nullptr) {
    return rocksdb::Status::InvalidArgument("lex range output is required");
  }

  LexRange parsed;
  if (!ParseLexBound(min, &parsed.min)) {
    return rocksdb::Status::InvalidArgument("invalid lex range min");
  }
  if (!ParseLexBound(max, &parsed.max)) {
    return rocksdb::Status::InvalidArgument("invalid lex range max");
  }

  const int comparison = CompareLexBoundValues(parsed.min, parsed.max);
  parsed.empty =
      comparison > 0 || (comparison == 0 &&
                         (!parsed.min.inclusive || !parsed.max.inclusive));
  *range = std::move(parsed);
  return rocksdb::Status::OK();
}

bool ScoreSatisfiesLower(double score, const ScoreBound& bound) {
  if (bound.negative_infinity) {
    return true;
  }
  if (bound.positive_infinity) {
    return std::isinf(score) && score > 0 && bound.inclusive;
  }
  if (score > bound.score) {
    return true;
  }
  if (score < bound.score) {
    return false;
  }
  return bound.inclusive;
}

bool ScoreExceedsUpper(double score, const ScoreBound& bound) {
  if (bound.positive_infinity) {
    return false;
  }
  if (bound.negative_infinity) {
    return !(std::isinf(score) && score < 0 && bound.inclusive);
  }
  if (score > bound.score) {
    return true;
  }
  if (score < bound.score) {
    return false;
  }
  return !bound.inclusive;
}

bool MemberSatisfiesLower(const std::string& member, const LexBound& bound) {
  if (bound.negative_infinity) {
    return true;
  }
  if (bound.positive_infinity) {
    return false;
  }
  if (member > bound.member) {
    return true;
  }
  if (member < bound.member) {
    return false;
  }
  return bound.inclusive;
}

bool MemberExceedsUpper(const std::string& member, const LexBound& bound) {
  if (bound.positive_infinity) {
    return false;
  }
  if (bound.negative_infinity) {
    return true;
  }
  if (member > bound.member) {
    return true;
  }
  if (member < bound.member) {
    return false;
  }
  return !bound.inclusive;
}

std::string EncodeZSetMemberPrefix(const std::string& key, uint64_t version) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  return out;
}

std::string EncodeZSetMemberKey(const std::string& key, uint64_t version,
                                const std::string& member) {
  std::string out = EncodeZSetMemberPrefix(key, version);
  out.append(member);
  return out;
}

std::string EncodeZSetScoreIndexPrefix(const std::string& key,
                                       uint64_t version) {
  return EncodeZSetMemberPrefix(key, version);
}

std::string EncodeZSetScoreIndexSeekKey(const std::string& key, uint64_t version,
                                        double score) {
  std::string out = EncodeZSetScoreIndexPrefix(key, version);
  AppendUint64(&out, EncodeSortableScore(score));
  return out;
}

std::string EncodeZSetScoreIndexKey(const std::string& key, uint64_t version,
                                    double score, const std::string& member) {
  std::string out = EncodeZSetScoreIndexSeekKey(key, version, score);
  out.append(member);
  return out;
}

bool ExtractMemberFromZSetMemberKey(const rocksdb::Slice& encoded_key,
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

bool DecodeZSetScoreIndexEntry(const rocksdb::Slice& encoded_key,
                               const rocksdb::Slice& prefix, double* score,
                               std::string* member) {
  if (!KeyCodec::StartsWith(encoded_key, prefix) ||
      encoded_key.size() < prefix.size() + sizeof(uint64_t)) {
    return false;
  }

  if (score != nullptr) {
    *score = DecodeSortableScore(DecodeUint64(encoded_key.data() + prefix.size()));
  }
  if (member != nullptr) {
    member->assign(encoded_key.data() + prefix.size() + sizeof(uint64_t),
                   encoded_key.size() - prefix.size() - sizeof(uint64_t));
  }
  return true;
}

std::string EncodeScoreValue(double score) {
  std::string out;
  AppendUint64(&out, DoubleToBits(CanonicalizeZero(score)));
  return out;
}

bool DecodeScoreValue(const rocksdb::Slice& value, double* score) {
  if (score == nullptr || value.size() != sizeof(uint64_t)) {
    return false;
  }

  const double decoded = BitsToDouble(DecodeUint64(value.data()));
  if (std::isnan(decoded)) {
    return false;
  }
  *score = CanonicalizeZero(decoded);
  return true;
}

rocksdb::Status RequireZSetEncoding(const KeyLookup& lookup) {
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  if (lookup.metadata.type != ObjectType::kZSet ||
      (lookup.metadata.encoding != ObjectEncoding::kZSetSkiplist &&
       lookup.metadata.encoding != ObjectEncoding::kZSetGeo)) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }
  return rocksdb::Status::OK();
}

KeyMetadata BuildZSetMetadata(const CoreKeyService* key_service,
                              const KeyLookup& lookup,
                              ObjectEncoding create_encoding) {
  if (lookup.exists) {
    return lookup.metadata;
  }

  KeyMetadata metadata =
      key_service->MakeMetadata(ObjectType::kZSet, create_encoding, lookup);
  metadata.size = 0;
  return metadata;
}

KeyMetadata BuildZSetTombstoneMetadata(const CoreKeyService* key_service,
                                       const KeyLookup& lookup) {
  KeyMetadata metadata = key_service->MakeTombstoneMetadata(lookup);
  metadata.size = 0;
  return metadata;
}

std::vector<ZSetEntry> CollapseEntriesByMember(
    const std::vector<ZSetEntry>& entries) {
  std::map<std::string, double> unique_entries;
  for (const auto& entry : entries) {
    unique_entries[entry.member] = CanonicalizeZero(entry.score);
  }

  std::vector<ZSetEntry> result;
  result.reserve(unique_entries.size());
  for (const auto& entry : unique_entries) {
    result.push_back(ZSetEntry{entry.first, entry.second});
  }
  return result;
}

std::vector<std::string> DeduplicateMembers(
    const std::vector<std::string>& members) {
  std::vector<std::string> unique_members = members;
  std::sort(unique_members.begin(), unique_members.end());
  unique_members.erase(
      std::unique(unique_members.begin(), unique_members.end()),
      unique_members.end());
  return unique_members;
}

rocksdb::Status ReadMemberScore(ModuleSnapshot* snapshot,
                                const ModuleKeyspace& members_keyspace,
                                const std::string& key, uint64_t version,
                                const std::string& member, double* score,
                                bool* found) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (score == nullptr) {
    return rocksdb::Status::InvalidArgument("member score output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("member found output is required");
  }

  *score = 0;
  *found = false;

  std::string raw_score;
  rocksdb::Status status = snapshot->Get(
      members_keyspace, EncodeZSetMemberKey(key, version, member), &raw_score);
  if (status.IsNotFound()) {
    return rocksdb::Status::OK();
  }
  if (!status.ok()) {
    return status;
  }
  if (!DecodeScoreValue(raw_score, score)) {
    return rocksdb::Status::Corruption("invalid zset member score");
  }

  *found = true;
  return rocksdb::Status::OK();
}

rocksdb::Status CollectMembers(ModuleSnapshot* snapshot,
                               const ModuleKeyspace& members_keyspace,
                               const std::string& key, uint64_t version,
                               std::vector<ZSetEntry>* out) {
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("zset member output is required");
  }

  out->clear();

  const std::string prefix = EncodeZSetMemberPrefix(key, version);
  std::unique_ptr<ModuleIterator> iter = snapshot->NewIterator(members_keyspace);
  for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
    if (!KeyCodec::StartsWith(iter->key(), prefix)) {
      break;
    }

    std::string member;
    if (!ExtractMemberFromZSetMemberKey(iter->key(), prefix, &member)) {
      break;
    }

    double score = 0;
    if (!DecodeScoreValue(iter->value(), &score)) {
      return rocksdb::Status::Corruption("invalid zset member score");
    }
    out->push_back(ZSetEntry{std::move(member), score});
  }

  return iter->status();
}

}  // namespace

rocksdb::Status ZSetModule::OnLoad(ModuleServices& services) {
  observers_.clear();
  services_ = &services;

  rocksdb::Status status = services.exports().Publish<ZSetBridge>(
      kZSetBridgeExportName, static_cast<ZSetBridge*>(this));
  if (!status.ok()) {
    return status;
  }

  return RegisterZSetCommands(services, this);
}

rocksdb::Status ZSetModule::OnStart(ModuleServices& services) {
  key_service_ = services.exports().Find<CoreKeyService>(
      kCoreKeyServiceQualifiedExportName);
  if (key_service_ == nullptr) {
    return rocksdb::Status::InvalidArgument("core key service is unavailable");
  }

  delete_registry_ = services.exports().Find<WholeKeyDeleteRegistry>(
      kWholeKeyDeleteRegistryQualifiedExportName);
  if (delete_registry_ == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "whole-key delete registry is unavailable");
  }

  rocksdb::Status status = delete_registry_->RegisterHandler(this);
  if (!status.ok()) {
    return status;
  }

  started_ = true;
  services.metrics().SetCounter("worker_count",
                                services.scheduler().worker_count());
  return rocksdb::Status::OK();
}

void ZSetModule::OnStop(ModuleServices& /*services*/) {
  observers_.clear();
  started_ = false;
  delete_registry_ = nullptr;
  key_service_ = nullptr;
  services_ = nullptr;
}

rocksdb::Status ZSetModule::AddMembersWithEncoding(
    const std::string& key, const std::vector<ZSetEntry>& entries,
    ObjectEncoding encoding, uint64_t* added_count) {
  if (added_count != nullptr) {
    *added_count = 0;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }
  if (encoding != ObjectEncoding::kZSetSkiplist &&
      encoding != ObjectEncoding::kZSetGeo) {
    return rocksdb::Status::InvalidArgument("invalid zset encoding");
  }

  const std::vector<ZSetEntry> unique_entries = CollapseEntriesByMember(entries);
  if (unique_entries.empty()) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (lookup.exists && lookup.metadata.encoding != encoding) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  const ModuleKeyspace score_index_keyspace =
      services_->storage().Keyspace("score_index");
  KeyMetadata after = BuildZSetMetadata(key_service_, lookup, encoding);
  uint64_t added = 0;
  bool changed = false;
  std::vector<ZSetEntry> changed_entries;
  changed_entries.reserve(unique_entries.size());

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  for (const auto& entry : unique_entries) {
    double existing_score = 0;
    bool found = false;
    status = ReadMemberScore(snapshot.get(), members_keyspace, key, after.version,
                             entry.member, &existing_score, &found);
    if (!status.ok()) {
      return status;
    }

    if (found) {
      if (existing_score == entry.score) {
        continue;
      }
      status = write_batch->Delete(
          score_index_keyspace,
          EncodeZSetScoreIndexKey(key, after.version, existing_score,
                                  entry.member));
      if (!status.ok()) {
        return status;
      }
    } else {
      ++added;
      ++after.size;
    }

    status = write_batch->Put(members_keyspace,
                              EncodeZSetMemberKey(key, after.version, entry.member),
                              EncodeScoreValue(entry.score));
    if (!status.ok()) {
      return status;
    }
    status = write_batch->Put(score_index_keyspace,
                              EncodeZSetScoreIndexKey(key, after.version,
                                                      entry.score, entry.member),
                              "");
    if (!status.ok()) {
      return status;
    }
    changed = true;
    changed_entries.push_back(entry);
  }

  if (!changed) {
    return rocksdb::Status::OK();
  }

  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }

  ZSetMutation mutation;
  mutation.type = ZSetMutation::Type::kUpsertMembers;
  mutation.key = key;
  mutation.upserted_entries = std::move(changed_entries);
  mutation.before = lookup.metadata;
  mutation.after = after;
  mutation.existed_before = lookup.exists;
  mutation.exists_after = true;
  status = NotifyObservers(mutation, snapshot.get(), write_batch.get());
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (added_count != nullptr) {
    *added_count = status.ok() ? added : 0;
  }
  return status;
}

rocksdb::Status ZSetModule::AddObserver(ZSetObserver* observer) {
  if (services_ == nullptr) {
    return rocksdb::Status::InvalidArgument("zset bridge is unavailable");
  }
  if (observer == nullptr) {
    return rocksdb::Status::InvalidArgument("zset observer is required");
  }
  if (std::find(observers_.begin(), observers_.end(), observer) !=
      observers_.end()) {
    return rocksdb::Status::InvalidArgument("zset observer already registered");
  }
  observers_.push_back(observer);
  return rocksdb::Status::OK();
}

rocksdb::Status ZSetModule::RemoveObserver(ZSetObserver* observer) {
  if (services_ == nullptr) {
    return rocksdb::Status::InvalidArgument("zset bridge is unavailable");
  }
  auto it = std::find(observers_.begin(), observers_.end(), observer);
  if (it == observers_.end()) {
    return rocksdb::Status::InvalidArgument("zset observer is not registered");
  }
  observers_.erase(it);
  return rocksdb::Status::OK();
}

rocksdb::Status ZSetModule::AddMembers(const std::string& key,
                                       const std::vector<ZSetEntry>& entries,
                                       uint64_t* added_count) {
  if (added_count != nullptr) {
    *added_count = 0;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  const std::vector<ZSetEntry> unique_entries = CollapseEntriesByMember(entries);
  if (unique_entries.empty()) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  const ModuleKeyspace score_index_keyspace =
      services_->storage().Keyspace("score_index");
  KeyMetadata after =
      BuildZSetMetadata(key_service_, lookup, ObjectEncoding::kZSetSkiplist);
  uint64_t added = 0;
  bool changed = false;
  std::vector<ZSetEntry> changed_entries;
  changed_entries.reserve(unique_entries.size());

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  for (const auto& entry : unique_entries) {
    double existing_score = 0;
    bool found = false;
    status = ReadMemberScore(snapshot.get(), members_keyspace, key, after.version,
                             entry.member, &existing_score, &found);
    if (!status.ok()) {
      return status;
    }

    if (found) {
      if (existing_score == entry.score) {
        continue;
      }
      status = write_batch->Delete(
          score_index_keyspace,
          EncodeZSetScoreIndexKey(key, after.version, existing_score,
                                  entry.member));
      if (!status.ok()) {
        return status;
      }
    } else {
      ++added;
      ++after.size;
    }

    status = write_batch->Put(members_keyspace,
                              EncodeZSetMemberKey(key, after.version, entry.member),
                              EncodeScoreValue(entry.score));
    if (!status.ok()) {
      return status;
    }
    status = write_batch->Put(score_index_keyspace,
                              EncodeZSetScoreIndexKey(key, after.version,
                                                      entry.score, entry.member),
                              "");
    if (!status.ok()) {
      return status;
    }
    changed = true;
    changed_entries.push_back(entry);
  }

  if (!changed) {
    return rocksdb::Status::OK();
  }

  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }

  ZSetMutation mutation;
  mutation.type = ZSetMutation::Type::kUpsertMembers;
  mutation.key = key;
  mutation.upserted_entries = std::move(changed_entries);
  mutation.before = lookup.metadata;
  mutation.after = after;
  mutation.existed_before = lookup.exists;
  mutation.exists_after = true;
  status = NotifyObservers(mutation, snapshot.get(), write_batch.get());
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (added_count != nullptr) {
    *added_count = status.ok() ? added : 0;
  }
  return status;
}

rocksdb::Status ZSetModule::Cardinality(const std::string& key, uint64_t* size) {
  if (size == nullptr) {
    return rocksdb::Status::InvalidArgument("zset size output is required");
  }
  *size = 0;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  *size = lookup.metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status ZSetModule::CountByScore(const std::string& key,
                                         const std::string& min,
                                         const std::string& max,
                                         uint64_t* count) {
  if (count == nullptr) {
    return rocksdb::Status::InvalidArgument("score count output is required");
  }
  *count = 0;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  ScoreRange range;
  rocksdb::Status status = ParseScoreRange(min, max, &range);
  if (!status.ok()) {
    return status;
  }
  if (range.empty) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace score_index_keyspace =
      services_->storage().Keyspace("score_index");
  const std::string prefix =
      EncodeZSetScoreIndexPrefix(key, lookup.metadata.version);
  const std::string seek_key =
      range.min.negative_infinity
          ? prefix
          : EncodeZSetScoreIndexSeekKey(key, lookup.metadata.version,
                                        range.min.score);

  std::unique_ptr<ModuleIterator> iter = snapshot->NewIterator(score_index_keyspace);
  for (iter->Seek(seek_key); iter->Valid(); iter->Next()) {
    if (!KeyCodec::StartsWith(iter->key(), prefix)) {
      break;
    }

    double score = 0;
    if (!DecodeZSetScoreIndexEntry(iter->key(), prefix, &score, nullptr)) {
      break;
    }
    if (!ScoreSatisfiesLower(score, range.min)) {
      continue;
    }
    if (ScoreExceedsUpper(score, range.max)) {
      break;
    }
    ++(*count);
  }

  return iter->status();
}

rocksdb::Status ZSetModule::IncrementBy(const std::string& key, double increment,
                                        const std::string& member,
                                        double* new_score) {
  if (new_score == nullptr) {
    return rocksdb::Status::InvalidArgument("new score output is required");
  }
  *new_score = 0;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  const ModuleKeyspace score_index_keyspace =
      services_->storage().Keyspace("score_index");
  KeyMetadata after =
      BuildZSetMetadata(key_service_, lookup, ObjectEncoding::kZSetSkiplist);

  double current_score = 0;
  bool found = false;
  status = ReadMemberScore(snapshot.get(), members_keyspace, key, after.version,
                           member, &current_score, &found);
  if (!status.ok()) {
    return status;
  }

  const double updated_score = CanonicalizeZero(current_score + increment);
  if (std::isnan(updated_score)) {
    return rocksdb::Status::InvalidArgument("resulting score is not a number");
  }
  if (found && updated_score == current_score) {
    *new_score = updated_score;
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  if (found) {
    status = write_batch->Delete(
        score_index_keyspace,
        EncodeZSetScoreIndexKey(key, after.version, current_score, member));
    if (!status.ok()) {
      return status;
    }
  } else {
    ++after.size;
  }

  status = write_batch->Put(members_keyspace,
                            EncodeZSetMemberKey(key, after.version, member),
                            EncodeScoreValue(updated_score));
  if (!status.ok()) {
    return status;
  }
  status = write_batch->Put(
      score_index_keyspace,
      EncodeZSetScoreIndexKey(key, after.version, updated_score, member), "");
  if (!status.ok()) {
    return status;
  }
  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }

  ZSetMutation mutation;
  mutation.type = ZSetMutation::Type::kUpsertMembers;
  mutation.key = key;
  mutation.upserted_entries.push_back(ZSetEntry{member, updated_score});
  mutation.before = lookup.metadata;
  mutation.after = after;
  mutation.existed_before = lookup.exists;
  mutation.exists_after = true;
  status = NotifyObservers(mutation, snapshot.get(), write_batch.get());
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (!status.ok()) {
    return status;
  }

  *new_score = updated_score;
  return rocksdb::Status::OK();
}

rocksdb::Status ZSetModule::CountByLex(const std::string& key,
                                       const std::string& min,
                                       const std::string& max,
                                       uint64_t* count) {
  if (count == nullptr) {
    return rocksdb::Status::InvalidArgument("lex count output is required");
  }
  *count = 0;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  LexRange range;
  rocksdb::Status status = ParseLexRange(min, max, &range);
  if (!status.ok()) {
    return status;
  }
  if (range.empty) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  const std::string prefix = EncodeZSetMemberPrefix(key, lookup.metadata.version);
  const std::string seek_key =
      range.min.negative_infinity ? prefix
                                  : EncodeZSetMemberKey(key, lookup.metadata.version,
                                                        range.min.member);

  std::unique_ptr<ModuleIterator> iter = snapshot->NewIterator(members_keyspace);
  for (iter->Seek(seek_key); iter->Valid(); iter->Next()) {
    if (!KeyCodec::StartsWith(iter->key(), prefix)) {
      break;
    }

    std::string member;
    if (!ExtractMemberFromZSetMemberKey(iter->key(), prefix, &member)) {
      break;
    }
    if (!MemberSatisfiesLower(member, range.min)) {
      continue;
    }
    if (MemberExceedsUpper(member, range.max)) {
      break;
    }
    ++(*count);
  }

  return iter->status();
}

rocksdb::Status ZSetModule::RangeByRank(const std::string& key, int64_t start,
                                        int64_t stop,
                                        std::vector<std::string>* out) {
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("zset range output is required");
  }
  out->clear();

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const NormalizedRange range = NormalizeRange(lookup.metadata.size, start, stop);
  if (range.empty) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace score_index_keyspace =
      services_->storage().Keyspace("score_index");
  const std::string prefix =
      EncodeZSetScoreIndexPrefix(key, lookup.metadata.version);

  size_t index = 0;
  std::unique_ptr<ModuleIterator> iter = snapshot->NewIterator(score_index_keyspace);
  for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
    if (!KeyCodec::StartsWith(iter->key(), prefix)) {
      break;
    }

    std::string member;
    if (!DecodeZSetScoreIndexEntry(iter->key(), prefix, nullptr, &member)) {
      break;
    }
    if (index >= range.begin && index <= range.end) {
      out->push_back(std::move(member));
    }
    if (index >= range.end) {
      break;
    }
    ++index;
  }

  return iter->status();
}

rocksdb::Status ZSetModule::RangeByLex(const std::string& key,
                                       const std::string& min,
                                       const std::string& max,
                                       std::vector<std::string>* out) {
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("zset range output is required");
  }
  out->clear();

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  LexRange range;
  rocksdb::Status status = ParseLexRange(min, max, &range);
  if (!status.ok()) {
    return status;
  }
  if (range.empty) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  const std::string prefix = EncodeZSetMemberPrefix(key, lookup.metadata.version);
  const std::string seek_key =
      range.min.negative_infinity ? prefix
                                  : EncodeZSetMemberKey(key, lookup.metadata.version,
                                                        range.min.member);

  std::unique_ptr<ModuleIterator> iter = snapshot->NewIterator(members_keyspace);
  for (iter->Seek(seek_key); iter->Valid(); iter->Next()) {
    if (!KeyCodec::StartsWith(iter->key(), prefix)) {
      break;
    }

    std::string member;
    if (!ExtractMemberFromZSetMemberKey(iter->key(), prefix, &member)) {
      break;
    }
    if (!MemberSatisfiesLower(member, range.min)) {
      continue;
    }
    if (MemberExceedsUpper(member, range.max)) {
      break;
    }
    out->push_back(std::move(member));
  }

  return iter->status();
}

rocksdb::Status ZSetModule::RangeByScore(const std::string& key,
                                         const std::string& min,
                                         const std::string& max,
                                         std::vector<std::string>* out) {
  if (out == nullptr) {
    return rocksdb::Status::InvalidArgument("zset range output is required");
  }
  out->clear();

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  ScoreRange range;
  rocksdb::Status status = ParseScoreRange(min, max, &range);
  if (!status.ok()) {
    return status;
  }
  if (range.empty) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace score_index_keyspace =
      services_->storage().Keyspace("score_index");
  const std::string prefix =
      EncodeZSetScoreIndexPrefix(key, lookup.metadata.version);
  const std::string seek_key =
      range.min.negative_infinity
          ? prefix
          : EncodeZSetScoreIndexSeekKey(key, lookup.metadata.version,
                                        range.min.score);

  std::unique_ptr<ModuleIterator> iter = snapshot->NewIterator(score_index_keyspace);
  for (iter->Seek(seek_key); iter->Valid(); iter->Next()) {
    if (!KeyCodec::StartsWith(iter->key(), prefix)) {
      break;
    }

    double score = 0;
    std::string member;
    if (!DecodeZSetScoreIndexEntry(iter->key(), prefix, &score, &member)) {
      break;
    }
    if (!ScoreSatisfiesLower(score, range.min)) {
      continue;
    }
    if (ScoreExceedsUpper(score, range.max)) {
      break;
    }
    out->push_back(std::move(member));
  }

  return iter->status();
}

rocksdb::Status ZSetModule::Rank(const std::string& key, const std::string& member,
                                 uint64_t* rank, bool* found) {
  if (rank == nullptr) {
    return rocksdb::Status::InvalidArgument("rank output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("rank found output is required");
  }
  *rank = 0;
  *found = false;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  double target_score = 0;
  status = ReadMemberScore(snapshot.get(), members_keyspace, key,
                           lookup.metadata.version, member, &target_score, found);
  if (!status.ok() || !*found) {
    return status;
  }

  const ModuleKeyspace score_index_keyspace =
      services_->storage().Keyspace("score_index");
  const std::string prefix =
      EncodeZSetScoreIndexPrefix(key, lookup.metadata.version);

  uint64_t index = 0;
  std::unique_ptr<ModuleIterator> iter = snapshot->NewIterator(score_index_keyspace);
  for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
    if (!KeyCodec::StartsWith(iter->key(), prefix)) {
      break;
    }

    double score = 0;
    std::string indexed_member;
    if (!DecodeZSetScoreIndexEntry(iter->key(), prefix, &score, &indexed_member)) {
      break;
    }
    if (score == target_score && indexed_member == member) {
      *rank = index;
      return iter->status();
    }
    if (score > target_score || (score == target_score && indexed_member > member)) {
      break;
    }
    ++index;
  }

  *found = false;
  return iter->status();
}

rocksdb::Status ZSetModule::RemoveMembers(
    const std::string& key, const std::vector<std::string>& members,
    uint64_t* removed_count) {
  if (removed_count != nullptr) {
    *removed_count = 0;
  }

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  const std::vector<std::string> unique_members = DeduplicateMembers(members);
  if (unique_members.empty()) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  const ModuleKeyspace score_index_keyspace =
      services_->storage().Keyspace("score_index");
  KeyMetadata after = lookup.metadata;
  uint64_t removed = 0;

  std::unique_ptr<ModuleWriteBatch> write_batch =
      services_->storage().CreateWriteBatch();
  for (const auto& member : unique_members) {
    double score = 0;
    bool found = false;
    status = ReadMemberScore(snapshot.get(), members_keyspace, key, after.version,
                             member, &score, &found);
    if (!status.ok()) {
      return status;
    }
    if (!found) {
      continue;
    }

    status = write_batch->Delete(members_keyspace,
                                 EncodeZSetMemberKey(key, after.version, member));
    if (!status.ok()) {
      return status;
    }
    status = write_batch->Delete(
        score_index_keyspace,
        EncodeZSetScoreIndexKey(key, after.version, score, member));
    if (!status.ok()) {
      return status;
    }
    ++removed;
  }

  if (removed == 0) {
    return rocksdb::Status::OK();
  }

  if (removed >= after.size) {
    after = BuildZSetTombstoneMetadata(key_service_, lookup);
  } else {
    after.size -= removed;
  }
  status = key_service_->PutMetadata(write_batch.get(), key, after);
  if (!status.ok()) {
    return status;
  }

  ZSetMutation mutation;
  mutation.type = ZSetMutation::Type::kRemoveMembers;
  mutation.key = key;
  mutation.removed_members = unique_members;
  mutation.before = lookup.metadata;
  mutation.after = after;
  mutation.existed_before = lookup.exists;
  mutation.exists_after = after.expire_at_ms != kLogicalDeleteExpireAtMs;
  status = NotifyObservers(mutation, snapshot.get(), write_batch.get());
  if (!status.ok()) {
    return status;
  }

  status = write_batch->Commit();
  if (removed_count != nullptr) {
    *removed_count = status.ok() ? removed : 0;
  }
  return status;
}

rocksdb::Status ZSetModule::Score(const std::string& key, const std::string& member,
                                  double* score, bool* found) {
  if (score == nullptr) {
    return rocksdb::Status::InvalidArgument("score output is required");
  }
  if (found == nullptr) {
    return rocksdb::Status::InvalidArgument("score found output is required");
  }
  *score = 0;
  *found = false;

  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }

  std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
  KeyLookup lookup;
  rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }
  status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  return ReadMemberScore(snapshot.get(), members_keyspace, key,
                         lookup.metadata.version, member, score, found);
}

rocksdb::Status ZSetModule::DeleteWholeKey(ModuleSnapshot* snapshot,
                                           ModuleWriteBatch* write_batch,
                                           const std::string& key,
                                           const KeyLookup& lookup) {
  rocksdb::Status ready_status = EnsureReady();
  if (!ready_status.ok()) {
    return ready_status;
  }
  if (snapshot == nullptr) {
    return rocksdb::Status::InvalidArgument("module snapshot is unavailable");
  }
  if (write_batch == nullptr) {
    return rocksdb::Status::InvalidArgument("module write batch is unavailable");
  }

  rocksdb::Status status = RequireZSetEncoding(lookup);
  if (!status.ok()) {
    return status;
  }
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  const ModuleKeyspace members_keyspace = services_->storage().Keyspace("members");
  const ModuleKeyspace score_index_keyspace =
      services_->storage().Keyspace("score_index");
  std::vector<ZSetEntry> members;
  status = CollectMembers(snapshot, members_keyspace, key, lookup.metadata.version,
                          &members);
  if (!status.ok()) {
    return status;
  }

  for (const auto& entry : members) {
    status = write_batch->Delete(
        members_keyspace,
        EncodeZSetMemberKey(key, lookup.metadata.version, entry.member));
    if (!status.ok()) {
      return status;
    }
    status = write_batch->Delete(
        score_index_keyspace,
        EncodeZSetScoreIndexKey(key, lookup.metadata.version, entry.score,
                                entry.member));
    if (!status.ok()) {
      return status;
    }
  }

  const KeyMetadata after = BuildZSetTombstoneMetadata(key_service_, lookup);
  status = key_service_->PutMetadata(write_batch, key, after);
  if (!status.ok()) {
    return status;
  }

  ZSetMutation mutation;
  mutation.type = ZSetMutation::Type::kDeleteKey;
  mutation.key = key;
  mutation.before = lookup.metadata;
  mutation.after = after;
  mutation.existed_before = lookup.exists;
  mutation.exists_after = false;
  mutation.removed_members.reserve(members.size());
  for (const auto& entry : members) {
    mutation.removed_members.push_back(entry.member);
  }
  return NotifyObservers(mutation, snapshot, write_batch);
}

rocksdb::Status ZSetModule::EnsureReady() const {
  if (services_ == nullptr || key_service_ == nullptr || !started_) {
    return rocksdb::Status::InvalidArgument("zset module is unavailable");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ZSetModule::NotifyObservers(const ZSetMutation& mutation,
                                            ModuleSnapshot* snapshot,
                                            ModuleWriteBatch* write_batch) const {
  for (ZSetObserver* observer : observers_) {
    rocksdb::Status status =
        observer->OnZSetMutation(mutation, snapshot, write_batch);
    if (!status.ok()) {
      return status;
    }
  }
  return rocksdb::Status::OK();
}

}  // namespace minikv
