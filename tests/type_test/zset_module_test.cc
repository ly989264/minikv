#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "core/core_module.h"
#include "core/key_service.h"
#include "execution/scheduler/scheduler.h"
#include "gtest/gtest.h"
#include "rocksdb/db.h"
#include "runtime/config.h"
#include "runtime/module/module.h"
#include "runtime/module/module_manager.h"
#include "runtime/module/module_services.h"
#include "storage/encoding/key_codec.h"
#include "storage/engine/storage_engine.h"
#include "storage/engine/write_context.h"
#include "types/zset/zset_module.h"

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

uint64_t DoubleToBits(double value) {
  uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

uint64_t EncodeSortableScore(double score) {
  if (score == 0) {
    score = 0.0;
  }
  const uint64_t bits = DoubleToBits(score);
  if ((bits & (1ull << 63)) != 0) {
    return ~bits;
  }
  return bits ^ (1ull << 63);
}

std::string EncodeZSetMemberLocalKey(const std::string& key, uint64_t version,
                                     const std::string& member) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  out.append(member);
  return out;
}

std::string EncodeZSetScoreIndexLocalKey(const std::string& key, uint64_t version,
                                         double score,
                                         const std::string& member) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  AppendUint64(&out, EncodeSortableScore(score));
  out.append(member);
  return out;
}

std::string EncodeScoreValue(double score) {
  std::string out;
  AppendUint64(&out, DoubleToBits(score == 0 ? 0.0 : score));
  return out;
}

void ExpectMembers(const std::vector<std::string>& actual,
                   const std::vector<std::string>& expected) {
  EXPECT_EQ(actual, expected);
}

class ZSetModuleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-zset-module-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
                   .string();
    OpenModule();
  }

  void TearDown() override {
    module_manager_.reset();
    scheduler_.reset();
    zset_module_ = nullptr;
    storage_engine_.reset();
    rocksdb::Options options;
    ASSERT_TRUE(rocksdb::DestroyDB(db_path_, options).ok());
  }

  void OpenModule() {
    minikv::Config config;
    config.db_path = db_path_;
    storage_engine_ = std::make_unique<minikv::StorageEngine>();
    ASSERT_TRUE(storage_engine_->Open(config).ok());
    scheduler_ = std::make_unique<minikv::Scheduler>(1, 16);

    std::vector<std::unique_ptr<minikv::Module>> modules;
    modules.push_back(std::make_unique<minikv::CoreModule>(
        [this]() { return current_time_ms_; }));
    auto zset_module = std::make_unique<minikv::ZSetModule>();
    zset_module_ = zset_module.get();
    modules.push_back(std::move(zset_module));

    module_manager_ = std::make_unique<minikv::ModuleManager>(
        storage_engine_.get(), scheduler_.get(), std::move(modules));
    ASSERT_TRUE(module_manager_->Initialize().ok());
  }

  void CloseModule() {
    module_manager_.reset();
    scheduler_.reset();
    zset_module_ = nullptr;
    storage_engine_.reset();
  }

  void ReopenModule() {
    CloseModule();
    OpenModule();
  }

  void PutRawMetadata(const std::string& key,
                      const minikv::KeyMetadata& metadata) {
    minikv::WriteContext write_context(storage_engine_.get());
    ASSERT_TRUE(write_context
                    .Put(minikv::StorageColumnFamily::kMeta,
                         minikv::KeyCodec::EncodeMetaKey(key),
                         minikv::DefaultCoreKeyService::EncodeMetadataValue(
                             metadata))
                    .ok());
    ASSERT_TRUE(write_context.Commit().ok());
  }

  minikv::KeyMetadata ReadRawMetadata(const std::string& key) const {
    std::string raw_meta;
    EXPECT_TRUE(storage_engine_
                    ->Get(minikv::StorageColumnFamily::kMeta,
                          minikv::KeyCodec::EncodeMetaKey(key), &raw_meta)
                    .ok());
    minikv::KeyMetadata metadata;
    EXPECT_TRUE(minikv::DefaultCoreKeyService::DecodeMetadataValue(raw_meta,
                                                                   &metadata));
    return metadata;
  }

  void PutRawZSetMember(const std::string& key, uint64_t version,
                        const std::string& member, double score) {
    minikv::WriteContext write_context(storage_engine_.get());
    const minikv::ModuleKeyspace members_keyspace("zset", "members");
    const minikv::ModuleKeyspace score_index_keyspace("zset", "score_index");
    ASSERT_TRUE(write_context
                    .Put(minikv::StorageColumnFamily::kModule,
                         members_keyspace.EncodeKey(
                             EncodeZSetMemberLocalKey(key, version, member)),
                         EncodeScoreValue(score))
                    .ok());
    ASSERT_TRUE(write_context
                    .Put(minikv::StorageColumnFamily::kModule,
                         score_index_keyspace.EncodeKey(EncodeZSetScoreIndexLocalKey(
                             key, version, score, member)),
                         "")
                    .ok());
    ASSERT_TRUE(write_context.Commit().ok());
  }

  bool HasModuleEntry(const minikv::ModuleKeyspace& keyspace,
                      const std::string& local_key) const {
    std::string value;
    return storage_engine_
        ->Get(minikv::StorageColumnFamily::kModule, keyspace.EncodeKey(local_key),
              &value)
        .ok();
  }

  minikv::DefaultCoreKeyService MakeKeyService() const {
    return minikv::DefaultCoreKeyService([this]() { return current_time_ms_; });
  }

  std::unique_ptr<minikv::ModuleSnapshot> CreateSnapshot() const {
    minikv::ModuleSnapshotService snapshots(minikv::ModuleNamespace("core"),
                                            storage_engine_.get());
    return snapshots.Create();
  }

  std::unique_ptr<minikv::ModuleWriteBatch> CreateWriteBatch() const {
    minikv::ModuleStorage storage(minikv::ModuleNamespace("core"),
                                  storage_engine_.get());
    return storage.CreateWriteBatch();
  }

  minikv::KeyLookup LookupKey(const std::string& key) const {
    minikv::DefaultCoreKeyService key_service = MakeKeyService();
    std::unique_ptr<minikv::ModuleSnapshot> snapshot = CreateSnapshot();
    minikv::KeyLookup lookup;
    EXPECT_TRUE(key_service.Lookup(snapshot.get(), key, &lookup).ok());
    return lookup;
  }

  void DeleteWholeKey(const std::string& key) {
    minikv::DefaultCoreKeyService key_service = MakeKeyService();
    std::unique_ptr<minikv::ModuleSnapshot> snapshot = CreateSnapshot();
    std::unique_ptr<minikv::ModuleWriteBatch> write_batch = CreateWriteBatch();
    minikv::KeyLookup lookup;
    ASSERT_TRUE(key_service.Lookup(snapshot.get(), key, &lookup).ok());
    ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kLive);
    ASSERT_TRUE(zset_module_
                    ->DeleteWholeKey(snapshot.get(), write_batch.get(), key, lookup)
                    .ok());
    ASSERT_TRUE(write_batch->Commit().ok());
  }

  static inline int counter_ = 0;
  std::string db_path_;
  uint64_t current_time_ms_ = 10'000;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  minikv::ZSetModule* zset_module_ = nullptr;
};

TEST_F(ZSetModuleTest, AddMembersCountRangeRankAndScoreWorkTogether) {
  uint64_t added = 0;
  ASSERT_TRUE(zset_module_
                  ->AddMembers("zset:1",
                               {{"alice", 2.5}, {"bob", 1.0}, {"alice", 3.0}},
                               &added)
                  .ok());
  EXPECT_EQ(added, 2U);

  ASSERT_TRUE(
      zset_module_->AddMembers("zset:1", {{"bob", 4.0}, {"carol", 4.0}}, &added)
          .ok());
  EXPECT_EQ(added, 1U);

  uint64_t size = 0;
  ASSERT_TRUE(zset_module_->Cardinality("zset:1", &size).ok());
  EXPECT_EQ(size, 3U);

  std::vector<std::string> members;
  ASSERT_TRUE(zset_module_->RangeByRank("zset:1", 0, -1, &members).ok());
  ExpectMembers(members, {"alice", "bob", "carol"});

  ASSERT_TRUE(zset_module_->RangeByScore("zset:1", "(3", "+inf", &members).ok());
  ExpectMembers(members, {"bob", "carol"});

  uint64_t count = 0;
  ASSERT_TRUE(zset_module_->CountByScore("zset:1", "3", "4", &count).ok());
  EXPECT_EQ(count, 3U);

  double score = 0;
  bool found = false;
  ASSERT_TRUE(zset_module_->Score("zset:1", "bob", &score, &found).ok());
  EXPECT_TRUE(found);
  EXPECT_DOUBLE_EQ(score, 4.0);

  uint64_t rank = 0;
  ASSERT_TRUE(zset_module_->Rank("zset:1", "carol", &rank, &found).ok());
  EXPECT_TRUE(found);
  EXPECT_EQ(rank, 2U);
}

TEST_F(ZSetModuleTest, IncrementByCreatesUpdatesAndReordersMembers) {
  double score = 0;
  ASSERT_TRUE(zset_module_->IncrementBy("zset:incr", 1.5, "dave", &score).ok());
  EXPECT_DOUBLE_EQ(score, 1.5);

  ASSERT_TRUE(zset_module_->AddMembers("zset:incr", {{"erin", 1.0}, {"frank", 3.0}},
                                       nullptr)
                  .ok());

  ASSERT_TRUE(zset_module_->IncrementBy("zset:incr", 3.0, "erin", &score).ok());
  EXPECT_DOUBLE_EQ(score, 4.0);

  std::vector<std::string> members;
  ASSERT_TRUE(zset_module_->RangeByRank("zset:incr", 0, -1, &members).ok());
  ExpectMembers(members, {"dave", "frank", "erin"});
}

TEST_F(ZSetModuleTest, LexCommandsUseMemberOrdering) {
  ASSERT_TRUE(zset_module_
                  ->AddMembers("zset:lex",
                               {{"aa", 0.0}, {"ab", 0.0}, {"ac", 0.0},
                                {"ad", 0.0}},
                               nullptr)
                  .ok());

  uint64_t count = 0;
  ASSERT_TRUE(zset_module_->CountByLex("zset:lex", "[ab", "(ad", &count).ok());
  EXPECT_EQ(count, 2U);

  std::vector<std::string> members;
  ASSERT_TRUE(zset_module_->RangeByLex("zset:lex", "[ab", "(ad", &members).ok());
  ExpectMembers(members, {"ab", "ac"});
}

TEST_F(ZSetModuleTest, RemoveMembersWritesTombstoneAfterLastDelete) {
  ASSERT_TRUE(
      zset_module_->AddMembers("zset:remove", {{"a", 1.0}, {"b", 2.0}}, nullptr)
          .ok());

  uint64_t removed = 0;
  ASSERT_TRUE(
      zset_module_->RemoveMembers("zset:remove", {"a", "a", "z"}, &removed).ok());
  EXPECT_EQ(removed, 1U);

  std::vector<std::string> members;
  ASSERT_TRUE(zset_module_->RangeByRank("zset:remove", 0, -1, &members).ok());
  ExpectMembers(members, {"b"});

  ASSERT_TRUE(zset_module_->RemoveMembers("zset:remove", {"b"}, &removed).ok());
  EXPECT_EQ(removed, 1U);
  ASSERT_TRUE(zset_module_->RangeByRank("zset:remove", 0, -1, &members).ok());
  EXPECT_TRUE(members.empty());

  const minikv::KeyMetadata tombstone = ReadRawMetadata("zset:remove");
  EXPECT_EQ(tombstone.size, 0U);
  EXPECT_EQ(tombstone.expire_at_ms, minikv::kLogicalDeleteExpireAtMs);

  const minikv::KeyLookup lookup = LookupKey("zset:remove");
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
}

TEST_F(ZSetModuleTest, MissingKeyOperationsReturnEmptySuccess) {
  uint64_t size = 7;
  ASSERT_TRUE(zset_module_->Cardinality("missing", &size).ok());
  EXPECT_EQ(size, 0U);

  uint64_t count = 9;
  ASSERT_TRUE(zset_module_->CountByScore("missing", "-inf", "+inf", &count).ok());
  EXPECT_EQ(count, 0U);

  ASSERT_TRUE(zset_module_->CountByLex("missing", "-", "+", &count).ok());
  EXPECT_EQ(count, 0U);

  std::vector<std::string> members;
  ASSERT_TRUE(zset_module_->RangeByRank("missing", 0, -1, &members).ok());
  EXPECT_TRUE(members.empty());

  ASSERT_TRUE(zset_module_->RangeByScore("missing", "-inf", "+inf", &members).ok());
  EXPECT_TRUE(members.empty());

  ASSERT_TRUE(zset_module_->RangeByLex("missing", "-", "+", &members).ok());
  EXPECT_TRUE(members.empty());

  bool found = true;
  double score = 7;
  ASSERT_TRUE(zset_module_->Score("missing", "a", &score, &found).ok());
  EXPECT_FALSE(found);
  EXPECT_EQ(score, 0.0);

  uint64_t rank = 1;
  ASSERT_TRUE(zset_module_->Rank("missing", "a", &rank, &found).ok());
  EXPECT_FALSE(found);
  EXPECT_EQ(rank, 0U);

  uint64_t removed = 5;
  ASSERT_TRUE(zset_module_->RemoveMembers("missing", {"a", "b"}, &removed).ok());
  EXPECT_EQ(removed, 0U);
}

TEST_F(ZSetModuleTest, ReopenPreservesZSetData) {
  ASSERT_TRUE(zset_module_
                  ->AddMembers("zset:reopen",
                               {{"alice", 1.0}, {"bob", 2.0}, {"carol", 2.0}},
                               nullptr)
                  .ok());
  ReopenModule();

  std::vector<std::string> members;
  ASSERT_TRUE(zset_module_->RangeByRank("zset:reopen", 0, -1, &members).ok());
  ExpectMembers(members, {"alice", "bob", "carol"});
}

TEST_F(ZSetModuleTest, NonZSetMetadataStillReturnsTypeMismatch) {
  minikv::KeyMetadata metadata;
  metadata.type = minikv::ObjectType::kString;
  metadata.encoding = minikv::ObjectEncoding::kRaw;
  metadata.version = 3;
  PutRawMetadata("zset:string", metadata);

  rocksdb::Status status =
      zset_module_->AddMembers("zset:string", {{"member", 1.0}}, nullptr);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  uint64_t size = 0;
  status = zset_module_->Cardinality("zset:string", &size);
  ASSERT_TRUE(status.IsInvalidArgument());

  uint64_t count = 0;
  status = zset_module_->CountByScore("zset:string", "-inf", "+inf", &count);
  ASSERT_TRUE(status.IsInvalidArgument());

  std::vector<std::string> members;
  status = zset_module_->RangeByRank("zset:string", 0, -1, &members);
  ASSERT_TRUE(status.IsInvalidArgument());

  double score = 0;
  bool found = false;
  status = zset_module_->Score("zset:string", "member", &score, &found);
  ASSERT_TRUE(status.IsInvalidArgument());

  status = zset_module_->IncrementBy("zset:string", 1.0, "member", &score);
  ASSERT_TRUE(status.IsInvalidArgument());
}

TEST_F(ZSetModuleTest, ExpiredMetadataRebuildBumpsVersion) {
  minikv::KeyMetadata expired;
  expired.type = minikv::ObjectType::kZSet;
  expired.encoding = minikv::ObjectEncoding::kZSetSkiplist;
  expired.version = 7;
  expired.size = 1;
  expired.expire_at_ms = current_time_ms_ - 1;
  PutRawMetadata("zset:expired", expired);
  PutRawZSetMember("zset:expired", expired.version, "stale", 4.0);

  std::vector<std::string> members;
  ASSERT_TRUE(zset_module_->RangeByRank("zset:expired", 0, -1, &members).ok());
  EXPECT_TRUE(members.empty());

  const minikv::KeyLookup lookup = LookupKey("zset:expired");
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kExpired);

  uint64_t added = 0;
  ASSERT_TRUE(
      zset_module_->AddMembers("zset:expired", {{"fresh", 2.0}}, &added).ok());
  EXPECT_EQ(added, 1U);

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("zset:expired");
  EXPECT_EQ(rebuilt.type, minikv::ObjectType::kZSet);
  EXPECT_EQ(rebuilt.encoding, minikv::ObjectEncoding::kZSetSkiplist);
  EXPECT_EQ(rebuilt.version, 8U);
  EXPECT_EQ(rebuilt.size, 1U);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);

  ASSERT_TRUE(zset_module_->RangeByRank("zset:expired", 0, -1, &members).ok());
  ExpectMembers(members, {"fresh"});
}

TEST_F(ZSetModuleTest, TombstoneSurvivesReopenAndRecreateBumpsVersion) {
  ASSERT_TRUE(zset_module_
                  ->AddMembers("zset:tombstone", {{"a", 1.0}, {"b", 2.0}}, nullptr)
                  .ok());

  const minikv::KeyMetadata before_delete = ReadRawMetadata("zset:tombstone");
  DeleteWholeKey("zset:tombstone");

  minikv::KeyLookup lookup = LookupKey("zset:tombstone");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);

  ReopenModule();

  lookup = LookupKey("zset:tombstone");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);

  std::vector<std::string> members;
  ASSERT_TRUE(zset_module_->RangeByRank("zset:tombstone", 0, -1, &members).ok());
  EXPECT_TRUE(members.empty());

  uint64_t added = 0;
  ASSERT_TRUE(
      zset_module_->AddMembers("zset:tombstone", {{"fresh", 1.0}}, &added).ok());
  EXPECT_EQ(added, 1U);

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("zset:tombstone");
  EXPECT_EQ(rebuilt.version, before_delete.version + 1);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);

  ASSERT_TRUE(zset_module_->RangeByRank("zset:tombstone", 0, -1, &members).ok());
  ExpectMembers(members, {"fresh"});
}

TEST_F(ZSetModuleTest, DeleteWholeKeyRemovesMemberAndScoreIndexEntries) {
  ASSERT_TRUE(zset_module_
                  ->AddMembers("zset:delete", {{"alpha", 1.0}, {"beta", 2.0}},
                               nullptr)
                  .ok());

  const minikv::KeyMetadata metadata = ReadRawMetadata("zset:delete");
  DeleteWholeKey("zset:delete");

  const minikv::ModuleKeyspace members_keyspace("zset", "members");
  const minikv::ModuleKeyspace score_index_keyspace("zset", "score_index");
  EXPECT_FALSE(HasModuleEntry(
      members_keyspace,
      EncodeZSetMemberLocalKey("zset:delete", metadata.version, "alpha")));
  EXPECT_FALSE(HasModuleEntry(
      members_keyspace,
      EncodeZSetMemberLocalKey("zset:delete", metadata.version, "beta")));
  EXPECT_FALSE(HasModuleEntry(
      score_index_keyspace,
      EncodeZSetScoreIndexLocalKey("zset:delete", metadata.version, 1.0,
                                   "alpha")));
  EXPECT_FALSE(HasModuleEntry(
      score_index_keyspace,
      EncodeZSetScoreIndexLocalKey("zset:delete", metadata.version, 2.0,
                                   "beta")));
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
