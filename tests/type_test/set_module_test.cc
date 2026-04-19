#include <cstdint>
#include <algorithm>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "runtime/module/module_services.h"
#include "runtime/config.h"
#include "gtest/gtest.h"
#include "execution/scheduler/scheduler.h"
#include "storage/encoding/key_codec.h"
#include "storage/engine/storage_engine.h"
#include "storage/engine/write_context.h"
#include "runtime/module/module.h"
#include "runtime/module/module_manager.h"
#include "core/core_module.h"
#include "core/key_service.h"
#include "types/list/list_module.h"
#include "types/set/set_module.h"
#include "rocksdb/db.h"

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

std::string EncodeSetMemberLocalKey(const std::string& key, uint64_t version,
                                    const std::string& member) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  out.append(member);
  return out;
}

void ExpectMembersUnordered(const std::vector<std::string>& actual,
                            const std::vector<std::string>& expected) {
  std::vector<std::string> actual_sorted = actual;
  std::vector<std::string> expected_sorted = expected;
  std::sort(actual_sorted.begin(), actual_sorted.end());
  std::sort(expected_sorted.begin(), expected_sorted.end());
  EXPECT_EQ(actual_sorted, expected_sorted);
}

class SetModuleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-set-module-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
                   .string();
    OpenModule();
  }

  void TearDown() override {
    module_manager_.reset();
    scheduler_.reset();
    set_module_ = nullptr;
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
    auto set_module = std::make_unique<minikv::SetModule>();
    set_module_ = set_module.get();
    modules.push_back(std::move(set_module));
    modules.push_back(std::make_unique<minikv::ListModule>());
    module_manager_ = std::make_unique<minikv::ModuleManager>(
        storage_engine_.get(), scheduler_.get(), std::move(modules));
    ASSERT_TRUE(module_manager_->Initialize().ok());
  }

  void CloseModule() {
    module_manager_.reset();
    scheduler_.reset();
    set_module_ = nullptr;
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

  void PutRawSetMember(const std::string& key, uint64_t version,
                       const std::string& member) {
    minikv::WriteContext write_context(storage_engine_.get());
    const minikv::ModuleKeyspace members_keyspace("set", "members");
    ASSERT_TRUE(write_context
                    .Put(minikv::StorageColumnFamily::kSet,
                         members_keyspace.EncodeKey(
                             EncodeSetMemberLocalKey(key, version, member)),
                         "")
                    .ok());
    ASSERT_TRUE(write_context.Commit().ok());
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
    ASSERT_TRUE(
        set_module_->DeleteWholeKey(snapshot.get(), write_batch.get(), key, lookup)
            .ok());
    ASSERT_TRUE(write_batch->Commit().ok());
  }

  static inline int counter_ = 0;
  std::string db_path_;
  uint64_t current_time_ms_ = 10'000;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  minikv::SetModule* set_module_ = nullptr;
};

TEST_F(SetModuleTest, AddMembersReadMembersAndMembership) {
  uint64_t added = 0;
  ASSERT_TRUE(
      set_module_->AddMembers("set:1", {"alice", "bob", "alice"}, &added).ok());
  ASSERT_EQ(added, 2U);
  ASSERT_TRUE(set_module_->AddMembers("set:1", {"bob", "carol"}, &added).ok());
  ASSERT_EQ(added, 1U);

  uint64_t size = 0;
  ASSERT_TRUE(set_module_->Cardinality("set:1", &size).ok());
  EXPECT_EQ(size, 3U);

  std::vector<std::string> members;
  ASSERT_TRUE(set_module_->ReadMembers("set:1", &members).ok());
  ExpectMembersUnordered(members, {"alice", "bob", "carol"});

  bool found = false;
  ASSERT_TRUE(set_module_->IsMember("set:1", "alice", &found).ok());
  EXPECT_TRUE(found);
  ASSERT_TRUE(set_module_->IsMember("set:1", "dave", &found).ok());
  EXPECT_FALSE(found);
}

TEST_F(SetModuleTest, RemoveMembersWritesTombstoneAfterLastDelete) {
  ASSERT_TRUE(set_module_->AddMembers("set:2", {"a", "b"}, nullptr).ok());

  uint64_t removed = 0;
  ASSERT_TRUE(set_module_->RemoveMembers("set:2", {"a", "a", "z"}, &removed).ok());
  ASSERT_EQ(removed, 1U);

  std::vector<std::string> members;
  ASSERT_TRUE(set_module_->ReadMembers("set:2", &members).ok());
  ExpectMembersUnordered(members, {"b"});

  ASSERT_TRUE(set_module_->RemoveMembers("set:2", {"b"}, &removed).ok());
  ASSERT_EQ(removed, 1U);
  ASSERT_TRUE(set_module_->ReadMembers("set:2", &members).ok());
  ASSERT_TRUE(members.empty());

  const minikv::KeyMetadata tombstone = ReadRawMetadata("set:2");
  EXPECT_EQ(tombstone.size, 0U);
  EXPECT_EQ(tombstone.expire_at_ms, minikv::kLogicalDeleteExpireAtMs);

  const minikv::KeyLookup lookup = LookupKey("set:2");
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
}

TEST_F(SetModuleTest, MissingKeyOperationsReturnEmptySuccess) {
  uint64_t size = 42;
  ASSERT_TRUE(set_module_->Cardinality("missing", &size).ok());
  EXPECT_EQ(size, 0U);

  std::vector<std::string> members;
  ASSERT_TRUE(set_module_->ReadMembers("missing", &members).ok());
  ASSERT_TRUE(members.empty());

  bool found = true;
  ASSERT_TRUE(set_module_->IsMember("missing", "a", &found).ok());
  EXPECT_FALSE(found);

  uint64_t removed = 42;
  ASSERT_TRUE(set_module_->RemoveMembers("missing", {"a", "b"}, &removed).ok());
  EXPECT_EQ(removed, 0U);

  std::string member = "seed";
  ASSERT_TRUE(set_module_->RandomMember("missing", &member, &found).ok());
  EXPECT_FALSE(found);
  EXPECT_TRUE(member.empty());

  member = "seed";
  found = true;
  ASSERT_TRUE(set_module_->PopRandomMember("missing", &member, &found).ok());
  EXPECT_FALSE(found);
  EXPECT_TRUE(member.empty());
}

TEST_F(SetModuleTest, ReopenPreservesSetData) {
  ASSERT_TRUE(set_module_->AddMembers("set:reopen", {"name", "city"}, nullptr)
                  .ok());
  ReopenModule();

  uint64_t size = 0;
  ASSERT_TRUE(set_module_->Cardinality("set:reopen", &size).ok());
  EXPECT_EQ(size, 2U);

  std::vector<std::string> members;
  ASSERT_TRUE(set_module_->ReadMembers("set:reopen", &members).ok());
  ExpectMembersUnordered(members, {"name", "city"});
}

TEST_F(SetModuleTest, NonSetMetadataStillReturnsTypeMismatch) {
  minikv::KeyMetadata metadata;
  metadata.type = minikv::ObjectType::kString;
  metadata.encoding = minikv::ObjectEncoding::kRaw;
  metadata.version = 3;
  PutRawMetadata("set:string", metadata);

  uint64_t size = 0;
  rocksdb::Status status =
      set_module_->AddMembers("set:string", {"member"}, nullptr);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  std::vector<std::string> members;
  status = set_module_->ReadMembers("set:string", &members);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  status = set_module_->Cardinality("set:string", &size);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  bool found = false;
  status = set_module_->IsMember("set:string", "member", &found);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  uint64_t removed = 0;
  status = set_module_->RemoveMembers("set:string", {"member"}, &removed);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  std::string member;
  status = set_module_->RandomMember("set:string", &member, &found);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  status = set_module_->PopRandomMember("set:string", &member, &found);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);
}

TEST_F(SetModuleTest, ExpiredMetadataRebuildBumpsVersion) {
  minikv::KeyMetadata expired;
  expired.type = minikv::ObjectType::kSet;
  expired.encoding = minikv::ObjectEncoding::kSetHashtable;
  expired.version = 7;
  expired.size = 1;
  expired.expire_at_ms = current_time_ms_ - 1;
  PutRawMetadata("set:expired", expired);
  PutRawSetMember("set:expired", expired.version, "stale");

  std::vector<std::string> members;
  ASSERT_TRUE(set_module_->ReadMembers("set:expired", &members).ok());
  ASSERT_TRUE(members.empty());

  const minikv::KeyLookup lookup = LookupKey("set:expired");
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kExpired);

  uint64_t added = 0;
  ASSERT_TRUE(set_module_->AddMembers("set:expired", {"fresh"}, &added).ok());
  ASSERT_EQ(added, 1U);

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("set:expired");
  EXPECT_EQ(rebuilt.type, minikv::ObjectType::kSet);
  EXPECT_EQ(rebuilt.encoding, minikv::ObjectEncoding::kSetHashtable);
  EXPECT_EQ(rebuilt.version, 8U);
  EXPECT_EQ(rebuilt.size, 1U);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);

  ASSERT_TRUE(set_module_->ReadMembers("set:expired", &members).ok());
  ExpectMembersUnordered(members, {"fresh"});
}

TEST_F(SetModuleTest, TombstoneSurvivesReopenAndRecreateBumpsVersion) {
  ASSERT_TRUE(set_module_->AddMembers("set:tombstone", {"a", "b"}, nullptr).ok());

  const minikv::KeyMetadata before_delete = ReadRawMetadata("set:tombstone");
  DeleteWholeKey("set:tombstone");

  minikv::KeyLookup lookup = LookupKey("set:tombstone");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);

  ReopenModule();

  lookup = LookupKey("set:tombstone");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);

  std::vector<std::string> members;
  ASSERT_TRUE(set_module_->ReadMembers("set:tombstone", &members).ok());
  ASSERT_TRUE(members.empty());

  uint64_t added = 0;
  ASSERT_TRUE(set_module_->AddMembers("set:tombstone", {"fresh"}, &added).ok());
  ASSERT_EQ(added, 1U);

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("set:tombstone");
  EXPECT_EQ(rebuilt.version, before_delete.version + 1);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);

  ASSERT_TRUE(set_module_->ReadMembers("set:tombstone", &members).ok());
  ExpectMembersUnordered(members, {"fresh"});
}

TEST_F(SetModuleTest, RandomMemberDoesNotRemoveAndPopRandomMemberDoes) {
  ASSERT_TRUE(set_module_->AddMembers("set:random", {"a", "b", "c"}, nullptr)
                  .ok());

  std::string member;
  bool found = false;
  ASSERT_TRUE(set_module_->RandomMember("set:random", &member, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_TRUE(member == "a" || member == "b" || member == "c");

  uint64_t size = 0;
  ASSERT_TRUE(set_module_->Cardinality("set:random", &size).ok());
  EXPECT_EQ(size, 3U);

  ASSERT_TRUE(set_module_->PopRandomMember("set:random", &member, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_TRUE(member == "a" || member == "b" || member == "c");

  ASSERT_TRUE(set_module_->Cardinality("set:random", &size).ok());
  EXPECT_EQ(size, 2U);

  ASSERT_TRUE(set_module_->IsMember("set:random", member, &found).ok());
  EXPECT_FALSE(found);
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
