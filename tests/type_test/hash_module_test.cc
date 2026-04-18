#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "storage/encoding/key_codec.h"
#include "runtime/config.h"
#include "gtest/gtest.h"
#include "execution/scheduler/scheduler.h"
#include "storage/engine/storage_engine.h"
#include "storage/engine/write_context.h"
#include "runtime/module/module.h"
#include "runtime/module/module_manager.h"
#include "runtime/module/module_services.h"
#include "core/core_module.h"
#include "core/key_service.h"
#include "types/hash/hash_module.h"
#include "rocksdb/db.h"

namespace {

class HashModuleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-hash-module-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
                   .string();
    OpenModule();
  }

  void TearDown() override {
    module_manager_.reset();
    scheduler_.reset();
    hash_module_ = nullptr;
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
    auto hash_module = std::make_unique<minikv::HashModule>();
    hash_module_ = hash_module.get();
    modules.push_back(std::move(hash_module));
    module_manager_ = std::make_unique<minikv::ModuleManager>(
        storage_engine_.get(), scheduler_.get(), std::move(modules));
    ASSERT_TRUE(module_manager_->Initialize().ok());
  }

  void CloseModule() {
    module_manager_.reset();
    scheduler_.reset();
    hash_module_ = nullptr;
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
        hash_module_->DeleteWholeKey(snapshot.get(), write_batch.get(), key, lookup)
            .ok());
    ASSERT_TRUE(write_batch->Commit().ok());
  }

  static inline int counter_ = 0;
  std::string db_path_;
  uint64_t current_time_ms_ = 10'000;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  minikv::HashModule* hash_module_ = nullptr;
};

TEST_F(HashModuleTest, PutFieldAndReadAll) {
  bool inserted = false;
  ASSERT_TRUE(hash_module_->PutField("user:1", "name", "alice", &inserted).ok());
  ASSERT_TRUE(inserted);
  ASSERT_TRUE(
      hash_module_->PutField("user:1", "city", "shanghai", &inserted).ok());
  ASSERT_TRUE(inserted);
  ASSERT_TRUE(
      hash_module_->PutField("user:1", "name", "alice-2", &inserted).ok());
  ASSERT_FALSE(inserted);

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("user:1", &values).ok());
  ASSERT_EQ(values.size(), 2U);
}

TEST_F(HashModuleTest, DeleteFieldsRemovesFieldsAndWritesTombstone) {
  ASSERT_TRUE(hash_module_->PutField("user:2", "a", "1", nullptr).ok());
  ASSERT_TRUE(hash_module_->PutField("user:2", "b", "2", nullptr).ok());

  uint64_t deleted = 0;
  ASSERT_TRUE(hash_module_->DeleteFields("user:2", {"a"}, &deleted).ok());
  ASSERT_EQ(deleted, 1U);

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("user:2", &values).ok());
  ASSERT_EQ(values.size(), 1U);

  ASSERT_TRUE(hash_module_->DeleteFields("user:2", {"b"}, &deleted).ok());
  ASSERT_EQ(deleted, 1U);
  ASSERT_TRUE(hash_module_->ReadAll("user:2", &values).ok());
  ASSERT_TRUE(values.empty());

  const minikv::KeyMetadata tombstone = ReadRawMetadata("user:2");
  EXPECT_EQ(tombstone.size, 0U);
  EXPECT_EQ(tombstone.expire_at_ms, minikv::kLogicalDeleteExpireAtMs);

  const minikv::KeyLookup lookup = LookupKey("user:2");
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
}

TEST_F(HashModuleTest, MissingKeyOperationsReturnEmptySuccess) {
  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("missing", &values).ok());
  ASSERT_TRUE(values.empty());

  uint64_t deleted = 42;
  ASSERT_TRUE(hash_module_->DeleteFields("missing", {"a", "b"}, &deleted).ok());
  ASSERT_EQ(deleted, 0U);
}

TEST_F(HashModuleTest, DeleteCountsOnlyExistingFields) {
  ASSERT_TRUE(hash_module_->PutField("user:4", "a", "1", nullptr).ok());
  ASSERT_TRUE(hash_module_->PutField("user:4", "b", "2", nullptr).ok());

  uint64_t deleted = 0;
  ASSERT_TRUE(
      hash_module_->DeleteFields("user:4", {"a", "x", "b", "y"}, &deleted).ok());
  ASSERT_EQ(deleted, 2U);

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("user:4", &values).ok());
  ASSERT_TRUE(values.empty());
}

TEST_F(HashModuleTest, ReopenPreservesHashData) {
  ASSERT_TRUE(hash_module_->PutField("user:reopen", "name", "alice", nullptr).ok());
  ReopenModule();

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("user:reopen", &values).ok());
  ASSERT_EQ(values.size(), 1U);
  ASSERT_EQ(values[0].field, "name");
  ASSERT_EQ(values[0].value, "alice");
}

TEST_F(HashModuleTest, NonHashMetadataStillReturnsTypeMismatch) {
  minikv::KeyMetadata metadata;
  metadata.type = minikv::ObjectType::kString;
  metadata.encoding = minikv::ObjectEncoding::kRaw;
  metadata.version = 3;
  PutRawMetadata("user:string", metadata);

  bool inserted = false;
  rocksdb::Status status =
      hash_module_->PutField("user:string", "name", "alice", &inserted);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);
  EXPECT_FALSE(inserted);

  std::vector<minikv::FieldValue> values;
  status = hash_module_->ReadAll("user:string", &values);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  uint64_t deleted = 0;
  status = hash_module_->DeleteFields("user:string", {"name"}, &deleted);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);
  EXPECT_EQ(deleted, 0U);
}

TEST_F(HashModuleTest, LookupReturnsExplicitLifecycleStates) {
  minikv::DefaultCoreKeyService key_service = MakeKeyService();
  std::unique_ptr<minikv::ModuleSnapshot> snapshot = CreateSnapshot();

  minikv::KeyLookup lookup;
  ASSERT_TRUE(key_service.Lookup(snapshot.get(), "missing", &lookup).ok());
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kMissing);

  minikv::KeyMetadata expired;
  expired.type = minikv::ObjectType::kHash;
  expired.encoding = minikv::ObjectEncoding::kHashPlain;
  expired.version = 3;
  expired.size = 1;
  expired.expire_at_ms = current_time_ms_ - 1;
  PutRawMetadata("user:expired-state", expired);

  minikv::KeyMetadata tombstone = expired;
  tombstone.version = 4;
  tombstone.expire_at_ms = minikv::kLogicalDeleteExpireAtMs;
  PutRawMetadata("user:tombstone-state", tombstone);

  snapshot = CreateSnapshot();
  ASSERT_TRUE(key_service.Lookup(snapshot.get(), "user:expired-state", &lookup).ok());
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kExpired);

  ASSERT_TRUE(
      key_service.Lookup(snapshot.get(), "user:tombstone-state", &lookup).ok());
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
}

TEST_F(HashModuleTest, ExpiredMetadataRebuildBumpsVersion) {
  minikv::KeyMetadata expired;
  expired.type = minikv::ObjectType::kHash;
  expired.encoding = minikv::ObjectEncoding::kHashPlain;
  expired.version = 7;
  expired.size = 1;
  expired.expire_at_ms = current_time_ms_ - 1;

  minikv::WriteContext write_context(storage_engine_.get());
  ASSERT_TRUE(write_context
                  .Put(minikv::StorageColumnFamily::kMeta,
                       minikv::KeyCodec::EncodeMetaKey("user:expired"),
                       minikv::DefaultCoreKeyService::EncodeMetadataValue(
                           expired))
                  .ok());
  ASSERT_TRUE(write_context
                  .Put(minikv::StorageColumnFamily::kHash,
                       minikv::KeyCodec::EncodeHashDataKey(
                           "user:expired", expired.version, "stale"),
                       "old-value")
                  .ok());
  ASSERT_TRUE(write_context.Commit().ok());

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("user:expired", &values).ok());
  ASSERT_TRUE(values.empty());

  const minikv::KeyLookup lookup = LookupKey("user:expired");
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kExpired);

  bool inserted = false;
  ASSERT_TRUE(
      hash_module_->PutField("user:expired", "fresh", "new-value", &inserted)
          .ok());
  ASSERT_TRUE(inserted);

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("user:expired");
  EXPECT_EQ(rebuilt.type, minikv::ObjectType::kHash);
  EXPECT_EQ(rebuilt.encoding, minikv::ObjectEncoding::kHashPlain);
  EXPECT_EQ(rebuilt.version, 8U);
  EXPECT_EQ(rebuilt.size, 1U);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);

  ASSERT_TRUE(hash_module_->ReadAll("user:expired", &values).ok());
  ASSERT_EQ(values.size(), 1U);
  EXPECT_EQ(values[0].field, "fresh");
  EXPECT_EQ(values[0].value, "new-value");
}

TEST_F(HashModuleTest, TombstoneSurvivesReopenAndRecreateBumpsVersion) {
  ASSERT_TRUE(hash_module_->PutField("user:tombstone", "name", "alice", nullptr)
                  .ok());
  ASSERT_TRUE(hash_module_->PutField("user:tombstone", "city", "shanghai",
                                     nullptr)
                  .ok());

  const minikv::KeyMetadata before_delete = ReadRawMetadata("user:tombstone");
  DeleteWholeKey("user:tombstone");

  minikv::KeyLookup lookup = LookupKey("user:tombstone");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);

  ReopenModule();

  lookup = LookupKey("user:tombstone");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("user:tombstone", &values).ok());
  ASSERT_TRUE(values.empty());

  bool inserted = false;
  ASSERT_TRUE(hash_module_->PutField("user:tombstone", "fresh", "new-value",
                                     &inserted)
                  .ok());
  ASSERT_TRUE(inserted);

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("user:tombstone");
  EXPECT_EQ(rebuilt.version, before_delete.version + 1);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);

  ASSERT_TRUE(hash_module_->ReadAll("user:tombstone", &values).ok());
  ASSERT_EQ(values.size(), 1U);
  EXPECT_EQ(values[0].field, "fresh");
  EXPECT_EQ(values[0].value, "new-value");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
