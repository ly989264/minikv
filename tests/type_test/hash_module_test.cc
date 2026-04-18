#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "codec/key_codec.h"
#include "config.h"
#include "gtest/gtest.h"
#include "kernel/scheduler.h"
#include "kernel/storage_engine.h"
#include "kernel/write_context.h"
#include "module/module.h"
#include "module/module_manager.h"
#include "modules/core/core_module.h"
#include "modules/core/key_service.h"
#include "rocksdb/db.h"
#include "modules/hash/hash_module.h"

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
    modules.push_back(std::make_unique<minikv::CoreModule>());
    auto hash_module = std::make_unique<minikv::HashModule>();
    hash_module_ = hash_module.get();
    modules.push_back(std::move(hash_module));
    module_manager_ = std::make_unique<minikv::ModuleManager>(
        storage_engine_.get(), scheduler_.get(), std::move(modules));
    ASSERT_TRUE(module_manager_->Initialize().ok());
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

  static inline int counter_ = 0;
  std::string db_path_;
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

TEST_F(HashModuleTest, DeleteFieldsRemovesFieldsAndMeta) {
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
  module_manager_.reset();
  scheduler_.reset();
  hash_module_ = nullptr;
  storage_engine_.reset();

  OpenModule();

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

TEST_F(HashModuleTest, ExpiredMetadataRebuildBumpsVersion) {
  minikv::KeyMetadata expired;
  expired.type = minikv::ObjectType::kHash;
  expired.encoding = minikv::ObjectEncoding::kHashPlain;
  expired.version = 7;
  expired.size = 1;
  expired.expire_at_ms = 1;

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

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("user:expired", &values).ok());
  ASSERT_EQ(values.size(), 1U);
  EXPECT_EQ(values[0].field, "fresh");
  EXPECT_EQ(values[0].value, "new-value");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
