#include <cstdint>
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
#include "types/string/string_module.h"

namespace {

class StringModuleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-string-module-test-" + std::to_string(::getpid()) +
                 "-" + std::to_string(counter_++)))
                   .string();
    OpenModule();
  }

  void TearDown() override {
    module_manager_.reset();
    scheduler_.reset();
    string_module_ = nullptr;
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
    auto string_module = std::make_unique<minikv::StringModule>();
    string_module_ = string_module.get();
    modules.push_back(std::move(string_module));
    module_manager_ = std::make_unique<minikv::ModuleManager>(
        storage_engine_.get(), scheduler_.get(), std::move(modules));
    ASSERT_TRUE(module_manager_->Initialize().ok());
  }

  void CloseModule() {
    module_manager_.reset();
    scheduler_.reset();
    string_module_ = nullptr;
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

  void PutRawStringValue(const std::string& key, const std::string& value) {
    minikv::WriteContext write_context(storage_engine_.get());
    const minikv::ModuleKeyspace data_keyspace("string", "data");
    ASSERT_TRUE(write_context
                    .Put(minikv::StorageColumnFamily::kModule,
                         data_keyspace.EncodeKey(key), value)
                    .ok());
    ASSERT_TRUE(write_context.Commit().ok());
  }

  bool HasRawStringValue(const std::string& key) const {
    std::string scratch;
    const minikv::ModuleKeyspace data_keyspace("string", "data");
    const rocksdb::Status status = storage_engine_->Get(
        minikv::StorageColumnFamily::kModule, data_keyspace.EncodeKey(key),
        &scratch);
    return status.ok();
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
    ASSERT_TRUE(string_module_
                    ->DeleteWholeKey(snapshot.get(), write_batch.get(), key,
                                     lookup)
                    .ok());
    ASSERT_TRUE(write_batch->Commit().ok());
  }

  static inline int counter_ = 0;
  std::string db_path_;
  uint64_t current_time_ms_ = 10'000;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  minikv::StringModule* string_module_ = nullptr;
};

TEST_F(StringModuleTest, SetGetOverwriteAndLength) {
  ASSERT_TRUE(string_module_->SetValue("str:1", "hello").ok());

  std::string value;
  bool found = false;
  ASSERT_TRUE(string_module_->GetValue("str:1", &value, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_EQ(value, "hello");

  uint64_t length = 0;
  ASSERT_TRUE(string_module_->Length("str:1", &length).ok());
  EXPECT_EQ(length, 5U);

  ASSERT_TRUE(string_module_->SetValue("str:1", "").ok());
  ASSERT_TRUE(string_module_->GetValue("str:1", &value, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_TRUE(value.empty());

  ASSERT_TRUE(string_module_->Length("str:1", &length).ok());
  EXPECT_EQ(length, 0U);
}

TEST_F(StringModuleTest, MissingKeyOperationsReturnEmptySuccess) {
  std::string value = "seed";
  bool found = true;
  ASSERT_TRUE(string_module_->GetValue("missing", &value, &found).ok());
  EXPECT_FALSE(found);
  EXPECT_TRUE(value.empty());

  uint64_t length = 42;
  ASSERT_TRUE(string_module_->Length("missing", &length).ok());
  EXPECT_EQ(length, 0U);
}

TEST_F(StringModuleTest, ReopenPreservesStringData) {
  ASSERT_TRUE(string_module_->SetValue("str:reopen", "persisted").ok());
  ReopenModule();

  std::string value;
  bool found = false;
  ASSERT_TRUE(string_module_->GetValue("str:reopen", &value, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_EQ(value, "persisted");

  uint64_t length = 0;
  ASSERT_TRUE(string_module_->Length("str:reopen", &length).ok());
  EXPECT_EQ(length, 9U);
}

TEST_F(StringModuleTest, NonStringMetadataStillReturnsTypeMismatch) {
  minikv::KeyMetadata metadata;
  metadata.type = minikv::ObjectType::kHash;
  metadata.encoding = minikv::ObjectEncoding::kHashPlain;
  metadata.version = 3;
  PutRawMetadata("str:hash", metadata);

  rocksdb::Status status = string_module_->SetValue("str:hash", "value");
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  std::string value;
  bool found = false;
  status = string_module_->GetValue("str:hash", &value, &found);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  uint64_t length = 0;
  status = string_module_->Length("str:hash", &length);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);
}

TEST_F(StringModuleTest, ExpiredMetadataRebuildBumpsVersionAndOverwritesStaleValue) {
  minikv::KeyMetadata expired;
  expired.type = minikv::ObjectType::kString;
  expired.encoding = minikv::ObjectEncoding::kRaw;
  expired.version = 7;
  expired.size = 5;
  expired.expire_at_ms = current_time_ms_ - 1;
  PutRawMetadata("str:expired", expired);
  PutRawStringValue("str:expired", "stale");

  std::string value;
  bool found = true;
  ASSERT_TRUE(string_module_->GetValue("str:expired", &value, &found).ok());
  EXPECT_FALSE(found);
  EXPECT_TRUE(value.empty());

  uint64_t length = 42;
  ASSERT_TRUE(string_module_->Length("str:expired", &length).ok());
  EXPECT_EQ(length, 0U);

  const minikv::KeyLookup lookup = LookupKey("str:expired");
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kExpired);

  ASSERT_TRUE(string_module_->SetValue("str:expired", "fresh").ok());

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("str:expired");
  EXPECT_EQ(rebuilt.type, minikv::ObjectType::kString);
  EXPECT_EQ(rebuilt.encoding, minikv::ObjectEncoding::kRaw);
  EXPECT_EQ(rebuilt.version, 8U);
  EXPECT_EQ(rebuilt.size, 5U);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);

  ASSERT_TRUE(string_module_->GetValue("str:expired", &value, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_EQ(value, "fresh");
}

TEST_F(StringModuleTest, DeleteWholeKeyWritesTombstoneAndRemovesStoredValue) {
  ASSERT_TRUE(string_module_->SetValue("str:delete", "alive").ok());

  const minikv::KeyMetadata before_delete = ReadRawMetadata("str:delete");
  DeleteWholeKey("str:delete");

  const minikv::KeyLookup lookup = LookupKey("str:delete");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);
  EXPECT_FALSE(HasRawStringValue("str:delete"));

  std::string value;
  bool found = true;
  ASSERT_TRUE(string_module_->GetValue("str:delete", &value, &found).ok());
  EXPECT_FALSE(found);
  EXPECT_TRUE(value.empty());

  uint64_t length = 42;
  ASSERT_TRUE(string_module_->Length("str:delete", &length).ok());
  EXPECT_EQ(length, 0U);
}

TEST_F(StringModuleTest, TombstoneSurvivesReopenAndRecreateBumpsVersion) {
  ASSERT_TRUE(string_module_->SetValue("str:tombstone", "value").ok());

  const minikv::KeyMetadata before_delete = ReadRawMetadata("str:tombstone");
  DeleteWholeKey("str:tombstone");

  minikv::KeyLookup lookup = LookupKey("str:tombstone");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);

  ReopenModule();

  lookup = LookupKey("str:tombstone");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);

  ASSERT_TRUE(string_module_->SetValue("str:tombstone", "fresh").ok());

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("str:tombstone");
  EXPECT_EQ(rebuilt.version, before_delete.version + 1);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);

  std::string value;
  bool found = false;
  ASSERT_TRUE(string_module_->GetValue("str:tombstone", &value, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_EQ(value, "fresh");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
