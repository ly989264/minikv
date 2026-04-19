#include <cstdint>
#include <filesystem>
#include <limits>
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
#include "types/bitmap/bitmap_module.h"
#include "types/hash/hash_module.h"
#include "types/string/string_module.h"

namespace {

class BitmapModuleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-bitmap-module-test-" + std::to_string(::getpid()) +
                 "-" + std::to_string(counter_++)))
                   .string();
    OpenModule();
  }

  void TearDown() override {
    module_manager_.reset();
    scheduler_.reset();
    bitmap_module_ = nullptr;
    hash_module_ = nullptr;
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
    auto bitmap_module = std::make_unique<minikv::BitmapModule>();
    bitmap_module_ = bitmap_module.get();
    modules.push_back(std::move(bitmap_module));
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
    return storage_engine_
        ->Get(minikv::StorageColumnFamily::kModule,
              data_keyspace.EncodeKey(key), &scratch)
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
  minikv::BitmapModule* bitmap_module_ = nullptr;
  minikv::HashModule* hash_module_ = nullptr;
  minikv::StringModule* string_module_ = nullptr;
};

TEST_F(BitmapModuleTest, MissingKeyReadsReturnZero) {
  int bit = 1;
  ASSERT_TRUE(bitmap_module_->GetBit("missing-bitmap", 99, &bit).ok());
  EXPECT_EQ(bit, 0);

  uint64_t count = 9;
  ASSERT_TRUE(bitmap_module_->CountBits("missing-bitmap", &count).ok());
  EXPECT_EQ(count, 0U);
}

TEST_F(BitmapModuleTest, SetBitCreatesZeroExtendedStringAndSharesBytes) {
  int old_bit = -1;
  ASSERT_TRUE(bitmap_module_->SetBit("str:bitmap", 15, 1, &old_bit).ok());
  EXPECT_EQ(old_bit, 0);

  int bit = 0;
  ASSERT_TRUE(bitmap_module_->GetBit("str:bitmap", 15, &bit).ok());
  EXPECT_EQ(bit, 1);
  ASSERT_TRUE(bitmap_module_->GetBit("str:bitmap", 0, &bit).ok());
  EXPECT_EQ(bit, 0);

  uint64_t count = 0;
  ASSERT_TRUE(bitmap_module_->CountBits("str:bitmap", &count).ok());
  EXPECT_EQ(count, 1U);

  std::string value;
  bool found = false;
  ASSERT_TRUE(string_module_->GetValue("str:bitmap", &value, &found).ok());
  ASSERT_TRUE(found);
  ASSERT_EQ(value.size(), 2U);
  EXPECT_EQ(value[0], '\0');
  EXPECT_EQ(static_cast<unsigned char>(value[1]), 0x01U);

  uint64_t length = 0;
  ASSERT_TRUE(string_module_->Length("str:bitmap", &length).ok());
  EXPECT_EQ(length, 2U);
}

TEST_F(BitmapModuleTest, BitmapReadsPlainStringValues) {
  ASSERT_TRUE(string_module_->SetValue("str:plain", "A").ok());

  int bit = 0;
  ASSERT_TRUE(bitmap_module_->GetBit("str:plain", 1, &bit).ok());
  EXPECT_EQ(bit, 1);
  ASSERT_TRUE(bitmap_module_->GetBit("str:plain", 7, &bit).ok());
  EXPECT_EQ(bit, 1);

  uint64_t count = 0;
  ASSERT_TRUE(bitmap_module_->CountBits("str:plain", &count).ok());
  EXPECT_EQ(count, 2U);
}

TEST_F(BitmapModuleTest, WrongTypeKeysReturnMismatch) {
  ASSERT_TRUE(hash_module_->PutField("bitmap:hash", "field", "value", nullptr)
                  .ok());

  int bit = 0;
  rocksdb::Status status = bitmap_module_->GetBit("bitmap:hash", 0, &bit);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  int old_bit = 0;
  status = bitmap_module_->SetBit("bitmap:hash", 0, 1, &old_bit);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  uint64_t count = 0;
  status = bitmap_module_->CountBits("bitmap:hash", &count);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);
}

TEST_F(BitmapModuleTest, ExpiredStringSetBitRebuildsMetadataAndDropsStaleValue) {
  minikv::KeyMetadata expired;
  expired.type = minikv::ObjectType::kString;
  expired.encoding = minikv::ObjectEncoding::kRaw;
  expired.version = 7;
  expired.size = 5;
  expired.expire_at_ms = current_time_ms_ - 1;
  PutRawMetadata("str:expired", expired);
  PutRawStringValue("str:expired", "stale");

  int bit = 1;
  ASSERT_TRUE(bitmap_module_->GetBit("str:expired", 0, &bit).ok());
  EXPECT_EQ(bit, 0);

  int old_bit = -1;
  ASSERT_TRUE(bitmap_module_->SetBit("str:expired", 1, 1, &old_bit).ok());
  EXPECT_EQ(old_bit, 0);

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("str:expired");
  EXPECT_EQ(rebuilt.type, minikv::ObjectType::kString);
  EXPECT_EQ(rebuilt.encoding, minikv::ObjectEncoding::kRaw);
  EXPECT_EQ(rebuilt.version, 8U);
  EXPECT_EQ(rebuilt.size, 1U);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);

  std::string value;
  bool found = false;
  ASSERT_TRUE(string_module_->GetValue("str:expired", &value, &found).ok());
  ASSERT_TRUE(found);
  ASSERT_EQ(value.size(), 1U);
  EXPECT_EQ(static_cast<unsigned char>(value[0]), 0x40U);
}

TEST_F(BitmapModuleTest, StringDeleteWholeKeyRemovesBitmapVisibleState) {
  int old_bit = 0;
  ASSERT_TRUE(bitmap_module_->SetBit("str:delete", 15, 1, &old_bit).ok());

  const minikv::KeyMetadata before_delete = ReadRawMetadata("str:delete");
  DeleteWholeKey("str:delete");

  const minikv::KeyLookup lookup = LookupKey("str:delete");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);
  EXPECT_FALSE(HasRawStringValue("str:delete"));

  int bit = 1;
  ASSERT_TRUE(bitmap_module_->GetBit("str:delete", 15, &bit).ok());
  EXPECT_EQ(bit, 0);

  uint64_t count = 9;
  ASSERT_TRUE(bitmap_module_->CountBits("str:delete", &count).ok());
  EXPECT_EQ(count, 0U);
}

TEST_F(BitmapModuleTest, StringOverwriteChangesSubsequentBitmapReads) {
  int old_bit = 0;
  ASSERT_TRUE(bitmap_module_->SetBit("str:overwrite", 15, 1, &old_bit).ok());
  ASSERT_TRUE(string_module_->SetValue("str:overwrite", "A").ok());

  int bit = 0;
  ASSERT_TRUE(bitmap_module_->GetBit("str:overwrite", 1, &bit).ok());
  EXPECT_EQ(bit, 1);
  ASSERT_TRUE(bitmap_module_->GetBit("str:overwrite", 15, &bit).ok());
  EXPECT_EQ(bit, 0);

  uint64_t count = 0;
  ASSERT_TRUE(bitmap_module_->CountBits("str:overwrite", &count).ok());
  EXPECT_EQ(count, 2U);
}

TEST_F(BitmapModuleTest, RejectsOffsetsTooLargeToMaterialize) {
  int old_bit = 0;
  const uint64_t too_large_offset = 512ULL * 1024ULL * 1024ULL * 8ULL;
  rocksdb::Status status =
      bitmap_module_->SetBit("str:huge", too_large_offset, 1, &old_bit);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("bitmap offset is too large"),
            std::string::npos);
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
