#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "codec/key_codec.h"
#include "gtest/gtest.h"
#include "kernel/snapshot.h"
#include "kernel/storage_engine.h"
#include "kernel/write_context.h"
#include "rocksdb/db.h"
#include "types/hash/hash_module.h"

namespace {

class SnapshotTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-snapshot-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
                   .string();

    minikv::Config config;
    config.db_path = db_path_;
    storage_engine_ = std::make_unique<minikv::StorageEngine>();
    ASSERT_TRUE(storage_engine_->Open(config).ok());
  }

  void TearDown() override {
    storage_engine_.reset();
    rocksdb::Options options;
    ASSERT_TRUE(rocksdb::DestroyDB(db_path_, options).ok());
  }

  void SeedHash(uint64_t size, const std::vector<minikv::FieldValue>& values) {
    minikv::KeyMetadata metadata;
    metadata.size = size;
    minikv::WriteContext write_context(storage_engine_.get());
    ASSERT_TRUE(write_context
                    .Put(minikv::StorageColumnFamily::kMeta,
                         minikv::KeyCodec::EncodeMetaKey("user:1"),
                         minikv::KeyCodec::EncodeMetaValue(metadata))
                    .ok());
    for (const auto& value : values) {
      ASSERT_TRUE(write_context
                      .Put(minikv::StorageColumnFamily::kHash,
                           minikv::KeyCodec::EncodeHashDataKey(
                               "user:1", metadata.version, value.field),
                           value.value)
                      .ok());
    }
    ASSERT_TRUE(write_context.Commit().ok());
  }

  static inline int counter_ = 0;
  std::string db_path_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
};

TEST_F(SnapshotTest, SnapshotKeepsMetaAndHashReadsOnSameView) {
  SeedHash(1, {{"name", "alice"}});

  std::unique_ptr<minikv::Snapshot> first = storage_engine_->CreateSnapshot();

  SeedHash(2, {{"name", "alice"}, {"city", "shanghai"}});

  std::string raw_meta;
  ASSERT_TRUE(first
                  ->Get(minikv::StorageColumnFamily::kMeta,
                        minikv::KeyCodec::EncodeMetaKey("user:1"), &raw_meta)
                  .ok());
  minikv::KeyMetadata first_metadata;
  ASSERT_TRUE(minikv::KeyCodec::DecodeMetaValue(raw_meta, &first_metadata));
  EXPECT_EQ(first_metadata.size, 1U);

  std::vector<minikv::FieldValue> first_values;
  ASSERT_TRUE(first
                  ->ScanPrefix(
                      minikv::StorageColumnFamily::kHash,
                      minikv::KeyCodec::EncodeHashDataPrefix(
                          "user:1", first_metadata.version),
                      [&first_values](const rocksdb::Slice& encoded_key,
                                      const rocksdb::Slice& value) {
                        const std::string prefix =
                            minikv::KeyCodec::EncodeHashDataPrefix("user:1", 1);
                        std::string field;
                        if (!minikv::KeyCodec::ExtractFieldFromHashDataKey(
                                encoded_key, prefix, &field)) {
                          return false;
                        }
                        first_values.push_back(
                            minikv::FieldValue{std::move(field), value.ToString()});
                        return true;
                      })
                  .ok());
  ASSERT_EQ(first_values.size(), 1U);
  EXPECT_EQ(first_values[0].field, "name");
  EXPECT_EQ(first_values[0].value, "alice");

  std::unique_ptr<minikv::Snapshot> second = storage_engine_->CreateSnapshot();
  ASSERT_TRUE(second
                  ->Get(minikv::StorageColumnFamily::kMeta,
                        minikv::KeyCodec::EncodeMetaKey("user:1"), &raw_meta)
                  .ok());
  minikv::KeyMetadata second_metadata;
  ASSERT_TRUE(minikv::KeyCodec::DecodeMetaValue(raw_meta, &second_metadata));
  EXPECT_EQ(second_metadata.size, 2U);

  std::vector<std::string> second_fields;
  ASSERT_TRUE(second
                  ->ScanPrefix(
                      minikv::StorageColumnFamily::kHash,
                      minikv::KeyCodec::EncodeHashDataPrefix(
                          "user:1", second_metadata.version),
                      [&second_fields](const rocksdb::Slice& encoded_key,
                                       const rocksdb::Slice&) {
                        const std::string prefix =
                            minikv::KeyCodec::EncodeHashDataPrefix("user:1", 1);
                        std::string field;
                        if (!minikv::KeyCodec::ExtractFieldFromHashDataKey(
                                encoded_key, prefix, &field)) {
                          return false;
                        }
                        second_fields.push_back(std::move(field));
                        return true;
                      })
                  .ok());
  ASSERT_EQ(second_fields.size(), 2U);
  EXPECT_EQ(second_fields[0], "city");
  EXPECT_EQ(second_fields[1], "name");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
