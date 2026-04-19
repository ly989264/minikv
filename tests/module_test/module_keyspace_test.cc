#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "runtime/config.h"
#include "gtest/gtest.h"
#include "storage/engine/storage_engine.h"
#include "runtime/module/module_services.h"
#include "rocksdb/db.h"

namespace {

class ModuleKeyspaceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-module-keyspace-test-" +
                 std::to_string(::getpid()) + "-" +
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

  static inline int counter_ = 0;
  std::string db_path_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
};

TEST_F(ModuleKeyspaceTest, SeparatesModuleIdentityFromStorageSubspaces) {
  minikv::ModuleStorage search_storage(minikv::ModuleNamespace("search"),
                                       storage_engine_.get(),
                                       minikv::StorageColumnFamily::kModule);
  minikv::ModuleStorage analytics_storage(minikv::ModuleNamespace("analytics"),
                                          storage_engine_.get(),
                                          minikv::StorageColumnFamily::kModule);
  minikv::ModuleSnapshotService search_snapshots(
      minikv::ModuleNamespace("search"), storage_engine_.get(),
      minikv::StorageColumnFamily::kModule);
  minikv::ModuleSnapshotService analytics_snapshots(
      minikv::ModuleNamespace("analytics"), storage_engine_.get(),
      minikv::StorageColumnFamily::kModule);

  const minikv::ModuleKeyspace search_docs = search_storage.Keyspace("docs");
  const minikv::ModuleKeyspace search_terms = search_storage.Keyspace("terms");
  const minikv::ModuleKeyspace analytics_docs =
      analytics_storage.Keyspace("docs");

  EXPECT_TRUE(search_docs.valid());
  EXPECT_EQ(search_docs.column_family(), minikv::StorageColumnFamily::kModule);
  EXPECT_EQ(search_docs.module_name(), "search");
  EXPECT_EQ(search_docs.local_name(), "docs");
  EXPECT_EQ(search_docs.QualifiedName(), "search.docs");
  EXPECT_NE(search_docs.Prefix(), search_terms.Prefix());
  EXPECT_NE(search_docs.Prefix(), analytics_docs.Prefix());

  const std::string encoded_key = search_docs.EncodeKey("doc:1");
  std::string decoded_key;
  ASSERT_TRUE(search_docs.DecodeKey(encoded_key, &decoded_key));
  EXPECT_EQ(decoded_key, "doc:1");
  EXPECT_FALSE(search_docs.DecodeKey(search_terms.EncodeKey("doc:1"),
                                     &decoded_key));

  std::unique_ptr<minikv::ModuleWriteBatch> batch =
      search_storage.CreateWriteBatch();
  ASSERT_TRUE(batch->Put(search_docs, "doc:1", "alice").ok());
  ASSERT_TRUE(batch->Put(search_terms, "name", "doc:1").ok());
  ASSERT_TRUE(batch->Commit().ok());

  batch = analytics_storage.CreateWriteBatch();
  ASSERT_TRUE(batch->Put(analytics_docs, "doc:1", "report").ok());
  ASSERT_TRUE(batch->Commit().ok());

  std::string raw_value;
  ASSERT_TRUE(storage_engine_
                  ->Get(minikv::StorageColumnFamily::kModule, encoded_key,
                        &raw_value)
                  .ok());
  EXPECT_EQ(raw_value, "alice");

  std::unique_ptr<minikv::ModuleSnapshot> snapshot = search_snapshots.Create();
  ASSERT_TRUE(snapshot->Get(search_docs, "doc:1", &raw_value).ok());
  EXPECT_EQ(raw_value, "alice");
  ASSERT_TRUE(snapshot->Get(search_terms, "name", &raw_value).ok());
  EXPECT_EQ(raw_value, "doc:1");
  EXPECT_TRUE(snapshot->Get(search_terms, "doc:1", &raw_value).IsNotFound());

  snapshot = analytics_snapshots.Create();
  ASSERT_TRUE(snapshot->Get(analytics_docs, "doc:1", &raw_value).ok());
  EXPECT_EQ(raw_value, "report");
}

TEST_F(ModuleKeyspaceTest, RejectsInvalidKeyspaceOnReadAndWrite) {
  minikv::ModuleStorage storage(minikv::ModuleNamespace("search"),
                                storage_engine_.get(),
                                minikv::StorageColumnFamily::kModule);
  minikv::ModuleSnapshotService snapshots(minikv::ModuleNamespace("search"),
                                          storage_engine_.get(),
                                          minikv::StorageColumnFamily::kModule);
  const minikv::ModuleKeyspace invalid("search", "");

  std::unique_ptr<minikv::ModuleWriteBatch> batch = storage.CreateWriteBatch();
  EXPECT_TRUE(batch->Put(invalid, "doc:1", "alice").IsInvalidArgument());
  EXPECT_TRUE(batch->Delete(invalid, "doc:1").IsInvalidArgument());

  std::unique_ptr<minikv::ModuleSnapshot> snapshot = snapshots.Create();
  std::string value;
  EXPECT_TRUE(snapshot->Get(invalid, "doc:1", &value).IsInvalidArgument());
  EXPECT_TRUE(
      snapshot->ScanPrefix(invalid, "", [](const rocksdb::Slice&,
                                           const rocksdb::Slice&) {
        return true;
      }).IsInvalidArgument());
}

TEST_F(ModuleKeyspaceTest, StorageEngineCreatesAndReopensAllRequiredColumnFamilies) {
  const std::vector<minikv::StorageColumnFamily> column_families = {
      minikv::StorageColumnFamily::kDefault,   minikv::StorageColumnFamily::kMeta,
      minikv::StorageColumnFamily::kString,    minikv::StorageColumnFamily::kHash,
      minikv::StorageColumnFamily::kList,      minikv::StorageColumnFamily::kSet,
      minikv::StorageColumnFamily::kZSet,      minikv::StorageColumnFamily::kStream,
      minikv::StorageColumnFamily::kJson,      minikv::StorageColumnFamily::kTimeseries,
      minikv::StorageColumnFamily::kVectorSet, minikv::StorageColumnFamily::kModule,
  };

  for (size_t index = 0; index < column_families.size(); ++index) {
    const std::string key = "cf:" + std::to_string(index);
    const std::string value = "value:" + std::to_string(index);
    ASSERT_TRUE(storage_engine_->Put(column_families[index], key, value).ok());
  }

  storage_engine_.reset();
  minikv::Config config;
  config.db_path = db_path_;
  storage_engine_ = std::make_unique<minikv::StorageEngine>();
  ASSERT_TRUE(storage_engine_->Open(config).ok());

  for (size_t index = 0; index < column_families.size(); ++index) {
    const std::string key = "cf:" + std::to_string(index);
    const std::string expected = "value:" + std::to_string(index);
    std::string actual;
    ASSERT_TRUE(storage_engine_->Get(column_families[index], key, &actual).ok());
    EXPECT_EQ(actual, expected);
  }
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
