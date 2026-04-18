#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>

#include "runtime/config.h"
#include "gtest/gtest.h"
#include "storage/engine/storage_engine.h"
#include "runtime/module/module_services.h"
#include "rocksdb/db.h"

namespace {

class ModuleIteratorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-module-iterator-test-" +
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

  void Seed(const minikv::ModuleKeyspace& keyspace,
            const std::vector<std::pair<std::string, std::string>>& entries) {
    minikv::ModuleStorage storage(minikv::ModuleNamespace(keyspace.module_name()),
                                  storage_engine_.get());
    std::unique_ptr<minikv::ModuleWriteBatch> batch = storage.CreateWriteBatch();
    for (const auto& entry : entries) {
      ASSERT_TRUE(batch->Put(keyspace, entry.first, entry.second).ok());
    }
    ASSERT_TRUE(batch->Commit().ok());
  }

  static inline int counter_ = 0;
  std::string db_path_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
};

TEST_F(ModuleIteratorTest, IteratesWithinOneKeyspaceAndDecodesLocalKeys) {
  minikv::ModuleStorage storage(minikv::ModuleNamespace("search"),
                                storage_engine_.get());
  minikv::ModuleSnapshotService snapshots(minikv::ModuleNamespace("search"),
                                          storage_engine_.get());

  const minikv::ModuleKeyspace docs = storage.Keyspace("docs");
  const minikv::ModuleKeyspace terms = storage.Keyspace("terms");

  Seed(docs, {{"alpha", "A"}, {"beta", "B"}, {"gamma", "C"}});
  Seed(terms, {{"beta", "doc:1"}});

  std::unique_ptr<minikv::ModuleSnapshot> snapshot = snapshots.Create();
  std::unique_ptr<minikv::ModuleIterator> iter = snapshot->NewIterator(docs);

  iter->Seek("beta");
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key().ToString(), "beta");
  EXPECT_EQ(iter->value().ToString(), "B");
  EXPECT_TRUE(iter->status().ok());

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key().ToString(), "gamma");
  EXPECT_EQ(iter->value().ToString(), "C");

  iter->Next();
  EXPECT_FALSE(iter->Valid());
  EXPECT_TRUE(iter->status().ok());
}

TEST_F(ModuleIteratorTest, SeekFromBeginningSupportsFullScan) {
  minikv::ModuleStorage storage(minikv::ModuleNamespace("search"),
                                storage_engine_.get());
  minikv::ModuleSnapshotService snapshots(minikv::ModuleNamespace("search"),
                                          storage_engine_.get());
  const minikv::ModuleKeyspace docs = storage.Keyspace("docs");

  Seed(docs, {{"alpha", "A"}, {"beta", "B"}});

  std::unique_ptr<minikv::ModuleSnapshot> snapshot = snapshots.Create();
  std::unique_ptr<minikv::ModuleIterator> iter = snapshot->NewIterator(docs);

  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (iter->Seek(""); iter->Valid(); iter->Next()) {
    keys.push_back(iter->key().ToString());
    values.push_back(iter->value().ToString());
  }

  EXPECT_EQ(keys, (std::vector<std::string>{"alpha", "beta"}));
  EXPECT_EQ(values, (std::vector<std::string>{"A", "B"}));
  EXPECT_TRUE(iter->status().ok());
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
