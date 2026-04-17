#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "gtest/gtest.h"
#include "kernel/mutation_hook.h"
#include "kernel/storage_engine.h"
#include "config.h"
#include "rocksdb/db.h"
#include "types/hash/hash_module.h"

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
    hash_module_.reset();
    storage_engine_.reset();
    rocksdb::Options options;
    ASSERT_TRUE(rocksdb::DestroyDB(db_path_, options).ok());
  }

  void OpenModule() {
    minikv::Config config;
    config.db_path = db_path_;
    storage_engine_ = std::make_unique<minikv::StorageEngine>();
    ASSERT_TRUE(storage_engine_->Open(config).ok());
    hash_module_ = std::make_unique<minikv::HashModule>(storage_engine_.get(),
                                                        &mutation_hook_);
  }

  static inline int counter_ = 0;
  std::string db_path_;
  minikv::NoopMutationHook mutation_hook_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  std::unique_ptr<minikv::HashModule> hash_module_;
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
  hash_module_.reset();
  storage_engine_.reset();

  OpenModule();

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("user:reopen", &values).ok());
  ASSERT_EQ(values.size(), 1U);
  ASSERT_EQ(values[0].field, "name");
  ASSERT_EQ(values[0].value, "alice");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
