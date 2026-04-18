#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "runtime/config.h"
#include "gtest/gtest.h"
#include "execution/scheduler/scheduler.h"
#include "storage/engine/storage_engine.h"
#include "runtime/module/module.h"
#include "runtime/module/module_manager.h"
#include "runtime/module/module_services.h"
#include "core/core_module.h"
#include "core/key_service.h"
#include "types/hash/hash_indexing_bridge.h"
#include "types/hash/hash_module.h"
#include "types/hash/hash_observer.h"
#include "types/set/set_module.h"
#include "rocksdb/db.h"

namespace {

class HashBridgeLookupModule : public minikv::Module {
 public:
  std::string_view Name() const override { return "lookup"; }

  rocksdb::Status OnLoad(minikv::ModuleServices& /*services*/) override {
    return rocksdb::Status::OK();
  }

  rocksdb::Status OnStart(minikv::ModuleServices& services) override {
    bridge_ = services.exports().Find<minikv::HashIndexingBridge>(
        minikv::kHashIndexingBridgeQualifiedExportName);
    if (bridge_ == nullptr) {
      return rocksdb::Status::InvalidArgument("hash bridge export is unavailable");
    }
    return rocksdb::Status::OK();
  }

  void OnStop(minikv::ModuleServices& /*services*/) override { bridge_ = nullptr; }

  minikv::HashIndexingBridge* bridge() const { return bridge_; }

 private:
  minikv::HashIndexingBridge* bridge_ = nullptr;
};

class MarkerObserver : public minikv::HashObserver {
 public:
  rocksdb::Status OnHashMutation(const minikv::HashMutation& mutation,
                                 minikv::ModuleWriteBatch* write_batch) override {
    if (mutation.type == minikv::HashMutation::Type::kPutField) {
      return write_batch->Put(minikv::StorageColumnFamily::kDefault,
                              PutMarkerKey(mutation.key),
                              mutation.values.front().field + "=" +
                                  mutation.values.front().value);
    }
    return write_batch->Put(minikv::StorageColumnFamily::kDefault,
                            DeleteMarkerKey(mutation.key),
                            Join(mutation.deleted_fields));
  }

  static std::string PutMarkerKey(const std::string& key) {
    return "observer.put." + key;
  }

  static std::string DeleteMarkerKey(const std::string& key) {
    return "observer.delete." + key;
  }

 private:
  static std::string Join(const std::vector<std::string>& values) {
    std::string out;
    for (size_t i = 0; i < values.size(); ++i) {
      if (i != 0) {
        out += ",";
      }
      out += values[i];
    }
    return out;
  }
};

class FailingObserver : public minikv::HashObserver {
 public:
  rocksdb::Status OnHashMutation(const minikv::HashMutation&,
                                 minikv::ModuleWriteBatch* /*write_batch*/) override {
    return rocksdb::Status::Aborted("observer failure");
  }
};

class HashObserverTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-hash-observer-test-" + std::to_string(::getpid()) +
                 "-" + std::to_string(counter_++)))
                   .string();

    minikv::Config config;
    config.db_path = db_path_;
    storage_engine_ = std::make_unique<minikv::StorageEngine>();
    ASSERT_TRUE(storage_engine_->Open(config).ok());
    scheduler_ = std::make_unique<minikv::Scheduler>(1, 16);

    auto lookup = std::make_unique<HashBridgeLookupModule>();
    lookup_module_ = lookup.get();
    auto hash = std::make_unique<minikv::HashModule>();
    hash_module_ = hash.get();

    std::vector<std::unique_ptr<minikv::Module>> modules;
    modules.push_back(std::make_unique<minikv::CoreModule>());
    modules.push_back(std::move(lookup));
    modules.push_back(std::move(hash));
    modules.push_back(std::make_unique<minikv::SetModule>());

    module_manager_ = std::make_unique<minikv::ModuleManager>(
        storage_engine_.get(), scheduler_.get(), std::move(modules));
    ASSERT_TRUE(module_manager_->Initialize().ok());
  }

  void TearDown() override {
    module_manager_.reset();
    scheduler_.reset();
    hash_module_ = nullptr;
    lookup_module_ = nullptr;
    storage_engine_.reset();
    rocksdb::Options options;
    ASSERT_TRUE(rocksdb::DestroyDB(db_path_, options).ok());
  }

  minikv::HashIndexingBridge* bridge() const {
    return lookup_module_ != nullptr ? lookup_module_->bridge() : nullptr;
  }

  rocksdb::Status ReadDefault(const std::string& key, std::string* value) const {
    return storage_engine_->Get(minikv::StorageColumnFamily::kDefault, key, value);
  }

  void DeleteWholeKey(const std::string& key) {
    minikv::DefaultCoreKeyService key_service;
    minikv::ModuleSnapshotService snapshots(minikv::ModuleNamespace("core"),
                                            storage_engine_.get());
    minikv::ModuleStorage storage(minikv::ModuleNamespace("core"),
                                  storage_engine_.get());
    std::unique_ptr<minikv::ModuleSnapshot> snapshot = snapshots.Create();
    std::unique_ptr<minikv::ModuleWriteBatch> write_batch =
        storage.CreateWriteBatch();
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
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  HashBridgeLookupModule* lookup_module_ = nullptr;
  minikv::HashModule* hash_module_ = nullptr;
};

TEST_F(HashObserverTest, ObserverCanAppendWritesOnPutInSameBatch) {
  ASSERT_NE(bridge(), nullptr);

  MarkerObserver marker;
  ASSERT_TRUE(bridge()->AddObserver(&marker).ok());

  bool inserted = false;
  ASSERT_TRUE(hash_module_->PutField("user:put", "name", "alice", &inserted).ok());
  ASSERT_TRUE(inserted);

  std::string marker_value;
  ASSERT_TRUE(
      ReadDefault(MarkerObserver::PutMarkerKey("user:put"), &marker_value).ok());
  EXPECT_EQ(marker_value, "name=alice");

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("user:put", &values).ok());
  ASSERT_EQ(values.size(), 1U);
  EXPECT_EQ(values[0].field, "name");
  EXPECT_EQ(values[0].value, "alice");
}

TEST_F(HashObserverTest, ObserverCanAppendWritesOnDeleteInSameBatch) {
  ASSERT_NE(bridge(), nullptr);
  ASSERT_TRUE(hash_module_->PutField("user:delete", "name", "alice", nullptr).ok());

  MarkerObserver marker;
  ASSERT_TRUE(bridge()->AddObserver(&marker).ok());

  uint64_t deleted = 0;
  ASSERT_TRUE(
      hash_module_->DeleteFields("user:delete", {"name"}, &deleted).ok());
  ASSERT_EQ(deleted, 1U);

  std::string marker_value;
  ASSERT_TRUE(ReadDefault(MarkerObserver::DeleteMarkerKey("user:delete"),
                          &marker_value)
                  .ok());
  EXPECT_EQ(marker_value, "name");

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("user:delete", &values).ok());
  EXPECT_TRUE(values.empty());
}

TEST_F(HashObserverTest,
       ObserverFailureAbortsBaseWriteWithoutPersistingBaseDataOrMarkers) {
  ASSERT_NE(bridge(), nullptr);

  MarkerObserver marker;
  FailingObserver failing;
  ASSERT_TRUE(bridge()->AddObserver(&marker).ok());
  ASSERT_TRUE(bridge()->AddObserver(&failing).ok());

  rocksdb::Status status =
      hash_module_->PutField("user:fail", "name", "alice", nullptr);
  ASSERT_TRUE(status.IsAborted());
  EXPECT_NE(status.ToString().find("observer failure"), std::string::npos);

  std::string marker_value;
  EXPECT_TRUE(ReadDefault(MarkerObserver::PutMarkerKey("user:fail"),
                          &marker_value)
                  .IsNotFound());

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("user:fail", &values).ok());
  EXPECT_TRUE(values.empty());
}

TEST_F(HashObserverTest, WholeKeyDeleteObserverEnumeratesVisibleFields) {
  ASSERT_NE(bridge(), nullptr);
  ASSERT_TRUE(hash_module_->PutField("user:whole", "name", "alice", nullptr).ok());
  ASSERT_TRUE(hash_module_->PutField("user:whole", "city", "shanghai", nullptr)
                  .ok());

  MarkerObserver marker;
  ASSERT_TRUE(bridge()->AddObserver(&marker).ok());

  DeleteWholeKey("user:whole");

  std::string marker_value;
  ASSERT_TRUE(ReadDefault(MarkerObserver::DeleteMarkerKey("user:whole"),
                          &marker_value)
                  .ok());
  EXPECT_EQ(marker_value, "city,name");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
