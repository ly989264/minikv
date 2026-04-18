#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "config.h"
#include "gtest/gtest.h"
#include "kernel/scheduler.h"
#include "kernel/storage_engine.h"
#include "module/module.h"
#include "module/module_manager.h"
#include "module/module_services.h"
#include "modules/core/core_module.h"
#include "modules/hash/hash_indexing_bridge.h"
#include "modules/hash/hash_module.h"
#include "modules/hash/hash_observer.h"
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

class RecordingObserver : public minikv::HashObserver {
 public:
  RecordingObserver(std::string label, std::vector<std::string>* events)
      : label_(std::move(label)), events_(events) {}

  rocksdb::Status OnHashMutation(const minikv::HashMutation& mutation,
                                 minikv::ModuleWriteBatch* /*write_batch*/) override {
    const char* op = mutation.type == minikv::HashMutation::Type::kPutField
                         ? "put"
                         : "delete";
    events_->push_back(label_ + "." + op + "." + mutation.key);
    return rocksdb::Status::OK();
  }

 private:
  std::string label_;
  std::vector<std::string>* events_ = nullptr;
};

class HashBridgeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-hash-bridge-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
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

  static inline int counter_ = 0;
  std::string db_path_;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  HashBridgeLookupModule* lookup_module_ = nullptr;
  minikv::HashModule* hash_module_ = nullptr;
};

TEST_F(HashBridgeTest, HashModulePublishesBridgeExport) {
  ASSERT_NE(bridge(), nullptr);
  EXPECT_EQ(bridge(), static_cast<minikv::HashIndexingBridge*>(hash_module_));
}

TEST_F(HashBridgeTest, ObserversRunInRegistrationOrderAndCanBeRemoved) {
  ASSERT_NE(bridge(), nullptr);

  std::vector<std::string> events;
  RecordingObserver first("first", &events);
  RecordingObserver second("second", &events);
  RecordingObserver missing("missing", &events);

  EXPECT_TRUE(bridge()->AddObserver(nullptr).IsInvalidArgument());
  ASSERT_TRUE(bridge()->AddObserver(&first).ok());
  ASSERT_TRUE(bridge()->AddObserver(&second).ok());
  EXPECT_TRUE(bridge()->AddObserver(&first).IsInvalidArgument());
  EXPECT_TRUE(bridge()->RemoveObserver(&missing).IsInvalidArgument());

  bool inserted = false;
  ASSERT_TRUE(hash_module_->PutField("user:1", "name", "alice", &inserted).ok());
  ASSERT_TRUE(inserted);
  EXPECT_EQ(events, (std::vector<std::string>{
                        "first.put.user:1",
                        "second.put.user:1",
                    }));

  ASSERT_TRUE(bridge()->RemoveObserver(&first).ok());

  uint64_t deleted = 0;
  ASSERT_TRUE(hash_module_->DeleteFields("user:1", {"name"}, &deleted).ok());
  ASSERT_EQ(deleted, 1U);
  EXPECT_EQ(events, (std::vector<std::string>{
                        "first.put.user:1",
                        "second.put.user:1",
                        "second.delete.user:1",
                    }));

  ASSERT_TRUE(hash_module_->DeleteFields("user:1", {"name"}, &deleted).ok());
  ASSERT_EQ(deleted, 0U);
  EXPECT_EQ(events.size(), 3U);
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
