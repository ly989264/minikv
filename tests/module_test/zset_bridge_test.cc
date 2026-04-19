#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "core/core_module.h"
#include "execution/scheduler/scheduler.h"
#include "gtest/gtest.h"
#include "rocksdb/db.h"
#include "runtime/config.h"
#include "runtime/module/module.h"
#include "runtime/module/module_manager.h"
#include "runtime/module/module_services.h"
#include "storage/engine/storage_engine.h"
#include "types/zset/zset_bridge.h"
#include "types/zset/zset_module.h"

namespace {

class ZSetBridgeLookupModule : public minikv::Module {
 public:
  std::string_view Name() const override { return "lookup"; }

  rocksdb::Status OnLoad(minikv::ModuleServices& /*services*/) override {
    return rocksdb::Status::OK();
  }

  rocksdb::Status OnStart(minikv::ModuleServices& services) override {
    bridge_ = services.exports().Find<minikv::ZSetBridge>(
        minikv::kZSetBridgeQualifiedExportName);
    if (bridge_ == nullptr) {
      return rocksdb::Status::InvalidArgument("zset bridge export is unavailable");
    }
    return rocksdb::Status::OK();
  }

  void OnStop(minikv::ModuleServices& /*services*/) override { bridge_ = nullptr; }

  minikv::ZSetBridge* bridge() const { return bridge_; }

 private:
  minikv::ZSetBridge* bridge_ = nullptr;
};

class RecordingObserver : public minikv::ZSetObserver {
 public:
  RecordingObserver(std::string label, std::vector<std::string>* events)
      : label_(std::move(label)), events_(events) {}

  rocksdb::Status OnZSetMutation(const minikv::ZSetMutation& mutation,
                                 minikv::ModuleSnapshot* /*snapshot*/,
                                 minikv::ModuleWriteBatch* /*write_batch*/) override {
    const char* op = mutation.type == minikv::ZSetMutation::Type::kUpsertMembers
                         ? "upsert"
                         : (mutation.type == minikv::ZSetMutation::Type::kRemoveMembers
                                ? "remove"
                                : "delete");
    events_->push_back(label_ + "." + op + "." + mutation.key);
    return rocksdb::Status::OK();
  }

 private:
  std::string label_;
  std::vector<std::string>* events_ = nullptr;
};

class ZSetBridgeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-zset-bridge-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
                   .string();

    minikv::Config config;
    config.db_path = db_path_;
    storage_engine_ = std::make_unique<minikv::StorageEngine>();
    ASSERT_TRUE(storage_engine_->Open(config).ok());
    scheduler_ = std::make_unique<minikv::Scheduler>(1, 16);

    auto lookup = std::make_unique<ZSetBridgeLookupModule>();
    lookup_module_ = lookup.get();
    auto zset = std::make_unique<minikv::ZSetModule>();
    zset_module_ = zset.get();

    std::vector<std::unique_ptr<minikv::Module>> modules;
    modules.push_back(std::make_unique<minikv::CoreModule>());
    modules.push_back(std::move(lookup));
    modules.push_back(std::move(zset));

    module_manager_ = std::make_unique<minikv::ModuleManager>(
        storage_engine_.get(), scheduler_.get(), std::move(modules));
    ASSERT_TRUE(module_manager_->Initialize().ok());
  }

  void TearDown() override {
    module_manager_.reset();
    scheduler_.reset();
    zset_module_ = nullptr;
    lookup_module_ = nullptr;
    storage_engine_.reset();
    rocksdb::Options options;
    ASSERT_TRUE(rocksdb::DestroyDB(db_path_, options).ok());
  }

  minikv::ZSetBridge* bridge() const {
    return lookup_module_ != nullptr ? lookup_module_->bridge() : nullptr;
  }

  static inline int counter_ = 0;
  std::string db_path_;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  ZSetBridgeLookupModule* lookup_module_ = nullptr;
  minikv::ZSetModule* zset_module_ = nullptr;
};

TEST_F(ZSetBridgeTest, ZSetModulePublishesBridgeExport) {
  ASSERT_NE(bridge(), nullptr);
  EXPECT_EQ(bridge(), static_cast<minikv::ZSetBridge*>(zset_module_));
}

TEST_F(ZSetBridgeTest, ObserversRunInRegistrationOrderAndCanBeRemoved) {
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

  uint64_t added = 0;
  ASSERT_TRUE(
      zset_module_->AddMembers("zset:1", {{"a", 1.0}, {"b", 2.0}}, &added).ok());
  EXPECT_EQ(added, 2U);
  EXPECT_EQ(events, (std::vector<std::string>{
                        "first.upsert.zset:1",
                        "second.upsert.zset:1",
                    }));

  ASSERT_TRUE(bridge()->RemoveObserver(&first).ok());

  uint64_t removed = 0;
  ASSERT_TRUE(zset_module_->RemoveMembers("zset:1", {"a"}, &removed).ok());
  EXPECT_EQ(removed, 1U);
  EXPECT_EQ(events, (std::vector<std::string>{
                        "first.upsert.zset:1",
                        "second.upsert.zset:1",
                        "second.remove.zset:1",
                    }));
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
