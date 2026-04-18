#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"
#include "kernel/scheduler.h"
#include "module/module.h"
#include "module/module_manager.h"
#include "module/module_services.h"

namespace {

class TestExportService {
 public:
  virtual ~TestExportService() = default;

  virtual std::string_view Identity() const = 0;
  virtual bool started() const = 0;
};

class ExportProviderModule : public minikv::Module, public TestExportService {
 public:
  std::string_view Name() const override { return "provider"; }

  rocksdb::Status OnLoad(minikv::ModuleServices& services) override {
    return services.exports().Publish<TestExportService>(
        "bridge", static_cast<TestExportService*>(this));
  }

  rocksdb::Status OnStart(minikv::ModuleServices& /*services*/) override {
    started_ = true;
    return rocksdb::Status::OK();
  }

  void OnStop(minikv::ModuleServices& /*services*/) override { started_ = false; }

  std::string_view Identity() const override { return "provider"; }
  bool started() const override { return started_; }

 private:
  bool started_ = false;
};

class ExportConsumerModule : public minikv::Module {
 public:
  std::string_view Name() const override { return "consumer"; }

  rocksdb::Status OnLoad(minikv::ModuleServices& /*services*/) override {
    return rocksdb::Status::OK();
  }

  rocksdb::Status OnStart(minikv::ModuleServices& services) override {
    bridge_ = services.exports().Find<TestExportService>("provider.bridge");
    if (bridge_ == nullptr) {
      return rocksdb::Status::InvalidArgument("provider export is unavailable");
    }
    found_identity_ = std::string(bridge_->Identity());
    provider_started_during_start_ = bridge_->started();
    return rocksdb::Status::OK();
  }

  void OnStop(minikv::ModuleServices& /*services*/) override { bridge_ = nullptr; }

  const TestExportService* bridge() const { return bridge_; }
  const std::string& found_identity() const { return found_identity_; }
  bool provider_started_during_start() const {
    return provider_started_during_start_;
  }

 private:
  TestExportService* bridge_ = nullptr;
  std::string found_identity_;
  bool provider_started_during_start_ = false;
};

class DuplicateExportModule : public minikv::Module, public TestExportService {
 public:
  std::string_view Name() const override { return "dup"; }

  rocksdb::Status OnLoad(minikv::ModuleServices& services) override {
    rocksdb::Status status = services.exports().Publish<TestExportService>(
        "bridge", static_cast<TestExportService*>(this));
    if (!status.ok()) {
      return status;
    }
    return services.exports().Publish<TestExportService>(
        "bridge", static_cast<TestExportService*>(this));
  }

  rocksdb::Status OnStart(minikv::ModuleServices& /*services*/) override {
    return rocksdb::Status::OK();
  }

  void OnStop(minikv::ModuleServices& /*services*/) override {}

  std::string_view Identity() const override { return "dup"; }
  bool started() const override { return false; }
};

class FailingExportModule : public minikv::Module, public TestExportService {
 public:
  std::string_view Name() const override { return "provider"; }

  rocksdb::Status OnLoad(minikv::ModuleServices& services) override {
    services_ = &services;
    rocksdb::Status status = services.exports().Publish<TestExportService>(
        "bridge", static_cast<TestExportService*>(this));
    if (!status.ok()) {
      return status;
    }
    return rocksdb::Status::Aborted("forced load failure");
  }

  rocksdb::Status OnStart(minikv::ModuleServices& /*services*/) override {
    return rocksdb::Status::OK();
  }

  void OnStop(minikv::ModuleServices& /*services*/) override {}

  bool ExportVisibleAfterFailure() const {
    return services_ != nullptr &&
           services_->exports().Find<TestExportService>("provider.bridge") !=
               nullptr;
  }

  std::string_view Identity() const override { return "provider"; }
  bool started() const override { return false; }

 private:
  minikv::ModuleServices* services_ = nullptr;
};

TEST(ModuleExportsTest, ProviderOnLoadIsVisibleToConsumerOnStart) {
  minikv::Scheduler scheduler(1, 8);

  auto consumer = std::make_unique<ExportConsumerModule>();
  ExportConsumerModule* consumer_ptr = consumer.get();
  auto provider = std::make_unique<ExportProviderModule>();

  std::vector<std::unique_ptr<minikv::Module>> modules;
  modules.push_back(std::move(consumer));
  modules.push_back(std::move(provider));

  minikv::ModuleManager manager(nullptr, &scheduler, std::move(modules));
  ASSERT_TRUE(manager.Initialize().ok());
  ASSERT_NE(consumer_ptr->bridge(), nullptr);
  EXPECT_EQ(consumer_ptr->found_identity(), "provider");
  EXPECT_FALSE(consumer_ptr->provider_started_during_start());
}

TEST(ModuleExportsTest, DuplicateQualifiedExportFailsInitialization) {
  minikv::Scheduler scheduler(1, 8);

  std::vector<std::unique_ptr<minikv::Module>> modules;
  modules.push_back(std::make_unique<DuplicateExportModule>());

  minikv::ModuleManager manager(nullptr, &scheduler, std::move(modules));
  rocksdb::Status status = manager.Initialize();
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("dup.bridge"), std::string::npos);
}

TEST(ModuleExportsTest, FailedLoadClearsPublishedExports) {
  minikv::Scheduler scheduler(1, 8);

  auto failing = std::make_unique<FailingExportModule>();
  FailingExportModule* failing_ptr = failing.get();

  std::vector<std::unique_ptr<minikv::Module>> modules;
  modules.push_back(std::move(failing));

  minikv::ModuleManager manager(nullptr, &scheduler, std::move(modules));
  rocksdb::Status status = manager.Initialize();
  ASSERT_TRUE(status.IsAborted());
  EXPECT_FALSE(failing_ptr->ExportVisibleAfterFailure());
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
