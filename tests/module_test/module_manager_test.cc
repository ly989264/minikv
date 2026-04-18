#include <memory>
#include <string>
#include <vector>

#include "execution/command/cmd.h"
#include "gtest/gtest.h"
#include "execution/scheduler/scheduler.h"
#include "runtime/module/module_manager.h"
#include "core/core_module.h"
#include "types/hash/hash_module.h"
#include "types/list/list_module.h"
#include "types/set/set_module.h"

namespace {

class DummyCmd : public minikv::Cmd {
 public:
  explicit DummyCmd(const minikv::CmdRegistration& registration)
      : minikv::Cmd(registration.name, registration.flags) {}

 private:
  rocksdb::Status DoInitial(const minikv::CmdInput& /*input*/) override {
    return rocksdb::Status::OK();
  }

  minikv::CommandResponse Do() override { return MakeSimpleString("OK"); }
};

std::unique_ptr<minikv::Cmd> CreateDummyCmd(
    const minikv::CmdRegistration& registration) {
  return std::make_unique<DummyCmd>(registration);
}

class RecordingModule : public minikv::Module {
 public:
  RecordingModule(std::string name, std::string command_name,
                  std::vector<std::string>* events, bool fail_load = false,
                  bool fail_start = false)
      : name_(std::move(name)),
        command_name_(std::move(command_name)),
        events_(events),
        fail_load_(fail_load),
        fail_start_(fail_start) {}

  std::string_view Name() const override { return name_; }

  rocksdb::Status OnLoad(minikv::ModuleServices& services) override {
    events_->push_back(name_ + ".load");
    if (!command_name_.empty()) {
      rocksdb::Status status = services.command_registry().Register(
          {command_name_, minikv::CmdFlags::kRead, minikv::CommandSource::kBuiltin,
           "", &CreateDummyCmd});
      if (!status.ok()) {
        return status;
      }
    }
    if (fail_load_) {
      return rocksdb::Status::InvalidArgument("forced load failure for " + name_);
    }
    return rocksdb::Status::OK();
  }

  rocksdb::Status OnStart(minikv::ModuleServices& services) override {
    events_->push_back(name_ + ".start");
    services.metrics().IncrementCounter("starts");
    if (fail_start_) {
      return rocksdb::Status::Aborted("forced start failure for " + name_);
    }
    return rocksdb::Status::OK();
  }

  void OnStop(minikv::ModuleServices& /*services*/) override {
    events_->push_back(name_ + ".stop");
  }

 private:
  std::string name_;
  std::string command_name_;
  std::vector<std::string>* events_ = nullptr;
  bool fail_load_ = false;
  bool fail_start_ = false;
};

TEST(ModuleManagerTest, InitializeLoadsStartsAndStopsInReverseOrder) {
  std::vector<std::string> events;
  std::vector<std::unique_ptr<minikv::Module>> modules;
  modules.push_back(
      std::make_unique<RecordingModule>("alpha", "ALPHA", &events));
  modules.push_back(std::make_unique<RecordingModule>("beta", "BETA", &events));

  minikv::Scheduler scheduler(1, 8);
  minikv::ModuleManager manager(nullptr, &scheduler, std::move(modules));
  ASSERT_TRUE(manager.Initialize().ok());
  ASSERT_NE(manager.command_registry().Find("ALPHA"), nullptr);
  ASSERT_NE(manager.command_registry().Find("BETA"), nullptr);

  manager.StopAll();
  EXPECT_EQ(events, (std::vector<std::string>{"alpha.load", "beta.load",
                                              "alpha.start", "beta.start",
                                              "beta.stop", "alpha.stop"}));
}

TEST(ModuleManagerTest, LoadFailureStopsPreviouslyLoadedModules) {
  std::vector<std::string> events;
  std::vector<std::unique_ptr<minikv::Module>> modules;
  modules.push_back(
      std::make_unique<RecordingModule>("alpha", "ALPHA", &events));
  modules.push_back(std::make_unique<RecordingModule>("broken", "", &events, true));

  minikv::Scheduler scheduler(1, 8);
  minikv::ModuleManager manager(nullptr, &scheduler, std::move(modules));
  rocksdb::Status status = manager.Initialize();
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("broken"), std::string::npos);
  EXPECT_EQ(events, (std::vector<std::string>{"alpha.load", "broken.load",
                                              "alpha.stop"}));
}

TEST(ModuleManagerTest, StartFailureStopsLoadedModulesInReverseOrder) {
  std::vector<std::string> events;
  std::vector<std::unique_ptr<minikv::Module>> modules;
  modules.push_back(
      std::make_unique<RecordingModule>("alpha", "ALPHA", &events));
  modules.push_back(std::make_unique<RecordingModule>("beta", "BETA", &events,
                                                      false, true));

  minikv::Scheduler scheduler(1, 8);
  minikv::ModuleManager manager(nullptr, &scheduler, std::move(modules));
  rocksdb::Status status = manager.Initialize();
  ASSERT_TRUE(status.IsAborted());
  EXPECT_NE(status.ToString().find("beta"), std::string::npos);
  EXPECT_EQ(events, (std::vector<std::string>{"alpha.load", "beta.load",
                                              "alpha.start", "beta.start",
                                              "beta.stop", "alpha.stop"}));
}

TEST(ModuleManagerTest, RejectsCommandNameConflictsDuringLoad) {
  std::vector<std::string> events;
  std::vector<std::unique_ptr<minikv::Module>> modules;
  modules.push_back(std::make_unique<RecordingModule>("core", "ping", &events));
  modules.push_back(std::make_unique<RecordingModule>("hash", "PING", &events));

  minikv::Scheduler scheduler(1, 8);
  minikv::ModuleManager manager(nullptr, &scheduler, std::move(modules));
  rocksdb::Status status = manager.Initialize();
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("PING"), std::string::npos);
  EXPECT_NE(status.ToString().find("core"), std::string::npos);
  EXPECT_NE(status.ToString().find("hash"), std::string::npos);
}

TEST(ModuleManagerTest, BuiltinModulesLoadIntoUnifiedRegistry) {
  std::vector<std::unique_ptr<minikv::Module>> modules;
  modules.push_back(std::make_unique<minikv::CoreModule>());
  modules.push_back(std::make_unique<minikv::HashModule>());
  modules.push_back(std::make_unique<minikv::ListModule>());
  modules.push_back(std::make_unique<minikv::SetModule>());

  minikv::Scheduler scheduler(2, 16);
  minikv::ModuleManager manager(nullptr, &scheduler, std::move(modules));
  ASSERT_TRUE(manager.Initialize().ok());

  const minikv::CmdRegistration* ping = manager.command_registry().Find("PING");
  const minikv::CmdRegistration* type = manager.command_registry().Find("TYPE");
  const minikv::CmdRegistration* exists =
      manager.command_registry().Find("EXISTS");
  const minikv::CmdRegistration* del = manager.command_registry().Find("DEL");
  const minikv::CmdRegistration* hset = manager.command_registry().Find("HSET");
  const minikv::CmdRegistration* hgetall =
      manager.command_registry().Find("HGETALL");
  const minikv::CmdRegistration* hdel = manager.command_registry().Find("HDEL");
  const minikv::CmdRegistration* lpush =
      manager.command_registry().Find("LPUSH");
  const minikv::CmdRegistration* lpop = manager.command_registry().Find("LPOP");
  const minikv::CmdRegistration* lrange =
      manager.command_registry().Find("LRANGE");
  const minikv::CmdRegistration* rpush =
      manager.command_registry().Find("RPUSH");
  const minikv::CmdRegistration* rpop = manager.command_registry().Find("RPOP");
  const minikv::CmdRegistration* lrem = manager.command_registry().Find("LREM");
  const minikv::CmdRegistration* ltrim =
      manager.command_registry().Find("LTRIM");
  const minikv::CmdRegistration* llen = manager.command_registry().Find("LLEN");
  const minikv::CmdRegistration* sadd = manager.command_registry().Find("SADD");
  const minikv::CmdRegistration* scard =
      manager.command_registry().Find("SCARD");
  const minikv::CmdRegistration* smembers =
      manager.command_registry().Find("SMEMBERS");
  const minikv::CmdRegistration* sismember =
      manager.command_registry().Find("SISMEMBER");
  const minikv::CmdRegistration* spop = manager.command_registry().Find("SPOP");
  const minikv::CmdRegistration* srandmember =
      manager.command_registry().Find("SRANDMEMBER");
  const minikv::CmdRegistration* srem = manager.command_registry().Find("SREM");

  ASSERT_NE(ping, nullptr);
  ASSERT_NE(type, nullptr);
  ASSERT_NE(exists, nullptr);
  ASSERT_NE(del, nullptr);
  ASSERT_NE(hset, nullptr);
  ASSERT_NE(hgetall, nullptr);
  ASSERT_NE(hdel, nullptr);
  ASSERT_NE(lpush, nullptr);
  ASSERT_NE(lpop, nullptr);
  ASSERT_NE(lrange, nullptr);
  ASSERT_NE(rpush, nullptr);
  ASSERT_NE(rpop, nullptr);
  ASSERT_NE(lrem, nullptr);
  ASSERT_NE(ltrim, nullptr);
  ASSERT_NE(llen, nullptr);
  ASSERT_NE(sadd, nullptr);
  ASSERT_NE(scard, nullptr);
  ASSERT_NE(smembers, nullptr);
  ASSERT_NE(sismember, nullptr);
  ASSERT_NE(spop, nullptr);
  ASSERT_NE(srandmember, nullptr);
  ASSERT_NE(srem, nullptr);
  EXPECT_EQ(ping->owner_module, "core");
  EXPECT_EQ(type->owner_module, "core");
  EXPECT_EQ(exists->owner_module, "core");
  EXPECT_EQ(del->owner_module, "core");
  EXPECT_EQ(hset->owner_module, "hash");
  EXPECT_EQ(hgetall->owner_module, "hash");
  EXPECT_EQ(hdel->owner_module, "hash");
  EXPECT_EQ(lpush->owner_module, "list");
  EXPECT_EQ(lpop->owner_module, "list");
  EXPECT_EQ(lrange->owner_module, "list");
  EXPECT_EQ(rpush->owner_module, "list");
  EXPECT_EQ(rpop->owner_module, "list");
  EXPECT_EQ(lrem->owner_module, "list");
  EXPECT_EQ(ltrim->owner_module, "list");
  EXPECT_EQ(llen->owner_module, "list");
  EXPECT_EQ(sadd->owner_module, "set");
  EXPECT_EQ(scard->owner_module, "set");
  EXPECT_EQ(smembers->owner_module, "set");
  EXPECT_EQ(sismember->owner_module, "set");
  EXPECT_EQ(spop->owner_module, "set");
  EXPECT_EQ(srandmember->owner_module, "set");
  EXPECT_EQ(srem->owner_module, "set");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
