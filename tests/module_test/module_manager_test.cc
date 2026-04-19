#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "execution/command/cmd.h"
#include "gtest/gtest.h"
#include "execution/scheduler/scheduler.h"
#include "runtime/config.h"
#include "runtime/module/module_manager.h"
#include "runtime/module/module_services.h"
#include "storage/engine/storage_engine.h"
#include "types/bitmap/bitmap_module.h"
#include "core/core_module.h"
#include "types/hash/hash_module.h"
#include "types/json/json_module.h"
#include "types/list/list_module.h"
#include "types/string/string_module.h"
#include "types/set/set_module.h"
#include "types/geo/geo_module.h"
#include "types/stream/stream_module.h"
#include "types/zset/zset_module.h"

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

TEST(ModuleManagerTest, RejectsLegacyTypedDataStoredInModuleColumnFamily) {
  static int counter = 0;
  const std::string db_path =
      (std::filesystem::temp_directory_path() /
       ("minikv-legacy-layout-test-" + std::to_string(::getpid()) + "-" +
        std::to_string(counter++)))
          .string();

  {
    minikv::Config config;
    config.db_path = db_path;
    minikv::StorageEngine storage_engine;
    ASSERT_TRUE(storage_engine.Open(config).ok());

    const minikv::ModuleKeyspace legacy_keyspace("string", "data");
    ASSERT_TRUE(storage_engine
                    .Put(minikv::StorageColumnFamily::kModule,
                         legacy_keyspace.EncodeKey("legacy:key"), "value")
                    .ok());

    std::vector<std::string> events;
    std::vector<std::unique_ptr<minikv::Module>> modules;
    modules.push_back(
        std::make_unique<RecordingModule>("alpha", "ALPHA", &events));

    minikv::Scheduler scheduler(1, 8);
    minikv::ModuleManager manager(&storage_engine, &scheduler,
                                  std::move(modules));
    const rocksdb::Status status = manager.Initialize();
    ASSERT_TRUE(status.IsInvalidArgument());
    EXPECT_NE(status.ToString().find("legacy typed data"), std::string::npos);
    EXPECT_NE(status.ToString().find("string.data"), std::string::npos);
    EXPECT_TRUE(events.empty());
  }

  rocksdb::Options options;
  ASSERT_TRUE(rocksdb::DestroyDB(db_path, options).ok());
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
  modules.push_back(std::make_unique<minikv::StringModule>());
  modules.push_back(std::make_unique<minikv::BitmapModule>());
  modules.push_back(std::make_unique<minikv::HashModule>());
  modules.push_back(std::make_unique<minikv::JsonModule>());
  modules.push_back(std::make_unique<minikv::ListModule>());
  modules.push_back(std::make_unique<minikv::SetModule>());
  modules.push_back(std::make_unique<minikv::ZSetModule>());
  modules.push_back(std::make_unique<minikv::GeoModule>());
  modules.push_back(std::make_unique<minikv::StreamModule>());

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
  const minikv::CmdRegistration* json_set =
      manager.command_registry().Find("JSON.SET");
  const minikv::CmdRegistration* json_get =
      manager.command_registry().Find("JSON.GET");
  const minikv::CmdRegistration* set = manager.command_registry().Find("SET");
  const minikv::CmdRegistration* get = manager.command_registry().Find("GET");
  const minikv::CmdRegistration* strlen =
      manager.command_registry().Find("STRLEN");
  const minikv::CmdRegistration* getbit =
      manager.command_registry().Find("GETBIT");
  const minikv::CmdRegistration* setbit =
      manager.command_registry().Find("SETBIT");
  const minikv::CmdRegistration* bitcount =
      manager.command_registry().Find("BITCOUNT");
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
  const minikv::CmdRegistration* zadd = manager.command_registry().Find("ZADD");
  const minikv::CmdRegistration* zcard =
      manager.command_registry().Find("ZCARD");
  const minikv::CmdRegistration* zcount =
      manager.command_registry().Find("ZCOUNT");
  const minikv::CmdRegistration* zincrby =
      manager.command_registry().Find("ZINCRBY");
  const minikv::CmdRegistration* zlexcount =
      manager.command_registry().Find("ZLEXCOUNT");
  const minikv::CmdRegistration* zrange =
      manager.command_registry().Find("ZRANGE");
  const minikv::CmdRegistration* zrangebylex =
      manager.command_registry().Find("ZRANGEBYLEX");
  const minikv::CmdRegistration* zrangebyscore =
      manager.command_registry().Find("ZRANGEBYSCORE");
  const minikv::CmdRegistration* zrank =
      manager.command_registry().Find("ZRANK");
  const minikv::CmdRegistration* zrem = manager.command_registry().Find("ZREM");
  const minikv::CmdRegistration* zscore =
      manager.command_registry().Find("ZSCORE");
  const minikv::CmdRegistration* geoadd =
      manager.command_registry().Find("GEOADD");
  const minikv::CmdRegistration* geopos =
      manager.command_registry().Find("GEOPOS");
  const minikv::CmdRegistration* geohash =
      manager.command_registry().Find("GEOHASH");
  const minikv::CmdRegistration* geodist =
      manager.command_registry().Find("GEODIST");
  const minikv::CmdRegistration* geosearch =
      manager.command_registry().Find("GEOSEARCH");
  const minikv::CmdRegistration* xadd = manager.command_registry().Find("XADD");
  const minikv::CmdRegistration* xtrim =
      manager.command_registry().Find("XTRIM");
  const minikv::CmdRegistration* xdel = manager.command_registry().Find("XDEL");
  const minikv::CmdRegistration* xlen = manager.command_registry().Find("XLEN");
  const minikv::CmdRegistration* xrange =
      manager.command_registry().Find("XRANGE");
  const minikv::CmdRegistration* xrevrange =
      manager.command_registry().Find("XREVRANGE");
  const minikv::CmdRegistration* xread =
      manager.command_registry().Find("XREAD");

  ASSERT_NE(ping, nullptr);
  ASSERT_NE(type, nullptr);
  ASSERT_NE(exists, nullptr);
  ASSERT_NE(del, nullptr);
  ASSERT_NE(hset, nullptr);
  ASSERT_NE(hgetall, nullptr);
  ASSERT_NE(hdel, nullptr);
  ASSERT_NE(json_set, nullptr);
  ASSERT_NE(json_get, nullptr);
  ASSERT_NE(set, nullptr);
  ASSERT_NE(get, nullptr);
  ASSERT_NE(strlen, nullptr);
  ASSERT_NE(getbit, nullptr);
  ASSERT_NE(setbit, nullptr);
  ASSERT_NE(bitcount, nullptr);
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
  ASSERT_NE(zadd, nullptr);
  ASSERT_NE(zcard, nullptr);
  ASSERT_NE(zcount, nullptr);
  ASSERT_NE(zincrby, nullptr);
  ASSERT_NE(zlexcount, nullptr);
  ASSERT_NE(zrange, nullptr);
  ASSERT_NE(zrangebylex, nullptr);
  ASSERT_NE(zrangebyscore, nullptr);
  ASSERT_NE(zrank, nullptr);
  ASSERT_NE(zrem, nullptr);
  ASSERT_NE(zscore, nullptr);
  ASSERT_NE(geoadd, nullptr);
  ASSERT_NE(geopos, nullptr);
  ASSERT_NE(geohash, nullptr);
  ASSERT_NE(geodist, nullptr);
  ASSERT_NE(geosearch, nullptr);
  ASSERT_NE(xadd, nullptr);
  ASSERT_NE(xtrim, nullptr);
  ASSERT_NE(xdel, nullptr);
  ASSERT_NE(xlen, nullptr);
  ASSERT_NE(xrange, nullptr);
  ASSERT_NE(xrevrange, nullptr);
  ASSERT_NE(xread, nullptr);
  EXPECT_EQ(ping->owner_module, "core");
  EXPECT_EQ(type->owner_module, "core");
  EXPECT_EQ(exists->owner_module, "core");
  EXPECT_EQ(del->owner_module, "core");
  EXPECT_EQ(hset->owner_module, "hash");
  EXPECT_EQ(hgetall->owner_module, "hash");
  EXPECT_EQ(hdel->owner_module, "hash");
  EXPECT_EQ(json_set->owner_module, "json");
  EXPECT_EQ(json_get->owner_module, "json");
  EXPECT_EQ(set->owner_module, "string");
  EXPECT_EQ(get->owner_module, "string");
  EXPECT_EQ(strlen->owner_module, "string");
  EXPECT_EQ(getbit->owner_module, "bitmap");
  EXPECT_EQ(setbit->owner_module, "bitmap");
  EXPECT_EQ(bitcount->owner_module, "bitmap");
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
  EXPECT_EQ(zadd->owner_module, "zset");
  EXPECT_EQ(zcard->owner_module, "zset");
  EXPECT_EQ(zcount->owner_module, "zset");
  EXPECT_EQ(zincrby->owner_module, "zset");
  EXPECT_EQ(zlexcount->owner_module, "zset");
  EXPECT_EQ(zrange->owner_module, "zset");
  EXPECT_EQ(zrangebylex->owner_module, "zset");
  EXPECT_EQ(zrangebyscore->owner_module, "zset");
  EXPECT_EQ(zrank->owner_module, "zset");
  EXPECT_EQ(zrem->owner_module, "zset");
  EXPECT_EQ(zscore->owner_module, "zset");
  EXPECT_EQ(geoadd->owner_module, "geo");
  EXPECT_EQ(geopos->owner_module, "geo");
  EXPECT_EQ(geohash->owner_module, "geo");
  EXPECT_EQ(geodist->owner_module, "geo");
  EXPECT_EQ(geosearch->owner_module, "geo");
  EXPECT_EQ(xadd->owner_module, "stream");
  EXPECT_EQ(xtrim->owner_module, "stream");
  EXPECT_EQ(xdel->owner_module, "stream");
  EXPECT_EQ(xlen->owner_module, "stream");
  EXPECT_EQ(xrange->owner_module, "stream");
  EXPECT_EQ(xrevrange->owner_module, "stream");
  EXPECT_EQ(xread->owner_module, "stream");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
