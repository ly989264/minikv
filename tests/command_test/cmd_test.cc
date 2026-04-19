#include <algorithm>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "execution/command/cmd_create.h"
#include "runtime/config.h"
#include "gtest/gtest.h"
#include "execution/scheduler/scheduler.h"
#include "storage/engine/storage_engine.h"
#include "runtime/module/module.h"
#include "runtime/module/module_manager.h"
#include "core/core_module.h"
#include "types/hash/hash_module.h"
#include "types/list/list_module.h"
#include "types/string/string_module.h"
#include "types/set/set_module.h"
#include "types/stream/stream_module.h"
#include "types/zset/zset_module.h"
#include "rocksdb/db.h"

namespace {

enum class TestReplyMode {
  kSimpleString,
  kInteger,
  kArray,
};

void ExpectFlags(minikv::CmdFlags flags, bool expect_read, bool expect_write,
                 bool expect_fast, bool expect_slow) {
  EXPECT_EQ(minikv::HasFlag(flags, minikv::CmdFlags::kRead), expect_read);
  EXPECT_EQ(minikv::HasFlag(flags, minikv::CmdFlags::kWrite), expect_write);
  EXPECT_EQ(minikv::HasFlag(flags, minikv::CmdFlags::kFast), expect_fast);
  EXPECT_EQ(minikv::HasFlag(flags, minikv::CmdFlags::kSlow), expect_slow);
}

void ExpectBulkStringArray(const minikv::ReplyNode& reply,
                           const std::vector<std::string>& values) {
  ASSERT_TRUE(reply.IsArray());
  ASSERT_EQ(reply.array().size(), values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_TRUE(reply.array()[i].IsBulkString());
    EXPECT_EQ(reply.array()[i].string(), values[i]);
  }
}

void ExpectBulkStringArrayUnordered(const minikv::ReplyNode& reply,
                                    const std::vector<std::string>& values) {
  ASSERT_TRUE(reply.IsArray());
  ASSERT_EQ(reply.array().size(), values.size());

  std::vector<std::string> actual;
  actual.reserve(reply.array().size());
  for (const auto& node : reply.array()) {
    ASSERT_TRUE(node.IsBulkString());
    actual.push_back(node.string());
  }

  std::vector<std::string> actual_sorted = actual;
  std::vector<std::string> expected_sorted = values;
  std::sort(actual_sorted.begin(), actual_sorted.end());
  std::sort(expected_sorted.begin(), expected_sorted.end());
  EXPECT_EQ(actual_sorted, expected_sorted);
}

void ExpectBulkString(const minikv::ReplyNode& reply,
                      const std::string& value) {
  ASSERT_TRUE(reply.IsBulkString());
  EXPECT_EQ(reply.string(), value);
}

void ExpectStreamFieldArray(
    const minikv::ReplyNode& reply,
    const std::vector<minikv::StreamFieldValue>& expected) {
  ASSERT_TRUE(reply.IsArray());
  ASSERT_EQ(reply.array().size(), expected.size() * 2);
  for (size_t index = 0; index < expected.size(); ++index) {
    ASSERT_TRUE(reply.array()[index * 2].IsBulkString());
    ASSERT_TRUE(reply.array()[index * 2 + 1].IsBulkString());
    EXPECT_EQ(reply.array()[index * 2].string(), expected[index].field);
    EXPECT_EQ(reply.array()[index * 2 + 1].string(), expected[index].value);
  }
}

void ExpectStreamEntryReply(
    const minikv::ReplyNode& reply, const std::string& id,
    const std::vector<minikv::StreamFieldValue>& expected_fields) {
  ASSERT_TRUE(reply.IsArray());
  ASSERT_EQ(reply.array().size(), 2U);
  ASSERT_TRUE(reply.array()[0].IsBulkString());
  EXPECT_EQ(reply.array()[0].string(), id);
  ExpectStreamFieldArray(reply.array()[1], expected_fields);
}

void ExpectMembersUnordered(const std::vector<std::string>& actual,
                            const std::vector<std::string>& expected) {
  std::vector<std::string> actual_sorted = actual;
  std::vector<std::string> expected_sorted = expected;
  std::sort(actual_sorted.begin(), actual_sorted.end());
  std::sort(expected_sorted.begin(), expected_sorted.end());
  EXPECT_EQ(actual_sorted, expected_sorted);
}

void ExpectLockPlan(const minikv::Cmd::LockPlan& plan,
                    minikv::Cmd::LockPlan::Kind kind,
                    const std::string& single_key,
                    const std::vector<std::string>& multi_keys) {
  EXPECT_EQ(plan.kind(), kind);
  EXPECT_EQ(plan.single_key(), single_key);
  EXPECT_EQ(plan.multi_keys(), multi_keys);
}

class TestCmd : public minikv::Cmd {
 public:
  TestCmd()
      : minikv::Cmd("TEST",
                    minikv::CmdFlags::kRead | minikv::CmdFlags::kFast) {}

  void FailInit(bool value) { fail_init_ = value; }
  void SetResponseMode(TestReplyMode type) { response_type_ = type; }
  void SetStatusToReturn(rocksdb::Status status) {
    status_to_return_ = std::move(status);
  }
  void SetRouteKeyToExpose(const std::string& key) { SetRouteKey(key); }
  void SetRouteKeysToExpose(std::vector<std::string> keys) {
    SetRouteKeys(std::move(keys));
  }

 protected:
  minikv::CommandResponse MakeStatusPublic(rocksdb::Status status) {
    return MakeStatus(std::move(status));
  }
  minikv::CommandResponse MakeSimpleStringPublic(const std::string& text) {
    return MakeSimpleString(text);
  }
  minikv::CommandResponse MakeIntegerPublic(long long value) {
    return MakeInteger(value);
  }
  minikv::CommandResponse MakeArrayPublic(
      const std::vector<std::string>& values) {
    return MakeArray(values);
  }

 private:
  rocksdb::Status DoInitial(const minikv::CmdInput& input) override {
    if (fail_init_) {
      return rocksdb::Status::InvalidArgument("forced init failure");
    }
    if (input.has_key) {
      SetRouteKey(input.key);
    }
    return rocksdb::Status::OK();
  }

  minikv::CommandResponse Do() override {
    if (!status_to_return_.ok()) {
      return MakeStatus(std::move(status_to_return_));
    }
    switch (response_type_) {
      case TestReplyMode::kSimpleString:
        return MakeSimpleString("OK");
      case TestReplyMode::kInteger:
        return MakeInteger(7);
      case TestReplyMode::kArray:
        return MakeArray(std::vector<std::string>{"a", "b"});
    }
    return MakeStatus(rocksdb::Status::Aborted("unexpected response type"));
  }

  bool fail_init_ = false;
  TestReplyMode response_type_ = TestReplyMode::kSimpleString;
  rocksdb::Status status_to_return_ = rocksdb::Status::OK();
};

class ModuleRuntimeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-cmd-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
                   .string();
    minikv::Config config;
    config.db_path = db_path_;
    storage_engine_ = std::make_unique<minikv::StorageEngine>();
    ASSERT_TRUE(storage_engine_->Open(config).ok());
    scheduler_ = std::make_unique<minikv::Scheduler>(2, 16);

    std::vector<std::unique_ptr<minikv::Module>> modules;
    modules.push_back(std::make_unique<minikv::CoreModule>(
        [this]() { return now_ms_; }));
    auto hash_module = std::make_unique<minikv::HashModule>();
    hash_module_ = hash_module.get();
    modules.push_back(std::move(hash_module));
    auto string_module = std::make_unique<minikv::StringModule>();
    string_module_ = string_module.get();
    modules.push_back(std::move(string_module));
    auto list_module = std::make_unique<minikv::ListModule>();
    list_module_ = list_module.get();
    modules.push_back(std::move(list_module));
    auto set_module = std::make_unique<minikv::SetModule>();
    set_module_ = set_module.get();
    modules.push_back(std::move(set_module));
    auto zset_module = std::make_unique<minikv::ZSetModule>();
    zset_module_ = zset_module.get();
    modules.push_back(std::move(zset_module));
    auto stream_module = std::make_unique<minikv::StreamModule>();
    stream_module_ = stream_module.get();
    modules.push_back(std::move(stream_module));

    module_manager_ = std::make_unique<minikv::ModuleManager>(
        storage_engine_.get(), scheduler_.get(), std::move(modules));
    ASSERT_TRUE(module_manager_->Initialize().ok());
  }

  void TearDown() override {
    module_manager_.reset();
    scheduler_.reset();
    storage_engine_.reset();
    rocksdb::Options options;
    ASSERT_TRUE(rocksdb::DestroyDB(db_path_, options).ok());
  }

  const minikv::CommandRegistry& registry() const {
    return module_manager_->command_registry();
  }

  std::unique_ptr<minikv::Cmd> CreateFromParts(
      const std::vector<std::string>& parts) {
    std::unique_ptr<minikv::Cmd> cmd;
    EXPECT_TRUE(minikv::CreateCmd(registry(), parts, &cmd).ok());
    return cmd;
  }

  void AdvanceTimeMs(uint64_t delta_ms) { now_ms_ += delta_ms; }

  static inline int counter_ = 0;
  std::string db_path_;
  uint64_t now_ms_ = 10'000;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  minikv::HashModule* hash_module_ = nullptr;
  minikv::StringModule* string_module_ = nullptr;
  minikv::ListModule* list_module_ = nullptr;
  minikv::SetModule* set_module_ = nullptr;
  minikv::ZSetModule* zset_module_ = nullptr;
  minikv::StreamModule* stream_module_ = nullptr;
};

TEST_F(ModuleRuntimeTest, FindsRegisteredCommandsByName) {
  const minikv::CmdRegistration* ping = registry().Find("PING");
  ASSERT_NE(ping, nullptr);
  EXPECT_EQ(ping->source, minikv::CommandSource::kBuiltin);
  EXPECT_EQ(ping->owner_module, "core");
  ExpectFlags(ping->flags, true, false, true, false);

  const minikv::CmdRegistration* type = registry().Find("TYPE");
  ASSERT_NE(type, nullptr);
  EXPECT_EQ(type->name, "TYPE");
  EXPECT_EQ(type->owner_module, "core");
  ExpectFlags(type->flags, true, false, true, false);

  const minikv::CmdRegistration* exists = registry().Find("EXISTS");
  ASSERT_NE(exists, nullptr);
  EXPECT_EQ(exists->name, "EXISTS");
  EXPECT_EQ(exists->owner_module, "core");
  ExpectFlags(exists->flags, true, false, true, false);

  const minikv::CmdRegistration* del = registry().Find("DEL");
  ASSERT_NE(del, nullptr);
  EXPECT_EQ(del->name, "DEL");
  EXPECT_EQ(del->owner_module, "core");
  ExpectFlags(del->flags, false, true, false, true);

  const minikv::CmdRegistration* expire = registry().Find("EXPIRE");
  ASSERT_NE(expire, nullptr);
  EXPECT_EQ(expire->name, "EXPIRE");
  EXPECT_EQ(expire->owner_module, "core");
  ExpectFlags(expire->flags, false, true, false, true);

  const minikv::CmdRegistration* ttl = registry().Find("TTL");
  ASSERT_NE(ttl, nullptr);
  EXPECT_EQ(ttl->name, "TTL");
  EXPECT_EQ(ttl->owner_module, "core");
  ExpectFlags(ttl->flags, true, false, true, false);

  const minikv::CmdRegistration* pttl = registry().Find("PTTL");
  ASSERT_NE(pttl, nullptr);
  EXPECT_EQ(pttl->name, "PTTL");
  EXPECT_EQ(pttl->owner_module, "core");
  ExpectFlags(pttl->flags, true, false, true, false);

  const minikv::CmdRegistration* persist = registry().Find("PERSIST");
  ASSERT_NE(persist, nullptr);
  EXPECT_EQ(persist->name, "PERSIST");
  EXPECT_EQ(persist->owner_module, "core");
  ExpectFlags(persist->flags, false, true, true, false);

  const minikv::CmdRegistration* hset = registry().Find("HSET");
  ASSERT_NE(hset, nullptr);
  EXPECT_EQ(hset->name, "HSET");
  EXPECT_EQ(hset->owner_module, "hash");
  ExpectFlags(hset->flags, false, true, true, false);

  const minikv::CmdRegistration* set = registry().Find("SET");
  ASSERT_NE(set, nullptr);
  EXPECT_EQ(set->name, "SET");
  EXPECT_EQ(set->owner_module, "string");
  ExpectFlags(set->flags, false, true, true, false);

  const minikv::CmdRegistration* get = registry().Find("GET");
  ASSERT_NE(get, nullptr);
  EXPECT_EQ(get->name, "GET");
  EXPECT_EQ(get->owner_module, "string");
  ExpectFlags(get->flags, true, false, true, false);

  const minikv::CmdRegistration* strlen = registry().Find("STRLEN");
  ASSERT_NE(strlen, nullptr);
  EXPECT_EQ(strlen->name, "STRLEN");
  EXPECT_EQ(strlen->owner_module, "string");
  ExpectFlags(strlen->flags, true, false, true, false);

  const minikv::CmdRegistration* hgetall = registry().Find("HGETALL");
  ASSERT_NE(hgetall, nullptr);
  EXPECT_EQ(hgetall->name, "HGETALL");
  EXPECT_EQ(hgetall->owner_module, "hash");
  ExpectFlags(hgetall->flags, true, false, false, true);

  const minikv::CmdRegistration* hdel = registry().Find("HDEL");
  ASSERT_NE(hdel, nullptr);
  EXPECT_EQ(hdel->name, "HDEL");
  EXPECT_EQ(hdel->owner_module, "hash");
  ExpectFlags(hdel->flags, false, true, false, true);

  const minikv::CmdRegistration* lpush = registry().Find("LPUSH");
  ASSERT_NE(lpush, nullptr);
  EXPECT_EQ(lpush->name, "LPUSH");
  EXPECT_EQ(lpush->owner_module, "list");
  ExpectFlags(lpush->flags, false, true, true, false);

  const minikv::CmdRegistration* lpop = registry().Find("LPOP");
  ASSERT_NE(lpop, nullptr);
  EXPECT_EQ(lpop->name, "LPOP");
  EXPECT_EQ(lpop->owner_module, "list");
  ExpectFlags(lpop->flags, false, true, true, false);

  const minikv::CmdRegistration* lrange = registry().Find("LRANGE");
  ASSERT_NE(lrange, nullptr);
  EXPECT_EQ(lrange->name, "LRANGE");
  EXPECT_EQ(lrange->owner_module, "list");
  ExpectFlags(lrange->flags, true, false, false, true);

  const minikv::CmdRegistration* rpush = registry().Find("RPUSH");
  ASSERT_NE(rpush, nullptr);
  EXPECT_EQ(rpush->name, "RPUSH");
  EXPECT_EQ(rpush->owner_module, "list");
  ExpectFlags(rpush->flags, false, true, true, false);

  const minikv::CmdRegistration* rpop = registry().Find("RPOP");
  ASSERT_NE(rpop, nullptr);
  EXPECT_EQ(rpop->name, "RPOP");
  EXPECT_EQ(rpop->owner_module, "list");
  ExpectFlags(rpop->flags, false, true, true, false);

  const minikv::CmdRegistration* lrem = registry().Find("LREM");
  ASSERT_NE(lrem, nullptr);
  EXPECT_EQ(lrem->name, "LREM");
  EXPECT_EQ(lrem->owner_module, "list");
  ExpectFlags(lrem->flags, false, true, false, true);

  const minikv::CmdRegistration* ltrim = registry().Find("LTRIM");
  ASSERT_NE(ltrim, nullptr);
  EXPECT_EQ(ltrim->name, "LTRIM");
  EXPECT_EQ(ltrim->owner_module, "list");
  ExpectFlags(ltrim->flags, false, true, false, true);

  const minikv::CmdRegistration* llen = registry().Find("LLEN");
  ASSERT_NE(llen, nullptr);
  EXPECT_EQ(llen->name, "LLEN");
  EXPECT_EQ(llen->owner_module, "list");
  ExpectFlags(llen->flags, true, false, true, false);

  const minikv::CmdRegistration* sadd = registry().Find("SADD");
  ASSERT_NE(sadd, nullptr);
  EXPECT_EQ(sadd->name, "SADD");
  EXPECT_EQ(sadd->owner_module, "set");
  ExpectFlags(sadd->flags, false, true, true, false);

  const minikv::CmdRegistration* scard = registry().Find("SCARD");
  ASSERT_NE(scard, nullptr);
  EXPECT_EQ(scard->name, "SCARD");
  EXPECT_EQ(scard->owner_module, "set");
  ExpectFlags(scard->flags, true, false, true, false);

  const minikv::CmdRegistration* smembers = registry().Find("SMEMBERS");
  ASSERT_NE(smembers, nullptr);
  EXPECT_EQ(smembers->name, "SMEMBERS");
  EXPECT_EQ(smembers->owner_module, "set");
  ExpectFlags(smembers->flags, true, false, false, true);

  const minikv::CmdRegistration* sismember = registry().Find("SISMEMBER");
  ASSERT_NE(sismember, nullptr);
  EXPECT_EQ(sismember->name, "SISMEMBER");
  EXPECT_EQ(sismember->owner_module, "set");
  ExpectFlags(sismember->flags, true, false, true, false);

  const minikv::CmdRegistration* spop = registry().Find("SPOP");
  ASSERT_NE(spop, nullptr);
  EXPECT_EQ(spop->name, "SPOP");
  EXPECT_EQ(spop->owner_module, "set");
  ExpectFlags(spop->flags, false, true, false, true);

  const minikv::CmdRegistration* srandmember = registry().Find("SRANDMEMBER");
  ASSERT_NE(srandmember, nullptr);
  EXPECT_EQ(srandmember->name, "SRANDMEMBER");
  EXPECT_EQ(srandmember->owner_module, "set");
  ExpectFlags(srandmember->flags, true, false, false, true);

  const minikv::CmdRegistration* srem = registry().Find("SREM");
  ASSERT_NE(srem, nullptr);
  EXPECT_EQ(srem->name, "SREM");
  EXPECT_EQ(srem->owner_module, "set");
  ExpectFlags(srem->flags, false, true, false, true);

  const minikv::CmdRegistration* zadd = registry().Find("ZADD");
  ASSERT_NE(zadd, nullptr);
  EXPECT_EQ(zadd->name, "ZADD");
  EXPECT_EQ(zadd->owner_module, "zset");
  ExpectFlags(zadd->flags, false, true, true, false);

  const minikv::CmdRegistration* zcard = registry().Find("ZCARD");
  ASSERT_NE(zcard, nullptr);
  EXPECT_EQ(zcard->name, "ZCARD");
  EXPECT_EQ(zcard->owner_module, "zset");
  ExpectFlags(zcard->flags, true, false, true, false);

  const minikv::CmdRegistration* zcount = registry().Find("ZCOUNT");
  ASSERT_NE(zcount, nullptr);
  EXPECT_EQ(zcount->name, "ZCOUNT");
  EXPECT_EQ(zcount->owner_module, "zset");
  ExpectFlags(zcount->flags, true, false, false, true);

  const minikv::CmdRegistration* zincrby = registry().Find("ZINCRBY");
  ASSERT_NE(zincrby, nullptr);
  EXPECT_EQ(zincrby->name, "ZINCRBY");
  EXPECT_EQ(zincrby->owner_module, "zset");
  ExpectFlags(zincrby->flags, false, true, true, false);

  const minikv::CmdRegistration* zlexcount = registry().Find("ZLEXCOUNT");
  ASSERT_NE(zlexcount, nullptr);
  EXPECT_EQ(zlexcount->name, "ZLEXCOUNT");
  EXPECT_EQ(zlexcount->owner_module, "zset");
  ExpectFlags(zlexcount->flags, true, false, false, true);

  const minikv::CmdRegistration* zrange = registry().Find("ZRANGE");
  ASSERT_NE(zrange, nullptr);
  EXPECT_EQ(zrange->name, "ZRANGE");
  EXPECT_EQ(zrange->owner_module, "zset");
  ExpectFlags(zrange->flags, true, false, false, true);

  const minikv::CmdRegistration* zrangebylex = registry().Find("ZRANGEBYLEX");
  ASSERT_NE(zrangebylex, nullptr);
  EXPECT_EQ(zrangebylex->name, "ZRANGEBYLEX");
  EXPECT_EQ(zrangebylex->owner_module, "zset");
  ExpectFlags(zrangebylex->flags, true, false, false, true);

  const minikv::CmdRegistration* zrangebyscore =
      registry().Find("ZRANGEBYSCORE");
  ASSERT_NE(zrangebyscore, nullptr);
  EXPECT_EQ(zrangebyscore->name, "ZRANGEBYSCORE");
  EXPECT_EQ(zrangebyscore->owner_module, "zset");
  ExpectFlags(zrangebyscore->flags, true, false, false, true);

  const minikv::CmdRegistration* zrank = registry().Find("ZRANK");
  ASSERT_NE(zrank, nullptr);
  EXPECT_EQ(zrank->name, "ZRANK");
  EXPECT_EQ(zrank->owner_module, "zset");
  ExpectFlags(zrank->flags, true, false, false, true);

  const minikv::CmdRegistration* zrem = registry().Find("ZREM");
  ASSERT_NE(zrem, nullptr);
  EXPECT_EQ(zrem->name, "ZREM");
  EXPECT_EQ(zrem->owner_module, "zset");
  ExpectFlags(zrem->flags, false, true, false, true);

  const minikv::CmdRegistration* zscore = registry().Find("ZSCORE");
  ASSERT_NE(zscore, nullptr);
  EXPECT_EQ(zscore->name, "ZSCORE");
  EXPECT_EQ(zscore->owner_module, "zset");
  ExpectFlags(zscore->flags, true, false, true, false);

  const minikv::CmdRegistration* xadd = registry().Find("XADD");
  ASSERT_NE(xadd, nullptr);
  EXPECT_EQ(xadd->name, "XADD");
  EXPECT_EQ(xadd->owner_module, "stream");
  ExpectFlags(xadd->flags, false, true, true, false);

  const minikv::CmdRegistration* xtrim = registry().Find("XTRIM");
  ASSERT_NE(xtrim, nullptr);
  EXPECT_EQ(xtrim->name, "XTRIM");
  EXPECT_EQ(xtrim->owner_module, "stream");
  ExpectFlags(xtrim->flags, false, true, false, true);

  const minikv::CmdRegistration* xdel = registry().Find("XDEL");
  ASSERT_NE(xdel, nullptr);
  EXPECT_EQ(xdel->name, "XDEL");
  EXPECT_EQ(xdel->owner_module, "stream");
  ExpectFlags(xdel->flags, false, true, false, true);

  const minikv::CmdRegistration* xlen = registry().Find("XLEN");
  ASSERT_NE(xlen, nullptr);
  EXPECT_EQ(xlen->name, "XLEN");
  EXPECT_EQ(xlen->owner_module, "stream");
  ExpectFlags(xlen->flags, true, false, true, false);

  const minikv::CmdRegistration* xrange = registry().Find("XRANGE");
  ASSERT_NE(xrange, nullptr);
  EXPECT_EQ(xrange->name, "XRANGE");
  EXPECT_EQ(xrange->owner_module, "stream");
  ExpectFlags(xrange->flags, true, false, false, true);

  const minikv::CmdRegistration* xrevrange = registry().Find("XREVRANGE");
  ASSERT_NE(xrevrange, nullptr);
  EXPECT_EQ(xrevrange->name, "XREVRANGE");
  EXPECT_EQ(xrevrange->owner_module, "stream");
  ExpectFlags(xrevrange->flags, true, false, false, true);

  const minikv::CmdRegistration* xread = registry().Find("XREAD");
  ASSERT_NE(xread, nullptr);
  EXPECT_EQ(xread->name, "XREAD");
  EXPECT_EQ(xread->owner_module, "stream");
  ExpectFlags(xread->flags, true, false, false, true);
}

TEST_F(ModuleRuntimeTest, ReturnsNullForUnknownRegistrations) {
  EXPECT_EQ(registry().Find("ping"), nullptr);
  EXPECT_EQ(registry().Find("UNKNOWN"), nullptr);
}

TEST_F(ModuleRuntimeTest, CreatesCommandsFromRespParts) {
  std::unique_ptr<minikv::Cmd> ping;
  ASSERT_TRUE(minikv::CreateCmd(registry(), {"PING"}, &ping).ok());
  ASSERT_NE(ping, nullptr);
  EXPECT_EQ(ping->Name(), "PING");
  EXPECT_TRUE(ping->RouteKey().empty());
  ExpectLockPlan(ping->lock_plan(), minikv::Cmd::LockPlan::Kind::kNone, "", {});
  ExpectFlags(ping->Flags(), true, false, true, false);

  std::unique_ptr<minikv::Cmd> type;
  ASSERT_TRUE(minikv::CreateCmd(registry(), {"TYPE", "user:1"}, &type).ok());
  ASSERT_NE(type, nullptr);
  EXPECT_EQ(type->Name(), "TYPE");
  EXPECT_EQ(type->RouteKey(), "user:1");
  ExpectLockPlan(type->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "user:1", {});
  ExpectFlags(type->Flags(), true, false, true, false);

  std::unique_ptr<minikv::Cmd> exists;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(),
                        {"EXISTS", "user:3", "user:1", "user:2", "user:1"},
                        &exists)
          .ok());
  ASSERT_NE(exists, nullptr);
  EXPECT_EQ(exists->Name(), "EXISTS");
  EXPECT_TRUE(exists->RouteKey().empty());
  ExpectLockPlan(exists->lock_plan(), minikv::Cmd::LockPlan::Kind::kMulti, "",
                 {"user:1", "user:2", "user:3"});
  ExpectFlags(exists->Flags(), true, false, true, false);

  std::unique_ptr<minikv::Cmd> del;
  ASSERT_TRUE(minikv::CreateCmd(registry(),
                                {"DEL", "user:4", "user:2", "user:3",
                                 "user:2"},
                                &del)
                  .ok());
  ASSERT_NE(del, nullptr);
  EXPECT_EQ(del->Name(), "DEL");
  EXPECT_TRUE(del->RouteKey().empty());
  ExpectLockPlan(del->lock_plan(), minikv::Cmd::LockPlan::Kind::kMulti, "",
                 {"user:2", "user:3", "user:4"});
  ExpectFlags(del->Flags(), false, true, false, true);

  std::unique_ptr<minikv::Cmd> expire;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"EXPIRE", "user:ttl", "5"}, &expire).ok());
  ASSERT_NE(expire, nullptr);
  EXPECT_EQ(expire->Name(), "EXPIRE");
  EXPECT_EQ(expire->RouteKey(), "user:ttl");
  ExpectLockPlan(expire->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "user:ttl", {});
  ExpectFlags(expire->Flags(), false, true, false, true);

  std::unique_ptr<minikv::Cmd> ttl;
  ASSERT_TRUE(minikv::CreateCmd(registry(), {"TTL", "user:ttl"}, &ttl).ok());
  ASSERT_NE(ttl, nullptr);
  EXPECT_EQ(ttl->Name(), "TTL");
  EXPECT_EQ(ttl->RouteKey(), "user:ttl");
  ExpectLockPlan(ttl->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "user:ttl", {});
  ExpectFlags(ttl->Flags(), true, false, true, false);

  std::unique_ptr<minikv::Cmd> hset;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"HSET", "user:1", "name", "alice"}, &hset)
          .ok());
  ASSERT_NE(hset, nullptr);
  EXPECT_EQ(hset->Name(), "HSET");
  EXPECT_EQ(hset->RouteKey(), "user:1");
  ExpectLockPlan(hset->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "user:1", {});
  ExpectFlags(hset->Flags(), false, true, true, false);

  std::unique_ptr<minikv::Cmd> lower;
  ASSERT_TRUE(minikv::CreateCmd(registry(), {"hgetall", "user:1"}, &lower).ok());
  ASSERT_NE(lower, nullptr);
  EXPECT_EQ(lower->Name(), "HGETALL");

  std::unique_ptr<minikv::Cmd> set;
  ASSERT_TRUE(minikv::CreateCmd(registry(), {"SET", "str:1", "value"}, &set).ok());
  ASSERT_NE(set, nullptr);
  EXPECT_EQ(set->Name(), "SET");
  EXPECT_EQ(set->RouteKey(), "str:1");
  ExpectLockPlan(set->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "str:1", {});
  ExpectFlags(set->Flags(), false, true, true, false);

  std::unique_ptr<minikv::Cmd> lower_string;
  ASSERT_TRUE(minikv::CreateCmd(registry(), {"get", "str:1"}, &lower_string)
                  .ok());
  ASSERT_NE(lower_string, nullptr);
  EXPECT_EQ(lower_string->Name(), "GET");

  std::unique_ptr<minikv::Cmd> lpush;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"LPUSH", "list:1", "a", "b"}, &lpush)
          .ok());
  ASSERT_NE(lpush, nullptr);
  EXPECT_EQ(lpush->Name(), "LPUSH");
  EXPECT_EQ(lpush->RouteKey(), "list:1");
  ExpectLockPlan(lpush->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "list:1", {});
  ExpectFlags(lpush->Flags(), false, true, true, false);

  std::unique_ptr<minikv::Cmd> lower_list;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"lrange", "list:1", "0", "-1"},
                        &lower_list)
          .ok());
  ASSERT_NE(lower_list, nullptr);
  EXPECT_EQ(lower_list->Name(), "LRANGE");

  std::unique_ptr<minikv::Cmd> sadd;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"SADD", "set:1", "a", "b", "a"}, &sadd)
          .ok());
  ASSERT_NE(sadd, nullptr);
  EXPECT_EQ(sadd->Name(), "SADD");
  EXPECT_EQ(sadd->RouteKey(), "set:1");
  ExpectLockPlan(sadd->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "set:1", {});
  ExpectFlags(sadd->Flags(), false, true, true, false);

  std::unique_ptr<minikv::Cmd> lower_set;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"srem", "set:1", "a"}, &lower_set).ok());
  ASSERT_NE(lower_set, nullptr);
  EXPECT_EQ(lower_set->Name(), "SREM");

  std::unique_ptr<minikv::Cmd> zadd;
  ASSERT_TRUE(minikv::CreateCmd(registry(),
                                {"ZADD", "zset:1", "1", "a", "2", "b"},
                                &zadd)
                  .ok());
  ASSERT_NE(zadd, nullptr);
  EXPECT_EQ(zadd->Name(), "ZADD");
  EXPECT_EQ(zadd->RouteKey(), "zset:1");
  ExpectLockPlan(zadd->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "zset:1", {});
  ExpectFlags(zadd->Flags(), false, true, true, false);

  std::unique_ptr<minikv::Cmd> lower_zset;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"zscore", "zset:1", "a"}, &lower_zset)
          .ok());
  ASSERT_NE(lower_zset, nullptr);
  EXPECT_EQ(lower_zset->Name(), "ZSCORE");

  std::unique_ptr<minikv::Cmd> xadd;
  ASSERT_TRUE(minikv::CreateCmd(
                  registry(),
                  {"XADD", "stream:1", "1-0", "field", "value", "city",
                   "shanghai"},
                  &xadd)
                  .ok());
  ASSERT_NE(xadd, nullptr);
  EXPECT_EQ(xadd->Name(), "XADD");
  EXPECT_EQ(xadd->RouteKey(), "stream:1");
  ExpectLockPlan(xadd->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "stream:1", {});
  ExpectFlags(xadd->Flags(), false, true, true, false);

  std::unique_ptr<minikv::Cmd> xread;
  ASSERT_TRUE(minikv::CreateCmd(
                  registry(),
                  {"XREAD", "STREAMS", "stream:3", "stream:1", "0-0", "1-0"},
                  &xread)
                  .ok());
  ASSERT_NE(xread, nullptr);
  EXPECT_EQ(xread->Name(), "XREAD");
  EXPECT_TRUE(xread->RouteKey().empty());
  ExpectLockPlan(xread->lock_plan(), minikv::Cmd::LockPlan::Kind::kMulti, "",
                 {"stream:1", "stream:3"});
  ExpectFlags(xread->Flags(), true, false, false, true);
}

TEST_F(ModuleRuntimeTest, RejectsBadArgumentsAndNullOutputs) {
  EXPECT_TRUE(
      minikv::CreateCmd(registry(), std::vector<std::string>{"PING"}, nullptr)
          .IsInvalidArgument());

  std::unique_ptr<minikv::Cmd> cmd;
  rocksdb::Status status =
      minikv::CreateCmd(registry(), std::vector<std::string>{}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("empty command"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"UNKNOWN"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("unsupported command"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"HSET"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("missing key"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"HSET", "user:1", "field"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HSET requires field and value"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"HGETALL", "user:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HGETALL takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SET", "str:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SET requires value"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"SET", "str:1", "a", "b"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SET requires value"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"GET", "str:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("GET takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"STRLEN", "str:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("STRLEN takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"HDEL", "user:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HDEL requires at least one field"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"LPUSH", "list:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("LPUSH requires at least one element"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"LPOP", "list:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("LPOP takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"LRANGE", "list:1", "0"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("LRANGE requires start and stop"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"LRANGE", "list:1", "bad", "1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("LRANGE requires integer start"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"LRANGE", "list:1", "0", "bad"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("LRANGE requires integer stop"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"RPUSH", "list:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("RPUSH requires at least one element"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"RPOP", "list:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("RPOP takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"LREM", "list:1", "0"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("LREM requires count and element"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"LREM", "list:1", "bad", "a"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("LREM requires integer count"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"LTRIM", "list:1", "0"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("LTRIM requires start and stop"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"LTRIM", "list:1", "bad", "1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("LTRIM requires integer start"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"LTRIM", "list:1", "0", "bad"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("LTRIM requires integer stop"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"LLEN", "list:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("LLEN takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SADD", "set:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SADD requires at least one member"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SCARD", "set:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SCARD takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SMEMBERS", "set:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SMEMBERS takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SISMEMBER", "set:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SISMEMBER requires member"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SPOP", "set:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SPOP takes no extra arguments"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"SRANDMEMBER", "set:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SRANDMEMBER takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SREM", "set:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SREM requires at least one member"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"ZADD", "zset:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZADD requires score/member pairs"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"ZADD", "zset:1", "1", "a", "2"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZADD requires score/member pairs"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"ZADD", "zset:1", "bad", "a"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZADD requires valid score"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"ZCARD", "zset:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZCARD takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"ZCOUNT", "zset:1", "0"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZCOUNT requires min and max"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"ZINCRBY", "zset:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZINCRBY requires increment and member"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"ZINCRBY", "zset:1", "bad", "a"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZINCRBY requires valid increment"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"ZLEXCOUNT", "zset:1", "-"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZLEXCOUNT requires min and max"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"ZRANGE", "zset:1", "0"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZRANGE requires start and stop"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"ZRANGE", "zset:1", "bad", "1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZRANGE requires integer start"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"ZRANGE", "zset:1", "0", "bad"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZRANGE requires integer stop"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"ZRANGEBYLEX", "zset:1", "-"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZRANGEBYLEX requires min and max"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"ZRANGEBYSCORE", "zset:1", "0"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZRANGEBYSCORE requires min and max"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"ZRANK", "zset:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZRANK requires member"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"ZREM", "zset:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZREM requires at least one member"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"ZSCORE", "zset:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("ZSCORE requires member"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"XADD", "stream:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("XADD requires id and field/value pairs"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"XADD", "stream:1", "1-0", "field"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("XADD requires id and field/value pairs"),
            std::string::npos);

  status = minikv::CreateCmd(
      registry(), {"XADD", "stream:1", "bad-id", "field", "value"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("valid id"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"XTRIM", "stream:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("XTRIM requires MAXLEN and threshold"),
            std::string::npos);

  status = minikv::CreateCmd(
      registry(), {"XTRIM", "stream:1", "MINID", "1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("XTRIM requires MAXLEN and threshold"),
            std::string::npos);

  status = minikv::CreateCmd(
      registry(), {"XTRIM", "stream:1", "MAXLEN", "bad"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("integer threshold"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"XDEL", "stream:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("XDEL requires at least one id"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"XDEL", "stream:1", "bad-id"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("XDEL requires valid id"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"XLEN", "stream:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("XLEN takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"XRANGE", "stream:1", "-"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("XRANGE requires start and end"),
            std::string::npos);

  status = minikv::CreateCmd(
      registry(), {"XRANGE", "stream:1", "bad", "+"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("valid start id"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"XREVRANGE", "stream:1", "+"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("XREVRANGE requires end and start"),
            std::string::npos);

  status = minikv::CreateCmd(
      registry(), {"XREVRANGE", "stream:1", "+", "bad"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("valid start id"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"XREAD"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("XREAD requires STREAMS keyword"),
            std::string::npos);

  status = minikv::CreateCmd(
      registry(), {"XREAD", "COUNT", "1", "STREAMS", "stream:1", "0-0"},
      &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("XREAD requires STREAMS keyword"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"XREAD", "STREAMS", "stream:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("matching stream keys and ids"),
            std::string::npos);

  status = minikv::CreateCmd(
      registry(), {"XREAD", "STREAMS", "stream:1", "$"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("valid id"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"PING", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("PING takes no arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"TYPE"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("missing key"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"TYPE", "user:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("TYPE takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"EXISTS"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("missing key"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"DEL"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("missing key"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"EXPIRE"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("missing key"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"EXPIRE", "user:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("EXPIRE requires seconds"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"EXPIRE", "user:1", "bad"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("EXPIRE requires integer seconds"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"TTL"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("missing key"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"TTL", "user:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("TTL takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"PTTL", "user:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("PTTL takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"PERSIST"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("missing key"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"PERSIST", "user:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("PERSIST takes no extra arguments"),
            std::string::npos);
}

TEST(CmdBaseTest, ExecuteRejectsUninitializedCommand) {
  TestCmd cmd;
  minikv::CommandResponse response = cmd.Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find(
                "command must be initialized before execution"),
            std::string::npos);
}

TEST(CmdBaseTest, FailedInitClearsPreviousLockPlan) {
  TestCmd cmd;
  minikv::CmdInput input;
  input.has_key = true;
  input.key = "route:1";
  ASSERT_TRUE(cmd.Init(input).ok());
  EXPECT_EQ(cmd.RouteKey(), "route:1");
  ExpectLockPlan(cmd.lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "route:1", {});

  cmd.FailInit(true);
  rocksdb::Status status = cmd.Init(input);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_TRUE(cmd.RouteKey().empty());
  ExpectLockPlan(cmd.lock_plan(), minikv::Cmd::LockPlan::Kind::kNone, "", {});
}

TEST(CmdBaseTest, SetRouteKeysCanonicalizesDuplicates) {
  TestCmd cmd;
  ASSERT_TRUE(cmd.Init(minikv::CmdInput{}).ok());

  cmd.SetRouteKeysToExpose({"user:3", "user:1", "user:2", "user:1"});
  ExpectLockPlan(cmd.lock_plan(), minikv::Cmd::LockPlan::Kind::kMulti, "",
                 {"user:1", "user:2", "user:3"});
}

TEST(CmdBaseTest, SharedResponseBuildersProduceExpectedShapes) {
  TestCmd cmd;
  ASSERT_TRUE(cmd.Init(minikv::CmdInput{}).ok());

  cmd.SetResponseMode(TestReplyMode::kSimpleString);
  minikv::CommandResponse response = cmd.Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());
  EXPECT_EQ(response.reply.string(), "OK");

  cmd.SetResponseMode(TestReplyMode::kInteger);
  response = cmd.Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 7);

  cmd.SetResponseMode(TestReplyMode::kArray);
  response = cmd.Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {"a", "b"});

  cmd.SetStatusToReturn(rocksdb::Status::Corruption("forced"));
  response = cmd.Execute();
  ASSERT_TRUE(response.status.IsCorruption());
}

TEST_F(ModuleRuntimeTest, EmptyStringKeyRemainsValidForHashCommands) {
  std::unique_ptr<minikv::Cmd> cmd;
  ASSERT_TRUE(minikv::CreateCmd(registry(), {"HGETALL", ""}, &cmd).ok());
  ASSERT_NE(cmd, nullptr);
  EXPECT_EQ(cmd->RouteKey(), "");
  EXPECT_EQ(cmd->Name(), "HGETALL");
  ExpectLockPlan(cmd->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle, "",
                 {});
}

TEST_F(ModuleRuntimeTest, EmptyStringKeyRemainsValidForStringCommands) {
  std::unique_ptr<minikv::Cmd> cmd;
  ASSERT_TRUE(minikv::CreateCmd(registry(), {"GET", ""}, &cmd).ok());
  ASSERT_NE(cmd, nullptr);
  EXPECT_EQ(cmd->RouteKey(), "");
  EXPECT_EQ(cmd->Name(), "GET");
  ExpectLockPlan(cmd->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle, "",
                 {});
}

TEST_F(ModuleRuntimeTest, PingExecuteReturnsPong) {
  std::unique_ptr<minikv::Cmd> ping = CreateFromParts({"PING"});
  ASSERT_NE(ping, nullptr);

  minikv::CommandResponse response = ping->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());
  EXPECT_EQ(response.reply.string(), "PONG");
}

TEST_F(ModuleRuntimeTest, TypeAndExistsExecuteAgainstEngine) {
  ASSERT_TRUE(hash_module_->PutField("user:type", "name", "alice", nullptr).ok());

  std::unique_ptr<minikv::Cmd> type = CreateFromParts({"TYPE", "user:type"});
  ASSERT_NE(type, nullptr);
  minikv::CommandResponse response = type->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "hash");

  std::unique_ptr<minikv::Cmd> exists =
      CreateFromParts({"EXISTS", "user:type", "user:type", "missing"});
  ASSERT_NE(exists, nullptr);
  response = exists->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);
}

TEST_F(ModuleRuntimeTest, ExpireTtlPttlAndPersistExecuteAgainstEngine) {
  ASSERT_TRUE(hash_module_->PutField("user:ttl", "name", "alice", nullptr).ok());

  minikv::CommandResponse response = CreateFromParts({"TTL", "user:ttl"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), -1);

  response = CreateFromParts({"EXPIRE", "user:ttl", "5"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"PTTL", "user:ttl"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 5000);

  response = CreateFromParts({"TTL", "user:ttl"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 5);

  response = CreateFromParts({"PERSIST", "user:ttl"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"TTL", "user:ttl"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), -1);

  response = CreateFromParts({"PERSIST", "user:ttl"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
}

TEST_F(ModuleRuntimeTest, ExpiredKeysBehaveLikeMissingForCoreCommands) {
  ASSERT_TRUE(
      hash_module_->PutField("user:expired", "name", "alice", nullptr).ok());
  minikv::CommandResponse response =
      CreateFromParts({"EXPIRE", "user:expired", "5"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), 1);

  AdvanceTimeMs(5000);

  response = CreateFromParts({"TTL", "user:expired"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), -2);

  response = CreateFromParts({"PTTL", "user:expired"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), -2);

  response = CreateFromParts({"TYPE", "user:expired"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "none");

  response = CreateFromParts({"EXISTS", "user:expired"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"PERSIST", "user:expired"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
}

TEST_F(ModuleRuntimeTest, ExpireZeroDeletesAndRecreateSeesFreshKey) {
  ASSERT_TRUE(
      hash_module_->PutField("user:expire0", "name", "alice", nullptr).ok());
  ASSERT_TRUE(
      hash_module_->PutField("user:expire0", "city", "shanghai", nullptr).ok());

  minikv::CommandResponse response =
      CreateFromParts({"EXPIRE", "user:expire0", "0"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"HGETALL", "user:expire0"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {});

  response = CreateFromParts({"HSET", "user:expire0", "fresh", "new"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"HGETALL", "user:expire0"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {"fresh", "new"});
}

TEST_F(ModuleRuntimeTest, HSetAndHGetAllExecuteAgainstEngine) {
  std::unique_ptr<minikv::Cmd> set_insert =
      CreateFromParts({"HSET", "user:2", "name", "alice"});
  ASSERT_NE(set_insert, nullptr);
  minikv::CommandResponse response = set_insert->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  std::unique_ptr<minikv::Cmd> set_update =
      CreateFromParts({"HSET", "user:2", "name", "alice-2"});
  ASSERT_NE(set_update, nullptr);
  response = set_update->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), 0);

  std::unique_ptr<minikv::Cmd> get =
      CreateFromParts({"HGETALL", "user:2"});
  ASSERT_NE(get, nullptr);
  response = get->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {"name", "alice-2"});
}

TEST_F(ModuleRuntimeTest, StringCommandsExecuteAgainstEngine) {
  std::unique_ptr<minikv::Cmd> set =
      CreateFromParts({"SET", "str:cmd", "hello"});
  ASSERT_NE(set, nullptr);
  minikv::CommandResponse response = set->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());
  EXPECT_EQ(response.reply.string(), "OK");

  std::unique_ptr<minikv::Cmd> get = CreateFromParts({"GET", "str:cmd"});
  ASSERT_NE(get, nullptr);
  response = get->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "hello");

  std::unique_ptr<minikv::Cmd> strlen =
      CreateFromParts({"STRLEN", "str:cmd"});
  ASSERT_NE(strlen, nullptr);
  response = strlen->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 5);

  response = CreateFromParts({"SET", "str:cmd", ""})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());
  EXPECT_EQ(response.reply.string(), "OK");

  response = get->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "");

  response = strlen->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
}

TEST_F(ModuleRuntimeTest, HDelExecuteRemovesFields) {
  ASSERT_TRUE(hash_module_->PutField("user:3", "a", "1", nullptr).ok());
  ASSERT_TRUE(hash_module_->PutField("user:3", "b", "2", nullptr).ok());

  std::unique_ptr<minikv::Cmd> del =
      CreateFromParts({"HDEL", "user:3", "a", "b", "c"});
  ASSERT_NE(del, nullptr);

  minikv::CommandResponse response = del->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(hash_module_->ReadAll("user:3", &values).ok());
  EXPECT_TRUE(values.empty());
}

TEST_F(ModuleRuntimeTest, SetCommandsExecuteAgainstEngine) {
  std::unique_ptr<minikv::Cmd> sadd =
      CreateFromParts({"SADD", "set:cmd", "a", "b", "a"});
  ASSERT_NE(sadd, nullptr);
  minikv::CommandResponse response = sadd->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);

  std::unique_ptr<minikv::Cmd> scard =
      CreateFromParts({"SCARD", "set:cmd"});
  ASSERT_NE(scard, nullptr);
  response = scard->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);

  std::unique_ptr<minikv::Cmd> sismember =
      CreateFromParts({"SISMEMBER", "set:cmd", "a"});
  ASSERT_NE(sismember, nullptr);
  response = sismember->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  std::unique_ptr<minikv::Cmd> smembers =
      CreateFromParts({"SMEMBERS", "set:cmd"});
  ASSERT_NE(smembers, nullptr);
  response = smembers->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArrayUnordered(response.reply, {"a", "b"});

  std::unique_ptr<minikv::Cmd> srem =
      CreateFromParts({"SREM", "set:cmd", "a", "a", "x"});
  ASSERT_NE(srem, nullptr);
  response = srem->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  std::vector<std::string> members;
  ASSERT_TRUE(set_module_->ReadMembers("set:cmd", &members).ok());
  ExpectMembersUnordered(members, {"b"});
}

TEST_F(ModuleRuntimeTest, ZSetCommandsExecuteAgainstEngine) {
  minikv::CommandResponse response =
      CreateFromParts({"ZADD", "zset:cmd", "2", "b", "1", "a", "2", "c"})
          ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 3);

  response = CreateFromParts({"ZCARD", "zset:cmd"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 3);

  response = CreateFromParts({"ZRANGE", "zset:cmd", "0", "-1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {"a", "b", "c"});

  response = CreateFromParts({"ZCOUNT", "zset:cmd", "2", "2"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);

  response = CreateFromParts({"ZRANGEBYSCORE", "zset:cmd", "(1", "+inf"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {"b", "c"});

  response = CreateFromParts({"ZSCORE", "zset:cmd", "b"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "2");

  response = CreateFromParts({"ZRANK", "zset:cmd", "c"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);

  response = CreateFromParts({"ZINCRBY", "zset:cmd", "2", "a"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "3");

  response = CreateFromParts({"ZRANGE", "zset:cmd", "0", "-1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {"b", "c", "a"});

  response = CreateFromParts({"ZADD", "zset:lex", "0", "aa", "0", "ab", "0",
                              "ac", "0", "ad"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), 4);

  response = CreateFromParts({"ZLEXCOUNT", "zset:lex", "[ab", "(ad"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);

  response =
      CreateFromParts({"ZRANGEBYLEX", "zset:lex", "[ab", "(ad"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {"ab", "ac"});

  response = CreateFromParts({"ZREM", "zset:cmd", "b", "x"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  double score = 0;
  bool found = false;
  ASSERT_TRUE(zset_module_->Score("zset:cmd", "b", &score, &found).ok());
  EXPECT_FALSE(found);
}

TEST_F(ModuleRuntimeTest, StreamCommandsExecuteAgainstEngine) {
  minikv::CommandResponse response =
      CreateFromParts({"XADD", "stream:cmd", "1-0", "name", "alice", "city",
                       "shanghai"})
          ->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "1-0");

  response =
      CreateFromParts({"XADD", "stream:cmd", "1-1", "name", "bob"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "1-1");

  response = CreateFromParts({"XLEN", "stream:cmd"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);

  response = CreateFromParts({"XRANGE", "stream:cmd", "-", "+"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 2U);
  ExpectStreamEntryReply(response.reply.array()[0], "1-0",
                         {{"name", "alice"}, {"city", "shanghai"}});
  ExpectStreamEntryReply(response.reply.array()[1], "1-1", {{"name", "bob"}});

  response =
      CreateFromParts({"XREVRANGE", "stream:cmd", "+", "-"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 2U);
  ExpectStreamEntryReply(response.reply.array()[0], "1-1", {{"name", "bob"}});
  ExpectStreamEntryReply(response.reply.array()[1], "1-0",
                         {{"name", "alice"}, {"city", "shanghai"}});

  response =
      CreateFromParts({"XREAD", "STREAMS", "stream:cmd", "0-0"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 1U);
  ASSERT_TRUE(response.reply.array()[0].IsArray());
  ASSERT_EQ(response.reply.array()[0].array().size(), 2U);
  ExpectBulkString(response.reply.array()[0].array()[0], "stream:cmd");
  ASSERT_TRUE(response.reply.array()[0].array()[1].IsArray());
  ASSERT_EQ(response.reply.array()[0].array()[1].array().size(), 2U);
  ExpectStreamEntryReply(response.reply.array()[0].array()[1].array()[0], "1-0",
                         {{"name", "alice"}, {"city", "shanghai"}});
  ExpectStreamEntryReply(response.reply.array()[0].array()[1].array()[1], "1-1",
                         {{"name", "bob"}});

  response = CreateFromParts({"XDEL", "stream:cmd", "1-0", "9-0"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response =
      CreateFromParts({"XTRIM", "stream:cmd", "MAXLEN", "0"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);
}

TEST_F(ModuleRuntimeTest, ListCommandsExecuteAgainstEngine) {
  std::unique_ptr<minikv::Cmd> rpush =
      CreateFromParts({"RPUSH", "list:cmd", "a", "b", "c"});
  ASSERT_NE(rpush, nullptr);
  minikv::CommandResponse response = rpush->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 3);

  std::unique_ptr<minikv::Cmd> lpush =
      CreateFromParts({"LPUSH", "list:cmd", "z"});
  ASSERT_NE(lpush, nullptr);
  response = lpush->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 4);

  response = CreateFromParts({"LLEN", "list:cmd"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 4);

  response = CreateFromParts({"LRANGE", "list:cmd", "0", "-1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {"z", "a", "b", "c"});

  response = CreateFromParts({"LREM", "list:cmd", "1", "b"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"LTRIM", "list:cmd", "0", "1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());
  EXPECT_EQ(response.reply.string(), "OK");

  response = CreateFromParts({"LRANGE", "list:cmd", "0", "-1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {"z", "a"});
}

TEST_F(ModuleRuntimeTest, ListPopCommandsMatchExpectedSideEffects) {
  ASSERT_TRUE(list_module_->PushRight("list:pop", {"a", "b"}, nullptr).ok());

  minikv::CommandResponse response =
      CreateFromParts({"LPOP", "list:pop"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "a");

  response = CreateFromParts({"RPOP", "list:pop"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "b");

  response = CreateFromParts({"LPOP", "list:pop"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(response.reply.IsNull());

  response = CreateFromParts({"RPOP", "list:pop"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(response.reply.IsNull());
}

TEST_F(ModuleRuntimeTest, RandomSetCommandsMatchExpectedSideEffects) {
  ASSERT_TRUE(set_module_->AddMembers("set:rand", {"a", "b", "c"}, nullptr).ok());

  std::unique_ptr<minikv::Cmd> srandmember =
      CreateFromParts({"SRANDMEMBER", "set:rand"});
  ASSERT_NE(srandmember, nullptr);
  minikv::CommandResponse response = srandmember->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsBulkString());
  EXPECT_TRUE(response.reply.string() == "a" || response.reply.string() == "b" ||
              response.reply.string() == "c");

  uint64_t size = 0;
  ASSERT_TRUE(set_module_->Cardinality("set:rand", &size).ok());
  EXPECT_EQ(size, 3U);

  std::unique_ptr<minikv::Cmd> spop = CreateFromParts({"SPOP", "set:rand"});
  ASSERT_NE(spop, nullptr);
  response = spop->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsBulkString());
  const std::string popped = response.reply.string();
  EXPECT_TRUE(popped == "a" || popped == "b" || popped == "c");

  ASSERT_TRUE(set_module_->Cardinality("set:rand", &size).ok());
  EXPECT_EQ(size, 2U);

  bool found = true;
  ASSERT_TRUE(set_module_->IsMember("set:rand", popped, &found).ok());
  EXPECT_FALSE(found);
}

TEST_F(ModuleRuntimeTest, SetCommandsOnMissingKeyReturnEmptySuccess) {
  minikv::CommandResponse response =
      CreateFromParts({"SCARD", "missing-set"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"SMEMBERS", "missing-set"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {});

  response = CreateFromParts({"SISMEMBER", "missing-set", "a"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"SREM", "missing-set", "a"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"SRANDMEMBER", "missing-set"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(response.reply.IsNull());

  response = CreateFromParts({"SPOP", "missing-set"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(response.reply.IsNull());
}

TEST_F(ModuleRuntimeTest, StreamCommandsOnMissingKeyReturnEmptySuccess) {
  minikv::CommandResponse response =
      CreateFromParts({"XLEN", "missing-stream"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response =
      CreateFromParts({"XRANGE", "missing-stream", "-", "+"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  EXPECT_TRUE(response.reply.array().empty());

  response =
      CreateFromParts({"XREVRANGE", "missing-stream", "+", "-"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  EXPECT_TRUE(response.reply.array().empty());

  response = CreateFromParts({"XDEL", "missing-stream", "1-0"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response =
      CreateFromParts({"XTRIM", "missing-stream", "MAXLEN", "1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts(
                 {"XREAD", "STREAMS", "missing-stream", "0-0"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(response.reply.IsNull());
}

TEST_F(ModuleRuntimeTest, StringCommandsOnMissingKeyReturnEmptySuccess) {
  minikv::CommandResponse response =
      CreateFromParts({"GET", "missing-string"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(response.reply.IsNull());

  response = CreateFromParts({"STRLEN", "missing-string"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
}

TEST_F(ModuleRuntimeTest, ListCommandsOnMissingKeyReturnEmptySuccess) {
  minikv::CommandResponse response =
      CreateFromParts({"LLEN", "missing-list"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"LRANGE", "missing-list", "0", "-1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {});

  response = CreateFromParts({"LREM", "missing-list", "0", "a"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"LTRIM", "missing-list", "0", "1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());
  EXPECT_EQ(response.reply.string(), "OK");

  response = CreateFromParts({"LPOP", "missing-list"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(response.reply.IsNull());

  response = CreateFromParts({"RPOP", "missing-list"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(response.reply.IsNull());
}

TEST_F(ModuleRuntimeTest, TypeDelAndExpireExecuteAgainstSetKeys) {
  ASSERT_TRUE(set_module_->AddMembers("set:lifecycle", {"a", "b"}, nullptr).ok());

  minikv::CommandResponse response =
      CreateFromParts({"TYPE", "set:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "set");

  response = CreateFromParts({"EXPIRE", "set:lifecycle", "0"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"TYPE", "set:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "none");

  response = CreateFromParts({"SADD", "set:lifecycle", "fresh"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"DEL", "set:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"EXISTS", "set:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
}

TEST_F(ModuleRuntimeTest, TypeDelAndExpireExecuteAgainstStringKeys) {
  ASSERT_TRUE(string_module_->SetValue("str:lifecycle", "hello").ok());

  minikv::CommandResponse response =
      CreateFromParts({"TYPE", "str:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "string");

  response = CreateFromParts({"EXPIRE", "str:lifecycle", "0"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"TYPE", "str:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "none");

  response = CreateFromParts({"SET", "str:lifecycle", "fresh"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());
  EXPECT_EQ(response.reply.string(), "OK");

  response = CreateFromParts({"DEL", "str:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"EXISTS", "str:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
}

TEST_F(ModuleRuntimeTest, TypeDelAndExpireExecuteAgainstListKeys) {
  ASSERT_TRUE(list_module_->PushRight("list:lifecycle", {"a", "b"}, nullptr).ok());

  minikv::CommandResponse response =
      CreateFromParts({"TYPE", "list:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "list");

  response = CreateFromParts({"EXPIRE", "list:lifecycle", "0"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"TYPE", "list:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "none");

  response = CreateFromParts({"RPUSH", "list:lifecycle", "fresh"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"DEL", "list:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"EXISTS", "list:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
}

TEST_F(ModuleRuntimeTest, TypeDelAndExpireExecuteAgainstStreamKeys) {
  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:lifecycle", "1-0", {{"field", "value"}},
                             nullptr)
                  .ok());

  minikv::CommandResponse response =
      CreateFromParts({"TYPE", "stream:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "stream");

  response = CreateFromParts({"EXPIRE", "stream:lifecycle", "0"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"TYPE", "stream:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "none");

  response = CreateFromParts(
                 {"XADD", "stream:lifecycle", "2-0", "field", "fresh"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "2-0");

  response = CreateFromParts({"DEL", "stream:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"EXISTS", "stream:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
}

TEST_F(ModuleRuntimeTest, HashCommandsOnMissingKeyReturnEmptySuccess) {
  std::unique_ptr<minikv::Cmd> get =
      CreateFromParts({"HGETALL", "missing"});
  ASSERT_NE(get, nullptr);
  minikv::CommandResponse response = get->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {});

  std::unique_ptr<minikv::Cmd> del =
      CreateFromParts({"HDEL", "missing", "field"});
  ASSERT_NE(del, nullptr);
  response = del->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
}

TEST_F(ModuleRuntimeTest, TypeAndExistsOnMissingKeyReturnNoneAndZero) {
  std::unique_ptr<minikv::Cmd> type = CreateFromParts({"TYPE", "missing"});
  ASSERT_NE(type, nullptr);
  minikv::CommandResponse response = type->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "none");

  std::unique_ptr<minikv::Cmd> exists =
      CreateFromParts({"EXISTS", "missing", "missing"});
  ASSERT_NE(exists, nullptr);
  response = exists->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
}

TEST_F(ModuleRuntimeTest, StringCommandsRejectWrongTypeKeys) {
  ASSERT_TRUE(hash_module_->PutField("user:string-wrong", "name", "alice", nullptr)
                  .ok());

  minikv::CommandResponse response =
      CreateFromParts({"GET", "user:string-wrong"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response = CreateFromParts({"SET", "user:string-wrong", "value"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response = CreateFromParts({"STRLEN", "user:string-wrong"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);
}

TEST_F(ModuleRuntimeTest, StreamCommandsRejectWrongTypeKeys) {
  ASSERT_TRUE(string_module_->SetValue("stream:wrong", "hello").ok());

  minikv::CommandResponse response =
      CreateFromParts({"XLEN", "stream:wrong"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response =
      CreateFromParts({"XRANGE", "stream:wrong", "-", "+"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response = CreateFromParts(
                 {"XADD", "stream:wrong", "1-0", "field", "value"})
                 ->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response =
      CreateFromParts({"XREAD", "STREAMS", "stream:wrong", "0-0"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);
}

TEST_F(ModuleRuntimeTest, DelExecuteRemovesMultipleKeys) {
  ASSERT_TRUE(hash_module_->PutField("user:del:1", "name", "alice", nullptr).ok());
  ASSERT_TRUE(hash_module_->PutField("user:del:2", "name", "bob", nullptr).ok());

  std::unique_ptr<minikv::Cmd> del = CreateFromParts(
      {"DEL", "user:del:1", "user:del:2", "missing"});
  ASSERT_NE(del, nullptr);

  minikv::CommandResponse response = del->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);

  std::unique_ptr<minikv::Cmd> type_first =
      CreateFromParts({"TYPE", "user:del:1"});
  std::unique_ptr<minikv::Cmd> type_second =
      CreateFromParts({"TYPE", "user:del:2"});
  response = type_first->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "none");
  response = type_second->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "none");
}

TEST_F(ModuleRuntimeTest, ExistsAndDelDuplicateKeySemanticsFollowRedisStyle) {
  ASSERT_TRUE(hash_module_->PutField("user:dup", "name", "alice", nullptr).ok());

  std::unique_ptr<minikv::Cmd> exists =
      CreateFromParts({"EXISTS", "user:dup", "user:dup", "missing"});
  ASSERT_NE(exists, nullptr);
  minikv::CommandResponse response = exists->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);

  std::unique_ptr<minikv::Cmd> del =
      CreateFromParts({"DEL", "user:dup", "user:dup", "missing"});
  ASSERT_NE(del, nullptr);
  response = del->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
