#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "command/cmd_create.h"
#include "config.h"
#include "gtest/gtest.h"
#include "kernel/scheduler.h"
#include "kernel/storage_engine.h"
#include "module/module.h"
#include "module/module_manager.h"
#include "modules/core/core_module.h"
#include "modules/hash/hash_module.h"
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

void ExpectBulkString(const minikv::ReplyNode& reply,
                      const std::string& value) {
  ASSERT_TRUE(reply.IsBulkString());
  EXPECT_EQ(reply.string(), value);
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
    modules.push_back(std::make_unique<minikv::CoreModule>());
    auto hash_module = std::make_unique<minikv::HashModule>();
    hash_module_ = hash_module.get();
    modules.push_back(std::move(hash_module));

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

  static inline int counter_ = 0;
  std::string db_path_;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  minikv::HashModule* hash_module_ = nullptr;
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

  const minikv::CmdRegistration* hset = registry().Find("HSET");
  ASSERT_NE(hset, nullptr);
  EXPECT_EQ(hset->name, "HSET");
  EXPECT_EQ(hset->owner_module, "hash");
  ExpectFlags(hset->flags, false, true, true, false);

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
  ExpectFlags(ping->Flags(), true, false, true, false);

  std::unique_ptr<minikv::Cmd> type;
  ASSERT_TRUE(minikv::CreateCmd(registry(), {"TYPE", "user:1"}, &type).ok());
  ASSERT_NE(type, nullptr);
  EXPECT_EQ(type->Name(), "TYPE");
  EXPECT_EQ(type->RouteKey(), "user:1");
  ExpectFlags(type->Flags(), true, false, true, false);

  std::unique_ptr<minikv::Cmd> exists;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"EXISTS", "user:1"}, &exists).ok());
  ASSERT_NE(exists, nullptr);
  EXPECT_EQ(exists->Name(), "EXISTS");
  EXPECT_EQ(exists->RouteKey(), "user:1");
  ExpectFlags(exists->Flags(), true, false, true, false);

  std::unique_ptr<minikv::Cmd> hset;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"HSET", "user:1", "name", "alice"}, &hset)
          .ok());
  ASSERT_NE(hset, nullptr);
  EXPECT_EQ(hset->Name(), "HSET");
  EXPECT_EQ(hset->RouteKey(), "user:1");
  ExpectFlags(hset->Flags(), false, true, true, false);

  std::unique_ptr<minikv::Cmd> lower;
  ASSERT_TRUE(minikv::CreateCmd(registry(), {"hgetall", "user:1"}, &lower).ok());
  ASSERT_NE(lower, nullptr);
  EXPECT_EQ(lower->Name(), "HGETALL");
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

  status = minikv::CreateCmd(registry(), {"HDEL", "user:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HDEL requires at least one field"),
            std::string::npos);

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

  status =
      minikv::CreateCmd(registry(), {"EXISTS", "user:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("EXISTS takes no extra arguments"),
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

TEST(CmdBaseTest, FailedInitClearsPreviousRouteKey) {
  TestCmd cmd;
  minikv::CmdInput input;
  input.has_key = true;
  input.key = "route:1";
  ASSERT_TRUE(cmd.Init(input).ok());
  EXPECT_EQ(cmd.RouteKey(), "route:1");

  cmd.FailInit(true);
  rocksdb::Status status = cmd.Init(input);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_TRUE(cmd.RouteKey().empty());
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
      CreateFromParts({"EXISTS", "user:type"});
  ASSERT_NE(exists, nullptr);
  response = exists->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);
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
      CreateFromParts({"EXISTS", "missing"});
  ASSERT_NE(exists, nullptr);
  response = exists->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
