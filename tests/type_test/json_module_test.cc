#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "execution/command/cmd_create.h"
#include "gtest/gtest.h"
#include "runtime/config.h"
#include "execution/scheduler/scheduler.h"
#include "runtime/module/module_manager.h"
#include "storage/encoding/key_codec.h"
#include "storage/engine/storage_engine.h"
#include "core/core_module.h"
#include "core/key_service.h"
#include "types/json/json_module.h"
#include "types/string/string_module.h"
#include "rocksdb/db.h"

namespace {

void ExpectBulkString(const minikv::ReplyNode& reply, const std::string& value) {
  ASSERT_TRUE(reply.IsBulkString());
  EXPECT_EQ(reply.string(), value);
}

class JsonModuleCommandTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-json-module-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
                   .string();
    OpenModules();
  }

  void TearDown() override {
    module_manager_.reset();
    scheduler_.reset();
    storage_engine_.reset();
    rocksdb::Options options;
    ASSERT_TRUE(rocksdb::DestroyDB(db_path_, options).ok());
  }

  void OpenModules() {
    minikv::Config config;
    config.db_path = db_path_;
    storage_engine_ = std::make_unique<minikv::StorageEngine>();
    ASSERT_TRUE(storage_engine_->Open(config).ok());
    scheduler_ = std::make_unique<minikv::Scheduler>(2, 32);

    std::vector<std::unique_ptr<minikv::Module>> modules;
    modules.push_back(std::make_unique<minikv::CoreModule>(
        [this]() { return now_ms_; }));
    modules.push_back(std::make_unique<minikv::StringModule>());
    modules.push_back(std::make_unique<minikv::JsonModule>());

    module_manager_ = std::make_unique<minikv::ModuleManager>(
        storage_engine_.get(), scheduler_.get(), std::move(modules));
    ASSERT_TRUE(module_manager_->Initialize().ok());
  }

  void ReopenModules() {
    module_manager_.reset();
    scheduler_.reset();
    storage_engine_.reset();
    OpenModules();
  }

  minikv::CommandResponse Execute(const std::vector<std::string>& parts) {
    std::unique_ptr<minikv::Cmd> cmd;
    EXPECT_TRUE(
        minikv::CreateCmd(module_manager_->command_registry(), parts, &cmd).ok());
    EXPECT_NE(cmd, nullptr);
    return cmd->Execute();
  }

  minikv::KeyMetadata ReadRawMetadata(const std::string& key) const {
    std::string raw_meta;
    EXPECT_TRUE(storage_engine_
                    ->Get(minikv::StorageColumnFamily::kMeta,
                          minikv::KeyCodec::EncodeMetaKey(key), &raw_meta)
                    .ok());
    minikv::KeyMetadata metadata;
    EXPECT_TRUE(minikv::DefaultCoreKeyService::DecodeMetadataValue(raw_meta,
                                                                   &metadata));
    return metadata;
  }

  void AdvanceTimeMs(uint64_t delta_ms) { now_ms_ += delta_ms; }

  static inline int counter_ = 0;
  std::string db_path_;
  uint64_t now_ms_ = 10'000;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
};

TEST_F(JsonModuleCommandTest, SetGetTypeAndNestedCreateWork) {
  minikv::CommandResponse response = Execute(
      {"JSON.SET", "doc:1", "$",
       "{\"name\":\"alice\",\"nested\":{\"flag\":true},\"items\":[1,2]}"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());
  EXPECT_EQ(response.reply.string(), "OK");

  response = Execute({"JSON.SET", "doc:1", "$.nested.extra", "2"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());
  EXPECT_EQ(response.reply.string(), "OK");

  response = Execute({"TYPE", "doc:1"});
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "json");

  response = Execute({"JSON.GET", "doc:1", "$.nested"});
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "[{\"flag\":true,\"extra\":2}]");

  response = Execute({"JSON.TYPE", "doc:1", "$.nested.extra"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 1U);
  ExpectBulkString(response.reply.array()[0], "integer");
}

TEST_F(JsonModuleCommandTest, WildcardRecursiveDeleteAndForgetWork) {
  minikv::CommandResponse response = Execute(
      {"JSON.SET", "doc:paths", "$",
       "{\"a\":1,\"nested\":{\"a\":2},\"arr\":[{\"a\":3},4]}"});
  ASSERT_TRUE(response.status.ok());

  response = Execute({"JSON.GET", "doc:paths", "$..a"});
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "[1,2,3]");

  response = Execute({"JSON.NUMINCRBY", "doc:paths", "$..a", "2"});
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "[3,4,5]");

  response = Execute({"JSON.DEL", "doc:paths", "$.nested.a"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = Execute({"JSON.FORGET", "doc:paths", "$.arr[0].a"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = Execute({"JSON.GET", "doc:paths", "$..a"});
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "[3]");
}

TEST_F(JsonModuleCommandTest, ClearToggleMissingPathsAndWrongTypeWork) {
  minikv::CommandResponse response = Execute(
      {"JSON.SET", "doc:clear", "$",
       "{\"flag\":true,\"n\":4,\"obj\":{\"x\":1},\"arr\":[1,2],\"text\":\"hi\"}"});
  ASSERT_TRUE(response.status.ok());

  response = Execute({"JSON.TOGGLE", "doc:clear", "$.flag"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 1U);
  ASSERT_TRUE(response.reply.array()[0].IsInteger());
  EXPECT_EQ(response.reply.array()[0].integer(), 0);

  response = Execute({"JSON.CLEAR", "doc:clear", "$.n"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = Execute({"JSON.CLEAR", "doc:clear", "$.obj"});
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), 1);

  response = Execute({"JSON.CLEAR", "doc:clear", "$.arr"});
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), 1);

  response = Execute({"JSON.GET", "doc:clear", "$"});
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(
      response.reply,
      "[{\"flag\":false,\"n\":0,\"obj\":{},\"arr\":[],\"text\":\"hi\"}]");

  response = Execute({"JSON.TOGGLE", "doc:clear", "$.text"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 1U);
  ASSERT_TRUE(response.reply.array()[0].IsNull());

  response = Execute({"JSON.SET", "doc:clear", "$.missing.leaf", "2"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsNull());

  response = Execute({"JSON.GET", "missing:json", "$"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsNull());

  response = Execute({"SET", "plain:string", "value"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());

  response = Execute({"JSON.GET", "plain:string", "$"});
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);
}

TEST_F(JsonModuleCommandTest, ReopenPreservesInteropAndDeleteFlows) {
  minikv::CommandResponse response =
      Execute({"JSON.SET", "doc:ttl", "$", "{\"v\":1}"});
  ASSERT_TRUE(response.status.ok());

  ReopenModules();

  response = Execute({"JSON.GET", "doc:ttl", "$.v"});
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "[1]");

  response = Execute({"EXPIRE", "doc:ttl", "5"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = Execute({"PTTL", "doc:ttl"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 5000);

  response = Execute({"TTL", "doc:ttl"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 5);

  response = Execute({"PERSIST", "doc:ttl"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = Execute({"TTL", "doc:ttl"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), -1);

  response = Execute({"DEL", "doc:ttl"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = Execute({"JSON.GET", "doc:ttl", "$"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsNull());
}

TEST_F(JsonModuleCommandTest, ExpireZeroAndRecreateBumpsVersion) {
  minikv::CommandResponse response =
      Execute({"JSON.SET", "doc:version", "$", "{\"v\":1}"});
  ASSERT_TRUE(response.status.ok());

  const minikv::KeyMetadata initial = ReadRawMetadata("doc:version");
  EXPECT_EQ(initial.type, minikv::ObjectType::kJson);
  EXPECT_EQ(initial.encoding, minikv::ObjectEncoding::kJsonDocument);
  EXPECT_EQ(initial.version, 1U);

  response = Execute({"EXPIRE", "doc:version", "0"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  const minikv::KeyMetadata tombstone = ReadRawMetadata("doc:version");
  EXPECT_EQ(tombstone.expire_at_ms, minikv::kLogicalDeleteExpireAtMs);
  EXPECT_EQ(tombstone.version, initial.version);

  response = Execute({"JSON.SET", "doc:version", "$", "{\"v\":2}"});
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());

  const minikv::KeyMetadata recreated = ReadRawMetadata("doc:version");
  EXPECT_EQ(recreated.type, minikv::ObjectType::kJson);
  EXPECT_EQ(recreated.encoding, minikv::ObjectEncoding::kJsonDocument);
  EXPECT_EQ(recreated.version, initial.version + 1);
  EXPECT_EQ(recreated.expire_at_ms, 0U);

  response = Execute({"JSON.GET", "doc:version", "$.v"});
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "[2]");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
