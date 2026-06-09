#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <unistd.h>
#include <vector>

#include "execution/command/cmd_create.h"
#include "runtime/config.h"
#include "gtest/gtest.h"
#include "execution/scheduler/scheduler.h"
#include "runtime/module/module_services.h"
#include "storage/engine/storage_engine.h"
#include "storage/engine/snapshot.h"
#include "storage/encoding/key_codec.h"
#include "runtime/module/module.h"
#include "runtime/module/module_manager.h"
#include "types/bitmap/bitmap_module.h"
#include "core/core_module.h"
#include "types/hash/hash_module.h"
#include "types/json/json_module.h"
#include "types/list/list_module.h"
#include "types/string/string_module.h"
#include "types/set/set_module.h"
#include "types/set/set_internal.h"
#include "types/geo/geo_module.h"
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

void ExpectBulkOrNullArray(const minikv::ReplyNode& reply,
                           const std::vector<minikv::StringValue>& values) {
  ASSERT_TRUE(reply.IsArray());
  ASSERT_EQ(reply.array().size(), values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].found) {
      ASSERT_TRUE(reply.array()[i].IsBulkString());
      EXPECT_EQ(reply.array()[i].string(), values[i].value);
    } else {
      EXPECT_TRUE(reply.array()[i].IsNull());
    }
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

void ExpectIntegerArray(const minikv::ReplyNode& reply,
                        const std::vector<long long>& values) {
  ASSERT_TRUE(reply.IsArray());
  ASSERT_EQ(reply.array().size(), values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_TRUE(reply.array()[i].IsInteger());
    EXPECT_EQ(reply.array()[i].integer(), values[i]);
  }
}

void ExpectHashPairsUnordered(
    const minikv::ReplyNode& reply,
    std::vector<minikv::FieldValue> expected) {
  ASSERT_TRUE(reply.IsArray());
  ASSERT_EQ(reply.array().size(), expected.size() * 2);

  std::vector<minikv::FieldValue> actual;
  actual.reserve(expected.size());
  for (size_t i = 0; i < reply.array().size(); i += 2) {
    ASSERT_TRUE(reply.array()[i].IsBulkString());
    ASSERT_TRUE(reply.array()[i + 1].IsBulkString());
    actual.push_back(
        minikv::FieldValue{reply.array()[i].string(),
                           reply.array()[i + 1].string()});
  }

  auto by_field = [](const minikv::FieldValue& lhs,
                     const minikv::FieldValue& rhs) {
    if (lhs.field == rhs.field) {
      return lhs.value < rhs.value;
    }
    return lhs.field < rhs.field;
  };
  std::sort(actual.begin(), actual.end(), by_field);
  std::sort(expected.begin(), expected.end(), by_field);
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(actual[i].field, expected[i].field);
    EXPECT_EQ(actual[i].value, expected[i].value);
  }
}

void ExpectBulkString(const minikv::ReplyNode& reply,
                      const std::string& value) {
  ASSERT_TRUE(reply.IsBulkString());
  EXPECT_EQ(reply.string(), value);
}

void ExpectGeoCoordinateReply(const minikv::ReplyNode& reply, double longitude,
                              double latitude, double tolerance = 1e-5) {
  ASSERT_TRUE(reply.IsArray());
  ASSERT_EQ(reply.array().size(), 2U);
  ASSERT_TRUE(reply.array()[0].IsBulkString());
  ASSERT_TRUE(reply.array()[1].IsBulkString());
  EXPECT_NEAR(std::stod(reply.array()[0].string()), longitude, tolerance);
  EXPECT_NEAR(std::stod(reply.array()[1].string()), latitude, tolerance);
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

void AppendUint32(std::string* out, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

void AppendUint64(std::string* out, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

std::string EncodeVersionedLocalPrefix(const std::string& key,
                                       uint64_t version) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  return out;
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

class BlockingWriteCmd : public minikv::Cmd {
 public:
  BlockingWriteCmd(std::string key, std::atomic<bool>* started,
                   std::atomic<bool>* release)
      : minikv::Cmd("BLOCKING_WRITE",
                    minikv::CmdFlags::kWrite | minikv::CmdFlags::kSlow),
        key_(std::move(key)),
        started_(started),
        release_(release) {}

 private:
  rocksdb::Status DoInitial(const minikv::CmdInput& input) override {
    if (input.has_key || !input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "blocking write takes no arguments");
    }
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  minikv::CommandResponse Do() override {
    started_->store(true, std::memory_order_release);
    while (!release_->load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return MakeSimpleString("OK");
  }

  std::string key_;
  std::atomic<bool>* started_ = nullptr;
  std::atomic<bool>* release_ = nullptr;
};

class ModuleRuntimeTest : public ::testing::Test {
 protected:
  virtual minikv::CoreModule::ActiveExpireOptions ActiveExpireOptionsForTest()
      const {
    return minikv::CoreModule::ActiveExpireOptions{};
  }
  virtual size_t WorkerThreadCountForTest() const { return 2; }
  virtual size_t MaxPendingRequestsPerWorkerForTest() const { return 16; }

  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-cmd-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
                   .string();
    minikv::Config config;
    config.db_path = db_path_;
    storage_engine_ = std::make_unique<minikv::StorageEngine>();
    ASSERT_TRUE(storage_engine_->Open(config).ok());
    scheduler_ = std::make_unique<minikv::Scheduler>(
        WorkerThreadCountForTest(), MaxPendingRequestsPerWorkerForTest());

    std::vector<std::unique_ptr<minikv::Module>> modules;
    modules.push_back(std::make_unique<minikv::CoreModule>(
        [this]() { return now_ms_; }, ActiveExpireOptionsForTest()));
    auto string_module = std::make_unique<minikv::StringModule>();
    string_module_ = string_module.get();
    modules.push_back(std::move(string_module));
    auto bitmap_module = std::make_unique<minikv::BitmapModule>();
    bitmap_module_ = bitmap_module.get();
    modules.push_back(std::move(bitmap_module));
    auto hash_module = std::make_unique<minikv::HashModule>();
    hash_module_ = hash_module.get();
    modules.push_back(std::move(hash_module));
    auto json_module = std::make_unique<minikv::JsonModule>();
    json_module_ = json_module.get();
    modules.push_back(std::move(json_module));
    auto list_module = std::make_unique<minikv::ListModule>();
    list_module_ = list_module.get();
    modules.push_back(std::move(list_module));
    auto set_module = std::make_unique<minikv::SetModule>();
    set_module_ = set_module.get();
    modules.push_back(std::move(set_module));
    auto zset_module = std::make_unique<minikv::ZSetModule>();
    zset_module_ = zset_module.get();
    modules.push_back(std::move(zset_module));
    auto geo_module = std::make_unique<minikv::GeoModule>();
    geo_module_ = geo_module.get();
    modules.push_back(std::move(geo_module));
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

  minikv::ModuleKeyspace ExpireIndexKeyspace() const {
    return minikv::ModuleKeyspace(minikv::StorageColumnFamily::kModule, "core",
                                  "expires");
  }

  bool HasExpireIndex(uint64_t expire_at_ms, const std::string& key) const {
    std::string value;
    const std::string local_key =
        minikv::DefaultCoreKeyService::EncodeExpireIndexKey(expire_at_ms, key);
    rocksdb::Status status = storage_engine_->Get(
        minikv::StorageColumnFamily::kModule,
        ExpireIndexKeyspace().EncodeKey(local_key), &value);
    return status.ok();
  }

  size_t ExpireIndexEntryCount() const {
    std::unique_ptr<minikv::Snapshot> snapshot =
        storage_engine_->CreateSnapshot();
    size_t count = 0;
    EXPECT_TRUE(snapshot
                    ->ScanPrefix(minikv::StorageColumnFamily::kModule,
                                 ExpireIndexKeyspace().Prefix(),
                                 [&count](const rocksdb::Slice&,
                                          const rocksdb::Slice&) {
                                   ++count;
                                   return true;
                                 })
                    .ok());
    return count;
  }

  bool TryReadRawMetadata(const std::string& key,
                          minikv::KeyMetadata* metadata) const {
    std::string raw_meta;
    rocksdb::Status status = storage_engine_->Get(
        minikv::StorageColumnFamily::kMeta,
        minikv::KeyCodec::EncodeMetaKey(key), &raw_meta);
    if (status.IsNotFound()) {
      return false;
    }
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(minikv::DefaultCoreKeyService::DecodeMetadataValue(raw_meta,
                                                                   metadata));
    return status.ok();
  }

  bool IsTombstoneMetadata(const std::string& key) const {
    minikv::KeyMetadata metadata;
    if (!TryReadRawMetadata(key, &metadata)) {
      return false;
    }
    return metadata.expire_at_ms == minikv::kLogicalDeleteExpireAtMs;
  }

  bool StorageKeyExists(minikv::StorageColumnFamily column_family,
                        const std::string& key) const {
    std::string value;
    rocksdb::Status status = storage_engine_->Get(column_family, key, &value);
    return status.ok();
  }

  size_t CountStoragePrefix(minikv::StorageColumnFamily column_family,
                            const std::string& prefix) const {
    std::unique_ptr<minikv::Snapshot> snapshot =
        storage_engine_->CreateSnapshot();
    size_t count = 0;
    EXPECT_TRUE(snapshot
                    ->ScanPrefix(column_family, prefix,
                                 [&count](const rocksdb::Slice&,
                                          const rocksdb::Slice&) {
                                   ++count;
                                   return true;
                                 })
                    .ok());
    return count;
  }

  uint64_t MetricCounter(const std::string& qualified_name) const {
    return module_manager_->GetMetricCounter(qualified_name);
  }

  void InsertExpireIndex(uint64_t expire_at_ms, const std::string& key) {
    const std::string index_key =
        minikv::DefaultCoreKeyService::EncodeExpireIndexKey(expire_at_ms, key);
    ASSERT_TRUE(storage_engine_
                    ->Put(minikv::StorageColumnFamily::kModule,
                          ExpireIndexKeyspace().EncodeKey(index_key), "")
                    .ok());
  }

  size_t TypedRowCountFor(const std::string& key,
                          const minikv::KeyMetadata& metadata) const {
    switch (metadata.type) {
      case minikv::ObjectType::kString: {
        const minikv::ModuleKeyspace data_keyspace(
            minikv::StorageColumnFamily::kString, "string", "data");
        return StorageKeyExists(minikv::StorageColumnFamily::kString,
                                data_keyspace.EncodeKey(key))
                   ? 1
                   : 0;
      }
      case minikv::ObjectType::kHash:
        return CountStoragePrefix(
            minikv::StorageColumnFamily::kHash,
            minikv::KeyCodec::EncodeHashDataPrefix(key, metadata.version));
      case minikv::ObjectType::kList: {
        const std::string local_prefix =
            EncodeVersionedLocalPrefix(key, metadata.version);
        const minikv::ModuleKeyspace entries_keyspace(
            minikv::StorageColumnFamily::kList, "list", "entries");
        const minikv::ModuleKeyspace state_keyspace(
            minikv::StorageColumnFamily::kList, "list", "state");
        return CountStoragePrefix(
                   minikv::StorageColumnFamily::kList,
                   entries_keyspace.Prefix() + local_prefix) +
               CountStoragePrefix(minikv::StorageColumnFamily::kList,
                                  state_keyspace.Prefix() + local_prefix);
      }
      case minikv::ObjectType::kSet: {
        const minikv::ModuleKeyspace members_keyspace(
            minikv::StorageColumnFamily::kSet, "set", "members");
        return CountStoragePrefix(
            minikv::StorageColumnFamily::kSet,
            members_keyspace.Prefix() +
                minikv::EncodeSetMemberPrefix(key, metadata.version));
      }
      case minikv::ObjectType::kZSet: {
        const std::string local_prefix =
            EncodeVersionedLocalPrefix(key, metadata.version);
        const minikv::ModuleKeyspace members_keyspace(
            minikv::StorageColumnFamily::kZSet, "zset", "members");
        const minikv::ModuleKeyspace score_index_keyspace(
            minikv::StorageColumnFamily::kZSet, "zset", "score_index");
        return CountStoragePrefix(
                   minikv::StorageColumnFamily::kZSet,
                   members_keyspace.Prefix() + local_prefix) +
               CountStoragePrefix(minikv::StorageColumnFamily::kZSet,
                                  score_index_keyspace.Prefix() + local_prefix);
      }
      case minikv::ObjectType::kStream: {
        const std::string local_prefix =
            EncodeVersionedLocalPrefix(key, metadata.version);
        const minikv::ModuleKeyspace entries_keyspace(
            minikv::StorageColumnFamily::kStream, "stream", "entries");
        const minikv::ModuleKeyspace state_keyspace(
            minikv::StorageColumnFamily::kStream, "stream", "state");
        return CountStoragePrefix(
                   minikv::StorageColumnFamily::kStream,
                   entries_keyspace.Prefix() + local_prefix) +
               CountStoragePrefix(minikv::StorageColumnFamily::kStream,
                                  state_keyspace.Prefix() + local_prefix);
      }
      case minikv::ObjectType::kJson: {
        const minikv::ModuleKeyspace data_keyspace(
            minikv::StorageColumnFamily::kJson, "json", "data");
        return StorageKeyExists(minikv::StorageColumnFamily::kJson,
                                data_keyspace.EncodeKey(key))
                   ? 1
                   : 0;
      }
    }
    return 0;
  }

  bool WaitUntil(const std::function<bool()>& predicate,
                 uint64_t timeout_ms = 2000) const {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
      if (predicate()) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return predicate();
  }

  static inline int counter_ = 0;
  std::string db_path_;
  uint64_t now_ms_ = 10'000;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  minikv::BitmapModule* bitmap_module_ = nullptr;
  minikv::HashModule* hash_module_ = nullptr;
  minikv::JsonModule* json_module_ = nullptr;
  minikv::StringModule* string_module_ = nullptr;
  minikv::ListModule* list_module_ = nullptr;
  minikv::SetModule* set_module_ = nullptr;
  minikv::ZSetModule* zset_module_ = nullptr;
  minikv::GeoModule* geo_module_ = nullptr;
  minikv::StreamModule* stream_module_ = nullptr;
};

class ActiveExpireRuntimeTest : public ModuleRuntimeTest {
 protected:
  minikv::CoreModule::ActiveExpireOptions ActiveExpireOptionsForTest()
      const override {
    minikv::CoreModule::ActiveExpireOptions options;
    options.enabled = true;
    options.interval_ms = 1;
    options.batch_size = 32;
    options.backfill_batch_size = 32;
    return options;
  }
};

class ActiveExpireBusyRuntimeTest : public ActiveExpireRuntimeTest {
 protected:
  size_t WorkerThreadCountForTest() const override { return 1; }
  size_t MaxPendingRequestsPerWorkerForTest() const override { return 1; }
};

TEST(CoreExpireIndexCodecTest, OrdersByExpireTimeAndDecodesBinaryKeys) {
  const std::string early =
      minikv::DefaultCoreKeyService::EncodeExpireIndexKey(100, "z");
  const std::string late =
      minikv::DefaultCoreKeyService::EncodeExpireIndexKey(200, "a");
  EXPECT_LT(early, late);

  const std::string binary_key(std::string("a\0b", 3));
  const std::string encoded =
      minikv::DefaultCoreKeyService::EncodeExpireIndexKey(12345, binary_key);
  uint64_t expire_at_ms = 0;
  std::string decoded_key;
  ASSERT_TRUE(minikv::DefaultCoreKeyService::DecodeExpireIndexKey(
      rocksdb::Slice(encoded), &expire_at_ms, &decoded_key));
  EXPECT_EQ(expire_at_ms, 12345U);
  EXPECT_EQ(decoded_key, binary_key);

  const std::string empty_key =
      minikv::DefaultCoreKeyService::EncodeExpireIndexKey(7, "");
  ASSERT_TRUE(minikv::DefaultCoreKeyService::DecodeExpireIndexKey(
      rocksdb::Slice(empty_key), &expire_at_ms, &decoded_key));
  EXPECT_EQ(expire_at_ms, 7U);
  EXPECT_TRUE(decoded_key.empty());

  EXPECT_FALSE(minikv::DefaultCoreKeyService::DecodeExpireIndexKey(
      rocksdb::Slice("short"), &expire_at_ms, &decoded_key));
}

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

  const minikv::CmdRegistration* hget = registry().Find("HGET");
  ASSERT_NE(hget, nullptr);
  EXPECT_EQ(hget->name, "HGET");
  EXPECT_EQ(hget->owner_module, "hash");
  ExpectFlags(hget->flags, true, false, true, false);

  const minikv::CmdRegistration* hmget = registry().Find("HMGET");
  ASSERT_NE(hmget, nullptr);
  EXPECT_EQ(hmget->name, "HMGET");
  EXPECT_EQ(hmget->owner_module, "hash");
  ExpectFlags(hmget->flags, true, false, true, false);

  const minikv::CmdRegistration* hlen = registry().Find("HLEN");
  ASSERT_NE(hlen, nullptr);
  EXPECT_EQ(hlen->name, "HLEN");
  EXPECT_EQ(hlen->owner_module, "hash");
  ExpectFlags(hlen->flags, true, false, true, false);

  const minikv::CmdRegistration* hexists = registry().Find("HEXISTS");
  ASSERT_NE(hexists, nullptr);
  EXPECT_EQ(hexists->name, "HEXISTS");
  EXPECT_EQ(hexists->owner_module, "hash");
  ExpectFlags(hexists->flags, true, false, true, false);

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

  const minikv::CmdRegistration* mget = registry().Find("MGET");
  ASSERT_NE(mget, nullptr);
  EXPECT_EQ(mget->name, "MGET");
  EXPECT_EQ(mget->owner_module, "string");
  ExpectFlags(mget->flags, true, false, true, false);

  const minikv::CmdRegistration* mset = registry().Find("MSET");
  ASSERT_NE(mset, nullptr);
  EXPECT_EQ(mset->name, "MSET");
  EXPECT_EQ(mset->owner_module, "string");
  ExpectFlags(mset->flags, false, true, false, true);

  const minikv::CmdRegistration* append = registry().Find("APPEND");
  ASSERT_NE(append, nullptr);
  EXPECT_EQ(append->name, "APPEND");
  EXPECT_EQ(append->owner_module, "string");
  ExpectFlags(append->flags, false, true, true, false);

  const minikv::CmdRegistration* getrange = registry().Find("GETRANGE");
  ASSERT_NE(getrange, nullptr);
  EXPECT_EQ(getrange->name, "GETRANGE");
  EXPECT_EQ(getrange->owner_module, "string");
  ExpectFlags(getrange->flags, true, false, false, true);

  const minikv::CmdRegistration* setrange = registry().Find("SETRANGE");
  ASSERT_NE(setrange, nullptr);
  EXPECT_EQ(setrange->name, "SETRANGE");
  EXPECT_EQ(setrange->owner_module, "string");
  ExpectFlags(setrange->flags, false, true, false, true);

  const minikv::CmdRegistration* getset = registry().Find("GETSET");
  ASSERT_NE(getset, nullptr);
  EXPECT_EQ(getset->name, "GETSET");
  EXPECT_EQ(getset->owner_module, "string");
  ExpectFlags(getset->flags, false, true, true, false);

  const minikv::CmdRegistration* incr = registry().Find("INCR");
  ASSERT_NE(incr, nullptr);
  EXPECT_EQ(incr->name, "INCR");
  EXPECT_EQ(incr->owner_module, "string");
  ExpectFlags(incr->flags, false, true, true, false);

  const minikv::CmdRegistration* decr = registry().Find("DECR");
  ASSERT_NE(decr, nullptr);
  EXPECT_EQ(decr->name, "DECR");
  EXPECT_EQ(decr->owner_module, "string");
  ExpectFlags(decr->flags, false, true, true, false);

  const minikv::CmdRegistration* incrby = registry().Find("INCRBY");
  ASSERT_NE(incrby, nullptr);
  EXPECT_EQ(incrby->name, "INCRBY");
  EXPECT_EQ(incrby->owner_module, "string");
  ExpectFlags(incrby->flags, false, true, true, false);

  const minikv::CmdRegistration* decrby = registry().Find("DECRBY");
  ASSERT_NE(decrby, nullptr);
  EXPECT_EQ(decrby->name, "DECRBY");
  EXPECT_EQ(decrby->owner_module, "string");
  ExpectFlags(decrby->flags, false, true, true, false);

  const minikv::CmdRegistration* getbit = registry().Find("GETBIT");
  ASSERT_NE(getbit, nullptr);
  EXPECT_EQ(getbit->name, "GETBIT");
  EXPECT_EQ(getbit->owner_module, "bitmap");
  ExpectFlags(getbit->flags, true, false, true, false);

  const minikv::CmdRegistration* setbit = registry().Find("SETBIT");
  ASSERT_NE(setbit, nullptr);
  EXPECT_EQ(setbit->name, "SETBIT");
  EXPECT_EQ(setbit->owner_module, "bitmap");
  ExpectFlags(setbit->flags, false, true, true, false);

  const minikv::CmdRegistration* bitcount = registry().Find("BITCOUNT");
  ASSERT_NE(bitcount, nullptr);
  EXPECT_EQ(bitcount->name, "BITCOUNT");
  EXPECT_EQ(bitcount->owner_module, "bitmap");
  ExpectFlags(bitcount->flags, true, false, false, true);

  const minikv::CmdRegistration* hgetall = registry().Find("HGETALL");
  ASSERT_NE(hgetall, nullptr);
  EXPECT_EQ(hgetall->name, "HGETALL");
  EXPECT_EQ(hgetall->owner_module, "hash");
  ExpectFlags(hgetall->flags, true, false, false, true);

  const minikv::CmdRegistration* hkeys = registry().Find("HKEYS");
  ASSERT_NE(hkeys, nullptr);
  EXPECT_EQ(hkeys->name, "HKEYS");
  EXPECT_EQ(hkeys->owner_module, "hash");
  ExpectFlags(hkeys->flags, true, false, false, true);

  const minikv::CmdRegistration* hvals = registry().Find("HVALS");
  ASSERT_NE(hvals, nullptr);
  EXPECT_EQ(hvals->name, "HVALS");
  EXPECT_EQ(hvals->owner_module, "hash");
  ExpectFlags(hvals->flags, true, false, false, true);

  const minikv::CmdRegistration* hdel = registry().Find("HDEL");
  ASSERT_NE(hdel, nullptr);
  EXPECT_EQ(hdel->name, "HDEL");
  EXPECT_EQ(hdel->owner_module, "hash");
  ExpectFlags(hdel->flags, false, true, false, true);

  const minikv::CmdRegistration* json_set = registry().Find("JSON.SET");
  ASSERT_NE(json_set, nullptr);
  EXPECT_EQ(json_set->name, "JSON.SET");
  EXPECT_EQ(json_set->owner_module, "json");
  ExpectFlags(json_set->flags, false, true, true, false);

  const minikv::CmdRegistration* json_get = registry().Find("JSON.GET");
  ASSERT_NE(json_get, nullptr);
  EXPECT_EQ(json_get->name, "JSON.GET");
  EXPECT_EQ(json_get->owner_module, "json");
  ExpectFlags(json_get->flags, true, false, true, false);

  const minikv::CmdRegistration* json_mget = registry().Find("JSON.MGET");
  ASSERT_NE(json_mget, nullptr);
  EXPECT_EQ(json_mget->name, "JSON.MGET");
  EXPECT_EQ(json_mget->owner_module, "json");
  ExpectFlags(json_mget->flags, true, false, true, false);

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

  const minikv::CmdRegistration* smismember = registry().Find("SMISMEMBER");
  ASSERT_NE(smismember, nullptr);
  EXPECT_EQ(smismember->name, "SMISMEMBER");
  EXPECT_EQ(smismember->owner_module, "set");
  ExpectFlags(smismember->flags, true, false, true, false);

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

  const minikv::CmdRegistration* smove = registry().Find("SMOVE");
  ASSERT_NE(smove, nullptr);
  EXPECT_EQ(smove->name, "SMOVE");
  EXPECT_EQ(smove->owner_module, "set");
  ExpectFlags(smove->flags, false, true, true, false);

  const minikv::CmdRegistration* sunion = registry().Find("SUNION");
  ASSERT_NE(sunion, nullptr);
  EXPECT_EQ(sunion->name, "SUNION");
  EXPECT_EQ(sunion->owner_module, "set");
  ExpectFlags(sunion->flags, true, false, false, true);

  const minikv::CmdRegistration* sinter = registry().Find("SINTER");
  ASSERT_NE(sinter, nullptr);
  EXPECT_EQ(sinter->name, "SINTER");
  EXPECT_EQ(sinter->owner_module, "set");
  ExpectFlags(sinter->flags, true, false, false, true);

  const minikv::CmdRegistration* sdiff = registry().Find("SDIFF");
  ASSERT_NE(sdiff, nullptr);
  EXPECT_EQ(sdiff->name, "SDIFF");
  EXPECT_EQ(sdiff->owner_module, "set");
  ExpectFlags(sdiff->flags, true, false, false, true);

  const minikv::CmdRegistration* sunionstore =
      registry().Find("SUNIONSTORE");
  ASSERT_NE(sunionstore, nullptr);
  EXPECT_EQ(sunionstore->name, "SUNIONSTORE");
  EXPECT_EQ(sunionstore->owner_module, "set");
  ExpectFlags(sunionstore->flags, false, true, false, true);

  const minikv::CmdRegistration* sinterstore =
      registry().Find("SINTERSTORE");
  ASSERT_NE(sinterstore, nullptr);
  EXPECT_EQ(sinterstore->name, "SINTERSTORE");
  EXPECT_EQ(sinterstore->owner_module, "set");
  ExpectFlags(sinterstore->flags, false, true, false, true);

  const minikv::CmdRegistration* sdiffstore =
      registry().Find("SDIFFSTORE");
  ASSERT_NE(sdiffstore, nullptr);
  EXPECT_EQ(sdiffstore->name, "SDIFFSTORE");
  EXPECT_EQ(sdiffstore->owner_module, "set");
  ExpectFlags(sdiffstore->flags, false, true, false, true);

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

  const minikv::CmdRegistration* geoadd = registry().Find("GEOADD");
  ASSERT_NE(geoadd, nullptr);
  EXPECT_EQ(geoadd->name, "GEOADD");
  EXPECT_EQ(geoadd->owner_module, "geo");
  ExpectFlags(geoadd->flags, false, true, true, false);

  const minikv::CmdRegistration* geopos = registry().Find("GEOPOS");
  ASSERT_NE(geopos, nullptr);
  EXPECT_EQ(geopos->name, "GEOPOS");
  EXPECT_EQ(geopos->owner_module, "geo");
  ExpectFlags(geopos->flags, true, false, true, false);

  const minikv::CmdRegistration* geohash = registry().Find("GEOHASH");
  ASSERT_NE(geohash, nullptr);
  EXPECT_EQ(geohash->name, "GEOHASH");
  EXPECT_EQ(geohash->owner_module, "geo");
  ExpectFlags(geohash->flags, true, false, true, false);

  const minikv::CmdRegistration* geodist = registry().Find("GEODIST");
  ASSERT_NE(geodist, nullptr);
  EXPECT_EQ(geodist->name, "GEODIST");
  EXPECT_EQ(geodist->owner_module, "geo");
  ExpectFlags(geodist->flags, true, false, true, false);

  const minikv::CmdRegistration* geosearch = registry().Find("GEOSEARCH");
  ASSERT_NE(geosearch, nullptr);
  EXPECT_EQ(geosearch->name, "GEOSEARCH");
  EXPECT_EQ(geosearch->owner_module, "geo");
  ExpectFlags(geosearch->flags, true, false, false, true);

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

  struct HashCreateCase {
    std::vector<std::string> parts;
    std::string name;
    bool fast;
    bool slow;
  };
  const std::vector<HashCreateCase> hash_read_cases = {
      {{"HGET", "user:1", "name"}, "HGET", true, false},
      {{"HMGET", "user:1", "name", "city"}, "HMGET", true, false},
      {{"HLEN", "user:1"}, "HLEN", true, false},
      {{"HEXISTS", "user:1", "name"}, "HEXISTS", true, false},
      {{"HKEYS", "user:1"}, "HKEYS", false, true},
      {{"HVALS", "user:1"}, "HVALS", false, true},
  };
  for (const auto& test_case : hash_read_cases) {
    std::unique_ptr<minikv::Cmd> hash_cmd;
    ASSERT_TRUE(
        minikv::CreateCmd(registry(), test_case.parts, &hash_cmd).ok());
    ASSERT_NE(hash_cmd, nullptr);
    EXPECT_EQ(hash_cmd->Name(), test_case.name);
    EXPECT_EQ(hash_cmd->RouteKey(), "user:1");
    ExpectLockPlan(hash_cmd->lock_plan(),
                   minikv::Cmd::LockPlan::Kind::kSingle, "user:1", {});
    ExpectFlags(hash_cmd->Flags(), true, false, test_case.fast,
                test_case.slow);
  }

  std::unique_ptr<minikv::Cmd> lower;
  ASSERT_TRUE(minikv::CreateCmd(registry(), {"hgetall", "user:1"}, &lower).ok());
  ASSERT_NE(lower, nullptr);
  EXPECT_EQ(lower->Name(), "HGETALL");

  std::unique_ptr<minikv::Cmd> json_set;
  ASSERT_TRUE(minikv::CreateCmd(
                  registry(),
                  {"JSON.SET", "doc:1", "$", "{\"name\":\"alice\"}"},
                  &json_set)
                  .ok());
  ASSERT_NE(json_set, nullptr);
  EXPECT_EQ(json_set->Name(), "JSON.SET");
  EXPECT_EQ(json_set->RouteKey(), "doc:1");
  ExpectLockPlan(json_set->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "doc:1", {});
  ExpectFlags(json_set->Flags(), false, true, true, false);

  std::unique_ptr<minikv::Cmd> json_get;
  ASSERT_TRUE(minikv::CreateCmd(registry(),
                                {"json.get", "doc:1", "INDENT", "  ", "$"},
                                &json_get)
                  .ok());
  ASSERT_NE(json_get, nullptr);
  EXPECT_EQ(json_get->Name(), "JSON.GET");
  EXPECT_EQ(json_get->RouteKey(), "doc:1");
  ExpectLockPlan(json_get->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "doc:1", {});
  ExpectFlags(json_get->Flags(), true, false, true, false);

  std::unique_ptr<minikv::Cmd> json_mget;
  ASSERT_TRUE(minikv::CreateCmd(
                  registry(),
                  {"JSON.MGET", "doc:3", "doc:1", "doc:2", "$.name"},
                  &json_mget)
                  .ok());
  ASSERT_NE(json_mget, nullptr);
  EXPECT_EQ(json_mget->Name(), "JSON.MGET");
  EXPECT_TRUE(json_mget->RouteKey().empty());
  ExpectLockPlan(json_mget->lock_plan(), minikv::Cmd::LockPlan::Kind::kMulti,
                 "", {"doc:1", "doc:2", "doc:3"});
  ExpectFlags(json_mget->Flags(), true, false, true, false);

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

  std::unique_ptr<minikv::Cmd> mget;
  ASSERT_TRUE(minikv::CreateCmd(registry(),
                                {"MGET", "str:3", "str:1", "str:2", "str:1"},
                                &mget)
                  .ok());
  ASSERT_NE(mget, nullptr);
  EXPECT_EQ(mget->Name(), "MGET");
  EXPECT_TRUE(mget->RouteKey().empty());
  ExpectLockPlan(mget->lock_plan(), minikv::Cmd::LockPlan::Kind::kMulti, "",
                 {"str:1", "str:2", "str:3"});
  ExpectFlags(mget->Flags(), true, false, true, false);

  std::unique_ptr<minikv::Cmd> mset;
  ASSERT_TRUE(minikv::CreateCmd(registry(),
                                {"MSET", "str:3", "v3", "str:1", "v1",
                                 "str:2", "v2", "str:1", "last"},
                                &mset)
                  .ok());
  ASSERT_NE(mset, nullptr);
  EXPECT_EQ(mset->Name(), "MSET");
  EXPECT_TRUE(mset->RouteKey().empty());
  ExpectLockPlan(mset->lock_plan(), minikv::Cmd::LockPlan::Kind::kMulti, "",
                 {"str:1", "str:2", "str:3"});
  ExpectFlags(mset->Flags(), false, true, false, true);

  struct StringSingleKeyCreateCase {
    std::vector<std::string> parts;
    std::string name;
    bool read;
    bool write;
    bool fast;
    bool slow;
  };
  const std::vector<StringSingleKeyCreateCase> string_cases = {
      {{"APPEND", "str:1", "x"}, "APPEND", false, true, true, false},
      {{"GETRANGE", "str:1", "0", "-1"}, "GETRANGE", true, false, false,
       true},
      {{"SETRANGE", "str:1", "2", "x"}, "SETRANGE", false, true, false,
       true},
      {{"GETSET", "str:1", "x"}, "GETSET", false, true, true, false},
      {{"INCR", "str:1"}, "INCR", false, true, true, false},
      {{"DECR", "str:1"}, "DECR", false, true, true, false},
      {{"INCRBY", "str:1", "2"}, "INCRBY", false, true, true, false},
      {{"DECRBY", "str:1", "2"}, "DECRBY", false, true, true, false},
  };
  for (const auto& test_case : string_cases) {
    std::unique_ptr<minikv::Cmd> string_cmd;
    ASSERT_TRUE(minikv::CreateCmd(registry(), test_case.parts, &string_cmd)
                    .ok());
    ASSERT_NE(string_cmd, nullptr);
    EXPECT_EQ(string_cmd->Name(), test_case.name);
    EXPECT_EQ(string_cmd->RouteKey(), "str:1");
    ExpectLockPlan(string_cmd->lock_plan(),
                   minikv::Cmd::LockPlan::Kind::kSingle, "str:1", {});
    ExpectFlags(string_cmd->Flags(), test_case.read, test_case.write,
                test_case.fast, test_case.slow);
  }

  std::unique_ptr<minikv::Cmd> getbit;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"GETBIT", "str:1", "7"}, &getbit).ok());
  ASSERT_NE(getbit, nullptr);
  EXPECT_EQ(getbit->Name(), "GETBIT");
  EXPECT_EQ(getbit->RouteKey(), "str:1");
  ExpectLockPlan(getbit->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "str:1", {});
  ExpectFlags(getbit->Flags(), true, false, true, false);

  std::unique_ptr<minikv::Cmd> setbit;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"SETBIT", "str:1", "7", "1"}, &setbit)
          .ok());
  ASSERT_NE(setbit, nullptr);
  EXPECT_EQ(setbit->Name(), "SETBIT");
  EXPECT_EQ(setbit->RouteKey(), "str:1");
  ExpectLockPlan(setbit->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "str:1", {});
  ExpectFlags(setbit->Flags(), false, true, true, false);

  std::unique_ptr<minikv::Cmd> bitcount;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"BITCOUNT", "str:1"}, &bitcount).ok());
  ASSERT_NE(bitcount, nullptr);
  EXPECT_EQ(bitcount->Name(), "BITCOUNT");
  EXPECT_EQ(bitcount->RouteKey(), "str:1");
  ExpectLockPlan(bitcount->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "str:1", {});
  ExpectFlags(bitcount->Flags(), true, false, false, true);

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

  std::unique_ptr<minikv::Cmd> smismember;
  ASSERT_TRUE(minikv::CreateCmd(registry(),
                                {"SMISMEMBER", "set:1", "a", "b"},
                                &smismember)
                  .ok());
  ASSERT_NE(smismember, nullptr);
  EXPECT_EQ(smismember->Name(), "SMISMEMBER");
  ExpectLockPlan(smismember->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "set:1", {});
  ExpectFlags(smismember->Flags(), true, false, true, false);

  std::unique_ptr<minikv::Cmd> smove;
  ASSERT_TRUE(
      minikv::CreateCmd(registry(), {"SMOVE", "set:1", "set:2", "a"}, &smove)
          .ok());
  ASSERT_NE(smove, nullptr);
  EXPECT_EQ(smove->Name(), "SMOVE");
  ExpectLockPlan(smove->lock_plan(), minikv::Cmd::LockPlan::Kind::kMulti, "",
                 {"set:1", "set:2"});
  ExpectFlags(smove->Flags(), false, true, true, false);

  std::unique_ptr<minikv::Cmd> sunion;
  ASSERT_TRUE(minikv::CreateCmd(registry(),
                                {"SUNION", "set:3", "set:1", "set:2",
                                 "set:1"},
                                &sunion)
                  .ok());
  ASSERT_NE(sunion, nullptr);
  EXPECT_EQ(sunion->Name(), "SUNION");
  ExpectLockPlan(sunion->lock_plan(), minikv::Cmd::LockPlan::Kind::kMulti, "",
                 {"set:1", "set:2", "set:3"});
  ExpectFlags(sunion->Flags(), true, false, false, true);

  std::unique_ptr<minikv::Cmd> sdiffstore;
  ASSERT_TRUE(minikv::CreateCmd(registry(),
                                {"SDIFFSTORE", "set:dest", "set:3",
                                 "set:1"},
                                &sdiffstore)
                  .ok());
  ASSERT_NE(sdiffstore, nullptr);
  EXPECT_EQ(sdiffstore->Name(), "SDIFFSTORE");
  ExpectLockPlan(sdiffstore->lock_plan(),
                 minikv::Cmd::LockPlan::Kind::kMulti, "",
                 {"set:1", "set:3", "set:dest"});
  ExpectFlags(sdiffstore->Flags(), false, true, false, true);

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

  std::unique_ptr<minikv::Cmd> geoadd;
  ASSERT_TRUE(minikv::CreateCmd(
                  registry(),
                  {"GEOADD", "geo:1", "0", "0", "center", "1", "1", "edge"},
                  &geoadd)
                  .ok());
  ASSERT_NE(geoadd, nullptr);
  EXPECT_EQ(geoadd->Name(), "GEOADD");
  EXPECT_EQ(geoadd->RouteKey(), "geo:1");
  ExpectLockPlan(geoadd->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "geo:1", {});
  ExpectFlags(geoadd->Flags(), false, true, true, false);

  std::unique_ptr<minikv::Cmd> geosearch;
  ASSERT_TRUE(minikv::CreateCmd(registry(),
                                {"GEOSEARCH", "geo:1", "FROMMEMBER", "center",
                                 "BYRADIUS", "5", "km"},
                                &geosearch)
                  .ok());
  ASSERT_NE(geosearch, nullptr);
  EXPECT_EQ(geosearch->Name(), "GEOSEARCH");
  EXPECT_EQ(geosearch->RouteKey(), "geo:1");
  ExpectLockPlan(geosearch->lock_plan(), minikv::Cmd::LockPlan::Kind::kSingle,
                 "geo:1", {});
  ExpectFlags(geosearch->Flags(), true, false, false, true);

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

  status = minikv::CreateCmd(registry(), {"HSET", "user:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("field/value pairs"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"HSET", "user:1", "field"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("field/value pairs"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"HGET", "user:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HGET requires field"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"HGET", "user:1", "field", "x"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HGET requires field"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"HMGET", "user:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HMGET requires at least one field"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"HLEN", "user:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HLEN takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"HEXISTS", "user:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HEXISTS requires field"), std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"HEXISTS", "user:1", "field", "x"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HEXISTS requires field"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"HGETALL", "user:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HGETALL takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"HKEYS", "user:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HKEYS takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"HVALS", "user:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("HVALS takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"JSON.SET", "doc"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("path, value"), std::string::npos);

  status = minikv::CreateCmd(
      registry(), {"JSON.SET", "doc", "$", "{\"a\":1}", "BAD"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("NX or XX"), std::string::npos);

  status = minikv::CreateCmd(
      registry(), {"JSON.SET", "doc", "$", "{bad-json}"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("invalid JSON value"), std::string::npos);

  status = minikv::CreateCmd(
      registry(), {"JSON.GET", "doc", "INDENT"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("requires a value"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"JSON.MGET", "doc"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("one or more keys and a path"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"JSON.TYPE", "doc", "$", "x"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("at most one path"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"JSON.TOGGLE", "doc"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("requires one path"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"JSON.NUMINCRBY", "doc", "$"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("requires path and increment"),
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

  status = minikv::CreateCmd(registry(), {"MGET"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("missing key"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"MSET", "str:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key/value pairs"), std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"MSET", "str:1", "v1", "str:2"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key/value pairs"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"APPEND", "str:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("APPEND requires value"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"GETRANGE", "str:1", "0"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("GETRANGE requires start and end"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"GETRANGE", "str:1", "bad", "1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("integer start and end"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SETRANGE", "str:1", "0"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SETRANGE requires offset and value"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"SETRANGE", "str:1", "-1", "x"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("non-negative integer offset"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"GETSET", "str:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("GETSET requires value"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"INCR", "str:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("INCR takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"DECR", "str:1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("DECR takes no extra arguments"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"INCRBY", "str:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("INCRBY requires increment"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"INCRBY", "str:1", "bad"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("INCRBY requires integer increment"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"DECRBY", "str:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("DECRBY requires increment"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"DECRBY", "str:1", "bad"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("DECRBY requires integer increment"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"GETBIT", "str:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("GETBIT requires offset"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"GETBIT", "str:1", "-1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("GETBIT requires non-negative integer offset"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SETBIT", "str:1", "7"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SETBIT requires offset and bit"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SETBIT", "str:1", "bad", "1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SETBIT requires non-negative integer offset"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SETBIT", "str:1", "7", "2"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SETBIT bit must be 0 or 1"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"BITCOUNT", "str:1", "0"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("BITCOUNT takes no extra arguments"),
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

  status = minikv::CreateCmd(registry(), {"SMISMEMBER", "set:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SMISMEMBER requires at least one member"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SPOP", "set:1", "-1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SPOP requires non-negative integer count"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"SPOP", "set:1", "1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SPOP takes at most count"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"SRANDMEMBER", "set:1", "bad"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SRANDMEMBER requires integer count"),
            std::string::npos);

  status = minikv::CreateCmd(registry(),
                             {"SRANDMEMBER", "set:1", "1", "extra"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SRANDMEMBER takes at most count"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SREM", "set:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SREM requires at least one member"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SMOVE", "set:1", "set:2"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SMOVE requires destination and member"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"SUNIONSTORE", "set:dst"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("SUNIONSTORE requires at least one key"),
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

  status = minikv::CreateCmd(registry(), {"GEOADD", "geo:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("triplets"), std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"GEOADD", "geo:1", "200", "0", "x"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("valid longitude/latitude"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"GEOPOS", "geo:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("GEOPOS requires at least one member"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"GEOHASH", "geo:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("GEOHASH requires at least one member"),
            std::string::npos);

  status = minikv::CreateCmd(registry(), {"GEODIST", "geo:1", "a"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("GEODIST requires two members"),
            std::string::npos);

  status =
      minikv::CreateCmd(registry(), {"GEODIST", "geo:1", "a", "b", "bad"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("unsupported unit"), std::string::npos);

  status = minikv::CreateCmd(registry(), {"GEOSEARCH", "geo:1"}, &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("center"), std::string::npos);

  status = minikv::CreateCmd(
      registry(),
      {"GEOSEARCH", "geo:1", "FROMMEMBER", "a", "BYRADIUS", "1", "km", "COUNT",
       "1", "ANY"},
      &cmd);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("unsupported"), std::string::npos);

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

TEST_F(ModuleRuntimeTest, JsonCommandsExecuteAgainstEngine) {
  minikv::CommandResponse response =
      CreateFromParts({"JSON.SET", "doc:json", "$",
                       "{\"name\":\"alice\",\"flag\":true,\"n\":1}"})
          ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());
  EXPECT_EQ(response.reply.string(), "OK");

  response = CreateFromParts({"TYPE", "doc:json"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "json");

  response = CreateFromParts({"JSON.GET", "doc:json", "$.name"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "[\"alice\"]");

  response =
      CreateFromParts({"JSON.NUMINCRBY", "doc:json", "$.n", "2"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "[3]");

  response = CreateFromParts({"JSON.TOGGLE", "doc:json", "$.flag"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 1U);
  ASSERT_TRUE(response.reply.array()[0].IsInteger());
  EXPECT_EQ(response.reply.array()[0].integer(), 0);

  response = CreateFromParts({"JSON.MGET", "doc:json", "missing", "$.name"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 2U);
  ExpectBulkString(response.reply.array()[0], "[\"alice\"]");
  ASSERT_TRUE(response.reply.array()[1].IsNull());
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

TEST_F(ModuleRuntimeTest, ExpireIndexTracksExpirePersistReplacementAndDel) {
  minikv::CommandResponse response =
      CreateFromParts({"SET", "str:index", "value"})->Execute();
  ASSERT_TRUE(response.status.ok());

  response = CreateFromParts({"EXPIRE", "str:index", "5"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(HasExpireIndex(15'000, "str:index"));
  EXPECT_EQ(ExpireIndexEntryCount(), 1U);

  response = CreateFromParts({"PERSIST", "str:index"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_FALSE(HasExpireIndex(15'000, "str:index"));
  EXPECT_EQ(ExpireIndexEntryCount(), 0U);

  response = CreateFromParts({"EXPIRE", "str:index", "5"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(HasExpireIndex(15'000, "str:index"));

  response = CreateFromParts({"SET", "str:index", "replace"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_FALSE(HasExpireIndex(15'000, "str:index"));
  EXPECT_EQ(ExpireIndexEntryCount(), 0U);

  response = CreateFromParts({"EXPIRE", "str:index", "5"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(HasExpireIndex(15'000, "str:index"));

  response = CreateFromParts({"DEL", "str:index"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_FALSE(HasExpireIndex(15'000, "str:index"));
  EXPECT_EQ(ExpireIndexEntryCount(), 0U);
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

TEST_F(ActiveExpireRuntimeTest, ActiveExpireDeletesExpiredKeysAcrossTypes) {
  ASSERT_TRUE(CreateFromParts({"SET", "ae:string", "value"})->Execute().status.ok());
  ASSERT_TRUE(CreateFromParts({"HSET", "ae:hash", "field", "value"})
                  ->Execute()
                  .status.ok());
  ASSERT_TRUE(CreateFromParts({"JSON.SET", "ae:json", "$", "{\"v\":1}"})
                  ->Execute()
                  .status.ok());
  ASSERT_TRUE(CreateFromParts({"RPUSH", "ae:list", "value"})->Execute().status.ok());
  ASSERT_TRUE(CreateFromParts({"SADD", "ae:set", "member"})->Execute().status.ok());
  ASSERT_TRUE(CreateFromParts({"ZADD", "ae:zset", "1", "member"})
                  ->Execute()
                  .status.ok());
  ASSERT_TRUE(CreateFromParts({"XADD", "ae:stream", "1-0", "field", "value"})
                  ->Execute()
                  .status.ok());

  const std::vector<std::string> keys = {
      "ae:string", "ae:hash", "ae:json", "ae:list",
      "ae:set",    "ae:zset", "ae:stream"};
  std::vector<std::pair<std::string, minikv::KeyMetadata>> metadata_before;
  metadata_before.reserve(keys.size());
  for (const auto& key : keys) {
    minikv::KeyMetadata metadata;
    ASSERT_TRUE(TryReadRawMetadata(key, &metadata));
    EXPECT_GT(TypedRowCountFor(key, metadata), 0U);
    metadata_before.emplace_back(key, metadata);
  }
  for (const auto& key : keys) {
    minikv::CommandResponse response =
        CreateFromParts({"EXPIRE", key, "1"})->Execute();
    ASSERT_TRUE(response.status.ok());
    EXPECT_EQ(response.reply.integer(), 1);
  }

  AdvanceTimeMs(1000);
  ASSERT_TRUE(WaitUntil([&]() {
    return std::all_of(keys.begin(), keys.end(), [&](const std::string& key) {
      return IsTombstoneMetadata(key);
    });
  }));
  EXPECT_EQ(ExpireIndexEntryCount(), 0U);
  for (const auto& [key, metadata] : metadata_before) {
    EXPECT_EQ(TypedRowCountFor(key, metadata), 0U);
  }
  EXPECT_GE(MetricCounter("core.active_expire.cycles"), 1U);
  EXPECT_GE(MetricCounter("core.active_expire.candidates"), keys.size());
  EXPECT_GE(MetricCounter("core.active_expire.deleted"), keys.size());

  for (const auto& key : keys) {
    minikv::CommandResponse response = CreateFromParts({"TYPE", key})->Execute();
    ASSERT_TRUE(response.status.ok());
    ExpectBulkString(response.reply, "none");
  }
}

TEST_F(ActiveExpireRuntimeTest, ActiveExpireBackfillsExistingTtlMetadata) {
  module_manager_.reset();

  minikv::KeyMetadata metadata;
  metadata.type = minikv::ObjectType::kString;
  metadata.encoding = minikv::ObjectEncoding::kRaw;
  metadata.version = 1;
  metadata.size = 5;
  metadata.expire_at_ms = now_ms_ + 1;
  ASSERT_TRUE(storage_engine_
                  ->Put(minikv::StorageColumnFamily::kMeta,
                        minikv::KeyCodec::EncodeMetaKey("ae:backfill"),
                        minikv::DefaultCoreKeyService::EncodeMetadataValue(
                            metadata))
                  .ok());
  const minikv::ModuleKeyspace string_data(
      minikv::StorageColumnFamily::kString, "string", "data");
  ASSERT_TRUE(storage_engine_
                  ->Put(minikv::StorageColumnFamily::kString,
                        string_data.EncodeKey("ae:backfill"), "value")
                  .ok());

  std::vector<std::unique_ptr<minikv::Module>> modules;
  modules.push_back(std::make_unique<minikv::CoreModule>(
      [this]() { return now_ms_; }, ActiveExpireOptionsForTest()));
  modules.push_back(std::make_unique<minikv::StringModule>());
  module_manager_ = std::make_unique<minikv::ModuleManager>(
      storage_engine_.get(), scheduler_.get(), std::move(modules));
  ASSERT_TRUE(module_manager_->Initialize().ok());

  AdvanceTimeMs(1);
  ASSERT_TRUE(WaitUntil(
      [&]() { return IsTombstoneMetadata("ae:backfill"); }, 2000));
  EXPECT_EQ(ExpireIndexEntryCount(), 0U);
  EXPECT_EQ(TypedRowCountFor("ae:backfill", metadata), 0U);
  EXPECT_GE(MetricCounter("core.active_expire.backfilled"), 1U);
  EXPECT_GE(MetricCounter("core.active_expire.deleted"), 1U);
}

TEST_F(ActiveExpireRuntimeTest,
       ActiveExpireDeleteWaitsForSameKeySchedulerLock) {
  minikv::CommandResponse response =
      CreateFromParts({"SET", "ae:locked", "value"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"EXPIRE", "ae:locked", "1"})->Execute();
  ASSERT_TRUE(response.status.ok());

  std::atomic<bool> blocker_started{false};
  std::atomic<bool> blocker_release{false};
  std::atomic<bool> blocker_done{false};
  std::atomic<bool> blocker_ok{false};
  auto blocker = std::make_unique<BlockingWriteCmd>(
      "ae:locked", &blocker_started, &blocker_release);
  ASSERT_TRUE(blocker->Init(minikv::CmdInput{}).ok());
  ASSERT_TRUE(scheduler_
                  ->Submit(std::move(blocker),
                           [&blocker_done,
                            &blocker_ok](minikv::CommandResponse result) {
                             blocker_ok.store(result.status.ok(),
                                              std::memory_order_release);
                             blocker_done.store(true,
                                                std::memory_order_release);
                           })
                  .ok());
  ASSERT_TRUE(WaitUntil([&]() {
    return blocker_started.load(std::memory_order_acquire);
  }));

  AdvanceTimeMs(1000);
  ASSERT_TRUE(WaitUntil([&]() {
    return MetricCounter("core.active_expire.candidates") >= 1;
  }));
  EXPECT_FALSE(IsTombstoneMetadata("ae:locked"));
  EXPECT_TRUE(HasExpireIndex(11'000, "ae:locked"));

  blocker_release.store(true, std::memory_order_release);
  ASSERT_TRUE(WaitUntil([&]() {
    return blocker_done.load(std::memory_order_acquire);
  }));
  EXPECT_TRUE(blocker_ok.load(std::memory_order_acquire));
  ASSERT_TRUE(WaitUntil([&]() { return IsTombstoneMetadata("ae:locked"); }));
  EXPECT_EQ(ExpireIndexEntryCount(), 0U);
  EXPECT_GE(MetricCounter("core.active_expire.deleted"), 1U);
}

TEST_F(ActiveExpireBusyRuntimeTest,
       ActiveExpireLeavesIndexWhenSchedulerIsBusyAndRetriesLater) {
  minikv::CommandResponse response =
      CreateFromParts({"SET", "ae:busy", "value"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"EXPIRE", "ae:busy", "1"})->Execute();
  ASSERT_TRUE(response.status.ok());

  std::atomic<bool> blockers_started{false};
  std::atomic<bool> blockers_release{false};
  std::atomic<int> blockers_done{0};
  std::atomic<bool> blockers_ok{true};
  auto running_blocker = std::make_unique<BlockingWriteCmd>(
      "busy:running", &blockers_started, &blockers_release);
  ASSERT_TRUE(running_blocker->Init(minikv::CmdInput{}).ok());
  ASSERT_TRUE(scheduler_
                  ->Submit(std::move(running_blocker),
                           [&blockers_done,
                            &blockers_ok](minikv::CommandResponse result) {
                             if (!result.status.ok()) {
                               blockers_ok.store(false,
                                                 std::memory_order_release);
                             }
                             blockers_done.fetch_add(
                                 1, std::memory_order_acq_rel);
                           })
                  .ok());
  ASSERT_TRUE(WaitUntil([&]() {
    return blockers_started.load(std::memory_order_acquire);
  }));

  std::atomic<bool> queued_blocker_started{false};
  auto queued_blocker = std::make_unique<BlockingWriteCmd>(
      "busy:queued", &queued_blocker_started, &blockers_release);
  ASSERT_TRUE(queued_blocker->Init(minikv::CmdInput{}).ok());
  ASSERT_TRUE(scheduler_
                  ->Submit(std::move(queued_blocker),
                           [&blockers_done,
                            &blockers_ok](minikv::CommandResponse result) {
                             if (!result.status.ok()) {
                               blockers_ok.store(false,
                                                 std::memory_order_release);
                             }
                             blockers_done.fetch_add(
                                 1, std::memory_order_acq_rel);
                           })
                  .ok());

  AdvanceTimeMs(1000);
  ASSERT_TRUE(WaitUntil([&]() {
    return MetricCounter("core.active_expire.scheduler_busy") >= 1;
  }));
  EXPECT_TRUE(HasExpireIndex(11'000, "ae:busy"));
  EXPECT_FALSE(IsTombstoneMetadata("ae:busy"));

  blockers_release.store(true, std::memory_order_release);
  ASSERT_TRUE(WaitUntil([&]() {
    return blockers_done.load(std::memory_order_acquire) == 2;
  }));
  EXPECT_TRUE(blockers_ok.load(std::memory_order_acquire));
  ASSERT_TRUE(WaitUntil([&]() { return IsTombstoneMetadata("ae:busy"); }));
  EXPECT_EQ(ExpireIndexEntryCount(), 0U);
  EXPECT_GE(MetricCounter("core.active_expire.deleted"), 1U);
}

TEST_F(ActiveExpireRuntimeTest, StaleExpireIndexDoesNotDeleteCurrentKey) {
  minikv::CommandResponse response =
      CreateFromParts({"SET", "ae:stale", "value"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"EXPIRE", "ae:stale", "5"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"PERSIST", "ae:stale"})->Execute();
  ASSERT_TRUE(response.status.ok());

  InsertExpireIndex(now_ms_ + 1, "ae:stale");

  AdvanceTimeMs(1);
  ASSERT_TRUE(WaitUntil([&]() { return ExpireIndexEntryCount() == 0; }));
  response = CreateFromParts({"GET", "ae:stale"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "value");
  minikv::KeyMetadata metadata;
  ASSERT_TRUE(TryReadRawMetadata("ae:stale", &metadata));
  EXPECT_EQ(metadata.expire_at_ms, 0U);
  EXPECT_GE(MetricCounter("core.active_expire.stale"), 1U);
}

TEST_F(ActiveExpireRuntimeTest,
       StaleExpireIndexDoesNotDeleteExtendedOrRecreatedKeys) {
  minikv::CommandResponse response =
      CreateFromParts({"SET", "ae:extend", "value"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"EXPIRE", "ae:extend", "1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"EXPIRE", "ae:extend", "10"})->Execute();
  ASSERT_TRUE(response.status.ok());
  InsertExpireIndex(now_ms_ + 1, "ae:extend");

  response = CreateFromParts({"SET", "ae:recreate", "old"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"EXPIRE", "ae:recreate", "1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"DEL", "ae:recreate"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"SET", "ae:recreate", "new"})->Execute();
  ASSERT_TRUE(response.status.ok());
  InsertExpireIndex(now_ms_ + 1, "ae:recreate");

  response = CreateFromParts({"SET", "ae:multi-stale", "stable"})->Execute();
  ASSERT_TRUE(response.status.ok());
  InsertExpireIndex(now_ms_ + 1, "ae:multi-stale");
  InsertExpireIndex(now_ms_ + 2, "ae:multi-stale");
  InsertExpireIndex(now_ms_ + 3, "ae:multi-stale");

  AdvanceTimeMs(3);
  ASSERT_TRUE(WaitUntil([&]() {
    return !HasExpireIndex(10'001, "ae:extend") &&
           !HasExpireIndex(10'001, "ae:recreate") &&
           !HasExpireIndex(10'001, "ae:multi-stale") &&
           !HasExpireIndex(10'002, "ae:multi-stale") &&
           !HasExpireIndex(10'003, "ae:multi-stale");
  }));

  response = CreateFromParts({"GET", "ae:extend"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "value");
  EXPECT_TRUE(HasExpireIndex(20'000, "ae:extend"));

  response = CreateFromParts({"TTL", "ae:extend"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 9);

  response = CreateFromParts({"GET", "ae:recreate"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "new");

  minikv::KeyMetadata metadata;
  ASSERT_TRUE(TryReadRawMetadata("ae:recreate", &metadata));
  EXPECT_EQ(metadata.expire_at_ms, 0U);

  response = CreateFromParts({"GET", "ae:multi-stale"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "stable");

  EXPECT_GE(MetricCounter("core.active_expire.stale"), 5U);
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

TEST_F(ModuleRuntimeTest, HashReadCommandsExecuteAgainstEngine) {
  std::unique_ptr<minikv::Cmd> set_insert =
      CreateFromParts({"HSET", "user:2", "name", "alice", "city",
                       "shanghai", "name", "alice-2"});
  ASSERT_NE(set_insert, nullptr);
  minikv::CommandResponse response = set_insert->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);

  std::unique_ptr<minikv::Cmd> set_update =
      CreateFromParts({"HSET", "user:2", "city", "beijing", "age", "30"});
  ASSERT_NE(set_update, nullptr);
  response = set_update->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"HGET", "user:2", "name"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "alice-2");

  response = CreateFromParts({"HGET", "user:2", "missing"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(response.reply.IsNull());

  response = CreateFromParts({"HMGET", "user:2", "city", "missing", "name"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 3U);
  ASSERT_TRUE(response.reply.array()[0].IsBulkString());
  EXPECT_EQ(response.reply.array()[0].string(), "beijing");
  EXPECT_TRUE(response.reply.array()[1].IsNull());
  ASSERT_TRUE(response.reply.array()[2].IsBulkString());
  EXPECT_EQ(response.reply.array()[2].string(), "alice-2");

  response = CreateFromParts({"HLEN", "user:2"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 3);

  response = CreateFromParts({"HEXISTS", "user:2", "age"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"HEXISTS", "user:2", "missing"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"HKEYS", "user:2"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArrayUnordered(response.reply, {"name", "city", "age"});

  response = CreateFromParts({"HVALS", "user:2"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArrayUnordered(response.reply,
                                 {"alice-2", "beijing", "30"});

  std::unique_ptr<minikv::Cmd> get =
      CreateFromParts({"HGETALL", "user:2"});
  ASSERT_NE(get, nullptr);
  response = get->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectHashPairsUnordered(
      response.reply,
      {{"name", "alice-2"}, {"city", "beijing"}, {"age", "30"}});
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

  response = CreateFromParts({"MSET", "str:a", "one", "str:b", "two",
                              "str:a", "final"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());
  EXPECT_EQ(response.reply.string(), "OK");

  response = CreateFromParts({"MGET", "str:a", "missing", "str:b", "str:a"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkOrNullArray(response.reply,
                        {{true, "final"}, {false, ""}, {true, "two"},
                         {true, "final"}});

  response = CreateFromParts({"APPEND", "str:a", "-tail"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 10);

  response = CreateFromParts({"GETRANGE", "str:a", "0", "4"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "final");

  response = CreateFromParts({"GETRANGE", "str:a", "-4", "-1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "tail");

  response = CreateFromParts({"SETRANGE", "str:range", "2", "xy"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 4);

  response = CreateFromParts({"GET", "str:range"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, std::string("\0\0xy", 4));

  response = CreateFromParts({"GETSET", "str:a", "reset"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "final-tail");

  response = CreateFromParts({"GETSET", "str:missing-getset", "created"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsNull());

  response = CreateFromParts({"INCR", "str:int"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"INCRBY", "str:int", "41"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 42);

  response = CreateFromParts({"DECR", "str:int"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 41);

  response = CreateFromParts({"DECRBY", "str:int", "-1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 42);

  response = CreateFromParts({"SET", "str:not-int", "12x"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"INCR", "str:not-int"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("not an integer"),
            std::string::npos);

  response = CreateFromParts({"SET", "str:max",
                              std::to_string(
                                  std::numeric_limits<int64_t>::max())})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"INCR", "str:max"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("overflow"), std::string::npos);
}

TEST_F(ModuleRuntimeTest, StringReplacementClearsTtlButMutationsPreserveTtl) {
  minikv::CommandResponse response =
      CreateFromParts({"SET", "str:ttl", "value"})->Execute();
  ASSERT_TRUE(response.status.ok());

  response = CreateFromParts({"EXPIRE", "str:ttl", "5"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"APPEND", "str:ttl", "!"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"TTL", "str:ttl"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), 5);

  response = CreateFromParts({"SETRANGE", "str:ttl", "0", "V"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"TTL", "str:ttl"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), 5);

  response = CreateFromParts({"INCR", "str:ttl-int"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"EXPIRE", "str:ttl-int", "5"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"INCRBY", "str:ttl-int", "1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"TTL", "str:ttl-int"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), 5);

  response = CreateFromParts({"SET", "str:ttl", "replace"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"TTL", "str:ttl"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), -1);

  response = CreateFromParts({"EXPIRE", "str:ttl", "5"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"GETSET", "str:ttl", "getset"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"TTL", "str:ttl"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), -1);

  response = CreateFromParts({"EXPIRE", "str:ttl", "5"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"MSET", "str:ttl", "mset"})->Execute();
  ASSERT_TRUE(response.status.ok());
  response = CreateFromParts({"TTL", "str:ttl"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_EQ(response.reply.integer(), -1);
}

TEST_F(ModuleRuntimeTest, BitmapCommandsShareStringBytesWithStringCommands) {
  minikv::CommandResponse response =
      CreateFromParts({"SETBIT", "str:bitmap", "15", "1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"GETBIT", "str:bitmap", "15"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"BITCOUNT", "str:bitmap"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"GET", "str:bitmap"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsBulkString());
  ASSERT_EQ(response.reply.string().size(), 2U);
  EXPECT_EQ(response.reply.string()[0], '\0');
  EXPECT_EQ(static_cast<unsigned char>(response.reply.string()[1]), 0x01U);

  response = CreateFromParts({"STRLEN", "str:bitmap"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);

  response = CreateFromParts({"SETBIT", "str:bitmap", "15", "1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"SET", "str:bitmap", "A"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsSimpleString());
  EXPECT_EQ(response.reply.string(), "OK");

  response = CreateFromParts({"GETBIT", "str:bitmap", "1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"BITCOUNT", "str:bitmap"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);
}

TEST_F(ModuleRuntimeTest, HDelExecuteRemovesFields) {
  ASSERT_TRUE(hash_module_->PutField("user:3", "a", "1", nullptr).ok());
  ASSERT_TRUE(hash_module_->PutField("user:3", "b", "2", nullptr).ok());

  std::unique_ptr<minikv::Cmd> del =
      CreateFromParts({"HDEL", "user:3", "a", "a", "b", "b", "c"});
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

TEST_F(ModuleRuntimeTest, SetCombinationCommandsExecuteAgainstEngine) {
  ASSERT_TRUE(set_module_->AddMembers("set:a", {"a", "b", "c"}, nullptr).ok());
  ASSERT_TRUE(set_module_->AddMembers("set:b", {"b", "c", "d"}, nullptr).ok());
  ASSERT_TRUE(set_module_->AddMembers("set:c", {"c", "e"}, nullptr).ok());

  minikv::CommandResponse response =
      CreateFromParts({"SMISMEMBER", "set:a", "a", "x", "a"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectIntegerArray(response.reply, {1, 0, 1});

  response = CreateFromParts({"SUNION", "set:a", "set:b", "missing-set"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArrayUnordered(response.reply, {"a", "b", "c", "d"});

  response = CreateFromParts({"SINTER", "set:a", "set:b", "set:c"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArrayUnordered(response.reply, {"c"});

  response = CreateFromParts({"SDIFF", "set:a", "set:b"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArrayUnordered(response.reply, {"a"});

  response = CreateFromParts({"SUNIONSTORE", "set:store", "set:a", "set:c"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 4);

  std::vector<std::string> members;
  ASSERT_TRUE(set_module_->ReadMembers("set:store", &members).ok());
  ExpectMembersUnordered(members, {"a", "b", "c", "e"});

  response = CreateFromParts({"SINTERSTORE", "set:store", "set:a", "set:b"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 2);
  ASSERT_TRUE(set_module_->ReadMembers("set:store", &members).ok());
  ExpectMembersUnordered(members, {"b", "c"});

  response = CreateFromParts({"SDIFFSTORE", "set:store", "set:a", "set:a"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
  ASSERT_TRUE(set_module_->ReadMembers("set:store", &members).ok());
  EXPECT_TRUE(members.empty());

  response = CreateFromParts({"SMOVE", "set:a", "set:dst", "a"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);
  ASSERT_TRUE(set_module_->ReadMembers("set:a", &members).ok());
  ExpectMembersUnordered(members, {"b", "c"});
  ASSERT_TRUE(set_module_->ReadMembers("set:dst", &members).ok());
  ExpectMembersUnordered(members, {"a"});

  response = CreateFromParts({"SMOVE", "set:a", "set:dst", "missing"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);
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

TEST_F(ModuleRuntimeTest, GeoCommandsExecuteAgainstEngine) {
  minikv::CommandResponse response =
      CreateFromParts({"GEOADD", "geo:cmd", "0", "0", "center", "0.1", "0",
                       "near", "2", "0", "far"})
          ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 3);

  response = CreateFromParts({"GEOPOS", "geo:cmd", "center", "missing"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 2U);
  ExpectGeoCoordinateReply(response.reply.array()[0], 0.0, 0.0);
  EXPECT_TRUE(response.reply.array()[1].IsNull());

  response = CreateFromParts({"GEOHASH", "geo:cmd", "center", "missing"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 2U);
  ASSERT_TRUE(response.reply.array()[0].IsBulkString());
  EXPECT_EQ(response.reply.array()[0].string().size(), 11U);
  EXPECT_TRUE(response.reply.array()[1].IsNull());

  response =
      CreateFromParts({"GEODIST", "geo:cmd", "center", "near", "km"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsBulkString());
  EXPECT_NEAR(std::stod(response.reply.string()), 11.1, 0.5);

  response = CreateFromParts({"GEOSEARCH", "geo:cmd", "FROMMEMBER", "center",
                              "BYRADIUS", "50", "km", "ASC", "COUNT", "2",
                              "WITHDIST", "WITHHASH", "WITHCOORD"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 2U);

  const auto& first = response.reply.array()[0];
  ASSERT_TRUE(first.IsArray());
  ASSERT_EQ(first.array().size(), 4U);
  ASSERT_TRUE(first.array()[0].IsBulkString());
  EXPECT_EQ(first.array()[0].string(), "center");
  ASSERT_TRUE(first.array()[1].IsBulkString());
  EXPECT_EQ(std::stod(first.array()[1].string()), 0.0);
  ASSERT_TRUE(first.array()[2].IsInteger());
  ExpectGeoCoordinateReply(first.array()[3], 0.0, 0.0);

  const auto& second = response.reply.array()[1];
  ASSERT_TRUE(second.IsArray());
  ASSERT_EQ(second.array().size(), 4U);
  ASSERT_TRUE(second.array()[0].IsBulkString());
  EXPECT_EQ(second.array()[0].string(), "near");
  ASSERT_TRUE(second.array()[1].IsBulkString());
  EXPECT_NEAR(std::stod(second.array()[1].string()), 11.1, 0.5);
  ASSERT_TRUE(second.array()[2].IsInteger());
  ExpectGeoCoordinateReply(second.array()[3], 0.1, 0.0);
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

  ASSERT_TRUE(set_module_->AddMembers("set:rand:count", {"a", "b", "c"},
                                      nullptr)
                  .ok());
  response = CreateFromParts({"SRANDMEMBER", "set:rand:count", "2"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  EXPECT_EQ(response.reply.array().size(), 2U);

  response = CreateFromParts({"SRANDMEMBER", "set:rand:count", "-4"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  EXPECT_EQ(response.reply.array().size(), 4U);

  response = CreateFromParts({"SPOP", "set:rand:count", "2"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  EXPECT_EQ(response.reply.array().size(), 2U);
  ASSERT_TRUE(set_module_->Cardinality("set:rand:count", &size).ok());
  EXPECT_EQ(size, 1U);
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

  response = CreateFromParts({"SRANDMEMBER", "missing-set", "2"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {});

  response = CreateFromParts({"SPOP", "missing-set", "2"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {});

  response = CreateFromParts({"SMISMEMBER", "missing-set", "a", "b"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectIntegerArray(response.reply, {0, 0});

  response = CreateFromParts({"SUNION", "missing-set", "missing:set:2"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {});
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

  response =
      CreateFromParts({"MGET", "missing-string", "missing-string-2"})
          ->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkOrNullArray(response.reply, {{false, ""}, {false, ""}});

  response = CreateFromParts({"GETRANGE", "missing-string", "0", "-1"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkString(response.reply, "");

  response = CreateFromParts({"SETRANGE", "missing-string-empty", "5", ""})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"GETSET", "missing-string-getset", "created"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsNull());

  response = CreateFromParts({"INCRBY", "missing-string-int", "3"})
                 ->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 3);

  response = CreateFromParts({"GETBIT", "missing-string", "9"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"BITCOUNT", "missing-string"})->Execute();
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

  response = CreateFromParts({"SETBIT", "str:lifecycle", "15", "1"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"BITCOUNT", "str:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 21);

  response = CreateFromParts({"DEL", "str:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  response = CreateFromParts({"EXISTS", "str:lifecycle"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"GETBIT", "str:lifecycle", "15"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"BITCOUNT", "str:lifecycle"})->Execute();
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

  response = CreateFromParts({"HGET", "missing", "field"})->Execute();
  ASSERT_TRUE(response.status.ok());
  EXPECT_TRUE(response.reply.IsNull());

  response = CreateFromParts({"HMGET", "missing", "a", "b"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsArray());
  ASSERT_EQ(response.reply.array().size(), 2U);
  EXPECT_TRUE(response.reply.array()[0].IsNull());
  EXPECT_TRUE(response.reply.array()[1].IsNull());

  response = CreateFromParts({"HLEN", "missing"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"HEXISTS", "missing", "field"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  response = CreateFromParts({"HKEYS", "missing"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ExpectBulkStringArray(response.reply, {});

  response = CreateFromParts({"HVALS", "missing"})->Execute();
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

  response = CreateFromParts({"MGET", "user:string-wrong"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response =
      CreateFromParts({"MSET", "user:string-wrong", "value"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response =
      CreateFromParts({"APPEND", "user:string-wrong", "value"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response =
      CreateFromParts({"GETRANGE", "user:string-wrong", "0", "-1"})
          ->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response =
      CreateFromParts({"SETRANGE", "user:string-wrong", "0", "x"})
          ->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response =
      CreateFromParts({"GETSET", "user:string-wrong", "value"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response = CreateFromParts({"INCR", "user:string-wrong"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response = CreateFromParts({"DECRBY", "user:string-wrong", "1"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response = CreateFromParts({"GETBIT", "user:string-wrong", "0"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response =
      CreateFromParts({"SETBIT", "user:string-wrong", "0", "1"})->Execute();
  ASSERT_TRUE(response.status.IsInvalidArgument());
  EXPECT_NE(response.status.ToString().find("key type mismatch"),
            std::string::npos);

  response = CreateFromParts({"BITCOUNT", "user:string-wrong"})->Execute();
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
