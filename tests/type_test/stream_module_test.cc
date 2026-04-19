#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "core/core_module.h"
#include "core/key_service.h"
#include "execution/scheduler/scheduler.h"
#include "gtest/gtest.h"
#include "rocksdb/db.h"
#include "runtime/config.h"
#include "runtime/module/module.h"
#include "runtime/module/module_manager.h"
#include "runtime/module/module_services.h"
#include "storage/encoding/key_codec.h"
#include "storage/engine/storage_engine.h"
#include "storage/engine/write_context.h"
#include "types/stream/stream_module.h"

namespace {

struct RawStreamId {
  uint64_t ms = 0;
  uint64_t seq = 0;
};

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

std::string EncodeStreamLocalPrefix(const std::string& key, uint64_t version) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  return out;
}

std::string EncodeStreamEntryLocalKey(const std::string& key, uint64_t version,
                                      const RawStreamId& id) {
  std::string out = EncodeStreamLocalPrefix(key, version);
  AppendUint64(&out, id.ms);
  AppendUint64(&out, id.seq);
  return out;
}

std::string EncodeStreamStateLocalKey(const std::string& key,
                                      uint64_t version) {
  return EncodeStreamLocalPrefix(key, version);
}

std::string EncodeStreamStateValue(const RawStreamId& id) {
  std::string out;
  AppendUint64(&out, id.ms);
  AppendUint64(&out, id.seq);
  return out;
}

std::string EncodeStreamEntryValue(
    const std::vector<minikv::StreamFieldValue>& values) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(values.size()));
  for (const auto& item : values) {
    AppendUint32(&out, static_cast<uint32_t>(item.field.size()));
    out.append(item.field);
    AppendUint32(&out, static_cast<uint32_t>(item.value.size()));
    out.append(item.value);
  }
  return out;
}

void ExpectEntryIds(const std::vector<minikv::StreamEntry>& actual,
                    const std::vector<std::string>& expected) {
  ASSERT_EQ(actual.size(), expected.size());
  for (size_t index = 0; index < expected.size(); ++index) {
    EXPECT_EQ(actual[index].id, expected[index]);
  }
}

void ExpectFieldValues(
    const std::vector<minikv::StreamFieldValue>& actual,
    const std::vector<minikv::StreamFieldValue>& expected) {
  ASSERT_EQ(actual.size(), expected.size());
  for (size_t index = 0; index < expected.size(); ++index) {
    EXPECT_EQ(actual[index].field, expected[index].field);
    EXPECT_EQ(actual[index].value, expected[index].value);
  }
}

class StreamModuleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-stream-module-test-" + std::to_string(::getpid()) +
                 "-" + std::to_string(counter_++)))
                   .string();
    OpenModule();
  }

  void TearDown() override {
    module_manager_.reset();
    scheduler_.reset();
    stream_module_ = nullptr;
    storage_engine_.reset();
    rocksdb::Options options;
    ASSERT_TRUE(rocksdb::DestroyDB(db_path_, options).ok());
  }

  void OpenModule() {
    minikv::Config config;
    config.db_path = db_path_;
    storage_engine_ = std::make_unique<minikv::StorageEngine>();
    ASSERT_TRUE(storage_engine_->Open(config).ok());
    scheduler_ = std::make_unique<minikv::Scheduler>(1, 16);

    std::vector<std::unique_ptr<minikv::Module>> modules;
    modules.push_back(std::make_unique<minikv::CoreModule>(
        [this]() { return current_time_ms_; }));
    auto stream_module = std::make_unique<minikv::StreamModule>();
    stream_module_ = stream_module.get();
    modules.push_back(std::move(stream_module));

    module_manager_ = std::make_unique<minikv::ModuleManager>(
        storage_engine_.get(), scheduler_.get(), std::move(modules));
    ASSERT_TRUE(module_manager_->Initialize().ok());
  }

  void CloseModule() {
    module_manager_.reset();
    scheduler_.reset();
    stream_module_ = nullptr;
    storage_engine_.reset();
  }

  void ReopenModule() {
    CloseModule();
    OpenModule();
  }

  void PutRawMetadata(const std::string& key,
                      const minikv::KeyMetadata& metadata) {
    minikv::WriteContext write_context(storage_engine_.get());
    ASSERT_TRUE(write_context
                    .Put(minikv::StorageColumnFamily::kMeta,
                         minikv::KeyCodec::EncodeMetaKey(key),
                         minikv::DefaultCoreKeyService::EncodeMetadataValue(
                             metadata))
                    .ok());
    ASSERT_TRUE(write_context.Commit().ok());
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

  void PutRawStreamEntry(const std::string& key, uint64_t version,
                         const RawStreamId& id,
                         const std::vector<minikv::StreamFieldValue>& values) {
    minikv::WriteContext write_context(storage_engine_.get());
    const minikv::ModuleKeyspace entries_keyspace("stream", "entries");
    ASSERT_TRUE(write_context
                    .Put(minikv::StorageColumnFamily::kModule,
                         entries_keyspace.EncodeKey(
                             EncodeStreamEntryLocalKey(key, version, id)),
                         EncodeStreamEntryValue(values))
                    .ok());
    ASSERT_TRUE(write_context.Commit().ok());
  }

  void PutRawStreamState(const std::string& key, uint64_t version,
                         const RawStreamId& id) {
    minikv::WriteContext write_context(storage_engine_.get());
    const minikv::ModuleKeyspace state_keyspace("stream", "state");
    ASSERT_TRUE(write_context
                    .Put(minikv::StorageColumnFamily::kModule,
                         state_keyspace.EncodeKey(
                             EncodeStreamStateLocalKey(key, version)),
                         EncodeStreamStateValue(id))
                    .ok());
    ASSERT_TRUE(write_context.Commit().ok());
  }

  bool HasModuleEntry(const minikv::ModuleKeyspace& keyspace,
                      const std::string& local_key) const {
    std::string value;
    return storage_engine_
        ->Get(minikv::StorageColumnFamily::kModule, keyspace.EncodeKey(local_key),
              &value)
        .ok();
  }

  minikv::DefaultCoreKeyService MakeKeyService() const {
    return minikv::DefaultCoreKeyService([this]() { return current_time_ms_; });
  }

  std::unique_ptr<minikv::ModuleSnapshot> CreateSnapshot() const {
    minikv::ModuleSnapshotService snapshots(minikv::ModuleNamespace("core"),
                                            storage_engine_.get());
    return snapshots.Create();
  }

  std::unique_ptr<minikv::ModuleWriteBatch> CreateWriteBatch() const {
    minikv::ModuleStorage storage(minikv::ModuleNamespace("core"),
                                  storage_engine_.get());
    return storage.CreateWriteBatch();
  }

  minikv::KeyLookup LookupKey(const std::string& key) const {
    minikv::DefaultCoreKeyService key_service = MakeKeyService();
    std::unique_ptr<minikv::ModuleSnapshot> snapshot = CreateSnapshot();
    minikv::KeyLookup lookup;
    EXPECT_TRUE(key_service.Lookup(snapshot.get(), key, &lookup).ok());
    return lookup;
  }

  void DeleteWholeKey(const std::string& key) {
    minikv::DefaultCoreKeyService key_service = MakeKeyService();
    std::unique_ptr<minikv::ModuleSnapshot> snapshot = CreateSnapshot();
    std::unique_ptr<minikv::ModuleWriteBatch> write_batch = CreateWriteBatch();
    minikv::KeyLookup lookup;
    ASSERT_TRUE(key_service.Lookup(snapshot.get(), key, &lookup).ok());
    ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kLive);
    ASSERT_TRUE(stream_module_
                    ->DeleteWholeKey(snapshot.get(), write_batch.get(), key, lookup)
                    .ok());
    ASSERT_TRUE(write_batch->Commit().ok());
  }

  static inline int counter_ = 0;
  std::string db_path_;
  uint64_t current_time_ms_ = 10'000;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  minikv::StreamModule* stream_module_ = nullptr;
};

TEST_F(StreamModuleTest, AddEntryAutoIdRangeReadAndLengthWorkTogether) {
  current_time_ms_ = 5;

  std::string added_id;
  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:1", "5-0",
                             {{"name", "alice"}, {"city", "shanghai"}},
                             &added_id)
                  .ok());
  EXPECT_EQ(added_id, "5-0");

  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:1", "*", {{"name", "bob"}}, &added_id)
                  .ok());
  EXPECT_EQ(added_id, "5-1");

  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:1", "*", {{"name", "carol"}}, &added_id)
                  .ok());
  EXPECT_EQ(added_id, "5-2");

  uint64_t length = 0;
  ASSERT_TRUE(stream_module_->Length("stream:1", &length).ok());
  EXPECT_EQ(length, 3U);

  std::vector<minikv::StreamEntry> entries;
  ASSERT_TRUE(stream_module_->Range("stream:1", "-", "+", &entries).ok());
  ExpectEntryIds(entries, {"5-0", "5-1", "5-2"});
  ExpectFieldValues(entries[0].values,
                    {{"name", "alice"}, {"city", "shanghai"}});
  ExpectFieldValues(entries[1].values, {{"name", "bob"}});
  ExpectFieldValues(entries[2].values, {{"name", "carol"}});

  ASSERT_TRUE(stream_module_->ReverseRange("stream:1", "+", "-", &entries).ok());
  ExpectEntryIds(entries, {"5-2", "5-1", "5-0"});

  std::vector<minikv::StreamReadResult> results;
  ASSERT_TRUE(
      stream_module_->Read({{"stream:1", "5-0"}}, &results).ok());
  ASSERT_EQ(results.size(), 1U);
  EXPECT_EQ(results[0].key, "stream:1");
  ExpectEntryIds(results[0].entries, {"5-1", "5-2"});
}

TEST_F(StreamModuleTest, AddEntryRejectsInvalidOrNonIncreasingIds) {
  std::string added_id;
  rocksdb::Status status =
      stream_module_->AddEntry("stream:ids", "0-0", {{"field", "value"}},
                               &added_id);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("greater than 0-0"), std::string::npos);

  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:ids", "7-1", {{"field", "value"}},
                             &added_id)
                  .ok());
  EXPECT_EQ(added_id, "7-1");

  status = stream_module_->AddEntry("stream:ids", "bad-id",
                                    {{"field", "value"}}, &added_id);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("valid id"), std::string::npos);

  status = stream_module_->AddEntry("stream:ids", "7-1",
                                    {{"field", "value"}}, &added_id);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("current stream top item"),
            std::string::npos);

  status = stream_module_->AddEntry("stream:ids", "7-0",
                                    {{"field", "value"}}, &added_id);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("current stream top item"),
            std::string::npos);

  current_time_ms_ = 7;
  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:ids", "*", {{"field", "value"}},
                             &added_id)
                  .ok());
  EXPECT_EQ(added_id, "7-2");
}

TEST_F(StreamModuleTest, DeleteEntriesAndTrimUpdateMetadataAndTombstone) {
  std::string added_id;
  ASSERT_TRUE(
      stream_module_->AddEntry("stream:trim", "1-0", {{"field", "a"}}, &added_id)
          .ok());
  ASSERT_TRUE(
      stream_module_->AddEntry("stream:trim", "1-1", {{"field", "b"}}, &added_id)
          .ok());
  ASSERT_TRUE(
      stream_module_->AddEntry("stream:trim", "1-2", {{"field", "c"}}, &added_id)
          .ok());

  uint64_t removed = 0;
  ASSERT_TRUE(stream_module_
                  ->DeleteEntries("stream:trim", {"1-1", "1-1", "9-9"}, &removed)
                  .ok());
  EXPECT_EQ(removed, 1U);

  uint64_t length = 0;
  ASSERT_TRUE(stream_module_->Length("stream:trim", &length).ok());
  EXPECT_EQ(length, 2U);

  std::vector<minikv::StreamEntry> entries;
  ASSERT_TRUE(stream_module_->Range("stream:trim", "-", "+", &entries).ok());
  ExpectEntryIds(entries, {"1-0", "1-2"});

  ASSERT_TRUE(stream_module_->TrimByMaxLen("stream:trim", 1, &removed).ok());
  EXPECT_EQ(removed, 1U);
  ASSERT_TRUE(stream_module_->Range("stream:trim", "-", "+", &entries).ok());
  ExpectEntryIds(entries, {"1-2"});

  ASSERT_TRUE(stream_module_->TrimByMaxLen("stream:trim", 0, &removed).ok());
  EXPECT_EQ(removed, 1U);
  ASSERT_TRUE(stream_module_->Range("stream:trim", "-", "+", &entries).ok());
  EXPECT_TRUE(entries.empty());

  const minikv::KeyMetadata tombstone = ReadRawMetadata("stream:trim");
  EXPECT_EQ(tombstone.size, 0U);
  EXPECT_EQ(tombstone.expire_at_ms, minikv::kLogicalDeleteExpireAtMs);

  const minikv::KeyLookup lookup = LookupKey("stream:trim");
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);

  const minikv::ModuleKeyspace state_keyspace("stream", "state");
  EXPECT_FALSE(HasModuleEntry(
      state_keyspace, EncodeStreamStateLocalKey("stream:trim", lookup.metadata.version)));
}

TEST_F(StreamModuleTest, MissingKeyOperationsReturnEmptySuccess) {
  uint64_t length = 9;
  ASSERT_TRUE(stream_module_->Length("missing", &length).ok());
  EXPECT_EQ(length, 0U);

  uint64_t removed = 3;
  ASSERT_TRUE(stream_module_->DeleteEntries("missing", {"1-0"}, &removed).ok());
  EXPECT_EQ(removed, 0U);

  ASSERT_TRUE(stream_module_->TrimByMaxLen("missing", 1, &removed).ok());
  EXPECT_EQ(removed, 0U);

  std::vector<minikv::StreamEntry> entries;
  ASSERT_TRUE(stream_module_->Range("missing", "-", "+", &entries).ok());
  EXPECT_TRUE(entries.empty());

  ASSERT_TRUE(stream_module_->ReverseRange("missing", "+", "-", &entries).ok());
  EXPECT_TRUE(entries.empty());

  std::vector<minikv::StreamReadResult> results;
  ASSERT_TRUE(stream_module_->Read({{"missing", "0-0"}}, &results).ok());
  EXPECT_TRUE(results.empty());
}

TEST_F(StreamModuleTest, NonStreamMetadataStillReturnsTypeMismatch) {
  minikv::KeyMetadata metadata;
  metadata.type = minikv::ObjectType::kString;
  metadata.encoding = minikv::ObjectEncoding::kRaw;
  metadata.version = 3;
  PutRawMetadata("stream:string", metadata);

  std::string added_id;
  rocksdb::Status status =
      stream_module_->AddEntry("stream:string", "1-0", {{"field", "value"}},
                               &added_id);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  uint64_t length = 0;
  status = stream_module_->Length("stream:string", &length);
  ASSERT_TRUE(status.IsInvalidArgument());

  std::vector<minikv::StreamEntry> entries;
  status = stream_module_->Range("stream:string", "-", "+", &entries);
  ASSERT_TRUE(status.IsInvalidArgument());

  status = stream_module_->ReverseRange("stream:string", "+", "-", &entries);
  ASSERT_TRUE(status.IsInvalidArgument());

  uint64_t removed = 0;
  status = stream_module_->DeleteEntries("stream:string", {"1-0"}, &removed);
  ASSERT_TRUE(status.IsInvalidArgument());

  status = stream_module_->TrimByMaxLen("stream:string", 1, &removed);
  ASSERT_TRUE(status.IsInvalidArgument());

  std::vector<minikv::StreamReadResult> results;
  status = stream_module_->Read({{"stream:string", "0-0"}}, &results);
  ASSERT_TRUE(status.IsInvalidArgument());
}

TEST_F(StreamModuleTest, ReopenPreservesEntriesAndStateForAutoIds) {
  current_time_ms_ = 100;

  std::string added_id;
  ASSERT_TRUE(
      stream_module_->AddEntry("stream:reopen", "*", {{"field", "a"}}, &added_id)
          .ok());
  EXPECT_EQ(added_id, "100-0");
  ASSERT_TRUE(
      stream_module_->AddEntry("stream:reopen", "*", {{"field", "b"}}, &added_id)
          .ok());
  EXPECT_EQ(added_id, "100-1");

  ReopenModule();

  std::vector<minikv::StreamEntry> entries;
  ASSERT_TRUE(stream_module_->Range("stream:reopen", "-", "+", &entries).ok());
  ExpectEntryIds(entries, {"100-0", "100-1"});

  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:reopen", "*", {{"field", "c"}}, &added_id)
                  .ok());
  EXPECT_EQ(added_id, "100-2");

  ASSERT_TRUE(stream_module_->Range("stream:reopen", "-", "+", &entries).ok());
  ExpectEntryIds(entries, {"100-0", "100-1", "100-2"});
}

TEST_F(StreamModuleTest, ExpiredMetadataRebuildBumpsVersion) {
  minikv::KeyMetadata expired;
  expired.type = minikv::ObjectType::kStream;
  expired.encoding = minikv::ObjectEncoding::kStreamRadixTree;
  expired.version = 7;
  expired.size = 1;
  expired.expire_at_ms = current_time_ms_ - 1;
  PutRawMetadata("stream:expired", expired);
  PutRawStreamEntry("stream:expired", expired.version, RawStreamId{4, 0},
                    {{"field", "stale"}});
  PutRawStreamState("stream:expired", expired.version, RawStreamId{4, 0});

  std::vector<minikv::StreamEntry> entries;
  ASSERT_TRUE(stream_module_->Range("stream:expired", "-", "+", &entries).ok());
  EXPECT_TRUE(entries.empty());

  const minikv::KeyLookup lookup = LookupKey("stream:expired");
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kExpired);

  std::string added_id;
  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:expired", "*", {{"field", "fresh"}},
                             &added_id)
                  .ok());
  EXPECT_EQ(added_id, "10000-0");

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("stream:expired");
  EXPECT_EQ(rebuilt.type, minikv::ObjectType::kStream);
  EXPECT_EQ(rebuilt.encoding, minikv::ObjectEncoding::kStreamRadixTree);
  EXPECT_EQ(rebuilt.version, 8U);
  EXPECT_EQ(rebuilt.size, 1U);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);

  ASSERT_TRUE(stream_module_->Range("stream:expired", "-", "+", &entries).ok());
  ExpectEntryIds(entries, {"10000-0"});
}

TEST_F(StreamModuleTest, TombstoneSurvivesReopenAndRecreateBumpsVersion) {
  current_time_ms_ = 50;

  std::string added_id;
  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:tombstone", "*", {{"field", "a"}},
                             &added_id)
                  .ok());
  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:tombstone", "*", {{"field", "b"}},
                             &added_id)
                  .ok());

  const minikv::KeyMetadata before_delete = ReadRawMetadata("stream:tombstone");

  uint64_t removed = 0;
  ASSERT_TRUE(stream_module_
                  ->DeleteEntries("stream:tombstone", {"50-0", "50-1"}, &removed)
                  .ok());
  EXPECT_EQ(removed, 2U);

  minikv::KeyLookup lookup = LookupKey("stream:tombstone");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);

  ReopenModule();

  lookup = LookupKey("stream:tombstone");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);

  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:tombstone", "60-0", {{"field", "fresh"}},
                             &added_id)
                  .ok());
  EXPECT_EQ(added_id, "60-0");

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("stream:tombstone");
  EXPECT_EQ(rebuilt.version, before_delete.version + 1);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);
}

TEST_F(StreamModuleTest, DeleteWholeKeyRemovesEntriesAndState) {
  std::string added_id;
  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:delete", "9-0", {{"field", "a"}},
                             &added_id)
                  .ok());
  ASSERT_TRUE(stream_module_
                  ->AddEntry("stream:delete", "9-1", {{"field", "b"}},
                             &added_id)
                  .ok());

  const minikv::KeyMetadata metadata = ReadRawMetadata("stream:delete");
  DeleteWholeKey("stream:delete");

  const minikv::ModuleKeyspace entries_keyspace("stream", "entries");
  const minikv::ModuleKeyspace state_keyspace("stream", "state");
  EXPECT_FALSE(HasModuleEntry(
      entries_keyspace,
      EncodeStreamEntryLocalKey("stream:delete", metadata.version, RawStreamId{9, 0})));
  EXPECT_FALSE(HasModuleEntry(
      entries_keyspace,
      EncodeStreamEntryLocalKey("stream:delete", metadata.version, RawStreamId{9, 1})));
  EXPECT_FALSE(HasModuleEntry(
      state_keyspace,
      EncodeStreamStateLocalKey("stream:delete", metadata.version)));
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
