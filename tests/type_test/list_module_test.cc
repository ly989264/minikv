#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "runtime/module/module_services.h"
#include "runtime/config.h"
#include "gtest/gtest.h"
#include "execution/scheduler/scheduler.h"
#include "storage/encoding/key_codec.h"
#include "storage/engine/storage_engine.h"
#include "storage/engine/write_context.h"
#include "runtime/module/module.h"
#include "runtime/module/module_manager.h"
#include "core/core_module.h"
#include "core/key_service.h"
#include "types/list/list_module.h"
#include "rocksdb/db.h"

namespace {

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

std::string EncodeListLocalPrefix(const std::string& key, uint64_t version) {
  std::string out;
  AppendUint32(&out, static_cast<uint32_t>(key.size()));
  out.append(key);
  AppendUint64(&out, version);
  return out;
}

std::string EncodeListEntryLocalKey(const std::string& key, uint64_t version,
                                    uint64_t sequence) {
  std::string out = EncodeListLocalPrefix(key, version);
  AppendUint64(&out, sequence);
  return out;
}

std::string EncodeListStateLocalKey(const std::string& key, uint64_t version) {
  return EncodeListLocalPrefix(key, version);
}

std::string EncodeListStateValue(uint64_t head_seq, uint64_t tail_seq) {
  std::string out;
  AppendUint64(&out, head_seq);
  AppendUint64(&out, tail_seq);
  return out;
}

class ListModuleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-list-module-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
                   .string();
    OpenModule();
  }

  void TearDown() override {
    module_manager_.reset();
    scheduler_.reset();
    list_module_ = nullptr;
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
    auto list_module = std::make_unique<minikv::ListModule>();
    list_module_ = list_module.get();
    modules.push_back(std::move(list_module));
    module_manager_ = std::make_unique<minikv::ModuleManager>(
        storage_engine_.get(), scheduler_.get(), std::move(modules));
    ASSERT_TRUE(module_manager_->Initialize().ok());
  }

  void CloseModule() {
    module_manager_.reset();
    scheduler_.reset();
    list_module_ = nullptr;
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

  void PutRawListEntry(const std::string& key, uint64_t version,
                       uint64_t sequence, const std::string& element) {
    minikv::WriteContext write_context(storage_engine_.get());
    const minikv::ModuleKeyspace entries_keyspace("list", "entries");
    ASSERT_TRUE(write_context
                    .Put(minikv::StorageColumnFamily::kList,
                         entries_keyspace.EncodeKey(
                             EncodeListEntryLocalKey(key, version, sequence)),
                         element)
                    .ok());
    ASSERT_TRUE(write_context.Commit().ok());
  }

  void PutRawListState(const std::string& key, uint64_t version,
                       uint64_t head_seq, uint64_t tail_seq) {
    minikv::WriteContext write_context(storage_engine_.get());
    const minikv::ModuleKeyspace state_keyspace("list", "state");
    ASSERT_TRUE(write_context
                    .Put(minikv::StorageColumnFamily::kList,
                         state_keyspace.EncodeKey(
                             EncodeListStateLocalKey(key, version)),
                         EncodeListStateValue(head_seq, tail_seq))
                    .ok());
    ASSERT_TRUE(write_context.Commit().ok());
  }

  bool HasRawListEntry(const std::string& key, uint64_t version,
                       uint64_t sequence) const {
    std::string scratch;
    const minikv::ModuleKeyspace entries_keyspace("list", "entries");
    const rocksdb::Status status = storage_engine_->Get(
        minikv::StorageColumnFamily::kList,
        entries_keyspace.EncodeKey(
            EncodeListEntryLocalKey(key, version, sequence)),
        &scratch);
    return status.ok();
  }

  bool HasRawListState(const std::string& key, uint64_t version) const {
    std::string scratch;
    const minikv::ModuleKeyspace state_keyspace("list", "state");
    const rocksdb::Status status = storage_engine_->Get(
        minikv::StorageColumnFamily::kList,
        state_keyspace.EncodeKey(EncodeListStateLocalKey(key, version)),
        &scratch);
    return status.ok();
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
    ASSERT_TRUE(list_module_
                    ->DeleteWholeKey(snapshot.get(), write_batch.get(), key,
                                     lookup)
                    .ok());
    ASSERT_TRUE(write_batch->Commit().ok());
  }

  static inline int counter_ = 0;
  std::string db_path_;
  uint64_t current_time_ms_ = 10'000;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  minikv::ListModule* list_module_ = nullptr;
};

TEST_F(ListModuleTest, PushPopRangeAndLengthMaintainListOrder) {
  uint64_t length = 0;
  ASSERT_TRUE(list_module_->PushRight("list:1", {"a", "b"}, &length).ok());
  ASSERT_EQ(length, 2U);
  ASSERT_TRUE(list_module_->PushLeft("list:1", {"z"}, &length).ok());
  ASSERT_EQ(length, 3U);

  std::vector<std::string> values;
  ASSERT_TRUE(list_module_->ReadRange("list:1", 0, -1, &values).ok());
  EXPECT_EQ(values, (std::vector<std::string>{"z", "a", "b"}));

  ASSERT_TRUE(list_module_->ReadRange("list:1", -2, -1, &values).ok());
  EXPECT_EQ(values, (std::vector<std::string>{"a", "b"}));

  std::string element;
  bool found = false;
  ASSERT_TRUE(list_module_->PopLeft("list:1", &element, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_EQ(element, "z");

  ASSERT_TRUE(list_module_->PopRight("list:1", &element, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_EQ(element, "b");

  ASSERT_TRUE(list_module_->Length("list:1", &length).ok());
  EXPECT_EQ(length, 1U);
  ASSERT_TRUE(list_module_->ReadRange("list:1", 0, -1, &values).ok());
  EXPECT_EQ(values, (std::vector<std::string>{"a"}));
}

TEST_F(ListModuleTest, RemoveElementsHonorsPositiveNegativeAndZeroCount) {
  ASSERT_TRUE(list_module_
                  ->PushRight("list:remove", {"a", "b", "a", "c", "a"}, nullptr)
                  .ok());

  uint64_t removed = 0;
  ASSERT_TRUE(
      list_module_->RemoveElements("list:remove", 1, "a", &removed).ok());
  ASSERT_EQ(removed, 1U);

  std::vector<std::string> values;
  ASSERT_TRUE(list_module_->ReadRange("list:remove", 0, -1, &values).ok());
  EXPECT_EQ(values, (std::vector<std::string>{"b", "a", "c", "a"}));

  ASSERT_TRUE(
      list_module_->RemoveElements("list:remove", -1, "a", &removed).ok());
  ASSERT_EQ(removed, 1U);
  ASSERT_TRUE(list_module_->ReadRange("list:remove", 0, -1, &values).ok());
  EXPECT_EQ(values, (std::vector<std::string>{"b", "a", "c"}));

  ASSERT_TRUE(
      list_module_->RemoveElements("list:remove", 0, "a", &removed).ok());
  ASSERT_EQ(removed, 1U);
  ASSERT_TRUE(list_module_->ReadRange("list:remove", 0, -1, &values).ok());
  EXPECT_EQ(values, (std::vector<std::string>{"b", "c"}));
}

TEST_F(ListModuleTest, TrimHonorsPositiveAndNegativeRangesAndCanTombstone) {
  ASSERT_TRUE(list_module_
                  ->PushRight("list:trim", {"a", "b", "c", "d", "e"}, nullptr)
                  .ok());

  ASSERT_TRUE(list_module_->Trim("list:trim", 1, 3).ok());

  std::vector<std::string> values;
  ASSERT_TRUE(list_module_->ReadRange("list:trim", 0, -1, &values).ok());
  EXPECT_EQ(values, (std::vector<std::string>{"b", "c", "d"}));

  ASSERT_TRUE(list_module_->Trim("list:trim", -2, -1).ok());
  ASSERT_TRUE(list_module_->ReadRange("list:trim", 0, -1, &values).ok());
  EXPECT_EQ(values, (std::vector<std::string>{"c", "d"}));

  ASSERT_TRUE(list_module_->Trim("list:trim", 5, 9).ok());
  ASSERT_TRUE(list_module_->ReadRange("list:trim", 0, -1, &values).ok());
  ASSERT_TRUE(values.empty());

  const minikv::KeyMetadata tombstone = ReadRawMetadata("list:trim");
  EXPECT_EQ(tombstone.size, 0U);
  EXPECT_EQ(tombstone.expire_at_ms, minikv::kLogicalDeleteExpireAtMs);
}

TEST_F(ListModuleTest, MissingKeyOperationsReturnEmptySuccess) {
  uint64_t length = 42;
  ASSERT_TRUE(list_module_->Length("missing", &length).ok());
  EXPECT_EQ(length, 0U);

  std::vector<std::string> values;
  ASSERT_TRUE(list_module_->ReadRange("missing", 0, -1, &values).ok());
  ASSERT_TRUE(values.empty());

  uint64_t removed = 42;
  ASSERT_TRUE(list_module_->RemoveElements("missing", 0, "a", &removed).ok());
  EXPECT_EQ(removed, 0U);

  std::string element = "seed";
  bool found = true;
  ASSERT_TRUE(list_module_->PopLeft("missing", &element, &found).ok());
  EXPECT_FALSE(found);
  EXPECT_TRUE(element.empty());

  element = "seed";
  found = true;
  ASSERT_TRUE(list_module_->PopRight("missing", &element, &found).ok());
  EXPECT_FALSE(found);
  EXPECT_TRUE(element.empty());

  ASSERT_TRUE(list_module_->Trim("missing", 0, 1).ok());
}

TEST_F(ListModuleTest, NonListMetadataStillReturnsTypeMismatch) {
  minikv::KeyMetadata metadata;
  metadata.type = minikv::ObjectType::kString;
  metadata.encoding = minikv::ObjectEncoding::kRaw;
  metadata.version = 3;
  PutRawMetadata("list:string", metadata);

  uint64_t length = 0;
  rocksdb::Status status =
      list_module_->PushRight("list:string", {"member"}, nullptr);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  std::vector<std::string> values;
  status = list_module_->ReadRange("list:string", 0, -1, &values);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  status = list_module_->Length("list:string", &length);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  uint64_t removed = 0;
  status = list_module_->RemoveElements("list:string", 0, "member", &removed);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  std::string element;
  bool found = false;
  status = list_module_->PopLeft("list:string", &element, &found);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  status = list_module_->PopRight("list:string", &element, &found);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  status = list_module_->Trim("list:string", 0, 1);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);
}

TEST_F(ListModuleTest, ExpiredMetadataRebuildBumpsVersion) {
  constexpr uint64_t kInitialSequence = 1ull << 63;
  minikv::KeyMetadata expired;
  expired.type = minikv::ObjectType::kList;
  expired.encoding = minikv::ObjectEncoding::kListQuicklist;
  expired.version = 7;
  expired.size = 1;
  expired.expire_at_ms = current_time_ms_ - 1;
  PutRawMetadata("list:expired", expired);
  PutRawListEntry("list:expired", expired.version, kInitialSequence, "stale");
  PutRawListState("list:expired", expired.version, kInitialSequence,
                  kInitialSequence);

  std::vector<std::string> values;
  ASSERT_TRUE(list_module_->ReadRange("list:expired", 0, -1, &values).ok());
  ASSERT_TRUE(values.empty());

  const minikv::KeyLookup lookup = LookupKey("list:expired");
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kExpired);

  uint64_t length = 0;
  ASSERT_TRUE(list_module_->PushRight("list:expired", {"fresh"}, &length).ok());
  ASSERT_EQ(length, 1U);

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("list:expired");
  EXPECT_EQ(rebuilt.type, minikv::ObjectType::kList);
  EXPECT_EQ(rebuilt.encoding, minikv::ObjectEncoding::kListQuicklist);
  EXPECT_EQ(rebuilt.version, 8U);
  EXPECT_EQ(rebuilt.size, 1U);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);

  ASSERT_TRUE(list_module_->ReadRange("list:expired", 0, -1, &values).ok());
  EXPECT_EQ(values, (std::vector<std::string>{"fresh"}));
}

TEST_F(ListModuleTest, DeleteWholeKeyRemovesVisibleEntriesAndStateAndRecreateBumpsVersion) {
  constexpr uint64_t kInitialSequence = 1ull << 63;
  ASSERT_TRUE(
      list_module_->PushRight("list:tombstone", {"a", "b"}, nullptr).ok());

  const minikv::KeyMetadata before_delete = ReadRawMetadata("list:tombstone");
  DeleteWholeKey("list:tombstone");

  EXPECT_FALSE(
      HasRawListEntry("list:tombstone", before_delete.version, kInitialSequence));
  EXPECT_FALSE(HasRawListEntry("list:tombstone", before_delete.version,
                               kInitialSequence + 1));
  EXPECT_FALSE(HasRawListState("list:tombstone", before_delete.version));

  const minikv::KeyLookup lookup = LookupKey("list:tombstone");
  ASSERT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
  EXPECT_EQ(lookup.metadata.version, before_delete.version);

  ReopenModule();

  std::vector<std::string> values;
  ASSERT_TRUE(list_module_->ReadRange("list:tombstone", 0, -1, &values).ok());
  ASSERT_TRUE(values.empty());

  uint64_t length = 0;
  ASSERT_TRUE(list_module_->PushLeft("list:tombstone", {"fresh"}, &length).ok());
  ASSERT_EQ(length, 1U);

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("list:tombstone");
  EXPECT_EQ(rebuilt.version, before_delete.version + 1);
  EXPECT_EQ(rebuilt.expire_at_ms, 0U);

  ASSERT_TRUE(list_module_->ReadRange("list:tombstone", 0, -1, &values).ok());
  EXPECT_EQ(values, (std::vector<std::string>{"fresh"}));
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
