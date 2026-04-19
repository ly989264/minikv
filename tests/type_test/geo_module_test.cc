#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "core/core_module.h"
#include "core/key_service.h"
#include "execution/command/cmd_create.h"
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
#include "types/geo/geo_module.h"
#include "types/zset/zset_module.h"

namespace {

class GeoModuleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-geo-module-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
                   .string();
    OpenModules();
  }

  void TearDown() override {
    module_manager_.reset();
    scheduler_.reset();
    geo_module_ = nullptr;
    zset_module_ = nullptr;
    storage_engine_.reset();
    rocksdb::Options options;
    ASSERT_TRUE(rocksdb::DestroyDB(db_path_, options).ok());
  }

  void OpenModules() {
    minikv::Config config;
    config.db_path = db_path_;
    storage_engine_ = std::make_unique<minikv::StorageEngine>();
    ASSERT_TRUE(storage_engine_->Open(config).ok());
    scheduler_ = std::make_unique<minikv::Scheduler>(1, 16);

    std::vector<std::unique_ptr<minikv::Module>> modules;
    modules.push_back(std::make_unique<minikv::CoreModule>(
        [this]() { return current_time_ms_; }));
    auto zset_module = std::make_unique<minikv::ZSetModule>();
    zset_module_ = zset_module.get();
    modules.push_back(std::move(zset_module));
    auto geo_module = std::make_unique<minikv::GeoModule>();
    geo_module_ = geo_module.get();
    modules.push_back(std::move(geo_module));

    module_manager_ = std::make_unique<minikv::ModuleManager>(
        storage_engine_.get(), scheduler_.get(), std::move(modules));
    ASSERT_TRUE(module_manager_->Initialize().ok());
  }

  void CloseModules() {
    module_manager_.reset();
    scheduler_.reset();
    geo_module_ = nullptr;
    zset_module_ = nullptr;
    storage_engine_.reset();
  }

  void ReopenModules() {
    CloseModules();
    OpenModules();
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

  minikv::KeyLookup LookupKey(const std::string& key) const {
    minikv::DefaultCoreKeyService key_service(
        [this]() { return current_time_ms_; });
    minikv::ModuleSnapshotService snapshots(minikv::ModuleNamespace("core"),
                                            storage_engine_.get());
    std::unique_ptr<minikv::ModuleSnapshot> snapshot = snapshots.Create();
    minikv::KeyLookup lookup;
    EXPECT_TRUE(key_service.Lookup(snapshot.get(), key, &lookup).ok());
    return lookup;
  }

  static inline int counter_ = 0;
  std::string db_path_;
  uint64_t current_time_ms_ = 10'000;
  std::unique_ptr<minikv::Scheduler> scheduler_;
  std::unique_ptr<minikv::ModuleManager> module_manager_;
  std::unique_ptr<minikv::StorageEngine> storage_engine_;
  minikv::GeoModule* geo_module_ = nullptr;
  minikv::ZSetModule* zset_module_ = nullptr;
};

TEST_F(GeoModuleTest, AddRoundTripDistanceAndSearchWorkTogether) {
  uint64_t added = 0;
  ASSERT_TRUE(geo_module_
                  ->AddLocations("geo:1",
                                 {{"center", 0.0, 0.0},
                                  {"near", 0.1, 0.0},
                                  {"mid", 0.3, 0.0},
                                  {"far", 2.0, 0.0},
                                  {"near", 0.1, 0.0}},
                                 &added)
                  .ok());
  EXPECT_EQ(added, 4U);

  minikv::GeoCoordinates coordinates;
  bool found = false;
  ASSERT_TRUE(geo_module_->Position("geo:1", "near", &coordinates, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_NEAR(coordinates.longitude, 0.1, 1e-5);
  EXPECT_NEAR(coordinates.latitude, 0.0, 1e-5);

  std::string geohash;
  ASSERT_TRUE(geo_module_->Hash("geo:1", "near", &geohash, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_EQ(geohash.size(), 11U);

  double distance_km = 0;
  ASSERT_TRUE(
      geo_module_->Distance("geo:1", "center", "near", 1000.0, &distance_km,
                            &found)
          .ok());
  ASSERT_TRUE(found);
  EXPECT_NEAR(distance_km, 11.1, 0.5);

  minikv::GeoSearchQuery radius_query;
  radius_query.origin_type = minikv::GeoSearchQuery::OriginType::kFromMember;
  radius_query.from_member = "center";
  radius_query.shape_type = minikv::GeoSearchQuery::ShapeType::kByRadius;
  radius_query.radius = 50;
  radius_query.unit_to_meters = 1000;
  radius_query.sort = minikv::GeoSearchSort::kAsc;
  radius_query.count = 2;

  std::vector<minikv::GeoSearchMatch> matches;
  ASSERT_TRUE(geo_module_->Search("geo:1", radius_query, &matches).ok());
  ASSERT_EQ(matches.size(), 2U);
  EXPECT_EQ(matches[0].member, "center");
  EXPECT_EQ(matches[1].member, "near");

  minikv::GeoSearchQuery box_query;
  box_query.origin_type = minikv::GeoSearchQuery::OriginType::kFromLonLat;
  box_query.origin = {0.0, 0.0};
  box_query.shape_type = minikv::GeoSearchQuery::ShapeType::kByBox;
  box_query.box_width = 80;
  box_query.box_height = 20;
  box_query.unit_to_meters = 1000;
  box_query.sort = minikv::GeoSearchSort::kDesc;
  ASSERT_TRUE(geo_module_->Search("geo:1", box_query, &matches).ok());
  ASSERT_EQ(matches.size(), 3U);
  EXPECT_EQ(matches[0].member, "mid");
  EXPECT_EQ(matches[1].member, "near");
  EXPECT_EQ(matches[2].member, "center");
}

TEST_F(GeoModuleTest, PlainZSetKeysRejectGeoOperations) {
  ASSERT_TRUE(
      zset_module_->AddMembers("geo:plain", {{"member", 1.0}}, nullptr).ok());

  uint64_t added = 0;
  rocksdb::Status status =
      geo_module_->AddLocations("geo:plain", {{"member", 0.0, 0.0}}, &added);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);

  minikv::GeoCoordinates coordinates;
  bool found = false;
  status = geo_module_->Position("geo:plain", "member", &coordinates, &found);
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("key type mismatch"), std::string::npos);
}

TEST_F(GeoModuleTest, ZSetMutationsKeepGeoSidecarInSync) {
  ASSERT_TRUE(geo_module_
                  ->AddLocations("geo:sync", {{"a", 0.0, 0.0}, {"b", 1.0, 1.0}},
                                 nullptr)
                  .ok());

  minikv::GeoCoordinates before_a;
  minikv::GeoCoordinates before_b;
  bool found = false;
  ASSERT_TRUE(geo_module_->Position("geo:sync", "a", &before_a, &found).ok());
  ASSERT_TRUE(found);
  ASSERT_TRUE(geo_module_->Position("geo:sync", "b", &before_b, &found).ok());
  ASSERT_TRUE(found);

  minikv::CommandResponse response =
      CreateFromParts({"ZADD", "geo:sync", "12345", "a"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 0);

  minikv::GeoCoordinates after_a;
  ASSERT_TRUE(geo_module_->Position("geo:sync", "a", &after_a, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_NE(after_a.longitude, before_a.longitude);

  response = CreateFromParts({"ZINCRBY", "geo:sync", "5", "b"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsBulkString());

  minikv::GeoCoordinates after_b;
  ASSERT_TRUE(geo_module_->Position("geo:sync", "b", &after_b, &found).ok());
  ASSERT_TRUE(found);
  EXPECT_TRUE(after_b.longitude != before_b.longitude ||
              after_b.latitude != before_b.latitude);

  response = CreateFromParts({"ZREM", "geo:sync", "a"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  ASSERT_TRUE(geo_module_->Position("geo:sync", "a", &after_a, &found).ok());
  EXPECT_FALSE(found);

  response = CreateFromParts({"DEL", "geo:sync"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  minikv::GeoSearchQuery query;
  query.origin_type = minikv::GeoSearchQuery::OriginType::kFromLonLat;
  query.origin = {0.0, 0.0};
  query.shape_type = minikv::GeoSearchQuery::ShapeType::kByRadius;
  query.radius = 500;
  query.unit_to_meters = 1000;

  std::vector<minikv::GeoSearchMatch> matches;
  ASSERT_TRUE(geo_module_->Search("geo:sync", query, &matches).ok());
  EXPECT_TRUE(matches.empty());
}

TEST_F(GeoModuleTest, ExpireZeroDeletesGeoVisibility) {
  ASSERT_TRUE(
      geo_module_->AddLocations("geo:ttl", {{"member", 0.0, 0.0}}, nullptr).ok());

  minikv::CommandResponse response =
      CreateFromParts({"EXPIRE", "geo:ttl", "0"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  minikv::GeoCoordinates coordinates;
  bool found = false;
  ASSERT_TRUE(geo_module_->Position("geo:ttl", "member", &coordinates, &found)
                  .ok());
  EXPECT_FALSE(found);

  const minikv::KeyLookup lookup = LookupKey("geo:ttl");
  EXPECT_EQ(lookup.state, minikv::KeyLifecycleState::kTombstone);
}

TEST_F(GeoModuleTest, ReopenAndRecreateUseNewVersionAndIgnoreOldSidecar) {
  ASSERT_TRUE(
      geo_module_->AddLocations("geo:reopen", {{"old", 0.0, 0.0}}, nullptr).ok());
  const minikv::KeyMetadata before_delete = ReadRawMetadata("geo:reopen");

  ReopenModules();

  minikv::GeoCoordinates coordinates;
  bool found = false;
  ASSERT_TRUE(
      geo_module_->Position("geo:reopen", "old", &coordinates, &found).ok());
  ASSERT_TRUE(found);

  minikv::CommandResponse response =
      CreateFromParts({"ZREM", "geo:reopen", "old"})->Execute();
  ASSERT_TRUE(response.status.ok());
  ASSERT_TRUE(response.reply.IsInteger());
  EXPECT_EQ(response.reply.integer(), 1);

  const minikv::KeyLookup tombstone = LookupKey("geo:reopen");
  EXPECT_EQ(tombstone.state, minikv::KeyLifecycleState::kTombstone);

  uint64_t added = 0;
  ASSERT_TRUE(geo_module_
                  ->AddLocations("geo:reopen", {{"fresh", 1.0, 1.0}}, &added)
                  .ok());
  EXPECT_EQ(added, 1U);

  const minikv::KeyMetadata rebuilt = ReadRawMetadata("geo:reopen");
  EXPECT_GT(rebuilt.version, before_delete.version);
  EXPECT_EQ(rebuilt.encoding, minikv::ObjectEncoding::kZSetGeo);

  ASSERT_TRUE(
      geo_module_->Position("geo:reopen", "old", &coordinates, &found).ok());
  EXPECT_FALSE(found);
  ASSERT_TRUE(
      geo_module_->Position("geo:reopen", "fresh", &coordinates, &found).ok());
  ASSERT_TRUE(found);

  minikv::GeoSearchQuery query;
  query.origin_type = minikv::GeoSearchQuery::OriginType::kFromLonLat;
  query.origin = {1.0, 1.0};
  query.shape_type = minikv::GeoSearchQuery::ShapeType::kByRadius;
  query.radius = 10;
  query.unit_to_meters = 1000;

  std::vector<minikv::GeoSearchMatch> matches;
  ASSERT_TRUE(geo_module_->Search("geo:reopen", query, &matches).ok());
  ASSERT_EQ(matches.size(), 1U);
  EXPECT_EQ(matches[0].member, "fresh");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
