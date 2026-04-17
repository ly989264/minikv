#include <filesystem>
#include <condition_variable>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <vector>

#include "gtest/gtest.h"
#include "minikv/minikv.h"
#include "rocksdb/db.h"

namespace {

class MiniKVHashTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
                   .string();
    minikv::Config config;
    config.db_path = db_path_;
    config.worker_threads = 4;
    ASSERT_TRUE(minikv::MiniKV::Open(config, &kv_).ok());
  }

  void TearDown() override {
    kv_.reset();
    rocksdb::Options options;
    ASSERT_TRUE(rocksdb::DestroyDB(db_path_, options).ok());
  }

  static inline int counter_ = 0;
  std::string db_path_;
  std::unique_ptr<minikv::MiniKV> kv_;
};

TEST_F(MiniKVHashTest, HSetAndHGetAll) {
  bool inserted = false;
  ASSERT_TRUE(kv_->HSet("user:1", "name", "alice", &inserted).ok());
  ASSERT_TRUE(inserted);
  ASSERT_TRUE(kv_->HSet("user:1", "city", "shanghai", &inserted).ok());
  ASSERT_TRUE(inserted);
  ASSERT_TRUE(kv_->HSet("user:1", "name", "alice-2", &inserted).ok());
  ASSERT_FALSE(inserted);

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(kv_->HGetAll("user:1", &values).ok());
  ASSERT_EQ(values.size(), 2U);
}

TEST_F(MiniKVHashTest, HDelRemovesFieldsAndMeta) {
  ASSERT_TRUE(kv_->HSet("user:2", "a", "1").ok());
  ASSERT_TRUE(kv_->HSet("user:2", "b", "2").ok());

  uint64_t deleted = 0;
  ASSERT_TRUE(kv_->HDel("user:2", {"a"}, &deleted).ok());
  ASSERT_EQ(deleted, 1U);

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(kv_->HGetAll("user:2", &values).ok());
  ASSERT_EQ(values.size(), 1U);

  ASSERT_TRUE(kv_->HDel("user:2", {"b"}, &deleted).ok());
  ASSERT_EQ(deleted, 1U);
  ASSERT_TRUE(kv_->HGetAll("user:2", &values).ok());
  ASSERT_TRUE(values.empty());
}

TEST_F(MiniKVHashTest, MissingKeyOperationsReturnEmptySuccess) {
  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(kv_->HGetAll("missing", &values).ok());
  ASSERT_TRUE(values.empty());

  uint64_t deleted = 42;
  ASSERT_TRUE(kv_->HDel("missing", {"a", "b"}, &deleted).ok());
  ASSERT_EQ(deleted, 0U);
}

TEST_F(MiniKVHashTest, HDelCountsOnlyExistingFields) {
  ASSERT_TRUE(kv_->HSet("user:4", "a", "1").ok());
  ASSERT_TRUE(kv_->HSet("user:4", "b", "2").ok());

  uint64_t deleted = 0;
  ASSERT_TRUE(kv_->HDel("user:4", {"a", "x", "b", "y"}, &deleted).ok());
  ASSERT_EQ(deleted, 2U);

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(kv_->HGetAll("user:4", &values).ok());
  ASSERT_TRUE(values.empty());
}

TEST_F(MiniKVHashTest, SameKeyConcurrentUpdatesStayConsistent) {
  std::vector<std::thread> threads;
  for (int i = 0; i < 16; ++i) {
    threads.emplace_back([this, i] {
      ASSERT_TRUE(
          kv_->HSet("user:3", "field:" + std::to_string(i),
                    "value:" + std::to_string(i))
              .ok());
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(kv_->HGetAll("user:3", &values).ok());
  ASSERT_EQ(values.size(), 16U);
}

TEST_F(MiniKVHashTest, SameFieldConcurrentOverwritePreservesSingleField) {
  std::vector<std::thread> threads;
  for (int i = 0; i < 16; ++i) {
    threads.emplace_back([this, i] {
      ASSERT_TRUE(
          kv_->HSet("user:5", "name", "value:" + std::to_string(i)).ok());
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(kv_->HGetAll("user:5", &values).ok());
  ASSERT_EQ(values.size(), 1U);
  ASSERT_EQ(values[0].field, "name");
}

TEST_F(MiniKVHashTest, DifferentKeysConcurrentOperationsDoNotInterfere) {
  std::vector<std::thread> threads;
  for (int i = 0; i < 32; ++i) {
    threads.emplace_back([this, i] {
      const std::string key = "user:key:" + std::to_string(i);
      ASSERT_TRUE(kv_->HSet(key, "field", "value").ok());
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  for (int i = 0; i < 32; ++i) {
    const std::string key = "user:key:" + std::to_string(i);
    std::vector<minikv::FieldValue> values;
    ASSERT_TRUE(kv_->HGetAll(key, &values).ok());
    ASSERT_EQ(values.size(), 1U);
    ASSERT_EQ(values[0].field, "field");
    ASSERT_EQ(values[0].value, "value");
  }
}

TEST_F(MiniKVHashTest, ExecuteUsesUnifiedCommandPath) {
  minikv::CommandResponse ping =
      kv_->Execute(minikv::CommandRequest{minikv::CommandType::kPing, "", {}});
  ASSERT_TRUE(ping.status.ok());
  ASSERT_TRUE(ping.reply.IsSimpleString());
  ASSERT_EQ(ping.reply.string(), "PONG");

  minikv::CommandResponse set = kv_->Execute(
      minikv::CommandRequest{minikv::CommandType::kHSet, "user:cmd",
                             {"name", "alice"}});
  ASSERT_TRUE(set.status.ok());
  ASSERT_TRUE(set.reply.IsInteger());
  ASSERT_EQ(set.reply.integer(), 1);

  minikv::CommandResponse get = kv_->Execute(
      minikv::CommandRequest{minikv::CommandType::kHGetAll, "user:cmd", {}});
  ASSERT_TRUE(get.status.ok());
  ASSERT_TRUE(get.reply.IsArray());
  ASSERT_EQ(get.reply.array().size(), 2U);
  ASSERT_EQ(get.reply.array()[0].string(), "name");
  ASSERT_EQ(get.reply.array()[1].string(), "alice");
}

TEST_F(MiniKVHashTest, PersistsTypedMetadataAcrossReopen) {
  ASSERT_TRUE(kv_->HSet("user:reopen", "name", "alice").ok());
  kv_.reset();

  minikv::Config config;
  config.db_path = db_path_;
  config.worker_threads = 4;
  ASSERT_TRUE(minikv::MiniKV::Open(config, &kv_).ok());

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(kv_->HGetAll("user:reopen", &values).ok());
  ASSERT_EQ(values.size(), 1U);
  ASSERT_EQ(values[0].field, "name");
  ASSERT_EQ(values[0].value, "alice");
}

TEST_F(MiniKVHashTest, RejectsOverloadedWorkerQueue) {
  kv_.reset();

  minikv::Config config;
  config.db_path = db_path_;
  config.worker_threads = 1;
  config.max_pending_requests_per_worker = 1;
  ASSERT_TRUE(minikv::MiniKV::Open(config, &kv_).ok());

  std::mutex mutex;
  std::condition_variable cv;
  bool callback_entered = false;
  bool release_callback = false;
  std::promise<void> queued_done;

  rocksdb::Status status = kv_->Submit(
      minikv::CommandRequest{minikv::CommandType::kPing, "", {}},
      [&](minikv::CommandResponse response) {
        ASSERT_TRUE(response.status.ok());
        {
          std::lock_guard<std::mutex> lock(mutex);
          callback_entered = true;
        }
        cv.notify_one();

        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [&] { return release_callback; });
      });
  ASSERT_TRUE(status.ok());

  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return callback_entered; });
  }

  status = kv_->Submit(
      minikv::CommandRequest{minikv::CommandType::kPing, "", {}},
      [&](minikv::CommandResponse response) {
        ASSERT_TRUE(response.status.ok());
        queued_done.set_value();
      });
  ASSERT_TRUE(status.ok());

  status = kv_->Submit(
      minikv::CommandRequest{minikv::CommandType::kPing, "", {}},
      [&](minikv::CommandResponse) {});
  ASSERT_TRUE(status.IsBusy());

  {
    std::lock_guard<std::mutex> lock(mutex);
    release_callback = true;
  }
  cv.notify_all();
  queued_done.get_future().wait();
}

TEST_F(MiniKVHashTest, AsyncSubmitProcessesParallelCommandsOnDifferentKeys) {
  std::vector<std::promise<void>> done(32);
  std::vector<std::future<void>> futures;
  futures.reserve(done.size());
  for (auto& promise : done) {
    futures.push_back(promise.get_future());
  }

  for (size_t i = 0; i < done.size(); ++i) {
    const std::string key = "async:key:" + std::to_string(i);
    rocksdb::Status status = kv_->Submit(
        minikv::CommandRequest{minikv::CommandType::kHSet, key,
                               {"field", "value:" + std::to_string(i)}},
        [&done, i](minikv::CommandResponse response) {
          ASSERT_TRUE(response.status.ok());
          ASSERT_TRUE(response.reply.IsInteger());
          done[i].set_value();
        });
    ASSERT_TRUE(status.ok());
  }

  for (auto& future : futures) {
    EXPECT_EQ(future.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
  }

  for (size_t i = 0; i < done.size(); ++i) {
    const std::string key = "async:key:" + std::to_string(i);
    std::vector<minikv::FieldValue> values;
    ASSERT_TRUE(kv_->HGetAll(key, &values).ok());
    ASSERT_EQ(values.size(), 1U);
    ASSERT_EQ(values[0].field, "field");
    ASSERT_EQ(values[0].value, "value:" + std::to_string(i));
  }
}

TEST_F(MiniKVHashTest, AsyncSubmitKeepsSameKeyStateConsistent) {
  std::vector<std::promise<void>> done(24);
  std::vector<std::future<void>> futures;
  futures.reserve(done.size());
  for (auto& promise : done) {
    futures.push_back(promise.get_future());
  }

  for (size_t i = 0; i < done.size(); ++i) {
    rocksdb::Status status = kv_->Submit(
        minikv::CommandRequest{minikv::CommandType::kHSet, "async:same",
                               {"field:" + std::to_string(i),
                                "value:" + std::to_string(i)}},
        [&done, i](minikv::CommandResponse response) {
          ASSERT_TRUE(response.status.ok());
          done[i].set_value();
        });
    ASSERT_TRUE(status.ok());
  }

  for (auto& future : futures) {
    EXPECT_EQ(future.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
  }

  std::vector<minikv::FieldValue> values;
  ASSERT_TRUE(kv_->HGetAll("async:same", &values).ok());
  ASSERT_EQ(values.size(), done.size());
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
