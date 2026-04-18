#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "kernel/scheduler.h"
#include "module/background_executor.h"
#include "module/module.h"
#include "module/module_manager.h"
#include "module/module_services.h"

namespace {

using namespace std::chrono_literals;

class BackgroundRecordingModule : public minikv::Module {
 public:
  BackgroundRecordingModule(std::vector<std::string>* events, bool* done,
                            std::mutex* mutex, std::condition_variable* cv)
      : events_(events), done_(done), mutex_(mutex), cv_(cv) {}

  std::string_view Name() const override { return "search"; }

  rocksdb::Status OnLoad(minikv::ModuleServices& /*services*/) override {
    return rocksdb::Status::OK();
  }

  rocksdb::Status OnStart(minikv::ModuleServices& services) override {
    return services.background().Submit([this]() {
      {
        std::lock_guard<std::mutex> lock(*mutex_);
        events_->push_back("search.background");
        *done_ = true;
      }
      cv_->notify_all();
    });
  }

  void OnStop(minikv::ModuleServices& /*services*/) override {}

 private:
  std::vector<std::string>* events_ = nullptr;
  bool* done_ = nullptr;
  std::mutex* mutex_ = nullptr;
  std::condition_variable* cv_ = nullptr;
};

TEST(BackgroundExecutorTest, ExecutesTasksInSubmissionOrder) {
  minikv::BackgroundExecutor executor("background-test");
  ASSERT_TRUE(executor.Start().ok());

  std::mutex mutex;
  std::condition_variable cv;
  std::vector<int> values;
  int remaining = 3;

  for (int value : {1, 2, 3}) {
    ASSERT_TRUE(executor
                    .Submit([&mutex, &cv, &values, &remaining, value]() {
                      {
                        std::lock_guard<std::mutex> lock(mutex);
                        values.push_back(value);
                        --remaining;
                      }
                      cv.notify_all();
                    })
                    .ok());
  }

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, 5s, [&remaining]() { return remaining == 0; }));
  }

  executor.Stop();
  EXPECT_EQ(values, (std::vector<int>{1, 2, 3}));
  EXPECT_TRUE(executor.Submit([]() {}).IsInvalidArgument());
}

TEST(BackgroundExecutorTest, ModuleManagerExposesBackgroundService) {
  std::vector<std::string> events;
  bool done = false;
  std::mutex mutex;
  std::condition_variable cv;

  std::vector<std::unique_ptr<minikv::Module>> modules;
  modules.push_back(std::make_unique<BackgroundRecordingModule>(
      &events, &done, &mutex, &cv));

  minikv::Scheduler scheduler(1, 8);
  minikv::ModuleManager manager(nullptr, &scheduler, std::move(modules));
  ASSERT_TRUE(manager.Initialize().ok());

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, 5s, [&done]() { return done; }));
  }

  manager.StopAll();
  EXPECT_EQ(events, (std::vector<std::string>{"search.background"}));
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
