#include <array>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "command/cmd.h"
#include "gtest/gtest.h"
#include "kernel/scheduler.h"

namespace {

struct Tracker {
  std::mutex mutex;
  std::condition_variable cv;
  int running = 0;
  int max_running = 0;
  size_t entered_count = 0;
  size_t completed_count = 0;
  std::vector<std::thread::id> thread_ids;
};

struct Gate {
  bool entered = false;
  bool completed = false;
  bool release = false;
};

class BlockingCmd : public minikv::Cmd {
 public:
  BlockingCmd(std::string route_key, Tracker* tracker, Gate* gate,
              std::promise<void>* entered = nullptr)
      : Cmd("BLOCK", minikv::CmdFlags::kWrite),
        route_key_(std::move(route_key)),
        tracker_(tracker),
        gate_(gate),
        entered_(entered) {}

 private:
  rocksdb::Status DoInitial(const minikv::CmdInput& input) override {
    if (input.has_key) {
      SetRouteKey(input.key);
    }
    return rocksdb::Status::OK();
  }

  minikv::CommandResponse Do(minikv::CommandServices* /*context*/) override {
    {
      std::lock_guard<std::mutex> lock(tracker_->mutex);
      gate_->entered = true;
      ++tracker_->running;
      ++tracker_->entered_count;
      tracker_->max_running = std::max(tracker_->max_running, tracker_->running);
      tracker_->thread_ids.push_back(std::this_thread::get_id());
    }
    tracker_->cv.notify_all();
    if (entered_ != nullptr) {
      entered_->set_value();
    }

    {
      std::unique_lock<std::mutex> lock(tracker_->mutex);
      tracker_->cv.wait(lock, [&] { return gate_->release; });
      gate_->completed = true;
      --tracker_->running;
      ++tracker_->completed_count;
    }
    tracker_->cv.notify_all();
    return MakeSimpleString("OK");
  }

  std::string route_key_;
  Tracker* tracker_;
  Gate* gate_;
  std::promise<void>* entered_;
};

std::unique_ptr<minikv::Cmd> MakeBlockingCmd(const std::string& route_key,
                                             Tracker* tracker, Gate* gate,
                                             std::promise<void>* entered = nullptr) {
  auto cmd =
      std::make_unique<BlockingCmd>(route_key, tracker, gate, entered);
  minikv::CmdInput input;
  if (!route_key.empty()) {
    input.has_key = true;
    input.key = route_key;
  }
  EXPECT_TRUE(cmd->Init(input).ok());
  return cmd;
}

class QuickCmd : public minikv::Cmd {
 public:
  explicit QuickCmd(std::string route_key)
      : Cmd("QUICK", minikv::CmdFlags::kRead),
        route_key_(std::move(route_key)) {}

 private:
  rocksdb::Status DoInitial(const minikv::CmdInput& input) override {
    if (input.has_key) {
      SetRouteKey(input.key);
    }
    return rocksdb::Status::OK();
  }

  minikv::CommandResponse Do(minikv::CommandServices* /*context*/) override {
    return MakeSimpleString("OK");
  }

  std::string route_key_;
};

std::unique_ptr<minikv::Cmd> MakeQuickCmd(const std::string& route_key) {
  auto cmd = std::make_unique<QuickCmd>(route_key);
  minikv::CmdInput input;
  if (!route_key.empty()) {
    input.has_key = true;
    input.key = route_key;
  }
  EXPECT_TRUE(cmd->Init(input).ok());
  return cmd;
}

bool WaitFor(Tracker* tracker, const std::function<bool()>& predicate,
             std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(tracker->mutex);
  return tracker->cv.wait_for(lock, timeout, predicate);
}

TEST(SchedulerTest, SameKeyTasksDoNotExecuteInParallel) {
  minikv::Scheduler scheduler(nullptr, 2, 4);
  Tracker tracker;
  Gate first_gate;
  Gate second_gate;
  std::promise<void> first_done;
  std::promise<void> second_done;

  ASSERT_TRUE(scheduler.Submit(
                          MakeBlockingCmd("user:1", &tracker, &first_gate),
                          [&](minikv::CommandResponse response) {
                            ASSERT_TRUE(response.status.ok());
                            first_done.set_value();
                          })
                  .ok());
  ASSERT_TRUE(WaitFor(&tracker, [&] { return first_gate.entered; },
                      std::chrono::seconds(1)));

  ASSERT_TRUE(scheduler.Submit(
                          MakeBlockingCmd("user:1", &tracker, &second_gate),
                          [&](minikv::CommandResponse response) {
                            ASSERT_TRUE(response.status.ok());
                            second_done.set_value();
                          })
                  .ok());

  const bool second_entered_early =
      WaitFor(&tracker, [&] { return second_gate.entered; },
              std::chrono::milliseconds(100));
  EXPECT_FALSE(second_entered_early);

  {
    std::lock_guard<std::mutex> lock(tracker.mutex);
    first_gate.release = true;
  }
  tracker.cv.notify_all();

  const bool second_entered =
      WaitFor(&tracker, [&] { return second_gate.entered; },
              std::chrono::seconds(1));
  EXPECT_TRUE(second_entered);
  EXPECT_EQ(tracker.max_running, 1);

  {
    std::lock_guard<std::mutex> lock(tracker.mutex);
    second_gate.release = true;
  }
  tracker.cv.notify_all();

  first_done.get_future().wait();
  second_done.get_future().wait();
}

TEST(SchedulerTest, SameKeyTasksSerializeEvenWhenMultipleWorkersPickThemUp) {
  minikv::Scheduler scheduler(nullptr, 4, 8);
  Tracker tracker;
  std::array<Gate, 4> gates;
  std::vector<std::promise<void>> done(4);
  std::vector<std::future<void>> done_futures;
  done_futures.reserve(done.size());
  for (auto& promise : done) {
    done_futures.push_back(promise.get_future());
  }

  for (size_t i = 0; i < gates.size(); ++i) {
    ASSERT_TRUE(scheduler.Submit(
                            MakeBlockingCmd("shared:key", &tracker, &gates[i]),
                            [&done, i](minikv::CommandResponse response) {
                              ASSERT_TRUE(response.status.ok());
                              done[i].set_value();
                            })
                    .ok());
  }

  ASSERT_TRUE(WaitFor(&tracker, [&] { return tracker.entered_count == 1; },
                      std::chrono::seconds(1)));

  {
    std::lock_guard<std::mutex> lock(tracker.mutex);
    EXPECT_EQ(tracker.max_running, 1);
    EXPECT_EQ(tracker.thread_ids.size(), 1U);
  }

  for (size_t released = 0; released < gates.size(); ++released) {
    size_t active_gate = gates.size();
    {
      std::lock_guard<std::mutex> lock(tracker.mutex);
      for (size_t i = 0; i < gates.size(); ++i) {
        if (gates[i].entered && !gates[i].completed && !gates[i].release) {
          active_gate = i;
          break;
        }
      }
      ASSERT_NE(active_gate, gates.size());
      gates[active_gate].release = true;
    }
    tracker.cv.notify_all();
    if (released + 1 < gates.size()) {
      ASSERT_TRUE(WaitFor(&tracker,
                          [&] { return tracker.entered_count >= released + 2; },
                          std::chrono::seconds(1)));
    }
  }

  ASSERT_TRUE(WaitFor(&tracker,
                      [&] { return tracker.completed_count == gates.size(); },
                      std::chrono::seconds(1)));
  for (auto& future : done_futures) {
    future.wait();
  }

  std::set<std::thread::id> unique_threads;
  {
    std::lock_guard<std::mutex> lock(tracker.mutex);
    unique_threads.insert(tracker.thread_ids.begin(), tracker.thread_ids.end());
    EXPECT_EQ(tracker.max_running, 1);
    EXPECT_EQ(tracker.thread_ids.size(), 4U);
  }
  EXPECT_GE(unique_threads.size(), 2U);
}

TEST(SchedulerTest, DifferentKeysCanExecuteInParallel) {
  minikv::Scheduler scheduler(nullptr, 2, 4);
  Tracker tracker;
  Gate first_gate;
  Gate second_gate;
  std::promise<void> first_done;
  std::promise<void> second_done;

  ASSERT_TRUE(scheduler.Submit(
                          MakeBlockingCmd("user:1", &tracker, &first_gate),
                          [&](minikv::CommandResponse response) {
                            ASSERT_TRUE(response.status.ok());
                            first_done.set_value();
                          })
                  .ok());
  ASSERT_TRUE(WaitFor(&tracker, [&] { return first_gate.entered; },
                      std::chrono::seconds(1)));

  ASSERT_TRUE(scheduler.Submit(
                          MakeBlockingCmd("user:2", &tracker, &second_gate),
                          [&](minikv::CommandResponse response) {
                            ASSERT_TRUE(response.status.ok());
                            second_done.set_value();
                          })
                  .ok());
  const bool second_entered =
      WaitFor(&tracker, [&] { return second_gate.entered; },
              std::chrono::seconds(1));
  EXPECT_TRUE(second_entered);
  if (second_entered) {
    EXPECT_GE(tracker.max_running, 2);
  }

  {
    std::lock_guard<std::mutex> lock(tracker.mutex);
    first_gate.release = true;
    second_gate.release = true;
  }
  tracker.cv.notify_all();

  first_done.get_future().wait();
  second_done.get_future().wait();
}

TEST(SchedulerTest, EmptyRouteKeyDoesNotTakeKeyLock) {
  minikv::Scheduler scheduler(nullptr, 2, 4);
  Tracker tracker;
  Gate first_gate;
  Gate second_gate;
  std::promise<void> first_done;
  std::promise<void> second_done;

  ASSERT_TRUE(scheduler.Submit(
                          MakeBlockingCmd("", &tracker, &first_gate),
                          [&](minikv::CommandResponse response) {
                            ASSERT_TRUE(response.status.ok());
                            first_done.set_value();
                          })
                  .ok());
  ASSERT_TRUE(WaitFor(&tracker, [&] { return first_gate.entered; },
                      std::chrono::seconds(1)));

  ASSERT_TRUE(scheduler.Submit(
                          MakeBlockingCmd("", &tracker, &second_gate),
                          [&](minikv::CommandResponse response) {
                            ASSERT_TRUE(response.status.ok());
                            second_done.set_value();
                          })
                  .ok());
  const bool second_entered =
      WaitFor(&tracker, [&] { return second_gate.entered; },
              std::chrono::seconds(1));
  EXPECT_TRUE(second_entered);
  if (second_entered) {
    EXPECT_GE(tracker.max_running, 2);
  }

  {
    std::lock_guard<std::mutex> lock(tracker.mutex);
    first_gate.release = true;
    second_gate.release = true;
  }
  tracker.cv.notify_all();

  first_done.get_future().wait();
  second_done.get_future().wait();
}

TEST(SchedulerTest, DifferentKeyQuickTaskCompletesWhileHotKeyIsBlocked) {
  minikv::Scheduler scheduler(nullptr, 2, 4);
  Tracker tracker;
  Gate blocked_gate;
  std::promise<void> blocked_entered;
  std::future<void> blocked_entered_future = blocked_entered.get_future();
  std::promise<void> blocked_done;
  std::future<void> blocked_done_future = blocked_done.get_future();
  std::promise<void> quick_done;
  std::future<void> quick_done_future = quick_done.get_future();

  ASSERT_TRUE(scheduler.Submit(
                          MakeBlockingCmd("user:hot", &tracker, &blocked_gate,
                                          &blocked_entered),
                          [&](minikv::CommandResponse response) {
                            ASSERT_TRUE(response.status.ok());
                            blocked_done.set_value();
                          })
                  .ok());
  blocked_entered_future.wait();

  ASSERT_TRUE(scheduler.Submit(
                          MakeQuickCmd("user:cold"),
                          [&](minikv::CommandResponse response) {
                            ASSERT_TRUE(response.status.ok());
                            quick_done.set_value();
                          })
                  .ok());

  EXPECT_EQ(quick_done_future.wait_for(std::chrono::milliseconds(200)),
            std::future_status::ready);

  {
    std::lock_guard<std::mutex> lock(tracker.mutex);
    blocked_gate.release = true;
  }
  tracker.cv.notify_all();
  blocked_done_future.wait();
}

TEST(SchedulerTest, SameKeyQuickTaskWaitsUntilBlockedTaskReleasesLock) {
  minikv::Scheduler scheduler(nullptr, 2, 4);
  Tracker tracker;
  Gate blocked_gate;
  std::promise<void> blocked_entered;
  std::future<void> blocked_entered_future = blocked_entered.get_future();
  std::promise<void> blocked_done;
  std::future<void> blocked_done_future = blocked_done.get_future();
  std::promise<void> quick_done;
  std::future<void> quick_done_future = quick_done.get_future();

  ASSERT_TRUE(scheduler.Submit(
                          MakeBlockingCmd("user:locked", &tracker, &blocked_gate,
                                          &blocked_entered),
                          [&](minikv::CommandResponse response) {
                            ASSERT_TRUE(response.status.ok());
                            blocked_done.set_value();
                          })
                  .ok());
  blocked_entered_future.wait();

  ASSERT_TRUE(scheduler.Submit(
                          MakeQuickCmd("user:locked"),
                          [&](minikv::CommandResponse response) {
                            ASSERT_TRUE(response.status.ok());
                            quick_done.set_value();
                          })
                  .ok());

  EXPECT_EQ(quick_done_future.wait_for(std::chrono::milliseconds(150)),
            std::future_status::timeout);

  {
    std::lock_guard<std::mutex> lock(tracker.mutex);
    blocked_gate.release = true;
  }
  tracker.cv.notify_all();

  blocked_done_future.wait();
  EXPECT_EQ(quick_done_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
}

TEST(SchedulerTest, MetricsSnapshotTracksBacklogRejectionsAndInflight) {
  minikv::Scheduler scheduler(nullptr, 1, 1);
  Tracker tracker;
  Gate blocked_gate;
  std::promise<void> blocked_entered;
  std::future<void> blocked_entered_future = blocked_entered.get_future();
  std::promise<void> blocked_done;
  std::future<void> blocked_done_future = blocked_done.get_future();
  std::promise<void> quick_done;
  std::future<void> quick_done_future = quick_done.get_future();
  std::promise<void> queued_done;
  std::future<void> queued_done_future = queued_done.get_future();

  ASSERT_TRUE(scheduler.Submit(
                          MakeBlockingCmd("user:metric", &tracker, &blocked_gate,
                                          &blocked_entered),
                          [&](minikv::CommandResponse response) {
                            ASSERT_TRUE(response.status.ok());
                            blocked_done.set_value();
                          })
                  .ok());

  blocked_entered_future.wait();

  minikv::MetricsSnapshot first = scheduler.GetMetricsSnapshot();
  ASSERT_EQ(first.worker_queue_depth.size(), 1U);
  EXPECT_GE(first.worker_queue_depth[0], 0U);
  EXPECT_EQ(first.worker_inflight, 1U);
  EXPECT_EQ(first.worker_rejections, 0U);

  ASSERT_TRUE(scheduler
                  .Submit(MakeBlockingCmd("user:metric:queued", &tracker, &blocked_gate),
                          [&](minikv::CommandResponse response) {
                            ASSERT_TRUE(response.status.ok());
                            queued_done.set_value();
                          })
                  .ok());

  rocksdb::Status rejected =
      scheduler.Submit(MakeQuickCmd("user:metric:busy"),
                       [&](minikv::CommandResponse response) {
                         ASSERT_TRUE(response.status.ok());
                         quick_done.set_value();
                       });
  ASSERT_TRUE(rejected.IsBusy());

  minikv::MetricsSnapshot second = scheduler.GetMetricsSnapshot();
  EXPECT_EQ(second.worker_rejections, 1U);
  EXPECT_EQ(second.worker_inflight, 2U);

  {
    std::lock_guard<std::mutex> lock(tracker.mutex);
    blocked_gate.release = true;
  }
  tracker.cv.notify_all();
  blocked_done_future.wait();
  queued_done_future.wait();

  minikv::MetricsSnapshot third = scheduler.GetMetricsSnapshot();
  EXPECT_EQ(third.worker_inflight, 0U);
  EXPECT_EQ(third.worker_rejections, 1U);
  EXPECT_EQ(quick_done_future.wait_for(std::chrono::milliseconds(10)),
            std::future_status::timeout);
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
