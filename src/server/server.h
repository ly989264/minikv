#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "minikv/config.h"
#include "minikv/minikv.h"
#include "metrics.h"
#include "rocksdb/status.h"

namespace minikv {

class Server {
 public:
  Server(const Config& config, MiniKV* minikv);
  ~Server();

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  rocksdb::Status Start();
  void Stop();
  void Wait();
  rocksdb::Status Run();
  uint16_t port() const { return bound_port_; }
  MetricsSnapshot GetMetricsSnapshot() const;

 private:
  struct Connection {
    uint64_t id = 0;
    int fd = -1;
    std::string read_buffer;
    std::string write_buffer;
    size_t write_offset = 0;
    size_t pending_requests = 0;
    uint64_t next_request_seq = 0;
    uint64_t next_response_seq = 0;
    std::map<uint64_t, CommandResponse> buffered_responses;
    bool close_after_write = false;
    bool close_due_to_idle_timeout = false;
    bool close_due_to_error = false;
    std::chrono::steady_clock::time_point last_activity;
  };

  struct CompletedResponse {
    uint64_t connection_id = 0;
    uint64_t request_seq = 0;
    CommandResponse response;
  };

  struct IOThreadState {
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<int> pending_fds;
    std::deque<CompletedResponse> completed;
    std::vector<Connection> connections;
    std::thread thread;
    int wakeup_read_fd = -1;
    int wakeup_write_fd = -1;
    std::atomic<size_t> inflight_requests{0};
  };

  rocksdb::Status SetupListenSocket();
  static rocksdb::Status SetNonBlocking(int fd);
  void AcceptLoop();
  void RunIOThread(size_t io_thread_id);
  void EnqueueConnection(int fd);
  void DrainIOState(IOThreadState* io_thread);
  void CloseIdleConnections(IOThreadState* io_thread);
  bool HandleReadable(size_t io_thread_id, Connection* connection);
  bool HandleWritable(Connection* connection);
  void CloseConnection(Connection* connection);
  void QueueResponse(Connection* connection, std::string response);
  Connection* FindConnection(IOThreadState* io_thread, uint64_t connection_id);
  void NotifyIOThread(IOThreadState* io_thread) const;

  Config config_;
  MiniKV* minikv_;
  int listen_fd_ = -1;
  uint16_t bound_port_ = 0;
  std::thread accept_thread_;
  std::vector<std::unique_ptr<IOThreadState>> io_threads_;
  std::atomic<size_t> next_io_thread_{0};
  std::atomic<uint64_t> next_connection_id_{1};
  std::atomic<size_t> connection_count_{0};
  std::atomic<uint64_t> accepted_connections_{0};
  std::atomic<uint64_t> closed_connections_{0};
  std::atomic<uint64_t> idle_timeout_connections_{0};
  std::atomic<uint64_t> errored_connections_{0};
  std::atomic<uint64_t> parse_errors_{0};
  std::atomic<bool> stopping_{false};
  std::atomic<bool> started_{false};
};

}  // namespace minikv
