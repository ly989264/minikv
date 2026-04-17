#include "network/network_server.h"

#ifndef _WIN32

#include <algorithm>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "command/cmd_create.h"
#include "command/command_types.h"
#include "common/thread_name.h"
#include "kernel/scheduler.h"
#include "minikv.h"
#include "network/resp_parser.h"

namespace minikv {

struct NetworkServer::Impl {
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

  Impl(const Config& config_value, MiniKV* minikv_value)
      : config(config_value), minikv(minikv_value) {}

  ~Impl() {
    Stop();
    Wait();
  }

  rocksdb::Status Start();
  void Stop();
  void Wait();
  rocksdb::Status Run();
  MetricsSnapshot GetMetricsSnapshot() const;

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

  Config config;
  MiniKV* minikv = nullptr;
  int listen_fd = -1;
  uint16_t bound_port = 0;
  std::thread accept_thread;
  std::vector<std::unique_ptr<IOThreadState>> io_threads;
  std::atomic<size_t> next_io_thread{0};
  std::atomic<uint64_t> next_connection_id{1};
  std::atomic<size_t> connection_count{0};
  std::atomic<uint64_t> accepted_connections{0};
  std::atomic<uint64_t> closed_connections{0};
  std::atomic<uint64_t> idle_timeout_connections{0};
  std::atomic<uint64_t> errored_connections{0};
  std::atomic<uint64_t> parse_errors{0};
  std::atomic<bool> stopping{false};
  std::atomic<bool> started{false};
};

NetworkServer::NetworkServer(const Config& config, MiniKV* minikv)
    : impl_(std::make_unique<Impl>(config, minikv)) {}

NetworkServer::~NetworkServer() = default;

rocksdb::Status NetworkServer::Start() { return impl_->Start(); }

void NetworkServer::Stop() { impl_->Stop(); }

void NetworkServer::Wait() { impl_->Wait(); }

rocksdb::Status NetworkServer::Run() { return impl_->Run(); }

uint16_t NetworkServer::port() const { return impl_->bound_port; }

MetricsSnapshot NetworkServer::GetMetricsSnapshot() const {
  return impl_->GetMetricsSnapshot();
}

Scheduler* NetworkServer::GetScheduler(MiniKV* minikv) {
  return minikv != nullptr ? minikv->scheduler() : nullptr;
}

rocksdb::Status NetworkServer::Impl::SetNonBlocking(int fd) {
  const int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return rocksdb::Status::IOError("fcntl(F_GETFL)", std::strerror(errno));
  }
  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    return rocksdb::Status::IOError("fcntl(F_SETFL)", std::strerror(errno));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status NetworkServer::Impl::SetupListenSocket() {
  listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    return rocksdb::Status::IOError("socket", std::strerror(errno));
  }

  int opt = 1;
  setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(config.port);
  if (inet_pton(AF_INET, config.bind_host.c_str(), &addr.sin_addr) != 1) {
    return rocksdb::Status::InvalidArgument("invalid bind host");
  }

  if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    return rocksdb::Status::IOError("bind", std::strerror(errno));
  }
  if (listen(listen_fd, 128) != 0) {
    return rocksdb::Status::IOError("listen", std::strerror(errno));
  }

  sockaddr_in bound_addr {};
  socklen_t bound_addr_len = sizeof(bound_addr);
  if (getsockname(listen_fd, reinterpret_cast<sockaddr*>(&bound_addr),
                  &bound_addr_len) != 0) {
    return rocksdb::Status::IOError("getsockname", std::strerror(errno));
  }
  bound_port = ntohs(bound_addr.sin_port);
  return SetNonBlocking(listen_fd);
}

rocksdb::Status NetworkServer::Impl::Start() {
  if (started.exchange(true)) {
    return rocksdb::Status::InvalidArgument("network server already started");
  }
  stopping.store(false);
  accepted_connections.store(0, std::memory_order_relaxed);
  closed_connections.store(0, std::memory_order_relaxed);
  idle_timeout_connections.store(0, std::memory_order_relaxed);
  errored_connections.store(0, std::memory_order_relaxed);
  parse_errors.store(0, std::memory_order_relaxed);
  rocksdb::Status status = SetupListenSocket();
  if (!status.ok()) {
    started.store(false);
    return status;
  }

  const size_t io_thread_count = std::max<size_t>(1, config.io_threads);
  io_threads.reserve(io_thread_count);
  for (size_t i = 0; i < io_thread_count; ++i) {
    auto io_thread = std::make_unique<IOThreadState>();
    int wakeup_pipe[2];
    if (pipe(wakeup_pipe) != 0) {
      return rocksdb::Status::IOError("pipe", std::strerror(errno));
    }
    io_thread->wakeup_read_fd = wakeup_pipe[0];
    io_thread->wakeup_write_fd = wakeup_pipe[1];

    status = SetNonBlocking(io_thread->wakeup_read_fd);
    if (!status.ok()) {
      close(io_thread->wakeup_read_fd);
      close(io_thread->wakeup_write_fd);
      return status;
    }
    status = SetNonBlocking(io_thread->wakeup_write_fd);
    if (!status.ok()) {
      close(io_thread->wakeup_read_fd);
      close(io_thread->wakeup_write_fd);
      return status;
    }
    io_threads.push_back(std::move(io_thread));
  }
  for (size_t i = 0; i < io_threads.size(); ++i) {
    io_threads[i]->thread = std::thread([this, i] { RunIOThread(i); });
  }

  accept_thread = std::thread([this] { AcceptLoop(); });
  return rocksdb::Status::OK();
}

void NetworkServer::Impl::Stop() {
  if (!started.load()) {
    return;
  }
  stopping.store(true);
  if (listen_fd >= 0) {
    shutdown(listen_fd, SHUT_RDWR);
    close(listen_fd);
    listen_fd = -1;
  }
  for (auto& io_thread : io_threads) {
    io_thread->cv.notify_one();
    NotifyIOThread(io_thread.get());
  }
}

void NetworkServer::Impl::Wait() {
  if (accept_thread.joinable()) {
    accept_thread.join();
  }
  for (auto& io_thread : io_threads) {
    if (io_thread->thread.joinable()) {
      io_thread->thread.join();
    }
    if (io_thread->wakeup_read_fd >= 0) {
      close(io_thread->wakeup_read_fd);
      io_thread->wakeup_read_fd = -1;
    }
    if (io_thread->wakeup_write_fd >= 0) {
      close(io_thread->wakeup_write_fd);
      io_thread->wakeup_write_fd = -1;
    }
  }
  io_threads.clear();
  started.store(false);
}

rocksdb::Status NetworkServer::Impl::Run() {
  rocksdb::Status status = Start();
  if (!status.ok()) {
    return status;
  }
  Wait();
  return rocksdb::Status::OK();
}

MetricsSnapshot NetworkServer::Impl::GetMetricsSnapshot() const {
  MetricsSnapshot snapshot;
  snapshot.active_connections = connection_count.load(std::memory_order_relaxed);
  snapshot.accepted_connections =
      accepted_connections.load(std::memory_order_relaxed);
  snapshot.closed_connections = closed_connections.load(std::memory_order_relaxed);
  snapshot.idle_timeout_connections =
      idle_timeout_connections.load(std::memory_order_relaxed);
  snapshot.errored_connections = errored_connections.load(std::memory_order_relaxed);
  snapshot.parse_errors = parse_errors.load(std::memory_order_relaxed);
  if (minikv != nullptr) {
    MetricsSnapshot worker_snapshot =
        GetScheduler(minikv)->GetMetricsSnapshot();
    snapshot.worker_queue_depth = std::move(worker_snapshot.worker_queue_depth);
    snapshot.worker_rejections = worker_snapshot.worker_rejections;
    snapshot.worker_inflight = worker_snapshot.worker_inflight;
  }
  return snapshot;
}

void NetworkServer::Impl::AcceptLoop() {
  SetCurrentThreadName("minikv-accept");
  while (!stopping.load()) {
    int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd < 0) {
      if (stopping.load()) {
        return;
      }
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        continue;
      }
      continue;
    }
    rocksdb::Status status = SetNonBlocking(client_fd);
    if (!status.ok()) {
      close(client_fd);
      continue;
    }
    EnqueueConnection(client_fd);
  }
}

void NetworkServer::Impl::EnqueueConnection(int fd) {
  const size_t observed = connection_count.fetch_add(1) + 1;
  if (observed > config.max_connections) {
    connection_count.fetch_sub(1);
    close(fd);
    return;
  }
  accepted_connections.fetch_add(1, std::memory_order_relaxed);

  const size_t io_index = next_io_thread.fetch_add(1) % io_threads.size();
  IOThreadState* io_thread = io_threads[io_index].get();
  {
    std::lock_guard<std::mutex> lock(io_thread->mutex);
    io_thread->pending_fds.push_back(fd);
  }
  NotifyIOThread(io_thread);
}

void NetworkServer::Impl::NotifyIOThread(IOThreadState* io_thread) const {
  if (io_thread->wakeup_write_fd < 0) {
    return;
  }
  const uint8_t token = 1;
  const ssize_t ignored = write(io_thread->wakeup_write_fd, &token, 1);
  (void)ignored;
}

void NetworkServer::Impl::DrainIOState(IOThreadState* io_thread) {
  std::deque<int> pending_fds;
  std::deque<CompletedResponse> completed;
  {
    std::lock_guard<std::mutex> lock(io_thread->mutex);
    pending_fds.swap(io_thread->pending_fds);
    completed.swap(io_thread->completed);
  }

  while (!pending_fds.empty()) {
    Connection connection;
    connection.id = next_connection_id.fetch_add(1);
    connection.fd = pending_fds.front();
    pending_fds.pop_front();
    connection.read_buffer.reserve(4096);
    connection.last_activity = std::chrono::steady_clock::now();
    io_thread->connections.push_back(std::move(connection));
  }

  while (!completed.empty()) {
    CompletedResponse item = std::move(completed.front());
    completed.pop_front();
    io_thread->inflight_requests.fetch_sub(1, std::memory_order_relaxed);

    Connection* connection = FindConnection(io_thread, item.connection_id);
    if (connection == nullptr) {
      continue;
    }
    if (connection->pending_requests > 0) {
      --connection->pending_requests;
    }
    connection->buffered_responses.emplace(item.request_seq,
                                           std::move(item.response));
    while (true) {
      auto response_it =
          connection->buffered_responses.find(connection->next_response_seq);
      if (response_it == connection->buffered_responses.end()) {
        break;
      }
      QueueResponse(connection, EncodeResponse(response_it->second));
      connection->buffered_responses.erase(response_it);
      ++connection->next_response_seq;
    }
    if (stopping.load() && connection->pending_requests == 0) {
      connection->close_after_write = true;
    }
  }
}

void NetworkServer::Impl::CloseIdleConnections(IOThreadState* io_thread) {
  const auto now = std::chrono::steady_clock::now();
  const auto idle_limit = std::chrono::milliseconds(config.idle_connection_timeout_ms);
  for (auto& connection : io_thread->connections) {
    if (connection.pending_requests != 0 ||
        connection.write_offset < connection.write_buffer.size()) {
      continue;
    }
    if (stopping.load() || now - connection.last_activity >= idle_limit) {
      if (!stopping.load() && now - connection.last_activity >= idle_limit) {
        connection.close_due_to_idle_timeout = true;
      }
      connection.close_after_write = true;
    }
  }
}

void NetworkServer::Impl::RunIOThread(size_t io_thread_id) {
  SetCurrentThreadName("minikv-io" + std::to_string(io_thread_id));
  IOThreadState* io_thread = io_threads[io_thread_id].get();

  while (true) {
    DrainIOState(io_thread);
    CloseIdleConnections(io_thread);

    {
      std::lock_guard<std::mutex> lock(io_thread->mutex);
      if (stopping.load() && io_thread->connections.empty() &&
          io_thread->pending_fds.empty() && io_thread->completed.empty() &&
          io_thread->inflight_requests.load(std::memory_order_relaxed) == 0) {
        return;
      }
    }

    std::vector<pollfd> poll_fds;
    poll_fds.reserve(io_thread->connections.size() + 1);

    pollfd wakeup_fd {};
    wakeup_fd.fd = io_thread->wakeup_read_fd;
    wakeup_fd.events = POLLIN;
    poll_fds.push_back(wakeup_fd);

    for (const auto& connection : io_thread->connections) {
      pollfd poll_fd {};
      poll_fd.fd = connection.fd;
      poll_fd.events = POLLIN;
      if (connection.write_offset < connection.write_buffer.size()) {
        poll_fd.events |= POLLOUT;
      }
      poll_fds.push_back(poll_fd);
    }

    const int ready_count = poll(poll_fds.data(), poll_fds.size(), 1000);
    if (ready_count < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    if (ready_count == 0) {
      continue;
    }

    if ((poll_fds[0].revents & POLLIN) != 0) {
      char wake_buffer[64];
      while (read(io_thread->wakeup_read_fd, wake_buffer, sizeof(wake_buffer)) > 0) {
      }
    }

    for (size_t index = 1; index < poll_fds.size();) {
      bool remove_connection = false;
      Connection* connection = &io_thread->connections[index - 1];
      if ((poll_fds[index].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
        connection->close_due_to_error = true;
        remove_connection = true;
      } else {
        if ((poll_fds[index].revents & POLLIN) != 0) {
          remove_connection = !HandleReadable(io_thread_id, connection);
          if (remove_connection) {
            connection->close_due_to_error = true;
          }
        }
        if (!remove_connection && (poll_fds[index].revents & POLLOUT) != 0) {
          remove_connection = !HandleWritable(connection);
          if (remove_connection) {
            connection->close_due_to_error = true;
          }
        }
        if (!remove_connection && connection->close_after_write &&
            connection->pending_requests == 0 &&
            connection->write_offset >= connection->write_buffer.size()) {
          remove_connection = true;
        }
      }

      if (remove_connection) {
        CloseConnection(connection);
        io_thread->connections[index - 1] =
            std::move(io_thread->connections.back());
        io_thread->connections.pop_back();
        poll_fds[index] = poll_fds.back();
        poll_fds.pop_back();
      } else {
        ++index;
      }
    }
  }

  for (auto& connection : io_thread->connections) {
    CloseConnection(&connection);
  }
  io_thread->connections.clear();
}

bool NetworkServer::Impl::HandleReadable(size_t io_thread_id,
                                         Connection* connection) {
  RespParser parser;
  char read_buffer[4096];
  while (true) {
    const ssize_t bytes = read(connection->fd, read_buffer, sizeof(read_buffer));
    if (bytes == 0) {
      connection->close_after_write = true;
      return connection->pending_requests > 0 ||
             connection->write_offset < connection->write_buffer.size();
    }
    if (bytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        break;
      }
      if (errno == EINTR) {
        continue;
      }
      return false;
    }

    connection->last_activity = std::chrono::steady_clock::now();
    connection->read_buffer.append(read_buffer, static_cast<size_t>(bytes));
    if (connection->read_buffer.size() > config.max_request_bytes) {
      QueueResponse(connection, EncodeError("ERR request too large"));
      connection->close_after_write = true;
      if (!HandleWritable(connection)) {
        return false;
      }
      if (connection->write_offset >= connection->write_buffer.size()) {
        return false;
      }
      return true;
    }

    while (true) {
      std::vector<std::string> parts;
      std::string error;
      const bool ready = parser.Parse(&connection->read_buffer, &parts, &error);
      if (!ready) {
        break;
      }
      if (!error.empty()) {
        parse_errors.fetch_add(1, std::memory_order_relaxed);
        QueueResponse(connection, EncodeError("ERR " + error));
        continue;
      }

      std::unique_ptr<Cmd> cmd;
      rocksdb::Status status =
          CreateCmd(minikv->command_registry(), parts, &cmd);
      if (!status.ok()) {
        QueueResponse(connection, EncodeError("ERR " + status.ToString()));
        continue;
      }

      ++connection->pending_requests;
      const uint64_t request_seq = connection->next_request_seq++;
      IOThreadState* io_thread = io_threads[io_thread_id].get();
      io_thread->inflight_requests.fetch_add(1, std::memory_order_relaxed);
      status = GetScheduler(minikv)->Submit(
          std::move(cmd),
          [this, io_thread_id, connection_id = connection->id, request_seq](
              CommandResponse response) mutable {
            IOThreadState* state = io_threads[io_thread_id].get();
            {
              std::lock_guard<std::mutex> lock(state->mutex);
              state->completed.push_back(
                  CompletedResponse{connection_id, request_seq, std::move(response)});
            }
            NotifyIOThread(state);
          });
      if (!status.ok()) {
        io_thread->inflight_requests.fetch_sub(1, std::memory_order_relaxed);
        --connection->pending_requests;
        QueueResponse(connection, EncodeError("ERR " + status.ToString()));
      }
    }
  }
  return true;
}

bool NetworkServer::Impl::HandleWritable(Connection* connection) {
  while (connection->write_offset < connection->write_buffer.size()) {
    const char* data =
        connection->write_buffer.data() + connection->write_offset;
    const size_t remaining =
        connection->write_buffer.size() - connection->write_offset;
    const ssize_t bytes = write(connection->fd, data, remaining);
    if (bytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return true;
      }
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (bytes == 0) {
      return false;
    }
    connection->write_offset += static_cast<size_t>(bytes);
    connection->last_activity = std::chrono::steady_clock::now();
  }

  connection->write_buffer.clear();
  connection->write_offset = 0;
  return true;
}

void NetworkServer::Impl::CloseConnection(Connection* connection) {
  if (connection->fd >= 0) {
    close(connection->fd);
    connection->fd = -1;
    connection_count.fetch_sub(1);
    closed_connections.fetch_add(1, std::memory_order_relaxed);
    if (connection->close_due_to_idle_timeout) {
      idle_timeout_connections.fetch_add(1, std::memory_order_relaxed);
    }
    if (connection->close_due_to_error) {
      errored_connections.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

void NetworkServer::Impl::QueueResponse(Connection* connection,
                                        std::string response) {
  if (connection->write_offset >= connection->write_buffer.size()) {
    connection->write_buffer.clear();
    connection->write_offset = 0;
  }
  connection->write_buffer.append(std::move(response));
  connection->last_activity = std::chrono::steady_clock::now();
}

NetworkServer::Impl::Connection* NetworkServer::Impl::FindConnection(
    IOThreadState* io_thread, uint64_t connection_id) {
  for (auto& connection : io_thread->connections) {
    if (connection.id == connection_id) {
      return &connection;
    }
  }
  return nullptr;
}

}  // namespace minikv

#else

namespace minikv {

struct NetworkServer::Impl {
  Impl(const Config&, MiniKV*) {}

  rocksdb::Status Start() {
    return rocksdb::Status::NotSupported("minikv_server is POSIX-only");
  }

  void Stop() {}
  void Wait() {}

  rocksdb::Status Run() {
    return rocksdb::Status::NotSupported("minikv_server is POSIX-only");
  }

  MetricsSnapshot GetMetricsSnapshot() const { return MetricsSnapshot{}; }

  uint16_t bound_port = 0;
};

NetworkServer::NetworkServer(const Config& config, MiniKV* minikv)
    : impl_(std::make_unique<Impl>(config, minikv)) {}

NetworkServer::~NetworkServer() = default;

rocksdb::Status NetworkServer::Start() { return impl_->Start(); }

void NetworkServer::Stop() { impl_->Stop(); }

void NetworkServer::Wait() { impl_->Wait(); }

rocksdb::Status NetworkServer::Run() { return impl_->Run(); }

uint16_t NetworkServer::port() const { return impl_->bound_port; }

MetricsSnapshot NetworkServer::GetMetricsSnapshot() const {
  return impl_->GetMetricsSnapshot();
}

}  // namespace minikv

#endif
