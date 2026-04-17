#include "server/server.h"

#ifndef _WIN32

#include <algorithm>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "command/cmd_create.h"
#include "common/thread_name.h"
#include "kernel/scheduler.h"
#include "server/resp_parser.h"

namespace minikv {

Server::Server(const Config& config, MiniKV* minikv)
    : config_(config), minikv_(minikv) {}

Server::~Server() {
  Stop();
  Wait();
}

rocksdb::Status Server::SetNonBlocking(int fd) {
  const int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return rocksdb::Status::IOError("fcntl(F_GETFL)", std::strerror(errno));
  }
  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    return rocksdb::Status::IOError("fcntl(F_SETFL)", std::strerror(errno));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Server::SetupListenSocket() {
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    return rocksdb::Status::IOError("socket", std::strerror(errno));
  }

  int opt = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(config_.port);
  if (inet_pton(AF_INET, config_.bind_host.c_str(), &addr.sin_addr) != 1) {
    return rocksdb::Status::InvalidArgument("invalid bind host");
  }

  if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    return rocksdb::Status::IOError("bind", std::strerror(errno));
  }
  if (listen(listen_fd_, 128) != 0) {
    return rocksdb::Status::IOError("listen", std::strerror(errno));
  }

  sockaddr_in bound_addr {};
  socklen_t bound_addr_len = sizeof(bound_addr);
  if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound_addr),
                  &bound_addr_len) != 0) {
    return rocksdb::Status::IOError("getsockname", std::strerror(errno));
  }
  bound_port_ = ntohs(bound_addr.sin_port);
  return SetNonBlocking(listen_fd_);
}

rocksdb::Status Server::Start() {
  if (started_.exchange(true)) {
    return rocksdb::Status::InvalidArgument("server already started");
  }
  accepted_connections_.store(0, std::memory_order_relaxed);
  closed_connections_.store(0, std::memory_order_relaxed);
  idle_timeout_connections_.store(0, std::memory_order_relaxed);
  errored_connections_.store(0, std::memory_order_relaxed);
  parse_errors_.store(0, std::memory_order_relaxed);
  rocksdb::Status status = SetupListenSocket();
  if (!status.ok()) {
    started_.store(false);
    return status;
  }

  const size_t io_thread_count = std::max<size_t>(1, config_.io_threads);
  io_threads_.reserve(io_thread_count);
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
    io_threads_.push_back(std::move(io_thread));
  }
  for (size_t i = 0; i < io_threads_.size(); ++i) {
    io_threads_[i]->thread = std::thread([this, i] { RunIOThread(i); });
  }

  accept_thread_ = std::thread([this] { AcceptLoop(); });
  return rocksdb::Status::OK();
}

void Server::Stop() {
  if (!started_.load()) {
    return;
  }
  stopping_.store(true);
  if (listen_fd_ >= 0) {
    shutdown(listen_fd_, SHUT_RDWR);
    close(listen_fd_);
    listen_fd_ = -1;
  }
  for (auto& io_thread : io_threads_) {
    io_thread->cv.notify_one();
    NotifyIOThread(io_thread.get());
  }
}

void Server::Wait() {
  if (accept_thread_.joinable()) {
    accept_thread_.join();
  }
  for (auto& io_thread : io_threads_) {
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
  io_threads_.clear();
  started_.store(false);
}

rocksdb::Status Server::Run() {
  rocksdb::Status status = Start();
  if (!status.ok()) {
    return status;
  }
  Wait();
  return rocksdb::Status::OK();
}

MetricsSnapshot Server::GetMetricsSnapshot() const {
  MetricsSnapshot snapshot;
  snapshot.active_connections = connection_count_.load(std::memory_order_relaxed);
  snapshot.accepted_connections =
      accepted_connections_.load(std::memory_order_relaxed);
  snapshot.closed_connections = closed_connections_.load(std::memory_order_relaxed);
  snapshot.idle_timeout_connections =
      idle_timeout_connections_.load(std::memory_order_relaxed);
  snapshot.errored_connections =
      errored_connections_.load(std::memory_order_relaxed);
  snapshot.parse_errors = parse_errors_.load(std::memory_order_relaxed);
  if (minikv_ != nullptr) {
    MetricsSnapshot worker_snapshot = minikv_->scheduler()->GetMetricsSnapshot();
    snapshot.worker_queue_depth = std::move(worker_snapshot.worker_queue_depth);
    snapshot.worker_rejections = worker_snapshot.worker_rejections;
    snapshot.worker_inflight = worker_snapshot.worker_inflight;
  }
  return snapshot;
}

void Server::AcceptLoop() {
  SetCurrentThreadName("minikv-accept");
  while (!stopping_.load()) {
    int client_fd = accept(listen_fd_, nullptr, nullptr);
    if (client_fd < 0) {
      if (stopping_.load()) {
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

void Server::EnqueueConnection(int fd) {
  const size_t observed = connection_count_.fetch_add(1) + 1;
  if (observed > config_.max_connections) {
    connection_count_.fetch_sub(1);
    close(fd);
    return;
  }
  accepted_connections_.fetch_add(1, std::memory_order_relaxed);

  const size_t io_index = next_io_thread_.fetch_add(1) % io_threads_.size();
  IOThreadState* io_thread = io_threads_[io_index].get();
  {
    std::lock_guard<std::mutex> lock(io_thread->mutex);
    io_thread->pending_fds.push_back(fd);
  }
  NotifyIOThread(io_thread);
}

void Server::NotifyIOThread(IOThreadState* io_thread) const {
  if (io_thread->wakeup_write_fd < 0) {
    return;
  }
  const uint8_t token = 1;
  const ssize_t ignored = write(io_thread->wakeup_write_fd, &token, 1);
  (void)ignored;
}

void Server::DrainIOState(IOThreadState* io_thread) {
  std::deque<int> pending_fds;
  std::deque<CompletedResponse> completed;
  {
    std::lock_guard<std::mutex> lock(io_thread->mutex);
    pending_fds.swap(io_thread->pending_fds);
    completed.swap(io_thread->completed);
  }

  while (!pending_fds.empty()) {
    Connection connection;
    connection.id = next_connection_id_.fetch_add(1);
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
    if (stopping_.load() && connection->pending_requests == 0) {
      connection->close_after_write = true;
    }
  }
}

void Server::CloseIdleConnections(IOThreadState* io_thread) {
  const auto now = std::chrono::steady_clock::now();
  const auto idle_limit =
      std::chrono::milliseconds(config_.idle_connection_timeout_ms);
  for (auto& connection : io_thread->connections) {
    if (connection.pending_requests != 0 ||
        connection.write_offset < connection.write_buffer.size()) {
      continue;
    }
    if (stopping_.load() || now - connection.last_activity >= idle_limit) {
      if (!stopping_.load() && now - connection.last_activity >= idle_limit) {
        connection.close_due_to_idle_timeout = true;
      }
      connection.close_after_write = true;
    }
  }
}

void Server::RunIOThread(size_t io_thread_id) {
  SetCurrentThreadName("minikv-io" + std::to_string(io_thread_id));
  IOThreadState* io_thread = io_threads_[io_thread_id].get();

  while (true) {
    DrainIOState(io_thread);
    CloseIdleConnections(io_thread);

    {
      std::lock_guard<std::mutex> lock(io_thread->mutex);
      if (stopping_.load() && io_thread->connections.empty() &&
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
    wakeup_fd.revents = 0;
    poll_fds.push_back(wakeup_fd);

    for (const auto& connection : io_thread->connections) {
      pollfd poll_fd {};
      poll_fd.fd = connection.fd;
      poll_fd.events = POLLIN;
      if (connection.write_offset < connection.write_buffer.size()) {
        poll_fd.events |= POLLOUT;
      }
      poll_fd.revents = 0;
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
      while (read(io_thread->wakeup_read_fd, wake_buffer, sizeof(wake_buffer)) >
             0) {
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

bool Server::HandleReadable(size_t io_thread_id, Connection* connection) {
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
    if (connection->read_buffer.size() > config_.max_request_bytes) {
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
        parse_errors_.fetch_add(1, std::memory_order_relaxed);
        QueueResponse(connection, EncodeError("ERR " + error));
        continue;
      }

      std::unique_ptr<Cmd> cmd;
      rocksdb::Status status = CreateCmd(parts, &cmd);
      if (!status.ok()) {
        QueueResponse(connection, EncodeError("ERR " + status.ToString()));
        continue;
      }

      ++connection->pending_requests;
      const uint64_t request_seq = connection->next_request_seq++;
      IOThreadState* io_thread = io_threads_[io_thread_id].get();
      io_thread->inflight_requests.fetch_add(1, std::memory_order_relaxed);
      status = minikv_->Submit(
          std::move(cmd),
          [this, io_thread_id, connection_id = connection->id, request_seq](
              CommandResponse response) mutable {
            IOThreadState* state = io_threads_[io_thread_id].get();
            {
              std::lock_guard<std::mutex> lock(state->mutex);
              state->completed.push_back(CompletedResponse{
                  connection_id, request_seq, std::move(response)});
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

bool Server::HandleWritable(Connection* connection) {
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

void Server::CloseConnection(Connection* connection) {
  if (connection->fd >= 0) {
    close(connection->fd);
    connection->fd = -1;
    connection_count_.fetch_sub(1);
    closed_connections_.fetch_add(1, std::memory_order_relaxed);
    if (connection->close_due_to_idle_timeout) {
      idle_timeout_connections_.fetch_add(1, std::memory_order_relaxed);
    }
    if (connection->close_due_to_error) {
      errored_connections_.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

void Server::QueueResponse(Connection* connection, std::string response) {
  if (connection->write_offset >= connection->write_buffer.size()) {
    connection->write_buffer.clear();
    connection->write_offset = 0;
  }
  connection->write_buffer.append(std::move(response));
  connection->last_activity = std::chrono::steady_clock::now();
}

Server::Connection* Server::FindConnection(IOThreadState* io_thread,
                                           uint64_t connection_id) {
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

Server::Server(const Config& config, MiniKV* minikv)
    : config_(config), minikv_(minikv) {}

Server::~Server() = default;

rocksdb::Status Server::Start() {
  return rocksdb::Status::NotSupported("minikv_server is POSIX-only");
}

void Server::Stop() {}

void Server::Wait() {}

rocksdb::Status Server::Run() {
  return rocksdb::Status::NotSupported("minikv_server is POSIX-only");
}

MetricsSnapshot Server::GetMetricsSnapshot() const { return MetricsSnapshot{}; }

}  // namespace minikv

#endif
