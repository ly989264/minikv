#include <algorithm>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "runtime/config.h"
#include "runtime/minikv.h"
#include "network/network_server.h"
#include "rocksdb/db.h"

namespace {

struct RespValue {
  enum class Type {
    kSimpleString,
    kError,
    kInteger,
    kBulkString,
    kArray,
    kNull,
  };

  Type type = Type::kSimpleString;
  std::string text;
  long long integer = 0;
  std::vector<RespValue> array;
};

std::string EncodeCommand(const std::vector<std::string>& parts) {
  std::string out = "*" + std::to_string(parts.size()) + "\r\n";
  for (const auto& part : parts) {
    out += "$" + std::to_string(part.size()) + "\r\n" + part + "\r\n";
  }
  return out;
}

void ExpectArrayTexts(const RespValue& value,
                      const std::vector<std::string>& expected) {
  ASSERT_EQ(value.type, RespValue::Type::kArray);
  ASSERT_EQ(value.array.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    ASSERT_EQ(value.array[i].type, RespValue::Type::kBulkString);
    EXPECT_EQ(value.array[i].text, expected[i]);
  }
}

void ExpectArrayTextsUnordered(const RespValue& value,
                               const std::vector<std::string>& expected) {
  ASSERT_EQ(value.type, RespValue::Type::kArray);
  ASSERT_EQ(value.array.size(), expected.size());

  std::vector<std::string> actual;
  actual.reserve(value.array.size());
  for (const auto& item : value.array) {
    ASSERT_EQ(item.type, RespValue::Type::kBulkString);
    actual.push_back(item.text);
  }

  std::vector<std::string> actual_sorted = actual;
  std::vector<std::string> expected_sorted = expected;
  std::sort(actual_sorted.begin(), actual_sorted.end());
  std::sort(expected_sorted.begin(), expected_sorted.end());
  EXPECT_EQ(actual_sorted, expected_sorted);
}

void WriteAll(int fd, const std::string& data) {
  size_t offset = 0;
  while (offset < data.size()) {
    const ssize_t bytes = write(fd, data.data() + offset, data.size() - offset);
    EXPECT_GT(bytes, 0);
    if (bytes <= 0) {
      return;
    }
    offset += static_cast<size_t>(bytes);
  }
}

void SetRecvTimeout(int fd, int timeout_ms) {
  timeval timeout {};
  timeout.tv_sec = timeout_ms / 1000;
  timeout.tv_usec = (timeout_ms % 1000) * 1000;
  EXPECT_EQ(
      setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);
}

std::string ReadExact(int fd, size_t length) {
  std::string out(length, '\0');
  size_t offset = 0;
  while (offset < length) {
    const ssize_t bytes = read(fd, &out[offset], length - offset);
    EXPECT_GT(bytes, 0);
    if (bytes <= 0) {
      return "";
    }
    offset += static_cast<size_t>(bytes);
  }
  return out;
}

std::string ReadLine(int fd) {
  std::string line;
  char c = '\0';
  while (true) {
    const ssize_t bytes = read(fd, &c, 1);
    EXPECT_GT(bytes, 0);
    if (bytes <= 0) {
      return "";
    }
    line.push_back(c);
    if (line.size() >= 2 &&
        line.compare(line.size() - 2, 2, "\r\n") == 0) {
      line.resize(line.size() - 2);
      return line;
    }
  }
}

RespValue ReadRespValue(int fd) {
  const std::string prefix = ReadExact(fd, 1);
  EXPECT_FALSE(prefix.empty());

  RespValue value;
  switch (prefix[0]) {
    case '+':
      value.type = RespValue::Type::kSimpleString;
      value.text = ReadLine(fd);
      return value;
    case '-':
      value.type = RespValue::Type::kError;
      value.text = ReadLine(fd);
      return value;
    case ':':
      value.type = RespValue::Type::kInteger;
      value.integer = std::stoll(ReadLine(fd));
      return value;
    case '$': {
      value.type = RespValue::Type::kBulkString;
      const long long length = std::stoll(ReadLine(fd));
      EXPECT_GE(length, 0);
      value.text = ReadExact(fd, static_cast<size_t>(length));
      EXPECT_EQ(ReadExact(fd, 2), "\r\n");
      return value;
    }
    case '*': {
      value.type = RespValue::Type::kArray;
      const long long count = std::stoll(ReadLine(fd));
      EXPECT_GE(count, 0);
      for (long long i = 0; i < count; ++i) {
        value.array.push_back(ReadRespValue(fd));
      }
      return value;
    }
    case '_':
      value.type = RespValue::Type::kNull;
      EXPECT_EQ(ReadExact(fd, 2), "\r\n");
      return value;
    default:
      ADD_FAILURE() << "unexpected RESP prefix";
      return value;
  }
}

int ConnectToServer(uint16_t port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  EXPECT_GE(fd, 0);

  sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  EXPECT_EQ(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr), 1);
  EXPECT_EQ(connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  return fd;
}

void SeedHashOverNetwork(uint16_t port, const std::string& key, int field_count,
                         size_t value_size) {
  const int fd = ConnectToServer(port);
  const std::string value(value_size, 'v');
  for (int i = 0; i < field_count; ++i) {
    WriteAll(
        fd,
        EncodeCommand({"HSET", key, "field:" + std::to_string(i), value}));
    RespValue reply = ReadRespValue(fd);
    ASSERT_EQ(reply.type, RespValue::Type::kInteger);
    ASSERT_EQ(reply.integer, 1);
  }
  close(fd);
}

class MiniKVServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = (std::filesystem::temp_directory_path() /
                ("minikv-server-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(counter_++)))
                   .string();

    minikv::Config config;
    config.db_path = db_path_;
    config.bind_host = "127.0.0.1";
    config.port = 0;
    config.io_threads = 3;
    config.worker_threads = 4;
    ASSERT_TRUE(minikv::MiniKV::Open(config, &kv_).ok());

    server_ = std::make_unique<minikv::NetworkServer>(config, kv_.get());
    ASSERT_TRUE(server_->Start().ok());
    ASSERT_GT(server_->port(), 0);
  }

  void TearDown() override {
    if (server_ != nullptr) {
      server_->Stop();
      server_->Wait();
      server_.reset();
    }
    kv_.reset();
    rocksdb::Options options;
    ASSERT_TRUE(rocksdb::DestroyDB(db_path_, options).ok());
  }

  static inline int counter_ = 0;
  std::string db_path_;
  std::unique_ptr<minikv::MiniKV> kv_;
  std::unique_ptr<minikv::NetworkServer> server_;
};

TEST_F(MiniKVServerTest, PingAndBasicHashLifecycle) {
  const int fd = ConnectToServer(server_->port());

  const std::string ping = EncodeCommand({"PING"});
  WriteAll(fd, ping);
  RespValue pong = ReadRespValue(fd);
  ASSERT_EQ(pong.type, RespValue::Type::kSimpleString);
  ASSERT_EQ(pong.text, "PONG");

  const std::string hset_insert =
      EncodeCommand({"HSET", "user:1", "name", "alice"});
  WriteAll(fd, hset_insert);
  RespValue hset_result = ReadRespValue(fd);
  ASSERT_EQ(hset_result.type, RespValue::Type::kInteger);
  ASSERT_EQ(hset_result.integer, 1);

  const std::string hset_update =
      EncodeCommand({"HSET", "user:1", "name", "alice-2"});
  WriteAll(fd, hset_update);
  hset_result = ReadRespValue(fd);
  ASSERT_EQ(hset_result.integer, 0);

  const std::string type_existing = EncodeCommand({"TYPE", "user:1"});
  WriteAll(fd, type_existing);
  RespValue type = ReadRespValue(fd);
  ASSERT_EQ(type.type, RespValue::Type::kBulkString);
  ASSERT_EQ(type.text, "hash");

  const std::string exists_existing = EncodeCommand({"EXISTS", "user:1"});
  WriteAll(fd, exists_existing);
  RespValue exists = ReadRespValue(fd);
  ASSERT_EQ(exists.type, RespValue::Type::kInteger);
  ASSERT_EQ(exists.integer, 1);

  const std::string hgetall = EncodeCommand({"HGETALL", "user:1"});
  WriteAll(fd, hgetall);
  RespValue all = ReadRespValue(fd);
  ASSERT_EQ(all.type, RespValue::Type::kArray);
  ASSERT_EQ(all.array.size(), 2U);
  ASSERT_EQ(all.array[0].text, "name");
  ASSERT_EQ(all.array[1].text, "alice-2");

  const std::string hdel = EncodeCommand({"HDEL", "user:1", "name"});
  WriteAll(fd, hdel);
  RespValue del = ReadRespValue(fd);
  ASSERT_EQ(del.type, RespValue::Type::kInteger);
  ASSERT_EQ(del.integer, 1);

  WriteAll(fd, hgetall);
  all = ReadRespValue(fd);
  ASSERT_EQ(all.type, RespValue::Type::kArray);
  ASSERT_TRUE(all.array.empty());

  WriteAll(fd, type_existing);
  type = ReadRespValue(fd);
  ASSERT_EQ(type.type, RespValue::Type::kBulkString);
  ASSERT_EQ(type.text, "none");

  WriteAll(fd, exists_existing);
  exists = ReadRespValue(fd);
  ASSERT_EQ(exists.type, RespValue::Type::kInteger);
  ASSERT_EQ(exists.integer, 0);

  const std::string type_missing = EncodeCommand({"TYPE", "missing"});
  WriteAll(fd, type_missing);
  type = ReadRespValue(fd);
  ASSERT_EQ(type.type, RespValue::Type::kBulkString);
  ASSERT_EQ(type.text, "none");

  const std::string exists_missing = EncodeCommand({"EXISTS", "missing"});
  WriteAll(fd, exists_missing);
  exists = ReadRespValue(fd);
  ASSERT_EQ(exists.type, RespValue::Type::kInteger);
  ASSERT_EQ(exists.integer, 0);

  close(fd);
}

TEST_F(MiniKVServerTest, FragmentedRespInputAndErrorPath) {
  const int fd = ConnectToServer(server_->port());

  const std::string fragmented =
      EncodeCommand({"HSET", "user:frag", "field", "value"});
  const size_t split = fragmented.size() / 2;
  ASSERT_GT(write(fd, fragmented.data(), split), 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  ASSERT_GT(write(fd, fragmented.data() + split, fragmented.size() - split), 0);

  RespValue result = ReadRespValue(fd);
  ASSERT_EQ(result.type, RespValue::Type::kInteger);
  ASSERT_EQ(result.integer, 1);

  const std::string unknown = EncodeCommand({"UNKNOWN"});
  WriteAll(fd, unknown);
  RespValue error = ReadRespValue(fd);
  ASSERT_EQ(error.type, RespValue::Type::kError);
  ASSERT_NE(error.text.find("unsupported command"), std::string::npos);

  close(fd);
}

TEST_F(MiniKVServerTest, MultiKeyExistsAndDelFollowRedisSemantics) {
  const int fd = ConnectToServer(server_->port());

  WriteAll(fd, EncodeCommand({"HSET", "user:multi:1", "name", "alice"}));
  RespValue reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 1);

  WriteAll(fd, EncodeCommand({"HSET", "user:multi:2", "name", "bob"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 1);

  WriteAll(fd, EncodeCommand({"EXISTS", "user:multi:1", "user:multi:1",
                              "missing", "user:multi:2"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 3);

  WriteAll(fd, EncodeCommand({"DEL", "user:multi:1", "user:multi:1",
                              "missing", "user:multi:2"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 2);

  WriteAll(fd, EncodeCommand({"TYPE", "user:multi:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kBulkString);
  ASSERT_EQ(reply.text, "none");

  WriteAll(fd, EncodeCommand({"HGETALL", "user:multi:2"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kArray);
  ASSERT_TRUE(reply.array.empty());

  close(fd);
}

TEST_F(MiniKVServerTest, ConcurrentClientsAcrossIoThreads) {
  std::vector<std::thread> clients;
  for (int i = 0; i < 12; ++i) {
    clients.emplace_back([this, i] {
      const int fd = ConnectToServer(server_->port());
      const std::string key = "user:client:" + std::to_string(i);
      const std::string hset =
          EncodeCommand({"HSET", key, "field", "value:" + std::to_string(i)});
      WriteAll(fd, hset);
      RespValue hset_reply = ReadRespValue(fd);
      ASSERT_EQ(hset_reply.type, RespValue::Type::kInteger);
      ASSERT_EQ(hset_reply.integer, 1);

      const std::string hgetall = EncodeCommand({"HGETALL", key});
      WriteAll(fd, hgetall);
      RespValue all = ReadRespValue(fd);
      ASSERT_EQ(all.type, RespValue::Type::kArray);
      ASSERT_EQ(all.array.size(), 2U);
      ASSERT_EQ(all.array[0].text, "field");
      ASSERT_EQ(all.array[1].text, "value:" + std::to_string(i));
      close(fd);
    });
  }

  for (auto& client : clients) {
    client.join();
  }
}

TEST_F(MiniKVServerTest, SetLifecycleAndRandomCommandsWorkOverNetwork) {
  const int fd = ConnectToServer(server_->port());

  WriteAll(fd, EncodeCommand({"SADD", "set:1", "a", "b", "c", "a"}));
  RespValue reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 3);

  WriteAll(fd, EncodeCommand({"TYPE", "set:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kBulkString);
  ASSERT_EQ(reply.text, "set");

  WriteAll(fd, EncodeCommand({"SCARD", "set:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 3);

  WriteAll(fd, EncodeCommand({"SISMEMBER", "set:1", "a"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 1);

  WriteAll(fd, EncodeCommand({"SISMEMBER", "set:1", "x"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 0);

  WriteAll(fd, EncodeCommand({"SMEMBERS", "set:1"}));
  reply = ReadRespValue(fd);
  ExpectArrayTextsUnordered(reply, {"a", "b", "c"});

  WriteAll(fd, EncodeCommand({"SRANDMEMBER", "set:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kBulkString);
  ASSERT_TRUE(reply.text == "a" || reply.text == "b" || reply.text == "c");

  WriteAll(fd, EncodeCommand({"SCARD", "set:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 3);

  WriteAll(fd, EncodeCommand({"SPOP", "set:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kBulkString);
  ASSERT_TRUE(reply.text == "a" || reply.text == "b" || reply.text == "c");

  WriteAll(fd, EncodeCommand({"SCARD", "set:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 2);

  WriteAll(fd, EncodeCommand({"SREM", "set:1", "a", "b", "c"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 2);

  WriteAll(fd, EncodeCommand({"TYPE", "set:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kBulkString);
  ASSERT_EQ(reply.text, "none");

  WriteAll(fd, EncodeCommand({"EXISTS", "set:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 0);

  WriteAll(fd, EncodeCommand({"SPOP", "set:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kNull);

  WriteAll(fd, EncodeCommand({"SRANDMEMBER", "set:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kNull);

  close(fd);
}

TEST_F(MiniKVServerTest, ListLifecycleCommandsWorkOverNetwork) {
  const int fd = ConnectToServer(server_->port());

  WriteAll(fd, EncodeCommand({"RPUSH", "list:1", "a", "b", "c"}));
  RespValue reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 3);

  WriteAll(fd, EncodeCommand({"LPUSH", "list:1", "z"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 4);

  WriteAll(fd, EncodeCommand({"LRANGE", "list:1", "0", "-1"}));
  reply = ReadRespValue(fd);
  ExpectArrayTexts(reply, {"z", "a", "b", "c"});

  WriteAll(fd, EncodeCommand({"LPOP", "list:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kBulkString);
  ASSERT_EQ(reply.text, "z");

  WriteAll(fd, EncodeCommand({"RPOP", "list:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kBulkString);
  ASSERT_EQ(reply.text, "c");

  WriteAll(fd, EncodeCommand({"LREM", "list:1", "0", "b"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 1);

  WriteAll(fd, EncodeCommand({"LTRIM", "list:1", "0", "0"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kSimpleString);
  ASSERT_EQ(reply.text, "OK");

  WriteAll(fd, EncodeCommand({"LLEN", "list:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 1);

  WriteAll(fd, EncodeCommand({"LRANGE", "list:1", "0", "-1"}));
  reply = ReadRespValue(fd);
  ExpectArrayTexts(reply, {"a"});

  WriteAll(fd, EncodeCommand({"TYPE", "list:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kBulkString);
  ASSERT_EQ(reply.text, "list");

  WriteAll(fd, EncodeCommand({"EXPIRE", "list:1", "0"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 1);

  WriteAll(fd, EncodeCommand({"TYPE", "list:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kBulkString);
  ASSERT_EQ(reply.text, "none");

  WriteAll(fd, EncodeCommand({"EXISTS", "list:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kInteger);
  ASSERT_EQ(reply.integer, 0);

  WriteAll(fd, EncodeCommand({"LPOP", "list:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kNull);

  WriteAll(fd, EncodeCommand({"RPOP", "list:1"}));
  reply = ReadRespValue(fd);
  ASSERT_EQ(reply.type, RespValue::Type::kNull);

  close(fd);
}

TEST_F(MiniKVServerTest, MalformedRespDoesNotPoisonNextCommand) {
  const int fd = ConnectToServer(server_->port());

  WriteAll(fd, "+oops\r\n");
  RespValue error = ReadRespValue(fd);
  ASSERT_EQ(error.type, RespValue::Type::kError);
  ASSERT_NE(error.text.find("expected RESP array"), std::string::npos);

  WriteAll(fd, EncodeCommand({"PING"}));
  RespValue pong = ReadRespValue(fd);
  ASSERT_EQ(pong.type, RespValue::Type::kSimpleString);
  ASSERT_EQ(pong.text, "PONG");

  close(fd);
}

TEST_F(MiniKVServerTest, RejectsOversizedRequest) {
  const int fd = ConnectToServer(server_->port());
  const std::string large_value(70 * 1024, 'x');

  WriteAll(fd, EncodeCommand({"HSET", "user:large", "field", large_value}));
  RespValue error = ReadRespValue(fd);
  ASSERT_EQ(error.type, RespValue::Type::kError);
  ASSERT_NE(error.text.find("request too large"), std::string::npos);

  SetRecvTimeout(fd, 200);
  bool saw_eof = false;
  for (int i = 0; i < 10; ++i) {
    char extra = '\0';
    const ssize_t bytes = read(fd, &extra, 1);
    if (bytes <= 0) {
      saw_eof = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_TRUE(saw_eof);
  close(fd);
}

TEST_F(MiniKVServerTest, PipelineResponsesStayInRequestOrderAcrossWorkers) {
  SeedHashOverNetwork(server_->port(), "user:slow", 2048, 256);

  const int fd = ConnectToServer(server_->port());
  WriteAll(fd, EncodeCommand({"HGETALL", "user:slow"}) + EncodeCommand({"PING"}));

  RespValue first = ReadRespValue(fd);
  ASSERT_EQ(first.type, RespValue::Type::kArray);
  ASSERT_EQ(first.array.size(), 4096U);

  RespValue second = ReadRespValue(fd);
  ASSERT_EQ(second.type, RespValue::Type::kSimpleString);
  ASSERT_EQ(second.text, "PONG");

  close(fd);
}

TEST_F(MiniKVServerTest, StopDrainsInflightRequestsBeforeClosingConnection) {
  SeedHashOverNetwork(server_->port(), "user:drain", 2048, 256);

  const int fd = ConnectToServer(server_->port());
  SetRecvTimeout(fd, 2000);
  WriteAll(fd, EncodeCommand({"HGETALL", "user:drain"}));
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  server_->Stop();

  RespValue response = ReadRespValue(fd);
  ASSERT_EQ(response.type, RespValue::Type::kArray);
  ASSERT_EQ(response.array.size(), 4096U);

  bool saw_eof = false;
  for (int i = 0; i < 10; ++i) {
    char extra = '\0';
    const ssize_t bytes = read(fd, &extra, 1);
    if (bytes <= 0) {
      saw_eof = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_TRUE(saw_eof);
  close(fd);
}

TEST_F(MiniKVServerTest, SlowCommandOnOneKeyDoesNotBlockFastCommandOnOtherKey) {
  SeedHashOverNetwork(server_->port(), "user:slow-cmd", 4096, 256);

  const int slow_fd = ConnectToServer(server_->port());
  const int fast_fd = ConnectToServer(server_->port());
  SetRecvTimeout(slow_fd, 3000);
  SetRecvTimeout(fast_fd, 500);

  WriteAll(slow_fd, EncodeCommand({"HGETALL", "user:slow-cmd"}));
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  WriteAll(fast_fd, EncodeCommand({"PING"}));

  RespValue fast = ReadRespValue(fast_fd);
  ASSERT_EQ(fast.type, RespValue::Type::kSimpleString);
  ASSERT_EQ(fast.text, "PONG");

  RespValue slow = ReadRespValue(slow_fd);
  ASSERT_EQ(slow.type, RespValue::Type::kArray);
  ASSERT_EQ(slow.array.size(), 8192U);

  close(fast_fd);
  close(slow_fd);
}

TEST_F(MiniKVServerTest, MetricsSnapshotTracksConnectionsAndParseErrors) {
  minikv::MetricsSnapshot initial = server_->GetMetricsSnapshot();
  EXPECT_EQ(initial.accepted_connections, 0U);
  EXPECT_EQ(initial.closed_connections, 0U);
  EXPECT_EQ(initial.parse_errors, 0U);
  EXPECT_EQ(initial.active_connections, 0U);

  const int fd = ConnectToServer(server_->port());
  bool saw_accepted = false;
  for (int i = 0; i < 20; ++i) {
    minikv::MetricsSnapshot accepted = server_->GetMetricsSnapshot();
    if (accepted.accepted_connections >= 1U && accepted.active_connections >= 1U) {
      saw_accepted = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_TRUE(saw_accepted);

  WriteAll(fd, "+bad\r\n");
  RespValue error = ReadRespValue(fd);
  ASSERT_EQ(error.type, RespValue::Type::kError);
  ASSERT_NE(error.text.find("expected RESP array"), std::string::npos);

  minikv::MetricsSnapshot parsed = server_->GetMetricsSnapshot();
  EXPECT_EQ(parsed.parse_errors, 1U);
  EXPECT_EQ(parsed.active_connections, 1U);

  close(fd);
  for (int i = 0; i < 20; ++i) {
    minikv::MetricsSnapshot snapshot = server_->GetMetricsSnapshot();
    if (snapshot.closed_connections >= 1U && snapshot.active_connections == 0U) {
      EXPECT_EQ(snapshot.closed_connections, 1U);
      EXPECT_EQ(snapshot.accepted_connections, 1U);
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  FAIL() << "connection metrics were not updated in time";
}

TEST(MiniKVServerIsolationTest, SlowClientDoesNotBlockOtherConnections) {
  static int counter = 0;
  const std::string db_path =
      (std::filesystem::temp_directory_path() /
       ("minikv-slow-client-" + std::to_string(::getpid()) + "-" +
        std::to_string(counter++)))
          .string();

  minikv::Config config;
  config.db_path = db_path;
  config.bind_host = "127.0.0.1";
  config.port = 0;
  config.io_threads = 1;
  config.worker_threads = 2;
  config.max_request_bytes = 8 * 1024 * 1024;

  std::unique_ptr<minikv::MiniKV> kv;
  ASSERT_TRUE(minikv::MiniKV::Open(config, &kv).ok());

  minikv::NetworkServer server(config, kv.get());
  ASSERT_TRUE(server.Start().ok());
  ASSERT_GT(server.port(), 0);
  SeedHashOverNetwork(server.port(), "user:slow", 512, 4096);

  const int slow_fd = ConnectToServer(server.port());
  const int small_buffer = 1024;
  ASSERT_EQ(setsockopt(slow_fd, SOL_SOCKET, SO_RCVBUF, &small_buffer,
                       sizeof(small_buffer)),
            0);
  WriteAll(slow_fd, EncodeCommand({"HGETALL", "user:slow"}));

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  const int fast_fd = ConnectToServer(server.port());
  SetRecvTimeout(fast_fd, 1000);
  WriteAll(fast_fd, EncodeCommand({"PING"}));
  RespValue pong = ReadRespValue(fast_fd);
  ASSERT_EQ(pong.type, RespValue::Type::kSimpleString);
  ASSERT_EQ(pong.text, "PONG");

  close(fast_fd);
  close(slow_fd);
  server.Stop();
  server.Wait();
  kv.reset();
  rocksdb::Options options;
  ASSERT_TRUE(rocksdb::DestroyDB(db_path, options).ok());
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
