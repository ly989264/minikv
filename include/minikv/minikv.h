#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "minikv/command.h"
#include "minikv/config.h"
#include "rocksdb/status.h"

namespace minikv {

class Cmd;
class Scheduler;
class Server;

class MiniKV {
 public:
  using CommandCallback = std::function<void(CommandResponse)>;

  ~MiniKV();

  MiniKV(const MiniKV&) = delete;
  MiniKV& operator=(const MiniKV&) = delete;

  static rocksdb::Status Open(const Config& config,
                              std::unique_ptr<MiniKV>* minikv);

  CommandResponse Execute(const CommandRequest& request);
  CommandResponse Execute(std::string name, std::vector<std::string> args = {});
  CommandResponse Execute(std::string name, std::string key,
                          std::vector<std::string> args);
  rocksdb::Status Submit(const CommandRequest& request, CommandCallback callback);
  rocksdb::Status Submit(std::string name, std::vector<std::string> args,
                         CommandCallback callback);
  rocksdb::Status Submit(std::string name, std::string key,
                         std::vector<std::string> args,
                         CommandCallback callback);

  rocksdb::Status HSet(const std::string& key, const std::string& field,
                       const std::string& value, bool* inserted = nullptr);
  rocksdb::Status HGetAll(const std::string& key, std::vector<FieldValue>* out);
  rocksdb::Status HDel(const std::string& key,
                       const std::vector<std::string>& fields,
                       uint64_t* deleted = nullptr);

 private:
  friend class Server;
  class Impl;

  explicit MiniKV(std::unique_ptr<Impl> impl);
  rocksdb::Status Submit(std::unique_ptr<Cmd> cmd, CommandCallback callback);
  CommandResponse Execute(std::unique_ptr<Cmd> cmd);
  Scheduler* scheduler();

  std::unique_ptr<Impl> impl_;
};

}  // namespace minikv
