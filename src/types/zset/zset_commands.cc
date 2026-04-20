#include "types/zset/zset_commands.h"

#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "types/zset/zset_module.h"

namespace minikv {

namespace {

double CanonicalizeZero(double value) {
  return value == 0 ? 0.0 : value;
}

std::string LowercaseAscii(std::string input) {
  for (char& ch : input) {
    if (ch >= 'A' && ch <= 'Z') {
      ch = static_cast<char>(ch - 'A' + 'a');
    }
  }
  return input;
}

bool ParseRawDouble(const std::string& input, double* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  errno = 0;
  char* parse_end = nullptr;
  const double parsed = std::strtod(input.c_str(), &parse_end);
  if (parse_end == nullptr || *parse_end != '\0' || std::isnan(parsed)) {
    return false;
  }
  if (errno == ERANGE && !std::isinf(parsed)) {
    return false;
  }

  *value = CanonicalizeZero(parsed);
  return true;
}

bool ParseScoreValue(const std::string& input, double* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  const std::string lowered = LowercaseAscii(input);
  if (lowered == "inf" || lowered == "+inf") {
    *value = std::numeric_limits<double>::infinity();
    return true;
  }
  if (lowered == "-inf") {
    *value = -std::numeric_limits<double>::infinity();
    return true;
  }

  double parsed = 0;
  if (!ParseRawDouble(input, &parsed) || std::isinf(parsed)) {
    return false;
  }
  *value = parsed;
  return true;
}

std::string FormatScore(double score) {
  score = CanonicalizeZero(score);
  if (std::isinf(score)) {
    return score < 0 ? "-inf" : "inf";
  }

  char buffer[64];
  const int length = std::snprintf(buffer, sizeof(buffer), "%.17g", score);
  if (length <= 0) {
    return "0";
  }
  return std::string(buffer, static_cast<size_t>(length));
}

bool ParseInt64(const std::string& input, int64_t* value) {
  if (value == nullptr || input.empty()) {
    return false;
  }

  errno = 0;
  char* parse_end = nullptr;
  const long long parsed = std::strtoll(input.c_str(), &parse_end, 10);
  if (parse_end == nullptr || *parse_end != '\0' || errno == ERANGE) {
    return false;
  }

  *value = static_cast<int64_t>(parsed);
  return true;
}

class ZAddCmd : public Cmd {
 public:
  ZAddCmd(const CmdRegistration& registration, ZSetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty() || (input.args.size() % 2) != 0) {
      return rocksdb::Status::InvalidArgument(
          "ZADD requires score/member pairs");
    }

    key_ = input.key;
    entries_.clear();
    entries_.reserve(input.args.size() / 2);
    for (size_t i = 0; i < input.args.size(); i += 2) {
      double score = 0;
      if (!ParseScoreValue(input.args[i], &score)) {
        return rocksdb::Status::InvalidArgument("ZADD requires valid score");
      }
      entries_.push_back(ZSetEntry{input.args[i + 1], score});
    }

    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("zset module is unavailable"));
    }

    uint64_t added = 0;
    rocksdb::Status status = module_->AddMembers(key_, entries_, &added);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(added));
  }

  ZSetModule* module_ = nullptr;
  std::string key_;
  std::vector<ZSetEntry> entries_;
};

class ZCardCmd : public Cmd {
 public:
  ZCardCmd(const CmdRegistration& registration, ZSetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("ZCARD takes no extra arguments");
    }

    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("zset module is unavailable"));
    }

    uint64_t size = 0;
    rocksdb::Status status = module_->Cardinality(key_, &size);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(size));
  }

  ZSetModule* module_ = nullptr;
  std::string key_;
};

class ZCountCmd : public Cmd {
 public:
  ZCountCmd(const CmdRegistration& registration, ZSetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument("ZCOUNT requires min and max");
    }

    key_ = input.key;
    min_ = input.args[0];
    max_ = input.args[1];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("zset module is unavailable"));
    }

    uint64_t count = 0;
    rocksdb::Status status = module_->CountByScore(key_, min_, max_, &count);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(count));
  }

  ZSetModule* module_ = nullptr;
  std::string key_;
  std::string min_;
  std::string max_;
};

class ZIncrByCmd : public Cmd {
 public:
  ZIncrByCmd(const CmdRegistration& registration, ZSetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument(
          "ZINCRBY requires increment and member");
    }
    if (!ParseScoreValue(input.args[0], &increment_)) {
      return rocksdb::Status::InvalidArgument(
          "ZINCRBY requires valid increment");
    }

    key_ = input.key;
    member_ = input.args[1];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("zset module is unavailable"));
    }

    double new_score = 0;
    rocksdb::Status status =
        module_->IncrementBy(key_, increment_, member_, &new_score);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeBulkString(FormatScore(new_score));
  }

  ZSetModule* module_ = nullptr;
  std::string key_;
  std::string member_;
  double increment_ = 0;
};

class ZLexCountCmd : public Cmd {
 public:
  ZLexCountCmd(const CmdRegistration& registration, ZSetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument("ZLEXCOUNT requires min and max");
    }

    key_ = input.key;
    min_ = input.args[0];
    max_ = input.args[1];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("zset module is unavailable"));
    }

    uint64_t count = 0;
    rocksdb::Status status = module_->CountByLex(key_, min_, max_, &count);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(count));
  }

  ZSetModule* module_ = nullptr;
  std::string key_;
  std::string min_;
  std::string max_;
};

class ZRangeCmd : public Cmd {
 public:
  ZRangeCmd(const CmdRegistration& registration, ZSetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument("ZRANGE requires start and stop");
    }
    if (!ParseInt64(input.args[0], &start_)) {
      return rocksdb::Status::InvalidArgument("ZRANGE requires integer start");
    }
    if (!ParseInt64(input.args[1], &stop_)) {
      return rocksdb::Status::InvalidArgument("ZRANGE requires integer stop");
    }

    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("zset module is unavailable"));
    }

    std::vector<std::string> members;
    rocksdb::Status status = module_->RangeByRank(key_, start_, stop_, &members);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeArray(std::move(members));
  }

  ZSetModule* module_ = nullptr;
  std::string key_;
  int64_t start_ = 0;
  int64_t stop_ = 0;
};

class ZRangeByLexCmd : public Cmd {
 public:
  ZRangeByLexCmd(const CmdRegistration& registration, ZSetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument(
          "ZRANGEBYLEX requires min and max");
    }

    key_ = input.key;
    min_ = input.args[0];
    max_ = input.args[1];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("zset module is unavailable"));
    }

    std::vector<std::string> members;
    rocksdb::Status status = module_->RangeByLex(key_, min_, max_, &members);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeArray(std::move(members));
  }

  ZSetModule* module_ = nullptr;
  std::string key_;
  std::string min_;
  std::string max_;
};

class ZRangeByScoreCmd : public Cmd {
 public:
  ZRangeByScoreCmd(const CmdRegistration& registration, ZSetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 2) {
      return rocksdb::Status::InvalidArgument(
          "ZRANGEBYSCORE requires min and max");
    }

    key_ = input.key;
    min_ = input.args[0];
    max_ = input.args[1];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("zset module is unavailable"));
    }

    std::vector<std::string> members;
    rocksdb::Status status = module_->RangeByScore(key_, min_, max_, &members);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeArray(std::move(members));
  }

  ZSetModule* module_ = nullptr;
  std::string key_;
  std::string min_;
  std::string max_;
};

class ZRankCmd : public Cmd {
 public:
  ZRankCmd(const CmdRegistration& registration, ZSetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("ZRANK requires member");
    }

    key_ = input.key;
    member_ = input.args[0];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("zset module is unavailable"));
    }

    uint64_t rank = 0;
    bool found = false;
    rocksdb::Status status = module_->Rank(key_, member_, &rank, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeInteger(static_cast<long long>(rank));
  }

  ZSetModule* module_ = nullptr;
  std::string key_;
  std::string member_;
};

class ZRemCmd : public Cmd {
 public:
  ZRemCmd(const CmdRegistration& registration, ZSetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "ZREM requires at least one member");
    }

    key_ = input.key;
    members_ = input.args;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("zset module is unavailable"));
    }

    uint64_t removed = 0;
    rocksdb::Status status = module_->RemoveMembers(key_, members_, &removed);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(static_cast<long long>(removed));
  }

  ZSetModule* module_ = nullptr;
  std::string key_;
  std::vector<std::string> members_;
};

class ZScoreCmd : public Cmd {
 public:
  ZScoreCmd(const CmdRegistration& registration, ZSetModule* module)
      : Cmd(registration.name, registration.flags), module_(module) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("ZSCORE requires member");
    }

    key_ = input.key;
    member_ = input.args[0];
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (module_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("zset module is unavailable"));
    }

    double score = 0;
    bool found = false;
    rocksdb::Status status = module_->Score(key_, member_, &score, &found);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!found) {
      return MakeNull();
    }
    return MakeBulkString(FormatScore(score));
  }

  ZSetModule* module_ = nullptr;
  std::string key_;
  std::string member_;
};

}  // namespace

rocksdb::Status RegisterZSetCommands(ModuleServices& services,
                                     ZSetModule* module) {
  rocksdb::Status status = services.command_registry().Register(
      {"ZADD", CmdFlags::kWrite | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<ZAddCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"ZCARD", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<ZCardCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"ZCOUNT", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<ZCountCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"ZINCRBY", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<ZIncrByCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"ZLEXCOUNT", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<ZLexCountCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"ZRANGE", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<ZRangeCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"ZRANGEBYLEX", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<ZRangeByLexCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"ZRANGEBYSCORE", CmdFlags::kRead | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<ZRangeByScoreCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"ZRANK", CmdFlags::kRead | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<ZRankCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"ZREM", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<ZRemCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"ZSCORE", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [module](const CmdRegistration& registration) {
         return std::make_unique<ZScoreCmd>(registration, module);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  return rocksdb::Status::OK();
}

}  // namespace minikv
