#include "core/core_module.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "execution/command/cmd.h"
#include "runtime/module/module_services.h"
#include "storage/encoding/key_codec.h"

namespace minikv {
namespace {

struct ExpireCandidate {
  std::string index_key;
  std::string key;
  uint64_t expire_at_ms = 0;
  bool decoded = false;
};

std::vector<std::string> CollectKeys(const CmdInput& input) {
  std::vector<std::string> keys;
  if (!input.has_key) {
    return keys;
  }
  keys.reserve(1 + input.args.size());
  keys.push_back(input.key);
  keys.insert(keys.end(), input.args.begin(), input.args.end());
  return keys;
}

bool IsLive(const KeyLookup& lookup) {
  return lookup.state == KeyLifecycleState::kLive;
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

uint64_t ComputeExpireAtMs(uint64_t now_ms, int64_t ttl_seconds) {
  const uint64_t max_value = std::numeric_limits<uint64_t>::max();
  const uint64_t ttl_seconds_u = static_cast<uint64_t>(ttl_seconds);
  if (ttl_seconds_u > max_value / 1000) {
    return max_value;
  }
  const uint64_t ttl_ms = ttl_seconds_u * 1000;
  if (ttl_ms > max_value - now_ms) {
    return max_value;
  }
  return now_ms + ttl_ms;
}

bool HasUserExpireAt(uint64_t expire_at_ms) {
  return expire_at_ms != 0 &&
         !DefaultCoreKeyService::IsLogicalDeleteExpireAt(expire_at_ms);
}

ModuleKeyspace CoreExpireIndexKeyspace() {
  return ModuleKeyspace(StorageColumnFamily::kModule, "core", "expires");
}

rocksdb::Status DeleteLiveKey(WholeKeyDeleteRegistry* delete_registry,
                              ModuleSnapshot* snapshot,
                              ModuleWriteBatch* write_batch,
                              const std::string& key,
                              const KeyLookup& lookup) {
  if (delete_registry == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "core delete services are unavailable");
  }
  return delete_registry->DeleteWholeKey(snapshot, write_batch, key, lookup);
}

class ActiveExpireDeleteCmd : public Cmd {
 public:
  ActiveExpireDeleteCmd(ModuleServices* services,
                        const CoreKeyService* key_service,
                        CoreModule* core_module,
                        std::vector<ExpireCandidate> candidates)
      : Cmd("ACTIVE_EXPIRE", CmdFlags::kWrite | CmdFlags::kSlow),
        services_(services),
        key_service_(key_service),
        core_module_(core_module),
        candidates_(std::move(candidates)) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (input.has_key || !input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "active expire command takes no arguments");
    }
    std::vector<std::string> keys;
    keys.reserve(candidates_.size());
    for (const auto& candidate : candidates_) {
      if (candidate.decoded) {
        keys.push_back(candidate.key);
      }
    }
    SetRouteKeys(std::move(keys));
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (services_ == nullptr || key_service_ == nullptr ||
        core_module_ == nullptr) {
      return MakeStatus(rocksdb::Status::InvalidArgument(
          "active expire services are unavailable"));
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    std::unique_ptr<ModuleWriteBatch> write_batch =
        services_->storage().CreateWriteBatch();
    const ModuleKeyspace expires_keyspace = CoreExpireIndexKeyspace();

    long long deleted = 0;
    long long stale = 0;
    for (const auto& candidate : candidates_) {
      rocksdb::Status status =
          write_batch->Delete(expires_keyspace, candidate.index_key);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }

      if (!candidate.decoded) {
        ++stale;
        continue;
      }

      KeyLookup lookup;
      status = key_service_->Lookup(snapshot.get(), candidate.key, &lookup);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      if (!lookup.found ||
          lookup.metadata.expire_at_ms != candidate.expire_at_ms ||
          lookup.state != KeyLifecycleState::kExpired) {
        ++stale;
        continue;
      }

      status = core_module_->DeleteExpiredWholeKey(snapshot.get(),
                                                  write_batch.get(),
                                                  candidate.key, lookup);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      ++deleted;
    }

    rocksdb::Status status = write_batch->Commit();
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    services_->metrics().IncrementCounter("active_expire.deleted",
                                          static_cast<uint64_t>(deleted));
    services_->metrics().IncrementCounter("active_expire.stale",
                                          static_cast<uint64_t>(stale));
    return MakeInteger(deleted);
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  CoreModule* core_module_ = nullptr;
  std::vector<ExpireCandidate> candidates_;
};

class PingCmd : public Cmd {
 public:
  explicit PingCmd(const CmdRegistration& registration)
      : Cmd(registration.name, registration.flags) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (input.has_key || !input.args.empty()) {
      return rocksdb::Status::InvalidArgument("PING takes no arguments");
    }
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override { return MakeSimpleString("PONG"); }
};

class TypeCmd : public Cmd {
 public:
  TypeCmd(const CmdRegistration& registration, ModuleServices* services,
          const CoreKeyService* key_service)
      : Cmd(registration.name, registration.flags),
        services_(services),
        key_service_(key_service) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument("TYPE takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (services_ == nullptr || key_service_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("core key service is unavailable"));
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    KeyLookup lookup;
    rocksdb::Status status = key_service_->Lookup(snapshot.get(), key_, &lookup);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!IsLive(lookup)) {
      return MakeBulkString("none");
    }
    return MakeBulkString(key_service_->ObjectTypeName(lookup.metadata.type));
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  std::string key_;
};

class ExistsCmd : public Cmd {
 public:
  ExistsCmd(const CmdRegistration& registration, ModuleServices* services,
            const CoreKeyService* key_service)
      : Cmd(registration.name, registration.flags),
        services_(services),
        key_service_(key_service) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    keys_ = CollectKeys(input);
    SetRouteKeys(keys_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (services_ == nullptr || key_service_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("core key service is unavailable"));
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    long long exists = 0;
    for (const auto& key : keys_) {
      KeyLookup lookup;
      rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      if (IsLive(lookup)) {
        ++exists;
      }
    }
    return MakeInteger(exists);
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  std::vector<std::string> keys_;
};

class DelCmd : public Cmd {
 public:
  DelCmd(const CmdRegistration& registration, ModuleServices* services,
         const CoreKeyService* key_service,
         WholeKeyDeleteRegistry* delete_registry)
      : Cmd(registration.name, registration.flags),
        services_(services),
        key_service_(key_service),
        delete_registry_(delete_registry) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    keys_ = CollectKeys(input);
    SetRouteKeys(keys_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (services_ == nullptr || key_service_ == nullptr ||
        delete_registry_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("core delete services are unavailable"));
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    std::unique_ptr<ModuleWriteBatch> write_batch =
        services_->storage().CreateWriteBatch();
    std::unordered_set<std::string> processed;
    processed.reserve(keys_.size());

    long long deleted = 0;
    for (const auto& key : keys_) {
      if (!processed.insert(key).second) {
        continue;
      }

      KeyLookup lookup;
      rocksdb::Status status = key_service_->Lookup(snapshot.get(), key, &lookup);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      if (!IsLive(lookup)) {
        continue;
      }

      status = DeleteLiveKey(delete_registry_, snapshot.get(),
                             write_batch.get(), key, lookup);
      if (!status.ok()) {
        return MakeStatus(std::move(status));
      }
      ++deleted;
    }

    if (deleted == 0) {
      return MakeInteger(0);
    }

    rocksdb::Status status = write_batch->Commit();
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(deleted);
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  WholeKeyDeleteRegistry* delete_registry_ = nullptr;
  std::vector<std::string> keys_;
};

class ExpireCmd : public Cmd {
 public:
  ExpireCmd(const CmdRegistration& registration, ModuleServices* services,
            const CoreKeyService* key_service,
            WholeKeyDeleteRegistry* delete_registry)
      : Cmd(registration.name, registration.flags),
        services_(services),
        key_service_(key_service),
        delete_registry_(delete_registry) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (input.args.size() != 1) {
      return rocksdb::Status::InvalidArgument("EXPIRE requires seconds");
    }
    if (!ParseInt64(input.args[0], &ttl_seconds_)) {
      return rocksdb::Status::InvalidArgument(
          "EXPIRE requires integer seconds");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (services_ == nullptr || key_service_ == nullptr ||
        delete_registry_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("core expire services are unavailable"));
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    KeyLookup lookup;
    rocksdb::Status status = key_service_->Lookup(snapshot.get(), key_, &lookup);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!IsLive(lookup)) {
      return MakeInteger(0);
    }

    std::unique_ptr<ModuleWriteBatch> write_batch =
        services_->storage().CreateWriteBatch();
    if (ttl_seconds_ <= 0) {
      status = DeleteLiveKey(delete_registry_, snapshot.get(),
                             write_batch.get(), key_, lookup);
    } else {
      KeyMetadata metadata = lookup.metadata;
      metadata.expire_at_ms =
          ComputeExpireAtMs(key_service_->CurrentTimeMs(), ttl_seconds_);
      status =
          key_service_->PutMetadata(write_batch.get(), key_, lookup, metadata);
    }
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }

    status = write_batch->Commit();
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(1);
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  WholeKeyDeleteRegistry* delete_registry_ = nullptr;
  std::string key_;
  int64_t ttl_seconds_ = 0;
};

class TtlCmd : public Cmd {
 public:
  TtlCmd(const CmdRegistration& registration, ModuleServices* services,
         const CoreKeyService* key_service, bool return_millis)
      : Cmd(registration.name, registration.flags),
        services_(services),
        key_service_(key_service),
        return_millis_(return_millis) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(Name() +
                                              " takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (services_ == nullptr || key_service_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("core key service is unavailable"));
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    KeyLookup lookup;
    rocksdb::Status status = key_service_->Lookup(snapshot.get(), key_, &lookup);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }

    const int64_t ttl_ms = key_service_->GetRemainingTtlMs(lookup);
    if (return_millis_ || ttl_ms < 0) {
      return MakeInteger(ttl_ms);
    }
    return MakeInteger(ttl_ms / 1000);
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  bool return_millis_ = false;
  std::string key_;
};

class PersistCmd : public Cmd {
 public:
  PersistCmd(const CmdRegistration& registration, ModuleServices* services,
             const CoreKeyService* key_service)
      : Cmd(registration.name, registration.flags),
        services_(services),
        key_service_(key_service) {}

 private:
  rocksdb::Status DoInitial(const CmdInput& input) override {
    if (!input.has_key) {
      return rocksdb::Status::InvalidArgument("missing key");
    }
    if (!input.args.empty()) {
      return rocksdb::Status::InvalidArgument(
          "PERSIST takes no extra arguments");
    }
    key_ = input.key;
    SetRouteKey(key_);
    return rocksdb::Status::OK();
  }

  CommandResponse Do() override {
    if (services_ == nullptr || key_service_ == nullptr) {
      return MakeStatus(
          rocksdb::Status::InvalidArgument("core key service is unavailable"));
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    KeyLookup lookup;
    rocksdb::Status status = key_service_->Lookup(snapshot.get(), key_, &lookup);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    if (!IsLive(lookup) || lookup.metadata.expire_at_ms == 0) {
      return MakeInteger(0);
    }

    KeyMetadata metadata = lookup.metadata;
    metadata.expire_at_ms = 0;
    std::unique_ptr<ModuleWriteBatch> write_batch =
        services_->storage().CreateWriteBatch();
    status =
        key_service_->PutMetadata(write_batch.get(), key_, lookup, metadata);
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    status = write_batch->Commit();
    if (!status.ok()) {
      return MakeStatus(std::move(status));
    }
    return MakeInteger(1);
  }

  ModuleServices* services_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  std::string key_;
};

}  // namespace

class CoreModule::ActiveExpireManager {
 public:
  ActiveExpireManager(CoreModule* owner, const CoreKeyService* key_service,
                      ActiveExpireOptions options)
      : owner_(owner),
        key_service_(key_service),
        options_(NormalizeOptions(options)) {}

  ~ActiveExpireManager() { Stop(); }

  ActiveExpireManager(const ActiveExpireManager&) = delete;
  ActiveExpireManager& operator=(const ActiveExpireManager&) = delete;

  rocksdb::Status Start(ModuleServices* services) {
    if (!options_.enabled) {
      return rocksdb::Status::OK();
    }
    if (services == nullptr || key_service_ == nullptr || owner_ == nullptr) {
      return rocksdb::Status::InvalidArgument(
          "active expire services are unavailable");
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (started_) {
      return rocksdb::Status::InvalidArgument(
          "active expire manager already started");
    }
    services_ = services;
    stopping_ = false;
    started_ = true;
    try {
      thread_ = std::thread([this]() { Run(); });
    } catch (const std::system_error& error) {
      started_ = false;
      services_ = nullptr;
      return rocksdb::Status::Aborted(error.what());
    }
    return rocksdb::Status::OK();
  }

  void Stop() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!started_) {
        return;
      }
      stopping_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) {
      thread_.join();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    started_ = false;
    stopping_ = false;
    services_ = nullptr;
  }

 private:
  static ActiveExpireOptions NormalizeOptions(ActiveExpireOptions options) {
    if (options.interval_ms == 0) {
      options.interval_ms = 1;
    }
    if (options.batch_size == 0) {
      options.batch_size = 1;
    }
    if (options.backfill_batch_size == 0) {
      options.backfill_batch_size = 1;
    }
    return options;
  }

  bool ShouldStop() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stopping_;
  }

  void IncrementMetric(const std::string& name, uint64_t delta = 1) {
    if (services_ != nullptr) {
      services_->metrics().IncrementCounter(name, delta);
    }
  }

  void Run() {
    BackfillExpireIndex();
    while (true) {
      {
        std::unique_lock<std::mutex> lock(mutex_);
        if (cv_.wait_for(lock,
                         std::chrono::milliseconds(options_.interval_ms),
                         [this]() { return stopping_; })) {
          break;
        }
      }
      if (ShouldStop()) {
        break;
      }
      RunCycle();
    }
  }

  void BackfillExpireIndex() {
    if (services_ == nullptr || ShouldStop()) {
      return;
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    std::unique_ptr<ModuleWriteBatch> write_batch =
        services_->storage().CreateWriteBatch();
    const ModuleKeyspace expires_keyspace = CoreExpireIndexKeyspace();
    size_t pending = 0;
    uint64_t backfilled = 0;
    rocksdb::Status commit_status;

    rocksdb::Status status = snapshot->ScanPrefix(
        StorageColumnFamily::kMeta, KeyCodec::MetaKeyPrefix(),
        [&](const rocksdb::Slice& encoded_key,
            const rocksdb::Slice& encoded_value) {
          if (ShouldStop()) {
            return false;
          }

          std::string user_key;
          KeyMetadata metadata;
          if (!KeyCodec::DecodeMetaKey(encoded_key, &user_key) ||
              !DefaultCoreKeyService::DecodeMetadataValue(encoded_value,
                                                          &metadata)) {
            IncrementMetric("active_expire.errors");
            return true;
          }
          if (!HasUserExpireAt(metadata.expire_at_ms)) {
            return true;
          }

          commit_status = write_batch->Put(
              expires_keyspace,
              DefaultCoreKeyService::EncodeExpireIndexKey(
                  metadata.expire_at_ms, user_key),
              "");
          if (!commit_status.ok()) {
            IncrementMetric("active_expire.errors");
            return false;
          }

          ++pending;
          ++backfilled;
          if (pending < options_.backfill_batch_size) {
            return true;
          }

          commit_status = write_batch->Commit();
          if (!commit_status.ok()) {
            IncrementMetric("active_expire.errors");
            return false;
          }
          write_batch = services_->storage().CreateWriteBatch();
          pending = 0;
          return true;
        });
    if (!status.ok()) {
      IncrementMetric("active_expire.errors");
      return;
    }
    if (!commit_status.ok()) {
      return;
    }
    if (pending > 0) {
      status = write_batch->Commit();
      if (!status.ok()) {
        IncrementMetric("active_expire.errors");
        return;
      }
    }
    if (backfilled > 0) {
      IncrementMetric("active_expire.backfilled", backfilled);
    }
  }

  void RunCycle() {
    if (services_ == nullptr || ShouldStop()) {
      return;
    }

    IncrementMetric("active_expire.cycles");
    std::vector<ExpireCandidate> candidates = CollectDueCandidates();
    if (candidates.empty()) {
      return;
    }
    IncrementMetric("active_expire.candidates",
                    static_cast<uint64_t>(candidates.size()));
    SubmitAndWait(std::move(candidates));
  }

  std::vector<ExpireCandidate> CollectDueCandidates() {
    std::vector<ExpireCandidate> candidates;
    if (services_ == nullptr) {
      return candidates;
    }

    std::unique_ptr<ModuleSnapshot> snapshot = services_->snapshot().Create();
    const ModuleKeyspace expires_keyspace = CoreExpireIndexKeyspace();
    std::unique_ptr<ModuleIterator> iter =
        snapshot->NewIterator(expires_keyspace);
    const uint64_t now_ms = key_service_->CurrentTimeMs();
    for (iter->Seek(""); iter->Valid() && candidates.size() < options_.batch_size;
         iter->Next()) {
      std::string index_key(iter->key().data(), iter->key().size());
      uint64_t expire_at_ms = 0;
      std::string key;
      const bool decoded = DefaultCoreKeyService::DecodeExpireIndexKey(
          rocksdb::Slice(index_key), &expire_at_ms, &key);
      if (decoded && expire_at_ms > now_ms) {
        break;
      }

      ExpireCandidate candidate;
      candidate.index_key = std::move(index_key);
      candidate.key = std::move(key);
      candidate.expire_at_ms = expire_at_ms;
      candidate.decoded = decoded;
      candidates.push_back(std::move(candidate));
    }
    rocksdb::Status status = iter->status();
    if (!status.ok()) {
      IncrementMetric("active_expire.errors");
      candidates.clear();
    }
    return candidates;
  }

  void SubmitAndWait(std::vector<ExpireCandidate> candidates) {
    auto cmd = std::make_unique<ActiveExpireDeleteCmd>(
        services_, key_service_, owner_, std::move(candidates));
    rocksdb::Status status = cmd->Init(CmdInput{});
    if (!status.ok()) {
      IncrementMetric("active_expire.errors");
      return;
    }

    bool completed = false;
    CommandResponse response;
    status = services_->scheduler().SubmitMaintenance(
        std::move(cmd), [this, &completed, &response](
                            CommandResponse command_response) {
          {
            std::lock_guard<std::mutex> lock(mutex_);
            response = std::move(command_response);
            completed = true;
          }
          cv_.notify_all();
        });
    if (status.IsBusy()) {
      IncrementMetric("active_expire.scheduler_busy");
      return;
    }
    if (!status.ok()) {
      IncrementMetric("active_expire.errors");
      return;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&completed]() { return completed; });
    lock.unlock();
    if (!response.status.ok()) {
      IncrementMetric("active_expire.errors");
    }
  }

  CoreModule* owner_ = nullptr;
  const CoreKeyService* key_service_ = nullptr;
  ActiveExpireOptions options_;
  ModuleServices* services_ = nullptr;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::thread thread_;
  bool started_ = false;
  bool stopping_ = false;
};

CoreModule::CoreModule(TimeSource time_source)
    : CoreModule(std::move(time_source), ActiveExpireOptions{}) {}

CoreModule::CoreModule(TimeSource time_source,
                       ActiveExpireOptions active_expire_options)
    : key_service_(std::move(time_source)),
      active_expire_options_(active_expire_options) {}

CoreModule::~CoreModule() = default;

rocksdb::Status CoreModule::OnLoad(ModuleServices& services) {
  rocksdb::Status status = services.exports().Publish<CoreKeyService>(
      kCoreKeyServiceExportName, &key_service_);
  if (!status.ok()) {
    return status;
  }
  status = services.exports().Publish<WholeKeyDeleteRegistry>(
      kWholeKeyDeleteRegistryExportName,
      static_cast<WholeKeyDeleteRegistry*>(this));
  if (!status.ok()) {
    return status;
  }

  ModuleServices* services_ptr = &services;
  status = services.command_registry().Register(
      {"PING", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [](const CmdRegistration& registration) {
         return std::make_unique<PingCmd>(registration);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"TYPE", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<TypeCmd>(registration, services_ptr,
                                          &key_service_);
       }});
  if (!status.ok()) {
    return status;
  }
  services.metrics().IncrementCounter("commands.registered");

  status = services.command_registry().Register(
      {"EXISTS", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin,
       "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<ExistsCmd>(registration, services_ptr,
                                            &key_service_);
       }});
  if (status.ok()) {
    services.metrics().IncrementCounter("commands.registered");
  }

  status = services.command_registry().Register(
      {"EXPIRE", CmdFlags::kWrite | CmdFlags::kSlow,
       CommandSource::kBuiltin, "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<ExpireCmd>(registration, services_ptr,
                                            &key_service_, this);
       }});
  if (status.ok()) {
    services.metrics().IncrementCounter("commands.registered");
  }

  status = services.command_registry().Register(
      {"TTL", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<TtlCmd>(registration, services_ptr,
                                         &key_service_, false);
       }});
  if (status.ok()) {
    services.metrics().IncrementCounter("commands.registered");
  }

  status = services.command_registry().Register(
      {"PTTL", CmdFlags::kRead | CmdFlags::kFast, CommandSource::kBuiltin, "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<TtlCmd>(registration, services_ptr,
                                         &key_service_, true);
       }});
  if (status.ok()) {
    services.metrics().IncrementCounter("commands.registered");
  }

  status = services.command_registry().Register(
      {"PERSIST", CmdFlags::kWrite | CmdFlags::kFast,
       CommandSource::kBuiltin, "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<PersistCmd>(registration, services_ptr,
                                             &key_service_);
       }});
  if (status.ok()) {
    services.metrics().IncrementCounter("commands.registered");
  }

  status = services.command_registry().Register(
      {"DEL", CmdFlags::kWrite | CmdFlags::kSlow, CommandSource::kBuiltin, "",
       [this, services_ptr](const CmdRegistration& registration) {
         return std::make_unique<DelCmd>(registration, services_ptr, &key_service_,
                                         this);
       }});
  if (status.ok()) {
    services.metrics().IncrementCounter("commands.registered");
  }
  return status;
}

rocksdb::Status CoreModule::OnStart(ModuleServices& services) {
  started_ = true;
  services.metrics().SetCounter("worker_count",
                                services.scheduler().worker_count());
  return rocksdb::Status::OK();
}

rocksdb::Status CoreModule::OnAfterStart(ModuleServices& services) {
  if (active_expire_options_.enabled) {
    active_expire_manager_ = std::make_unique<ActiveExpireManager>(
        this, &key_service_, active_expire_options_);
    rocksdb::Status status = active_expire_manager_->Start(&services);
    if (!status.ok()) {
      active_expire_manager_.reset();
      started_ = false;
      return status;
    }
  }
  return rocksdb::Status::OK();
}

void CoreModule::OnPrepareStop(ModuleServices& /*services*/) {
  if (active_expire_manager_ != nullptr) {
    active_expire_manager_->Stop();
  }
}

void CoreModule::OnStop(ModuleServices& /*services*/) {
  if (active_expire_manager_ != nullptr) {
    active_expire_manager_->Stop();
    active_expire_manager_.reset();
  }
  delete_handlers_.clear();
  started_ = false;
}

rocksdb::Status CoreModule::RegisterHandler(WholeKeyDeleteHandler* handler) {
  if (handler == nullptr) {
    return rocksdb::Status::InvalidArgument("whole-key delete handler is required");
  }

  auto [it, inserted] =
      delete_handlers_.emplace(handler->HandledType(), handler);
  if (!inserted) {
    return rocksdb::Status::InvalidArgument(
        "whole-key delete handler already registered");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status CoreModule::DeleteWholeKey(ModuleSnapshot* snapshot,
                                           ModuleWriteBatch* write_batch,
                                           const std::string& key,
                                           const KeyLookup& lookup) {
  if (!lookup.exists) {
    return rocksdb::Status::OK();
  }

  WholeKeyDeleteHandler* handler = FindHandler(lookup.metadata.type);
  if (handler == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "DEL is unsupported for key type");
  }
  return handler->DeleteWholeKey(snapshot, write_batch, key, lookup);
}

rocksdb::Status CoreModule::DeleteExpiredWholeKey(ModuleSnapshot* snapshot,
                                                  ModuleWriteBatch* write_batch,
                                                  const std::string& key,
                                                  const KeyLookup& lookup) {
  if (lookup.state != KeyLifecycleState::kExpired) {
    return rocksdb::Status::OK();
  }
  KeyLookup deletable_lookup = lookup;
  deletable_lookup.state = KeyLifecycleState::kLive;
  deletable_lookup.expired = false;
  deletable_lookup.exists = true;
  return DeleteWholeKey(snapshot, write_batch, key, deletable_lookup);
}

WholeKeyDeleteHandler* CoreModule::FindHandler(ObjectType type) const {
  auto it = delete_handlers_.find(type);
  if (it == delete_handlers_.end()) {
    return nullptr;
  }
  return it->second;
}

}  // namespace minikv
