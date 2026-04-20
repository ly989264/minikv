#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "execution/reply/reply_node.h"
#include "runtime/module/module.h"
#include "runtime/module/module_services.h"
#include "types/stream/stream_module.h"

namespace minikv {

class CoreKeyService;

namespace stream_internal {

struct StreamId {
  uint64_t ms = 0;
  uint64_t seq = 0;
};

bool operator==(const StreamId& lhs, const StreamId& rhs);
bool operator<(const StreamId& lhs, const StreamId& rhs);
bool operator>(const StreamId& lhs, const StreamId& rhs);
bool operator<=(const StreamId& lhs, const StreamId& rhs);

bool ParseUint64(const std::string& input, uint64_t* value);
std::string NormalizeKeyword(const std::string& input);
bool ParseStreamId(const std::string& input, StreamId* id);
bool ParseRangeId(const std::string& input, StreamId* id);
std::string FormatStreamId(const StreamId& id);

std::string EncodeStreamEntryLocalKey(const std::string& key, uint64_t version,
                                      const StreamId& id);
std::string EncodeStreamStateLocalKey(const std::string& key, uint64_t version);

bool DecodeStreamEntryId(const rocksdb::Slice& encoded_key,
                         const rocksdb::Slice& prefix, StreamId* id);

std::string EncodeStreamEntryValue(const std::vector<StreamFieldValue>& values);
bool DecodeStreamEntryValue(const rocksdb::Slice& value,
                            std::vector<StreamFieldValue>* out);

rocksdb::Status RequireStreamEncoding(const KeyLookup& lookup);
KeyMetadata BuildStreamMetadata(const CoreKeyService* key_service,
                                const KeyLookup& lookup);
KeyMetadata BuildStreamTombstoneMetadata(const CoreKeyService* key_service,
                                         const KeyLookup& lookup);

rocksdb::Status LoadStreamState(ModuleSnapshot* snapshot,
                                const ModuleKeyspace& state_keyspace,
                                const std::string& key, uint64_t version,
                                StreamId* id);
rocksdb::Status PutStreamState(ModuleWriteBatch* write_batch,
                               const ModuleKeyspace& state_keyspace,
                               const std::string& key, uint64_t version,
                               const StreamId& id);
rocksdb::Status DeleteStreamState(ModuleWriteBatch* write_batch,
                                  const ModuleKeyspace& state_keyspace,
                                  const std::string& key, uint64_t version);
rocksdb::Status ResolveLastStreamId(ModuleSnapshot* snapshot,
                                    const ModuleKeyspace& entries_keyspace,
                                    const ModuleKeyspace& state_keyspace,
                                    const std::string& key, uint64_t version,
                                    StreamId* id, bool* found);
rocksdb::Status CollectEntryIds(ModuleSnapshot* snapshot,
                                const ModuleKeyspace& entries_keyspace,
                                const std::string& key, uint64_t version,
                                size_t limit, std::vector<StreamId>* out);
rocksdb::Status CollectEntriesInRange(ModuleSnapshot* snapshot,
                                      const ModuleKeyspace& entries_keyspace,
                                      const std::string& key, uint64_t version,
                                      const StreamId& start,
                                      const StreamId& end,
                                      std::vector<StreamEntry>* out);
rocksdb::Status CollectEntriesAfterId(ModuleSnapshot* snapshot,
                                      const ModuleKeyspace& entries_keyspace,
                                      const std::string& key, uint64_t version,
                                      const StreamId& last_seen,
                                      std::vector<StreamEntry>* out);
std::vector<StreamId> DeduplicateIds(const std::vector<StreamId>& ids);

ReplyNode MakeStreamEntryReply(const StreamEntry& entry);
ReplyNode MakeStreamReadResultReply(const StreamReadResult& result);

}  // namespace stream_internal
}  // namespace minikv
