#include "codec/key_codec.h"

#include <cstring>

namespace minikv {

namespace {

constexpr char kMetaPrefix[] = "m|";
constexpr char kHashPrefix[] = "h|";

}  // namespace

void KeyCodec::AppendUint32(std::string* out, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

void KeyCodec::AppendUint64(std::string* out, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

uint32_t KeyCodec::DecodeUint32(const char* input) {
  uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value = (value << 8) | static_cast<unsigned char>(input[i]);
  }
  return value;
}

uint64_t KeyCodec::DecodeUint64(const char* input) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value = (value << 8) | static_cast<unsigned char>(input[i]);
  }
  return value;
}

std::string KeyCodec::EncodeMetaKey(const std::string& user_key) {
  std::string out(kMetaPrefix);
  AppendUint32(&out, static_cast<uint32_t>(user_key.size()));
  out.append(user_key);
  return out;
}

std::string KeyCodec::EncodeHashDataPrefix(const std::string& user_key,
                                           uint64_t version) {
  std::string out(kHashPrefix);
  AppendUint32(&out, static_cast<uint32_t>(user_key.size()));
  out.append(user_key);
  AppendUint64(&out, version);
  return out;
}

std::string KeyCodec::EncodeHashDataKey(const std::string& user_key,
                                        uint64_t version,
                                        const std::string& field) {
  std::string out = EncodeHashDataPrefix(user_key, version);
  out.append(field);
  return out;
}

bool KeyCodec::StartsWith(const rocksdb::Slice& value,
                          const rocksdb::Slice& prefix) {
  return value.size() >= prefix.size() &&
         std::memcmp(value.data(), prefix.data(), prefix.size()) == 0;
}

bool KeyCodec::ExtractFieldFromHashDataKey(const rocksdb::Slice& encoded_key,
                                           const rocksdb::Slice& prefix,
                                           std::string* field) {
  if (!StartsWith(encoded_key, prefix)) {
    return false;
  }
  field->assign(encoded_key.data() + prefix.size(),
                encoded_key.size() - prefix.size());
  return true;
}

}  // namespace minikv
