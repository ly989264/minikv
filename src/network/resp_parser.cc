#include "network/resp_parser.h"

#include <cstdlib>

namespace minikv {

namespace {

bool ReadLine(const std::string& buffer, size_t start, size_t* end) {
  size_t pos = buffer.find("\r\n", start);
  if (pos == std::string::npos) {
    return false;
  }
  *end = pos;
  return true;
}

}  // namespace

bool RespParser::Parse(std::string* buffer, std::vector<std::string>* parts,
                       std::string* error) {
  parts->clear();
  error->clear();
  size_t pos = 0;
  if (buffer->empty()) {
    return false;
  }
  if (buffer->at(0) != '*') {
    *error = "expected RESP array";
    buffer->clear();
    return true;
  }
  if (!ParseArray(*buffer, &pos, parts, error)) {
    return false;
  }
  if (!error->empty()) {
    buffer->clear();
  } else {
    buffer->erase(0, pos);
  }
  return true;
}

bool RespParser::ParseArray(const std::string& buffer, size_t* pos,
                            std::vector<std::string>* parts,
                            std::string* error) {
  ++(*pos);
  size_t count = 0;
  if (!ParseNumber(buffer, pos, &count, error)) {
    return false;
  }
  if (!error->empty()) {
    return true;
  }
  for (size_t i = 0; i < count; ++i) {
    std::string part;
    if (!ParseBulkString(buffer, pos, &part, error)) {
      return false;
    }
    if (!error->empty()) {
      return true;
    }
    parts->push_back(std::move(part));
  }
  return true;
}

bool RespParser::ParseBulkString(const std::string& buffer, size_t* pos,
                                 std::string* out, std::string* error) {
  if (*pos >= buffer.size()) {
    return false;
  }
  if (buffer[*pos] != '$') {
    *error = "expected bulk string";
    return true;
  }
  ++(*pos);
  size_t len = 0;
  if (!ParseNumber(buffer, pos, &len, error)) {
    return false;
  }
  if (!error->empty()) {
    return true;
  }
  if (*pos + len + 2 > buffer.size()) {
    return false;
  }
  out->assign(buffer.data() + *pos, len);
  *pos += len;
  if (buffer.compare(*pos, 2, "\r\n") != 0) {
    *error = "bulk string missing CRLF";
    return true;
  }
  *pos += 2;
  return true;
}

bool RespParser::ParseNumber(const std::string& buffer, size_t* pos,
                             size_t* value, std::string* error) {
  size_t end = 0;
  if (!ReadLine(buffer, *pos, &end)) {
    return false;
  }
  const std::string_view slice(buffer.data() + *pos, end - *pos);
  char* parse_end = nullptr;
  long long parsed = std::strtoll(std::string(slice).c_str(), &parse_end, 10);
  if (parse_end == nullptr || *parse_end != '\0' || parsed < 0) {
    *error = "invalid number";
    return true;
  }
  *value = static_cast<size_t>(parsed);
  *pos = end + 2;
  return true;
}

std::string EncodeSimpleString(const std::string& value) {
  return "+" + value + "\r\n";
}

std::string EncodeError(const std::string& value) {
  return "-" + value + "\r\n";
}

std::string EncodeInteger(long long value) {
  return ":" + std::to_string(value) + "\r\n";
}

std::string EncodeBulkString(const std::string& value) {
  return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}

std::string EncodeArray(const std::vector<std::string>& values) {
  std::string out = "*" + std::to_string(values.size()) + "\r\n";
  for (const auto& value : values) {
    out += EncodeBulkString(value);
  }
  return out;
}

std::string EncodeMap(const std::vector<ReplyNode::MapEntry>& entries) {
  std::string out = "%" + std::to_string(entries.size()) + "\r\n";
  for (const auto& entry : entries) {
    out += EncodeReply(entry.first);
    out += EncodeReply(entry.second);
  }
  return out;
}

std::string EncodeNull() { return "_\r\n"; }

std::string EncodeReply(const ReplyNode& reply) {
  switch (reply.type()) {
    case ReplyNode::Type::kSimpleString:
      return EncodeSimpleString(reply.string());
    case ReplyNode::Type::kError:
      return EncodeError(reply.string());
    case ReplyNode::Type::kInteger:
      return EncodeInteger(reply.integer());
    case ReplyNode::Type::kBulkString:
      return EncodeBulkString(reply.string());
    case ReplyNode::Type::kArray: {
      std::string out = "*" + std::to_string(reply.array().size()) + "\r\n";
      for (const auto& child : reply.array()) {
        out += EncodeReply(child);
      }
      return out;
    }
    case ReplyNode::Type::kMap:
      return EncodeMap(reply.map());
    case ReplyNode::Type::kNull:
      return EncodeNull();
  }
  return EncodeError("ERR unsupported reply type");
}

std::string EncodeResponse(const CommandResponse& response) {
  if (!response.status.ok()) {
    return EncodeError("ERR " + response.status.ToString());
  }
  return EncodeReply(response.reply);
}

}  // namespace minikv
