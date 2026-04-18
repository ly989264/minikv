#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "execution/command/command_types.h"

namespace minikv {

class RespParser {
 public:
  bool Parse(std::string* buffer, std::vector<std::string>* parts,
             std::string* error);

 private:
  bool ParseArray(const std::string& buffer, size_t* pos,
                  std::vector<std::string>* parts, std::string* error);
  bool ParseBulkString(const std::string& buffer, size_t* pos, std::string* out,
                       std::string* error);
  bool ParseNumber(const std::string& buffer, size_t* pos, size_t* value,
                   std::string* error);
};

std::string EncodeSimpleString(const std::string& value);
std::string EncodeError(const std::string& value);
std::string EncodeInteger(long long value);
std::string EncodeBulkString(const std::string& value);
std::string EncodeArray(const std::vector<std::string>& values);
std::string EncodeMap(const std::vector<ReplyNode::MapEntry>& entries);
std::string EncodeNull();
std::string EncodeReply(const ReplyNode& reply);
std::string EncodeResponse(const CommandResponse& response);

}  // namespace minikv
