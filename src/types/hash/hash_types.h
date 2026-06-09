#pragma once

#include <string>

namespace minikv {

struct FieldValue {
  std::string field;
  std::string value;
};

struct FieldLookup {
  bool found = false;
  std::string value;
};

}  // namespace minikv
