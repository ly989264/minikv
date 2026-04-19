#include "types/json/json_path.h"

#include <cctype>
#include <utility>

namespace minikv {
namespace {

bool IsIdentifierBoundary(char ch) {
  return ch == '.' || ch == '[' || ch == ']';
}

bool IsIdentifierChar(char ch) {
  return !std::isspace(static_cast<unsigned char>(ch)) &&
         !IsIdentifierBoundary(ch);
}

rocksdb::Status InvalidPathStatus(const std::string& message) {
  return rocksdb::Status::InvalidArgument("invalid JSON path: " + message);
}

class JsonPathParser {
 public:
  explicit JsonPathParser(const std::string& text) : text_(text) {}

  rocksdb::Status Parse(JsonPath* path) {
    if (path == nullptr) {
      return rocksdb::Status::InvalidArgument("JSON path output is required");
    }
    path->text = text_;
    path->steps.clear();
    path->dialect = !text_.empty() && text_[0] == '$'
                        ? JsonPathDialect::kJsonPath
                        : JsonPathDialect::kLegacy;

    if (path->dialect == JsonPathDialect::kJsonPath) {
      pos_ = 1;
      if (text_.size() == 1) {
        return rocksdb::Status::OK();
      }
    } else if (text_ == ".") {
      pos_ = text_.size();
      return rocksdb::Status::OK();
    } else if (!text_.empty() && text_[0] == '.') {
      pos_ = 1;
      if (pos_ == text_.size()) {
        return rocksdb::Status::OK();
      }
    }

    while (pos_ < text_.size()) {
      if (text_[pos_] == '.') {
        if (pos_ + 1 < text_.size() && text_[pos_ + 1] == '.') {
          pos_ += 2;
          rocksdb::Status status = ParseRecursiveStep(path);
          if (!status.ok()) {
            return status;
          }
          continue;
        }
        ++pos_;
        if (pos_ >= text_.size()) {
          return InvalidPathStatus("trailing '.'");
        }
        rocksdb::Status status = ParseDirectStep(path);
        if (!status.ok()) {
          return status;
        }
        continue;
      }
      if (text_[pos_] == '[') {
        rocksdb::Status status = ParseBracketStep(path, false);
        if (!status.ok()) {
          return status;
        }
        continue;
      }
      if (path->dialect == JsonPathDialect::kLegacy) {
        rocksdb::Status status = ParseIdentifierStep(path, false);
        if (!status.ok()) {
          return status;
        }
        continue;
      }
      return InvalidPathStatus("unexpected character");
    }

    return rocksdb::Status::OK();
  }

 private:
  rocksdb::Status ParseDirectStep(JsonPath* path) {
    if (text_[pos_] == '[') {
      return ParseBracketStep(path, false);
    }
    if (text_[pos_] == '*') {
      ++pos_;
      JsonPathStep step;
      step.kind = JsonPathStep::Kind::kWildcard;
      path->steps.push_back(std::move(step));
      return rocksdb::Status::OK();
    }
    return ParseIdentifierStep(path, false);
  }

  rocksdb::Status ParseRecursiveStep(JsonPath* path) {
    if (pos_ >= text_.size()) {
      return InvalidPathStatus("trailing recursive descent");
    }
    if (text_[pos_] == '[') {
      return ParseBracketStep(path, true);
    }
    if (text_[pos_] == '*') {
      ++pos_;
      JsonPathStep step;
      step.kind = JsonPathStep::Kind::kRecursiveWildcard;
      path->steps.push_back(std::move(step));
      return rocksdb::Status::OK();
    }
    return ParseIdentifierStep(path, true);
  }

  rocksdb::Status ParseIdentifierStep(JsonPath* path, bool recursive) {
    const size_t start = pos_;
    while (pos_ < text_.size() && IsIdentifierChar(text_[pos_])) {
      ++pos_;
    }
    if (start == pos_) {
      return InvalidPathStatus("expected identifier");
    }
    JsonPathStep step;
    step.kind = recursive ? JsonPathStep::Kind::kRecursiveField
                          : JsonPathStep::Kind::kField;
    step.field = text_.substr(start, pos_ - start);
    path->steps.push_back(std::move(step));
    return rocksdb::Status::OK();
  }

  rocksdb::Status ParseBracketStep(JsonPath* path, bool recursive) {
    if (pos_ >= text_.size() || text_[pos_] != '[') {
      return InvalidPathStatus("expected '['");
    }
    ++pos_;
    if (pos_ >= text_.size()) {
      return InvalidPathStatus("unterminated bracket expression");
    }

    JsonPathStep step;
    if (text_[pos_] == '*') {
      ++pos_;
      step.kind = recursive ? JsonPathStep::Kind::kRecursiveWildcard
                            : JsonPathStep::Kind::kWildcard;
    } else if (text_[pos_] == '"' || text_[pos_] == '\'') {
      std::string key;
      rocksdb::Status status = ParseQuotedKey(&key);
      if (!status.ok()) {
        return status;
      }
      step.kind = recursive ? JsonPathStep::Kind::kRecursiveField
                            : JsonPathStep::Kind::kField;
      step.field = std::move(key);
    } else {
      rocksdb::Status status = ParseIndex(&step.index);
      if (!status.ok()) {
        return status;
      }
      if (recursive) {
        return InvalidPathStatus("recursive array indexes are unsupported");
      }
      step.kind = JsonPathStep::Kind::kIndex;
    }

    if (pos_ >= text_.size() || text_[pos_] != ']') {
      return InvalidPathStatus("expected ']'");
    }
    ++pos_;
    path->steps.push_back(std::move(step));
    return rocksdb::Status::OK();
  }

  rocksdb::Status ParseQuotedKey(std::string* out) {
    if (out == nullptr) {
      return InvalidPathStatus("quoted key output is required");
    }
    const char quote = text_[pos_++];
    out->clear();
    while (pos_ < text_.size()) {
      const char ch = text_[pos_++];
      if (ch == quote) {
        return rocksdb::Status::OK();
      }
      if (ch != '\\') {
        out->push_back(ch);
        continue;
      }
      if (pos_ >= text_.size()) {
        return InvalidPathStatus("unterminated escape");
      }
      const char escaped = text_[pos_++];
      switch (escaped) {
        case '\\':
        case '\'':
        case '"':
          out->push_back(escaped);
          break;
        case 'n':
          out->push_back('\n');
          break;
        case 'r':
          out->push_back('\r');
          break;
        case 't':
          out->push_back('\t');
          break;
        default:
          return InvalidPathStatus("unsupported escape sequence");
      }
    }
    return InvalidPathStatus("unterminated quoted key");
  }

  rocksdb::Status ParseIndex(int64_t* out) {
    if (out == nullptr) {
      return InvalidPathStatus("index output is required");
    }
    const size_t start = pos_;
    if (text_[pos_] == '-' || text_[pos_] == '+') {
      ++pos_;
    }
    const size_t digits_start = pos_;
    while (pos_ < text_.size() &&
           std::isdigit(static_cast<unsigned char>(text_[pos_]))) {
      ++pos_;
    }
    if (digits_start == pos_) {
      return InvalidPathStatus("expected array index");
    }
    const std::string raw = text_.substr(start, pos_ - start);
    try {
      *out = std::stoll(raw);
    } catch (...) {
      return InvalidPathStatus("invalid array index");
    }
    return rocksdb::Status::OK();
  }

  const std::string& text_;
  size_t pos_ = 0;
};

bool NormalizeIndex(int64_t index, size_t size, size_t* normalized) {
  if (normalized == nullptr) {
    return false;
  }
  int64_t candidate = index;
  if (candidate < 0) {
    candidate = static_cast<int64_t>(size) + candidate;
  }
  if (candidate < 0 || static_cast<size_t>(candidate) >= size) {
    return false;
  }
  *normalized = static_cast<size_t>(candidate);
  return true;
}

JsonResolvedPath PushField(const JsonResolvedPath& base,
                           const std::string& field) {
  JsonResolvedPath out = base;
  JsonResolvedPathSegment segment;
  segment.kind = JsonResolvedPathSegment::Kind::kField;
  segment.field = field;
  out.segments.push_back(std::move(segment));
  return out;
}

JsonResolvedPath PushIndex(const JsonResolvedPath& base, size_t index) {
  JsonResolvedPath out = base;
  JsonResolvedPathSegment segment;
  segment.kind = JsonResolvedPathSegment::Kind::kIndex;
  segment.index = index;
  out.segments.push_back(segment);
  return out;
}

void CollectRecursive(const minijson::Value& node,
                      const JsonPathStep& step,
                      const JsonPath& path,
                      size_t step_index,
                      const JsonResolvedPath& current,
                      std::vector<JsonResolvedPath>* matches);

void CollectMatches(const minijson::Value& node, const JsonPath& path,
                    size_t step_index, const JsonResolvedPath& current,
                    std::vector<JsonResolvedPath>* matches) {
  if (matches == nullptr) {
    return;
  }
  if (step_index == path.steps.size()) {
    matches->push_back(current);
    return;
  }

  const JsonPathStep& step = path.steps[step_index];
  switch (step.kind) {
    case JsonPathStep::Kind::kField:
      if (node.IsObject()) {
        const minijson::Value::ObjectEntry* member = node.FindMember(step.field);
        if (member != nullptr) {
          CollectMatches(member->second, path, step_index + 1,
                         PushField(current, step.field), matches);
        }
      }
      return;
    case JsonPathStep::Kind::kIndex:
      if (node.IsArray()) {
        size_t index = 0;
        if (NormalizeIndex(step.index, node.array_items().size(), &index)) {
          CollectMatches(node.array_items()[index], path, step_index + 1,
                         PushIndex(current, index), matches);
        }
      }
      return;
    case JsonPathStep::Kind::kWildcard:
      if (node.IsObject()) {
        for (const auto& entry : node.object_items()) {
          CollectMatches(entry.second, path, step_index + 1,
                         PushField(current, entry.first), matches);
        }
      } else if (node.IsArray()) {
        for (size_t i = 0; i < node.array_items().size(); ++i) {
          CollectMatches(node.array_items()[i], path, step_index + 1,
                         PushIndex(current, i), matches);
        }
      }
      return;
    case JsonPathStep::Kind::kRecursiveField:
    case JsonPathStep::Kind::kRecursiveWildcard:
      CollectRecursive(node, step, path, step_index, current, matches);
      return;
  }
}

void CollectRecursive(const minijson::Value& node,
                      const JsonPathStep& step,
                      const JsonPath& path,
                      size_t step_index,
                      const JsonResolvedPath& current,
                      std::vector<JsonResolvedPath>* matches) {
  if (node.IsObject()) {
    for (const auto& entry : node.object_items()) {
      JsonResolvedPath child_path = PushField(current, entry.first);
      if (step.kind == JsonPathStep::Kind::kRecursiveWildcard ||
          entry.first == step.field) {
        CollectMatches(entry.second, path, step_index + 1, child_path, matches);
      }
      CollectRecursive(entry.second, step, path, step_index, child_path, matches);
    }
    return;
  }
  if (node.IsArray()) {
    for (size_t i = 0; i < node.array_items().size(); ++i) {
      JsonResolvedPath child_path = PushIndex(current, i);
      if (step.kind == JsonPathStep::Kind::kRecursiveWildcard) {
        CollectMatches(node.array_items()[i], path, step_index + 1, child_path,
                       matches);
      }
      CollectRecursive(node.array_items()[i], step, path, step_index, child_path,
                       matches);
    }
  }
}

}  // namespace

bool JsonPath::is_dynamic() const {
  for (const auto& step : steps) {
    if (step.kind == JsonPathStep::Kind::kWildcard ||
        step.kind == JsonPathStep::Kind::kRecursiveField ||
        step.kind == JsonPathStep::Kind::kRecursiveWildcard) {
      return true;
    }
  }
  return false;
}

rocksdb::Status ParseJsonPath(const std::string& text, JsonPath* path) {
  if (text.empty()) {
    return InvalidPathStatus("path must not be empty");
  }
  JsonPathParser parser(text);
  return parser.Parse(path);
}

void CollectJsonPathMatches(const minijson::Value& root, const JsonPath& path,
                            std::vector<JsonResolvedPath>* matches) {
  if (matches == nullptr) {
    return;
  }
  matches->clear();
  JsonResolvedPath current;
  CollectMatches(root, path, 0, current, matches);
}

bool ResolveJsonPath(const minijson::Value& root, const JsonResolvedPath& path,
                     const minijson::Value** value) {
  if (value == nullptr) {
    return false;
  }
  const minijson::Value* current = &root;
  for (const auto& segment : path.segments) {
    if (segment.kind == JsonResolvedPathSegment::Kind::kField) {
      if (!current->IsObject()) {
        return false;
      }
      const minijson::Value::ObjectEntry* member =
          current->FindMember(segment.field);
      if (member == nullptr) {
        return false;
      }
      current = &member->second;
      continue;
    }
    if (!current->IsArray() || segment.index >= current->array_items().size()) {
      return false;
    }
    current = &current->array_items()[segment.index];
  }
  *value = current;
  return true;
}

bool ResolveMutableJsonPath(minijson::Value* root, const JsonResolvedPath& path,
                            minijson::Value** value) {
  if (root == nullptr || value == nullptr) {
    return false;
  }
  minijson::Value* current = root;
  for (const auto& segment : path.segments) {
    if (segment.kind == JsonResolvedPathSegment::Kind::kField) {
      if (!current->IsObject()) {
        return false;
      }
      minijson::Value::ObjectEntry* member = current->FindMember(segment.field);
      if (member == nullptr) {
        return false;
      }
      current = &member->second;
      continue;
    }
    if (!current->IsArray() || segment.index >= current->array_items().size()) {
      return false;
    }
    current = &current->array_items()[segment.index];
  }
  *value = current;
  return true;
}

bool ResolveMutableJsonParent(minijson::Value* root,
                              const JsonResolvedPath& path,
                              minijson::Value** parent,
                              JsonResolvedPathSegment* leaf) {
  if (root == nullptr || parent == nullptr || leaf == nullptr) {
    return false;
  }
  if (path.segments.empty()) {
    return false;
  }
  JsonResolvedPath parent_path;
  parent_path.segments.assign(path.segments.begin(), path.segments.end() - 1);
  *leaf = path.segments.back();
  return ResolveMutableJsonPath(root, parent_path, parent);
}

bool SplitJsonPathForObjectMemberCreate(const JsonPath& path,
                                        JsonPath* parent_path,
                                        std::string* member) {
  if (parent_path == nullptr || member == nullptr || path.steps.empty()) {
    return false;
  }
  if (path.is_dynamic()) {
    return false;
  }

  const JsonPathStep& last = path.steps.back();
  if (last.kind != JsonPathStep::Kind::kField) {
    return false;
  }

  *member = last.field;
  *parent_path = path;
  parent_path->steps.pop_back();
  return true;
}

}  // namespace minikv
