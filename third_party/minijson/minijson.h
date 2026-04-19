#pragma once

#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace minijson {

class Value {
 public:
  enum class Type : uint8_t {
    kNull = 0,
    kBool = 1,
    kNumber = 2,
    kString = 3,
    kArray = 4,
    kObject = 5,
  };

  struct NumberData {
    std::string text;
    long double value = 0.0L;
    bool integral = false;
  };

  using Array = std::vector<Value>;
  using ObjectEntry = std::pair<std::string, Value>;
  using Object = std::vector<ObjectEntry>;

  Value() = default;

  static Value Null() { return Value(Type::kNull); }

  static Value Bool(bool value) {
    Value out(Type::kBool);
    out.bool_value_ = value;
    return out;
  }

  static Value Number(std::string text, long double value, bool integral) {
    Value out(Type::kNumber);
    out.number_.text = std::move(text);
    out.number_.value = value;
    out.number_.integral = integral;
    return out;
  }

  static Value String(std::string value) {
    Value out(Type::kString);
    out.string_value_ = std::move(value);
    return out;
  }

  static Value ArrayValue(Array value) {
    Value out(Type::kArray);
    out.array_value_ = std::move(value);
    return out;
  }

  static Value ObjectValue(Object value) {
    Value out(Type::kObject);
    out.object_value_ = std::move(value);
    return out;
  }

  Type type() const { return type_; }
  bool IsNull() const { return type_ == Type::kNull; }
  bool IsBool() const { return type_ == Type::kBool; }
  bool IsNumber() const { return type_ == Type::kNumber; }
  bool IsString() const { return type_ == Type::kString; }
  bool IsArray() const { return type_ == Type::kArray; }
  bool IsObject() const { return type_ == Type::kObject; }

  bool bool_value() const { return bool_value_; }
  const NumberData& number() const { return number_; }
  NumberData& number() { return number_; }
  const std::string& string_value() const { return string_value_; }
  std::string& string_value() { return string_value_; }
  const Array& array_items() const { return array_value_; }
  Array& array_items() { return array_value_; }
  const Object& object_items() const { return object_value_; }
  Object& object_items() { return object_value_; }

  const ObjectEntry* FindMember(const std::string& key) const {
    for (const auto& entry : object_value_) {
      if (entry.first == key) {
        return &entry;
      }
    }
    return nullptr;
  }

  ObjectEntry* FindMember(const std::string& key) {
    for (auto& entry : object_value_) {
      if (entry.first == key) {
        return &entry;
      }
    }
    return nullptr;
  }

  bool EraseMember(const std::string& key) {
    for (auto it = object_value_.begin(); it != object_value_.end(); ++it) {
      if (it->first == key) {
        object_value_.erase(it);
        return true;
      }
    }
    return false;
  }

 private:
  explicit Value(Type type) : type_(type) {}

  Type type_ = Type::kNull;
  bool bool_value_ = false;
  NumberData number_;
  std::string string_value_;
  Array array_value_;
  Object object_value_;
};

struct SerializeOptions {
  std::string indent;
  std::string newline;
  std::string space;
};

namespace detail {

inline void AppendUtf8(std::string* out, uint32_t code_point) {
  if (out == nullptr) {
    return;
  }
  if (code_point <= 0x7f) {
    out->push_back(static_cast<char>(code_point));
    return;
  }
  if (code_point <= 0x7ff) {
    out->push_back(static_cast<char>(0xc0 | ((code_point >> 6) & 0x1f)));
    out->push_back(static_cast<char>(0x80 | (code_point & 0x3f)));
    return;
  }
  if (code_point <= 0xffff) {
    out->push_back(static_cast<char>(0xe0 | ((code_point >> 12) & 0x0f)));
    out->push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | (code_point & 0x3f)));
    return;
  }
  out->push_back(static_cast<char>(0xf0 | ((code_point >> 18) & 0x07)));
  out->push_back(static_cast<char>(0x80 | ((code_point >> 12) & 0x3f)));
  out->push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3f)));
  out->push_back(static_cast<char>(0x80 | (code_point & 0x3f)));
}

inline void AppendIndent(std::string* out, const SerializeOptions& options,
                         size_t depth) {
  if (out == nullptr || options.indent.empty()) {
    return;
  }
  for (size_t i = 0; i < depth; ++i) {
    out->append(options.indent);
  }
}

inline std::string EscapeString(const std::string& input) {
  std::string out;
  out.reserve(input.size() + 2);
  out.push_back('"');
  for (unsigned char ch : input) {
    switch (ch) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\b':
        out += "\\b";
        break;
      case '\f':
        out += "\\f";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        if (ch < 0x20) {
          static const char kHex[] = "0123456789abcdef";
          out += "\\u00";
          out.push_back(kHex[(ch >> 4) & 0x0f]);
          out.push_back(kHex[ch & 0x0f]);
        } else {
          out.push_back(static_cast<char>(ch));
        }
        break;
    }
  }
  out.push_back('"');
  return out;
}

inline std::string FormatNumber(long double value) {
  if (!std::isfinite(static_cast<double>(value))) {
    return std::string();
  }
  if (std::fabsl(value) < 0.5e-18L) {
    return "0";
  }

  std::ostringstream stream;
  stream << std::setprecision(std::numeric_limits<long double>::digits10 + 1)
         << std::defaultfloat << value;
  std::string out = stream.str();

  if (out.find_first_of(".eE") == std::string::npos) {
    return out;
  }

  size_t exponent_pos = out.find_first_of("eE");
  std::string mantissa = exponent_pos == std::string::npos
                             ? out
                             : out.substr(0, exponent_pos);
  std::string exponent = exponent_pos == std::string::npos
                             ? std::string()
                             : out.substr(exponent_pos);

  if (mantissa.find('.') != std::string::npos) {
    while (!mantissa.empty() && mantissa.back() == '0') {
      mantissa.pop_back();
    }
    if (!mantissa.empty() && mantissa.back() == '.') {
      mantissa.pop_back();
    }
  }
  if (mantissa == "-0") {
    mantissa = "0";
  }
  return mantissa + exponent;
}

class Parser {
 public:
  explicit Parser(const std::string& input) : input_(input) {}

  bool Parse(Value* out, std::string* error) {
    if (out == nullptr) {
      if (error != nullptr) {
        *error = "output value is required";
      }
      return false;
    }
    SkipWhitespace();
    if (!ParseValue(out)) {
      if (error != nullptr && error->empty()) {
        *error = error_;
      }
      return false;
    }
    SkipWhitespace();
    if (pos_ != input_.size()) {
      if (error != nullptr) {
        *error = "unexpected trailing characters";
      }
      return false;
    }
    if (error != nullptr) {
      error->clear();
    }
    return true;
  }

 private:
  bool ParseValue(Value* out) {
    if (!HasMore()) {
      return SetError("unexpected end of input");
    }
    switch (Peek()) {
      case 'n':
        return ParseKeyword("null", Value::Null(), out);
      case 't':
        return ParseKeyword("true", Value::Bool(true), out);
      case 'f':
        return ParseKeyword("false", Value::Bool(false), out);
      case '"':
        return ParseStringValue(out);
      case '[':
        return ParseArray(out);
      case '{':
        return ParseObject(out);
      default:
        if (Peek() == '-' || std::isdigit(static_cast<unsigned char>(Peek()))) {
          return ParseNumberValue(out);
        }
        return SetError("unexpected character");
    }
  }

  bool ParseKeyword(const char* text, const Value& value, Value* out) {
    const std::string expected(text);
    if (input_.compare(pos_, expected.size(), expected) != 0) {
      return SetError("invalid literal");
    }
    pos_ += expected.size();
    *out = value;
    return true;
  }

  bool ParseStringValue(Value* out) {
    std::string value;
    if (!ParseString(&value)) {
      return false;
    }
    *out = Value::String(std::move(value));
    return true;
  }

  bool ParseString(std::string* out) {
    if (out == nullptr) {
      return SetError("string output is required");
    }
    if (!Consume('"')) {
      return SetError("expected string");
    }

    out->clear();
    while (HasMore()) {
      const char ch = Next();
      if (ch == '"') {
        return true;
      }
      if (static_cast<unsigned char>(ch) < 0x20) {
        return SetError("control characters must be escaped");
      }
      if (ch != '\\') {
        out->push_back(ch);
        continue;
      }
      if (!HasMore()) {
        return SetError("unterminated escape sequence");
      }
      const char escaped = Next();
      switch (escaped) {
        case '"':
        case '\\':
        case '/':
          out->push_back(escaped);
          break;
        case 'b':
          out->push_back('\b');
          break;
        case 'f':
          out->push_back('\f');
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
        case 'u': {
          uint32_t code_point = 0;
          if (!ParseHexQuad(&code_point)) {
            return false;
          }
          if (code_point >= 0xd800 && code_point <= 0xdbff) {
            if (!Consume('\\') || !Consume('u')) {
              return SetError("invalid unicode surrogate pair");
            }
            uint32_t low = 0;
            if (!ParseHexQuad(&low)) {
              return false;
            }
            if (low < 0xdc00 || low > 0xdfff) {
              return SetError("invalid unicode surrogate pair");
            }
            code_point =
                0x10000 + (((code_point - 0xd800) << 10) | (low - 0xdc00));
          } else if (code_point >= 0xdc00 && code_point <= 0xdfff) {
            return SetError("invalid unicode surrogate pair");
          }
          AppendUtf8(out, code_point);
          break;
        }
        default:
          return SetError("invalid escape sequence");
      }
    }
    return SetError("unterminated string");
  }

  bool ParseHexQuad(uint32_t* out) {
    if (out == nullptr) {
      return SetError("unicode output is required");
    }
    if (pos_ + 4 > input_.size()) {
      return SetError("incomplete unicode escape");
    }
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
      const char ch = input_[pos_++];
      value <<= 4;
      if (ch >= '0' && ch <= '9') {
        value |= static_cast<uint32_t>(ch - '0');
      } else if (ch >= 'a' && ch <= 'f') {
        value |= static_cast<uint32_t>(10 + ch - 'a');
      } else if (ch >= 'A' && ch <= 'F') {
        value |= static_cast<uint32_t>(10 + ch - 'A');
      } else {
        return SetError("invalid unicode escape");
      }
    }
    *out = value;
    return true;
  }

  bool ParseNumberValue(Value* out) {
    size_t start = pos_;
    if (Peek() == '-') {
      ++pos_;
      if (!HasMore()) {
        return SetError("invalid number");
      }
    }

    if (Peek() == '0') {
      ++pos_;
      if (HasMore() && std::isdigit(static_cast<unsigned char>(Peek()))) {
        return SetError("invalid number");
      }
    } else if (std::isdigit(static_cast<unsigned char>(Peek()))) {
      while (HasMore() && std::isdigit(static_cast<unsigned char>(Peek()))) {
        ++pos_;
      }
    } else {
      return SetError("invalid number");
    }

    bool integral = true;
    if (HasMore() && Peek() == '.') {
      integral = false;
      ++pos_;
      if (!HasMore() || !std::isdigit(static_cast<unsigned char>(Peek()))) {
        return SetError("invalid number");
      }
      while (HasMore() && std::isdigit(static_cast<unsigned char>(Peek()))) {
        ++pos_;
      }
    }

    if (HasMore() && (Peek() == 'e' || Peek() == 'E')) {
      integral = false;
      ++pos_;
      if (HasMore() && (Peek() == '+' || Peek() == '-')) {
        ++pos_;
      }
      if (!HasMore() || !std::isdigit(static_cast<unsigned char>(Peek()))) {
        return SetError("invalid number");
      }
      while (HasMore() && std::isdigit(static_cast<unsigned char>(Peek()))) {
        ++pos_;
      }
    }

    const std::string text = input_.substr(start, pos_ - start);
    errno = 0;
    char* parse_end = nullptr;
    const long double parsed = std::strtold(text.c_str(), &parse_end);
    if (parse_end == nullptr || *parse_end != '\0' || errno == ERANGE ||
        !std::isfinite(static_cast<double>(parsed))) {
      return SetError("invalid number");
    }
    *out = Value::Number(text, parsed, integral);
    return true;
  }

  bool ParseArray(Value* out) {
    if (!Consume('[')) {
      return SetError("expected array");
    }
    Value::Array values;
    SkipWhitespace();
    if (Consume(']')) {
      *out = Value::ArrayValue(std::move(values));
      return true;
    }

    while (true) {
      SkipWhitespace();
      Value element;
      if (!ParseValue(&element)) {
        return false;
      }
      values.push_back(std::move(element));
      SkipWhitespace();
      if (Consume(']')) {
        *out = Value::ArrayValue(std::move(values));
        return true;
      }
      if (!Consume(',')) {
        return SetError("expected ',' or ']'");
      }
      SkipWhitespace();
    }
  }

  bool ParseObject(Value* out) {
    if (!Consume('{')) {
      return SetError("expected object");
    }
    Value::Object values;
    SkipWhitespace();
    if (Consume('}')) {
      *out = Value::ObjectValue(std::move(values));
      return true;
    }

    while (true) {
      SkipWhitespace();
      std::string key;
      if (!ParseString(&key)) {
        return false;
      }
      SkipWhitespace();
      if (!Consume(':')) {
        return SetError("expected ':'");
      }
      SkipWhitespace();
      Value value;
      if (!ParseValue(&value)) {
        return false;
      }
      values.emplace_back(std::move(key), std::move(value));
      SkipWhitespace();
      if (Consume('}')) {
        *out = Value::ObjectValue(std::move(values));
        return true;
      }
      if (!Consume(',')) {
        return SetError("expected ',' or '}'");
      }
      SkipWhitespace();
    }
  }

  void SkipWhitespace() {
    while (HasMore() &&
           std::isspace(static_cast<unsigned char>(input_[pos_]))) {
      ++pos_;
    }
  }

  bool HasMore() const { return pos_ < input_.size(); }
  char Peek() const { return input_[pos_]; }
  char Next() { return input_[pos_++]; }

  bool Consume(char expected) {
    if (!HasMore() || input_[pos_] != expected) {
      return false;
    }
    ++pos_;
    return true;
  }

  bool SetError(const std::string& error) {
    error_ = error;
    return false;
  }

  const std::string& input_;
  size_t pos_ = 0;
  std::string error_;
};

inline void SerializeValue(const Value& value, std::string* out,
                           const SerializeOptions& options, size_t depth) {
  const bool pretty = !options.indent.empty() || !options.newline.empty() ||
                      !options.space.empty();
  switch (value.type()) {
    case Value::Type::kNull:
      out->append("null");
      return;
    case Value::Type::kBool:
      out->append(value.bool_value() ? "true" : "false");
      return;
    case Value::Type::kNumber:
      out->append(value.number().text);
      return;
    case Value::Type::kString:
      out->append(EscapeString(value.string_value()));
      return;
    case Value::Type::kArray: {
      out->push_back('[');
      const auto& items = value.array_items();
      if (items.empty()) {
        out->push_back(']');
        return;
      }
      if (pretty && !options.newline.empty()) {
        out->append(options.newline);
      }
      for (size_t i = 0; i < items.size(); ++i) {
        if (pretty && !options.newline.empty()) {
          AppendIndent(out, options, depth + 1);
        }
        SerializeValue(items[i], out, options, depth + 1);
        if (i + 1 < items.size()) {
          out->push_back(',');
          if (pretty && !options.newline.empty()) {
            out->append(options.newline);
          } else if (pretty && !options.space.empty()) {
            out->append(options.space);
          }
        }
      }
      if (pretty && !options.newline.empty()) {
        out->append(options.newline);
        AppendIndent(out, options, depth);
      }
      out->push_back(']');
      return;
    }
    case Value::Type::kObject: {
      out->push_back('{');
      const auto& items = value.object_items();
      if (items.empty()) {
        out->push_back('}');
        return;
      }
      if (pretty && !options.newline.empty()) {
        out->append(options.newline);
      }
      for (size_t i = 0; i < items.size(); ++i) {
        if (pretty && !options.newline.empty()) {
          AppendIndent(out, options, depth + 1);
        }
        out->append(EscapeString(items[i].first));
        out->push_back(':');
        if (pretty && !options.space.empty()) {
          out->append(options.space);
        }
        SerializeValue(items[i].second, out, options, depth + 1);
        if (i + 1 < items.size()) {
          out->push_back(',');
          if (pretty && !options.newline.empty()) {
            out->append(options.newline);
          } else if (pretty && !options.space.empty()) {
            out->append(options.space);
          }
        }
      }
      if (pretty && !options.newline.empty()) {
        out->append(options.newline);
        AppendIndent(out, options, depth);
      }
      out->push_back('}');
      return;
    }
  }
}

}  // namespace detail

inline bool Parse(const std::string& input, Value* out, std::string* error) {
  detail::Parser parser(input);
  return parser.Parse(out, error);
}

inline std::string Serialize(const Value& value,
                             const SerializeOptions& options = {}) {
  std::string out;
  detail::SerializeValue(value, &out, options, 0);
  return out;
}

inline std::string FormatNumber(long double value) {
  return detail::FormatNumber(value);
}

inline Value MakeNumber(long double value) {
  const std::string text = detail::FormatNumber(value);
  const bool integral =
      text.find_first_of(".eE") == std::string::npos;
  return Value::Number(text, value, integral);
}

}  // namespace minijson
