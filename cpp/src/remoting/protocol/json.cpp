// Minimal JSON parsing / generation implementation (no third-party dependencies).
//
// Behaviour contract:
//   - Objects keep insertion order: backed by vector<pair>, and set() **keeps the original position** when overwriting an existing key (as the online parser does);
//   - numbers distinguish int64 / double: integer fields are emitted as-is so a long timestamp is not written as a float;
//   - strings are emitted as raw UTF-8 (not escaped to \uXXXX), while parsing accepts \uXXXX including surrogate pairs;
//   - parsing tolerates fastjson2's non-standard output:
//       * **unquoted numeric keys** such as {0:"x"};
//       * **objects used as map keys** such as {{"k":"v"}:{...}} (invalid JSON, always present in the admin-side offsetTable) --
//         the key keeps the raw JSON text and is restored by parseMessageQueueKey();
//       * NaN / Infinity / -Infinity;
//       * trailing commas.
#include "rocketmq/remoting/protocol/json.h"

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace rocketmq {

namespace {

// ---------------------------------------------------------------- Serialisation

void appendUtf8Escaped(std::string& out, const std::string& s) {
    out.push_back('"');
    for (unsigned char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    // Emit UTF-8 bytes as-is, no \uXXXX escaping
                    out.push_back(static_cast<char>(c));
                }
        }
    }
    out.push_back('"');
}

void appendDouble(std::string& out, double v) {
    // std::isnan / std::isinf cannot be used: MinGW's math.h defines both names as **function-like macros**
    // (`#define isnan(x) ...`, active in C++ mode too), which breaks the
    // `using ::isnan;` declaration in <cmath> and turns `std::isnan` into an "unresolved using
    // declaration". <limits> constants are used instead for the equivalent test, avoiding the macros entirely.
    const double inf = std::numeric_limits<double>::infinity();
    if (v != v || v == inf || v == -inf) {  // NaN / ±Inf
        out += "null";  // JSON has no NaN/Inf
        return;
    }
    char buf[40];
    // %.17g guarantees double round-trip precision; then ensure a decimal/exponent point so it is not parsed as an integer
    std::snprintf(buf, sizeof(buf), "%.17g", v);
    std::string s(buf);
    if (s.find_first_of(".eE") == std::string::npos) {
        s += ".0";
    }
    out += s;
}

}  // namespace

// ---------------------------------------------------------------- JsonValue factories

JsonValue JsonValue::makeBool(bool v) {
    JsonValue j;
    j.type_ = Type::Bool;
    j.bool_ = v;
    return j;
}

JsonValue JsonValue::makeInt(int64_t v) {
    JsonValue j;
    j.type_ = Type::Number;
    j.intIsInt64_ = true;
    j.intVal_ = v;
    j.num_ = static_cast<double>(v);
    return j;
}

JsonValue JsonValue::makeDouble(double v) {
    JsonValue j;
    j.type_ = Type::Number;
    j.intIsInt64_ = false;
    j.num_ = v;
    j.intVal_ = static_cast<int64_t>(v);
    return j;
}

JsonValue JsonValue::makeString(const std::string& v) {
    JsonValue j;
    j.type_ = Type::String;
    j.str_ = v;
    return j;
}

JsonValue JsonValue::makeArray() {
    JsonValue j;
    j.type_ = Type::Array;
    return j;
}

JsonValue JsonValue::makeObject() {
    JsonValue j;
    j.type_ = Type::Object;
    return j;
}

// ---------------------------------------------------------------- array

const JsonValue& JsonValue::at(size_t i) const {
    static const JsonValue kNull{};
    if (i >= arr_.size()) return kNull;
    return arr_[i];
}

void JsonValue::pushArray(const JsonValue& v) {
    type_ = Type::Array;
    arr_.push_back(v);
}

// ---------------------------------------------------------------- object

bool JsonValue::contains(const std::string& key) const {
    for (const auto& kv : obj_) {
        if (kv.first == key) return true;
    }
    return false;
}

const JsonValue* JsonValue::find(const std::string& key) const {
    for (const auto& kv : obj_) {
        if (kv.first == key) return &kv.second;
    }
    return nullptr;
}

const JsonValue& JsonValue::get(const std::string& key) const {
    static const JsonValue kNull{};
    const JsonValue* p = find(key);
    return p ? *p : kNull;
}

void JsonValue::set(const std::string& key, const JsonValue& v) {
    type_ = Type::Object;
    for (auto& kv : obj_) {
        if (kv.first == key) {
            kv.second = v;  // overwrite in place: keeps the original position
            return;
        }
    }
    obj_.emplace_back(key, v);
}

bool JsonValue::tryGetString(const std::string& key, std::string& out) const {
    const JsonValue* p = find(key);
    if (!p || !p->isString()) return false;
    out = p->stringValue();
    return true;
}

bool JsonValue::tryGetInt(const std::string& key, int64_t& out) const {
    const JsonValue* p = find(key);
    if (!p || !p->isNumber()) return false;
    out = p->intValue();
    return true;
}

bool JsonValue::tryGetBool(const std::string& key, bool& out) const {
    const JsonValue* p = find(key);
    if (!p || !p->isBool()) return false;
    out = p->boolValue();
    return true;
}

// ---------------------------------------------------------------- dump

std::string JsonValue::dump() const {
    std::string out;
    dumpImpl(out);
    return out;
}

void JsonValue::dumpImpl(std::string& out) const {
    switch (type_) {
        case Type::Null: out += "null"; break;
        case Type::Bool: out += bool_ ? "true" : "false"; break;
        case Type::Number:
            if (intIsInt64_) {
                out += std::to_string(intVal_);
            } else if (static_cast<double>(intVal_) == num_) {
                out += std::to_string(intVal_);
            } else {
                appendDouble(out, num_);
            }
            break;
        case Type::String: appendUtf8Escaped(out, str_); break;
        case Type::Array: {
            out.push_back('[');
            for (size_t i = 0; i < arr_.size(); ++i) {
                if (i) out.push_back(',');
                arr_[i].dumpImpl(out);
            }
            out.push_back(']');
            break;
        }
        case Type::Object: {
            out.push_back('{');
            bool first = true;
            for (const auto& kv : obj_) {
                if (!first) out.push_back(',');
                first = false;
                appendUtf8Escaped(out, kv.first);
                out.push_back(':');
                kv.second.dumpImpl(out);
            }
            out.push_back('}');
            break;
        }
    }
}

// ---------------------------------------------------------------- Parsing

namespace {

class Parser {
public:
    Parser(const std::string& text) : s_(text) {}

    bool parse(JsonValue& out) {
        skipWs();
        if (!parseValue(out)) return false;
        skipWs();
        return pos_ == s_.size();
    }

private:
    const std::string& s_;
    size_t pos_ = 0;
    std::string err_;

    void skipWs() {
        while (pos_ < s_.size()) {
            char c = s_[pos_];
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                ++pos_;
            } else {
                break;
            }
        }
    }

    bool eof() const { return pos_ >= s_.size(); }
    char peek() const { return pos_ < s_.size() ? s_[pos_] : '\0'; }

    bool parseValue(JsonValue& out) {
        skipWs();
        if (eof()) return false;
        char c = peek();
        switch (c) {
            case '{': return parseObject(out);
            case '[': return parseArray(out);
            case '"': {
                std::string str;
                if (!parseString(str)) return false;
                out = JsonValue::makeString(str);
                return true;
            }
            case 't': case 'f': return parseLiteral(out);
            case 'n': return parseNull(out);
            // fastjson2 writes these three non-standard literals when WriteMapNullValue=false / floating point is out of range;
            // standard JSON does not have them. Built from <limits> constants to dodge
            // MinGW's function-like isnan/isinf macros (see the appendDouble comment).
            case 'N': return parseNonFinite(out, "NaN", std::numeric_limits<double>::quiet_NaN());
            case 'I': return parseNonFinite(out, "Infinity", std::numeric_limits<double>::infinity());
            case '-':
                if (s_.compare(pos_, 9, "-Infinity") == 0) {
                    pos_ += 9;
                    out = JsonValue::makeDouble(-std::numeric_limits<double>::infinity());
                    return true;
                }
                return parseNumber(out);
            default: return parseNumber(out);
        }
    }

    bool parseNonFinite(JsonValue& out, const char* token, double value) {
        size_t len = std::strlen(token);
        if (s_.compare(pos_, len, token) != 0) return false;
        pos_ += len;
        out = JsonValue::makeDouble(value);
        return true;
    }

    bool parseLiteral(JsonValue& out) {
        if (s_.compare(pos_, 4, "true") == 0) {
            pos_ += 4;
            out = JsonValue::makeBool(true);
            return true;
        }
        if (s_.compare(pos_, 5, "false") == 0) {
            pos_ += 5;
            out = JsonValue::makeBool(false);
            return true;
        }
        return false;
    }

    bool parseNull(JsonValue& out) {
        if (s_.compare(pos_, 4, "null") == 0) {
            pos_ += 4;
            out = JsonValue::makeNull();
            return true;
        }
        return false;
    }

    bool parseNumber(JsonValue& out) {
        size_t start = pos_;
        while (pos_ < s_.size()) {
            char c = s_[pos_];
            if ((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E') {
                ++pos_;
            } else {
                break;
            }
        }
        if (pos_ == start) return false;
        std::string tok = s_.substr(start, pos_ - start);
        bool isFloat = tok.find_first_of(".eE") != std::string::npos;
        try {
            if (isFloat) {
                out = JsonValue::makeDouble(std::stod(tok));
            } else {
                out = JsonValue::makeInt(static_cast<int64_t>(std::stoll(tok)));
            }
        } catch (...) {
            return false;
        }
        return true;
    }

    // Parse a quoted string; decode \uXXXX to UTF-8 (including surrogate pairs)
    bool parseString(std::string& out) {
        if (peek() != '"') return false;
        ++pos_;
        out.clear();
        while (pos_ < s_.size()) {
            char c = s_[pos_++];
            if (c == '"') return true;
            if (c != '\\') {
                out.push_back(c);
                continue;
            }
            if (eof()) return false;
            char esc = s_[pos_++];
            switch (esc) {
                case '"': out.push_back('"'); break;
                case '\\': out.push_back('\\'); break;
                case '/': out.push_back('/'); break;
                case 'b': out.push_back('\b'); break;
                case 'f': out.push_back('\f'); break;
                case 'n': out.push_back('\n'); break;
                case 'r': out.push_back('\r'); break;
                case 't': out.push_back('\t'); break;
                case 'u': {
                    uint32_t cp = 0;
                    if (!parseHex4(cp)) return false;
                    // Surrogate pair handling
                    if (cp >= 0xD800 && cp <= 0xDBFF && s_.compare(pos_, 2, "\\u") == 0) {
                        size_t save = pos_;
                        pos_ += 2;
                        uint32_t lo = 0;
                        if (parseHex4(lo) && lo >= 0xDC00 && lo <= 0xDFFF) {
                            cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                        } else {
                            pos_ = save;
                        }
                    }
                    appendUtf8(out, cp);
                    break;
                }
                default: return false;
            }
        }
        return false;
    }

    bool parseHex4(uint32_t& cp) {
        if (pos_ + 4 > s_.size()) return false;
        cp = 0;
        for (int i = 0; i < 4; ++i) {
            char c = s_[pos_++];
            cp <<= 4;
            if (c >= '0' && c <= '9') cp |= static_cast<uint32_t>(c - '0');
            else if (c >= 'a' && c <= 'f') cp |= static_cast<uint32_t>(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') cp |= static_cast<uint32_t>(c - 'A' + 10);
            else return false;
        }
        return true;
    }

    static void appendUtf8(std::string& out, uint32_t cp) {
        if (cp <= 0x7F) {
            out.push_back(static_cast<char>(cp));
        } else if (cp <= 0x7FF) {
            out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        } else if (cp <= 0xFFFF) {
            out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        } else {
            out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        }
    }

    // Read key names leniently, covering fastjson2's two non-standard forms:
    //   1) unquoted numeric keys: {"offsetTable":{0:1,1:2}};
    //   2) **an object/array used as a map key** (invalid JSON, always present on the admin side): a MessageQueue is inlined as
    //      {"offsetTable":{{"brokerName":"b","queueId":3,"topic":"t"}:{...}}}.
    // An inlined key keeps the **raw JSON text** as its name; the caller restores it with parseMessageQueueKey().
    bool parseKey(std::string& out) {
        skipWs();
        char c = peek();
        if (c == '"') {
            return parseString(out);
        }
        if (c == '{' || c == '[') {
            size_t start = pos_;
            JsonValue ignored;
            if (!parseValue(ignored)) return false;
            out = s_.substr(start, pos_ - start);
            return true;
        }
        // Unquoted: read until a colon or whitespace (numeric keys / bare literals such as true / false / null)
        size_t start = pos_;
        while (pos_ < s_.size()) {
            char k = s_[pos_];
            if (k == ':' || k == ' ' || k == '\t' || k == '\n' || k == '\r') break;
            ++pos_;
        }
        if (pos_ == start) return false;
        out = s_.substr(start, pos_ - start);
        return true;
    }

    bool parseObject(JsonValue& out) {
        if (peek() != '{') return false;
        ++pos_;
        out = JsonValue::makeObject();
        skipWs();
        if (peek() == '}') { ++pos_; return true; }
        while (true) {
            std::string key;
            if (!parseKey(key)) return false;
            skipWs();
            if (peek() != ':') return false;
            ++pos_;
            JsonValue val;
            if (!parseValue(val)) return false;
            out.set(key, val);
            skipWs();
            char c = peek();
            if (c == ',') {
                ++pos_;
                skipWs();
                if (peek() == '}') { ++pos_; return true; }  // tolerate a trailing comma
                continue;
            }
            if (c == '}') { ++pos_; return true; }
            return false;
        }
    }

    bool parseArray(JsonValue& out) {
        if (peek() != '[') return false;
        ++pos_;
        out = JsonValue::makeArray();
        skipWs();
        if (peek() == ']') { ++pos_; return true; }
        while (true) {
            JsonValue val;
            if (!parseValue(val)) return false;
            out.pushArray(val);
            skipWs();
            char c = peek();
            if (c == ',') {
                ++pos_;
                skipWs();
                if (peek() == ']') { ++pos_; return true; }  // tolerate a trailing comma
                continue;
            }
            if (c == ']') { ++pos_; return true; }
            return false;
        }
    }
};

}  // namespace

bool jsonParse(const std::string& text, JsonValue& out, std::string* err) {
    if (text.empty()) {
        if (err) *err = "empty input";
        return false;
    }
    Parser p(text);
    if (!p.parse(out)) {
        if (err) *err = "json parse error";
        return false;
    }
    return true;
}

std::string jsonDump(const JsonValue& v) { return v.dump(); }

}  // namespace rocketmq
