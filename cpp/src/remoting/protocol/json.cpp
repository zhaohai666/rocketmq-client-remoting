// 极简 JSON 解析 / 生成实现（零第三方依赖）。
//
// 与 python/rocketmq/remoting/protocol 的 JSON 行为对齐：
//   - Object 保序：底层用 vector<pair>，set() 覆盖同名键时**保留原位置**（与 Python dict 一致）；
//   - 数字区分 int64 / double：整数字段原样输出，避免 long 型 timestamp 被写成浮点；
//   - 字符串按 UTF-8 原样输出（不解码为 \uXXXX），解析时兼容 \uXXXX 与代理对；
//   - 解析对 fastjson2 的非标准写法宽容（对齐 Python _FastJsonParser）：
//       * {0:"x"} 这类**无引号数字键**；
//       * {{"k":"v"}:{...}} 这类**对象作为 map 键**（非法 JSON，管理端 offsetTable 必现）——
//         键名保留原始 JSON 文本，由 parseMessageQueueKey() 还原；
//       * NaN / Infinity / -Infinity；
//       * 尾随逗号。
#include "rocketmq/remoting/protocol/json.h"

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace rocketmq {

namespace {

// ---------------------------------------------------------------- 序列化

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
                    // UTF-8 字节原样输出（与 Python ensure_ascii=False 一致）
                    out.push_back(static_cast<char>(c));
                }
        }
    }
    out.push_back('"');
}

void appendDouble(std::string& out, double v) {
    // 不能用 std::isnan / std::isinf：MinGW 的 math.h 把这两个名字定义成**函数式宏**
    // （`#define isnan(x) ...`，且 C++ 模式下同样生效），会把 <cmath> 里的
    // `using ::isnan;` 展开成非法代码，导致 `std::isnan` 成为"unresolved using
    // declaration"。这里改用 <limits> 的常量做等价判断，完全绕开宏。
    const double inf = std::numeric_limits<double>::infinity();
    if (v != v || v == inf || v == -inf) {  // NaN / ±Inf
        out += "null";  // JSON 无 NaN/Inf
        return;
    }
    char buf[40];
    // %.17g 保证 double 往返精度；再确保带小数/指数点，避免被当成整数解析
    std::snprintf(buf, sizeof(buf), "%.17g", v);
    std::string s(buf);
    if (s.find_first_of(".eE") == std::string::npos) {
        s += ".0";
    }
    out += s;
}

}  // namespace

// ---------------------------------------------------------------- JsonValue 工厂

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
            kv.second = v;  // 覆盖式插入：保留原位置
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

// ---------------------------------------------------------------- 解析

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
            // fastjson2 在 WriteMapNullValue=false / 浮点越界时会写出这三种非标准字面量，
            // 标准 JSON 没有它们。用 <limits> 常量构造，避开 MinGW 把 isnan/isinf
            // 定义成函数式宏的问题（见 appendDouble 的注释）。
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

    // 解析带引号字符串；\uXXXX 转 UTF-8（含代理对）
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
                    // 代理对处理
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

    // 宽容读取键名，覆盖 fastjson2 的两种非标准写法（对齐 Python _FastJsonParser._key）：
    //   1) 无引号数字键：{"offsetTable":{0:1,1:2}}；
    //   2) **对象/数组作为 map 键**（非法 JSON，管理端必现）：MessageQueue 会被内联成
    //      {"offsetTable":{{"brokerName":"b","queueId":3,"topic":"t"}:{...}}}。
    // 内联键保留**原始 JSON 文本**作为键名，调用方再用 parseMessageQueueKey() 还原。
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
        // 无引号：读到冒号或空白前（数字键 / true / false / null 等裸字面量）
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
                if (peek() == '}') { ++pos_; return true; }  // 容忍尾随逗号
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
                if (peek() == ']') { ++pos_; return true; }  // 容忍尾随逗号
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
