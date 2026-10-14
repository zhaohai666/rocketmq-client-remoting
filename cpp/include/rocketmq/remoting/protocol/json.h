// Minimal JSON parsing / generation (covers only what RemotingSerializable needs; zero third-party dependencies).
//
// The reason for a built-in implementation: RocketMQ JSON serialization only uses the six types object / array /
// string / number / bool / null, and a third-party library would add unnecessary build burden.
//
// Constraints:
//   - Object keeps insertion order (vector plus overwrite-on-set), so the key order is stable across encode -> decode;
//   - numbers distinguish int64 from double: integer fields are printed verbatim, so a long timestamp is not written as a float;
//   - strings are printed as UTF-8 by default (no \uXXXX escaping).
#ifndef ROCKETMQ_REMOTING_PROTOCOL_JSON_H
#define ROCKETMQ_REMOTING_PROTOCOL_JSON_H

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace rocketmq {

class JsonValue {
public:
    enum class Type { Null, Bool, Number, String, Array, Object };
    using Array = std::vector<JsonValue>;
    using Object = std::vector<std::pair<std::string, JsonValue>>;

    JsonValue() : type_(Type::Null) {}

    static JsonValue makeNull() { return JsonValue(); }
    static JsonValue makeBool(bool v);
    static JsonValue makeInt(int64_t v);
    static JsonValue makeDouble(double v);
    static JsonValue makeString(const std::string& v);
    static JsonValue makeArray();
    static JsonValue makeObject();

    Type type() const { return type_; }
    bool isNull() const { return type_ == Type::Null; }
    bool isBool() const { return type_ == Type::Bool; }
    bool isNumber() const { return type_ == Type::Number; }
    bool isString() const { return type_ == Type::String; }
    bool isArray() const { return type_ == Type::Array; }
    bool isObject() const { return type_ == Type::Object; }

    bool boolValue(bool def = false) const {
        return type_ == Type::Bool ? bool_ : (type_ == Type::Number ? num_ != 0 : def);
    }
    int64_t intValue(int64_t def = 0) const {
        if (type_ == Type::Number) return intIsInt64_ ? intVal_ : static_cast<int64_t>(num_);
        return def;
    }
    double doubleValue(double def = 0.0) const {
        if (type_ == Type::Number) return intIsInt64_ ? static_cast<double>(intVal_) : num_;
        return def;
    }
    std::string stringValue(const std::string& def = std::string()) const {
        return type_ == Type::String ? str_ : def;
    }

    // ---- array ----
    size_t size() const { return arr_.size(); }
    const JsonValue& at(size_t i) const;
    void pushArray(const JsonValue& v);

    // ---- object ----
    // Order-preserving key/value view (for traversal; the order matches insertion/parse order)
    const Object& objectItems() const { return obj_; }
    bool contains(const std::string& key) const;
    const JsonValue* find(const std::string& key) const;
    // Returns a reference to a static Null when the key is absent, so lookups can be chained
    const JsonValue& get(const std::string& key) const;
    void set(const std::string& key, const JsonValue& v);

    // Convenience accessors: return true only when the key exists and the type matches
    bool tryGetString(const std::string& key, std::string& out) const;
    bool tryGetInt(const std::string& key, int64_t& out) const;
    bool tryGetBool(const std::string& key, bool& out) const;

    std::string dump() const;

private:
    // Recursive serialization implementation (ordered object traversal needs access to the private storage)
    void dumpImpl(std::string& out) const;

    Type type_ = Type::Null;
    bool bool_ = false;
    double num_ = 0.0;
    int64_t intVal_ = 0;
    bool intIsInt64_ = false;
    std::string str_;
    Array arr_;
    Object obj_;
};

// Returns false on parse failure; err is an optional output message
bool jsonParse(const std::string& text, JsonValue& out, std::string* err = nullptr);

// Serialize to JSON text
std::string jsonDump(const JsonValue& v);

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_JSON_H
