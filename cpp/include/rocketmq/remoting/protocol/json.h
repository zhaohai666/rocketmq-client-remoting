// 极简 JSON 解析 / 生成（仅满足 RemotingSerializable 的需求，零第三方依赖）。
//
// 之所以自带实现：RocketMQ 的 JSON 序列化只用得到 object / array / string / number /
// bool / null 六种类型，引入第三方库会带来不必要的构建负担。
//
// 约束：
//   - Object 保序（vector + 覆盖式插入），因此 encode -> decode 的键顺序稳定；
//   - 数字区分 int64 与 double：整数字段原样输出，避免 long 型 timestamp 被写成浮点；
//   - 字符串默认按 UTF-8 原样输出（与 Python ensure_ascii=False 一致）。
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
    // 保序的键值对视图（供遍历；顺序与插入/解析顺序一致）
    const Object& objectItems() const { return obj_; }
    bool contains(const std::string& key) const;
    const JsonValue* find(const std::string& key) const;
    // 找不到时返回静态 Null 引用，便于链式取值
    const JsonValue& get(const std::string& key) const;
    void set(const std::string& key, const JsonValue& v);

    // 便捷取值：key 存在且类型匹配才返回 true
    bool tryGetString(const std::string& key, std::string& out) const;
    bool tryGetInt(const std::string& key, int64_t& out) const;
    bool tryGetBool(const std::string& key, bool& out) const;

    std::string dump() const;

private:
    // 递归序列化实现（object 保序遍历需要访问私有存储）
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

// 解析失败返回 false，err 为可空的输出信息
bool jsonParse(const std::string& text, JsonValue& out, std::string* err = nullptr);

// 序列化
std::string jsonDump(const JsonValue& v);

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_JSON_H
