// Wire format and codec (verified against a real 5.5.1 cluster):
//   - wire layout: totalLength(4) | headerLength(high 8 bits carry the serialize type, 4) | header | body
//   - header(JSON): RemotingSerializable JSON encoding; decoding also accepts 5.x replies that write language as an enum name string
//   - header(ROCKETMQ): RocketMQSerializable private binary encoding
#include "rocketmq/remoting/protocol/remoting_command.h"

#include <atomic>
#include <cctype>
#include <cstdlib>

#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/headers.h"

namespace rocketmq {

namespace {

constexpr const char* SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
constexpr const char* SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
constexpr const char* REMOTING_VERSION_KEY = "rocketmq.remoting.version";

// Protocol `MQVersion.CURRENT_VERSION` (= `Version.V5_5_1.ordinal()`, the local 5.5.1 cluster).
// The broker uses the client version carried in heartbeats/requests to decide whether it may call the client back:
// `AdminBrokerProcessor#callConsumer` (307) answers
// "The Consumer <x> Version <0> too low to finish" straight away below `V3_1_8_SNAPSHOT` (ordinal 62); `Broker2Client#getConsumeStatus`
// (223->221) answers "the client does not support this
// feature. version=V3_0_7_SNAPSHOT" below `V3_0_7_SNAPSHOT` (ordinal 28), and the online branch of `resetOffset` skips on the same 28 threshold.
constexpr int32_t CURRENT_VERSION = 515;

std::atomic<int32_t>& opaqueCounter() {
    static std::atomic<int32_t> counter{0};
    return counter;
}

// Global serialize-type config: env ROCKETMQ_SERIALIZE_TYPE / rocketmq.serialize.type,
// "ROCKETMQ" selects the binary encoding, anything else JSON.
uint8_t loadSerializeTypeConfig() {
    const char* v = std::getenv(SERIALIZE_TYPE_ENV);
    if (v == nullptr) v = std::getenv(SERIALIZE_TYPE_PROPERTY);
    if (v == nullptr) return SerializeType::JSON;
    std::string s(v);
    for (auto& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return (s == "ROCKETMQ") ? SerializeType::ROCKETMQ : SerializeType::JSON;
}

uint8_t& serializeTypeConfig() {
    static uint8_t config = loadSerializeTypeConfig();
    return config;
}

int32_t loadVersionConfig() {
    const char* v = std::getenv(REMOTING_VERSION_KEY);
    if (v == nullptr) return CURRENT_VERSION;
    try {
        return std::stoi(v);
    } catch (...) {
        return CURRENT_VERSION;
    }
}

void setCmdVersion(RemotingCommand& cmd) { cmd.version = loadVersionConfig(); }

// language name -> code (accepts 5.x serializing language as an enum name string)
uint8_t languageNameToCode(const std::string& name) {
    std::string up = name;
    for (auto& c : up) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    struct Pair { const char* n; uint8_t c; };
    static const Pair kPairs[] = {
        {"JAVA", LanguageCode::JAVA},   {"CPP", LanguageCode::CPP},
        {"DOTNET", LanguageCode::DOTNET}, {"PYTHON", LanguageCode::PYTHON},
        {"DELPHI", LanguageCode::DELPHI}, {"ERLANG", LanguageCode::ERLANG},
        {"RUBY", LanguageCode::RUBY},   {"OTHER", LanguageCode::OTHER},
        {"HTTP", LanguageCode::HTTP},   {"GO", LanguageCode::GO},
        {"PHP", LanguageCode::PHP},     {"OMS", LanguageCode::OMS},
        {"RUST", LanguageCode::RUST},   {"NODE_JS", LanguageCode::NODE_JS},
    };
    for (const auto& p : kPairs) {
        if (up == p.n) return p.c;
    }
    return LanguageCode::JAVA;
}

}  // namespace

// ---------------------------------------------------------------- Construction and factories

RemotingCommand::RemotingCommand(int32_t code_, std::shared_ptr<CommandCustomHeader> header,
                                 const std::string& remark_, int32_t opaque_, int32_t flag_,
                                 const Bytes& body_)
    : code(code_),
      language(LanguageCode::CPP),
      version(0),
      opaque(opaque_),
      flag(flag_),
      remark(remark_),
      hasRemark(!remark_.empty()),
      customHeader(std::move(header)),
      body(body_),
      hasBody(true),
      serializeTypeCurrentRPC(serializeTypeConfig()) {}

RemotingCommand RemotingCommand::createRequestCommand(int32_t code,
                                                      std::shared_ptr<CommandCustomHeader> header) {
    RemotingCommand cmd;
    cmd.code = code;
    cmd.customHeader = std::move(header);
    cmd.opaque = nextOpaque();
    cmd.serializeTypeCurrentRPC = serializeTypeConfig();
    setCmdVersion(cmd);
    return cmd;
}

RemotingCommand RemotingCommand::createResponseCommandWithHeader(
    int32_t code, std::shared_ptr<CommandCustomHeader> header) {
    RemotingCommand cmd;
    cmd.code = code;
    cmd.markResponseType();
    cmd.customHeader = std::move(header);
    cmd.serializeTypeCurrentRPC = serializeTypeConfig();
    setCmdVersion(cmd);
    return cmd;
}

RemotingCommand RemotingCommand::createResponseCommand(int32_t code, const std::string& remark_) {
    RemotingCommand cmd;
    cmd.code = code;
    cmd.remark = remark_;
    cmd.hasRemark = !remark_.empty();
    cmd.markResponseType();
    cmd.serializeTypeCurrentRPC = serializeTypeConfig();
    setCmdVersion(cmd);
    return cmd;
}

RemotingCommand RemotingCommand::buildErrorResponse(int32_t code, const std::string& remark_) {
    return createResponseCommand(code, remark_);
}

void RemotingCommand::setSerializeTypeConfig(uint8_t type) { serializeTypeConfig() = type; }

uint8_t RemotingCommand::getSerializeTypeConfig() { return serializeTypeConfig(); }

int32_t RemotingCommand::nextOpaque() { return opaqueCounter().fetch_add(1); }

// ---------------------------------------------------------------- Header expansion

void RemotingCommand::makeCustomHeaderToNet() {
    if (customHeader) {
        for (const auto& kv : customHeader->toExtFields()) {
            extFields[kv.first] = kv.second;
        }
    }
}

JsonValue RemotingCommand::toJsonObject() const {
    JsonValue obj = JsonValue::makeObject();
    obj.set("code", JsonValue::makeInt(code));
    obj.set("language", JsonValue::makeInt(static_cast<int64_t>(language)));
    obj.set("version", JsonValue::makeInt(version));
    obj.set("opaque", JsonValue::makeInt(opaque));
    obj.set("flag", JsonValue::makeInt(flag));
    if (hasRemark && !remark.empty()) {
        obj.set("remark", JsonValue::makeString(remark));
    }
    if (!extFields.empty()) {
        JsonValue ext = JsonValue::makeObject();
        for (const auto& kv : extFields) {
            ext.set(kv.first, JsonValue::makeString(kv.second));
        }
        obj.set("extFields", ext);
    }
    return obj;
}

// ---------------------------------------------------------------- Encoding and decoding

Bytes RemotingCommand::headerEncode() {
    makeCustomHeaderToNet();
    if (serializeTypeCurrentRPC == SerializeType::ROCKETMQ) {
        ProtocolHeaderFields fields;
        fields.code = code;
        fields.language = language;
        fields.version = version;
        fields.opaque = opaque;
        fields.flag = flag;
        fields.remark = remark;
        fields.hasRemark = hasRemark && !remark.empty();
        fields.extFields = extFields;
        return RocketMQSerializable::rocketMQProtocolEncode(fields);
    }
    return RemotingSerializable::encode(toJsonObject());
}

Bytes RemotingCommand::encode() {
    int32_t length = 4;
    Bytes headerData = headerEncode();
    length += static_cast<int32_t>(headerData.size());
    // Body presence: hasBody set explicitly, or a non-empty body (matching the protocol, which omits body when it is null)
    bool withBody = hasBody || !body.empty();
    if (withBody) {
        length += static_cast<int32_t>(body.size());
    }
    Bytes out;
    putInt32(out, length);
    putInt32(out, markProtocolType(static_cast<int32_t>(headerData.size()),
                                   serializeTypeCurrentRPC));
    out += headerData;
    if (withBody) {
        out += body;
    }
    return out;
}

Bytes RemotingCommand::encodeHeader(int32_t bodyLength) {
    int32_t length = 4;
    Bytes headerData = headerEncode();
    length += static_cast<int32_t>(headerData.size()) + bodyLength;
    Bytes out;
    putInt32(out, length);
    putInt32(out, markProtocolType(static_cast<int32_t>(headerData.size()),
                                   serializeTypeCurrentRPC));
    out += headerData;
    return out;
}

bool RemotingCommand::tryDecode(const Bytes& data, RemotingCommand& out, std::string* err) {
    auto fail = [&](const char* msg) {
        if (err) *err = msg;
        return false;
    };
    if (data.size() < 8) return fail("decode error, data too short");

    int32_t totalLength = ByteReader::getInt32At(data, 0);
    size_t offset = 4;
    if (totalLength > static_cast<int32_t>(data.size()) - 4) {
        return fail("decode error, bad total length");
    }
    int32_t oriHeaderLen = ByteReader::getInt32At(data, 4);
    offset += 4;
    int32_t headerLength = getHeaderLength(oriHeaderLen);
    if (headerLength < 0 || static_cast<size_t>(headerLength) > data.size() - offset) {
        return fail("decode error, bad header length");
    }
    uint8_t protocolType = getProtocolType(oriHeaderLen);

    Bytes headerData = data.substr(offset, static_cast<size_t>(headerLength));
    offset += static_cast<size_t>(headerLength);

    RemotingCommand cmd;
    if (protocolType == SerializeType::ROCKETMQ) {
        ProtocolHeaderFields fields;
        if (!RocketMQSerializable::rocketMQProtocolDecode(headerData, fields)) {
            return fail("decode error, rocketmq protocol decode failed");
        }
        cmd.code = fields.code;
        cmd.remark = fields.remark;
        cmd.hasRemark = fields.hasRemark;
        cmd.language = fields.language;
        cmd.version = fields.version;
        cmd.opaque = fields.opaque;
        cmd.flag = fields.flag;
        cmd.extFields = fields.extFields;
    } else {
        JsonValue obj;
        if (!jsonParse(headerData, obj, nullptr)) {
            return fail("decode error, json parse failed");
        }
        cmd.code = static_cast<int32_t>(obj.get("code").intValue(0));
        cmd.remark = obj.get("remark").stringValue();
        cmd.hasRemark = !cmd.remark.empty();
        cmd.flag = static_cast<int32_t>(obj.get("flag").intValue(0));
        const JsonValue& lang = obj.get("language");
        if (lang.isString()) {
            cmd.language = languageNameToCode(lang.stringValue());
        } else {
            cmd.language = static_cast<uint8_t>(lang.intValue(LanguageCode::JAVA));
        }
        cmd.version = static_cast<int32_t>(obj.get("version").intValue(0));
        cmd.opaque = static_cast<int32_t>(obj.get("opaque").intValue(-1));
        const JsonValue* ext = obj.find("extFields");
        if (ext && ext->isObject()) {
            for (const auto& kv : ext->objectItems()) {
                cmd.extFields[kv.first] = kv.second.stringValue();
            }
        }
    }
    cmd.serializeTypeCurrentRPC = protocolType;

    if (data.size() > offset) {
        cmd.body = data.substr(offset);
        cmd.hasBody = true;
    } else {
        cmd.body.clear();
        cmd.hasBody = false;
    }
    out = cmd;
    return true;
}

RemotingCommand RemotingCommand::decode(const Bytes& data) {
    RemotingCommand cmd;
    std::string err;
    if (!tryDecode(data, cmd, &err)) {
        throw RemotingCommandException(err.empty() ? "decode error" : err);
    }
    return cmd;
}

void RemotingCommand::decodeCommandCustomHeader(CommandCustomHeader& header) const {
    header.fromExtFields(extFields);
}

std::string RemotingCommand::getExtField(const std::string& key) const {
    auto it = extFields.find(key);
    return it == extFields.end() ? std::string() : it->second;
}

std::string RemotingCommand::toString() const {
    std::string ext;
    for (const auto& kv : extFields) {
        if (!ext.empty()) ext += ",";
        ext += kv.first + "=" + kv.second;
    }
    return std::string("RemotingCommand [code=") + std::to_string(code) +
           ", language=" + std::to_string(language) +
           ", version=" + std::to_string(version) +
           ", opaque=" + std::to_string(opaque) +
           ", flag=" + std::to_string(flag) +
           ", remark=" + remark +
           ", extFields={" + ext + "}" +
           ", serializeTypeCurrentRPC=" + std::to_string(serializeTypeCurrentRPC) + "]";
}

}  // namespace rocketmq
