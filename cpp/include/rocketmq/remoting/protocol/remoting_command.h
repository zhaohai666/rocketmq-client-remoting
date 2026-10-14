// Wire format: totalLength(4) | headerLength (top 8 bits carry the serialization type, 4) | header | body
//
//   header(JSON)      : {"code":..,"language":..,"version":..,"opaque":..,"flag":..,
//                        "remark":"..","extFields":{..}}
//   header(ROCKETMQ)  : code(2) language(1) version(2) opaque(4) flag(4)
//                       remark(int+utf8) extFields(int + [key(short+utf8) value(int+utf8)]...)
#ifndef ROCKETMQ_REMOTING_PROTOCOL_REMOTING_COMMAND_H
#define ROCKETMQ_REMOTING_PROTOCOL_REMOTING_COMMAND_H

#include <cstdint>
#include <memory>
#include <string>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/types.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/serialize.h"

namespace rocketmq {

struct CommandCustomHeader;

constexpr int32_t RPC_TYPE = 0;     // 0 -> REQUEST, 1 -> RESPONSE
constexpr int32_t RPC_ONEWAY = 1;

class RemotingCommand {
public:
    int32_t code = 0;
    uint8_t language = LanguageCode::CPP;
    int32_t version = 0;
    int32_t opaque = 0;
    int32_t flag = 0;
    std::string remark;
    bool hasRemark = false;
    PropertyMap extFields;
    std::shared_ptr<CommandCustomHeader> customHeader;
    Bytes body;
    bool hasBody = false;
    uint8_t serializeTypeCurrentRPC = SerializeType::JSON;

    RemotingCommand() = default;
    RemotingCommand(int32_t code, std::shared_ptr<CommandCustomHeader> header,
                    const std::string& remark, int32_t opaque, int32_t flag, const Bytes& body);

    // ---------------- factory methods ----------------
    static RemotingCommand createRequestCommand(int32_t code,
                                                std::shared_ptr<CommandCustomHeader> header = nullptr);
    static RemotingCommand createResponseCommandWithHeader(
        int32_t code, std::shared_ptr<CommandCustomHeader> header = nullptr);
    static RemotingCommand createResponseCommand(int32_t code, const std::string& remark);
    static RemotingCommand buildErrorResponse(int32_t code, const std::string& remark);

    // Whether this RPC globally uses ROCKETMQ binary serialization (controlled by setSerializeTypeConfig)
    static void setSerializeTypeConfig(uint8_t type);
    static uint8_t getSerializeTypeConfig();

    // ---------------- frame utilities ----------------
    // Frame-protocol getProtocolType: take the first byte (here the same as the top 8 bits of the int)
    static uint8_t getProtocolType(int32_t source) {
        return static_cast<uint8_t>((static_cast<uint32_t>(source) >> 24) & 0xFF);
    }
    static int32_t getHeaderLength(int32_t length) { return length & 0xFFFFFF; }
    static int32_t markProtocolType(int32_t source, uint8_t type) {
        return static_cast<int32_t>(((static_cast<uint32_t>(type) & 0xFF) << 24)
                                    | (static_cast<uint32_t>(source) & 0x00FFFFFFu));
    }

    void markResponseType() { flag |= (1 << RPC_TYPE); }
    bool isResponseType() const { return (flag & (1 << RPC_TYPE)) == (1 << RPC_TYPE); }
    void markOnewayRpc() { flag |= (1 << RPC_ONEWAY); }
    bool isOnewayRpc() const { return (flag & (1 << RPC_ONEWAY)) == (1 << RPC_ONEWAY); }

    const char* getType() const {
        return isResponseType() ? RemotingCommandType::RESPONSE_COMMAND
                                : RemotingCommandType::REQUEST_COMMAND;
    }

    // ---------------- encode / decode ----------------
    // Expand the non-empty fields of customHeader into extFields (matching the broker's reflection-based field convention)
    void makeCustomHeaderToNet();

    JsonValue toJsonObject() const;

    Bytes headerEncode();
    Bytes encode();
    // Encode only the header (the body length is added separately), used for framed sending
    Bytes encodeHeader(int32_t bodyLength = 0);

    // A parse failure throws RemotingCommandException
    static bool tryDecode(const Bytes& data, RemotingCommand& out, std::string* err = nullptr);
    static RemotingCommand decode(const Bytes& data);

    // Map extFields back onto the custom header object
    void decodeCommandCustomHeader(CommandCustomHeader& header) const;

    void addExtField(const std::string& key, const std::string& value) { extFields[key] = value; }
    std::string getExtField(const std::string& key) const;

    std::string toString() const;

    static int32_t nextOpaque();
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_REMOTING_COMMAND_H
