// Protocol serialization: JSON (RemotingSerializable) and the RocketMQ private binary format (RocketMQSerializable).
//
//
// RocketMQ binary header wire format:
//   code(2) | language(1) | version(2) | opaque(4) | flag(4)
//   | remark(int + utf8) | extFields(int + [key(short + utf8) value(int + utf8)]...)
#ifndef ROCKETMQ_REMOTING_PROTOCOL_SERIALIZE_H
#define ROCKETMQ_REMOTING_PROTOCOL_SERIALIZE_H

#include <cstdint>
#include <map>
#include <string>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/types.h"
#include "rocketmq/remoting/protocol/json.h"

namespace rocketmq {

// The field set of a RocketMQ binary header. Deliberately independent of RemotingCommand to avoid a circular include with the calling layer.
struct ProtocolHeaderFields {
    int32_t code = 0;
    uint8_t language = 0;
    int32_t version = 0;
    int32_t opaque = 0;
    int32_t flag = 0;
    std::string remark;
    bool hasRemark = false;
    PropertyMap extFields;
};

struct RemotingSerializable {
    // JsonValue -> UTF-8 byte string
    static Bytes encode(const JsonValue& v);
    static std::string dump(const JsonValue& v);
    // Byte string -> JsonValue; an empty string or a parse failure returns false
    static bool decode(const Bytes& data, JsonValue& out);
};

struct RocketMQSerializable {
    // Written as decimal ASCII: reserve an int length slot first, patch it after writing
    static void writeDecimalLong(Bytes& buf, int64_t value);
    static void writeDecimalInt(Bytes& buf, int32_t value);

    // useShortLength=true uses a 2-byte length (extFields keys), false uses 4 bytes (value / remark)
    static void writeStr(Bytes& buf, bool useShortLength, const std::string& s);

    // A read returning false means out of range
    static bool readStr(const Bytes& buf, size_t offset, bool useShortLength,
                        std::string& out, size_t& newOffset);

    static Bytes mapSerialize(const PropertyMap& mapData);

    static int32_t calTotalLen(const std::string& remark, bool hasRemark, size_t extLen);

    static Bytes rocketMQProtocolEncode(const ProtocolHeaderFields& header);

    static bool rocketMQProtocolDecode(const Bytes& headerBytes, ProtocolHeaderFields& out);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_SERIALIZE_H
