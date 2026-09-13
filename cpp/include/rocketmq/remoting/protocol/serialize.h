// 协议序列化：JSON（RemotingSerializable）与 RocketMQ 私有二进制（RocketMQSerializable）。
//
// 对应 org.apache.rocketmq.remoting.protocol.RemotingSerializable / RocketMQSerializable。
//
// RocketMQ 二进制 header 线格式：
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

// RocketMQ 二进制 header 的字段集合。刻意不依赖 RemotingCommand，避免与调用层循环包含。
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

// org.apache.rocketmq.remoting.protocol.RemotingSerializable
struct RemotingSerializable {
    // JsonValue -> UTF-8 字节串
    static Bytes encode(const JsonValue& v);
    static std::string dump(const JsonValue& v);
    // 字节串 -> JsonValue；空串或解析失败返回 false
    static bool decode(const Bytes& data, JsonValue& out);
};

// org.apache.rocketmq.remoting.protocol.RocketMQSerializable
struct RocketMQSerializable {
    // 十进制 ASCII 写入：先占位 int 长度，写完回补
    static void writeDecimalLong(Bytes& buf, int64_t value);
    static void writeDecimalInt(Bytes& buf, int32_t value);

    // useShortLength=true 用 2 字节长度（extFields 的 key），false 用 4 字节（value / remark）
    static void writeStr(Bytes& buf, bool useShortLength, const std::string& s);

    // 读取返回 false 表示越界
    static bool readStr(const Bytes& buf, size_t offset, bool useShortLength,
                        std::string& out, size_t& newOffset);

    static Bytes mapSerialize(const PropertyMap& mapData);

    static int32_t calTotalLen(const std::string& remark, bool hasRemark, size_t extLen);

    static Bytes rocketMQProtocolEncode(const ProtocolHeaderFields& header);

    static bool rocketMQProtocolDecode(const Bytes& headerBytes, ProtocolHeaderFields& out);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_SERIALIZE_H
