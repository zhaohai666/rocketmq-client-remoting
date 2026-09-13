// 消息二进制编解码（对应 org.apache.rocketmq.common.message.MessageDecoder）。
//
// 与 Java 一致，存在**两条互不可混用**的编码路径：
//
// 1) 17 段存储格式 MessageDecoder.encode(MessageExt, needCompress) / decode(ByteBuffer)
//    broker 写入与 pull/get 返回的消息体：
//
//    TOTALSIZE(4) | MAGICCODE(4, v1=-626843481 / v2=-626843477) | BODYCRC(4) | QUEUEID(4)
//    | FLAG(4) | QUEUEOFFSET(8) | PHYSICALOFFSET(8) | SYSFLAG(4) | BORNTIMESTAMP(8)
//    | BORNHOST(4|16B + port4) | STORETIMESTAMP(8) | STOREHOST(4|16B + port4)
//    | RECONSUMETIMES(4) | PREPAREDTRANSACTIONOFFSET(8)
//    | BODY(4 + len) | TOPIC(1B v1 / 2B v2 + bytes) | PROPERTIES(2 + len)
//
// 2) 6 段轻量格式 MessageDecoder.encodeMessage(Message) / decodeMessage(ByteBuffer)
//    仅用于**批量消息**的 body，不含 topic / crc：
//
//    TOTALSIZE(4) | MAGICCODE(4, 固定 0) | BODYCRC(4, 固定 0) | FLAG(4)
//    | BODY(4 + len) | PROPERTIES(2 + len)
#ifndef ROCKETMQ_COMMON_MESSAGE_DECODER_H
#define ROCKETMQ_COMMON_MESSAGE_DECODER_H

#include <cstdint>
#include <string>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/message.h"

namespace rocketmq {

constexpr int32_t CHARSET_UTF8_SEPARATOR = 0;  // 占位：C++ 侧统一使用 UTF-8
constexpr uint8_t NAME_VALUE_SEPARATOR = 1;
constexpr uint8_t PROPERTY_SEPARATOR = 2;

constexpr int32_t MESSAGE_MAGIC_CODE = -626843481;
constexpr int32_t MESSAGE_MAGIC_CODE_V2 = -626843477;
constexpr int32_t BLANK_MAGIC_CODE = -875286124;

// 字段固定偏移，与 Java MessageDecoder 常量一致
constexpr size_t MESSAGE_MAGIC_CODE_POSITION = 4;
constexpr size_t MESSAGE_FLAG_POSITION = 16;
constexpr size_t MESSAGE_PHYSIC_OFFSET_POSITION = 28;
constexpr size_t QUEUE_OFFSET_POSITION = 4 + 4 + 4 + 4 + 4;
constexpr size_t PHY_POS_POSITION = 4 + 4 + 4 + 4 + 4 + 8;
constexpr size_t SYSFLAG_POSITION = 4 + 4 + 4 + 4 + 4 + 8 + 8;
constexpr size_t MESSAGE_STORE_TIMESTAMP_POSITION = 56;

// ---------------------------------------------------------------- 基础工具

// IP + port -> 8B(v4) / 20B(v6)，与 broker 侧 InetSocketAddress 编码一致
Bytes ipAndPortToBytes(const std::string& ip, int32_t port, bool v6 = false);

// 8B / 20B -> (ip, port)
bool bytesToIpAndPort(const Bytes& raw, std::string& ip, int32_t& port);

uint32_t crc32(const Bytes& data);

std::string bytes2String(const Bytes& bs);

// ------------------------------------------------------- 属性串 <-> Map

// Java MessageDecoder.messageProperties2String：k\x01v\x02 逐项拼接
std::string messagePropertiesToString(const PropertyMap& properties);

// Java MessageDecoder.string2messageProperties
PropertyMap stringToMessageProperties(const std::string& propertiesStr);

// ---------------------------------------------------------------- msgId

// ip+port(8 或 20B) + 8B commitLogOffset -> 大写十六进制 msgId
std::string createMessageId(const Bytes& addrBytes, int64_t offset);

// Java MessageDecoder.decodeMessageId -> (ip, port, offset)
bool decodeMessageId(const std::string& msgId, std::string& ip, int32_t& port, int64_t& offset);

// ------------------------------------------- 1) 17 段存储格式：MessageExt

Bytes encodeMessageExt(const MessageExt& messageExt, bool needCompress = false);

// 解码失败返回 false（对应 Java decode 返回 null）
bool decodeMessage(const Bytes& raw, MessageExt& out, bool readBody = true,
                   bool decompressBody = true, bool isClient = true, bool checkCrc = false);

// 消息流 -> MessageExt 列表（对应 MessageDecoder.decodes，用于 pull 结果）
std::vector<MessageExt> decodeMessages(const Bytes& raw, bool readBody = true);

// --------------------------------------- 2) 6 段轻量格式：批量消息 body

// Java MessageDecoder.encodeMessage(Message)
Bytes encodeMessage(const Message& message);

// Java MessageDecoder.encodeMessages(List<Message>)
Bytes encodeMessages(const std::vector<Message>& messages);

// Java MessageDecoder.decodeMessage(ByteBuffer)：单条批量单元 -> Message
bool decodeBatchMessage(const Bytes& raw, Message& out);

// Java MessageDecoder.decodeMessages(ByteBuffer)：批量 body -> Message 列表
std::vector<Message> decodeBatchMessages(const Bytes& raw);

// Java MessageDecoder.countInnerMsgNum
int32_t countInnerMsgNum(const Bytes& raw);

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_MESSAGE_DECODER_H
