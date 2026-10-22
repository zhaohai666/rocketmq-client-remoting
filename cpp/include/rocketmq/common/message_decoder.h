// 1) 17-field storage format MessageDecoder.encode(MessageExt, needCompress) / decode(ByteBuffer)
//    the message body the broker writes and returns from pull/get:
//
//    TOTALSIZE(4) | MAGICCODE(4, v1=-626843481 / v2=-626843477) | BODYCRC(4) | QUEUEID(4)
//    | FLAG(4) | QUEUEOFFSET(8) | PHYSICALOFFSET(8) | SYSFLAG(4) | BORNTIMESTAMP(8)
//    | BORNHOST(4|16B + port4) | STORETIMESTAMP(8) | STOREHOST(4|16B + port4)
//    | RECONSUMETIMES(4) | PREPAREDTRANSACTIONOFFSET(8)
//    | BODY(4 + len) | TOPIC(1B v1 / 2B v2 + bytes) | PROPERTIES(2 + len)
//
// 2) 6-field lightweight format MessageDecoder.encodeMessage(Message) / decodeMessage(ByteBuffer)
//    used only for the body of **batch messages**, without topic / crc:
//
//    TOTALSIZE(4) | MAGICCODE(4, always 0) | BODYCRC(4, always 0) | FLAG(4)
//    | BODY(4 + len) | PROPERTIES(2 + len)
#ifndef ROCKETMQ_COMMON_MESSAGE_DECODER_H
#define ROCKETMQ_COMMON_MESSAGE_DECODER_H

#include <cstdint>
#include <string>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/message.h"

namespace rocketmq {

constexpr int32_t CHARSET_UTF8_SEPARATOR = 0;  // Placeholder: the C++ side always uses UTF-8
constexpr uint8_t NAME_VALUE_SEPARATOR = 1;
constexpr uint8_t PROPERTY_SEPARATOR = 2;

constexpr int32_t MESSAGE_MAGIC_CODE = -626843481;
constexpr int32_t MESSAGE_MAGIC_CODE_V2 = -626843477;
constexpr int32_t BLANK_MAGIC_CODE = -875286124;

constexpr size_t MESSAGE_MAGIC_CODE_POSITION = 4;
constexpr size_t MESSAGE_FLAG_POSITION = 16;
constexpr size_t MESSAGE_PHYSIC_OFFSET_POSITION = 28;
constexpr size_t QUEUE_OFFSET_POSITION = 4 + 4 + 4 + 4 + 4;
constexpr size_t PHY_POS_POSITION = 4 + 4 + 4 + 4 + 4 + 8;
constexpr size_t SYSFLAG_POSITION = 4 + 4 + 4 + 4 + 4 + 8 + 8;
constexpr size_t MESSAGE_STORE_TIMESTAMP_POSITION = 56;

// ---------------------------------------------------------------- basic utilities

// IP + port -> 8B(v4) / 20B(v6), matching the broker-side InetSocketAddress encoding
Bytes ipAndPortToBytes(const std::string& ip, int32_t port, bool v6 = false);

// 8B / 20B -> (ip, port)
bool bytesToIpAndPort(const Bytes& raw, std::string& ip, int32_t& port);

// Body checksum in the form the store writes into BODYCRC (sign bit dropped); pairs with that field.
uint32_t crc32(const Bytes& data);

std::string bytes2String(const Bytes& bs);

// ------------------------------------------------------- property string <-> Map

std::string messagePropertiesToString(const PropertyMap& properties);

PropertyMap stringToMessageProperties(const std::string& propertiesStr);

// ---------------------------------------------------------------- msgId

// ip+port(8 or 20B) + 8B commitLogOffset -> upper-case hexadecimal msgId
std::string createMessageId(const Bytes& addrBytes, int64_t offset);

bool decodeMessageId(const std::string& msgId, std::string& ip, int32_t& port, int64_t& offset);

// ------------------------------------------- 1) 17-field storage format: MessageExt

Bytes encodeMessageExt(const MessageExt& messageExt, bool needCompress = false);

bool decodeMessage(const Bytes& raw, MessageExt& out, bool readBody = true,
                   bool decompressBody = true, bool isClient = true, bool checkCrc = false);

// Message stream -> list of MessageExt (decoding of the storage format, used for pull results)
std::vector<MessageExt> decodeMessages(const Bytes& raw, bool readBody = true);

// --------------------------------------- 2) 6-field lightweight format: batch message body

Bytes encodeMessage(const Message& message);

Bytes encodeMessages(const std::vector<Message>& messages);

bool decodeBatchMessage(const Bytes& raw, Message& out);

std::vector<Message> decodeBatchMessages(const Bytes& raw);

int32_t countInnerMsgNum(const Bytes& raw);

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_MESSAGE_DECODER_H
