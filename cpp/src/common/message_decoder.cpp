// 消息二进制编解码实现（对应 org.apache.rocketmq.common.message.MessageDecoder）。
//
// 严格对齐 python/rocketmq/common/message_decoder.py（已对真实 5.5.1 集群验证），
// 两条**互不可混用**的路径：
//   1) 17 段存储格式：encodeMessageExt / decodeMessage / decodeMessages（broker 写入与 pull 返回）；
//   2) 6 段轻量格式：encodeMessage / encodeMessages / decodeBatchMessage / decodeBatchMessages（批量 body）。
//
// 压缩说明：与 Java/Python 对齐——消息体压缩用 **zlib 流格式**（RFC1950），
// 压缩类型取自 sysFlag 的 bit8~10；类型位为 0 的老版本消息按 ZLIB 解。
// 编解码统一走 CompressorFactory（见 common/compression.h），
// 失败或不支持的类型会抛异常，而不是静默透传压缩字节。
#include "rocketmq/common/message_decoder.h"

#include "rocketmq/common/compression.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"

namespace rocketmq {

namespace {

// 压缩：仅在 needCompress 且 sysFlag 已置 COMPRESSED_FLAG 时执行。
// 与 Java MessageDecoder.encode 一致，压缩类型取自 sysFlag 的 bit8~10。
Bytes maybeCompress(const Bytes& body, bool needCompress, int32_t sysFlag) {
    if (!needCompress) return body;
    if (!MessageSysFlag::isCompressed(sysFlag)) return body;
    const int32_t type = MessageSysFlag::getCompressionType(sysFlag);
    return CompressorFactory::compress(body, type, 5);
}

// 解压：与 Java MessageDecoder.decode 一致，压缩类型取自 sysFlag 的 bit8~10。
// 类型位为 0 的老版本压缩消息由 CompressionType::findByValue 映射到 ZLIB。
// **失败或不支持的类型会抛异常**，绝不返回压缩字节——否则上层会把压缩流当正文，
// 属静默数据损坏（Java 同样是抛 IOException / RuntimeException）。
Bytes maybeDecompress(const Bytes& body, bool decompressBody, int32_t sysFlag) {
    if (!decompressBody) return body;
    if (!MessageSysFlag::isCompressed(sysFlag)) return body;
    const int32_t type = MessageSysFlag::getCompressionType(sysFlag);
    return CompressorFactory::decompress(body, type);
}

}  // namespace

// ---------------------------------------------------------------- 基础工具

Bytes ipAndPortToBytes(const std::string& ip, int32_t port, bool v6) {
    Bytes out;
    Bytes addr;
    if (!UtilAll::ipToBytes(ip, v6, addr)) {
        // 非法 IP 回退 127.0.0.1（保持长度与调用方期望一致）
        UtilAll::ipToBytes("127.0.0.1", false, addr);
        if (v6) addr.assign(16, '\0');
    }
    out += addr;
    putInt32U(out, static_cast<uint32_t>(port));
    return out;
}

bool bytesToIpAndPort(const Bytes& raw, std::string& ip, int32_t& port) {
    if (raw.size() == 8) {
        if (!UtilAll::bytesToIp(raw.substr(0, 4), ip)) return false;
        port = ByteReader::getInt32At(raw, 4);
        return true;
    }
    if (raw.size() == 20) {
        if (!UtilAll::bytesToIp(raw.substr(0, 16), ip)) return false;
        port = ByteReader::getInt32At(raw, 16);
        return true;
    }
    return false;
}

uint32_t crc32(const Bytes& data) { return UtilAll::crc32(data); }

std::string bytes2String(const Bytes& bs) { return UtilAll::bytes2String(bs); }

// ------------------------------------------------------- 属性串 <-> Map

std::string messagePropertiesToString(const PropertyMap& properties) {
    std::string out;
    for (const auto& kv : properties) {
        out += kv.first;
        out.push_back(static_cast<char>(NAME_VALUE_SEPARATOR));
        out += kv.second;
        out.push_back(static_cast<char>(PROPERTY_SEPARATOR));
    }
    return out;
}

PropertyMap stringToMessageProperties(const std::string& propertiesStr) {
    PropertyMap result;
    if (propertiesStr.empty()) return result;
    const size_t length = propertiesStr.size();
    size_t index = 0;
    while (index < length) {
        size_t newIndex = propertiesStr.find(static_cast<char>(PROPERTY_SEPARATOR), index);
        if (newIndex == std::string::npos) newIndex = length;
        if (newIndex - index >= 3) {
            size_t kvSep = propertiesStr.find(static_cast<char>(NAME_VALUE_SEPARATOR), index);
            if (kvSep != std::string::npos && kvSep > index && kvSep < newIndex - 1) {
                result[propertiesStr.substr(index, kvSep - index)] =
                    propertiesStr.substr(kvSep + 1, newIndex - kvSep - 1);
            }
        }
        index = newIndex + 1;
    }
    return result;
}

// ---------------------------------------------------------------- msgId

std::string createMessageId(const Bytes& addrBytes, int64_t offset) {
    Bytes raw = addrBytes;
    putInt64(raw, offset);
    return UtilAll::bytes2String(raw);
}

bool decodeMessageId(const std::string& msgId, std::string& ip, int32_t& port, int64_t& offset) {
    Bytes raw = UtilAll::string2Bytes(msgId);
    if (raw.size() != 16 && raw.size() != 28) return false;
    size_t ipLen = (raw.size() == 16) ? 4 : 16;
    if (!UtilAll::bytesToIp(raw.substr(0, ipLen), ip)) return false;
    port = ByteReader::getInt32At(raw, ipLen);
    offset = ByteReader::getInt64At(raw, ipLen + 4);
    return true;
}

// ------------------------------------------- 1) 17 段存储格式：MessageExt

Bytes encodeMessageExt(const MessageExt& messageExt, bool needCompress) {
    Bytes body = maybeCompress(messageExt.body, needCompress, messageExt.sysFlag);
    int32_t bodyLength = static_cast<int32_t>(body.size());

    const std::string& topicBytes = messageExt.topic;
    size_t topicLen = topicBytes.size();
    std::string propertiesBytes = messagePropertiesToString(messageExt.properties);
    size_t propertiesLength = propertiesBytes.size();

    int32_t sysFlag = messageExt.sysFlag;
    size_t bornhostLength = (sysFlag & MessageSysFlag::BORNHOST_V6_FLAG) ? 20 : 8;
    size_t storehostLength = (sysFlag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) ? 20 : 8;

    size_t computedSize = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8
                        + bornhostLength + storehostLength + 4 + 8
                        + 4 + static_cast<size_t>(bodyLength)
                        + 1 + topicLen
                        + 2 + propertiesLength;
    int32_t storeSize = messageExt.storeSize > 0 ? messageExt.storeSize
                                                 : static_cast<int32_t>(computedSize);
    if (static_cast<size_t>(storeSize) < computedSize) {
        storeSize = static_cast<int32_t>(computedSize);
    }

    std::string bornHost = messageExt.bornHost.empty() ? "127.0.0.1" : messageExt.bornHost;
    int32_t bornPort = messageExt.bornHostPort;
    std::string storeHost = messageExt.storeHost.empty() ? "127.0.0.1" : messageExt.storeHost;
    int32_t storePort = messageExt.storeHostPort;

    Bytes buf;
    putInt32(buf, storeSize);                                        // 1 TOTALSIZE
    putInt32(buf, MESSAGE_MAGIC_CODE);                               // 2 MAGICCODE
    putInt32U(buf, messageExt.bodyCrc);                              // 3 BODYCRC
    putInt32(buf, messageExt.queueId);                               // 4 QUEUEID
    putInt32(buf, messageExt.flag);                                  // 5 FLAG
    putInt64(buf, messageExt.queueOffset);                           // 6 QUEUEOFFSET
    putInt64(buf, messageExt.commitLogOffset);                       // 7 PHYSICALOFFSET
    putInt32(buf, sysFlag);                                          // 8 SYSFLAG
    putInt64(buf, messageExt.bornTimestamp);                         // 9 BORNTIMESTAMP
    buf += ipAndPortToBytes(bornHost, bornPort, (sysFlag & MessageSysFlag::BORNHOST_V6_FLAG) != 0);
    putInt64(buf, messageExt.storeTimestamp);                        // 11 STORETIMESTAMP
    buf += ipAndPortToBytes(storeHost, storePort,
                            (sysFlag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) != 0);
    putInt32(buf, messageExt.reconsumeTimes);                        // 13 RECONSUMETIMES
    putInt64(buf, messageExt.preparedTransactionOffset);             // 14
    putInt32(buf, bodyLength);                                       // 15 BODY
    buf += body;
    putInt8(buf, static_cast<uint8_t>(topicLen & 0xFF));             // 16 TOPIC
    buf += topicBytes;
    putInt16U(buf, static_cast<uint32_t>(propertiesLength));         // 17 PROPERTIES
    buf += propertiesBytes;
    return buf;
}

bool decodeMessage(const Bytes& raw, MessageExt& out, bool readBody, bool decompressBody,
                   bool isClient, bool checkCrc) {
    try {
        ByteReader r(raw);
        int32_t storeSize = r.readInt32();
        int32_t magicCode = r.readInt32();
        if (magicCode != MESSAGE_MAGIC_CODE && magicCode != MESSAGE_MAGIC_CODE_V2) {
            return false;  // 未知魔数 -> 解码失败（对应 Java 返回 null）
        }
        bool useV2 = (magicCode == MESSAGE_MAGIC_CODE_V2);

        uint32_t bodyCrc = r.readUnsignedInt32();
        int32_t queueId = r.readInt32();
        int32_t flag = r.readInt32();
        int64_t queueOffset = r.readInt64();
        int64_t physicOffset = r.readInt64();
        int32_t sysFlag = r.readInt32();
        int64_t bornTimestamp = r.readInt64();

        size_t bornhostLen = (sysFlag & MessageSysFlag::BORNHOST_V6_FLAG) ? 20 : 8;
        std::string bornHost;
        int32_t bornPort = 0;
        if (!bytesToIpAndPort(r.readBytes(bornhostLen), bornHost, bornPort)) return false;

        int64_t storeTimestamp = r.readInt64();
        size_t storehostLen = (sysFlag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) ? 20 : 8;
        std::string storeHost;
        int32_t storePort = 0;
        if (!bytesToIpAndPort(r.readBytes(storehostLen), storeHost, storePort)) return false;

        int32_t reconsumeTimes = r.readInt32();
        int64_t preparedTransactionOffset = r.readInt64();

        out.storeSize = storeSize;
        out.bodyCrc = bodyCrc;
        out.queueId = queueId;
        out.flag = flag;
        out.queueOffset = queueOffset;
        out.commitLogOffset = physicOffset;
        out.sysFlag = sysFlag;
        out.bornTimestamp = bornTimestamp;
        out.bornHost = bornHost;
        out.bornHostPort = bornPort;
        out.storeTimestamp = storeTimestamp;
        out.storeHost = storeHost;
        out.storeHostPort = storePort;
        out.reconsumeTimes = reconsumeTimes;
        out.preparedTransactionOffset = preparedTransactionOffset;

        // 15 BODY
        int32_t bodyLen = r.readInt32();
        if (bodyLen > 0) {
            if (readBody) {
                Bytes body = r.readBytes(static_cast<size_t>(bodyLen));
                if (checkCrc && crc32(body) != bodyCrc) return false;
                body = maybeDecompress(body, decompressBody, sysFlag);
                out.body = body;
                out.hasBody = true;
                // 对齐 Java：解压成功后清掉 COMPRESSED_FLAG（保留 bit8~10 的类型位）。
                // 条件与 Java `if (deCompressBody && isCompressed(sysFlag))` 完全相同。
                if (decompressBody && MessageSysFlag::isCompressed(sysFlag)) {
                    out.sysFlag = MessageSysFlag::clearCompressedFlag(sysFlag);
                }
            } else {
                r.skip(static_cast<size_t>(bodyLen));
                out.body.clear();
                out.hasBody = false;
            }
        } else {
            out.body.clear();
            out.hasBody = false;
        }

        // 16 TOPIC
        size_t topicLen = useV2 ? static_cast<size_t>(r.readInt16()) : static_cast<size_t>(r.readUnsignedInt8());
        out.topic = r.readBytes(topicLen);

        // 17 PROPERTIES
        size_t propertiesLength = static_cast<size_t>(r.readInt16());
        if (propertiesLength > 0) {
            out.properties = stringToMessageProperties(r.readBytes(propertiesLength));
        } else {
            out.properties.clear();
        }

        // msgId = storeHost(ip+port) + commitLogOffset
        Bytes storeAddrRaw = ipAndPortToBytes(storeHost, storePort, storehostLen == 20);
        out.msgId = createMessageId(storeAddrRaw, physicOffset);
        if (isClient) out.offsetMsgId = out.msgId;
        return true;
    } catch (...) {
        return false;
    }
}

std::vector<MessageExt> decodeMessages(const Bytes& raw, bool readBody) {
    std::vector<MessageExt> result;
    size_t pos = 0;
    size_t total = raw.size();
    while (pos < total) {
        if (total - pos < 4) break;
        int32_t storeSize = ByteReader::getInt32At(raw, pos);
        if (storeSize <= 0 || static_cast<size_t>(storeSize) > total - pos) break;
        MessageExt msg;
        if (!decodeMessage(raw.substr(pos, static_cast<size_t>(storeSize)), msg, readBody)) break;
        result.push_back(msg);
        pos += static_cast<size_t>(storeSize);
    }
    return result;
}

// --------------------------------------- 2) 6 段轻量格式：批量消息 body

Bytes encodeMessage(const Message& message) {
    const Bytes& body = message.body;
    std::string propertiesBytes = messagePropertiesToString(message.properties);
    size_t propertiesLength = propertiesBytes.size();
    int32_t storeSize = static_cast<int32_t>(4 + 4 + 4 + 4 + 4 + body.size() + 2 + propertiesLength);

    Bytes buf;
    putInt32(buf, storeSize);                                  // 1 TOTALSIZE
    putInt32(buf, 0);                                          // 2 MAGICCODE（批量场景固定 0）
    putInt32(buf, 0);                                          // 3 BODYCRC
    putInt32(buf, message.flag);                               // 4 FLAG
    putInt32(buf, static_cast<int32_t>(body.size()));          // 5 BODY
    buf += body;
    putInt16U(buf, static_cast<uint32_t>(propertiesLength));   // 6 PROPERTIES
    buf += propertiesBytes;
    return buf;
}

Bytes encodeMessages(const std::vector<Message>& messages) {
    Bytes out;
    for (const auto& m : messages) {
        out += encodeMessage(m);
    }
    return out;
}

bool decodeBatchMessage(const Bytes& raw, Message& out) {
    try {
        ByteReader r(raw);
        r.skip(4);  // TOTALSIZE
        r.skip(4);  // MAGICCODE
        r.skip(4);  // BODYCRC
        int32_t flag = r.readInt32();
        int32_t bodyLen = r.readInt32();
        Bytes body = r.readBytes(static_cast<size_t>(bodyLen));
        size_t propertiesLen = static_cast<size_t>(r.readInt16());
        PropertyMap properties;
        if (propertiesLen > 0) {
            properties = stringToMessageProperties(r.readBytes(propertiesLen));
        }
        out.flag = flag;
        out.body = body;
        out.hasBody = true;
        out.properties = properties;
        return true;
    } catch (...) {
        return false;
    }
}

std::vector<Message> decodeBatchMessages(const Bytes& raw) {
    std::vector<Message> result;
    size_t pos = 0;
    size_t total = raw.size();
    while (pos < total) {
        if (total - pos < 4) break;
        int32_t storeSize = ByteReader::getInt32At(raw, pos);
        if (storeSize <= 0 || static_cast<size_t>(storeSize) > total - pos) break;
        Message msg;
        if (!decodeBatchMessage(raw.substr(pos, static_cast<size_t>(storeSize)), msg)) break;
        result.push_back(msg);
        pos += static_cast<size_t>(storeSize);
    }
    return result;
}

int32_t countInnerMsgNum(const Bytes& raw) {
    int32_t count = 0;
    size_t pos = 0;
    size_t total = raw.size();
    while (pos < total) {
        count += 1;
        if (total - pos < 4) break;
        int32_t size = ByteReader::getInt32At(raw, pos);
        if (size <= 0 || static_cast<size_t>(size) > total - pos) break;
        pos += static_cast<size_t>(size);
    }
    return count;
}

}  // namespace rocketmq
