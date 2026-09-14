// 协议序列化实现：JSON（RemotingSerializable）与 RocketMQ 私有二进制（RocketMQSerializable）。
//
// 严格对齐 python/rocketmq/remoting/protocol/serialize.py（已对真实 5.5.1 集群验证）：
//   - JSON：JsonValue -> UTF-8 字节串；
//   - ROCKETMQ header 线格式：
//       code(2) | language(1) | version(2) | opaque(4) | flag(4)
//       | remark(int + utf8) | extFields(int + [key(short + utf8) value(int + utf8)]...)
//   - writeDecimalLong：先占位 4B 长度，写完十进制 ASCII 后回补真实长度。
#include "rocketmq/remoting/protocol/serialize.h"

#include <limits>

namespace rocketmq {

// ---------------------------------------------------------------- RemotingSerializable

Bytes RemotingSerializable::encode(const JsonValue& v) { return v.dump(); }

std::string RemotingSerializable::dump(const JsonValue& v) { return v.dump(); }

bool RemotingSerializable::decode(const Bytes& data, JsonValue& out) {
    if (data.empty()) return false;
    return jsonParse(data, out, nullptr);
}

// ---------------------------------------------------------------- RocketMQSerializable

void RocketMQSerializable::writeDecimalLong(Bytes& buf, int64_t value) {
    // 先占位 4 字节长度
    size_t lenPos = buf.size();
    putInt32(buf, 0);
    size_t start = buf.size();

    if (value == 0) {
        buf.push_back('0');
    } else {
        bool neg = value < 0;
        if (neg) {
            buf.push_back('-');
            if (value == std::numeric_limits<int64_t>::min()) {
                // -(INT64_MIN) 溢出，直接写字符串常量
                const char* kMin = "9223372036854775808";
                buf += kMin;
                int32_t n = static_cast<int32_t>(buf.size() - start);
                // 回补长度
                buf[lenPos + 0] = static_cast<char>((n >> 24) & 0xFF);
                buf[lenPos + 1] = static_cast<char>((n >> 16) & 0xFF);
                buf[lenPos + 2] = static_cast<char>((n >> 8) & 0xFF);
                buf[lenPos + 3] = static_cast<char>(n & 0xFF);
                return;
            }
            value = -value;
        }
        std::string digits = std::to_string(value);
        buf += digits;
    }

    int32_t n = static_cast<int32_t>(buf.size() - start);
    buf[lenPos + 0] = static_cast<char>((n >> 24) & 0xFF);
    buf[lenPos + 1] = static_cast<char>((n >> 16) & 0xFF);
    buf[lenPos + 2] = static_cast<char>((n >> 8) & 0xFF);
    buf[lenPos + 3] = static_cast<char>(n & 0xFF);
}

void RocketMQSerializable::writeDecimalInt(Bytes& buf, int32_t value) {
    writeDecimalLong(buf, static_cast<int64_t>(value));
}

void RocketMQSerializable::writeStr(Bytes& buf, bool useShortLength, const std::string& s) {
    size_t n = s.size();
    if (useShortLength) {
        putInt16U(buf, static_cast<uint32_t>(n));
    } else {
        putInt32U(buf, static_cast<uint32_t>(n));
    }
    buf += s;
}

bool RocketMQSerializable::readStr(const Bytes& buf, size_t offset, bool useShortLength,
                                   std::string& out, size_t& newOffset) {
    size_t n = 0;
    if (useShortLength) {
        if (offset + 2 > buf.size()) return false;
        n = (static_cast<unsigned char>(buf[offset]) << 8)
          | static_cast<unsigned char>(buf[offset + 1]);
        offset += 2;
    } else {
        if (offset + 4 > buf.size()) return false;
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) v = (v << 8) | static_cast<unsigned char>(buf[offset + i]);
        n = v;
        offset += 4;
    }
    if (offset + n > buf.size()) return false;
    out.assign(buf, offset, n);
    offset += n;
    newOffset = offset;
    return true;
}

Bytes RocketMQSerializable::mapSerialize(const PropertyMap& mapData) {
    if (mapData.empty()) return Bytes();
    Bytes buf;
    for (const auto& kv : mapData) {
        writeStr(buf, true, kv.first);
        writeStr(buf, false, kv.second);
    }
    return buf;
}

int32_t RocketMQSerializable::calTotalLen(const std::string& remark, bool hasRemark,
                                          size_t extLen) {
    if (!hasRemark) {
        return static_cast<int32_t>(2 + 1 + 2 + 4 + 4 + 4 + 0 + 4 + extLen);
    }
    size_t remarkLen = remark.empty() ? 0 : remark.size();
    return static_cast<int32_t>(2 + 1 + 2 + 4 + 4 + 4 + remarkLen + 4 + extLen);
}

Bytes RocketMQSerializable::rocketMQProtocolEncode(const ProtocolHeaderFields& header) {
    Bytes remarkBytes;
    bool hasRemark = header.hasRemark && !header.remark.empty();
    if (hasRemark) remarkBytes = header.remark;

    Bytes extFieldsBytes = mapSerialize(header.extFields);
    bool hasExt = !extFieldsBytes.empty();

    Bytes buf;
    putInt16(buf, header.code & 0xFFFF);
    putInt8(buf, static_cast<uint8_t>(header.language & 0xFF));
    putInt16(buf, header.version & 0xFFFF);
    putInt32(buf, header.opaque);
    putInt32(buf, header.flag);

    if (hasRemark) {
        putInt32(buf, static_cast<int32_t>(remarkBytes.size()));
        buf += remarkBytes;
    } else {
        putInt32(buf, 0);
    }

    if (hasExt) {
        putInt32(buf, static_cast<int32_t>(extFieldsBytes.size()));
        buf += extFieldsBytes;
    } else {
        putInt32(buf, 0);
    }
    return buf;
}

bool RocketMQSerializable::rocketMQProtocolDecode(const Bytes& headerBytes,
                                                  ProtocolHeaderFields& out) {
    size_t p = 0;
    auto need = [&](size_t n) { return p + n <= headerBytes.size(); };

    if (!need(2)) return false;
    int32_t code = (static_cast<unsigned char>(headerBytes[p]) << 8)
                 | static_cast<unsigned char>(headerBytes[p + 1]);
    p += 2;

    if (!need(1)) return false;
    uint8_t language = static_cast<uint8_t>(headerBytes[p]);
    p += 1;

    if (!need(2)) return false;
    int32_t version = (static_cast<unsigned char>(headerBytes[p]) << 8)
                    | static_cast<unsigned char>(headerBytes[p + 1]);
    p += 2;

    if (!need(4)) return false;
    uint32_t opaque = 0;
    for (int i = 0; i < 4; ++i) opaque = (opaque << 8) | static_cast<unsigned char>(headerBytes[p + i]);
    p += 4;

    if (!need(4)) return false;
    uint32_t flag = 0;
    for (int i = 0; i < 4; ++i) flag = (flag << 8) | static_cast<unsigned char>(headerBytes[p + i]);
    p += 4;

    std::string remark;
    if (!readStr(headerBytes, p, false, remark, p)) return false;

    if (!need(4)) return false;
    uint32_t extLen = 0;
    for (int i = 0; i < 4; ++i) extLen = (extLen << 8) | static_cast<unsigned char>(headerBytes[p + i]);
    p += 4;

    PropertyMap extFields;
    if (extLen > 0) {
        size_t end = p + extLen;
        if (end > headerBytes.size()) return false;
        while (p < end) {
            std::string k;
            std::string v;
            if (!readStr(headerBytes, p, true, k, p)) return false;
            if (!readStr(headerBytes, p, false, v, p)) return false;
            extFields[k] = v;
        }
    }

    out.code = code & 0xFFFF;
    out.language = language;
    out.version = version & 0xFFFF;
    out.opaque = static_cast<int32_t>(opaque);
    out.flag = static_cast<int32_t>(flag);
    out.remark = remark;
    out.hasRemark = !remark.empty();
    out.extFields = extFields;
    return true;
}

}  // namespace rocketmq
