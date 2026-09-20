#include "rocketmq/common/recall_message_handle.h"

#include <cstdint>
#include <string>
#include <vector>

#include "rocketmq/client/exception.h"

namespace rocketmq {
namespace {

constexpr const char* kSeparator = " ";
constexpr const char* kVersion1 = "v1";
constexpr const char* kInvalidHandle = "recall handle is invalid";
constexpr char kAlphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

std::string base64UrlEncode(const std::string& raw) {
    const auto* bytes = reinterpret_cast<const unsigned char*>(raw.data());
    const size_t n = raw.size();
    std::string out;
    out.reserve(((n + 2) / 3) * 4);

    size_t i = 0;
    while (i + 3 <= n) {
        const uint32_t v = (static_cast<uint32_t>(bytes[i]) << 16) |
                           (static_cast<uint32_t>(bytes[i + 1]) << 8) |
                           static_cast<uint32_t>(bytes[i + 2]);
        out.push_back(kAlphabet[(v >> 18) & 0x3F]);
        out.push_back(kAlphabet[(v >> 12) & 0x3F]);
        out.push_back(kAlphabet[(v >> 6) & 0x3F]);
        out.push_back(kAlphabet[v & 0x3F]);
        i += 3;
    }
    const size_t rest = n - i;
    if (rest == 1) {
        const uint32_t v = static_cast<uint32_t>(bytes[i]) << 16;
        out.push_back(kAlphabet[(v >> 18) & 0x3F]);
        out.push_back(kAlphabet[(v >> 12) & 0x3F]);
        out.append("==");
    } else if (rest == 2) {
        const uint32_t v = static_cast<uint32_t>(bytes[i]) << 16 |
                           static_cast<uint32_t>(bytes[i + 1]) << 8;
        out.push_back(kAlphabet[(v >> 18) & 0x3F]);
        out.push_back(kAlphabet[(v >> 12) & 0x3F]);
        out.push_back(kAlphabet[(v >> 6) & 0x3F]);
        out.push_back('=');
    }
    return out;
}

int8_t base64Value(char c) {
    if (c >= 'A' && c <= 'Z') return static_cast<int8_t>(c - 'A');
    if (c >= 'a' && c <= 'z') return static_cast<int8_t>(c - 'a' + 26);
    if (c >= '0' && c <= '9') return static_cast<int8_t>(c - '0' + 52);
    if (c == '-') return 62;
    if (c == '_') return 63;
    return -1;
}

// 解码 base64url；带不带 '=' 填充都吃（见头文件里的差异说明）。
// 返回 false 表示输入不是合法 base64url（Java 此处抛 DecoderException）。
bool base64UrlDecode(const std::string& text, std::string& out) {
    std::vector<uint8_t> buf;
    buf.reserve(text.size() / 4 * 3 + 3);
    uint32_t acc = 0;
    int bits = 0;
    for (char c : text) {
        if (c == '=') break;  // 填充之后不该再有数据，尾部一律忽略
        const int8_t v = base64Value(c);
        if (v < 0) return false;
        acc = (acc << 6) | static_cast<uint32_t>(v);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            buf.push_back(static_cast<uint8_t>((acc >> bits) & 0xFF));
        }
    }
    // 剩下不足一个字节的悬挂位必须是 0，否则是伪造的尾位（Java 解码器同样拒绝）。
    if (bits > 0 && ((acc & ((1U << bits) - 1U)) != 0)) return false;
    out.assign(reinterpret_cast<const char*>(buf.data()), buf.size());
    return true;
}

// Java 的 new String(bytes, UTF_8) 对非法序列是替换而不是报错，本端口与 Rust 版一致：
// 非法 utf-8 直接判为「句柄非法」。差别只在畸形句柄上，正常句柄永远是 ASCII。
bool isUtf8(const std::string& s) {
    size_t i = 0;
    const auto* b = reinterpret_cast<const unsigned char*>(s.data());
    while (i < s.size()) {
        const unsigned char c = b[i];
        size_t need = 0;
        unsigned int cp = 0;
        if (c < 0x80) {
            ++i;
            continue;
        } else if (c >= 0xC2 && c <= 0xDF) {
            need = 1;
            cp = c & 0x1Fu;
        } else if (c >= 0xE0 && c <= 0xEF) {
            need = 2;
            cp = c & 0x0Fu;
        } else if (c >= 0xF0 && c <= 0xF4) {
            need = 3;
            cp = c & 0x07u;
        } else {
            return false;
        }
        if (i + need >= s.size()) return false;
        for (size_t k = 1; k <= need; ++k) {
            const unsigned char cc = b[i + k];
            if (cc < 0x80 || cc > 0xBF) return false;
            cp = (cp << 6) | (cc & 0x3Fu);
        }
        if (need == 2 && cp < 0x800) return false;
        if (need == 3 && (cp < 0x10000 || cp > 0x10FFFF)) return false;
        i += need + 1;
    }
    return true;
}

std::vector<std::string> splitBySpace(const std::string& s) {
    std::vector<std::string> out;
    size_t start = 0;
    while (true) {
        const size_t pos = s.find(kSeparator[0], start);
        if (pos == std::string::npos) {
            out.push_back(s.substr(start));
            break;
        }
        out.push_back(s.substr(start, pos - start));
        start = pos + 1;
    }
    return out;
}

}  // namespace

std::string buildRecallHandle(const std::string& topic, const std::string& brokerName,
                              const std::string& timestampStr, const std::string& messageId) {
    std::string raw;
    raw.reserve(topic.size() + brokerName.size() + timestampStr.size() + messageId.size() + 8);
    raw += kVersion1;
    raw += kSeparator;
    raw += topic;
    raw += kSeparator;
    raw += brokerName;
    raw += kSeparator;
    raw += timestampStr;
    raw += kSeparator;
    raw += messageId;
    return base64UrlEncode(raw);
}

HandleV1 decodeRecallHandle(const std::string& handle) {
    if (handle.empty()) {
        throw MQClientException(kInvalidHandle);
    }
    std::string raw;
    if (!base64UrlDecode(handle, raw) || !isUtf8(raw)) {
        throw MQClientException(kInvalidHandle);
    }
    // Java: split(" ") 后 items[0] 必须是 v1 且长度 >= 5，取 items[1..4]。
    const std::vector<std::string> items = splitBySpace(raw);
    if (items.size() < 5 || items[0] != kVersion1) {
        throw MQClientException(kInvalidHandle);
    }
    HandleV1 parsed;
    parsed.topic = items[1];
    parsed.brokerName = items[2];
    parsed.timestampStr = items[3];
    parsed.messageId = items[4];
    return parsed;
}

}  // namespace rocketmq
