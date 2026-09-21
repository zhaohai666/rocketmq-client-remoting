// AclClientRPCHook 实现 + 自带的 SHA1 / HMAC-SHA1 / Base64。
//
// 为什么自己实现而不用 OpenSSL：仓库至今只依赖 zlib（压缩），CMakeLists 里
// 没有 find_package(OpenSSL)。为了一个 20 字节摘要引入一个重量级依赖不划算，
// 且 SHA1/HMAC/Base64 都是几十行的定长算法，自带实现更容易与 Java 逐字节对拍。
#include "rocketmq/remoting/rpchook.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/codes.h"

namespace rocketmq {

namespace {

inline uint32_t rotl32(uint32_t v, int n) {
    return (v << n) | (v >> (32 - n));
}

// ---------------------------------------------------------------- SHA1 (RFC 3174)
class Sha1 {
public:
    void update(const uint8_t* data, size_t len) {
        total_ += len;
        while (len > 0) {
            const size_t take = std::min(len, kBlock - bufLen_);
            std::memcpy(buf_ + bufLen_, data, take);
            bufLen_ += take;
            data += take;
            len -= take;
            if (bufLen_ == kBlock) {
                processBlock(buf_);
                bufLen_ = 0;
            }
        }
    }

    Bytes digest() {
        const uint64_t bitLen = total_ * 8;
        const uint8_t one = 0x80;
        update(&one, 1);
        const uint8_t zero = 0x00;
        while (bufLen_ != kBlock - 8) {
            update(&zero, 1);
        }
        uint8_t lenBytes[8];
        for (int i = 0; i < 8; ++i) {
            lenBytes[i] = static_cast<uint8_t>((bitLen >> (56 - 8 * i)) & 0xFF);
        }
        std::memcpy(buf_ + bufLen_, lenBytes, 8);
        processBlock(buf_);
        Bytes out(20, '\0');
        for (int i = 0; i < 5; ++i) {
            out[static_cast<size_t>(i) * 4 + 0] = static_cast<char>((h_[i] >> 24) & 0xFF);
            out[static_cast<size_t>(i) * 4 + 1] = static_cast<char>((h_[i] >> 16) & 0xFF);
            out[static_cast<size_t>(i) * 4 + 2] = static_cast<char>((h_[i] >> 8) & 0xFF);
            out[static_cast<size_t>(i) * 4 + 3] = static_cast<char>(h_[i] & 0xFF);
        }
        return out;
    }

private:
    static constexpr size_t kBlock = 64;

    void processBlock(const uint8_t* block) {
        uint32_t w[80];
        for (int i = 0; i < 16; ++i) {
            const size_t o = static_cast<size_t>(i) * 4;
            w[i] = (static_cast<uint32_t>(block[o]) << 24)
                   | (static_cast<uint32_t>(block[o + 1]) << 16)
                   | (static_cast<uint32_t>(block[o + 2]) << 8)
                   | static_cast<uint32_t>(block[o + 3]);
        }
        for (int i = 16; i < 80; ++i) {
            w[i] = rotl32(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
        }

        uint32_t a = h_[0], b = h_[1], c = h_[2], d = h_[3], e = h_[4];
        for (int i = 0; i < 80; ++i) {
            uint32_t f = 0;
            uint32_t k = 0;
            if (i < 20) {
                f = (b & c) | ((~b) & d);
                k = 0x5A827999u;
            } else if (i < 40) {
                f = b ^ c ^ d;
                k = 0x6ED9EBA1u;
            } else if (i < 60) {
                f = (b & c) | (b & d) | (c & d);
                k = 0x8F1BBCDCu;
            } else {
                f = b ^ c ^ d;
                k = 0xCA62C1D6u;
            }
            const uint32_t tmp = rotl32(a, 5) + f + e + k + w[i];
            e = d;
            d = c;
            c = rotl32(b, 30);
            b = a;
            a = tmp;
        }
        h_[0] += a;
        h_[1] += b;
        h_[2] += c;
        h_[3] += d;
        h_[4] += e;
    }

    uint32_t h_[5] = {0x67452301u, 0xEFCDAB89u, 0x98BADCFEu, 0x10325476u, 0xC3D2E1F0u};
    uint8_t buf_[kBlock] = {0};
    size_t bufLen_ = 0;
    uint64_t total_ = 0;
};

}  // namespace

Bytes sha1(const Bytes& data) {
    Sha1 ctx;
    if (!data.empty()) {
        ctx.update(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    }
    return ctx.digest();
}

Bytes hmacSha1(const std::string& key, const Bytes& data) {
    // RFC 2104：>64 字节的 key 先做一次 SHA1
    uint8_t k[64];
    std::memset(k, 0, sizeof(k));
    if (key.size() > sizeof(k)) {
        const Bytes hashed = sha1(key);
        std::memcpy(k, hashed.data(), hashed.size());
    } else if (!key.empty()) {
        std::memcpy(k, key.data(), key.size());
    }

    Bytes inner;
    Bytes outer;
    inner.reserve(64 + data.size());
    outer.reserve(64 + 20);
    for (size_t i = 0; i < sizeof(k); ++i) {
        inner.push_back(static_cast<char>(static_cast<uint8_t>(k[i]) ^ 0x36));
        outer.push_back(static_cast<char>(static_cast<uint8_t>(k[i]) ^ 0x5C));
    }
    inner += data;
    outer += sha1(inner);
    return sha1(outer);
}

std::string base64Encode(const Bytes& data) {
    static const char* kTable =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    const size_t n = data.size();
    out.reserve(((n + 2) / 3) * 4);

    size_t i = 0;
    while (i + 3 <= n) {
        const uint32_t v = (static_cast<uint32_t>(static_cast<uint8_t>(data[i])) << 16)
                           | (static_cast<uint32_t>(static_cast<uint8_t>(data[i + 1])) << 8)
                           | static_cast<uint32_t>(static_cast<uint8_t>(data[i + 2]));
        out.push_back(kTable[(v >> 18) & 0x3F]);
        out.push_back(kTable[(v >> 12) & 0x3F]);
        out.push_back(kTable[(v >> 6) & 0x3F]);
        out.push_back(kTable[v & 0x3F]);
        i += 3;
    }

    const size_t rest = n - i;
    if (rest == 1) {
        const uint32_t v = static_cast<uint32_t>(static_cast<uint8_t>(data[i])) << 16;
        out.push_back(kTable[(v >> 18) & 0x3F]);
        out.push_back(kTable[(v >> 12) & 0x3F]);
        out.push_back('=');
        out.push_back('=');
    } else if (rest == 2) {
        const uint32_t v = (static_cast<uint32_t>(static_cast<uint8_t>(data[i])) << 16)
                           | (static_cast<uint32_t>(static_cast<uint8_t>(data[i + 1])) << 8);
        out.push_back(kTable[(v >> 18) & 0x3F]);
        out.push_back(kTable[(v >> 12) & 0x3F]);
        out.push_back(kTable[(v >> 6) & 0x3F]);
        out.push_back('=');
    }
    return out;
}

std::string hmacSha1Base64(const std::string& key, const Bytes& data) {
    return base64Encode(hmacSha1(key, data));
}

// ---------------------------------------------------------------- AclClientRPCHook

Bytes AclClientRPCHook::buildRequestContent(RemotingCommand& request) {
    // 对应 Java AclClientRPCHook#parseRequestContent 的第一步：把 customHeader 的
    // 字段展开进 extFields —— 它们同样是签名内容的一部分。
    request.makeCustomHeaderToNet();

    Bytes buf;
    // extFields 是 std::map<std::string,...>，天然按 key 升序，与 Java TreeMap 等价
    // （ACL 涉及的键都是 ASCII 属性名，字节序与 Java String 自然序一致）。
    for (const auto& kv : request.extFields) {
        if (kv.first == SessionCredentials::SIGNATURE) {
            continue;  // Signature 自身不参与签名（与 broker 侧一致）
        }
        buf += kv.second;
    }
    if (request.hasBody) {
        buf += request.body;
    }
    return buf;
}

std::string AclClientRPCHook::calcSignature(const std::string& secretKey,
                                            RemotingCommand& request) {
    return hmacSha1Base64(secretKey, buildRequestContent(request));
}

void AclClientRPCHook::doBeforeRequest(const std::string& remoteAddr,
                                       RemotingCommand& request) {
    (void)remoteAddr;
    // 顺序必须与 Java doBeforeRequest 一致：
    //   1) 写 AccessKey（2) 可选 SecurityToken）—— 它们参与签名
    //   3) 算签名   4) 写 Signature —— 自身不参与签名
    request.addExtField(SessionCredentials::ACCESS_KEY, credentials_.accessKey());
    if (!credentials_.securityToken().empty()) {
        request.addExtField(SessionCredentials::SECURITY_TOKEN, credentials_.securityToken());
    }
    const std::string signature = calcSignature(credentials_.secretKey(), request);
    request.addExtField(SessionCredentials::SIGNATURE, signature);
}

void StreamTypeRPCHook::doBeforeRequest(const std::string& remoteAddr,
                                        RemotingCommand& request) {
    (void)remoteAddr;
    // Java: request.addExtField(MixAll.REQ_T, String.valueOf(RequestType.STREAM.getCode()))
    // RequestType.STREAM 的 code 是 0，故写入字面量 "0"。
    request.addExtField(MixAll::REQ_T, std::to_string(RequestType::STREAM));
}

void ChainedRPCHook::doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) {
    // 严格按注册顺序执行 —— 顺序决定 ACL 签名覆盖的字段集（见头文件注释）。
    for (const std::shared_ptr<RPCHook>& hook : hooks_) {
        if (hook) hook->doBeforeRequest(remoteAddr, request);
    }
}

void ChainedRPCHook::doAfterResponse(const std::string& remoteAddr, const RemotingCommand& request,
                                     const RemotingCommand* response) {
    for (const std::shared_ptr<RPCHook>& hook : hooks_) {
        if (hook) hook->doAfterResponse(remoteAddr, request, response);
    }
}

std::shared_ptr<RPCHook> composeRequestHooks(bool enableStreamRequestType,
                                             const std::shared_ptr<RPCHook>& userHook) {
    // Java MQClientAPIImpl:329-335 的注册顺序是 Namespace → Stream → 用户钩子 →
    // DynamicalExtField；本端口没有前两者之外的钩子，只保留 Stream/用户这一对。
    if (!enableStreamRequestType) return userHook;
    std::vector<std::shared_ptr<RPCHook>> hooks;
    hooks.push_back(std::make_shared<StreamTypeRPCHook>());
    if (userHook) hooks.push_back(userHook);
    return std::make_shared<ChainedRPCHook>(std::move(hooks));
}

}  // namespace rocketmq
