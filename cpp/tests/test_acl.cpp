// ACL 鉴权单测：SHA1 / HMAC-SHA1 / Base64 原语 + AclClientRPCHook 签名内容与签名值。
//
// 关键点：本文件的**签名向量全部来自 Java 官方实现**（用
// org.apache.rocketmq.acl.common.AclClientRPCHook#doBeforeRequest 跑出来的
// content + Signature），不是"自己算自己验"。任何一处拼接顺序 / 分隔符 /
// 字符集 / Base64 字母表的偏差都会立刻暴露。
//
// SHA1 / HMAC / Base64 的原语向量则取自公开标准（RFC 3174 / RFC 2202 / RFC 4648），
// 用来保证即使签名向量通过，也不是"两个错误互相抵消"。
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "rocketmq/remoting/rpchook.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

using namespace rocketmq;

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg)                                                       \
    do {                                                                       \
        if (cond) {                                                            \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << "\n";                           \
        }                                                                      \
    } while (0)

namespace {

std::string hex(const Bytes& b) {
    static const char* digits = "0123456789abcdef";
    std::string out;
    out.reserve(b.size() * 2);
    for (unsigned char c : b) {
        out.push_back(digits[(c >> 4) & 0xF]);
        out.push_back(digits[c & 0xF]);
    }
    return out;
}

// 构造一个只填 extFields/body 的请求（与 Java 探针 createRequestCommand(code,null) 等价）
RemotingCommand makeRequest(const std::vector<std::pair<std::string, std::string>>& ext,
                            const Bytes& body) {
    RemotingCommand cmd = RemotingCommand::createRequestCommand(310, nullptr);
    for (const auto& kv : ext) {
        cmd.addExtField(kv.first, kv.second);
    }
    if (!body.empty()) {
        cmd.body = body;
        cmd.hasBody = true;
    }
    return cmd;
}

}  // namespace

// ---------------------------------------------------------------- SHA1 原语
static void testSha1Vectors() {
    CHECK(hex(sha1("")) == "da39a3ee5e6b4b0d3255bfef95601890afd80709",
          "sha1(\"\") standard vector");
    CHECK(hex(sha1("abc")) == "a9993e364706816aba3e25717850c26c9cd0d89d",
          "sha1(\"abc\") standard vector");
    CHECK(hex(sha1("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"))
              == "84983e441c3bd26ebaae4aa1f95129e5e54670f1",
          "sha1(RFC3174 test#2) standard vector");
    CHECK(hex(sha1("The quick brown fox jumps over the lazy dog"))
              == "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12",
          "sha1(quick brown fox) standard vector");

    // 100 万个 'a'：跨多块 + 长度字段超 2^23 bit，专门压块循环与填充
    std::string million(1000000, 'a');
    CHECK(hex(sha1(million)) == "34aa973cd4c4daa4f61eeb2bdbad27316534016f",
          "sha1(1M x 'a') standard vector");

    // 填充边界：55/56/63/64/65 字节对块循环与填充长度编码最敏感，
    // 这里统一校验摘要长度恒为 20 字节（内容已由上面的标准向量覆盖）。
    for (size_t n : {size_t(55), size_t(56), size_t(63), size_t(64), size_t(65)}) {
        CHECK(sha1(std::string(n, 'x')).size() == 20, "sha1 padding boundary keeps 20 bytes");
    }
}

// ---------------------------------------------------------------- HMAC-SHA1 原语
static void testHmacVectors() {
    // RFC 2202 test case 1
    CHECK(hex(hmacSha1(std::string(20, static_cast<char>(0x0b)), "Hi There"))
              == "b617318655057264e28bc0b6fb378c8ef146be00",
          "hmac-sha1 RFC2202 #1");
    // RFC 2202 test case 2
    CHECK(hex(hmacSha1("Jefe", "what do ya want for nothing?"))
              == "effcdf6ae5eb2fa2d27416d5f184df9c259a7c79",
          "hmac-sha1 RFC2202 #2");
    // RFC 2202 test case 6：key 长于块大小（64B），必须先 SHA1 压缩
    CHECK(hex(hmacSha1(std::string(80, static_cast<char>(0xaa)),
                       "Test Using Larger Than Block-Size Key - Hash Key First"))
              == "aa4ae5e15272d00e95705637ce8a3b55ed402112",
          "hmac-sha1 RFC2202 #6 (key longer than block size)");
}

// ---------------------------------------------------------------- Base64 原语
static void testBase64Vectors() {
    CHECK(base64Encode("") == "", "base64 empty");
    CHECK(base64Encode("M") == "TQ==", "base64 1-byte padding");
    CHECK(base64Encode("Ma") == "TWE=", "base64 2-byte padding");
    CHECK(base64Encode("Man") == "TWFu", "base64 3-byte no padding");
    CHECK(base64Encode("any carnal pleasure") == "YW55IGNhcm5hbCBwbGVhc3VyZQ==",
          "base64 RFC4648 example 2");
    CHECK(base64Encode("any carnal pleasur") == "YW55IGNhcm5hbCBwbGVhc3Vy",
          "base64 RFC4648 example 3");
    CHECK(base64Encode("any carnal pleasure.") == "YW55IGNhcm5hbCBwbGVhc3VyZS4=",
          "base64 RFC4648 example 1");
    // 标准字母表必须出现 '+' 与 '/'（URL-safe 会用 -_ 代替）
    CHECK(base64Encode(Bytes("\xfb\xff\xbf", 3)) == "+/+/", "base64 uses standard alphabet (+/)");
}

// ---------------------------------------------------------------- 与 Java 逐字节对拍
//
// 下面 4 组向量的 content_hex 与 signature 全部由 Java 官方 AclClientRPCHook
// 对同一个 (extFields, body, accessKey, secretKey, token) 跑出来：
//   RemotingCommand cmd = RemotingCommand.createRequestCommand(310, null);
//   ... setExtFields / setBody ...
//   new AclClientRPCHook(new SessionCredentials(ak, sk[, token])).doBeforeRequest(addr, cmd);
static void testJavaSignatureVectors() {
    const std::string sk = "SK_TEST_SECRET_12345678";
    const std::vector<std::pair<std::string, std::string>> v1Ext = {
        {"topic", "MyTopic"}, {"producerGroup", "MyGroup"}, {"a", "1"},
        {"Zz", "last"},       {"batch", "false"},
    };
    const Bytes v1Body("\x01\x02\x03\x04\x05", 5);

    // ---- V1：多字段乱序插入 + body + 无 token ----
    {
        RemotingCommand cmd = makeRequest(v1Ext, v1Body);
        AclClientRPCHook hook(SessionCredentials("AK_TEST", sk));
        hook.doBeforeRequest("127.0.0.1:9876", cmd);

        CHECK(cmd.getExtField("AccessKey") == "AK_TEST", "V1 AccessKey injected");
        CHECK(cmd.getExtField("SecurityToken").empty(), "V1 no SecurityToken when token empty");
        CHECK(hex(AclClientRPCHook::buildRequestContent(cmd))
                  == "414b5f544553546c6173743166616c73654d7947726f75704d79546f7069630102030405",
              "V1 content matches Java combineRequestContent");
        CHECK(cmd.getExtField("Signature") == "qQhdzvXfV+g0r8LdwNCt+chJ4XY=",
              "V1 signature matches Java AclClientRPCHook");
    }

    // ---- V2：带 SecurityToken（token 参与签名，结果必须与 V1 不同）----
    {
        RemotingCommand cmd = makeRequest(v1Ext, v1Body);
        AclClientRPCHook hook(SessionCredentials("AK_TEST", sk, "TOKEN-ABC"));
        hook.doBeforeRequest("127.0.0.1:9876", cmd);

        CHECK(cmd.getExtField("SecurityToken") == "TOKEN-ABC", "V2 SecurityToken injected");
        CHECK(hex(AclClientRPCHook::buildRequestContent(cmd))
                  == "414b5f54455354544f4b454e2d4142436c6173743166616c73654d7947726f75704d79546f7069630102030405",
              "V2 content matches Java (token included, sorted after AccessKey)");
        CHECK(cmd.getExtField("Signature") == "5YIp2FNQL8pxQP3w6YKnSv3kAsw=",
              "V2 signature matches Java AclClientRPCHook");
        CHECK(cmd.getExtField("Signature") != "qQhdzvXfV+g0r8LdwNCt+chJ4XY=",
              "V2 token actually changes the signature");
    }

    // ---- V3：无 body ----
    {
        RemotingCommand cmd = makeRequest({{"a", "1"}, {"b", "2"}}, Bytes());
        AclClientRPCHook hook(SessionCredentials("AK", "SK"));
        hook.doBeforeRequest("127.0.0.1:9876", cmd);
        CHECK(hex(AclClientRPCHook::buildRequestContent(cmd)) == "414b3132",
              "V3 content without body matches Java");
        CHECK(cmd.getExtField("Signature") == "d3vJKL2iRdr4ZykZfY+lxfQlfdc=",
              "V3 signature matches Java AclClientRPCHook");
    }

    // ---- 插入顺序无关：等价 extFields 必须得到同一签名 ----
    {
        RemotingCommand a = makeRequest(v1Ext, v1Body);
        RemotingCommand b = makeRequest({{"Zz", "last"}, {"batch", "false"}, {"a", "1"},
                                         {"producerGroup", "MyGroup"}, {"topic", "MyTopic"}},
                                       v1Body);
        AclClientRPCHook hook(SessionCredentials("AK_TEST", sk));
        hook.doBeforeRequest("x", a);
        hook.doBeforeRequest("x", b);
        CHECK(a.getExtField("Signature") == b.getExtField("Signature"),
              "signature independent of insertion order (map is sorted)");
    }
}

// ---------------------------------------------------------------- Signature 必须被排除
static void testSignatureExcluded() {
    // Java 侧：combineRequestContent 会跳过 key == "Signature" 的项。
    RemotingCommand withoutSig = makeRequest({{"topic", "MyTopic"}}, Bytes());
    RemotingCommand withFakeSig =
        makeRequest({{"topic", "MyTopic"}, {"Signature", "SHOULD_BE_EXCLUDED"}}, Bytes());

    // 手工调用 buildRequestContent 时（此时还没写入真实 Signature）
    CHECK(AclClientRPCHook::buildRequestContent(withFakeSig) == "MyTopic",
          "pre-existing Signature is excluded from signed content");
    CHECK(AclClientRPCHook::buildRequestContent(withoutSig)
              == AclClientRPCHook::buildRequestContent(withFakeSig),
          "content identical with/without a pre-existing Signature field");

    // 再算签名：两者必须一致（证明 Signature 不参与自己的签名）
    const std::string sigA =
        AclClientRPCHook::calcSignature("SK", withoutSig);
    const std::string sigB =
        AclClientRPCHook::calcSignature("SK", withFakeSig);
    CHECK(sigA == sigB, "calcSignature ignores an existing Signature field");
}

// ---------------------------------------------------------------- 未注册钩子时零改动
static void testNoHookNoMutation() {
    RemotingCommand cmd = makeRequest({{"topic", "MyTopic"}}, Bytes("body", 4));
    const size_t before = cmd.extFields.size();
    // 不注册钩子 = 不做任何事：extFields 不应被改动
    CHECK(cmd.extFields.size() == before && cmd.extFields.find("AccessKey") == cmd.extFields.end(),
          "no hook => no AccessKey/Signature injected");
    CHECK(cmd.getExtField("Signature").empty(), "no hook => empty signature");
}

int main() {
    testSha1Vectors();
    testHmacVectors();
    testBase64Vectors();
    testJavaSignatureVectors();
    testSignatureExcluded();
    testNoHookNoMutation();

    std::cout << "acl: PASS=" << g_pass << " FAIL=" << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
