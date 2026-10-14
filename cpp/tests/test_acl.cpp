// ACL authentication unit tests: SHA1 / HMAC-SHA1 / Base64 primitives + AclClientRPCHook signed content and signature values.
//
//
// The SHA1 / HMAC / Base64 primitive vectors come from public standards (RFC 3174 / RFC 2202 / RFC 4648),
// ensuring that even when the signature vectors pass, it is not "two errors cancelling each other out".
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rocketmq/common/mix_all.h"
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

// Records the call order and the extFields snapshot seen when each hook runs, proving the stream hook executes first.
class LogHook : public RPCHook {
public:
    LogHook(std::vector<std::string>& log, std::string name)
        : log_(log), name_(std::move(name)) {}

    void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) override {
        (void)remoteAddr;
        log_.push_back(name_);
        sawReqT = !request.getExtField(MixAll::REQ_T).empty();
    }

    void doAfterResponse(const std::string& remoteAddr, const RemotingCommand& request,
                         const RemotingCommand* response) override {
        (void)remoteAddr;
        (void)request;
        (void)response;
        log_.push_back(name_ + "#after");
    }

    std::vector<std::string>& log_;
    std::string name_;
    bool sawReqT = false;
};

}  // namespace

// ---------------------------------------------------------------- SHA1 primitives
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

// 1M 'a' chars: spans multiple blocks and drives the length field past 2^23 bits, stressing the block loop and padding
    std::string million(1000000, 'a');
    CHECK(hex(sha1(million)) == "34aa973cd4c4daa4f61eeb2bdbad27316534016f",
          "sha1(1M x 'a') standard vector");

// Padding boundaries: 55/56/63/64/65 bytes are the most sensitive cases for the block loop and padding-length encoding;
// only the digest length is checked here (always 20 bytes); contents are covered by the standard vectors above.
    for (size_t n : {size_t(55), size_t(56), size_t(63), size_t(64), size_t(65)}) {
        CHECK(sha1(std::string(n, 'x')).size() == 20, "sha1 padding boundary keeps 20 bytes");
    }
}

// ---------------------------------------------------------------- HMAC-SHA1 primitives
static void testHmacVectors() {
    // RFC 2202 test case 1
    CHECK(hex(hmacSha1(std::string(20, static_cast<char>(0x0b)), "Hi There"))
              == "b617318655057264e28bc0b6fb378c8ef146be00",
          "hmac-sha1 RFC2202 #1");
    // RFC 2202 test case 2
    CHECK(hex(hmacSha1("Jefe", "what do ya want for nothing?"))
              == "effcdf6ae5eb2fa2d27416d5f184df9c259a7c79",
          "hmac-sha1 RFC2202 #2");
// RFC 2202 test case 6: key longer than the block size (64B), so the key must be SHA1-compressed first
    CHECK(hex(hmacSha1(std::string(80, static_cast<char>(0xaa)),
                       "Test Using Larger Than Block-Size Key - Hash Key First"))
              == "aa4ae5e15272d00e95705637ce8a3b55ed402112",
          "hmac-sha1 RFC2202 #6 (key longer than block size)");
}

// ---------------------------------------------------------------- Base64 primitives
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
// The standard alphabet must contain '+' and '/' (URL-safe alphabets replace them with -_)
    CHECK(base64Encode(Bytes("\xfb\xff\xbf", 3)) == "+/+/", "base64 uses standard alphabet (+/)");
}

static void testAclSignatureVectors() {
    const std::string sk = "SK_TEST_SECRET_12345678";
    const std::vector<std::pair<std::string, std::string>> v1Ext = {
        {"topic", "MyTopic"}, {"producerGroup", "MyGroup"}, {"a", "1"},
        {"Zz", "last"},       {"batch", "false"},
    };
    const Bytes v1Body("\x01\x02\x03\x04\x05", 5);

// ---- V1: multiple fields inserted out of order + body + no token ----
    {
        RemotingCommand cmd = makeRequest(v1Ext, v1Body);
        AclClientRPCHook hook(SessionCredentials("AK_TEST", sk));
        hook.doBeforeRequest("127.0.0.1:9876", cmd);

        CHECK(cmd.getExtField("AccessKey") == "AK_TEST", "V1 AccessKey injected");
        CHECK(cmd.getExtField("SecurityToken").empty(), "V1 no SecurityToken when token empty");
        CHECK(hex(AclClientRPCHook::buildRequestContent(cmd))
                  == "414b5f544553546c6173743166616c73654d7947726f75704d79546f7069630102030405",
              "V1 content matches the reference content builder");
        CHECK(cmd.getExtField("Signature") == "qQhdzvXfV+g0r8LdwNCt+chJ4XY=",
              "V1 signature matches the reference signer");
    }

// ---- V2: with SecurityToken (the token participates in signing, so the result must differ from V1) ----
    {
        RemotingCommand cmd = makeRequest(v1Ext, v1Body);
        AclClientRPCHook hook(SessionCredentials("AK_TEST", sk, "TOKEN-ABC"));
        hook.doBeforeRequest("127.0.0.1:9876", cmd);

        CHECK(cmd.getExtField("SecurityToken") == "TOKEN-ABC", "V2 SecurityToken injected");
        CHECK(hex(AclClientRPCHook::buildRequestContent(cmd))
                  == "414b5f54455354544f4b454e2d4142436c6173743166616c73654d7947726f75704d79546f7069630102030405",
              "V2 content matches the reference (token included, sorted after AccessKey)");
        CHECK(cmd.getExtField("Signature") == "5YIp2FNQL8pxQP3w6YKnSv3kAsw=",
              "V2 signature matches the reference signer");
        CHECK(cmd.getExtField("Signature") != "qQhdzvXfV+g0r8LdwNCt+chJ4XY=",
              "V2 token actually changes the signature");
    }

// ---- V3: no body ----
    {
        RemotingCommand cmd = makeRequest({{"a", "1"}, {"b", "2"}}, Bytes());
        AclClientRPCHook hook(SessionCredentials("AK", "SK"));
        hook.doBeforeRequest("127.0.0.1:9876", cmd);
        CHECK(hex(AclClientRPCHook::buildRequestContent(cmd)) == "414b3132",
              "V3 content without body matches the reference");
        CHECK(cmd.getExtField("Signature") == "d3vJKL2iRdr4ZykZfY+lxfQlfdc=",
              "V3 signature matches the reference signer");
    }

// ---- Insertion-order independence: equivalent extFields must produce the same signature ----
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

// ---------------------------------------------------------------- Signature must be excluded
static void testSignatureExcluded() {
    RemotingCommand withoutSig = makeRequest({{"topic", "MyTopic"}}, Bytes());
    RemotingCommand withFakeSig =
        makeRequest({{"topic", "MyTopic"}, {"Signature", "SHOULD_BE_EXCLUDED"}}, Bytes());

// Calling buildRequestContent manually (before any real Signature has been written)
    CHECK(AclClientRPCHook::buildRequestContent(withFakeSig) == "MyTopic",
          "pre-existing Signature is excluded from signed content");
    CHECK(AclClientRPCHook::buildRequestContent(withoutSig)
              == AclClientRPCHook::buildRequestContent(withFakeSig),
          "content identical with/without a pre-existing Signature field");

// Then compute signatures: both must match (Signature never participates in its own signing)
    const std::string sigA =
        AclClientRPCHook::calcSignature("SK", withoutSig);
    const std::string sigB =
        AclClientRPCHook::calcSignature("SK", withFakeSig);
    CHECK(sigA == sigB, "calcSignature ignores an existing Signature field");
}

// ---------------------------------------------------------------- No hook registered: zero mutation
static void testNoHookNoMutation() {
    RemotingCommand cmd = makeRequest({{"topic", "MyTopic"}}, Bytes("body", 4));
    const size_t before = cmd.extFields.size();
// Registering no hook = doing nothing: extFields must not be modified
    CHECK(cmd.extFields.size() == before && cmd.extFields.find("AccessKey") == cmd.extFields.end(),
          "no hook => no AccessKey/Signature injected");
    CHECK(cmd.getExtField("Signature").empty(), "no hook => empty signature");
}

static void testStreamHookWritesReqT() {
    RemotingCommand cmd = makeRequest({{"topic", "MyTopic"}}, Bytes());
    StreamTypeRPCHook hook;
    hook.doBeforeRequest("127.0.0.1:10911", cmd);
    CHECK(cmd.getExtField(MixAll::REQ_T) == "0", "stream hook writes ReqT=0");
    CHECK(cmd.extFields.size() == 2, "stream hook only adds ReqT");
    std::vector<std::string> log;
    auto probe = std::make_shared<LogHook>(log, "probe");
    ChainedRPCHook chain{std::vector<std::shared_ptr<RPCHook>>{probe}};
    chain.doAfterResponse("127.0.0.1:10911", cmd, nullptr);
    CHECK(log.size() == 1 && log[0] == "probe#after", "chained doAfterResponse forwards");
}

// ---------------------------------------------------------------- composeRequestHooks shapes
static void testComposeRequestHooksShape() {
    auto user = std::make_shared<AclClientRPCHook>(SessionCredentials("AK", "SK"));

// Neither enabled: no hook registered at all (zero overhead); the facade skips registerRPCHook
    CHECK(composeRequestHooks(false, nullptr) == nullptr, "no stream + no user => no hook");

// User hook only: the original object is returned directly, not wrapped in a ChainedRPCHook shell
    const std::shared_ptr<RPCHook> onlyUser = composeRequestHooks(false, user);
    CHECK(onlyUser.get() == user.get(), "user only => the very same hook instance");
    CHECK(std::dynamic_pointer_cast<ChainedRPCHook>(onlyUser) == nullptr,
          "user only => not wrapped in a chain");

// stream + user hook: both hooks must be chained; the order is guaranteed by the chain order above
    CHECK(std::dynamic_pointer_cast<ChainedRPCHook>(composeRequestHooks(true, user)) != nullptr,
          "stream + user => chained");
// A single remaining hook is not wrapped in a chain shell (same shape as "user hook only" above): this is what happens when the facade enables stream alone
    const std::shared_ptr<RPCHook> onlyStream = composeRequestHooks(true, nullptr);
    CHECK(std::dynamic_pointer_cast<StreamTypeRPCHook>(onlyStream) != nullptr,
          "stream only => the single StreamTypeRPCHook, unwrapped");
    CHECK(std::dynamic_pointer_cast<ChainedRPCHook>(onlyStream) == nullptr,
          "stream only => no chain shell around a single hook");

    const std::shared_ptr<RPCHook> streamAndNs =
        composeRequestHooks(true, nullptr, [] { return std::string("MQINST_shape"); });
    auto nsStreamChain = std::dynamic_pointer_cast<ChainedRPCHook>(streamAndNs);
    CHECK(nsStreamChain != nullptr, "stream + namespace getter => chained");
    {
        RemotingCommand cmd = makeRequest({{"topic", "T"}}, Bytes());
        streamAndNs->doBeforeRequest("127.0.0.1:10911", cmd);
        CHECK(cmd.getExtField(MixAll::RPC_REQUEST_HEADER_NAMESPACE_FIELD) == "MQINST_shape",
              "namespace getter is read per request");
        CHECK(cmd.getExtField(MixAll::REQ_T) == "0", "the same hook tags ReqT too");
    }
    {
        RemotingCommand cmd = makeRequest({{"topic", "T"}}, Bytes());
        onlyStream->doBeforeRequest("127.0.0.1:10911", cmd);
        CHECK(cmd.getExtField(MixAll::REQ_T) == "0", "stream-only chain still tags ReqT");
        CHECK(cmd.getExtField(SessionCredentials::SIGNATURE).empty(),
              "stream-only chain adds no signature");
    }
}

static void testStreamRunsBeforeUserHook() {
    std::vector<std::string> log;
    auto probe = std::make_shared<LogHook>(log, "user");
    ChainedRPCHook chain{
        std::vector<std::shared_ptr<RPCHook>>{std::make_shared<StreamTypeRPCHook>(), probe}};

    RemotingCommand cmd = makeRequest({{"topic", "MyTopic"}}, Bytes());
    chain.doBeforeRequest("127.0.0.1:10911", cmd);
    CHECK(probe->sawReqT, "user hook already sees ReqT when it runs");
    CHECK(cmd.getExtField(MixAll::REQ_T) == "0", "ReqT survives to the wire");

// Reversed order (sign first, tag afterwards) would be a bug: the signed content would miss ReqT.
    std::vector<std::string> wrongOrder;
    auto signer = std::make_shared<LogHook>(wrongOrder, "acl");
    auto tagger = std::make_shared<LogHook>(wrongOrder, "stream");
    ChainedRPCHook reversed{
        std::vector<std::shared_ptr<RPCHook>>{signer, tagger}};
    RemotingCommand reversedCmd = makeRequest({{"topic", "MyTopic"}}, Bytes());
    reversed.doBeforeRequest("127.0.0.1:10911", reversedCmd);
    CHECK(!signer->sawReqT, "reversed order: signer runs before ReqT exists");
    const std::vector<std::string> expectedReversed{"acl", "stream"};
    CHECK(wrongOrder == expectedReversed, "chain runs hooks in order");
    const std::vector<std::string> expectedForward{"user"};
    CHECK(log == expectedForward, "chain does not reorder user hooks");
}

static void testReqTIsCoveredByAclSignature() {
    const SessionCredentials credentials("RocketMQ", "FRo3RvIPW7ih");

// 1) Correct order: chain(Stream, ACL)
    RemotingCommand chained = makeRequest({{"topic", "MyTopic"}}, Bytes("hello", 5));
    composeRequestHooks(true, std::make_shared<AclClientRPCHook>(credentials))
        ->doBeforeRequest("127.0.0.1:10911", chained);

// 2) Reference: write ReqT manually first, then run ACL alone -- the content matches what the broker reassembles
    RemotingCommand reference = makeRequest({{"topic", "MyTopic"}, {MixAll::REQ_T, "0"}},
                                            Bytes("hello", 5));
    AclClientRPCHook(credentials).doBeforeRequest("127.0.0.1:10911", reference);
    CHECK(chained.getExtField(SessionCredentials::SIGNATURE)
              == reference.getExtField(SessionCredentials::SIGNATURE),
          "ReqT is inside the signed content (chain == manual order)");
    CHECK(!chained.getExtField(SessionCredentials::SIGNATURE).empty(),
          "chain actually produced a signature");

// 3) Wrong order (ACL first, tag afterwards): ReqT is missing from the signature, so it must differ from case 1,
//    and a broker with authentication enabled would fail the verification. This case guards the ordering regression.
    RemotingCommand wrong = makeRequest({{"topic", "MyTopic"}}, Bytes("hello", 5));
    AclClientRPCHook(credentials).doBeforeRequest("127.0.0.1:10911", wrong);
    const std::string wrongSig = wrong.getExtField(SessionCredentials::SIGNATURE);
    wrong.addExtField(MixAll::REQ_T, "0");
    CHECK(wrongSig != chained.getExtField(SessionCredentials::SIGNATURE),
          "signing before ReqT produces a different (rejected) signature");
// The broker recomputes over all fields it received (ReqT included), so the content of case 2) is the correct one
    CHECK(AclClientRPCHook::buildRequestContent(reference).size()
              == AclClientRPCHook::buildRequestContent(chained).size(),
          "signed content matches what the broker recomputes");
}

int main() {
    testSha1Vectors();
    testHmacVectors();
    testBase64Vectors();
    testAclSignatureVectors();
    testSignatureExcluded();
    testNoHookNoMutation();
    testStreamHookWritesReqT();
    testComposeRequestHooksShape();
    testStreamRunsBeforeUserHook();
    testReqTIsCoveredByAclSignature();

    std::cout << "acl: PASS=" << g_pass << " FAIL=" << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
