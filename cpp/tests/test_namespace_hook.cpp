// NamespaceRpcHook（5.x 新命名空间 / Aliyun 实例 ID 形态）单测。
//
// 对应 Java：
//   * client/src/main/java/org/apache/rocketmq/client/rpchook/NamespaceRpcHook.java
//     （doBeforeRequest 只在 namespaceV2 非空时加 `nsd=true` / `ns=<namespaceV2>`，
//      doAfterResponse 空；空命名空间时 extFields 一个键都不加）
//   * common/src/main/java/org/apache/rocketmq/common/MixAll.java:122-123
//     （RPC_REQUEST_HEADER_NAMESPACED_FIELD = "nsd" / _NAMESPACE_FIELD = "ns"）
//   * client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java:329-335
//     （注册顺序 Namespace → Stream → 用户钩子（ACL）→ DynamicalExtField；
//      Namespace 必须在 ACL **之前**，签名才覆盖 nsd/ns）
//   * client/src/test/java/org/apache/rocketmq/client/rpchook/NamespaceRpcHookTest.java
//     （无命名空间时请求的 extFields 保持原样）
//
// 覆盖四件事：
//   (a) 钩子加 nsd=true / ns=<值>；
//   (b) 空命名空间时 extFields 原样不动（不物化任何键）；
//   (c) 组合链序 Namespace → Stream → ACL（含只有一个/零个钩子时的形态）；
//   (d) 设了 namespaceV2 之后 ACL 签名跟着变（broker 侧算法重放能验通）。
//
// 后半部分再从 socket 这头取证：五个门面（生产者 / 推送 / 拉取 / 轻量拉取 / 管理端）
// 都真的把 nsd/ns 装上了链 —— composeRequestHooks 自身的单测锁不住「门面有没有用它」，
// 曾经就漏过 lite 消费者那一处（见 test_send_retry.cpp 用例 9 的同款回归）。
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/client/trace_dispatcher.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/rpchook.h"

using namespace rocketmq;

namespace {

using netcompat::kInvalidSocket;
using netcompat::socket_t;
using netcompat::socklen_type;

#ifdef _WIN32
constexpr int kShutdownHow = SD_BOTH;
#else
constexpr int kShutdownHow = SHUT_RDWR;
#endif

int g_pass = 0;
int g_fail = 0;

#define CHECK(cond, msg)                                                       \
    do {                                                                       \
        if (cond) {                                                            \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << "\n";                           \
        }                                                                      \
    } while (0)

const std::string kNsKey(MixAll::RPC_REQUEST_HEADER_NAMESPACE_FIELD);
const std::string kNsdKey(MixAll::RPC_REQUEST_HEADER_NAMESPACED_FIELD);

// 对应 Java NamespaceRpcHookTest 的建法：PULL_MESSAGE + PullMessageRequestHeader。
// 本端口的 createRequestCommand 与 Java 一样不把 customHeader 预先展开进 extFields
// （展开是签名/编码时 makeCustomHeaderToNet() 干的），所以刚建好的请求 extFields 为空，
// 「无命名空间 ⇒ extFields 保持原样」在这里就是「仍然为空」。
RemotingCommand pullRequest() {
    return RemotingCommand::createRequestCommand(
        RequestCode::PULL_MESSAGE, std::make_shared<PullMessageRequestHeader>());
}

RemotingCommand makeRequest(const std::vector<std::pair<std::string, std::string>>& ext) {
    RemotingCommand cmd = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, nullptr);
    for (const auto& kv : ext) cmd.addExtField(kv.first, kv.second);
    return cmd;
}

std::function<std::string()> getterOf(std::string value) {
    return [value] { return value; };
}

// 记录「自己被调用时看到了什么」的探针钩子，用来证明链序（探针跑在前面时看不到后面的字段）。
class ProbeHook : public RPCHook {
public:
    explicit ProbeHook(std::string name) : name_(std::move(name)) {}

    void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) override {
        (void)remoteAddr;
        ++calls;
        sawNsd = request.extFields.count(kNsdKey) > 0;
        sawNs = request.extFields.count(kNsKey) > 0;
        sawReqT = !request.getExtField(MixAll::REQ_T).empty();
        extSizeWhenRun = request.extFields.size();
    }

    const std::string name_;
    int calls = 0;
    bool sawNsd = false;
    bool sawNs = false;
    bool sawReqT = false;
    size_t extSizeWhenRun = 0;
};

// ---------------------------------------------------------------- (a) 配了 namespaceV2
void testHookAddsNamespaceFields() {
    RemotingCommand cmd = pullRequest();
    NamespaceRpcHook hook(getterOf("MQINST-abc"));
    hook.doBeforeRequest("127.0.0.1:9876", cmd);

    CHECK(cmd.getExtField(kNsdKey) == "true", "nsd=true injected");
    CHECK(cmd.getExtField(kNsKey) == "MQINST-abc", "ns=<namespaceV2> injected");
    CHECK(cmd.extFields.size() == 2, "exactly the two namespace fields are added");
    // Java MixAll.java:122-123 的两枚键名（写错 broker 直接读不到）
    CHECK(kNsdKey == "nsd", "MixAll nsd key spelling");
    CHECK(kNsKey == "ns", "MixAll ns key spelling");

    // 已有字段原样保留（钩子只做加法，不改写别人的键）
    RemotingCommand withExt = makeRequest({{"topic", "MyTopic"}});
    hook.doBeforeRequest("127.0.0.1:9876", withExt);
    CHECK(withExt.getExtField("topic") == "MyTopic", "pre-existing extFields survive");
    CHECK(withExt.extFields.size() == 3, "old + nsd + ns");

    // 重复调用幂等（每笔请求都会过钩子，同一请求被跑两次也不能长出第三个键）
    hook.doBeforeRequest("127.0.0.1:9876", withExt);
    CHECK(withExt.extFields.size() == 3, "addExtField overwrite, not append");

    // doAfterResponse：Java 是空实现（基类默认），响应不能被改写
    RemotingCommand resp = RemotingCommand::createResponseCommand(
        ResponseCode::SUCCESS, "ok");
    hook.doAfterResponse("127.0.0.1:9876", cmd, &resp);
    CHECK(resp.extFields.empty(), "doAfterResponse leaves the response untouched");

    // 取值函数每笔请求实时读（Java 直接读 clientConfig.getNamespaceV2()）：
    // 钩子建好之后再改配置，下一笔报文要跟着变。
    std::string live = "ns-before";
    NamespaceRpcHook liveHook([&live] { return live; });
    RemotingCommand first = pullRequest();
    liveHook.doBeforeRequest("x", first);
    live = "ns-after";
    RemotingCommand second = pullRequest();
    liveHook.doBeforeRequest("x", second);
    CHECK(first.getExtField(kNsKey) == "ns-before", "live getter: first request value");
    CHECK(second.getExtField(kNsKey) == "ns-after", "live getter: config change is picked up");
}

// ---------------------------------------------------------------- (b) 没配 namespaceV2
// 对应 Java NamespaceRpcHookTest#testDoBeforeRequestWithoutNamespace：
// extFields 必须**原样**（Java 断言 getExtFields() 为 null；本端口是 std::map，
// 等价口径 = 一个键都不加、也不物化任何字段）。
void testEmptyNamespaceLeavesExtFieldsUntouched() {
    RemotingCommand cmd = pullRequest();
    const size_t before = cmd.extFields.size();
    NamespaceRpcHook hook(getterOf(""));
    hook.doBeforeRequest("127.0.0.1:9876", cmd);
    CHECK(cmd.extFields.size() == before, "empty namespace => extFields unchanged");
    CHECK(cmd.extFields.empty(), "empty namespace => no field materialized");
    CHECK(cmd.extFields.count(kNsdKey) == 0, "empty namespace => no nsd key");
    CHECK(cmd.extFields.count(kNsKey) == 0, "empty namespace => no ns key");

    // 已有 extFields 也不能被动过
    RemotingCommand withExt = makeRequest({{"topic", "MyTopic"}, {"producerGroup", "G"}});
    hook.doBeforeRequest("127.0.0.1:9876", withExt);
    CHECK(withExt.extFields.size() == 2, "empty namespace => existing keys untouched");
    CHECK(withExt.getExtField("topic") == "MyTopic", "empty namespace => topic still MyTopic");

    // 取值函数为空 / 未传（facade 没配 namespaceV2 时的形状）同样什么都不做
    RemotingCommand noGetter = pullRequest();
    const std::function<std::string()> noNamespaceWired;
    NamespaceRpcHook emptyHook(noNamespaceWired);
    emptyHook.doBeforeRequest("127.0.0.1:9876", noGetter);
    CHECK(noGetter.extFields.empty(), "null getter behaves like an empty namespace");
}

// ---------------------------------------------------------------- (c) 链序
void testCompositionOrderNamespaceThenStreamThenAcl() {
    auto acl = std::make_shared<AclClientRPCHook>(SessionCredentials("AK", "SK"));
    const std::function<std::string()> nsGetter = getterOf("MQINST-order");

    // 三环齐活：Namespace → Stream → 用户（Java MQClientAPIImpl:329-335）
    std::shared_ptr<RPCHook> composed = composeRequestHooks(true, acl, nsGetter);
    auto chain = std::dynamic_pointer_cast<ChainedRPCHook>(composed);
    CHECK(chain != nullptr, "namespace + stream + user => chained");
    if (chain) {
        const std::vector<std::shared_ptr<RPCHook>>& hooks = chain->hooks();
        CHECK(hooks.size() == 3, "chain holds exactly three hooks");
        bool typesInOrder = hooks.size() == 3
                            && std::dynamic_pointer_cast<NamespaceRpcHook>(hooks[0]) != nullptr
                            && std::dynamic_pointer_cast<StreamTypeRPCHook>(hooks[1]) != nullptr
                            && hooks[2].get() == acl.get();
        CHECK(typesInOrder, "chain order is Namespace -> Stream -> user(ACL)");

        // 行为证据：ACL 跑到时 nsd/ns/ReqT 都已就位（Namespace 又在 Stream 之前）。
        auto probe = std::make_shared<ProbeHook>("user");
        std::vector<std::shared_ptr<RPCHook>> probed{
            std::make_shared<NamespaceRpcHook>(nsGetter),
            std::make_shared<StreamTypeRPCHook>(),
            probe};
        RemotingCommand cmd = makeRequest({{"topic", "MyTopic"}});
        ChainedRPCHook(probed).doBeforeRequest("127.0.0.1:9876", cmd);
        CHECK(probe->calls == 1, "user hook runs exactly once");
        CHECK(probe->sawNsd, "user hook already sees nsd when it runs");
        CHECK(probe->sawNs, "user hook already sees ns when it runs");
        CHECK(probe->sawReqT, "user hook already sees ReqT when it runs");
        CHECK(probe->extSizeWhenRun == 4, "user hook signs topic + ns + nsd + ReqT");
        CHECK(cmd.extFields.count(kNsdKey) == 1 && cmd.extFields.count(kNsKey) == 1,
              "nsd/ns survive to the wire");
        CHECK(cmd.getExtField(kNsKey) == "MQINST-order", "ns value reaches the wire");

        // 反序（先签名后打标）就是 bug：签的内容里没有 nsd/ns。
        auto signer = std::make_shared<ProbeHook>("acl");
        ChainedRPCHook reversed{std::vector<std::shared_ptr<RPCHook>>{
            signer, std::make_shared<NamespaceRpcHook>(nsGetter)}};
        RemotingCommand wrong = makeRequest({{"topic", "MyTopic"}});
        reversed.doBeforeRequest("127.0.0.1:9876", wrong);
        CHECK(!signer->sawNsd, "reversed order: signer runs before nsd exists");
        CHECK(wrong.extFields.count(kNsdKey) == 1, "reversed order still puts nsd on the wire");
    }

    // 只配了 namespaceV2（没开 stream、没 ACL）：单钩子，不套链壳
    const std::shared_ptr<RPCHook> onlyNs = composeRequestHooks(false, nullptr, nsGetter);
    CHECK(std::dynamic_pointer_cast<NamespaceRpcHook>(onlyNs) != nullptr,
          "namespace only => the NamespaceRpcHook itself");
    CHECK(std::dynamic_pointer_cast<ChainedRPCHook>(onlyNs) == nullptr,
          "namespace only => not wrapped in a chain");
    // namespaceV2 **没接线**（两参调用点）+ 没开 stream：形态与历史完全一致
    CHECK(composeRequestHooks(false, acl).get() == acl.get(),
          "no namespace arg + no stream => the very same user hook");
    // ⚠ 判据是「有没有接线」而不是「当前值空不空」：见下一段的现读语义。
    const std::function<std::string()> notWiredAtAll;
    CHECK(composeRequestHooks(false, nullptr, notWiredAtAll) == nullptr,
          "nothing wired => no hook at all (zero overhead)");
    CHECK(composeRequestHooks(false, acl) == acl,
          "two-arg overload still returns the user hook untouched");

    // getter 接了线但当前为空：**照 Java 无条件装链**（钩子每笔请求现读，
    // start() 之后才 setNamespaceV2 也能生效），空值时钩子自己退化成 no-op
    {
        std::string live;  // 此刻还没配 namespaceV2
        const std::shared_ptr<RPCHook> wrapped =
            composeRequestHooks(false, acl, [&live] { return live; });
        auto chain = std::dynamic_pointer_cast<ChainedRPCHook>(wrapped);
        CHECK(chain && chain->hooks().size() == 2,
              "empty-but-wired namespace still installs the hook (live re-read)");
        RemotingCommand before = makeRequest({{"topic", "MyTopic"}});
        wrapped->doBeforeRequest("127.0.0.1:9876", before);
        CHECK(before.extFields.count(kNsdKey) == 0, "empty namespace => hook adds nothing");
        live = "MQINST-late";
        RemotingCommand after = makeRequest({{"topic", "MyTopic"}});
        wrapped->doBeforeRequest("127.0.0.1:9876", after);
        CHECK(after.extFields.count(kNsdKey) == 1 && after.getExtField(kNsKey) == "MQINST-late",
              "namespaceV2 set after start() still takes effect");
    }

    // 两参重载（未接线 namespace 的老调用点）形态不变
    {
        const std::shared_ptr<RPCHook> noNsArg = composeRequestHooks(true, acl);
        const auto chained = std::dynamic_pointer_cast<ChainedRPCHook>(noNsArg);
        CHECK(chained && chained->hooks().size() == 2,
              "default namespace arg => Stream + user only (no namespace hook)");
    }
}

// ---------------------------------------------------------------- (d) 签名覆盖 nsd/ns
void testAclSignatureCoversNamespace() {
    const SessionCredentials credentials("AK_TEST", "SK_TEST_SECRET_12345678");
    const Bytes body("hello", 5);

    // 1) 链式：Namespace → ACL（namespaceV2 已配）
    RemotingCommand chained = makeRequest({{"topic", "MyTopic"}});
    chained.body = body;
    chained.hasBody = true;
    composeRequestHooks(false, std::make_shared<AclClientRPCHook>(credentials),
                        getterOf("MQINST-sign"))
        ->doBeforeRequest("127.0.0.1:9876", chained);

    // 2) 参照：手工把 nsd/ns 摆进 extFields 再单独跑 ACL —— 与 broker 侧重组出来的一致
    RemotingCommand reference =
        makeRequest({{"topic", "MyTopic"}, {kNsKey, "MQINST-sign"}, {kNsdKey, "true"}});
    reference.body = body;
    reference.hasBody = true;
    AclClientRPCHook(credentials).doBeforeRequest("127.0.0.1:9876", reference);

    CHECK(chained.getExtField(SessionCredentials::SIGNATURE)
              == reference.getExtField(SessionCredentials::SIGNATURE),
          "nsd/ns sit inside the signed content (chain == manual order)");

    // 3) 不设 namespaceV2 的同一笔请求：签名必须不同（否则「命名空间」这层等于没签进去）
    RemotingCommand plain = makeRequest({{"topic", "MyTopic"}});
    plain.body = body;
    plain.hasBody = true;
    AclClientRPCHook(credentials).doBeforeRequest("127.0.0.1:9876", plain);
    const std::string plainSig = plain.getExtField(SessionCredentials::SIGNATURE);
    CHECK(!plainSig.empty(), "acl still signs without a namespace");
    CHECK(plainSig != chained.getExtField(SessionCredentials::SIGNATURE),
          "setting namespaceV2 changes the ACL signature");

    // 4) 先签名后补 nsd/ns（错误顺序）产出的签名与链式不同 —— broker 会验签失败。
    //    这一条就是 MQClientAPIImpl:329 把 Namespace 装在 ACL 之前的全部理由。
    RemotingCommand late = makeRequest({{"topic", "MyTopic"}});
    late.body = body;
    late.hasBody = true;
    AclClientRPCHook(credentials).doBeforeRequest("127.0.0.1:9876", late);
    const std::string lateSig = late.getExtField(SessionCredentials::SIGNATURE);
    late.addExtField(kNsdKey, "true");
    late.addExtField(kNsKey, "MQINST-sign");
    CHECK(lateSig == plainSig, "without a namespace the signature is the plain one");
    CHECK(lateSig != chained.getExtField(SessionCredentials::SIGNATURE),
          "signing before nsd/ns produces a different (rejected) signature");

    // 5) 签名的**内容**逐字节核对（拼接顺序 = extFields 键的字典序，只取值）：
    //    AccessKey < ns < nsd < topic（std::map 字节序；Java TreeMap 同结论），
    //    所以内容 = "AK_TEST" + "MQINST-sign" + "true" + "MyTopic" + body。
    //    这一条把「nsd/ns 真的进了签名的字节」钉死，而不是只比对自己算出来的两个值。
    CHECK(AclClientRPCHook::buildRequestContent(chained)
              == Bytes("AK_TESTMQINST-signtrueMyTopic") + body,
          "signed content is AccessKey + ns + nsd + topic + body");
    // broker 侧算法重放：拿上线的 extFields + body 再算一遍，等式成立才说明顺序对
    {
        RemotingCommand replay;
        replay.extFields = chained.extFields;
        replay.body = chained.body;
        replay.hasBody = chained.hasBody;
        CHECK(AclClientRPCHook::calcSignature(credentials.secretKey(), replay)
                  == chained.getExtField(SessionCredentials::SIGNATURE),
              "broker-side replay verifies the namespaced signature");
    }

    // 6) Namespace + Stream + ACL 三环：ReqT 也在签名之内（链序不能只对齐前两环）
    {
        RemotingCommand three = makeRequest({{"topic", "MyTopic"}});
        three.body = body;
        three.hasBody = true;
        composeRequestHooks(true, std::make_shared<AclClientRPCHook>(credentials),
                            getterOf("MQINST-sign"))
            ->doBeforeRequest("127.0.0.1:9876", three);
        RemotingCommand replay;
        replay.extFields = three.extFields;
        replay.body = three.body;
        replay.hasBody = three.hasBody;
        CHECK(AclClientRPCHook::calcSignature(credentials.secretKey(), replay)
                  == three.getExtField(SessionCredentials::SIGNATURE),
              "namespace + stream + acl: replay verifies");
        // extFields 按 key 字节序取**值**拼接：大写字母 < 大写下划线后的字母 < 小写，
        // 故 AccessKey < ReqT < ns < nsd < topic（Java TreeMap 同结论）。
        CHECK(AclClientRPCHook::buildRequestContent(three)
                  == Bytes("AK_TEST0MQINST-signtrueMyTopic") + body,
              "content order is AccessKey, ReqT, ns, nsd, topic, body");
    }
}

// ---------------------------------------------------------------- 门面配置面
// 五个门面的 setNamespaceV2/getNamespaceV2（对应 Java ClientConfig 的同一枚旋钮）。
void testFacadeConfigSurface() {
    DefaultMQProducer p("PG_nsface");
    p.setNamespaceV2("inst-p");
    CHECK(p.namespaceV2() == "inst-p", "producer exposes namespaceV2");

    DefaultMQPushConsumer c("CG_nsface");
    c.setNamespaceV2("inst-c");
    CHECK(c.namespaceV2() == "inst-c", "push consumer exposes namespaceV2");

    DefaultMQPullConsumer pc("CG_nsface_pull");
    pc.setNamespaceV2("inst-pc");
    CHECK(pc.namespaceV2() == "inst-pc", "pull consumer exposes namespaceV2");

    DefaultLitePullConsumer lc("CG_nsface_lite");
    lc.setNamespaceV2("inst-lc");
    CHECK(lc.namespaceV2() == "inst-lc", "lite pull consumer exposes namespaceV2");

    DefaultMQAdminExt admin;
    admin.setNamespaceV2("inst-ad");
    CHECK(admin.namespaceV2() == "inst-ad", "admin exposes namespaceV2");

    // 默认空串（未配置 = 钩子不加字段），且与老 namespace 互不干扰
    DefaultMQProducer fresh("PG_nsface_fresh");
    CHECK(fresh.namespaceV2().empty(), "namespaceV2 defaults to empty");
    fresh.setNamespace("legacy");
    CHECK(fresh.namespaceV2().empty() && fresh.namespaceOf() == "legacy",
          "namespaceV2 and legacy namespace are independent knobs");

    // 轨迹分发器的传导缝（Java AsyncTraceDispatcher:79/147 + start():155）
    AsyncTraceDispatcher dispatcher("PG_nsface_trace", TraceDispatcherType::PRODUCE);
    CHECK(dispatcher.namespaceV2().empty(), "dispatcher namespaceV2 defaults to empty");
    dispatcher.setNamespaceV2("inst-trace");
    CHECK(dispatcher.namespaceV2() == "inst-trace", "dispatcher carries the host namespaceV2");
}

// ---------------------------------------------------------------- socket 工具
bool readN(socket_t s, char* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        int r = static_cast<int>(::recv(s, buf + got, static_cast<int>(n - got), 0));
        if (r <= 0) return false;
        got += static_cast<size_t>(r);
    }
    return true;
}

bool writeAll(socket_t s, const Bytes& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        int n = static_cast<int>(::send(s, reinterpret_cast<const char*>(data.data() + sent),
                                        static_cast<int>(data.size() - sent),
                                        netcompat::sendFlags()));
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool readFrame(socket_t s, const std::atomic<bool>& alive, Bytes& out) {
    for (;;) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(s, &rfds);
        timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 100 * 1000;
        int ready = ::select(netcompat::selectNfds(s), &rfds, nullptr, nullptr, &tv);
        if (!alive.load()) return false;
        if (ready == 0) continue;
        if (ready < 0) return false;
        break;
    }
    char lenBuf[4];
    if (!readN(s, lenBuf, 4)) return false;
    Bytes lenStr(lenBuf, 4);
    const int32_t totalLen = ByteReader::getInt32At(lenStr, 0);
    if (totalLen <= 0 || totalLen > 8 * 1024 * 1024) return false;
    Bytes payload(static_cast<size_t>(totalLen), '\0');
    if (!readN(s, reinterpret_cast<char*>(payload.data()), static_cast<size_t>(totalLen))) {
        return false;
    }
    out = lenStr + payload;
    return true;
}

struct Conn {
    explicit Conn(socket_t s) : sock(s) {}
    socket_t sock;
    std::mutex writeM;
};

// 同时扮演 name server 与 broker：路由按登记的 topic 回，其余一律回 SUCCESS
// （SEND 补 msgId/queueId/queueOffset，消费者侧的拉取/位点回 NO_MESSAGE 形状即可，
//  本文件只取「报文里有没有 nsd/ns」这一件事）。
class MockEndpoint {
public:
    struct WireRecord {
        int32_t code = 0;
        PropertyMap ext;
        Bytes body;
        bool hasBody = false;
    };

    MockEndpoint() {
        netcompat::ensureInitialized();
        listen_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&one), static_cast<int>(sizeof(one)));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(listen_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_, 16);
        socklen_type len = sizeof(addr);
        ::getsockname(listen_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        running_.store(true);
        acceptor_ = std::thread([this]() { acceptLoop(); });
    }

    ~MockEndpoint() {
        running_.store(false);
        if (acceptor_.joinable()) acceptor_.join();
        for (;;) {
            std::vector<std::thread> pending;
            {
                std::lock_guard<std::mutex> lk(workersM_);
                pending.swap(workers_);
            }
            if (pending.empty()) break;
            for (auto& t : pending) {
                if (t.joinable()) t.join();
            }
        }
        ::shutdown(listen_, kShutdownHow);
        netcompat::closeSocket(listen_);
    }

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void addRoute(const std::string& topic, const TopicRouteData& route) {
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    // 请求码为 `code`、且带 key==value 扩展字段的报文条数
    int countRequestsWith(int32_t code, const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lk(state_);
        int n = 0;
        for (const WireRecord& r : requestLog_) {
            if (r.code != code) continue;
            auto it = r.ext.find(key);
            if (it != r.ext.end() && it->second == value) ++n;
        }
        return n;
    }

    int countRequests(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        int n = 0;
        for (const WireRecord& r : requestLog_) {
            if (r.code == code) ++n;
        }
        return n;
    }

    WireRecord lastRequest(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        for (auto it = requestLog_.rbegin(); it != requestLog_.rend(); ++it) {
            if (it->code == code) return *it;
        }
        return WireRecord{};
    }

private:
    void acceptLoop() {
        while (running_.load()) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(listen_, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            int ready = ::select(netcompat::selectNfds(listen_), &rfds, nullptr, nullptr, &tv);
            if (ready == 0) continue;
            if (ready < 0) break;
            socket_t sock = ::accept(listen_, nullptr, nullptr);
            if (sock == kInvalidSocket) break;
            auto conn = std::make_shared<Conn>(sock);
            std::lock_guard<std::mutex> lk(workersM_);
            workers_.emplace_back([this, conn]() { serveConn(conn); });
        }
    }

    void serveConn(const std::shared_ptr<Conn>& conn) {
        while (running_.load()) {
            Bytes frame;
            if (!readFrame(conn->sock, running_, frame)) break;
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) break;
            respond(conn, req);
        }
        ::shutdown(conn->sock, kShutdownHow);
        netcompat::closeSocket(conn->sock);
    }

    void respond(const std::shared_ptr<Conn>& conn, const RemotingCommand& req) {
        {
            std::lock_guard<std::mutex> lk(state_);
            requestLog_.push_back({req.code, req.extFields, req.body, req.hasBody});
        }
        RemotingCommand resp;
        resp.opaque = req.opaque;
        resp.markResponseType();

        if (req.code == RequestCode::GET_ROUTEINFO_BY_TOPIC) {
            const std::string topic =
                req.extFields.count("topic") ? req.extFields.at("topic") : std::string();
            Bytes body;
            bool found = false;
            {
                std::lock_guard<std::mutex> lk(state_);
                auto it = routes_.find(topic);
                if (it != routes_.end()) {
                    body = it->second;
                    found = true;
                }
            }
            resp.code = found ? ResponseCode::SUCCESS : ResponseCode::TOPIC_NOT_EXIST;
            if (found) {
                resp.body = body;
                resp.hasBody = true;
            }
        } else if (req.code == RequestCode::SEND_MESSAGE_V2
                   || req.code == RequestCode::SEND_MESSAGE) {
            resp.code = ResponseCode::SUCCESS;
            resp.extFields["msgId"] = "AC10000100001234567890ABCDEF0001";
            resp.extFields["queueId"] = "0";
            resp.extFields["queueOffset"] = "1000";
        } else {
            if (req.isOnewayRpc()) return;
            resp.code = ResponseCode::SUCCESS;
        }
        const Bytes out = resp.encode();
        std::lock_guard<std::mutex> lk(conn->writeM);
        writeAll(conn->sock, out);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<WireRecord> requestLog_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

TopicRouteData makeRoute(const std::string& brokerAddr, int brokers, int queuesPerBroker) {
    TopicRouteData route;
    for (int b = 0; b < brokers; ++b) {
        const std::string name = "broker-" + std::string(1, static_cast<char>('a' + b));
        route.queueDatas.emplace_back(name, queuesPerBroker, queuesPerBroker, 6, 0);
        std::map<int64_t, std::string> addrs;
        addrs[0] = brokerAddr;
        route.brokerDatas.emplace_back("DefaultCluster", name, addrs);
    }
    return route;
}

Message plainMessage(const std::string& topic) {
    Message m(topic, Bytes{'h', 'i'});
    m.setTags("T1");
    return m;
}

// 推送消费者的 start() 按 Java checkConfig:1067 要求 listener 就位（本文件只取报文，
// 不关心消费结果）。
class NoopListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

// 每个门面用**独立 mock + 独立 instanceName**：
//   * 独立 mock —— 上一个门面的后台路由刷新线程可能在 shutdown() 之后仍打到同一个端点，
//     共用一个 mock 会把「别人的报文」算进这一家的账（真机不存在这问题，同进程多门面
//     才会）；端点随作用域销毁，报文账本就只属于这一家。
//   * 独立 instanceName —— 同进程 clientId 相同会复用同一个 MQClientInstance，而钩子槽是
//     first-wins，复用会把后面门面的钩子静默吞掉。
// 判据是「GET_ROUTEINFO 的总条数 == 带 ns=<值> 的条数 == 带 nsd=true 的条数」：
// 钩子装在实例上，本门面的**每一笔**报文（start 期间的路由/心跳/注销）都必须打标。
void checkAllRequestsMarked(MockEndpoint& mock, const std::string& tag, bool expectReqT) {
    const int total = mock.countRequests(RequestCode::GET_ROUTEINFO_BY_TOPIC);
    const int marked =
        mock.countRequestsWith(RequestCode::GET_ROUTEINFO_BY_TOPIC, kNsKey, "MQINST-wire");
    const int markedNsd =
        mock.countRequestsWith(RequestCode::GET_ROUTEINFO_BY_TOPIC, kNsdKey, "true");
    const int markedReqT =
        mock.countRequestsWith(RequestCode::GET_ROUTEINFO_BY_TOPIC, MixAll::REQ_T, "0");
    CHECK(total > 0, tag + ": facade talked to the namesrv");
    CHECK(marked == total, tag + ": every request carries ns=<namespaceV2>");
    CHECK(markedNsd == total, tag + ": every request carries nsd=true");
    // 开着 stream 的门面：Namespace 之后还有 Stream（链序 Namespace → Stream → 用户）
    CHECK(!expectReqT || markedReqT == total, tag + ": ReqT follows the namespace hook");
}

void testFacadesPutNamespaceOnTheWire() {
    const std::string topic = "NsV2WireTopic";

    // 1. 生产者：发送报文 + broker 侧算法重放验签（nsd/ns 必须在签名之内）
    {
        MockEndpoint mock;
        mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
        DefaultMQProducer p("PG_nsv2_wire");
        p.setNamesrvAddr(mock.address());
        p.setInstanceName("nsv2-producer");
        p.setNamespaceV2("MQINST-wire");
        p.setCredentials("AK-wire", "SK-wire");
        p.start();
        p.send(plainMessage(topic), 3000);
        p.shutdown();

        const auto rec = mock.lastRequest(RequestCode::SEND_MESSAGE_V2);
        CHECK(rec.code == RequestCode::SEND_MESSAGE_V2, "producer send hit the mock broker");
        CHECK(rec.ext.count(kNsKey) == 1 && rec.ext.at(kNsKey) == "MQINST-wire",
              "producer: ns on the wire");
        CHECK(rec.ext.count(kNsdKey) == 1 && rec.ext.at(kNsdKey) == "true",
              "producer: nsd on the wire");
        RemotingCommand replay;
        replay.extFields = rec.ext;
        replay.body = rec.body;
        replay.hasBody = rec.hasBody;
        CHECK(AclClientRPCHook::calcSignature("SK-wire", replay)
                  == rec.ext.at(std::string(SessionCredentials::SIGNATURE)),
              "producer: broker-side replay verifies the namespaced signature");
        // 生产者默认关 stream（Java DefaultMQProducer 从不置 enableStreamRequestType）
        checkAllRequestsMarked(mock, "producer", false);
        CHECK(mock.countRequestsWith(RequestCode::GET_ROUTEINFO_BY_TOPIC, MixAll::REQ_T, "0") == 0,
              "producer: stream off => no ReqT (namespace hook installed independently)");
    }

    // 2. 推送消费者（默认关 stream）
    {
        MockEndpoint mock;
        mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
        DefaultMQPushConsumer c("CG_nsv2_wire");
        c.setNamesrvAddr(mock.address());
        c.setInstanceName("nsv2-push");
        c.setNamespaceV2("MQINST-wire");
        c.subscribe(topic, "*");
        c.setMessageListener(std::make_shared<NoopListener>());
        c.start();
        c.shutdown();
        checkAllRequestsMarked(mock, "push consumer", false);
    }

    // 3. 拉模式消费者（默认开 stream：链序必须是 Namespace → Stream）
    //    registerTopics 是**只读**访问器，登记入口是 registerMessageQueueListener（需非空
    //    监听器）；本用例不关心订阅，直接调 fetchSubscribeMessageQueues —— 它必然发一笔
    //    GET_ROUTEINFO_BY_TOPIC，报文账本可判定。
    {
        MockEndpoint mock;
        mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
        DefaultMQPullConsumer c("CG_nsv2_wire_pull");
        c.setNamesrvAddr(mock.address());
        c.setInstanceName("nsv2-pull");
        c.setNamespaceV2("MQINST-wire");
        c.start();
        c.fetchSubscribeMessageQueues(topic);
        c.shutdown();
        checkAllRequestsMarked(mock, "pull consumer", true);
    }

    // 4. 轻量拉取消费者（曾经绕过 composeRequestHooks 的那一处回归）
    {
        MockEndpoint mock;
        mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
        DefaultLitePullConsumer c("CG_nsv2_wire_lite");
        c.setNamesrvAddr(mock.address());
        c.setInstanceName("nsv2-lite");
        c.setNamespaceV2("MQINST-wire");
        c.subscribe(topic, "*");
        c.start();
        c.shutdown();
        checkAllRequestsMarked(mock, "lite pull consumer", true);
    }

    // 5. 管理端
    {
        MockEndpoint mock;
        mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
        DefaultMQAdminExt admin;
        admin.setNamesrvAddr(mock.address());
        admin.setInstanceName("nsv2-admin");
        admin.setNamespaceV2("MQINST-wire");
        admin.start();
        const int total = mock.countRequests(RequestCode::GET_ROUTEINFO_BY_TOPIC);
        const int marked =
            mock.countRequestsWith(RequestCode::GET_ROUTEINFO_BY_TOPIC, kNsKey, "MQINST-wire");
        CHECK(total == 0, "admin start itself asks no route (Java DefaultMQAdminExtImpl:161-172)");
        CHECK(marked == total, "admin: nothing unsigned before it talks");
        admin.examineTopicRoute(topic);
        checkAllRequestsMarked(mock, "admin", false);
        admin.shutdown();
    }
}

}  // namespace

int main() {
    testHookAddsNamespaceFields();
    testEmptyNamespaceLeavesExtFieldsUntouched();
    testCompositionOrderNamespaceThenStreamThenAcl();
    testAclSignatureCoversNamespace();
    testFacadeConfigSurface();
    testFacadesPutNamespaceOnTheWire();

    std::cout << "namespace_hook: PASS=" << g_pass << " FAIL=" << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
