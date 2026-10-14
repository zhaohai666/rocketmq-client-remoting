// Unit tests for NamespaceRpcHook (the 5.x new-namespace / Aliyun instance-ID form).
//
//
// Covers four things:
//   (a) the hook adds nsd=true / ns=<value>;
//   (b) with an empty namespace extFields stays untouched (no key is materialized);
//   (c) composition order Namespace → Stream → ACL (including the shape with one / zero hooks);
//   (d) once namespaceV2 is set the ACL signature changes accordingly (a broker-side algorithm replay verifies it).
//
// The second half takes evidence from the socket side: all five facades (producer / push / pull / lite-pull / admin)
// really put nsd/ns on the chain — unit tests of composeRequestHooks alone cannot lock down whether a facade uses it,
// and the lite-consumer site was once exactly such a regression (see the same-shaped case 9 in test_send_retry.cpp).
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

// A probe hook recording what it saw when invoked, used to prove chain order (a probe running first cannot see later fields).
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

// ---------------------------------------------------------------- (a) namespaceV2 configured
void testHookAddsNamespaceFields() {
    RemotingCommand cmd = pullRequest();
    NamespaceRpcHook hook(getterOf("MQINST-abc"));
    hook.doBeforeRequest("127.0.0.1:9876", cmd);

    CHECK(cmd.getExtField(kNsdKey) == "true", "nsd=true injected");
    CHECK(cmd.getExtField(kNsKey) == "MQINST-abc", "ns=<namespaceV2> injected");
    CHECK(cmd.extFields.size() == 2, "exactly the two namespace fields are added");
    CHECK(kNsdKey == "nsd", "MixAll nsd key spelling");
    CHECK(kNsKey == "ns", "MixAll ns key spelling");

    // pre-existing fields are preserved as-is (the hook only adds, it never rewrites others' keys)
    RemotingCommand withExt = makeRequest({{"topic", "MyTopic"}});
    hook.doBeforeRequest("127.0.0.1:9876", withExt);
    CHECK(withExt.getExtField("topic") == "MyTopic", "pre-existing extFields survive");
    CHECK(withExt.extFields.size() == 3, "old + nsd + ns");

    // repeated calls are idempotent (every request passes the hooks; running the same request twice must not grow a third key)
    hook.doBeforeRequest("127.0.0.1:9876", withExt);
    CHECK(withExt.extFields.size() == 3, "addExtField overwrite, not append");

    RemotingCommand resp = RemotingCommand::createResponseCommand(
        ResponseCode::SUCCESS, "ok");
    hook.doAfterResponse("127.0.0.1:9876", cmd, &resp);
    CHECK(resp.extFields.empty(), "doAfterResponse leaves the response untouched");

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

void testEmptyNamespaceLeavesExtFieldsUntouched() {
    RemotingCommand cmd = pullRequest();
    const size_t before = cmd.extFields.size();
    NamespaceRpcHook hook(getterOf(""));
    hook.doBeforeRequest("127.0.0.1:9876", cmd);
    CHECK(cmd.extFields.size() == before, "empty namespace => extFields unchanged");
    CHECK(cmd.extFields.empty(), "empty namespace => no field materialized");
    CHECK(cmd.extFields.count(kNsdKey) == 0, "empty namespace => no nsd key");
    CHECK(cmd.extFields.count(kNsKey) == 0, "empty namespace => no ns key");

    // existing extFields must not be touched either
    RemotingCommand withExt = makeRequest({{"topic", "MyTopic"}, {"producerGroup", "G"}});
    hook.doBeforeRequest("127.0.0.1:9876", withExt);
    CHECK(withExt.extFields.size() == 2, "empty namespace => existing keys untouched");
    CHECK(withExt.getExtField("topic") == "MyTopic", "empty namespace => topic still MyTopic");

    // an empty / absent getter (the shape when the facade did not configure namespaceV2) likewise does nothing
    RemotingCommand noGetter = pullRequest();
    const std::function<std::string()> noNamespaceWired;
    NamespaceRpcHook emptyHook(noNamespaceWired);
    emptyHook.doBeforeRequest("127.0.0.1:9876", noGetter);
    CHECK(noGetter.extFields.empty(), "null getter behaves like an empty namespace");
}

// ---------------------------------------------------------------- (c) chain order
void testCompositionOrderNamespaceThenStreamThenAcl() {
    auto acl = std::make_shared<AclClientRPCHook>(SessionCredentials("AK", "SK"));
    const std::function<std::string()> nsGetter = getterOf("MQINST-order");

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

        // behavioral evidence: when ACL runs, nsd/ns/ReqT are all in place (Namespace is again before Stream).
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

        // the reverse order (sign first, tag later) is the bug: the signed content contains no nsd/ns.
        auto signer = std::make_shared<ProbeHook>("acl");
        ChainedRPCHook reversed{std::vector<std::shared_ptr<RPCHook>>{
            signer, std::make_shared<NamespaceRpcHook>(nsGetter)}};
        RemotingCommand wrong = makeRequest({{"topic", "MyTopic"}});
        reversed.doBeforeRequest("127.0.0.1:9876", wrong);
        CHECK(!signer->sawNsd, "reversed order: signer runs before nsd exists");
        CHECK(wrong.extFields.count(kNsdKey) == 1, "reversed order still puts nsd on the wire");
    }

    // only namespaceV2 configured (no stream, no ACL): a single hook, not wrapped in a chain
    const std::shared_ptr<RPCHook> onlyNs = composeRequestHooks(false, nullptr, nsGetter);
    CHECK(std::dynamic_pointer_cast<NamespaceRpcHook>(onlyNs) != nullptr,
          "namespace only => the NamespaceRpcHook itself");
    CHECK(std::dynamic_pointer_cast<ChainedRPCHook>(onlyNs) == nullptr,
          "namespace only => not wrapped in a chain");
    // namespaceV2 **not wired** (two-arg call site) + no stream: the shape is exactly as before
    CHECK(composeRequestHooks(false, acl).get() == acl.get(),
          "no namespace arg + no stream => the very same user hook");
    // ⚠ the criterion is whether it is wired, not whether the current value is empty: see the live re-read semantics below.
    const std::function<std::string()> notWiredAtAll;
    CHECK(composeRequestHooks(false, nullptr, notWiredAtAll) == nullptr,
          "nothing wired => no hook at all (zero overhead)");
    CHECK(composeRequestHooks(false, acl) == acl,
          "two-arg overload still returns the user hook untouched");

    {
        std::string live;  // namespaceV2 not configured yet at this point
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

    // the two-arg overload (old call sites without a wired namespace) keeps its shape
    {
        const std::shared_ptr<RPCHook> noNsArg = composeRequestHooks(true, acl);
        const auto chained = std::dynamic_pointer_cast<ChainedRPCHook>(noNsArg);
        CHECK(chained && chained->hooks().size() == 2,
              "default namespace arg => Stream + user only (no namespace hook)");
    }
}

// ---------------------------------------------------------------- (d) the signature covers nsd/ns
void testAclSignatureCoversNamespace() {
    const SessionCredentials credentials("AK_TEST", "SK_TEST_SECRET_12345678");
    const Bytes body("hello", 5);

    // 1) chained: Namespace → ACL (namespaceV2 configured)
    RemotingCommand chained = makeRequest({{"topic", "MyTopic"}});
    chained.body = body;
    chained.hasBody = true;
    composeRequestHooks(false, std::make_shared<AclClientRPCHook>(credentials),
                        getterOf("MQINST-sign"))
        ->doBeforeRequest("127.0.0.1:9876", chained);

    // 2) reference: manually place nsd/ns into extFields then run ACL alone — matches what the broker reassembles
    RemotingCommand reference =
        makeRequest({{"topic", "MyTopic"}, {kNsKey, "MQINST-sign"}, {kNsdKey, "true"}});
    reference.body = body;
    reference.hasBody = true;
    AclClientRPCHook(credentials).doBeforeRequest("127.0.0.1:9876", reference);

    CHECK(chained.getExtField(SessionCredentials::SIGNATURE)
              == reference.getExtField(SessionCredentials::SIGNATURE),
          "nsd/ns sit inside the signed content (chain == manual order)");

    // 3) the same request without namespaceV2: the signature must differ (otherwise the namespace layer is effectively unsigned)
    RemotingCommand plain = makeRequest({{"topic", "MyTopic"}});
    plain.body = body;
    plain.hasBody = true;
    AclClientRPCHook(credentials).doBeforeRequest("127.0.0.1:9876", plain);
    const std::string plainSig = plain.getExtField(SessionCredentials::SIGNATURE);
    CHECK(!plainSig.empty(), "acl still signs without a namespace");
    CHECK(plainSig != chained.getExtField(SessionCredentials::SIGNATURE),
          "setting namespaceV2 changes the ACL signature");

    // 4) signing first and adding nsd/ns afterwards (wrong order) yields a signature different from the chain — the broker's verification fails.
    //    That is the whole reason the signing chain installs Namespace before ACL.
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

    CHECK(AclClientRPCHook::buildRequestContent(chained)
              == Bytes("AK_TESTMQINST-signtrueMyTopic") + body,
          "signed content is AccessKey + ns + nsd + topic + body");
    // broker-side algorithm replay: recompute from the on-the-wire extFields + body; the equality only holds when the order is right
    {
        RemotingCommand replay;
        replay.extFields = chained.extFields;
        replay.body = chained.body;
        replay.hasBody = chained.hasBody;
        CHECK(AclClientRPCHook::calcSignature(credentials.secretKey(), replay)
                  == chained.getExtField(SessionCredentials::SIGNATURE),
              "broker-side replay verifies the namespaced signature");
    }

    // 6) the three-ring Namespace + Stream + ACL chain: ReqT is inside the signature too (order must align all three, not just the first two)
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
        CHECK(AclClientRPCHook::buildRequestContent(three)
                  == Bytes("AK_TEST0MQINST-signtrueMyTopic") + body,
              "content order is AccessKey, ReqT, ns, nsd, topic, body");
    }
}

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

    // defaults to an empty string (unconfigured = the hook adds nothing), and it is independent from the legacy namespace
    DefaultMQProducer fresh("PG_nsface_fresh");
    CHECK(fresh.namespaceV2().empty(), "namespaceV2 defaults to empty");
    fresh.setNamespace("legacy");
    CHECK(fresh.namespaceV2().empty() && fresh.namespaceOf() == "legacy",
          "namespaceV2 and legacy namespace are independent knobs");

    AsyncTraceDispatcher dispatcher("PG_nsface_trace", TraceDispatcherType::PRODUCE);
    CHECK(dispatcher.namespaceV2().empty(), "dispatcher namespaceV2 defaults to empty");
    dispatcher.setNamespaceV2("inst-trace");
    CHECK(dispatcher.namespaceV2() == "inst-trace", "dispatcher carries the host namespaceV2");
}

// ---------------------------------------------------------------- socket utilities
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

// Plays both the name server and the broker: routes answer for registered topics, everything else answers SUCCESS
// (SEND carries msgId/queueId/queueOffset; consumer-side pull/offset replies in the NO_MESSAGE shape are fine;
//  this file only evidences whether the wire request carries nsd/ns.
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

    // number of wire requests with request code `code` and ext field key==value
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

class NoopListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

// Every facade uses **its own mock + its own instanceName**:
//   * its own mock — the previous facade's background route-refresh thread may still hit the same endpoint
//     after shutdown(); sharing one mock would charge other facades' traffic to this one (this problem does not
//     exist against a real cluster, only with multiple facades in one process); the endpoint dies with its scope.
//   * its own instanceName — the same clientId in one process reuses the same MQClientInstance, and the
//     hook slot is first-wins, so reuse would silently swallow the later facade's hook.
// the criterion is: total GET_ROUTEINFO count == count carrying ns=<value> == count carrying nsd=true:
// the hook sits on the instance, so every request of this facade (route/heartbeat/unregister during start()) must be marked.
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
    // facades with stream on: Stream still follows Namespace (chain order Namespace → Stream → user)
    CHECK(!expectReqT || markedReqT == total, tag + ": ReqT follows the namespace hook");
}

void testFacadesPutNamespaceOnTheWire() {
    const std::string topic = "NsV2WireTopic";

    // 1. producer: the send request + a broker-side algorithm replay verifies the signature (nsd/ns must be inside it)
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
        checkAllRequestsMarked(mock, "producer", false);
        CHECK(mock.countRequestsWith(RequestCode::GET_ROUTEINFO_BY_TOPIC, MixAll::REQ_T, "0") == 0,
              "producer: stream off => no ReqT (namespace hook installed independently)");
    }

    // 2. push consumer (stream off by default)
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

    // 3. pull consumer (stream on by default: the chain order must be Namespace → Stream)
    //    registerTopics is a **read-only** accessor; the registration entry is registerMessageQueueListener (needs a non-null
    //    listener); this case does not care about subscription and calls fetchSubscribeMessageQueues directly — it necessarily
    //    sends one GET_ROUTEINFO_BY_TOPIC, which the wire ledger can decide.
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

    // 4. lite pull consumer (the site that once bypassed composeRequestHooks)
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

    // 5. admin
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
        CHECK(total == 0, "admin start itself asks no route");
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
