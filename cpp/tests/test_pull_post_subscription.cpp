// P5：postSubscriptionWhenPull + pullFromWhichNode / findBrokerAddressInSubscribe 离线单测。
//
// 对齐基准（Java 5.5.0，逐行读过；与 python/tests/test_pull_post_subscription.py、
// rust/src/client/consumer.rs + mq_client.rs、csharp/Tests/PullPostSubscriptionTests.cs 同题）：
//   * DefaultMQPushConsumer#postSubscriptionWhenPull 默认 false；
//     DefaultMQPushConsumerImpl.pullMessage:458-468 里
//     `subExpression = (postSubscriptionWhenPull && !sd.isClassFilterMode()) ? sd.getSubString() : null`，
//     sysFlag 的 SUBSCRIPTION 位 = `subExpression != null`。默认关闭是安全的：tag 过滤由客户端
//     filterMessagesForDelivery 兜底。
//   * PullAPIWrapper#pullKernelImpl:197-205 用 recalculatePullFromWhichNode(mq) 调
//     MQClientInstance#findBrokerAddressInSubscribe:1307-1336；命中从节点时
//     PullSysFlag.clearCommitOffsetFlag(:219-221) —— 从节点不维护消费位点。
//   * PullAPIWrapper#processPullResult:77 用应答头的 suggestWhichBrokerId 回写
//     pullFromWhichNodeTable（:157-164 的 updatePullFromWhichNode）。
//
// 报文形状（SUBSCRIPTION 位、`subscription` 键是否上线、打给 master 还是 slave、
// COMMIT_OFFSET 是否被清）用进程内假端点从 socket 上取证，不靠调用方自说自话。
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"

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

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

void expectInt(long long actual, long long expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        std::printf("FAIL %s (actual=%lld expected=%lld)\n", name.c_str(), actual, expected);
    }
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
    Bytes body(static_cast<size_t>(totalLen), '\0');
    if (!readN(s, reinterpret_cast<char*>(body.data()), static_cast<size_t>(totalLen))) {
        return false;
    }
    out = lenStr + body;
    return true;
}

// ---------------------------------------------------------------- 假端点（name server + broker）

// 一条拉取请求的取证：上线原样的 extFields
struct PullRecord {
    PropertyMap ext;
};

// 同时扮演 name server（回路由）与 broker（回 PULL_MESSAGE）。
// 与 test_check_client_config.cpp 的 MockBroker 同一套 socket 骨架，只是这里关心的是拉取。
class MockEndpoint {
public:
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
        netcompat::closeSocket(listen_);
    }

    MockEndpoint(const MockEndpoint&) = delete;
    MockEndpoint& operator=(const MockEndpoint&) = delete;

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void addRoute(const std::string& topic, const TopicRouteData& route) {
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    // 脚本化 PULL_MESSAGE 的应答头（nextBeginOffset / maxOffset / suggestWhichBrokerId…）
    void scriptPullResponse(const PropertyMap& ext) {
        std::lock_guard<std::mutex> lk(state_);
        pullReply_ = ext;
        pulls_.clear();
    }

    size_t pullCount() {
        std::lock_guard<std::mutex> lk(state_);
        return pulls_.size();
    }

    PropertyMap pullExt(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= pulls_.size()) return PropertyMap();
        return pulls_[i].ext;
    }

    bool pullHasKey(size_t i, const std::string& key) {
        PropertyMap ext = pullExt(i);
        return ext.find(key) != ext.end();
    }

    static int64_t extInt(const PropertyMap& ext, const std::string& key, int64_t fallback = -1) {
        auto it = ext.find(key);
        if (it == ext.end()) return fallback;
        try {
            return std::stoll(it->second);
        } catch (...) {
            return fallback;
        }
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
            std::lock_guard<std::mutex> lk(workersM_);
            workers_.emplace_back([this, sock]() { serveConn(sock); });
        }
    }

    void serveConn(socket_t sock) {
        Bytes frame;
        while (running_.load() && readFrame(sock, running_, frame)) {
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) break;
            RemotingCommand resp;
            resp.opaque = req.opaque;
            resp.markResponseType();
            if (req.code == RequestCode::GET_ROUTEINFO_BY_TOPIC) {
                const std::string topic =
                    req.extFields.count("topic") ? req.extFields.at("topic") : std::string();
                std::lock_guard<std::mutex> lk(state_);
                auto it = routes_.find(topic);
                if (it != routes_.end()) {
                    resp.code = ResponseCode::SUCCESS;
                    resp.body = it->second;
                    resp.hasBody = true;
                } else {
                    resp.code = ResponseCode::TOPIC_NOT_EXIST;
                }
            } else if (req.code == RequestCode::PULL_MESSAGE) {
                PropertyMap reply;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    pulls_.push_back(PullRecord{req.extFields});
                    reply = pullReply_;
                }
                resp.code = ResponseCode::SUCCESS;
                resp.extFields = reply;
            } else {
                // 心跳之类别来烦测试
                resp.code = ResponseCode::SUCCESS;
            }
            Bytes out = resp.encode();
            if (!writeAll(sock, out)) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    PropertyMap pullReply_;
    std::vector<PullRecord> pulls_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- 脚手架

const char* kGroup = "GID_P5Unit";
const char* kTopic = "P5PullTopic";
const char* kBroker = "broker-a";
const char* kClientId = "127.0.0.1@5555#1";

TopicRouteData routeWithMasterAndSlave(const std::string& masterAddr, const std::string& slaveAddr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = masterAddr;
    addrs[MixAll::MASTER_ID + 1] = slaveAddr;
    route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
    return route;
}

TopicRouteData routeWithMasterOnly(const std::string& masterAddr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = masterAddr;
    route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
    return route;
}

PropertyMap pullRespExt(bool withSuggest, int64_t suggest, int64_t nextOffset = 1) {
    PropertyMap ext;
    ext["nextBeginOffset"] = std::to_string(nextOffset);
    ext["minOffset"] = "0";
    ext["maxOffset"] = "10";
    if (withSuggest) ext["suggestWhichBrokerId"] = std::to_string(suggest);
    return ext;
}

// 起实例并把 kTopic 的路由塞进缓存（真 RPC 走一趟 mock name server）
std::unique_ptr<MQClientInstance> seeded(MockEndpoint& namesrv) {
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{namesrv.address()});
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    return instance;
}

// ---------------------------------------------------------------- 1. findBrokerAddressInSubscribe

// Java findBrokerAddressInSubscribe:1307-1336 的四个分支。isSlave 一律按**命中的 id** 判，
// 不是传进来的 brokerId。
void testFindBrokerAddressInSubscribeBranches() {
    std::map<int64_t, std::string> addrs;
    addrs[0] = "127.0.0.1:10911";
    addrs[1] = "127.0.0.1:10921";
    addrs[2] = "127.0.0.1:10931";

    auto hit = MQClientInstance::findBrokerAddressInSubscribe(addrs, 0);
    expect(hit.first == "127.0.0.1:10911" && !hit.second, "命中 master：地址直取、isSlave=false");
    hit = MQClientInstance::findBrokerAddressInSubscribe(addrs, 1);
    expect(hit.first == "127.0.0.1:10921" && hit.second, "命中 slave=1：isSlave=true");

    // 从节点 id 缺失：按 id+1 再试（Java 的从节点编号约定）
    std::map<int64_t, std::string> sparse;
    sparse[0] = "127.0.0.1:10911";
    sparse[2] = "127.0.0.1:10931";
    hit = MQClientInstance::findBrokerAddressInSubscribe(sparse, 1);
    expect(hit.first == "127.0.0.1:10931" && hit.second, "slave id 缺失按 id+1 再试");

    // 都不命中且不限定时回退到 id 最小的（Java 取 map 首项，这里取确定性形态）
    hit = MQClientInstance::findBrokerAddressInSubscribe(addrs, 9);
    expect(hit.first == "127.0.0.1:10911" && !hit.second, "brokerId 超范围回退到 id 最小者（master）");
    std::map<int64_t, std::string> onlySlave;
    onlySlave[1] = "127.0.0.1:10921";
    hit = MQClientInstance::findBrokerAddressInSubscribe(onlySlave, 3);
    expect(hit.first == "127.0.0.1:10921" && hit.second, "只有从节点时回退到它并判为 slave");

    // onlyThisBroker：宁缺毋滥
    hit = MQClientInstance::findBrokerAddressInSubscribe(addrs, 9, /*onlyThisBroker=*/true);
    expect(hit.first.empty() && !hit.second, "onlyThisBroker 时不回退");
    hit = MQClientInstance::findBrokerAddressInSubscribe({}, 0);
    expect(hit.first.empty() && !hit.second, "空地址表返回空");
}

// ---------------------------------------------------------------- 2. 线上报文：选从节点

// brokerId=1（从节点）：请求打到 slave；COMMIT_OFFSET 位被清掉（Java :219-221）；
// SUBSCRIPTION 位关闭时 `subscription` 键整个不上线（Java 的 subExpression=null → 丢字段）。
void testPullToSlaveClearsCommitOffsetAndOmitsSubscription() {
    MockEndpoint namesrv;
    MockEndpoint master;
    MockEndpoint slave;
    namesrv.addRoute(kTopic, routeWithMasterAndSlave(master.address(), slave.address()));
    slave.scriptPullResponse(pullRespExt(/*withSuggest=*/true, 0));

    auto instance = seeded(namesrv);
    MessageQueue mq(kTopic, kBroker, 0);
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/true, /*suspend=*/true,
                                                      /*subscription=*/false,
                                                      /*classFilter=*/false);
    expect(PullSysFlag::hasCommitOffsetFlag(sysFlag), "前置：sysFlag 带 COMMIT_OFFSET");

    PullResult result = instance->pullMessage(
        kGroup, mq, 0, 32, sysFlag, /*commitOffset=*/0, "TagA", 0, ExpressionType::TAG,
        /*timeoutMillis=*/3000, /*maxMsgBytes=*/-1, /*suspendTimeoutMillis=*/15000,
        /*addr=*/std::string(), /*requestSource=*/0, /*brokerId=*/MixAll::MASTER_ID + 1);

    expectInt(static_cast<long long>(slave.pullCount()), 1, "拉取打到 slave");
    expectInt(static_cast<long long>(master.pullCount()), 0, "master 没收到");

    PropertyMap sent = slave.pullExt(0);
    const int64_t sentFlag = MockEndpoint::extInt(sent, "sysFlag");
    expect(!PullSysFlag::hasCommitOffsetFlag(static_cast<int32_t>(sentFlag)),
           "slave 上一次 COMMIT_OFFSET 都没有意义（Java :219-221）");
    expect(PullSysFlag::hasSuspendFlag(static_cast<int32_t>(sentFlag)), "suspend 位保持");
    expect(!slave.pullHasKey(0, "subscription"),
           "SUBSCRIPTION 位关闭时 subscription 键根本不进 extFields");
    expect(result.suggestWhichBrokerId.has_value() && *result.suggestWhichBrokerId == 0,
           "应答头 suggestWhichBrokerId 透传给调用方");
}

// ---------------------------------------------------------------- 3. 线上报文：选主节点

// brokerId=0（主节点）：打到 master、COMMIT_OFFSET 保留；SUBSCRIPTION 位置位时
// `subscription` 才上线；老 broker 不带 suggestWhichBrokerId → 透传「没有」。
void testPullToMasterKeepsCommitOffsetAndPostsSubscription() {
    MockEndpoint namesrv;
    MockEndpoint master;
    MockEndpoint slave;
    namesrv.addRoute(kTopic, routeWithMasterAndSlave(master.address(), slave.address()));
    master.scriptPullResponse(pullRespExt(/*withSuggest=*/false, 0, /*nextOffset=*/7));

    auto instance = seeded(namesrv);
    MessageQueue mq(kTopic, kBroker, 0);
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/true, /*suspend=*/true,
                                                      /*subscription=*/true,
                                                      /*classFilter=*/false);

    PullResult result = instance->pullMessage(
        kGroup, mq, 3, 32, sysFlag, /*commitOffset=*/0, "TagA||TagB", 0, ExpressionType::TAG,
        /*timeoutMillis=*/3000, /*maxMsgBytes=*/-1, /*suspendTimeoutMillis=*/15000,
        /*addr=*/std::string(), /*requestSource=*/0, /*brokerId=*/MixAll::MASTER_ID);

    expectInt(static_cast<long long>(master.pullCount()), 1, "拉取打到 master");
    expectInt(static_cast<long long>(slave.pullCount()), 0, "slave 没收到");

    PropertyMap sent = master.pullExt(0);
    const int64_t sentFlag = MockEndpoint::extInt(sent, "sysFlag");
    expect(PullSysFlag::hasCommitOffsetFlag(static_cast<int32_t>(sentFlag)), "master 上位点照提交");
    expect(PullSysFlag::hasSubscriptionFlag(static_cast<int32_t>(sentFlag)), "subscription 位置位");
    auto subIt = sent.find("subscription");
    expect(subIt != sent.end() && subIt->second == "TagA||TagB", "subscription 内容上线");
    expectInt(MockEndpoint::extInt(sent, "queueOffset"), 3, "queueOffset 上线");
    expect(!result.suggestWhichBrokerId.has_value(), "老 broker 不带 suggest → 透传「没有」");
    expectInt(result.nextBeginOffset, 7, "nextBeginOffset 透传");
}

// ---------------------------------------------------------------- 4. 从节点缺席 → 回退主节点

// Java 的 findBrokerAddressInSubscribe 在 onlyThisBroker=false 时会**回退到主节点**，
// 所以 brokerId=3 而表里只有 master 时必须打到 master 而不是抛异常；且 isSlave 按命中的
// id 判 → COMMIT_OFFSET 位**保留**。
void testMissingSlaveFallsBackToMasterAndKeepsCommitOffset() {
    MockEndpoint namesrv;
    MockEndpoint master;
    namesrv.addRoute(kTopic, routeWithMasterOnly(master.address()));
    master.scriptPullResponse(pullRespExt(/*withSuggest=*/false, 0));

    auto instance = seeded(namesrv);
    MessageQueue mq(kTopic, kBroker, 0);
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/true, /*suspend=*/true,
                                                      /*subscription=*/false,
                                                      /*classFilter=*/false);

    instance->pullMessage(kGroup, mq, 0, 32, sysFlag, /*commitOffset=*/0, "TagA", 0,
                          ExpressionType::TAG, 3000, -1, 15000, std::string(), 0,
                          /*brokerId=*/3);

    expectInt(static_cast<long long>(master.pullCount()), 1, "从节点缺席时回退到 master");
    const int64_t sentFlag = MockEndpoint::extInt(master.pullExt(0), "sysFlag");
    expect(PullSysFlag::hasCommitOffsetFlag(static_cast<int32_t>(sentFlag)),
           "回退到的是 master，位点照提交（isSlave 按命中的 id 判）");
}

// ---------------------------------------------------------------- 5. push 侧订阅门控

// Java DefaultMQPushConsumer#postSubscriptionWhenPull 默认 false；打开且非类过滤模式才上送。
void testPullSubscriptionGatingFollowsJavaDefault() {
    expect(!DefaultMQPushConsumer::shouldPostSubscriptionWhenPull(false, false),
           "默认关闭 → 不上送");
    expect(DefaultMQPushConsumer::shouldPostSubscriptionWhenPull(true, false),
           "打开且非类过滤 → 上送");
    expect(!DefaultMQPushConsumer::shouldPostSubscriptionWhenPull(true, true),
           "类过滤模式即使打开也不上送（表达式是过滤类名，broker 的 TAG 过滤会误判）");
    expect(!DefaultMQPushConsumer::shouldPostSubscriptionWhenPull(false, true),
           "双关");

    DefaultMQPushConsumer consumer(kGroup);
    expect(!consumer.isPostSubscriptionWhenPull(), "Java 5.x 默认 false");
    consumer.setPostSubscriptionWhenPull(true);
    expect(consumer.isPostSubscriptionWhenPull(), "setter 生效");
}

// ---------------------------------------------------------------- 6. 拉消费者的表读写

// Java PullAPIWrapper#recalculatePullFromWhichNode / #updatePullFromWhichNode：无记录按
// master=0；应答缺 suggestWhichBrokerId 也按 0 记账（Java long 原语，不是"保留旧值"）；
// 表按队列隔离。
void testPullFromWhichNodeRoundTrip() {
    DefaultMQPullConsumer pull(kGroup);
    MessageQueue mq(kTopic, kBroker, 0);
    MessageQueue other(kTopic, kBroker, 1);

    expectInt(pull.recalculatePullFromWhichNode(mq), MixAll::MASTER_ID, "首轮打主节点");

    PullResult withSuggest;
    withSuggest.suggestWhichBrokerId = MixAll::MASTER_ID + 1;
    pull.updatePullFromWhichNode(mq, withSuggest);
    expectInt(pull.recalculatePullFromWhichNode(mq), MixAll::MASTER_ID + 1, "suggest 写回表");

    // 老 broker 不带 suggest：按 master 记账，而不是保留旧值
    pull.updatePullFromWhichNode(mq, PullResult{});
    expectInt(pull.recalculatePullFromWhichNode(mq), MixAll::MASTER_ID,
              "缺 suggestWhichBrokerId 按 master 记账（Java long 原语口径）");

    PullResult two;
    two.suggestWhichBrokerId = 2;
    pull.updatePullFromWhichNode(mq, two);
    expectInt(pull.recalculatePullFromWhichNode(mq), 2, "按队列隔离（本队列已更新）");
    expectInt(pull.recalculatePullFromWhichNode(other), MixAll::MASTER_ID, "按队列隔离（别的队列不受影响）");
}

}  // namespace

int main() {
    testFindBrokerAddressInSubscribeBranches();
    testPullToSlaveClearsCommitOffsetAndOmitsSubscription();
    testPullToMasterKeepsCommitOffsetAndPostsSubscription();
    testMissingSlaveFallsBackToMasterAndKeepsCommitOffset();
    testPullSubscriptionGatingFollowsJavaDefault();
    testPullFromWhichNodeRoundTrip();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
