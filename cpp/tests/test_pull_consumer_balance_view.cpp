// 拉模式消费者的**平衡视图**（fetchMessageQueuesInBalance）离线单测 —— 不需要集群。
//
// 对齐基准（Java 5.5.1，逐行读过；与 python/tests/test_pull_consumer.py、
// php/tests/RunClientConsumer.php、csharp PullBalanceViewTests、rust 同名用例对拍）：
//   * Java `MQPullConsumer#fetchMessageQueuesInBalance:187` →
//     `DefaultMQPullConsumerImpl:120-135`：isRunning() 守卫 → topic==null 抛
//     IllegalArgumentException → 过滤 `rebalanceImpl.getProcessQueueTable()` 的键 →
//     `parseSubscribeMessageQueues:153-161` **剥掉命名空间**。
//     官方用法见 `example/simple/PullConsumer.java:62`（去拉「自己那一份」而不是全部）。
//   * 本端口拉模式没有后台 rebalance 线程（见 pull_consumer.h 头文件的偏离说明），
//     视图按 `RebalanceImpl#rebalanceByTopic` 的同一条公式当场算，所以这里锁的是
//     「算得对不对」：BROADCASTING 全量；CLUSTERING 只拿自己那一份；算不动时保持
//     现有分配（拉过的队列），**绝不回退成独占全部队列**。
//
// 判据取自**线上报文**：假端点记下 38（GET_CONSUMER_LIST_BY_GROUP）的应答内容与
// 11（PULL_MESSAGE）的到达，不靠调用方自说自话。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/body.h"
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

void expectInt(long long actual, long long expected, const std::string& name,
               const std::string& detail = "") {
    ++checks;
    if (actual != expected) {
        std::printf("FAIL %s (actual=%lld expected=%lld) %s\n", name.c_str(), actual, expected,
                    detail.c_str());
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

// ---------------------------------------------------------------- 假端点

// 一台同时扮演 name server（回路由）与 broker（回 38 / 11）的假端点。
// 平衡视图只需要「一份路由 + 一份消费者列表」，收件人是谁无所谓，故合成一台。
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

    /// 设定 38 的应答内容：本组有哪些 clientId。空列表 = 「broker 不认识这个组」，
    /// 真实 broker 对没注册过的组也是这么回（不是报错）。
    void setCidList(const std::vector<std::string>& cids) {
        std::lock_guard<std::mutex> lk(state_);
        cidList_ = cids;
    }

    /// 收到的 PULL_MESSAGE 队列（topic@queueId），按到达顺序。
    std::vector<std::string> pulledQueues() {
        std::lock_guard<std::mutex> lk(state_);
        return pulled_;
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
            } else if (req.code == RequestCode::GET_CONSUMER_LIST_BY_GROUP) {
                GetConsumerListByGroupResponseBody body;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    body.consumerIdList = cidList_;
                }
                resp.code = ResponseCode::SUCCESS;
                resp.body = body.encode();
                resp.hasBody = true;
            } else if (req.code == RequestCode::PULL_MESSAGE
                       || req.code == RequestCode::LITE_PULL_MESSAGE) {
                const std::string topic =
                    req.extFields.count("topic") ? req.extFields.at("topic") : std::string();
                const std::string queueId =
                    req.extFields.count("queueId") ? req.extFields.at("queueId") : std::string();
                {
                    std::lock_guard<std::mutex> lk(state_);
                    pulled_.push_back(topic + "@" + queueId);
                }
                // 空 broker 的忠实形态：PULL_NOT_FOUND 并把 nextBeginOffset 回填成请求位点，
                // 免得客户端把游标推到 0（本用例只关心拉取被记进了本地分配表）。
                resp.code = ResponseCode::PULL_NOT_FOUND;
                resp.extFields["nextBeginOffset"] = "0";
                resp.extFields["minOffset"] = "0";
                resp.extFields["maxOffset"] = "0";
                resp.extFields["suggestWhichBrokerId"] = "0";
            } else {
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
    std::vector<std::string> cidList_;
    std::vector<std::string> pulled_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- 脚手架

const char* kGroup = "PG_BalUnit";
const char* kTopic = "BalViewTopic";
const char* kBrokerKey = "broker-a";
const int32_t kQueues = 4;

/// Java registerMessageQueueListener(topic, listener) 兼作「登记 topic」入口，
/// 本端口同款；平衡视图不关心回调，给一个空监听器。
class NoopListener : public MessageQueueListener {
public:
    void messageQueueChanged(const std::string&, const std::vector<MessageQueue>&,
                             const std::vector<MessageQueue>&) override {}
};

/// 队列清单的可读形态，失败时能一眼看出差在哪。
std::string describe(const std::vector<MessageQueue>& mqs) {
    std::string s;
    for (const MessageQueue& mq : mqs) {
        if (!s.empty()) s += ",";
        s += mq.topic + "@" + mq.brokerName + "@" + std::to_string(mq.queueId);
    }
    return s;
}

/// `n` 个读队列的那一台 broker（1 个队列时「自己那一份」与「全部」同形，
/// 什么也证明不了，所以默认 4 个）。
TopicRouteData routeN(int32_t n, const std::string& broker, const std::string& addr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", broker, addrs);
    route.queueDatas.emplace_back(broker, n, n, 6, 0);
    return route;
}

/// 起一个指向假端点的拉模式消费者。实例名必须每个用例唯一（同 clientId 会共享实例）。
/// DefaultMQPullConsumer 不可拷贝，故返回堆上的那一个。
std::unique_ptr<DefaultMQPullConsumer> started(const std::string& instance,
                                               const std::string& group, MockEndpoint& ep,
                                               const std::string& topic,
                                               const std::string& model = MessageModel::CLUSTERING,
                                               const std::string& ns = std::string()) {
    auto c = std::make_unique<DefaultMQPullConsumer>(group);
    c->setInstanceName(instance);
    c->setNamesrvAddr(ep.address());
    c->setMessageModel(model);
    c->setNamespace(ns);
    c->registerMessageQueueListener(topic, std::make_shared<NoopListener>());
    c->start();
    return c;
}

std::vector<std::string> sortedCids(const std::vector<std::string>& in) {
    std::vector<std::string> out = in;
    std::sort(out.begin(), out.end());
    return out;
}

// ---------------------------------------------------------------- 1. 单实例认领全部

// Java 表里那一刻装的就是「全部」：同组只有自己时分配结果必然是整份订阅信息，
// 而且按 house 口径（topic → brokerName → queueId）排好序。
void testSoleInstanceTakesEveryQueue() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(kQueues, kBrokerKey, ep.address()));
    auto c = started("bal_sole", kGroup, ep, kTopic);
    const std::string cid = c->clientId();
    ep.setCidList({cid});

    const std::vector<MessageQueue> view = c->fetchMessageQueuesInBalance(kTopic);
    expectInt(static_cast<long long>(view.size()), kQueues, "单实例认领全部 4 个队列");
    std::vector<int32_t> ids;
    for (const MessageQueue& mq : view) ids.push_back(mq.queueId);
    expect(ids == std::vector<int32_t>({0, 1, 2, 3}), "队列按 queueId 升序", describe(view));
    bool sameIdentity = true;
    for (const MessageQueue& mq : view) {
        if (mq.topic != kTopic || mq.brokerName != kBrokerKey) sameIdentity = false;
    }
    expect(sameIdentity, "队列身份未被改写（topic/brokerName 原样）", describe(view));
    c->shutdown();
}

// ---------------------------------------------------------------- 2. 只拿自己那一份

/// 官方 `PullConsumer.java:62` 的用法之所以成立，全靠「每个实例只拿到自己那一份」。
/// 两个真实例互相对拍：各 2 个、不重叠、合起来覆盖 4 个。
void testTwoInstancesSplitTheQueuesWithoutOverlap() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(kQueues, kBrokerKey, ep.address()));
    auto a = started("bal_pair_a", kGroup, ep, kTopic);
    auto b = started("bal_pair_b", kGroup, ep, kTopic);
    // 分配前 cidAll 必排序（Java `Collections.sort(cidAll)`）；真实 broker 的返回顺序不定，
    // 用例先排好，两边看到的才是同一份列表。
    ep.setCidList(sortedCids({a->clientId(), b->clientId()}));

    const std::vector<MessageQueue> va = a->fetchMessageQueuesInBalance(kTopic);
    const std::vector<MessageQueue> vb = b->fetchMessageQueuesInBalance(kTopic);
    expectInt(static_cast<long long>(va.size()), 2, "两个实例分 4 个队列：每个只拿 2 个");
    expectInt(static_cast<long long>(vb.size()), 2, "另一个也是 2 个");
    std::vector<MessageQueue> both = va;
    both.insert(both.end(), vb.begin(), vb.end());
    std::sort(both.begin(), both.end());
    std::vector<MessageQueue> dedup = both;
    dedup.erase(std::unique(dedup.begin(), dedup.end()), dedup.end());
    expectInt(static_cast<long long>(dedup.size()), 4, "两份不重叠（重叠即重复消费）",
              describe(both));
    std::vector<int32_t> ids;
    for (const MessageQueue& mq : dedup) ids.push_back(mq.queueId);
    expect(ids == std::vector<int32_t>({0, 1, 2, 3}), "两份合起来覆盖全部队列", describe(dedup));
    a->shutdown();
    b->shutdown();
}

// ---------------------------------------------------------------- 3. 算不动 → 保持现有分配

/// broker 不认识本组（38 回空列表）= **算不动**，不是「算出来是空」：此时保持现有分配
/// （本地拉过的那些队列），绝不回退成独占全部队列。
void testUnknownGroupKeepsCurrentAssignmentInsteadOfTakingAll() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(kQueues, kBrokerKey, ep.address()));
    auto c = started("bal_unknown", kGroup, ep, kTopic);
    ep.setCidList({});

    expect(c->fetchMessageQueuesInBalance(kTopic).empty(),
           "还没拉过任何队列：现有分配就是空（不是全部 4 个）");

    std::vector<MessageQueue> all = c->fetchSubscribeMessageQueues(kTopic);
    expectInt(static_cast<long long>(all.size()), kQueues, "订阅信息可查（路由里的 4 个队列）");
    std::sort(all.begin(), all.end());
    const PullResult result = c->pull(all[2], "*", 0, 32);
    (void)result;
    const std::vector<MessageQueue> view = c->fetchMessageQueuesInBalance(kTopic);
    expectInt(static_cast<long long>(view.size()), 1, "兜底只认领自己拉过的那一个");
    if (view.size() == 1) {
        expect(view[0] == all[2], "兜底返回的就是拉过的那个队列", describe(view));
    }
    expect(ep.pulledQueues().size() == 1, "线上报文：确实发过一笔 PULL_MESSAGE");

    // 兜底同样按 topic 收口：没有路由的另一个 topic 既不抛也不漏给它 T 的队列。
    expect(c->fetchMessageQueuesInBalance("NoSuchBalTopic").empty(),
           "查不到路由时退回本 topic 的现有分配，别的 topic 得空表");
    c->shutdown();
}

// ---------------------------------------------------------------- 4. 广播不看消费者列表

// Java `rebalanceByTopic` 对 BROADCASTING 不查 cidAll、全量认领：列表里没有本实例也照样。
void testBroadcastingTakesAllWithoutTheConsumerList() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(kQueues, kBrokerKey, ep.address()));
    auto c = started("bal_bcast", kGroup, ep, kTopic, MessageModel::BROADCASTING);
    ep.setCidList({"someone-else"});

    const std::vector<MessageQueue> view = c->fetchMessageQueuesInBalance(kTopic);
    expectInt(static_cast<long long>(view.size()), kQueues, "广播不看消费者列表：全量认领",
              describe(view));
    c->shutdown();
}

// ---------------------------------------------------------------- 5. 未启动直接抛

// Java `isRunning()`：未 start 抛 MQClientException，而不是静默返回空表
// （空表会让调用方以为「没我的队列」而停拉）。
void testRequiresAStartedConsumer() {
    DefaultMQPullConsumer c("PG_BalNotStarted");
    bool threw = false;
    try {
        c.fetchMessageQueuesInBalance(kTopic);
    } catch (const MQClientException& e) {
        threw = std::string(e.what()).find("not started") != std::string::npos;
    }
    expect(threw, "未启动必须抛 MQClientException（Java isRunning()）");
}

// ---------------------------------------------------------------- 6. 命名空间：返回裸 topic

// Java `parseSubscribeMessageQueues:153-161` 把结果剥回裸 topic：查询用 "ns%topic"，
// 返回给调用方的队列 topic 是 "topic"，于是可以直接喂给 pull()（pull 自己会再拼上 ns）。
void testNamespacedInstanceReturnsBareTopics() {
    const std::string ns = "MQ_INST_bal";
    const std::string plain = "NsBalTopic";
    const std::string wrapped = ns + "%" + plain;

    MockEndpoint ep;
    ep.addRoute(wrapped, routeN(kQueues, kBrokerKey, ep.address()));
    auto c = started("bal_ns", kGroup, ep, plain, MessageModel::CLUSTERING, ns);
    ep.setCidList({c->clientId()});

    const std::vector<MessageQueue> view = c->fetchMessageQueuesInBalance(plain);
    expectInt(static_cast<long long>(view.size()), kQueues,
              "带命名空间照样算得出（路由查询用的是 ns%topic）", describe(view));
    bool allBare = true;
    for (const MessageQueue& mq : view) {
        if (mq.topic != plain) allBare = false;
    }
    expect(allBare, "返回的队列已剥掉命名空间（Java parseSubscribeMessageQueues）",
          describe(view));
    c->shutdown();
}

}  // namespace

int main() {
    testSoleInstanceTakesEveryQueue();
    testTwoInstancesSplitTheQueuesWithoutOverlap();
    testUnknownGroupKeepsCurrentAssignmentInsteadOfTakingAll();
    testBroadcastingTakesAllWithoutTheConsumerList();
    testRequiresAStartedConsumer();
    testNamespacedInstanceReturnsBareTopics();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
