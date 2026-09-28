// P6（#98）：经典拉模式消费者 / 轻量拉取消费者的**心跳**离线单测。
//
// 对齐基准（Java 5.5.1，逐行读过；与 python/tests/test_pull_consumer.py、
// python/tests/test_lite_pull_consumer.py 同题）：
//   * Java DefaultMQPullConsumerImpl.start():746 把本组注册进实例的 consumerTable，
//     实例级周期任务再替它发心跳；心跳口径由 prepareHeartbeatData:1031-1045 取
//     consumeType()（:348 恒为 CONSUME_ACTIVELY）、consumeFromWhere()（:353 恒为
//     CONSUME_FROM_LAST_OFFSET）、messageModel() 与 subscriptions()（:357-385：
//     逐条 buildSubscriptionData(topic, SUB_ALL) 并 **setSubVersion(0L)**）。
//   * Java DefaultLitePullConsumerImpl.consumeType():1111-1112 同样是 CONSUME_ACTIVELY。
//   * shutdown():689-692 = unregisterConsumer → mQClientFactory.shutdown()，即对每台
//     broker 发 UNREGISTER_CLIENT(35)。
// 为什么值得测：broker 的 ConsumerManager.consumerTable 是按台的，缺心跳时
// consumerConnection/38 看不到本组，rejectPullConsumerEnabled 的 broker 会拒拉
// （PullMessageProcessor:493-505），而拉取本身仍成功 —— 失效是静默的。
//
// 判据取自**线上报文**：进程内假端点从 socket 上收 HEART_BEAT 的 body（JSON）与
// UNREGISTER_CLIENT 的 extFields，不靠调用方自说自话。
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
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/json.h"
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

// ---------------------------------------------------------------- 假端点

// 同时扮演 name server（回路由）与 broker（收 HEART_BEAT / UNREGISTER_CLIENT）。
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

    size_t heartbeatCount() {
        std::lock_guard<std::mutex> lk(state_);
        return heartbeats_.size();
    }

    // 第 i 份心跳（按到达顺序）里的第一个 ConsumerData
    bool consumerData(size_t i, ConsumerData& out) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= heartbeats_.size()) return false;
        return decodeConsumerData(heartbeats_[i], out);
    }

    size_t unregisterCount() {
        std::lock_guard<std::mutex> lk(state_);
        return unregisters_.size();
    }

    PropertyMap unregisterExt(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= unregisters_.size()) return PropertyMap();
        return unregisters_[i];
    }

    static bool decodeConsumerData(const std::string& body, ConsumerData& out) {
        JsonValue v;
        std::string err;
        if (!jsonParse(body, v, &err)) return false;
        HeartbeatData hb = HeartbeatData::fromJson(v);
        if (hb.consumerDataSet.empty()) return false;
        out = hb.consumerDataSet.front();
        return true;
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
            } else if (req.code == RequestCode::HEART_BEAT) {
                std::lock_guard<std::mutex> lk(state_);
                heartbeats_.push_back(std::string(req.body.begin(), req.body.end()));
                resp.code = ResponseCode::SUCCESS;
            } else if (req.code == RequestCode::UNREGISTER_CLIENT) {
                std::lock_guard<std::mutex> lk(state_);
                unregisters_.push_back(req.extFields);
                resp.code = ResponseCode::SUCCESS;
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
    std::vector<std::string> heartbeats_;
    std::vector<PropertyMap> unregisters_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- 脚手架

const char* kGroup = "PG_PullHbUnit";
const char* kTopic = "PullHbTopic";
const char* kBroker = "broker-a";

// Java DefaultMQPullConsumer.registerMessageQueueListener(topic, listener) 兼作
// 「登记 topic」入口（registerTopics.add(topic)），本端口同款，所以给一个空监听器。
class NoopListener : public MessageQueueListener {
public:
    void messageQueueChanged(const std::string&, const std::vector<MessageQueue>&,
                             const std::vector<MessageQueue>&) override {}
};

TopicRouteData routeOne(const std::string& broker, int64_t id, const std::string& addr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[id] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", broker, addrs);
    // queueDatas 决定 TopicPublishInfo.msgQueueList（fetchSubscribeMessageQueues 的返回）
    route.queueDatas.emplace_back(broker, 4, 4, 6, 0);
    return route;
}

TopicRouteData routeTwo(const std::string& masterAddr, const std::string& slaveAddr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = masterAddr;
    addrs[MixAll::MASTER_ID + 1] = slaveAddr;
    route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
    route.queueDatas.emplace_back(kBroker, 4, 4, 6, 0);
    return route;
}

// 等条件成立（默认 5s 上限），返回是否等到
template <typename Fn>
bool waitFor(Fn fn, int timeoutMs = 5000) {
    const int64_t deadline = UtilAll::currentTimeMillis() + timeoutMs;
    while (UtilAll::currentTimeMillis() < deadline) {
        if (fn()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return fn();
}

// ---------------------------------------------------------------- 1. 启动即注册

// Java start():746 注册 + 实例周期任务首个 1s 内发心跳；本端口 start() 里同步发一轮。
// 判据：broker 真的收到 HEART_BEAT，且 ConsumerData 的四项口径与 Java 一致。
void testStartAnnouncesTheGroupWithTheJavaShape() {
    MockEndpoint namesrv;
    MockEndpoint broker;
    namesrv.addRoute(kTopic, routeOne(kBroker, MixAll::MASTER_ID, broker.address()));

    DefaultMQPullConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    auto listener = std::make_shared<NoopListener>();
    consumer.registerMessageQueueListener(kTopic, listener);
    consumer.setUnitMode(true);
    consumer.start();
    const bool got = waitFor([&] { return broker.heartbeatCount() >= 1; });
    expect(got, "start() 后 broker 收到 HEART_BEAT");
    expectInt(consumer.heartbeatCount(), 1, "启动时同步发一轮（心跳计数=1）");

    ConsumerData cd;
    const bool decoded = broker.consumerData(0, cd);
    expect(decoded, "心跳 body 能按 Java 字段名解出 ConsumerData");
    if (decoded) {
        expect(cd.groupName == kGroup, "ConsumerData.groupName 是本组", "got=" + cd.groupName);
        // DefaultMQPullConsumerImpl:348
        expect(cd.consumeType == ConsumeType::CONSUME_ACTIVELY,
               "consumeType=CONSUME_ACTIVELY（Java DefaultMQPullConsumerImpl:348）",
               "got=" + cd.consumeType);
        // :353
        expect(cd.consumeFromWhere == ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
               "consumeFromWhere=CONSUME_FROM_LAST_OFFSET（:353）", "got=" + cd.consumeFromWhere);
        expect(cd.messageModel == MessageModel::CLUSTERING,
               "messageModel=CLUSTERING", "got=" + cd.messageModel);
        expect(cd.unitMode, "unitMode 如实上报（MQClientInstance:1039）");
        expectInt(static_cast<long long>(cd.subscriptionDataSet.size()), 1,
                  "订阅集来自 registerTopics（subscriptions():357-385）");
        if (!cd.subscriptionDataSet.empty()) {
            const SubscriptionData& sub = cd.subscriptionDataSet.front();
            expect(sub.topic == kTopic, "订阅 topic 原样（不加命名空间）", "got=" + sub.topic);
            expect(sub.subString == "*", "subString=SUB_ALL", "got=" + sub.subString);
            // Java 显式 setSubVersion(0L)：拉模式没有订阅版本语义，带时间戳会让 broker
            // 每轮心跳都以为订阅变了
            expectInt(sub.subVersion, 0, "subVersion=0（Java setSubVersion(0L)）");
        }
    }

    consumer.shutdown();
}

// ---------------------------------------------------------------- 2. 扇出到每一台

// Java sendHeartbeatToAllBroker:732-750 遍历 brokerAddrTable 的**每个 brokerId**，
// 仅当 consumerEmpty && id != MASTER_ID 才跳过；消费者心跳必带 ConsumerData，故从节点
// 也发。broker 的 consumerTable 按台各一份，从节点漏发会让指向它的拉取走不到订阅表。
void testHeartbeatFansOutToEveryBrokerId() {
    MockEndpoint namesrv;
    MockEndpoint master;
    MockEndpoint slave;
    namesrv.addRoute(kTopic, routeTwo(master.address(), slave.address()));

    DefaultMQPullConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    auto listener = std::make_shared<NoopListener>();
    consumer.registerMessageQueueListener(kTopic, listener);
    consumer.start();
    const bool both = waitFor([&] {
        return master.heartbeatCount() >= 1 && slave.heartbeatCount() >= 1;
    });
    expect(both, "主从两台都收到心跳", "master=" + std::to_string(master.heartbeatCount()) +
                                      " slave=" + std::to_string(slave.heartbeatCount()));
    expectInt(consumer.heartbeatCount(), 1, "一轮心跳=只算一次（成功台数不重复计数）");
    consumer.shutdown();
}

// ---------------------------------------------------------------- 3. 没有订阅也要注册本组

// Java subscriptions():357-385 在 registerTopics 为空时返回空集，但 ConsumerData 本身
// 照样上线 —— 组是"注册过"，只是没有订阅条目（broker 侧的 ConsumerGroupInfo 会被建出来，
// consumerConnection 看得到本组）。心跳收件人靠实例路由表，所以先显式取一次路由。
void testHeartbeatStillAnnouncesTheGroupWithoutRegisterTopics() {
    MockEndpoint namesrv;
    MockEndpoint broker;
    namesrv.addRoute(kTopic, routeOne(kBroker, MixAll::MASTER_ID, broker.address()));

    DefaultMQPullConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    consumer.start();
    // 无 registerTopics → start() 时路由表为空、那一轮发 0 份；取一次队列把路由灌进来
    const std::vector<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(kTopic);
    expect(!mqs.empty(), "前置：路由已拉到（mock name server 回的路由）");
    const int32_t sent = consumer.sendHeartbeatToAllBroker();
    expectInt(sent, 1, "显式心跳发到 1 台");

    ConsumerData cd;
    const bool decoded = broker.consumerData(0, cd);
    expect(decoded, "心跳 body 可解");
    if (decoded) {
        expect(cd.groupName == kGroup, "空订阅也把本组注册上去", "got=" + cd.groupName);
        expectInt(static_cast<long long>(cd.subscriptionDataSet.size()), 0,
                  "订阅集为空（没有 registerTopics）");
    }
    consumer.shutdown();
}

// ---------------------------------------------------------------- 4. 周期循环

// Java 心跳是周期任务（默认 30s）；本端口是消费者自带线程。用 300ms 周期验证循环真的在跑。
void testHeartbeatLoopRepeats() {
    MockEndpoint namesrv;
    MockEndpoint broker;
    namesrv.addRoute(kTopic, routeOne(kBroker, MixAll::MASTER_ID, broker.address()));

    DefaultMQPullConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    consumer.setHeartbeatBrokerIntervalMillis(300);
    auto listener = std::make_shared<NoopListener>();
    consumer.registerMessageQueueListener(kTopic, listener);
    consumer.start();
    const bool repeated = waitFor([&] { return consumer.heartbeatCount() >= 3; }, 6000);
    expect(repeated, "心跳循环按周期重复发送", "count=" + std::to_string(consumer.heartbeatCount()));

    // 关掉开关后循环空转（启动时那一轮不受影响）
    consumer.setHeartbeatEnabled(false);
    const int32_t before = consumer.heartbeatCount();
    std::this_thread::sleep_for(std::chrono::milliseconds(900));
    expectInt(consumer.heartbeatCount(), before, "heartbeatEnabled=false 后循环不再发");
    consumer.shutdown();
}

// ---------------------------------------------------------------- 5. shutdown 注销

// Java DefaultMQPullConsumerImpl.shutdown:689-692 → unregisterConsumer → 每台发 35。
// 少了它，broker 要等心跳超时（默认 ~120s）才摘掉本组，这段时间 consumerConnection
// 还挂着已退出的实例。
void testShutdownUnregistersTheGroupOnEveryBroker() {
    MockEndpoint namesrv;
    MockEndpoint master;
    MockEndpoint slave;
    namesrv.addRoute(kTopic, routeTwo(master.address(), slave.address()));

    DefaultMQPullConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    auto listener = std::make_shared<NoopListener>();
    consumer.registerMessageQueueListener(kTopic, listener);
    consumer.start();
    waitFor([&] { return master.heartbeatCount() >= 1; });
    consumer.shutdown();

    const bool bothUnregistered = master.unregisterCount() >= 1 && slave.unregisterCount() >= 1;
    expect(bothUnregistered, "shutdown 对每台 broker 发 UNREGISTER_CLIENT(35)",
           "master=" + std::to_string(master.unregisterCount()) +
               " slave=" + std::to_string(slave.unregisterCount()));
    if (bothUnregistered) {
        const PropertyMap ext = master.unregisterExt(0);
        expect(ext.count("consumerGroup") && ext.at("consumerGroup") == kGroup,
               "35 带 consumerGroup", "ext=" + (ext.count("consumerGroup") ? ext.at("consumerGroup")
                                                                           : std::string("<none>")));
        // 空着的那个槽位不上线（broker ClientManageProcessor:228/237 判的是 group != null）
        expect(ext.count("producerGroup") == 0, "35 不带 producerGroup 槽位");
    }
    // 幂等：重复 shutdown 不再发
    const size_t before = master.unregisterCount();
    consumer.shutdown();
    expectInt(static_cast<long long>(master.unregisterCount()), static_cast<long long>(before),
              "重复 shutdown 幂等");
}

// ---------------------------------------------------------------- 6. 轻量拉取也是 ACTIVELY

// Java DefaultLitePullConsumerImpl.consumeType():1111-1112 同样 return CONSUME_ACTIVELY。
// broker 侧读它的地方见 lite_pull_consumer.cpp buildHeartbeat 的注释。
void testLitePullHeartbeatIsActivelyTyped() {
    MockEndpoint namesrv;
    MockEndpoint broker;
    namesrv.addRoute(kTopic, routeOne(kBroker, MixAll::MASTER_ID, broker.address()));

    DefaultLitePullConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    consumer.subscribe(kTopic, "*");
    consumer.start();
    const bool got = waitFor([&] { return broker.heartbeatCount() >= 1; });
    expect(got, "lite start() 后 broker 收到 HEART_BEAT");

    ConsumerData cd;
    const bool decoded = broker.consumerData(0, cd);
    expect(decoded, "lite 心跳 body 可解");
    if (decoded) {
        expect(cd.groupName == kGroup, "lite ConsumerData.groupName 是本组", "got=" + cd.groupName);
        expect(cd.consumeType == ConsumeType::CONSUME_ACTIVELY,
               "lite consumeType=CONSUME_ACTIVELY（Java DefaultLitePullConsumerImpl:1111-1112）",
               "got=" + cd.consumeType);
    }
    consumer.shutdown();
}

}  // namespace

int main() {
    testStartAnnouncesTheGroupWithTheJavaShape();
    testHeartbeatFansOutToEveryBrokerId();
    testHeartbeatStillAnnouncesTheGroupWithoutRegisterTopics();
    testHeartbeatLoopRepeats();
    testShutdownUnregistersTheGroupOnEveryBroker();
    testLitePullHeartbeatIsActivelyTyped();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
