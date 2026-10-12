// 第六到第八组是**订阅口径**的另一半（#104）：`findBrokerAddressInSubscribe(brokerName,
// MASTER_ID, true)` 的三处调用面 —— 顺序消费的锁（RebalanceImpl#lock:153/lockAll:195，
// 只认主、**不刷路由**、拿不到整台跳过）、POP 拉取（PullAPIWrapper#popAsync:369-373，
// 刷一次路由后仍只认主）、消费位点读取（RemoteBrokerOffsetStore#fetchConsumeOffsetFromBroker:237-241，
// 刷一次路由后**放宽**到从节点）。三条口径在同一份「只剩从节点」的路由上的表现各不相同，
// 这正是它们必须分开测的原因。
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/remoting/protocol/codes.h"
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

// ---------------------------------------------------------------- 假端点（name server / broker 一个壳）

struct ReqRecord {
    int32_t code = 0;
    PropertyMap ext;
};

// 同一套 socket 骨架（与 test_pull_post_subscription.cpp 的 MockEndpoint 同源）：
// 回路由给 name server，回 GET_MAX_OFFSET 的 scripted offset 给 broker，
// 并把每条到达的请求按 code 记账 —— 「刷没刷路由」这类断言只能从线上报文取。
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

    void scriptOffset(int64_t offset) {
        std::lock_guard<std::mutex> lk(state_);
        offset_ = offset;
    }

    void scriptQueryOffset(int64_t offset) {
        std::lock_guard<std::mutex> lk(state_);
        queryOffset_ = offset;
    }

    size_t countCode(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        size_t n = 0;
        for (const ReqRecord& r : reqs_) {
            if (r.code == code) ++n;
        }
        return n;
    }

    PropertyMap lastExt() {
        std::lock_guard<std::mutex> lk(state_);
        return reqs_.empty() ? PropertyMap() : reqs_.back().ext;
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
            {
                std::lock_guard<std::mutex> lk(state_);
                reqs_.push_back(ReqRecord{req.code, req.extFields});
            }
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
            } else if (req.code == RequestCode::GET_MAX_OFFSET) {
                std::lock_guard<std::mutex> lk(state_);
                resp.code = ResponseCode::SUCCESS;
                resp.extFields["offset"] = std::to_string(offset_);
            } else if (req.code == RequestCode::QUERY_CONSUMER_OFFSET) {
                std::lock_guard<std::mutex> lk(state_);
                resp.code = ResponseCode::SUCCESS;
                resp.extFields["offset"] = std::to_string(queryOffset_);
            } else if (req.code == RequestCode::LOCK_BATCH_MQ) {
                if (req.hasBody) {
                    const std::string text(req.body.begin(), req.body.end());
                    JsonValue root;
                    std::string err;
                    if (jsonParse(text, root, &err)) {
                        const JsonValue* mqSet = root.find("mqSet");
                        if (mqSet != nullptr) {
                            JsonValue out = JsonValue::makeObject();
                            out.set("lockOKMQSet", *mqSet);
                            const std::string dumped = out.dump();
                            resp.body = Bytes(dumped.begin(), dumped.end());
                            resp.hasBody = true;
                        }
                    }
                }
                resp.code = ResponseCode::SUCCESS;
            } else if (req.code == RequestCode::POP_MESSAGE) {
                // 假 broker 对 POP 一律回 POLLING_TIMEOUT(210)：既能证明请求打在主地址上，
                // 又顺带证明客户端把 210 翻成 PollingNotFound。
                resp.code = ResponseCode::POLLING_TIMEOUT;
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
    int64_t offset_ = 0;
    int64_t queryOffset_ = 0;
    std::vector<ReqRecord> reqs_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- 脚手架

const char* kTopic = "PublishRouteTopic";
const char* kBroker = "broker-a";
const char* kClientId = "127.0.0.1@100#1";

TopicRouteData routeOf(const std::string& brokerName, const std::map<int64_t, std::string>& addrs) {
    TopicRouteData route;
    route.queueDatas.emplace_back(brokerName, 2, 2, 6, 0);
    route.brokerDatas.emplace_back("DefaultCluster", brokerName, addrs);
    return route;
}

std::map<int64_t, std::string> onlySlave(const std::string& addr) {
    return {{MixAll::MASTER_ID + 1, addr}};
}

std::map<int64_t, std::string> masterAndSlave(const std::string& master,
                                              const std::string& slave) {
    return {{MixAll::MASTER_ID, master}, {MixAll::MASTER_ID + 1, slave}};
}

// 起实例，并用**真 RPC**（GET_ROUTEINFO_BY_TOPIC 打假 name server）把 kTopic 的路由
// 刷进缓存 —— 平表的写入点就在 updateTopicRouteInfoFromNameServer 里面。
std::unique_ptr<MQClientInstance> seeded(MockEndpoint& namesrv) {
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{namesrv.address()});
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    return instance;
}

// ---------------------------------------------------------------- 1. 查表口径

void testPublishLookupTakesOnlyTheMasterWhileTheTolerantLookupFallsBack() {
    MockEndpoint namesrv, master, slave;
    namesrv.addRoute(kTopic, routeOf(kBroker, onlySlave(slave.address())));
    auto instance = seeded(namesrv);

    expect(instance->findBrokerAddressInPublish(kBroker).empty(),
           "只剩从节点（brokerId=1）时发布地址查不到：Java map.get(MASTER_ID) == null");
    // 负控：同一份路由上，退让口径（心跳/拉取/位点查询用）必须拿得到从节点
    expect(instance->brokerAddrOf(kBroker) == slave.address(),
           "同一份路由上 brokerAddrOf 退到从节点 —— 两条口径不能合成一条");

    namesrv.addRoute(kTopic, routeOf(kBroker, masterAndSlave(master.address(), slave.address())));
    expect(instance->updateTopicRouteInfoFromNameServer(kTopic), "master 重新注册后刷路由成功");
    expect(instance->findBrokerAddressInPublish(kBroker) == master.address(),
           "跳的是「没有 master」而不是名字：master 一注册立刻解析得出");
}

// ---------------------------------------------------------------- 2. 刷路由再查

void testPublishAddrForRefreshesTheRouteThenRechecks() {
    MockEndpoint namesrv, master;
    namesrv.addRoute(kTopic, routeOf(kBroker, {{MixAll::MASTER_ID, master.address()}}));
    // 缓存为空：本端还没见过这个 topic（定点发送不会先去取发布信息，这是唯一的路由来源）
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{namesrv.address()});
    const size_t before = namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC);

    expect(instance->publishAddrFor(kBroker, kTopic) == master.address(),
           "Java sendKernelImpl:919-924：查不到先按 topic 刷一次路由再查");
    expectInt(static_cast<long long>(
                  namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC) - before),
              1, "恰好刷一次");
}

// ---------------------------------------------------------------- 3. 主掉线 → not exist

void testPublishAddrForReportsNotExistWhenTheMasterIsGone() {
    MockEndpoint namesrv, master, slave;
    namesrv.addRoute(kTopic, routeOf(kBroker, onlySlave(slave.address())));
    auto instance = seeded(namesrv);
    const size_t before = namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC);

    bool threw = false;
    try {
        instance->publishAddrFor(kBroker, kTopic);
    } catch (const MQClientException& e) {
        threw = std::string(e.what()) == "The broker[broker-a] not exist";
        expectInt(e.getResponseCode(), -1, "not exist 的 responseCode == -1");
    }
    expect(threw, "只剩从节点时 publishAddrFor 报 not exist");
    expectInt(static_cast<long long>(
                  namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC) - before),
              1, "报 not exist 之前刷过一次路由");

    namesrv.addRoute(kTopic, routeOf(kBroker, masterAndSlave(master.address(), slave.address())));
    expect(instance->publishAddrFor(kBroker, kTopic) == master.address(),
           "负控：master 一注册立刻解析得出");
}

// ---------------------------------------------------------------- 4. 不认识的 brokerName

void testPublishAddrForReportsNotExistForAnUnknownBroker() {
    MockEndpoint namesrv, other;
    namesrv.addRoute(kTopic, routeOf("broker-b", {{MixAll::MASTER_ID, other.address()}}));
    auto instance = seeded(namesrv);

    bool threw = false;
    try {
        instance->publishAddrFor(kBroker, kTopic);
    } catch (const MQClientException& e) {
        threw = std::string(e.what()) == "The broker[broker-a] not exist";
    }
    expect(threw, "路由里压根没有这个 brokerName：同样报 not exist，不退到别的 broker");
}

// ---------------------------------------------------------------- 5. 管理 offset 查询

void testAdminOffsetQueriesAreMasterOnlyToo() {
    MockEndpoint namesrv, master, slave;
    namesrv.addRoute(kTopic, routeOf(kBroker, onlySlave(slave.address())));
    auto instance = seeded(namesrv);
    master.scriptOffset(7);
    MessageQueue mq(kTopic, kBroker, 0);

    bool threw = false;
    try {
        instance->getMaxOffset(mq);
    } catch (const MQClientException& e) {
        threw = std::string(e.what()) == "The broker[broker-a] not exist";
    }
    expect(threw, "只剩从节点时 getMaxOffset 报 not exist（Java MQAdminImpl:195 只打主）");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::GET_MAX_OFFSET)), 0,
              "是本端结论：从节点一台都没收到 offset 查询");

    // 负控：主回来之后查得到，且请求落在**主**地址上（不是路由里那台从节点）
    namesrv.addRoute(kTopic, routeOf(kBroker, masterAndSlave(master.address(), slave.address())));
    expectInt(instance->getMaxOffset(mq), 7, "主回来之后查得到");
    expectInt(static_cast<long long>(master.countCode(RequestCode::GET_MAX_OFFSET)), 1,
              "请求落在主地址上");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::GET_MAX_OFFSET)), 0,
              "从节点始终没被打过");
    expect(master.lastExt().at("topic") == kTopic, "offset 查询带 topic");
}


void testOrderlyLocksSkipTheBrokerWhenTheMasterIsGone() {
    MockEndpoint namesrv, master, slave;
    namesrv.addRoute(kTopic, routeOf(kBroker, onlySlave(slave.address())));
    auto instance = seeded(namesrv);
    const size_t before = namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC);
    std::vector<MessageQueue> mqs{MessageQueue(kTopic, kBroker, 0),
                                  MessageQueue(kTopic, kBroker, 1)};

    expect(instance->lockBatchMq("G", kClientId, mqs).empty(),
           "只剩从节点时一台队列都锁不上");
    expectInt(static_cast<long long>(
                  namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC) - before),
              0, "锁/解锁不刷路由（与 publishAddrFor 的分界）");
    instance->unlockBatchMq("G", kClientId, mqs);
    expectInt(static_cast<long long>(master.countCode(RequestCode::LOCK_BATCH_MQ)), 0,
              "LOCK_BATCH_MQ 一台没发");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::LOCK_BATCH_MQ)), 0,
              "从节点也不该被锁");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::UNLOCK_BATCH_MQ)), 0,
              "UNLOCK 同样一台没发");

    // 负控：master 一注册立刻锁上，请求落在**主**地址、返回集来自响应的 lockOKMQSet
    namesrv.addRoute(kTopic, routeOf(kBroker, masterAndSlave(master.address(), slave.address())));
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    std::vector<MessageQueue> locked = instance->lockBatchMq("G", kClientId, mqs);
    expectInt(static_cast<long long>(locked.size()), 2, "两台队列都锁上了");
    expect(locked.size() == 2 && locked[0].brokerName == kBroker && locked[0].queueId == 0 &&
               locked[1].queueId == 1,
           "返回集是响应里的 lockOKMQSet 原样");
    expectInt(static_cast<long long>(master.countCode(RequestCode::LOCK_BATCH_MQ)), 1,
              "LOCK_BATCH_MQ 落在主地址上");
    instance->unlockBatchMq("G", kClientId, mqs);
    expectInt(static_cast<long long>(master.countCode(RequestCode::UNLOCK_BATCH_MQ)), 1,
              "UNLOCK_BATCH_MQ 也落在主地址上");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::LOCK_BATCH_MQ)), 0,
              "从节点始终没被打过");
}

void testPopMessageIsMasterOnlyAndReportsNotExist() {
    MockEndpoint namesrv, master, slave;
    namesrv.addRoute(kTopic, routeOf(kBroker, onlySlave(slave.address())));
    auto instance = seeded(namesrv);
    const size_t before = namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC);

    bool threw = false;
    try {
        instance->popMessage("G", kTopic, 0, 32, 30000, 0, 0);
    } catch (const MQClientException& e) {
        threw = std::string(e.what()) == "The broker[broker-a] not exist";
        expectInt(e.getResponseCode(), -1, "not exist 的 responseCode == -1");
    }
    expect(threw, "只剩从节点时 POP 报 not exist");
    expectInt(static_cast<long long>(
                  namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC) - before),
              1, "报错前恰好刷一次路由");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::POP_MESSAGE)), 0,
              "从节点一台都没收到 POP —— 写请求打到从节点只会换一个 SYSTEM_BUSY(2)");

    // 负控：master 回来之后 POP 打得出去，210 翻成 PollingNotFound
    namesrv.addRoute(kTopic, routeOf(kBroker, masterAndSlave(master.address(), slave.address())));
    PopResult result = instance->popMessage("G", kTopic, 0, 32, 30000, 0, 0);
    expect(result.status == PopStatus::POLLING_NOT_FOUND,
           "假 broker 回 210 翻成 PollingNotFound");
    expectInt(static_cast<long long>(master.countCode(RequestCode::POP_MESSAGE)), 1,
              "POP 落在主地址上");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::POP_MESSAGE)), 0,
              "从节点始终没被打过");
}

void testConsumerOffsetFallsBackToTheSlaveAfterARefresh() {
    MockEndpoint namesrv, master, slave, other;
    namesrv.addRoute(kTopic, routeOf(kBroker, onlySlave(slave.address())));
    auto instance = seeded(namesrv);
    slave.scriptQueryOffset(424242);
    master.scriptQueryOffset(111);
    MessageQueue mq(kTopic, kBroker, 0);
    const size_t before = namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC);

    int64_t offset = 0;
    expect(instance->queryConsumerOffset("G", mq, offset), "只剩从节点时位点查得到");
    expectInt(offset, 424242, "刷一次路由后放宽到从节点");
    expectInt(static_cast<long long>(
                  namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC) - before),
              1, "放宽前恰好刷一次路由");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::QUERY_CONSUMER_OFFSET)), 1,
              "QUERY_CONSUMER_OFFSET 落在从节点上");

    // 负控：master 在时先打主（主分支不进刷新逻辑，从节点计数冻住）
    namesrv.addRoute(kTopic, routeOf(kBroker, masterAndSlave(master.address(), slave.address())));
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    int64_t masterOffset = 0;
    expect(instance->queryConsumerOffset("G", mq, masterOffset), "master 在时查得到");
    expectInt(masterOffset, 111, "位点读主的");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::QUERY_CONSUMER_OFFSET)), 1,
              "从节点的计数冻住");

    // 路由里压根没有这个 brokerName：报 not exist，不退到别的 broker 上，也不静默返回 0
    MockEndpoint ns2;
    ns2.addRoute(kTopic, routeOf("broker-b", {{MixAll::MASTER_ID, other.address()}}));
    auto instance2 = seeded(ns2);
    bool threw = false;
    int64_t unused = 0;
    try {
        instance2->queryConsumerOffset("G", MessageQueue(kTopic, kBroker, 0), unused);
    } catch (const MQClientException& e) {
        threw = std::string(e.what()) == "The broker[broker-a] not exist";
        expectInt(e.getResponseCode(), -1, "not exist 的 responseCode == -1");
    }
    expect(threw, "未知 brokerName 同样报 not exist");
    expectInt(static_cast<long long>(other.countCode(RequestCode::QUERY_CONSUMER_OFFSET)), 0,
              "别的 broker 一次没被打");
}

}  // namespace

int main() {
    testPublishLookupTakesOnlyTheMasterWhileTheTolerantLookupFallsBack();
    testPublishAddrForRefreshesTheRouteThenRechecks();
    testPublishAddrForReportsNotExistWhenTheMasterIsGone();
    testPublishAddrForReportsNotExistForAnUnknownBroker();
    testAdminOffsetQueriesAreMasterOnlyToo();
    testOrderlyLocksSkipTheBrokerWhenTheMasterIsGone();
    testPopMessageIsMasterOnlyAndReportsNotExist();
    testConsumerOffsetFallsBackToTheSlaveAfterARefresh();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
