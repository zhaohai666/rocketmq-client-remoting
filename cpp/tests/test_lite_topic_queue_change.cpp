// LitePull 消费者的「topic 队列集合变更」监听（registerTopicMessageQueueChangeListener）
// 离线单测 —— 不需要集群。
//
// 这里锁的是本端自己的契约，判据全部取自线上报文：
//   * 回调只在**队列集合**相对上一次快照真的变化时发一次；数量没变、内容换了也算变化。
//   * 启动前注册 = 没有快照，后台第一趟必然回调一次（把当前集合交给调用方）；
//     运行中注册 = 立刻记一版快照，当前状态不会被当成「变化」重复上报。
//   * 每趟比对都**现问 name server**（不吃路由缓存），否则扩容最快要等一次周期轮询才看得见。
//   * 查不到队列时**抛**而不是回空表 —— "查不到" ≠ "这个 topic 缩到 0 队列"，
//     后者会让监听器收到一次假缩容回调、并把快照刷成空集。
//   * 一个 topic 取路由失败只跳过它自己，本轮其余 topic 照常比对。
//   * 重复注册同一 topic 覆盖旧监听器。
//   * 比对每趟都真的重查 nameserver（GET_ROUTEINFO_BY_TOPIC 计数递增）。
//
// 后台线程首查延迟 10s，单测不等它：比对那一趟由测试直接驱动
//（fetchTopicMessageQueuesAndCompare 公开就是为这个）。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/util_all.h"
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

std::string idsOf(const std::vector<MessageQueue>& mqs) {
    std::vector<int32_t> ids;
    for (const MessageQueue& mq : mqs) ids.push_back(mq.queueId);
    std::sort(ids.begin(), ids.end());
    std::string s;
    for (int32_t id : ids) {
        if (!s.empty()) s += ",";
        s += std::to_string(id);
    }
    return s;
}

std::string joinTopics(const std::vector<std::string>& topics) {
    std::string s;
    for (const std::string& t : topics) {
        if (!s.empty()) s += " ";
        s += t;
    }
    return s;
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

// ---------------------------------------------------------------- 假 name server
//
// 路由**按请求现造**：测试中途 setQueueNums() 之后，下一笔路由请求就是新的队列数，
// 这样「集合变化 → 回调」是被真的观察到，不是靠测试自己塞进去的假数据。
class MockNameServer {
public:
    MockNameServer() {
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

    ~MockNameServer() {
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

    MockNameServer(const MockNameServer&) = delete;
    MockNameServer& operator=(const MockNameServer&) = delete;

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void setQueueNums(const std::string& topic, int32_t n) {
        std::lock_guard<std::mutex> lk(state_);
        queueNums_[topic] = n;
    }

    /// topic 在表里但队列为 0：路由能查到、订阅信息为空，正是「缩到 0 队列」。
    /// 不在表里则是「没有这个 topic 的路由」。
    void dropRoute(const std::string& topic) {
        std::lock_guard<std::mutex> lk(state_);
        queueNums_.erase(topic);
    }

    int routeRequests() {
        std::lock_guard<std::mutex> lk(state_);
        return routeReqs_;
    }

    /// 收到过的路由查询 topic（按到达顺序，含重复）。
    std::vector<std::string> requestedTopics() {
        std::lock_guard<std::mutex> lk(state_);
        return requested_;
    }

    void clearRequests() {
        std::lock_guard<std::mutex> lk(state_);
        routeReqs_ = 0;
        requested_.clear();
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
                ++routeReqs_;
                requested_.push_back(topic);
                auto it = queueNums_.find(topic);
                if (it == queueNums_.end()) {
                    resp.code = ResponseCode::TOPIC_NOT_EXIST;
                } else {
                    resp.code = ResponseCode::SUCCESS;
                    resp.body = buildRoute(topic, it->second).encode();
                    resp.hasBody = true;
                }
            } else {
                resp.code = ResponseCode::SUCCESS;
            }
            Bytes out = resp.encode();
            if (!writeAll(sock, out)) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    TopicRouteData buildRoute(const std::string& topic, int32_t n) const {
        (void)topic;
        TopicRouteData route;
        std::map<int64_t, std::string> addrs;
        addrs[MixAll::MASTER_ID] = address();
        route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
        route.queueDatas.emplace_back(kBroker, n, n, 6, 0);
        return route;
    }

    static constexpr const char* kBroker = "broker-a";

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::mutex state_;
    std::map<std::string, int32_t> queueNums_;
    int routeReqs_ = 0;
    std::vector<std::string> requested_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- 记录用监听器

class Recorder : public TopicMessageQueueChangeListener {
public:
    void onChanged(const std::string& topic, const std::vector<MessageQueue>& queues) override {
        std::lock_guard<std::mutex> lk(m_);
        events_.emplace_back(topic, idsOf(queues));
    }

    std::vector<std::pair<std::string, std::string>> events() {
        std::lock_guard<std::mutex> lk(m_);
        return events_;
    }

    size_t count() {
        std::lock_guard<std::mutex> lk(m_);
        return events_.size();
    }

private:
    std::mutex m_;
    std::vector<std::pair<std::string, std::string>> events_;
};

// ---------------------------------------------------------------- 脚手架

const char* kGroup = "PG_LiteMqUnit";
const char* kTopic = "LiteMqChangeTopic";
// 排在 kTopic 之前的名字（比对按 topic 名有序遍历），这样「坏 topic 会不会打断本轮」
// 才有判据：坏的那个先被处理，如果异常逃出了循环体，后面正常的就收不到回调。
const char* kGhost = "ALiteMqGhost";

/// 起一个指向假 name server 的 lite 消费者：subscribe 是启动前置条件，
/// 顺带把 topic 登记进实例的在用路由表。
std::unique_ptr<DefaultLitePullConsumer> started(const std::string& instance,
                                                 const MockNameServer& ns,
                                                 const std::string& topic,
                                                 const std::string& nameSpace = std::string()) {
    auto c = std::make_unique<DefaultLitePullConsumer>(kGroup);
    c->setInstanceName(instance);
    c->setNamesrvAddr(ns.address());
    c->setNamespace(nameSpace);
    c->subscribe(topic, "*");
    c->start();
    return c;
}

// ---------------------------------------------------------------- 1. 入参守卫与周期

void testGuardsAndInterval() {
    DefaultLitePullConsumer c(kGroup);
    bool topicGuard = false;
    bool listenerGuard = false;
    try {
        c.registerTopicMessageQueueChangeListener("", std::make_shared<Recorder>());
    } catch (const MQClientException& e) {
        topicGuard = std::string(e.what()).find("Topic or listener is null") != std::string::npos;
    }
    try {
        c.registerTopicMessageQueueChangeListener(kTopic, nullptr);
    } catch (const MQClientException& e) {
        listenerGuard = std::string(e.what()).find("Topic or listener is null") != std::string::npos;
    }
    expect(topicGuard, "空 topic 被拒");
    expect(listenerGuard, "空监听器被拒");
    expectInt(c.topicMetadataCheckIntervalMillis(), 30000, "默认比对周期 30s");
    c.setTopicMetadataCheckIntervalMillis(0);
    expectInt(c.topicMetadataCheckIntervalMillis(), 1000, "周期下限收到 1s（0 会让后台永不推进）");
    c.setTopicMetadataCheckIntervalMillis(5000);
    expectInt(c.topicMetadataCheckIntervalMillis(), 5000, "周期可放大");
}

// ---------------------------------------------------------------- 2. 启动前注册

void testRegistrationBeforeStartFiresOnceThenQuiets() {
    MockNameServer ns;
    ns.setQueueNums(kTopic, 2);
    auto rec = std::make_shared<Recorder>();
    DefaultLitePullConsumer c(kGroup);
    c.setInstanceName("lite_mq_pre");
    c.setNamesrvAddr(ns.address());
    // 注册在 start() 之前：这时记不了快照（还没法查队列），所以首趟必然回调一次。
    c.subscribe(kTopic, "*");
    c.registerTopicMessageQueueChangeListener(kTopic, rec);
    c.start();
    // 没快照，所以第一趟把「当前集合」当成变化交出来 —— 调用方由此拿到初始状态，
    // 之后才只需要收增量。
    c.fetchTopicMessageQueuesAndCompare();
    expect(rec->count() == 1, "启动后第一趟回调一次", std::to_string(rec->count()));
    const auto ev = rec->events();
    if (!ev.empty()) expect(ev[0].second == "0,1", "首回调带当前队列集合", ev[0].second);
    c.fetchTopicMessageQueuesAndCompare();
    expect(rec->count() == 1, "集合没变就不再回调");
    c.shutdown();
}

// ---------------------------------------------------------------- 3. 运行中注册 + 扩缩容

void testRunningRegistrationSnapshotsAndTracksScale() {
    MockNameServer ns;
    ns.setQueueNums(kTopic, 1);
    auto c = started("lite_mq_running", ns, kTopic);
    auto rec = std::make_shared<Recorder>();
    // 运行中注册：立刻记一版快照，「此刻存在的东西」不能被当成变化上报。
    c->registerTopicMessageQueueChangeListener(kTopic, rec);
    c->fetchTopicMessageQueuesAndCompare();
    expect(rec->count() == 0, "运行中注册后第一趟不误报", std::to_string(rec->count()));

    ns.setQueueNums(kTopic, 3);
    c->fetchTopicMessageQueuesAndCompare();
    auto ev = rec->events();
    expectInt(static_cast<long long>(ev.size()), 1, "扩容被观察到");
    if (ev.size() == 1) expect(ev[0].second == "0,1,2", "回调带扩容后的集合", ev[0].second);
    c->fetchTopicMessageQueuesAndCompare();
    expect(rec->count() == 1, "同一集合不重复回调");

    ns.setQueueNums(kTopic, 2);
    c->fetchTopicMessageQueuesAndCompare();
    ev = rec->events();
    expectInt(static_cast<long long>(ev.size()), 2, "缩容也被观察到");
    if (ev.size() == 2) expect(ev[1].second == "0,1", "回调带缩容后的集合", ev[1].second);
    c->shutdown();
}

// ---------------------------------------------------------------- 4. 每趟现问 name server

void testEachRoundRequeriesTheNameserver() {
    MockNameServer ns;
    ns.setQueueNums(kTopic, 2);
    auto c = started("lite_mq_fresh", ns, kTopic);
    c->registerTopicMessageQueueChangeListener(kTopic, std::make_shared<Recorder>());
    ns.clearRequests();
    c->fetchTopicMessageQueuesAndCompare();
    const int afterFirst = ns.routeRequests();
    c->fetchTopicMessageQueuesAndCompare();
    expect(afterFirst >= 1, "比对这一趟会问路由", std::to_string(afterFirst));
    expect(ns.routeRequests() > afterFirst, "下一趟再问一次（不吃缓存，否则扩容慢一个周期）",
           std::to_string(afterFirst) + "->" + std::to_string(ns.routeRequests()));
    c->shutdown();
}

// ---------------------------------------------------------------- 5. 一个 topic 失败不饿死别人

void testFailingTopicDoesNotBlockOthers() {
    MockNameServer ns;
    ns.setQueueNums(kTopic, 2);
    auto good = std::make_shared<Recorder>();
    auto bad = std::make_shared<Recorder>();
    auto c = started("lite_mq_isolate", ns, kTopic);
    c->registerTopicMessageQueueChangeListener(kTopic, good);
    c->registerTopicMessageQueueChangeListener(kGhost, bad);
    ns.setQueueNums(kTopic, 3);
    // Ghost 没有路由：取队列那步抛异常，本趟跳过它自己，排在它后面的照常比对。
    c->fetchTopicMessageQueuesAndCompare();
    expect(bad->count() == 0, "查不到路由的 topic 不发回调（也绝不回空表假装缩容）");
    expectInt(static_cast<long long>(good->count()), 1, "同轮其余 topic 照常",
              std::to_string(good->count()));
    const auto ev = good->events();
    if (!ev.empty()) expect(ev[0].second == "0,1,2", "正常 topic 收到的是新集合", ev[0].second);
    c->shutdown();
}

// ---------------------------------------------------------------- 6. 重复注册覆盖旧的

void testReRegisterOverwritesOldListener() {
    MockNameServer ns;
    ns.setQueueNums(kTopic, 1);
    auto c = started("lite_mq_overwrite", ns, kTopic);
    auto first = std::make_shared<Recorder>();
    auto second = std::make_shared<Recorder>();
    c->registerTopicMessageQueueChangeListener(kTopic, first);
    c->registerTopicMessageQueueChangeListener(kTopic, second);
    ns.setQueueNums(kTopic, 2);
    c->fetchTopicMessageQueuesAndCompare();
    expect(first->count() == 0, "旧监听器不再收到回调", std::to_string(first->count()));
    expect(second->count() == 1, "只有新监听器收到", std::to_string(second->count()));
    c->shutdown();
}

// ---------------------------------------------------------------- 7. 带命名空间

// 本端内部一律以「已套命名空间」的 topic 为键：注册用裸名，回调里收到的是套好的键，
// 而每趟比对都拿它去问路由（假端点按 ns%topic 命中才回路由）。
void testNamespacedKeyIsUsedThroughout() {
    const std::string nameSpace = "MQ_INST_lite";
    const std::string plain = "LiteMqNsTopic";
    const std::string wrapped = nameSpace + "%" + plain;

    MockNameServer ns;
    ns.setQueueNums(wrapped, 1);
    auto rec = std::make_shared<Recorder>();
    auto c = started("lite_mq_ns", ns, plain, nameSpace);
    c->registerTopicMessageQueueChangeListener(plain, rec);
    ns.clearRequests();
    ns.setQueueNums(wrapped, 2);
    c->fetchTopicMessageQueuesAndCompare();
    const auto ev = rec->events();
    expectInt(static_cast<long long>(ev.size()), 1, "带命名空间也能比对出变化",
              std::to_string(ev.size()));
    if (!ev.empty()) {
        expect(ev[0].first == wrapped, "回调的 topic 是套好命名空间的那一个", ev[0].first);
        expect(ev[0].second == "0,1", "回调带扩容后的集合", ev[0].second);
    }
    std::vector<std::string> asked = ns.requestedTopics();
    expect(std::find(asked.begin(), asked.end(), wrapped) != asked.end(),
          "比对每趟拿套好命名空间的键去问路由", joinTopics(asked));

    // 套壳必须幂等：拿已经套好的键再注册一次，只会覆盖同一个条目，
    // 不会冒出 ns%ns%topic 的第二个监听器（那样的监听器永远查不到路由）。
    c->registerTopicMessageQueueChangeListener(wrapped, rec);
    ns.clearRequests();
    ns.setQueueNums(wrapped, 3);
    c->fetchTopicMessageQueuesAndCompare();
    expectInt(static_cast<long long>(rec->count()), 2, "重复注册同一个（已套壳）键不会多出一份监听器",
              std::to_string(rec->count()));
    asked = ns.requestedTopics();
    bool doubleWrapped = false;
    for (const std::string& t : asked) {
        if (t == nameSpace + "%" + wrapped) doubleWrapped = true;
    }
    expect(!doubleWrapped, "绝不出现二次套壳的查询", joinTopics(asked));
    c->shutdown();
}

// ---------------------------------------------------------------- 8. 未启动就查队列

void testFetchQueuesRequiresStartedConsumer() {
    DefaultLitePullConsumer c("PG_LiteMqNotStarted");
    bool threw = false;
    try {
        c.fetchMessageQueues(kTopic);
    } catch (const MQClientException& e) {
        threw = std::string(e.what()).find("not started") != std::string::npos;
    }
    expect(threw, "未启动查队列直接报错（返回空表会被读成「没有队列」）");
}

}  // namespace

int main() {
    testGuardsAndInterval();
    testRegistrationBeforeStartFiresOnceThenQuiets();
    testRunningRegistrationSnapshotsAndTracksScale();
    testEachRoundRequeriesTheNameserver();
    testFailingTopicDoesNotBlockOthers();
    testReRegisterOverwritesOldListener();
    testNamespacedKeyIsUsedThroughout();
    testFetchQueuesRequiresStartedConsumer();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
