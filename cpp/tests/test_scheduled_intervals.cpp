// On a real cluster both deviations only look like "slow", never like "wrong": no missing field
// and no exception, only "the route of a new topic waits 30s to refresh", which nobody times. So
// a local fake name server times every GET_ROUTEINFO_BY_TOPIC **arrival** to pin the cadence down.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

using netcompat::kInvalidSocket;
using netcompat::socket_t;
using netcompat::socklen_type;

int checks = 0;
int fails = 0;

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
        ++fails;
        std::printf("FAIL %s (actual=%lld expected=%lld)\n", name.c_str(), actual, expected);
    }
}

using Clock = std::chrono::steady_clock;

int64_t msBetween(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(b - a).count();
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
    Bytes body(static_cast<size_t>(totalLen), '\0');
    if (!readN(s, reinterpret_cast<char*>(body.data()), static_cast<size_t>(totalLen))) {
        return false;
    }
    out = lenStr + body;
    return true;
}

// ---------------------------------------------------------------- fake name server

// Does two things only: reply with route data and record the **arrival time** of each
// GET_ROUTEINFO_BY_TOPIC. Other requests (heartbeat and the like) answer SUCCESS; only cadence matters.
class FakeNamesrv {
public:
    FakeNamesrv() {
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
        ::listen(listen_, 8);
        socklen_type len = sizeof(addr);
        ::getsockname(listen_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        running_.store(true);
        acceptor_ = std::thread([this]() { acceptLoop(); });
    }

    ~FakeNamesrv() {
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

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void addRoute(const std::string& topic, const TopicRouteData& route) {
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    // Timing origin: the case calls it before start(), so every arrival time recorded after is relative.
    void markStart() {
        std::lock_guard<std::mutex> lk(state_);
        t0_ = Clock::now();
        arrivals_.clear();
    }

    // GET_ROUTEINFO arrival times for a topic (ms, relative to markStart), in arrival order.
    std::vector<int64_t> arrivalsFor(const std::string& topic) {
        std::lock_guard<std::mutex> lk(state_);
        std::vector<int64_t> out;
        for (const auto& a : arrivals_) {
            if (a.first == topic) out.push_back(a.second);
        }
        return out;
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
            if (ready <= 0) continue;
            socket_t sock = ::accept(listen_, nullptr, nullptr);
            if (sock == kInvalidSocket) break;
            std::lock_guard<std::mutex> lk(workersM_);
            workers_.emplace_back([this, sock]() { serveConn(sock); });
        }
    }

    void serveConn(socket_t sock) {
        while (running_.load()) {
            Bytes frame;
            if (!readFrame(sock, running_, frame)) break;
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) break;
            RemotingCommand resp;
            resp.opaque = req.opaque;
            resp.markResponseType();
            if (req.code == RequestCode::GET_ROUTEINFO_BY_TOPIC) {
                const std::string topic =
                    req.extFields.count("topic") ? req.extFields["topic"] : std::string();
                {
                    std::lock_guard<std::mutex> lk(state_);
                    arrivals_.emplace_back(topic, msBetween(t0_, Clock::now()));
                }
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
                resp.remark = found ? "route ok" : "topic not exist";
                resp.hasRemark = true;
                if (found) {
                    resp.body = body;
                    resp.hasBody = true;
                }
            } else {
                if (req.isOnewayRpc()) continue;
                resp.code = ResponseCode::SUCCESS;
            }
            const Bytes out = resp.encode();
            if (!writeAll(sock, out)) break;
        }
        netcompat::closeSocket(sock);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::mutex state_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
    std::map<std::string, Bytes> routes_;
    std::vector<std::pair<std::string, int64_t>> arrivals_;
    Clock::time_point t0_ = Clock::now();
};

TopicRouteData makeRoute(const std::string& brokerAddr) {
    TopicRouteData route;
    route.queueDatas.emplace_back("broker-a", 1, 1, 6, 0);
    std::map<int64_t, std::string> addrs;
    addrs[0] = brokerAddr;
    route.brokerDatas.emplace_back("DefaultCluster", "broker-a", addrs);
    return route;
}

class NullListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

// ---------------------------------------------------------------- cases

void testFacadeDefaults() {
    DefaultMQProducer producer("PG_intervals_default");
    expectInt(producer.pollNameServerIntervalMillis(), 30000, "producer.pollNameServerInterval default");
    DefaultMQPushConsumer consumer("CID_intervals_default");
    expectInt(consumer.pollNameServerIntervalMillis(), 30000, "consumer.pollNameServerInterval default");
    expectInt(consumer.persistConsumerOffsetIntervalMillis(), 5000,
              "consumer.persistConsumerOffsetInterval default");
    DefaultMQPullConsumer pull("PG_intervals_pull");
    expectInt(pull.pollNameServerIntervalMillis(), 30000, "pullConsumer.pollNameServerInterval default");
    DefaultLitePullConsumer lite("PG_intervals_lite");
    expectInt(lite.pollNameServerIntervalMillis(), 30000, "litePullConsumer.pollNameServerInterval default");
    DefaultMQAdminExt admin("PG_intervals_admin");
    expectInt(admin.pollNameServerIntervalMillis(), 30000, "admin.pollNameServerInterval default");

    producer.setPollNameServerIntervalMillis(700);
    consumer.setPollNameServerIntervalMillis(1500);
    consumer.setPersistConsumerOffsetIntervalMillis(250);
    pull.setPollNameServerIntervalMillis(800);
    lite.setPollNameServerIntervalMillis(900);
    admin.setPollNameServerIntervalMillis(1100);
    expectInt(producer.pollNameServerIntervalMillis(), 700, "producer setter");
    expectInt(consumer.pollNameServerIntervalMillis(), 1500, "consumer setter");
    expectInt(consumer.persistConsumerOffsetIntervalMillis(), 250, "consumer persist setter");
    expectInt(pull.pollNameServerIntervalMillis(), 800, "pullConsumer setter");
    expectInt(lite.pollNameServerIntervalMillis(), 900, "litePullConsumer setter");
    expectInt(admin.pollNameServerIntervalMillis(), 1100, "admin setter");
}

void testForwardingToInstance(FakeNamesrv& ns) {
    const std::string topic = "SchedIntervalForward";
    ns.addRoute(topic, makeRoute(ns.address()));
    {
        DefaultMQProducer p("PG_intervals_fwd");
        p.setInstanceName("sched_fwd_producer");
        p.setNamesrvAddr(ns.address());
        p.setPollNameServerIntervalMillis(700);
        p.start();
        expectInt(p.client().pollNameServerIntervalMillis(), 700,
                  "producer.pollNameServerInterval reaches MQClientInstance");
        p.shutdown();
    }
    {
        DefaultMQPushConsumer c("CID_intervals_fwd");
        c.setInstanceName("sched_fwd_consumer");
        c.setNamesrvAddr(ns.address());
        c.subscribe(topic, "*");
        c.setMessageListener(std::make_shared<NullListener>());
        c.setPollNameServerIntervalMillis(1500);
        c.setPersistConsumerOffsetIntervalMillis(250);
        c.start();
        expectInt(c.client().pollNameServerIntervalMillis(), 1500,
                  "consumer.pollNameServerInterval reaches MQClientInstance");
        c.shutdown();
    }
}

// 3/4. The first tick lands at the initialDelay (10ms); the period follows the configured value.
//
// Both historical wrong shapes surface here:
//   a. old version: the loop head slept a whole 30s (hard-coded period) -> no request within 5s;
//   b. wrong order: sleep initialDelay first, then sleep a full period at the loop head, so the
//      first tick ~= one period (1200ms) and exceeds the period/2 threshold.
// The threshold is period/2: an ideal first tick is 10ms plus a loopback connect; only a wrong order doubles it.
void testRouteRefreshTiming(FakeNamesrv& ns) {
    const std::string topic = "SchedIntervalTick";
    ns.addRoute(topic, makeRoute(ns.address()));
    const int32_t periodMillis = 1200;

    MQClientInstance instance("sched_tick_instance",
                              {ns.address()},
                              /*connectTimeoutMillis=*/3000,
                              /*invokeTimeoutMillis=*/15000,
                              /*tlsEnable=*/false,
                              /*unitName=*/std::string(),
                              periodMillis);
    expectInt(instance.pollNameServerIntervalMillis(), periodMillis,
              "instance keeps the configured poll interval");
    instance.registerTopicInUse(topic);
    ns.markStart();
    instance.start();

    // Wait until 3 ticks are in (first ~10ms + 2 x 1200ms), capped by a 6s fallback
    const auto deadline = Clock::now() + std::chrono::milliseconds(6000);
    std::vector<int64_t> arrivals;
    while (Clock::now() < deadline) {
        arrivals = ns.arrivalsFor(topic);
        if (arrivals.size() >= 3) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    instance.shutdown();

    expect(arrivals.size() >= 3,
           "route refresh ticks 3 times within 6s",
           "got " + std::to_string(arrivals.size()));
    if (arrivals.empty()) return;
    // One line of the measured cadence (first tick ~10ms, gap ~1200ms; the old shape logged none)
    std::printf("  INFO route refresh arrivals (ms since start, period=%dms):", periodMillis);
    for (size_t i = 0; i < arrivals.size() && i < 6; ++i) {
        std::printf(" %lld", static_cast<long long>(arrivals[i]));
    }
    std::printf("\n");

    expect(arrivals[0] < periodMillis / 2,
           "first tick lands at the initialDelay, not one period later",
           "first=" + std::to_string(arrivals[0]) + "ms");
    if (arrivals.size() < 2) return;
    const int64_t gap = arrivals[1] - arrivals[0];
    expect(gap >= 1000 && gap <= 2400, "second tick follows the configured period",
           "gap=" + std::to_string(gap) + "ms period=" + std::to_string(periodMillis) + "ms");
    const int64_t gap2 = arrivals[2] - arrivals[1];
    expect(gap2 >= 1000 && gap2 <= 2400, "third tick follows the configured period",
           "gap=" + std::to_string(gap2) + "ms");
}

void testBadPeriodFallsBackToDefault() {
    MQClientInstance instance("sched_bad_period_instance", {"127.0.0.1:1"}, 3000, 15000, false,
                              std::string(), 0);
    expectInt(instance.pollNameServerIntervalMillis(), 30000,
              "non-positive poll interval falls back to the default 30000");
    MQClientInstance negative("sched_neg_period_instance", {"127.0.0.1:1"}, 3000, 15000, false,
                              std::string(), -1);
    expectInt(negative.pollNameServerIntervalMillis(), 30000,
              "negative poll interval falls back to the default 30000");
}

}  // namespace

int main() {
    FakeNamesrv ns;
    testFacadeDefaults();
    testForwardingToInstance(ns);
    testRouteRefreshTiming(ns);
    testBadPeriodFallsBackToDefault();

    std::printf("scheduled_intervals: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
