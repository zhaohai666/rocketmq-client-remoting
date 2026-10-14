// Groups six to eight are the other half of the **subscribe-side lookup** contract (#104): the three call
// sites of `findBrokerAddressInSubscribe(brokerName, MASTER_ID, true)` -- orderly-consumption locking
// (master only, **never refreshes the route**, and a broker whose master is missing is skipped whole),
// POP pulling (refreshes the route once and still takes only the master) and consume-offset reading
// (refreshes the route once, then **relaxes** to the slave). The three contracts behave differently on the
// same "only a slave left" route, which is exactly why they must be tested separately.
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

// ---------------------------------------------------------------- fake endpoint (one shell for name server / broker)

struct ReqRecord {
    int32_t code = 0;
    PropertyMap ext;
};

// The same socket skeleton as MockEndpoint in test_pull_post_subscription.cpp: it answers routes as the
// name server, answers GET_MAX_OFFSET with the scripted offset as the broker, and books every arriving
// request by code -- assertions like "was the route refreshed" can only come from the wire.
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
                // the fake broker always answers POP with POLLING_TIMEOUT(210): that proves the request went
                // to the master address and, in passing, that the client translates 210 into PollingNotFound.
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

// ---------------------------------------------------------------- scaffolding

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

// Start an instance and push kTopic's route into the cache with a **real RPC** (GET_ROUTEINFO_BY_TOPIC to
// the fake name server) -- the flat table is written inside updateTopicRouteInfoFromNameServer itself.
std::unique_ptr<MQClientInstance> seeded(MockEndpoint& namesrv) {
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{namesrv.address()});
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    return instance;
}

// ---------------------------------------------------------------- 1. Lookup contracts

void testPublishLookupTakesOnlyTheMasterWhileTheTolerantLookupFallsBack() {
    MockEndpoint namesrv, master, slave;
    namesrv.addRoute(kTopic, routeOf(kBroker, onlySlave(slave.address())));
    auto instance = seeded(namesrv);

    expect(instance->findBrokerAddressInPublish(kBroker).empty(),
           "publish lookup finds nothing when only a slave (brokerId=1) remains: querying by MASTER_ID is empty");
    // Negative control: on the same route the tolerant lookup (heartbeat/pull/offset query) must still reach the slave
    expect(instance->brokerAddrOf(kBroker) == slave.address(),
           "on the same route brokerAddrOf falls back to the slave -- the two contracts must not be merged");

    namesrv.addRoute(kTopic, routeOf(kBroker, masterAndSlave(master.address(), slave.address())));
    expect(instance->updateTopicRouteInfoFromNameServer(kTopic), "refreshing the route after the master re-registers succeeds");
    expect(instance->findBrokerAddressInPublish(kBroker) == master.address(),
           "what is skipped is 'no master', not the name: as soon as the master registers it resolves");
}

// ---------------------------------------------------------------- 2. Refresh the route then recheck

void testPublishAddrForRefreshesTheRouteThenRechecks() {
    MockEndpoint namesrv, master;
    namesrv.addRoute(kTopic, routeOf(kBroker, {{MixAll::MASTER_ID, master.address()}}));
    // The cache is empty: this client has never seen the topic (targeted sending does not fetch publish info first, so this is the only route source)
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{namesrv.address()});
    const size_t before = namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC);

    expect(instance->publishAddrFor(kBroker, kTopic) == master.address(),
           "publishAddrFor contract: when nothing is found, refresh the route by topic once and recheck");
    expectInt(static_cast<long long>(
                  namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC) - before),
              1, "refreshed exactly once");
}

// ---------------------------------------------------------------- 3. Master gone → not exist

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
        expectInt(e.getResponseCode(), -1, "the responseCode of not exist == -1");
    }
    expect(threw, "publishAddrFor reports not exist when only a slave remains");
    expectInt(static_cast<long long>(
                  namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC) - before),
              1, "the route was refreshed once before reporting not exist");

    namesrv.addRoute(kTopic, routeOf(kBroker, masterAndSlave(master.address(), slave.address())));
    expect(instance->publishAddrFor(kBroker, kTopic) == master.address(),
           "negative control: the master resolves as soon as it registers");
}

// ---------------------------------------------------------------- 4. An unknown brokerName

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
    expect(threw, "no such brokerName in the route: likewise not exist, without falling back to another broker");
}

// ---------------------------------------------------------------- 5. Admin offset queries

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
    expect(threw, "getMaxOffset reports not exist when only a slave remains (this lookup only targets the master)");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::GET_MAX_OFFSET)), 0,
              "this is a local verdict: the slave received no offset query at all");

    // Negative control: once the master is back the query succeeds and lands on the **master** address (not on the slave in the route)
    namesrv.addRoute(kTopic, routeOf(kBroker, masterAndSlave(master.address(), slave.address())));
    expectInt(instance->getMaxOffset(mq), 7, "the query succeeds once the master is back");
    expectInt(static_cast<long long>(master.countCode(RequestCode::GET_MAX_OFFSET)), 1,
              "the request landed on the master address");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::GET_MAX_OFFSET)), 0,
              "the slave was never hit");
    expect(master.lastExt().at("topic") == kTopic, "the offset query carries topic");
}


void testOrderlyLocksSkipTheBrokerWhenTheMasterIsGone() {
    MockEndpoint namesrv, master, slave;
    namesrv.addRoute(kTopic, routeOf(kBroker, onlySlave(slave.address())));
    auto instance = seeded(namesrv);
    const size_t before = namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC);
    std::vector<MessageQueue> mqs{MessageQueue(kTopic, kBroker, 0),
                                  MessageQueue(kTopic, kBroker, 1)};

    expect(instance->lockBatchMq("G", kClientId, mqs).empty(),
           "not a single queue can be locked when only a slave remains");
    expectInt(static_cast<long long>(
                  namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC) - before),
              0, "lock/unlock never refresh the route (the dividing line from publishAddrFor)");
    instance->unlockBatchMq("G", kClientId, mqs);
    expectInt(static_cast<long long>(master.countCode(RequestCode::LOCK_BATCH_MQ)), 0,
              "no LOCK_BATCH_MQ was sent at all");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::LOCK_BATCH_MQ)), 0,
              "the slave must not be locked either");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::UNLOCK_BATCH_MQ)), 0,
              "UNLOCK likewise sent to none");

    // Negative control: as soon as the master registers, the locks succeed on the **master** address and the returned set comes from the response's lockOKMQSet
    namesrv.addRoute(kTopic, routeOf(kBroker, masterAndSlave(master.address(), slave.address())));
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    std::vector<MessageQueue> locked = instance->lockBatchMq("G", kClientId, mqs);
    expectInt(static_cast<long long>(locked.size()), 2, "both queues got locked");
    expect(locked.size() == 2 && locked[0].brokerName == kBroker && locked[0].queueId == 0 &&
               locked[1].queueId == 1,
           "the returned set is exactly the lockOKMQSet from the response");
    expectInt(static_cast<long long>(master.countCode(RequestCode::LOCK_BATCH_MQ)), 1,
              "LOCK_BATCH_MQ landed on the master address");
    instance->unlockBatchMq("G", kClientId, mqs);
    expectInt(static_cast<long long>(master.countCode(RequestCode::UNLOCK_BATCH_MQ)), 1,
              "UNLOCK_BATCH_MQ also landed on the master address");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::LOCK_BATCH_MQ)), 0,
              "the slave was never hit");
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
        expectInt(e.getResponseCode(), -1, "the responseCode of not exist == -1");
    }
    expect(threw, "POP reports not exist when only a slave remains");
    expectInt(static_cast<long long>(
                  namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC) - before),
              1, "the route was refreshed exactly once before the error");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::POP_MESSAGE)), 0,
              "the slave received no POP at all -- a write to a slave would only trade for SYSTEM_BUSY(2)");

    // Negative control: once the master returns, POP goes out and 210 becomes PollingNotFound
    namesrv.addRoute(kTopic, routeOf(kBroker, masterAndSlave(master.address(), slave.address())));
    PopResult result = instance->popMessage("G", kTopic, 0, 32, 30000, 0, 0);
    expect(result.status == PopStatus::POLLING_NOT_FOUND,
           "the fake broker's 210 becomes PollingNotFound");
    expectInt(static_cast<long long>(master.countCode(RequestCode::POP_MESSAGE)), 1,
              "POP landed on the master address");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::POP_MESSAGE)), 0,
              "the slave was never hit");
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
    expect(instance->queryConsumerOffset("G", mq, offset), "the offset is still readable when only a slave remains");
    expectInt(offset, 424242, "after one route refresh it relaxes to the slave");
    expectInt(static_cast<long long>(
                  namesrv.countCode(RequestCode::GET_ROUTEINFO_BY_TOPIC) - before),
              1, "the route was refreshed exactly once before relaxing");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::QUERY_CONSUMER_OFFSET)), 1,
              "QUERY_CONSUMER_OFFSET landed on the slave");

    // Negative control: with the master present the query goes to it (the master branch never enters the refresh logic, so the slave's counter stays frozen)
    namesrv.addRoute(kTopic, routeOf(kBroker, masterAndSlave(master.address(), slave.address())));
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    int64_t masterOffset = 0;
    expect(instance->queryConsumerOffset("G", mq, masterOffset), "the query succeeds while the master is present");
    expectInt(masterOffset, 111, "the offset is read from the master");
    expectInt(static_cast<long long>(slave.countCode(RequestCode::QUERY_CONSUMER_OFFSET)), 1,
              "the slave's counter stayed frozen");

    // The route has no such brokerName at all: report not exist, do not fall back to another broker, and do not silently return 0
    MockEndpoint ns2;
    ns2.addRoute(kTopic, routeOf("broker-b", {{MixAll::MASTER_ID, other.address()}}));
    auto instance2 = seeded(ns2);
    bool threw = false;
    int64_t unused = 0;
    try {
        instance2->queryConsumerOffset("G", MessageQueue(kTopic, kBroker, 0), unused);
    } catch (const MQClientException& e) {
        threw = std::string(e.what()) == "The broker[broker-a] not exist";
        expectInt(e.getResponseCode(), -1, "the responseCode of not exist == -1");
    }
    expect(threw, "an unknown brokerName likewise reports not exist");
    expectInt(static_cast<long long>(other.countCode(RequestCode::QUERY_CONSUMER_OFFSET)), 0,
              "the other broker was never hit");
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
