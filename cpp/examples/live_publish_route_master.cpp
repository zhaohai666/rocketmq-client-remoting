// Live publish-route check under master/slave failover, asserting each scenario in turn.
//
// Prerequisite: name server + master + slave all running (per the local cluster runbook); the script itself
// **stops the master only once** (scripts/rmq_test_broker.sh only knows the master's java process), and the teardown block guarantees the master comes back.
//
//
//
// Offline unit tests: tests/test_route_heartbeat.cpp locks the publish/subscription "queue-set criterion" of the group locks,
// tests/test_publish_route_master.cpp locks the "address criterion"; the live run locks the result of those
// criteria applied to real route shapes — broker-a in the name server really does shrink to {1: slave}.
//
//
// Usage: ./rmq_live_publish_route_master 127.0.0.1:9876 [127.0.0.1:10911] [127.0.0.1:10931]
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::printf("  [%s] %s%s\n", ok ? "PASS" : "FAIL", name.c_str(),
                detail.empty() ? "" : ("  " + detail).c_str());
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

double monotonicSeconds() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// In a live window the convergence of routes/offsets is asynchronous: a timeout is not an assertion failure, the caller asserts on the returned value.
bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 500) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

std::string trimRight(const std::string& s) {
    size_t end = s.find_last_not_of(" \t\r\n");
    return end == std::string::npos ? std::string() : s.substr(0, end + 1);
}

std::string readFile(const std::string& path) {
    std::ifstream in(path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return trimRight(ss.str());
}

std::string gBrokerCtl;

/// Run scripts/rmq_test_broker.sh. Its output goes to a **file** rather than a pipe: start turns the broker into
/// a daemon process, and whoever inherits its write end would never see EOF.
bool brokerCtl(const std::string& action, std::string& out) {
    const std::string logPath = "/tmp/rmq_pr_master_cpp_broker_ctl." + action + ".log";
    const std::string cmd = "sh '" + gBrokerCtl + "' " + action + " > '" + logPath + "' 2>&1";
    int rc = std::system(cmd.c_str());
    out = readFile(logPath);
    return rc != -1 && WEXITSTATUS(rc) == 0;
}

const int32_t kQueues = 4;
const char* const kBrokerName = "broker-a";
// Upper bound for local-side failures: with empty publish info there is no wire call at all; 500ms on a live machine already leaves an order-of-magnitude margin.
constexpr double kLocalBudgetMs = 500.0;

// Collect bodies only: this run cares whether the 4 messages on the slave can all be received, not offsets or order.
struct BodySink {
    std::mutex mtx;
    std::vector<std::string> bodies;

    std::vector<std::string> snapshot() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies;
    }
};

class BodyListener : public MessageListenerConcurrently {
public:
    explicit BodyListener(BodySink& sink) : sink_(sink) {}

    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) {
            sink_.bodies.push_back(m.getBody());
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    BodySink& sink_;
};

std::string fmtList(const std::vector<std::string>& items) {
    std::string s;
    for (const std::string& it : items) {
        if (!s.empty()) s += ",";
        s += it;
    }
    return s;
}

std::string fmtAddrs(const std::map<int64_t, std::string>& addrs) {
    std::string s;
    for (const auto& kv : addrs) {
        if (!s.empty()) s += " ";
        s += std::to_string(kv.first) + ":" + kv.second;
    }
    return "{" + s + "}";
}

std::string fmtQueues(const std::vector<MessageQueue>& qs) {
    std::string s;
    for (const MessageQueue& q : qs) {
        if (!s.empty()) s += " ";
        s += "(" + q.brokerName + "," + std::to_string(q.queueId) + ")";
    }
    return s;
}

std::string fmtMqOffsets(const std::vector<std::pair<MessageQueue, int64_t>>& rows) {
    std::string s;
    for (const auto& r : rows) {
        if (!s.empty()) s += " ";
        s += "q" + std::to_string(r.first.queueId) + ":" + std::to_string(r.second);
    }
    return s;
}

struct Fixture {
    std::string namesrv;
    std::string master;
    std::string slave;
    std::string stamp;
    DefaultMQAdminExt admin;
    std::shared_ptr<DefaultMQProducer> producer;

    void start(const std::string& ns, const std::string& masterAddr, const std::string& slaveAddr) {
        namesrv = ns;
        master = masterAddr;
        slave = slaveAddr;
        stamp = std::to_string(nowMs() % 1000000);
        admin.setNamesrvAddr(ns);
        admin.start();
        // The admin instance and the producer instance have different clientIds ⇒ two MQClientInstances inside the process:
        // S5's "stale cache" looks at the sender instance's own table, so both must be drivable separately here.
        producer = std::make_shared<DefaultMQProducer>("PID_PrMasterCpp_" + stamp);
        producer->setNamesrvAddr(ns);
        producer->setInstanceName("pr_master_cpp_" + stamp);
        producer->start();
    }

    std::string topic() const { return "PrMasterCpp" + stamp; }

    // Take broker-a's brokerAddrs after a forced refresh (empty map when the refresh fails).
    std::map<int64_t, std::string> routeAddrs(const std::string& t) {
        try {
            admin.client().updateTopicRouteInfoFromNameServer(t, 5000, false);
        } catch (const std::exception& e) {
            std::printf("    (route refresh failed: %s)\n", e.what());
        }
        std::shared_ptr<TopicRouteData> route = admin.client().getTopicRouteData(t);
        if (route == nullptr) return std::map<int64_t, std::string>();
        for (const BrokerData& bd : route->getBrokerDatas()) {
            if (bd.getBrokerName() == kBrokerName) return bd.getBrokerAddrs();
        }
        return std::map<int64_t, std::string>();
    }

    std::vector<MessageQueue> seedQueues() {
        return admin.client().getTopicPublishInfo(topic(), false)->msgQueueList;
    }

    int64_t maxOffsetOf(const MessageQueue& mq, const std::string& addr) {
        return admin.client().getMaxOffset(mq, 5000, addr);
    }

    void shutdown() {
        if (producer) {
            try {
                producer->shutdown();
            } catch (const std::exception&) {
            }
        }
        try {
            admin.shutdown();
        } catch (const std::exception&) {
        }
    }
};

bool hasId(const std::map<int64_t, std::string>& addrs, int64_t id) {
    return addrs.find(id) != addrs.end();
}

/// The criterion for "the publish-info group yields no queues": the accessor returns only when the publish info **has queues**,
/// otherwise it throws MQClientException("Can not find Message Queue for topic: ...") locally.
/// Returns true when exactly that one was thrown (detail carries the original text); returns false when a non-empty list is unexpectedly obtained.
bool publishInfoEmpty(MQClientInstance& client, const std::string& topic, std::string& detail) {
    try {
        std::shared_ptr<TopicPublishInfo> info = client.getTopicPublishInfo(topic, false);
        detail = "unexpectedly got " + std::to_string(info->msgQueueList.size()) + " queues";
        return false;
    } catch (const MQClientException& e) {
        detail = e.what();
        return std::string(e.what()).find("Can not find Message Queue for topic") !=
               std::string::npos;
    }
}

void scenario(Fixture& fx, std::shared_ptr<DefaultMQPushConsumer>& consumer) {
    const std::string topic = fx.topic();

    // ---------------- S0 control leg ----------------
    std::printf("S0 control leg (master up): route {0: master, 1: slave}, publish/subscribe each %d queues\n", kQueues);
    try {
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic, kQueues);
    } catch (const std::exception& e) {
        check("S0 create topic (master)", false, e.what());
        return;
    }
    // Also create it directly on the slave instead of betting on SlaveSynchronize's 5s cycle
    try {
        fx.admin.createTopicInBroker(fx.slave, topic, kQueues, kQueues);
    } catch (const std::exception& e) {
        std::printf("    (topic creation on the slave failed: %s)\n", e.what());
    }

    std::map<int64_t, std::string> addrs;
    const bool routeOk = waitUntil(
        [&] {
            addrs = fx.routeAddrs(topic);
            return hasId(addrs, 0) && hasId(addrs, 1);
        },
        30000);
    check("S0 route contains {0: master, 1: slave}", routeOk, "brokerAddrs=" + fmtAddrs(addrs));
    if (!routeOk) return;
    check("S0 slave address matches the argument", addrs[1] == fx.slave,
          "route=" + addrs[1] + " argv=" + fx.slave);

    std::vector<MessageQueue> queues;
    try {
        queues = fx.seedQueues();
    } catch (const std::exception& e) {
        check("S0 publish queues 4 (control)", false, e.what());
        return;
    }
    check("S0 publish queues 4 (control)", queues.size() == static_cast<size_t>(kQueues),
          "queues=" + fmtQueues(queues));
    const std::vector<MessageQueue> subs = fx.admin.client().getTopicSubscribeInfo(topic);
    check("S0 subscribe queues 4 (control)", subs.size() == static_cast<size_t>(kQueues),
          "queues=" + fmtQueues(subs));
    if (queues.size() != static_cast<size_t>(kQueues)) return;

    // ---------------- S1 seeding ----------------
    std::printf("\nS1 seed %d messages (one pinned per queue) and wait for the slave store to catch up\n", kQueues);
    std::vector<std::string> seeded;
    for (size_t i = 0; i < queues.size(); ++i) {
        const std::string body = "pr-master-" + std::to_string(i);
        try {
            Message msg(topic, body);
            SendResult r = fx.producer->send(msg, queues[i], 20000);
            if (r.sendStatus == SendStatus::SEND_OK) {
                seeded.push_back(body);
            } else {
                check("S1 seeded message #" + std::to_string(i) + " SEND_OK", false,
                      "status=" + std::to_string(static_cast<int>(r.sendStatus)));
            }
        } catch (const std::exception& e) {
            check("S1 seeded message #" + std::to_string(i) + " SEND_OK", false, e.what());
        }
    }
    check("S1 all 4 seeded messages SEND_OK", seeded.size() == static_cast<size_t>(kQueues),
          "seeded=" + std::to_string(seeded.size()));
    if (seeded.size() != static_cast<size_t>(kQueues)) return;

    std::vector<std::pair<MessageQueue, int64_t>> masterMax;
    for (const MessageQueue& mq : queues) {
        try {
            masterMax.emplace_back(mq, fx.maxOffsetOf(mq, fx.master));
        } catch (const std::exception& e) {
            std::printf("    (master maxOffset query failed: %s)\n", e.what());
        }
    }
    std::vector<std::pair<MessageQueue, int64_t>> slaveMax;
    const bool caughtUp = waitUntil(
        [&] {
            if (masterMax.empty()) return false;
            std::vector<std::pair<MessageQueue, int64_t>> now;
            for (const MessageQueue& mq : queues) {
                try {
                    now.emplace_back(mq, fx.maxOffsetOf(mq, fx.slave));
                } catch (const std::exception& e) {
                    std::printf("    (slave maxOffset query failed: %s)\n", e.what());
                    return false;
                }
            }
            for (const auto& m : masterMax) {
                bool covered = false;
                for (const auto& s : now) {
                    if (s.first.queueId == m.first.queueId && s.second >= m.second) covered = true;
                }
                if (!covered) return false;
            }
            slaveMax = now;
            return true;
        },
        30000);
    check("S1 the 4 messages are replicated to the slave store", caughtUp,
          "master=" + fmtMqOffsets(masterMax) + " slave=" + fmtMqOffsets(slaveMax));

    // ---------------- S2 stop master ----------------
    std::printf("\nS2 stop the master (scripts/rmq_test_broker.sh stop) and wait until the route keeps only the slave\n");
    std::string out;
    check("S2 master shut down gracefully", brokerCtl("stop", out), out);

    std::map<int64_t, std::string> downAddrs;
    const bool masterless = waitUntil(
        [&] {
            downAddrs = fx.routeAddrs(topic);
            return !hasId(downAddrs, 0) && hasId(downAddrs, 1);
        },
        60000);
    check("S2 broker-a in the route keeps only {1: slave}", masterless, "brokerAddrs=" + fmtAddrs(downAddrs));
    if (!masterless) return;

    // ---------------- S3 (A) publish queues ----------------
    std::printf("\nS3 (A) publish info skips brokers that have no master\n");
    std::string pubDetail;
    check("S3 publish queues == 0 after the master stops (the accessor throws 'no queue selectable' locally)",
          publishInfoEmpty(fx.admin.client(), topic, pubDetail), pubDetail);

    // ---------------- S4 (C) subscribe queues ----------------
    const std::vector<MessageQueue> subsDown = fx.admin.client().getTopicSubscribeInfo(topic);
    bool allBrokerA = subsDown.size() == static_cast<size_t>(kQueues);
    for (const MessageQueue& q : subsDown) {
        if (q.brokerName != kBrokerName) allBrokerA = false;
    }
    check("S4 (C) subscribe queues are still 4 and all on broker-a (the consumer side does not look at the master)", allBrokerA,
          "queues=" + fmtQueues(subsDown));

    // ---------------- S5 (A qualified) fast send failure ----------------
    // Split into two phases: while **the cache has not refreshed** (in practice the 30s periodic task has not fired) the sender instance still holds the
    // pre-stop route, address resolution lands on the dead master, failing fast and never silently rerouting to the slave; after **the route is
    // refreshed into the post-stop shape** (periodic task / explicit refresh), (A) takes effect: the publish queue list is empty and the send emits no wire at all.
    std::printf("\nS5 sync send without a chosen queue: stale-cache fast failure -> local fast failure after refresh\n");
    const double staleBegin = monotonicSeconds();
    std::string staleText;
    bool staleFailed = false;
    try {
        Message msg(topic, "must-not-send");
        SendResult r = fx.producer->send(msg, 20000);
        staleText = "Ok(status=" + std::to_string(static_cast<int>(r.sendStatus)) + ")";
    } catch (const std::exception& e) {
        staleFailed = true;
        staleText = e.what();
    }
    const double staleMs = (monotonicSeconds() - staleBegin) * 1000.0;
    // This leg is opportunistic: whether the periodic refresh already ran is not decided by this script. The shared criterion of both legs is
    // "fast failure + never land on the slave address" (the stale-cache leg hits the dead master).
    check("S5 the send fails fast and the error carries no slave address (never rerouted to the slave)",
          staleFailed && staleMs < kLocalBudgetMs &&
              staleText.find(fx.slave) == std::string::npos,
          std::to_string(static_cast<int>(staleMs)) + "ms " + staleText);

    // Let the sender instance refresh its own route cache into the post-stop shape (the same code path as the 30s periodic task)
    try {
        fx.producer->client().updateTopicRouteInfoFromNameServer(topic, 5000, false);
    } catch (const std::exception& e) {
        std::printf("    (sender route refresh failed: %s)\n", e.what());
    }
    std::string sendPubDetail;
    check("S5b the sender instance's publish queues also == 0 ((A) applies right here)",
          publishInfoEmpty(fx.producer->client(), topic, sendPubDetail), sendPubDetail);

    const double freshBegin = monotonicSeconds();
    std::string freshText;
    int32_t freshCode = 0;
    bool freshFailed = false;
    try {
        Message msg2(topic, "must-not-send-2");
        SendResult r = fx.producer->send(msg2, 20000);
        freshText = "Ok(status=" + std::to_string(static_cast<int>(r.sendStatus)) + ")";
    } catch (const MQClientException& e) {
        freshFailed = true;
        freshCode = e.getResponseCode();
        freshText = e.what();
    } catch (const std::exception& e) {
        freshFailed = true;
        freshText = e.what();
    }
    const double freshMs = (monotonicSeconds() - freshBegin) * 1000.0;
    check("S5c after the refresh: local 10005 throws 'no queue selectable', no wire call (no BrokersSent)",
          freshFailed && freshCode == ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION &&
              freshText.find("Can not find Message Queue for topic") != std::string::npos &&
              freshText.find("BrokersSent") == std::string::npos && freshMs < kLocalBudgetMs,
          std::to_string(static_cast<int>(freshMs)) + "ms code=" + std::to_string(freshCode) +
              ": " + freshText);

    std::printf("\nS5d (B) control: pinned send to that queue -> the local side reports the broker does not exist (no wire)\n");
    const double pinnedBegin = monotonicSeconds();
    std::string pinnedText;
    bool pinnedFailed = false;
    bool pinnedIsClientError = false;
    try {
        Message msg3(topic, "pinned-to-slave");
        SendResult r = fx.producer->send(msg3, MessageQueue(topic, kBrokerName, 0), 20000);
        pinnedText = "Ok(status=" + std::to_string(static_cast<int>(r.sendStatus)) + ")";
    } catch (const MQClientException& e) {
        // MQBrokerException does not derive from MQClientException: reaching here means the code did not come from the broker
        pinnedFailed = true;
        pinnedIsClientError = true;
        pinnedText = e.what();
    } catch (const std::exception& e) {
        pinnedFailed = true;
        pinnedText = e.what();
    }
    const double pinnedMs = (monotonicSeconds() - pinnedBegin) * 1000.0;
    check("S5d the pinned send reports 'The broker[broker-a] not exist' locally, with no wire"
          " (not SYSTEM_BUSY(2) answered by the slave)",
          pinnedFailed && pinnedIsClientError &&
              pinnedText == "The broker[" + std::string(kBrokerName) + "] not exist" &&
              pinnedMs < kLocalBudgetMs,
          std::to_string(static_cast<int>(pinnedMs)) + "ms " + pinnedText);

    std::printf("\nS5e (D) order lock: the whole broker is skipped inside the stopped window (no route refresh, no wire)\n");
    const std::string group = "GID_PrMasterCpp_" + fx.stamp;
    const std::string clientId = "pr_master_cpp_lock_" + fx.stamp;
    const std::vector<MessageQueue> lockMqs{MessageQueue(topic, kBrokerName, 0)};
    const double lockBegin = monotonicSeconds();
    std::vector<MessageQueue> locks;
    try {
        locks = fx.admin.client().lockBatchMq(group, clientId, lockMqs, 3000);
    } catch (const std::exception& e) {
        std::printf("    (lockBatchMq threw: %s)\n", e.what());
    }
    const double lockMs = (monotonicSeconds() - lockBegin) * 1000.0;
    check("S5e with only the slave left no queue is locked (the old behavior would fall back to locking on the slave)",
          locks.empty() && lockMs < kLocalBudgetMs,
          std::to_string(static_cast<int>(lockMs)) + "ms locked=[" + fmtQueues(locks) + "]");

    // Control leg: the same request body names the slave directly — the slave *does* grant the lock by design
    JsonValue lockBody = JsonValue::makeObject();
    lockBody.set("consumerGroup", JsonValue::makeString(group));
    lockBody.set("clientId", JsonValue::makeString(clientId));
    JsonValue mqArr = JsonValue::makeArray();
    JsonValue mqObj = JsonValue::makeObject();
    mqObj.set("topic", JsonValue::makeString(topic));
    mqObj.set("brokerName", JsonValue::makeString(kBrokerName));
    mqObj.set("queueId", JsonValue::makeInt(0));
    mqArr.pushArray(mqObj);
    lockBody.set("mqSet", mqArr);
    const std::string lockBodyText = lockBody.dump();
    const Bytes lockPayload(lockBodyText.begin(), lockBodyText.end());
    int64_t slaveOk = -1;
    try {
        RemotingCommand resp = fx.admin.client().invokeSyncRaw(
            fx.slave, RequestCode::LOCK_BATCH_MQ, PropertyMap(), lockPayload, true, 3000);
        if (resp.hasBody) {
            JsonValue root;
            std::string err;
            if (jsonParse(std::string(resp.body.begin(), resp.body.end()), root, &err)) {
                const JsonValue* okSet = root.find("lockOKMQSet");
                if (okSet != nullptr && okSet->isArray()) {
                    slaveOk = static_cast<int64_t>(okSet->size());
                }
            }
        }
    } catch (const std::exception& e) {
        std::printf("    (slave lock request threw: %s)\n", e.what());
    }
    check("S5e2 control: naming the slave grants the lock (an empty set is not a broker refusal)", slaveOk == 1,
          "slave lockOKMQSet=" + std::to_string(slaveOk));
    // The lock taken on the control leg must be handed back (nobody would release it from the slave's lock manager)
    try {
        fx.admin.client().invokeSyncRaw(fx.slave, RequestCode::UNLOCK_BATCH_MQ, PropertyMap(),
                                        lockPayload, true, 3000);
    } catch (const std::exception&) {
    }

    const double unlockBegin = monotonicSeconds();
    try {
        fx.admin.client().unlockBatchMq(group, clientId, lockMqs, 3000);
    } catch (const std::exception& e) {
        std::printf("    (unlockBatchMq threw: %s)\n", e.what());
    }
    const double unlockMs = (monotonicSeconds() - unlockBegin) * 1000.0;
    check("S5e3 unlocking is skipped just as quietly (no throw, no send)", unlockMs < kLocalBudgetMs,
          std::to_string(static_cast<int>(unlockMs)) + "ms");

    std::printf("\nS5f (E) POP pull: the local side reports 'The broker[broker-a] not exist' (no wire)\n");
    const double popBegin = monotonicSeconds();
    std::string popText;
    bool popFailed = false;
    bool popIsClientError = false;
    try {
        PopResult popResult = fx.admin.client().popMessage(group, topic, 0, 1, 30000, 100, 0);
        popText = std::string("Ok(status=") + popStatusName(popResult.status) + ")";
    } catch (const MQClientException& e) {
        popFailed = true;
        popIsClientError = true;
        popText = e.what();
    } catch (const std::exception& e) {
        popFailed = true;
        popText = e.what();
    }
    const double popMs = (monotonicSeconds() - popBegin) * 1000.0;
    check("S5f POP inside the stopped window reports 'The broker[broker-a] not exist' locally (not an error answered by the slave)",
          popFailed && popIsClientError &&
              popText == "The broker[" + std::string(kBrokerName) + "] not exist" &&
              popMs < kLocalBudgetMs,
          std::to_string(static_cast<int>(popMs)) + "ms " + popText);

    std::printf("\nS5g (F) offset reads: a cold instance refreshes the route once then falls back to the slave\n");
    {
        MQClientInstance cold("pr_master_cpp_cold_" + fx.stamp,
                              std::vector<std::string>{fx.namesrv});
        std::string coldDetail;
        bool coldOk = false;
        try {
            int64_t off = 0;
            const bool found = cold.queryConsumerOffset(group, MessageQueue(topic, kBrokerName, 0), off);
            coldOk = true;
            coldDetail = found ? ("offset=" + std::to_string(off)) : "offset=None(QUERY_NOT_FOUND)";
        } catch (const std::exception& e) {
            coldDetail = e.what();
        }
        check("S5g the cold instance reads the offset without error: refresh the route -> fall back to the slave and let the broker answer", coldOk, coldDetail);
    }

    // ---------------- S6 (C end to end) consuming inside the stopped window ----------------
    std::printf("\nS6 a push consumer started inside the stopped window: 4 queues + all 4 pre-seeded messages received from the slave\n");
    BodySink sink;
    consumer = std::make_shared<DefaultMQPushConsumer>(group);
    consumer->setNamesrvAddr(fx.namesrv);
    consumer->setInstanceName("pr_master_cpp_c_" + fx.stamp);
    consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    consumer->setMessageListener(std::make_shared<BodyListener>(sink));
    consumer->subscribe(topic, "*");
    try {
        consumer->start();
    } catch (const std::exception& e) {
        check("S6 consumer start", false, e.what());
        consumer.reset();
        return;
    }
    // client() requires a started consumer (it throws otherwise on the C++ side), so it can only be taken after start.
    try {
        const std::vector<MessageQueue> subsInWindow =
            consumer->client().getTopicSubscribeInfo(topic);
        check("S6a the consumer's own subscription info inside the window is also 4 queues",
              subsInWindow.size() == static_cast<size_t>(kQueues), "queues=" + fmtQueues(subsInWindow));
    } catch (const std::exception& e) {
        check("S6a the consumer's own subscription info inside the window is also 4 queues", false, e.what());
    }
    std::vector<std::string> expected = seeded;
    std::sort(expected.begin(), expected.end());
    const bool gotOk = waitUntil(
        [&] {
            std::vector<std::string> got = sink.snapshot();
            std::sort(got.begin(), got.end());
            return got == expected;
        },
        60000);
    check("S6 all 4 messages received from the slave while the master is down", gotOk, "got=[" + fmtList(sink.snapshot()) + "]");

    // ---------------- S7 recovery control (bring the master back first) ----------------
    std::printf("\nS7 recovery control: after the master returns, publish queues recover and sending recovers\n");
    std::string outStart;
    if (!brokerCtl("start", outStart)) {
        check("S7 master restart", false, outStart);
    }

    std::vector<MessageQueue> queuesBack;
    const bool publishBack = waitUntil(
        [&] {
            try {
                queuesBack = fx.seedQueues();
                return queuesBack.size() == static_cast<size_t>(kQueues);
            } catch (const std::exception&) {
                return false;
            }
        },
        60000);
    check("S7 publish queues recovered to 4", publishBack, "queues=" + fmtQueues(queuesBack));
    // Neither failed send left a message on the broker: every seeded queue's maxOffset is still 1
    std::vector<std::pair<MessageQueue, int64_t>> after;
    for (const MessageQueue& mq : (queuesBack.empty() ? queues : queuesBack)) {
        try {
            after.emplace_back(mq, fx.maxOffsetOf(mq, fx.master));
        } catch (const std::exception& e) {
            std::printf("    (maxOffset query failed: %s)\n", e.what());
        }
    }
    bool allOne = !after.empty();
    for (const auto& r : after) {
        if (r.second != 1) allOne = false;
    }
    check("S7b the two failed sends left no messages (maxOffset is still 1)", allOne, "maxOffset=" + fmtMqOffsets(after));
    try {
        Message back(topic, "pr-master-back");
        SendResult r = fx.producer->send(back, 20000);
        check("S7c send recovered with SEND_OK", r.sendStatus == SendStatus::SEND_OK,
              "status=" + std::to_string(static_cast<int>(r.sendStatus)));
    } catch (const std::exception& e) {
        check("S7c send recovered with SEND_OK", false, e.what());
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    const std::string nsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    const std::string masterAddr = argc > 2 ? argv[2] : "127.0.0.1:10911";
    const std::string slaveAddr = argc > 3 ? argv[3] : "127.0.0.1:10931";
    const char* root = std::getenv("RMQ_REPO_ROOT");
    gBrokerCtl = (root != nullptr ? std::string(root) : std::string("..")) +
                 "/scripts/rmq_test_broker.sh";

    std::printf("broker_ctl=%s namesrv=%s master=%s slave=%s\n", gBrokerCtl.c_str(),
                nsAddr.c_str(), masterAddr.c_str(), slaveAddr.c_str());
    std::string out;
    if (!brokerCtl("status", out)) {
        std::printf("the master is not running: first bring up namesrv + master + slave per the local cluster runbook (%s)\n",
                    out.c_str());
        return 2;
    }

    Fixture fx;
    std::shared_ptr<DefaultMQPushConsumer> consumer;
    try {
        fx.start(nsAddr, masterAddr, slaveAddr);
    } catch (const std::exception& e) {
        check("cluster liveness / client start", false, e.what());
        return 1;
    }

    try {
        scenario(fx, consumer);
    } catch (const std::exception& e) {
        check("the verification threw an exception", false, e.what());
    }

    // ---------------- Teardown ----------------
    // The master must come back (start is idempotent, and the exception paths also reach here); then shut the clients down and delete the topic.
    std::string outBack;
    if (!brokerCtl("start", outBack)) {
        std::printf("  [FAIL] master restart failed: %s\n", outBack.c_str());
        ++gFail;
    }
    if (consumer) {
        try {
            consumer->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (consumer shutdown failed: %s)\n", e.what());
        }
    }
    try {
        fx.admin.deleteTopic(fx.topic());
    } catch (const std::exception& e) {
        std::printf("    (deleteTopic(%s) failed: %s)\n", fx.topic().c_str(), e.what());
    }
    fx.shutdown();

    std::printf("\n== result: %d/%d passed ==\n", gPass, gPass + gFail);
    return gFail == 0 ? 0 : 1;
}
