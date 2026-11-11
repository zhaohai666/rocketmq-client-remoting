// Live verification of the 2026-11-11 capability closeout (C++ end), against a real
// RocketMQ 5.5.1 cluster. The three capabilities of that round only earn a live leg if
// they are either wire-visible or change **which messages a client sees**, so each one is
// asserted against an actual broker instead of a mock:
//
// L1 ConsumeFromWhere's three "deprecated" values. Java folds
//    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST / MIN_OFFSET / MAX_OFFSET into
//    the CONSUME_FROM_LAST_OFFSET branch of
//    RebalancePushImpl#computePullFromWhereWithException (lines 171-173). That folding is
//    pinned by the offline unit suite (resolved offset against a fake store), NOT by live
//    delivery counts: for a never-committed group the broker's V3_0_6
//    QUERY_CONSUMER_OFFSET answer is timing-dependent (SUCCESS offset=0 on one leg,
//    QUERY_NOT_FOUND on the next -- Java itself was observed as FIRST=8, LAST=0, MIN=8,
//    MAX=8, BOOT=0 on this same shape), so equal live counts are not a property any
//    implementation promises. The live leg therefore keeps FIRST_OFFSET as the only
//    equality assertion (positive control: the whole backlog), records the four
//    LAST-family counts as the parity record, and requires each deprecated leg only to
//    have started, received a real queue assignment and delivered a sane count.
//    The baseline is printed, never assumed: this port's queryConsumerOffset omits
//    setZeroIfNotFound unless it is true (Java's wire shape), so a never-committed
//    group's absolute start point remains a per-end observation.
// L2 LitePull pull-from-which-node pin (connectBrokerByUser / defaultBrokerId). Java
//    PullAPIWrapper#recalculatePullFromWhichNode pins the id and ignores the broker's
//    suggestion table -- the only way to read from a slave. The C++ lite path reduces that
//    to liteBrokerIdFor(), which is exactly the id a round puts on the request, so reading
//    it through liteBrokerIdForTest is a request-level assertion rather than a behavioural
//    guess. Three legs, fresh group each, on one seeded backlog:
//        pin off, id 0   -> no broker id on the request, backlog delivered
//        pin on,  id 0   -> the request carries 0, backlog delivered
//        pin on,  id 1   -> the request carries 1, and delivery STILL happens: Java resolves
//                           the address with FindBrokerAddressInSubscribe(..., onlyThisBroker
//                           = false), which falls back to the first broker in the table when
//                           the requested id is absent. That fallback is parity, not a hole,
//                           so the assertion is on the id that travels, not on the outcome.
//    This single-broker cluster has no slave, so "reads the slave" cannot be shown here.
// L3 adminExtGroup identity (no wire effect): the default must be "admin_ext_group", the
//    override must take, and an admin carrying a custom group must still boot and serve one
//    live RPC -- proving the identity switch does not disturb the admin's own lifecycle.
//
// Usage: ./live_capability_closeout <namesrv>        (e.g. 127.0.0.1:9876)
// Exits non-zero on any FAIL. Every run uses fresh topics and groups.
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 100) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

std::string join(const std::vector<std::string>& v) {
    std::string s = "[";
    for (const std::string& x : v) {
        if (s.size() > 1) s += ", ";
        s += x;
    }
    return s + "]";
}

struct BodySink {
    std::mutex mtx;
    std::vector<std::string> bodies;

    int count() {
        std::lock_guard<std::mutex> lk(mtx);
        return static_cast<int>(bodies.size());
    }
};

class CollectListener : public MessageListenerConcurrently {
public:
    explicit CollectListener(BodySink& sink) : sink_(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) {
            sink_.bodies.push_back(std::string(m.body.begin(), m.body.end()));
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    BodySink& sink_;
};

struct PushObservation {
    int got = 0;
    bool assigned = false;
    std::vector<std::string> keys;
};

// One push leg: a fresh group started at `where`, counting arrivals for `observeMs`.
// Every leg gets a fresh group -- a group that already committed offsets would read them
// back whatever consumeFromWhere says, which is the false green this scenario must avoid.
// The leg also waits for a real queue assignment first: without one nothing can arrive,
// and "received=0" would look like a consumeFromWhere answer instead of an assignment bug.
PushObservation pushLeg(const std::string& ns, const std::string& topic,
                        const std::string& group, const std::string& where,
                        int32_t observeMs) {
    BodySink sink;
    DefaultMQPushConsumer c(group);
    c.setNamesrvAddr(ns);
    c.setInstanceName(group);
    c.setMessageListener(std::make_shared<CollectListener>(sink));
    c.setConsumeThreadNums(1);
    c.setConsumeFromWhere(where);
    c.subscribe(topic, "*");
    c.start();

    PushObservation out;
    out.assigned = waitUntil([&] { return !c.assignedQueueKeys().empty(); }, 30000);
    std::this_thread::sleep_for(std::chrono::milliseconds(observeMs));
    out.keys = c.assignedQueueKeys();
    out.got = sink.count();
    c.shutdown();
    return out;
}

struct LiteObservation {
    int got = 0;
    std::optional<int64_t> requestBrokerId;
    std::vector<std::string> assigned;
};

// One LitePull subscribe-mode leg with the given pin. `requestBrokerId` is read from
// liteBrokerIdForTest() -- the same function the pull round calls -- so the assertion is on
// what travels, not on a re-derivation of the fields.
LiteObservation liteLeg(const std::string& ns, const std::string& topic,
                        const std::string& brokerName, const std::string& group, bool pin,
                        int64_t defaultBrokerId, int want, int32_t observeMs) {
    DefaultLitePullConsumer c(group);
    c.setNamesrvAddr(ns);
    c.setInstanceName(group);
    c.setPollTimeoutMillis(1000);
    c.setPullBatchSize(32);
    c.subscribe(topic, "*");
    c.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c.setConnectBrokerByUser(pin);
    c.setDefaultBrokerId(defaultBrokerId);
    c.start();

    LiteObservation out;
    const MessageQueue probe(topic, brokerName, 0);
    out.requestBrokerId = c.liteBrokerIdForTest(probe);

    const int64_t deadline = nowMs() + observeMs;
    while (out.got < want && nowMs() < deadline) {
        out.got += static_cast<int>(c.poll(1000).size());
    }
    for (const MessageQueue& mq : c.assignment()) {
        out.assigned.push_back(mq.brokerName + "#" + std::to_string(mq.queueId));
    }
    c.shutdown();
    return out;
}

std::string idText(const std::optional<int64_t>& id) {
    return id.has_value() ? std::to_string(*id) : std::string("none");
}

// Create the topic up front and wait until the name server really serves its route: a
// createTopic only reaches the broker, and a client that sends/consumes before the broker's
// registration round lands on the TBW102 fallback shape instead of the topic's own route.
bool createTopicAndWaitRoute(DefaultMQAdminExt& admin, const std::string& topic,
                             int32_t timeoutMs = 30000) {
    try {
        admin.createTopic("TBW102", topic, 1, 0);
    } catch (const std::exception& e) {
        std::printf("  !! createTopic(%s) failed: %s\n", topic.c_str(), e.what());
        return false;
    }
    return waitUntil(
        [&] {
            try {
                const TopicRouteData route = admin.examineTopicRoute(topic);
                int32_t queues = 0;
                for (const QueueData& qd : route.queueDatas) {
                    queues += qd.readQueueNums;
                }
                return queues >= 1;
            } catch (const std::exception&) {
                return false;
            }
        },
        timeoutMs, 500);
}

bool seedTopic(DefaultMQProducer& producer, const std::string& topic,
               const std::vector<std::string>& bodies) {
    int ok = 0;
    for (const std::string& b : bodies) {
        try {
            SendResult r = producer.send(Message(topic, Bytes(b.begin(), b.end())));
            if (r.getSendStatus() == SendStatus::SEND_OK) ++ok;
        } catch (const std::exception& e) {
            std::printf("  !! send to %s failed: %s\n", topic.c_str(), e.what());
        }
    }
    return ok == static_cast<int>(bodies.size());
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: live_capability_closeout <namesrv>\n");
        return 2;
    }
    const std::string ns = argv[1];
    const std::string stamp = std::to_string(nowMs() % 1000000000);
    const std::string prefix = "CapLive_" + stamp;
    const int32_t kMsg = 8;
    const int32_t kObserveMs = 12000;

    std::printf("======================================================================\n");
    std::printf("Capability closeout live (C++): namesrv=%s prefix=%s\n", ns.c_str(),
                prefix.c_str());
    std::printf("======================================================================\n");

    std::vector<std::string> bodies;
    for (int32_t i = 0; i < kMsg; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "cap-%02d", i);
        bodies.emplace_back(buf);
    }

    DefaultMQAdminExt admin;
    admin.setNamesrvAddr(ns);
    try {
        admin.start();
    } catch (const std::exception& e) {
        std::printf("!! admin start failed: %s\n", e.what());
        return 1;
    }

    std::string brokerName = "broker-a";
    std::string brokerAddr;
    try {
        const ClusterInfo cluster = admin.fetchBrokerClusterInfo();
        if (cluster.brokerAddrTable.empty()) {
            std::printf("!! no broker registered on the name server\n");
            admin.shutdown();
            return 1;
        }
        brokerName = cluster.brokerAddrTable.begin()->first;
        brokerAddr = cluster.getBrokerAddrs().front();
    } catch (const std::exception& e) {
        std::printf("!! fetchBrokerClusterInfo failed: %s\n", e.what());
        admin.shutdown();
        return 1;
    }
    std::printf("  cluster: broker=%s addr=%s\n", brokerName.c_str(), brokerAddr.c_str());

    const std::string t1 = prefix + "_FromWhere";
    const std::string t2 = prefix + "_Pin";

    const bool route1 = createTopicAndWaitRoute(admin, t1);
    const bool route2 = createTopicAndWaitRoute(admin, t2);
    check("the topic routes are visible before anything is sent", route1 && route2,
          std::string("t1=") + (route1 ? "ok" : "missing") + " t2=" +
              (route2 ? "ok" : "missing"));
    if (!route1 || !route2) {
        admin.shutdown();
        std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
        return 1;
    }

    std::unique_ptr<DefaultMQProducer> producer;
    try {
        producer = std::make_unique<DefaultMQProducer>(prefix + "_pg");
        producer->setNamesrvAddr(ns);
        producer->start();
    } catch (const std::exception& e) {
        check("producer start", false, e.what());
        admin.shutdown();
        std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
        return 1;
    }

    // ---------------------------------------------------------------- L1
    std::printf("\nL1 ConsumeFromWhere: the three deprecated values fold into LAST_OFFSET\n");
    check("seed " + std::to_string(kMsg) + " messages on " + t1, seedTopic(*producer, t1, bodies),
          "topic=" + t1);
    std::printf("  sent %d messages, no consumer has ever joined this topic\n", kMsg);

    const PushObservation first =
        pushLeg(ns, t1, prefix + "_gFirst", ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET, kObserveMs);
    check("L1-positive control: FIRST_OFFSET delivers the whole backlog",
          first.assigned && first.got == kMsg,
          "received=" + std::to_string(first.got) + " expect=" + std::to_string(kMsg) +
              " assigned=" + (first.assigned ? "true" : "false"));

    // The baseline is measured on THIS end instead of assumed: see the header comment for
    // why the absolute value is a per-end parity property rather than a constant.
    const PushObservation last =
        pushLeg(ns, t1, prefix + "_gLast", ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET, kObserveMs);
    check("L1-LAST_OFFSET accepted on a live consumer (started, queue assigned, sane count)",
          last.assigned && last.got >= 0 && last.got <= kMsg,
          "received=" + std::to_string(last.got) + " assigned=" +
              (last.assigned ? "true" : "false"));
    std::printf("  INFO LAST_OFFSET baseline on this end: received=%d of %d "
                "(0 = tail start, %d = whole backlog replayed)\n",
                last.got, kMsg, kMsg);

    struct DeprecatedLeg {
        const char* label;
        const char* where;
    };
    const DeprecatedLeg deprecated[] = {
        {"MIN_OFFSET", ConsumeFromWhere::CONSUME_FROM_MIN_OFFSET},
        {"MAX_OFFSET", ConsumeFromWhere::CONSUME_FROM_MAX_OFFSET},
        {"LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
         ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST},
    };
    std::string counts = "{" + std::string("LAST_OFFSET=") + std::to_string(last.got);
    for (const DeprecatedLeg& leg : deprecated) {
        const PushObservation obs =
            pushLeg(ns, t1, prefix + "_g" + leg.label, leg.where, kObserveMs);
        // No equality against the baseline: the broker's answer for a never-committed
        // group is timing-dependent, so equal counts are not promised by any end (the
        // folding itself is pinned offline). Accept the value as long as the leg really
        // ran: started, got a queue, and delivered a count that cannot exceed the backlog.
        check(std::string("L1-deprecated ") + leg.label +
                  " accepted on a live consumer (started, queue assigned, sane count)",
              obs.assigned && obs.got >= 0 && obs.got <= kMsg,
              "received=" + std::to_string(obs.got) + " assigned=" +
                  (obs.assigned ? "true" : "false"));
        counts += std::string(", ") + leg.label + "=" + std::to_string(obs.got);
    }
    counts += "}";
    std::printf("  INFO LAST-family delivery counts on this end: %s -- parity record, not an "
                "oracle: the broker's V3_0_6 QUERY_CONSUMER_OFFSET answer for a never-committed "
                "group is timing-dependent\n",
                counts.c_str());

    // ---------------------------------------------------------------- L2
    std::printf("\nL2 LitePull pin (connectBrokerByUser / defaultBrokerId)\n");
    check("seed " + std::to_string(kMsg) + " messages on " + t2, seedTopic(*producer, t2, bodies),
          "topic=" + t2);

    const LiteObservation off =
        liteLeg(ns, t2, brokerName, prefix + "_pinOff", false, 0, kMsg, 20000);
    check("L2-pin off: the request carries no broker id (route resolution as before)",
          !off.requestBrokerId.has_value(),
          "requestBrokerId=" + idText(off.requestBrokerId));
    check("L2-pin off: delivers the backlog", off.got == kMsg,
          "got=" + std::to_string(off.got) + " expect=" + std::to_string(kMsg) +
              " assigned=" + join(off.assigned));

    const LiteObservation master =
        liteLeg(ns, t2, brokerName, prefix + "_pinMaster", true, 0, kMsg, 20000);
    check("L2-pin to the master (0): the request carries brokerId=0",
          master.requestBrokerId.has_value() && *master.requestBrokerId == 0,
          "requestBrokerId=" + idText(master.requestBrokerId));
    check("L2-pin to the master (0): delivers the backlog", master.got == kMsg,
          "got=" + std::to_string(master.got) + " expect=" + std::to_string(kMsg) +
              " assigned=" + join(master.assigned));

    const LiteObservation slave =
        liteLeg(ns, t2, brokerName, prefix + "_pinSlave", true, 1, kMsg, 12000);
    check("L2-pin to a non-existent slave (1): the request carries brokerId=1",
          slave.requestBrokerId.has_value() && *slave.requestBrokerId == 1,
          "requestBrokerId=" + idText(slave.requestBrokerId));
    check("L2-pin to a non-existent slave (1): FindBrokerAddressInSubscribe's "
          "onlyThisBroker=false fallback still delivers (Java parity, not a hole)",
          slave.got == kMsg,
          "got=" + std::to_string(slave.got) + " expect=" + std::to_string(kMsg) +
              " assigned=" + join(slave.assigned));

    // ---------------------------------------------------------------- L3
    std::printf("\nL3 adminExtGroup (identity only, no wire effect)\n");
    {
        DefaultMQAdminExt custom;
        custom.setNamesrvAddr(ns);
        check("L3-default adminExtGroup is 'admin_ext_group'",
              custom.getAdminExtGroup() == "admin_ext_group",
              "value=" + custom.getAdminExtGroup());
        const std::string group = "cap_closeout_admin_" + prefix;
        custom.setAdminExtGroup(group);
        check("L3-override takes", custom.getAdminExtGroup() == group,
              "value=" + custom.getAdminExtGroup());
        bool booted = false;
        bool found = false;
        int32_t topics = 0;
        try {
            custom.start();
            booted = custom.isStarted();
            TopicList list = custom.fetchAllTopicList();
            topics = static_cast<int32_t>(list.topicList.size());
            found = std::find(list.topicList.begin(), list.topicList.end(), t2) !=
                    list.topicList.end();
            custom.shutdown();
        } catch (const std::exception& e) {
            std::printf("  !! custom-group admin failed: %s\n", e.what());
            try {
                custom.shutdown();
            } catch (...) {
            }
        }
        check("L3-a custom-group admin still boots and serves an RPC on the live cluster",
              booted && found, "topics=" + std::to_string(topics));
    }

    try {
        producer->shutdown();
    } catch (const std::exception& e) {
        std::printf("    (producer shutdown failed: %s)\n", e.what());
    }
    try {
        admin.shutdown();
    } catch (const std::exception& e) {
        std::printf("    (admin shutdown failed: %s)\n", e.what());
    }

    std::printf("\n%s: PASS=%d FAIL=%d\n", prefix.c_str(), gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
