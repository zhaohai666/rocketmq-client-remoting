// Covers:
// 1. tier-by-tier mapping of the latency threshold table (0->0, 50->0, 550->2000, 1800->5000, 5000->10000, 15000->30000, 10000->10000);
// 2. isolation (isolation=true) is always computed on the fixed 10000ms tier;
// 3. FaultItem.updateNotAvailableDuration only extends, never shortens;
// 4. with no record in the fault table isAvailable/isReachable=true; false after isolation, recovered on expiry;
// 5. nothing is recorded while the strategy is disabled;
// 6. strategy enabled: a healthy broker is picked normally; after single-broker isolation it degrades to plain polling and still picks a queue.
#include <chrono>
#include <cstdio>
#include <thread>

#include "rocketmq/client/latency.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/common/util_all.h"

using namespace rocketmq;

namespace {
int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name) {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("  [FAIL] %s\n", name.c_str());
    }
}

void fillTpInfo(TopicPublishInfo& tp, const std::string& topic, const std::string& brokerA,
                const std::string& brokerB) {
    tp.msgQueueList.push_back(MessageQueue(topic, brokerA, 0));
    if (!brokerB.empty()) {
        tp.msgQueueList.push_back(MessageQueue(topic, brokerB, 0));
        tp.msgQueueList.push_back(MessageQueue(topic, brokerB, 1));
    }
}
}  // namespace

int main() {
    // ------------------------------------------------ threshold table tier mapping
    {
        MQFaultStrategy s(true);
        expect(s.computeNotAvailableDuration(0) == 0, "lat 0 -> dur 0");
        expect(s.computeNotAvailableDuration(49.9) == 0, "lat 49.9 -> dur 0");
        expect(s.computeNotAvailableDuration(50) == 0, "lat 50 -> dur 0");
        expect(s.computeNotAvailableDuration(549.9) == 0, "lat 549.9 -> dur 0");
        expect(s.computeNotAvailableDuration(550) == 2000, "lat 550 -> dur 2000");
        expect(s.computeNotAvailableDuration(1799) == 2000, "lat 1799 -> dur 2000");
        expect(s.computeNotAvailableDuration(1800) == 5000, "lat 1800 -> dur 5000");
        expect(s.computeNotAvailableDuration(3000) == 6000, "lat 3000 -> dur 6000");
        expect(s.computeNotAvailableDuration(5000) == 10000, "lat 5000 -> dur 10000");
        expect(s.computeNotAvailableDuration(10000) == 10000, "lat 10000 -> dur 10000");
        expect(s.computeNotAvailableDuration(15000) == 30000, "lat 15000 -> dur 30000");
        expect(s.computeNotAvailableDuration(60000) == 30000, "lat 60000 -> dur 30000");
    }

    // ------------------------------------------------ isolation tier / fault-tolerance table
    {
        MQFaultStrategy s(true);
        s.updateFaultItem("broker-a", 12.0, /*isolation=*/true, /*reachable=*/false);
        const FaultItem* item = s.latencyFaultTolerance().getFaultItem("broker-a");
        expect(item != nullptr, "isolation records a fault item");
        expect(item->currentLatency() == 12.0, "currentLatency keeps measured value");
        // the isolation tier is fixed at 10000ms -> isolated for 10000ms (isAvailable is false immediately)
        expect(!s.latencyFaultTolerance().isAvailable("broker-a"), "isolated broker unavailable");
        expect(!s.latencyFaultTolerance().isReachable("broker-a"), "isolated broker unreachable");

        // updateNotAvailableDuration only extends, never shortens
        FaultItem f("x");
        f.updateNotAvailableDuration(5000);
        const int64_t ts1 = f.startTimestamp();
        f.updateNotAvailableDuration(1000);
        expect(f.startTimestamp() == ts1, "updateNotAvailableDuration only extends");

        // after remove the item is available again by default
        s.latencyFaultTolerance().remove("broker-a");
        expect(s.latencyFaultTolerance().isAvailable("broker-a")
                   && s.latencyFaultTolerance().isReachable("broker-a"),
               "removed item defaults to available/reachable");

        // a broker with no record defaults to available/reachable
        expect(s.latencyFaultTolerance().isAvailable("never-seen")
                   && s.latencyFaultTolerance().isReachable("never-seen"),
               "unknown broker defaults available");
    }

    // ------------------------------------------------ expiry recovery
    {
        LatencyFaultToleranceImpl tol;
        tol.updateFaultItem("b", 1.0, 50 /*ms*/, true);
        expect(!tol.isAvailable("b"), "short isolation unavailable immediately");
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        expect(tol.isAvailable("b"), "isolation expires");
    }

    // ------------------------------------------------ strategy off: nothing recorded
    {
        MQFaultStrategy s(false);
        s.updateFaultItem("broker-a", 99999.0, true, false);
        expect(s.latencyFaultTolerance().getFaultItem("broker-a") == nullptr,
               "disabled strategy records nothing");
    }

    // ------------------------------------------------ queue selection
    {
        // two brokers: after isolating broker-a, the enabled strategy should only pick broker-b
        MQFaultStrategy s(true);
        s.updateFaultItem("broker-a", 99999.0, true, false);
        TopicPublishInfo tp;
        fillTpInfo(tp, "T", "broker-a", "broker-b");
        for (int i = 0; i < 6; ++i) {
            MessageQueue mq = s.selectOneMessageQueue(tp, "");
            expect(mq.brokerName == "broker-b", "isolated broker-a avoided (round " +
                                                     std::to_string(i) + ")");
        }
    }
    {
        // single broker fully isolated: nothing passes available/reachable -> the plain-polling fallback still picks it
        MQFaultStrategy s(true);
        s.updateFaultItem("broker-a", 99999.0, true, false);
        TopicPublishInfo tp;
        fillTpInfo(tp, "T", "broker-a", "");
        MessageQueue mq = s.selectOneMessageQueue(tp, "");
        expect(mq.brokerName == "broker-a", "single-broker fallback to plain polling");
    }
    {
        // lastBrokerName avoidance: with two brokers the previous broker is not returned
        MQFaultStrategy s(false);
        TopicPublishInfo tp;
        fillTpInfo(tp, "T", "broker-a", "broker-b");
        MessageQueue mq = s.selectOneMessageQueue(tp, "broker-a");
        expect(mq.brokerName == "broker-b", "lastBrokerName avoided");
    }
    {
        // after resetIndex, polling restarts from the head
        MQFaultStrategy s(true);
        TopicPublishInfo tp;
        fillTpInfo(tp, "T", "broker-a", "broker-b");
        (void)s.selectOneMessageQueue(tp, "", true);
        (void)s.selectOneMessageQueue(tp, "", true);
        tp.resetIndex();
        MessageQueue mq = s.selectOneMessageQueue(tp, "", true);
        expect(mq.queueId == 0 && mq.brokerName == "broker-a", "resetIndex restarts polling");
    }

    std::printf("latency: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
