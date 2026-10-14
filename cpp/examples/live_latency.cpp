// Live-cluster verification of send-latency fault tolerance (sendLatencyFaultEnable)
// (asserting scenarios S1-S5 one by one).
// Usage: rmq_live_latency 127.0.0.1:9876
//
// Scenarios (single broker broker-a / DefaultCluster):
//   S1 default off: send 10 messages all OK, the fault-tolerance table records nothing
//   S2 fault avoidance on: send 20 messages all OK (the broker is healthy, no isolation triggered)
//   S3 after a successful send the fault-tolerance table holds a record: currentLatency > 0 and available/reachable
//   S4 manually inject isolation -> the single broker still sends everything via the available->reachable->normal-round-robin degradation chain
//   S5 isolation expiry recovery (inject 2000ms, about 2 s later it is available again and sending is normal)
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

#include "rocketmq/client/latency.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"

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

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_latency <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    const int64_t stamp = nowMs() % 100000000;
    const std::string topic = "LatencyCpp_" + std::to_string(stamp);
    const std::string broker = "broker-a";

    std::printf("== LatencyFaultTolerance live verification  namesrv=%s  topic=%s ==\n", nsAddr.c_str(),
                topic.c_str());

    DefaultMQProducer prep{"PG_LatPrep_" + std::to_string(stamp)};
    prep.setNamesrvAddr(nsAddr);
    prep.start();
    try {
        prep.createTopic("TBW102", topic, 4);
    } catch (const std::exception& e) {
        std::printf("  pre-creating the topic failed (falling back to auto-creation): %s\n", e.what());
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
    prep.shutdown();

    // ---------------- S1 default off ----------------
    std::printf("\nS1 default off: behaviour unchanged, the fault-tolerance table records nothing\n");
    DefaultMQProducer p1{"PG_LatOff_" + std::to_string(stamp)};
    p1.setNamesrvAddr(nsAddr);
    check("S1 sendLatencyFaultEnable defaults to false", !p1.isSendLatencyFaultEnable());
    p1.start();
    // the topic points at this scenario (sendOk composes the topic from the key; here we send on the real topic)
    int32_t ok = 0;
    for (int32_t i = 0; i < 10; ++i) {
        try {
            p1.send(Message(topic, "off-" + std::to_string(i)));
            ++ok;
        } catch (const std::exception& e) {
            std::printf("    send %d failed: %s\n", i, e.what());
        }
    }
    check("S1 with the switch off all 10 sends succeed", ok == 10, "ok=" + std::to_string(ok));
    check("S1 with the switch off nothing is recorded in the fault-tolerance table",
          p1.mqFaultStrategy().latencyFaultTolerance().getFaultItem(broker) == nullptr);
    p1.shutdown();

    // ---------------- S2/S3 healthy path after enabling ----------------
    std::printf("\nS2/S3 fault avoidance on: a healthy broker sends normally and its latency is recorded\n");
    DefaultMQProducer p2{"PG_LatOn_" + std::to_string(stamp)};
    p2.setNamesrvAddr(nsAddr);
    p2.setSendLatencyFaultEnable(true);
    check("S2 the switch takes effect", p2.isSendLatencyFaultEnable());
    p2.start();
    ok = 0;
    for (int32_t i = 0; i < 20; ++i) {
        try {
            p2.send(Message(topic, "on-" + std::to_string(i)));
            ++ok;
        } catch (const std::exception& e) {
            std::printf("    send %d failed: %s\n", i, e.what());
        }
    }
    check("S2 with the switch on all 20 sends succeed", ok == 20, "ok=" + std::to_string(ok));
    const FaultItem* item = p2.mqFaultStrategy().latencyFaultTolerance().getFaultItem(broker);
    check("S3 the fault-tolerance table has recorded broker-a", item != nullptr);
    if (item != nullptr) {
        check("S3 the recorded measured latency > 0", item->currentLatency() > 0.0,
              "latency=" + std::to_string(item->currentLatency()) + "ms");
        check("S3 the healthy broker is still available", item->isAvailable() && item->isReachable());
        check("S3 the latency is below the first-tier threshold (no isolation triggered)", item->startTimestamp() == 0);
    }

    // ---------------- S4 inject isolation -> degradation chain ----------------
    std::printf("\nS4 injected isolation: the available->reachable->normal-round-robin degradation chain still sends\n");
    p2.mqFaultStrategy().updateFaultItem(broker, 99999.0, true, false);
    check("S4 after injection broker-a is unavailable", !p2.mqFaultStrategy().latencyFaultTolerance().isAvailable(broker));
    ok = 0;
    for (int32_t i = 0; i < 5; ++i) {
        try {
            p2.send(Message(topic, "iso-" + std::to_string(i)));
            ++ok;
        } catch (const std::exception& e) {
            std::printf("    send %d failed: %s\n", i, e.what());
        }
    }
    check("S4 while isolated the single-broker degraded round-robin still sends 5 messages", ok == 5, "ok=" + std::to_string(ok));

    // ---------------- S5 isolation expiry recovery ----------------
    std::printf("\nS5 isolation expiry recovery (after remove, inject 2000ms)\n");
    p2.mqFaultStrategy().latencyFaultTolerance().remove(broker);
    p2.mqFaultStrategy().latencyFaultTolerance().updateFaultItem(broker, 1.0, 2000, true);
    check("S5 unavailable during the isolation period", !p2.mqFaultStrategy().latencyFaultTolerance().isAvailable(broker));
    std::this_thread::sleep_for(std::chrono::milliseconds(2300));
    check("S5 available again after expiry", p2.mqFaultStrategy().latencyFaultTolerance().isAvailable(broker));
    ok = 0;
    for (int32_t i = 0; i < 3; ++i) {
        try {
            p2.send(Message(topic, "after-" + std::to_string(i)));
            ++ok;
        } catch (const std::exception& e) {
            std::printf("    send %d failed: %s\n", i, e.what());
        }
    }
    check("S5 sending is normal after recovery", ok == 3, "ok=" + std::to_string(ok));
    p2.shutdown();

    std::printf("\n== result: PASS=%d FAIL=%d ==\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
