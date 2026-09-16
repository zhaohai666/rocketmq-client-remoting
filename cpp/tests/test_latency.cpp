// 发送延迟故障容错单测（对齐 python/tests 侧语义 + Java MQFaultStrategy 阈值表）。
//
// 覆盖：
// 1. 阈值表逐档映射（0→0、50→0、550→2000、1800→5000、5000→10000、15000→30000、10000→10000）；
// 2. 隔离（isolation=true）固定按 10000ms 算档位；
// 3. FaultItem.updateNotAvailableDuration 只延长不缩短；
// 4. 容错表无记录 isAvailable/isReachable=true；隔离后 false、到期恢复；
// 5. 策略关闭时不记录任何故障项；
// 6. 策略开启：健康 broker 正常选出；单 broker 注入隔离后退化为普通轮询仍能选出队列。
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
    // ------------------------------------------------ 阈值表逐档映射
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

    // ------------------------------------------------ 隔离档位 / 容错表
    {
        MQFaultStrategy s(true);
        s.updateFaultItem("broker-a", 12.0, /*isolation=*/true, /*reachable=*/false);
        const FaultItem* item = s.latencyFaultTolerance().getFaultItem("broker-a");
        expect(item != nullptr, "isolation records a fault item");
        expect(item->currentLatency() == 12.0, "currentLatency keeps measured value");
        // 隔离档位固定 10000ms -> 隔离 10000ms（isAvailable 立即为 false）
        expect(!s.latencyFaultTolerance().isAvailable("broker-a"), "isolated broker unavailable");
        expect(!s.latencyFaultTolerance().isReachable("broker-a"), "isolated broker unreachable");

        // updateNotAvailableDuration 只延长不缩短
        FaultItem f("x");
        f.updateNotAvailableDuration(5000);
        const int64_t ts1 = f.startTimestamp();
        f.updateNotAvailableDuration(1000);
        expect(f.startTimestamp() == ts1, "updateNotAvailableDuration only extends");

        // remove 后恢复默认可用
        s.latencyFaultTolerance().remove("broker-a");
        expect(s.latencyFaultTolerance().isAvailable("broker-a")
                   && s.latencyFaultTolerance().isReachable("broker-a"),
               "removed item defaults to available/reachable");

        // 无记录的 broker 默认可用/可达
        expect(s.latencyFaultTolerance().isAvailable("never-seen")
                   && s.latencyFaultTolerance().isReachable("never-seen"),
               "unknown broker defaults available");
    }

    // ------------------------------------------------ 到期恢复
    {
        LatencyFaultToleranceImpl tol;
        tol.updateFaultItem("b", 1.0, 50 /*ms*/, true);
        expect(!tol.isAvailable("b"), "short isolation unavailable immediately");
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        expect(tol.isAvailable("b"), "isolation expires");
    }

    // ------------------------------------------------ 策略关闭：不记录
    {
        MQFaultStrategy s(false);
        s.updateFaultItem("broker-a", 99999.0, true, false);
        expect(s.latencyFaultTolerance().getFaultItem("broker-a") == nullptr,
               "disabled strategy records nothing");
    }

    // ------------------------------------------------ 队列选择
    {
        // 双 broker：隔离 broker-a 后开启策略应只选 broker-b
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
        // 单 broker 全隔离：available/reachable 都选不出 -> 退化普通轮询仍能选出
        MQFaultStrategy s(true);
        s.updateFaultItem("broker-a", 99999.0, true, false);
        TopicPublishInfo tp;
        fillTpInfo(tp, "T", "broker-a", "");
        MessageQueue mq = s.selectOneMessageQueue(tp, "");
        expect(mq.brokerName == "broker-a", "single-broker fallback to plain polling");
    }
    {
        // lastBrokerName 避开语义：双 broker 下不返回上次的 broker
        MQFaultStrategy s(false);
        TopicPublishInfo tp;
        fillTpInfo(tp, "T", "broker-a", "broker-b");
        MessageQueue mq = s.selectOneMessageQueue(tp, "broker-a");
        expect(mq.brokerName == "broker-b", "lastBrokerName avoided");
    }
    {
        // resetIndex 后从头轮询
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
