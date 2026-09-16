// 发送延迟故障容错（sendLatencyFaultEnable）真机验证
// （与 python/verify_latency_live.py 的 S1–S5 同套场景）。
// 用法：rmq_live_latency 127.0.0.1:9876
//
// 场景（单 broker broker-a / DefaultCluster）：
//   S1 默认关闭：发 10 条全 OK，容错表不记录
//   S2 开启故障规避：发 20 条全 OK（broker 健康，不触发隔离）
//   S3 成功发送后容错表有记录：currentLatency > 0 且可用/可达
//   S4 手工注入隔离 → 单 broker 走 available→reachable→普通轮询 退化链仍全部发出
//   S5 隔离到期恢复（注入 2000ms，≈2s 后恢复可用、发送正常）
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

    std::printf("== LatencyFaultTolerance 真机验证  namesrv=%s  topic=%s ==\n", nsAddr.c_str(),
                topic.c_str());

    DefaultMQProducer prep{"PG_LatPrep_" + std::to_string(stamp)};
    prep.setNamesrvAddr(nsAddr);
    prep.start();
    try {
        prep.createTopic("TBW102", topic, 4);
    } catch (const std::exception& e) {
        std::printf("  预建 topic 失败（改用自动创建）: %s\n", e.what());
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
    prep.shutdown();

    // ---------------- S1 默认关闭 ----------------
    std::printf("\nS1 默认关闭：行为不变，不记录容错表\n");
    DefaultMQProducer p1{"PG_LatOff_" + std::to_string(stamp)};
    p1.setNamesrvAddr(nsAddr);
    check("S1 默认 sendLatencyFaultEnable=false", !p1.isSendLatencyFaultEnable());
    p1.start();
    // topic 指到本场景（sendOk 里用 key 拼 topic，这里换成真实 topic 发送）
    int32_t ok = 0;
    for (int32_t i = 0; i < 10; ++i) {
        try {
            p1.send(Message(topic, "off-" + std::to_string(i)));
            ++ok;
        } catch (const std::exception& e) {
            std::printf("    send %d failed: %s\n", i, e.what());
        }
    }
    check("S1 关闭时发 10 条全部成功", ok == 10, "ok=" + std::to_string(ok));
    check("S1 关闭时不记录容错表",
          p1.mqFaultStrategy().latencyFaultTolerance().getFaultItem(broker) == nullptr);
    p1.shutdown();

    // ---------------- S2/S3 开启后健康路径 ----------------
    std::printf("\nS2/S3 开启故障规避：健康 broker 正常发送并记录延迟\n");
    DefaultMQProducer p2{"PG_LatOn_" + std::to_string(stamp)};
    p2.setNamesrvAddr(nsAddr);
    p2.setSendLatencyFaultEnable(true);
    check("S2 开关生效", p2.isSendLatencyFaultEnable());
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
    check("S2 开启后发 20 条全部成功", ok == 20, "ok=" + std::to_string(ok));
    const FaultItem* item = p2.mqFaultStrategy().latencyFaultTolerance().getFaultItem(broker);
    check("S3 容错表已记录 broker-a", item != nullptr);
    if (item != nullptr) {
        check("S3 记录的实测延迟 > 0", item->currentLatency() > 0.0,
              "latency=" + std::to_string(item->currentLatency()) + "ms");
        check("S3 健康 broker 仍可用", item->isAvailable() && item->isReachable());
        check("S3 延迟低于第一档阈值（未触发隔离）", item->startTimestamp() == 0);
    }

    // ---------------- S4 注入隔离 → 退化链 ----------------
    std::printf("\nS4 注入隔离：available→reachable→普通轮询 退化链仍能发出\n");
    p2.mqFaultStrategy().updateFaultItem(broker, 99999.0, true, false);
    check("S4 注入后 broker-a 不可用", !p2.mqFaultStrategy().latencyFaultTolerance().isAvailable(broker));
    ok = 0;
    for (int32_t i = 0; i < 5; ++i) {
        try {
            p2.send(Message(topic, "iso-" + std::to_string(i)));
            ++ok;
        } catch (const std::exception& e) {
            std::printf("    send %d failed: %s\n", i, e.what());
        }
    }
    check("S4 隔离中单 broker 退化轮询仍发出 5 条", ok == 5, "ok=" + std::to_string(ok));

    // ---------------- S5 隔离到期恢复 ----------------
    std::printf("\nS5 隔离到期恢复（remove 后注入 2000ms）\n");
    // 注意：S4 注入的是隔离档位 10000ms，而 updateNotAvailableDuration **只延长不缩短**
    // （Java 语义），必须先 remove 才能注入更短的 2000ms 档。
    p2.mqFaultStrategy().latencyFaultTolerance().remove(broker);
    p2.mqFaultStrategy().latencyFaultTolerance().updateFaultItem(broker, 1.0, 2000, true);
    check("S5 隔离期内不可用", !p2.mqFaultStrategy().latencyFaultTolerance().isAvailable(broker));
    std::this_thread::sleep_for(std::chrono::milliseconds(2300));
    check("S5 到期后恢复可用", p2.mqFaultStrategy().latencyFaultTolerance().isAvailable(broker));
    ok = 0;
    for (int32_t i = 0; i < 3; ++i) {
        try {
            p2.send(Message(topic, "after-" + std::to_string(i)));
            ++ok;
        } catch (const std::exception& e) {
            std::printf("    send %d failed: %s\n", i, e.what());
        }
    }
    check("S5 恢复后发送正常", ok == 3, "ok=" + std::to_string(ok));
    p2.shutdown();

    std::printf("\n== 结果: PASS=%d FAIL=%d ==\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
