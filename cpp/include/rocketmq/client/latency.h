// 发送延迟故障容错（对应 org.apache.rocketmq.client.latency.* 与 Python client/latency.py）。
//
// 实现 MQFaultStrategy + LatencyFaultToleranceImpl（带 FaultItem）：追踪每个 broker 的
// 发送延迟，延迟过高或发生异常时**隔离**一段时间（不分配给新消息），默认关闭。
//
// 与 Java 关键点逐条对齐：
//   * latencyMax / notAvailableDuration 两套阈值表（逐字一致）；
//   * updateFaultItem 的 notAvailableDuration 取 computeNotAvailableDuration，
//     隔离（异常）场景固定按 10000ms 算档位；
//   * FaultItem.isAvailable() = now >= startTimestamp（隔离期未过则不可用）；
//   * LatencyFaultToleranceImpl.isAvailable/isReachable 在没有记录时返回 true，
//     即"从未出过问题的 broker 默认可用/可达"。
//
// 有意差异（与 Python 参考实现一致）：省略 Java 的"后台可达性探测线程"（startDetector），
// reachable 由 updateFaultItem 的调用方写入。默认 sendLatencyFaultEnable=false，开启后才记录与生效。
#ifndef ROCKETMQ_CLIENT_LATENCY_H
#define ROCKETMQ_CLIENT_LATENCY_H

#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "rocketmq/common/message.h"

namespace rocketmq {

class TopicPublishInfo;

// 单个 broker 的故障项（对应 Java LatencyFaultToleranceImpl.FaultItem）。
// 字段由 LatencyFaultToleranceImpl 的表锁保护，自身不再加锁。
class FaultItem {
public:
    explicit FaultItem(const std::string& name);

    // Java：only when now + dur > startTimestamp 才更新（保持最长隔离期）
    void updateNotAvailableDuration(int64_t notAvailableDurationMillis);

    bool isAvailable() const;
    bool isReachable() const { return reachableFlag_; }

    const std::string& name() const { return name_; }
    double currentLatency() const { return currentLatency_; }
    int64_t startTimestamp() const { return startTimestamp_; }

    void setCurrentLatency(double latency) { currentLatency_ = latency; }
    void setReachable(bool reachable) { reachableFlag_ = reachable; }

private:
    std::string name_;
    double currentLatency_ = 0.0;
    int64_t startTimestamp_ = 0;
    bool reachableFlag_ = true;
};

// 对应 Java client.latency.LatencyFaultToleranceImpl（纯内存版，无探测线程）。
class LatencyFaultToleranceImpl {
public:
    void updateFaultItem(const std::string& name, double currentLatency,
                         int64_t notAvailableDurationMillis, bool reachable);
    bool isAvailable(const std::string& name);
    bool isReachable(const std::string& name);
    void remove(const std::string& name);

    // 供测试/诊断读取（不存在返回 nullptr）
    const FaultItem* getFaultItem(const std::string& name);

private:
    std::mutex m_;
    std::unordered_map<std::string, FaultItem> table_;
};

// 对应 Java client.latency.MQFaultStrategy。
//
// 仅当 sendLatencyFaultEnable 为 true 时，发送选队列阶段会：
//   1) 优先选 available（隔离期已过）的 broker；
//   2) 否则选 reachable 的 broker；
//   3) 否则退化为普通轮询。
// 发送结果/异常会回调 updateFaultItem 写延迟与隔离信息。
class MQFaultStrategy {
public:
    // 两套阈值表（Java DefaultMQProducer 的默认值）
    static constexpr int32_t kLatencyMaxSize = 7;
    static const int32_t kLatencyMax[kLatencyMaxSize];
    static const int32_t kNotAvailableDuration[kLatencyMaxSize];

    explicit MQFaultStrategy(bool sendLatencyFaultEnable = false);

    void setSendLatencyFaultEnable(bool enable) { sendLatencyFaultEnable_ = enable; }
    bool isSendLatencyFaultEnable() const { return sendLatencyFaultEnable_; }

    // 队列选择（对应 Python select_one_message_queue / Java 同名方法）。
    // lastBrokerName 非空时尽量避开该 broker；enable=false 时退化为普通轮询。
    MessageQueue selectOneMessageQueue(TopicPublishInfo& tpInfo,
                                       const std::string& lastBrokerName,
                                       bool resetIndex = false);

    // 故障记录：isolation=true 时 latency 固定按 10000ms 算档位（→ 隔离 10000ms）。
    // 未开启时直接忽略（与 Java/Python 一致）。
    void updateFaultItem(const std::string& brokerName, double currentLatency,
                         bool isolation, bool reachable);

    int64_t computeNotAvailableDuration(double currentLatency) const;

    LatencyFaultToleranceImpl& latencyFaultTolerance() { return tolerance_; }

private:
    bool sendLatencyFaultEnable_;
    LatencyFaultToleranceImpl tolerance_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_LATENCY_H
