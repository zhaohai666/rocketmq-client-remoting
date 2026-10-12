// 实现 MQFaultStrategy + LatencyFaultToleranceImpl（带 FaultItem）：追踪每个 broker 的
// 发送延迟，延迟过高或发生异常时**隔离**一段时间（不分配给新消息），默认关闭。
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

class FaultItem {
public:
    explicit FaultItem(const std::string& name);

    void updateNotAvailableDuration(int64_t notAvailableDurationMillis);

    bool isAvailable() const;
    bool isReachable() const { return reachableFlag_; }

    const std::string& name() const { return name_; }
    double currentLatency() const { return currentLatency_; }
    int64_t startTimestamp() const { return startTimestamp_; }
    int64_t checkStamp() const { return checkStamp_; }
    void setCheckStamp(int64_t stamp) { checkStamp_ = stamp; }

    void setCurrentLatency(double latency) { currentLatency_ = latency; }
    void setReachable(bool reachable) { reachableFlag_ = reachable; }

private:
    std::string name_;
    double currentLatency_ = 0.0;
    int64_t startTimestamp_ = 0;
    int64_t checkStamp_ = 0;
    bool reachableFlag_ = true;
};

class LatencyFaultToleranceImpl {
public:
    using Resolver = std::function<std::string(const std::string& brokerName)>;
    using ServiceDetector = std::function<bool(const std::string& endpoint, int32_t timeoutMillis)>;

    LatencyFaultToleranceImpl() = default;

    void updateFaultItem(const std::string& name, double currentLatency,
                         int64_t notAvailableDurationMillis, bool reachable);
    bool isAvailable(const std::string& name);
    bool isReachable(const std::string& name);
    void remove(const std::string& name);

    // 供测试/诊断读取（不存在返回 nullptr）
    const FaultItem* getFaultItem(const std::string& name);

    void setResolver(Resolver resolver) { resolver_ = std::move(resolver); }
    void setServiceDetector(ServiceDetector detector) { serviceDetector_ = std::move(detector); }

    void setDetectTimeout(int32_t millis) { detectTimeout_ = millis; }
    int32_t detectTimeout() const { return detectTimeout_; }
    void setDetectInterval(int32_t millis) { detectInterval_ = millis; }
    int32_t detectInterval() const { return detectInterval_; }

    void setStartDetectorEnable(bool enable) { startDetectorEnable_ = enable; }
    bool isStartDetectorEnable() const { return startDetectorEnable_; }

    void detectByOneRound();

private:
    std::mutex m_;
    std::unordered_map<std::string, FaultItem> table_;
    int32_t detectTimeout_ = 200;
    int32_t detectInterval_ = 2000;
    bool startDetectorEnable_ = false;
    Resolver resolver_;
    ServiceDetector serviceDetector_;
};

// 仅当 sendLatencyFaultEnable 为 true 时，发送选队列阶段会：
//   1) 优先选 available（隔离期已过）的 broker；
//   2) 否则选 reachable 的 broker；
//   3) 否则退化为普通轮询。
// 发送结果/异常会回调 updateFaultItem 写延迟与隔离信息。
class MQFaultStrategy {
public:
    static constexpr int32_t kLatencyMaxSize = 7;
    static const int32_t kLatencyMax[kLatencyMaxSize];
    static const int32_t kNotAvailableDuration[kLatencyMaxSize];

    explicit MQFaultStrategy(bool sendLatencyFaultEnable = false);

    void setSendLatencyFaultEnable(bool enable) { sendLatencyFaultEnable_ = enable; }
    bool isSendLatencyFaultEnable() const { return sendLatencyFaultEnable_; }

    void setStartDetectorEnable(bool enable) { tolerance_.setStartDetectorEnable(enable); }
    bool isStartDetectorEnable() const { return tolerance_.isStartDetectorEnable(); }

    MessageQueue selectOneMessageQueue(TopicPublishInfo& tpInfo,
                                       const std::string& lastBrokerName,
                                       bool resetIndex = false);

    void updateFaultItem(const std::string& brokerName, double currentLatency,
                         bool isolation, bool reachable);

    int64_t computeNotAvailableDuration(double currentLatency) const;

    LatencyFaultToleranceImpl& latencyFaultTolerance() { return tolerance_; }
    const LatencyFaultToleranceImpl& latencyFaultTolerance() const { return tolerance_; }

private:
    bool sendLatencyFaultEnable_;
    LatencyFaultToleranceImpl tolerance_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_LATENCY_H
