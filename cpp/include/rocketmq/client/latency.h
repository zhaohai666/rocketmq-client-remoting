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
// 主动探测（Java 5.x ClientConfig 的 startDetectorEnable/detectTimeout/detectInterval +
// DefaultMQProducerImpl 构造函数里的匿名 ServiceDetector）：
//   * LatencyFaultToleranceImpl 持 Resolver（brokerName → 主发布地址）与 ServiceDetector
//     两个函数对象；detectByOneRound() 对故障表里 checkStamp 到期的每一项：先占坑
//     （checkStamp = now + detectInterval），resolver 解析不到地址就摘除该故障项，然后
//     发一次探测 —— 成功且当前不可达 → 翻回 reachable（Java "is reachable now, then it
//     can be used."）；失败**不动**标志（置不可达是发送路径的职责，与 Java 逐行一致）。
//   * 探测线程归生产者所有（cpp 没有 Java 的共享 scheduled executor，与 Go 端
//     fault_detector.go 同一口径）：DefaultMQProducer 的 start()/shutdown() 启停一根
//     线程，固定 3s 一跳（Java scheduleAtFixedRate(3, 3, SECONDS)），每跳先看
//     startDetectorEnable（运行期可开关）。默认关闭（与 Java ClientConfig 一致），
//     detectTimeout 默认 200ms、detectInterval 默认 2000ms（同一个类的默认值）。
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
    // 主动探测的下一次到期时刻（Java FaultItem.checkStamp；0 = 立即到期）
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

// 对应 Java client.latency.LatencyFaultToleranceImpl（含探测半边：Resolver +
// ServiceDetector 由 DefaultMQProducer 在 start() 时注入 —— Java 在 MQFaultStrategy
// 构造函数里注入，cpp 的客户端实例也是 start() 时才建，注入时机随之顺延）。
class LatencyFaultToleranceImpl {
public:
    // Java client.latency.Resolver：brokerName → 主发布地址；空串 = 没有这台 broker
    // （detectByOneRound 会摘掉该故障项，与 Java resolver 返回 null 同义）。
    using Resolver = std::function<std::string(const std::string& brokerName)>;
    // Java client.latency.ServiceDetector：对 endpoint 发一次探测，true = 服务已恢复。
    using ServiceDetector = std::function<bool(const std::string& endpoint, int32_t timeoutMillis)>;

    LatencyFaultToleranceImpl() = default;

    void updateFaultItem(const std::string& name, double currentLatency,
                         int64_t notAvailableDurationMillis, bool reachable);
    bool isAvailable(const std::string& name);
    bool isReachable(const std::string& name);
    void remove(const std::string& name);

    // 供测试/诊断读取（不存在返回 nullptr）
    const FaultItem* getFaultItem(const std::string& name);

    // ---------------- 主动探测（Java LatencyFaultToleranceImpl 的探测半边）----------------
    // 两个函数对象必须在探测线程跑起来之前装好（生产者 start() 里做）。
    void setResolver(Resolver resolver) { resolver_ = std::move(resolver); }
    void setServiceDetector(ServiceDetector detector) { serviceDetector_ = std::move(detector); }

    // Java ClientConfig 默认值：单次探测 200ms、每个 broker 的探测周期 2000ms
    void setDetectTimeout(int32_t millis) { detectTimeout_ = millis; }
    int32_t detectTimeout() const { return detectTimeout_; }
    void setDetectInterval(int32_t millis) { detectInterval_ = millis; }
    int32_t detectInterval() const { return detectInterval_; }

    // Java 的开关在容忍表上是 volatile、探测线程的 runnable 每跳都判（运行期可改）
    void setStartDetectorEnable(bool enable) { startDetectorEnable_ = enable; }
    bool isStartDetectorEnable() const { return startDetectorEnable_; }

    // Java detectByOneRound：一跳探测。对故障表里 checkStamp 已到期的每一项：先占坑
    // （checkStamp = now + detectInterval）→ resolver 解析地址（空则摘除该故障项）→
    // 探测成功且当前不可达时翻回 reachable。失败不动标志。线程安全（表锁只包内存操作，
    // 网络 IO 在锁外 —— 一跳最坏阻塞 detectTimeout 毫秒）。
    void detectByOneRound();

private:
    std::mutex m_;
    std::unordered_map<std::string, FaultItem> table_;
    // ---------------- 主动探测配置（Java LatencyFaultToleranceImpl 的同名字段）--------
    int32_t detectTimeout_ = 200;
    int32_t detectInterval_ = 2000;
    bool startDetectorEnable_ = false;
    Resolver resolver_;
    ServiceDetector serviceDetector_;
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

    // 对应 Java MQFaultStrategy.setStartDetectorEnable：开关存在容忍表上，策略只转发
    // （Java 同：`:121-128` 把 volatile 一路写到 latencyFaultTolerance）。
    void setStartDetectorEnable(bool enable) { tolerance_.setStartDetectorEnable(enable); }
    bool isStartDetectorEnable() const { return tolerance_.isStartDetectorEnable(); }

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
    const LatencyFaultToleranceImpl& latencyFaultTolerance() const { return tolerance_; }

private:
    bool sendLatencyFaultEnable_;
    LatencyFaultToleranceImpl tolerance_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_LATENCY_H
