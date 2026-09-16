#include "rocketmq/client/latency.h"

#include "rocketmq/client/mq_client.h"
#include "rocketmq/common/util_all.h"

namespace rocketmq {

const int32_t MQFaultStrategy::kLatencyMax[kLatencyMaxSize] = {50, 100, 550, 1800, 3000, 5000, 15000};
const int32_t MQFaultStrategy::kNotAvailableDuration[kLatencyMaxSize] = {0, 0, 2000, 5000, 6000,
                                                                         10000, 30000};

namespace {
int64_t nowMillis() { return UtilAll::currentTimeMillis(); }
}  // namespace

// ---------------------------------------------------------------- FaultItem

FaultItem::FaultItem(const std::string& name) : name_(name) {}

void FaultItem::updateNotAvailableDuration(int64_t notAvailableDurationMillis) {
    // Java：only when now + dur > startTimestamp 才更新（保持最长隔离期）
    if (notAvailableDurationMillis > 0
        && nowMillis() + notAvailableDurationMillis > startTimestamp_) {
        startTimestamp_ = nowMillis() + notAvailableDurationMillis;
    }
}

bool FaultItem::isAvailable() const { return nowMillis() >= startTimestamp_; }

// ---------------------------------------------------------------- LatencyFaultToleranceImpl

void LatencyFaultToleranceImpl::updateFaultItem(const std::string& name, double currentLatency,
                                                int64_t notAvailableDurationMillis, bool reachable) {
    std::lock_guard<std::mutex> lk(m_);
    auto it = table_.find(name);
    if (it == table_.end()) {
        FaultItem item(name);
        item.setCurrentLatency(currentLatency);
        item.updateNotAvailableDuration(notAvailableDurationMillis);
        item.setReachable(reachable);
        table_.emplace(name, std::move(item));
        return;
    }
    it->second.setCurrentLatency(currentLatency);
    it->second.updateNotAvailableDuration(notAvailableDurationMillis);
    it->second.setReachable(reachable);
}

bool LatencyFaultToleranceImpl::isAvailable(const std::string& name) {
    std::lock_guard<std::mutex> lk(m_);
    auto it = table_.find(name);
    // 没有记录 = 从未出过问题，默认可用（与 Java/Python 一致）
    return it == table_.end() ? true : it->second.isAvailable();
}

bool LatencyFaultToleranceImpl::isReachable(const std::string& name) {
    std::lock_guard<std::mutex> lk(m_);
    auto it = table_.find(name);
    return it == table_.end() ? true : it->second.isReachable();
}

void LatencyFaultToleranceImpl::remove(const std::string& name) {
    std::lock_guard<std::mutex> lk(m_);
    table_.erase(name);
}

const FaultItem* LatencyFaultToleranceImpl::getFaultItem(const std::string& name) {
    std::lock_guard<std::mutex> lk(m_);
    auto it = table_.find(name);
    return it == table_.end() ? nullptr : &it->second;
}

// ---------------------------------------------------------------- MQFaultStrategy

MQFaultStrategy::MQFaultStrategy(bool sendLatencyFaultEnable)
    : sendLatencyFaultEnable_(sendLatencyFaultEnable) {}

MessageQueue MQFaultStrategy::selectOneMessageQueue(TopicPublishInfo& tpInfo,
                                                    const std::string& lastBrokerName,
                                                    bool resetIndex) {
    // broker 避开过滤器：lastBrokerName 非空时一轮内跳过它
    std::function<bool(const MessageQueue&)> brokerFilter;
    if (lastBrokerName.empty()) {
        brokerFilter = [](const MessageQueue&) { return true; };
    } else {
        std::string last = lastBrokerName;
        brokerFilter = [last](const MessageQueue& mq) { return mq.brokerName != last; };
    }

    if (sendLatencyFaultEnable_) {
        if (resetIndex) {
            tpInfo.resetIndex();
        }
        // 1) available：隔离期已过的 broker
        std::function<bool(const MessageQueue&)> availableFilter =
            [this](const MessageQueue& mq) { return tolerance_.isAvailable(mq.brokerName); };
        if (auto mq = tpInfo.selectOneMessageQueue(availableFilter, brokerFilter)) {
            return *mq;
        }
        // 2) reachable：隔离中但通道仍可达的 broker
        std::function<bool(const MessageQueue&)> reachableFilter =
            [this](const MessageQueue& mq) { return tolerance_.isReachable(mq.brokerName); };
        if (auto mq = tpInfo.selectOneMessageQueue(reachableFilter, brokerFilter)) {
            return *mq;
        }
        // 3) 全部被隔离且不可达：退化为普通轮询
        return tpInfo.selectOneMessageQueue();
    }

    // 关闭时退化为普通轮询（lastBrokerName 非空时避开它；全部同名时该重载内部已兜底）
    return tpInfo.selectOneMessageQueue(lastBrokerName);
}

void MQFaultStrategy::updateFaultItem(const std::string& brokerName, double currentLatency,
                                      bool isolation, bool reachable) {
    if (!sendLatencyFaultEnable_) {
        return;
    }
    const double latency = isolation ? 10000.0 : currentLatency;
    const int64_t duration = computeNotAvailableDuration(latency);
    tolerance_.updateFaultItem(brokerName, currentLatency, duration, reachable);
}

int64_t MQFaultStrategy::computeNotAvailableDuration(double currentLatency) const {
    // 从表尾向首找第一个 latency >= LATENCY_MAX[i] 的档位
    for (int i = kLatencyMaxSize - 1; i >= 0; --i) {
        if (currentLatency >= static_cast<double>(kLatencyMax[i])) {
            return kNotAvailableDuration[i];
        }
    }
    return 0;
}

}  // namespace rocketmq
