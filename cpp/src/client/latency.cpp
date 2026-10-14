#include "rocketmq/client/latency.h"

#include "rocketmq/client/mq_client.h"
#include "rocketmq/common/logging.h"
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
    // Update only when now + dur > startTimestamp (keep the longest isolation window)
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
    // No record = never any problem, available by default
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

// ---------------------------------------------------------------- Active detection

void LatencyFaultToleranceImpl::detectByOneRound() {
    std::vector<std::string> due;
    const int64_t now = nowMillis();
    {
        std::lock_guard<std::mutex> lk(m_);
        due.reserve(table_.size());
        for (auto& kv : table_) {
            if (now - kv.second.checkStamp() >= 0) {
                kv.second.setCheckStamp(now + detectInterval_);
                due.push_back(kv.first);
            }
        }
    }
    for (const std::string& name : due) {
        if (!resolver_) {
            return;
        }
        const std::string addr = resolver_(name);
        if (addr.empty()) {
            remove(name);
            continue;
        }
        if (!serviceDetector_) {
            continue;
        }
        if (serviceDetector_(addr, detectTimeout_)) {
            std::lock_guard<std::mutex> lk(m_);
            auto it = table_.find(name);
            if (it != table_.end() && !it->second.isReachable()) {
                logger_info(name + " is reachable now, then it can be used.");
                it->second.setReachable(true);
            }
        }
    }
}

// ---------------------------------------------------------------- MQFaultStrategy

MQFaultStrategy::MQFaultStrategy(bool sendLatencyFaultEnable)
    : sendLatencyFaultEnable_(sendLatencyFaultEnable) {}

MessageQueue MQFaultStrategy::selectOneMessageQueue(TopicPublishInfo& tpInfo,
                                                    const std::string& lastBrokerName,
                                                    bool resetIndex) {
    // Broker avoidance filter: skip lastBrokerName within one round when it is not empty
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
        // 1) available: brokers whose isolation window has passed
        std::function<bool(const MessageQueue&)> availableFilter =
            [this](const MessageQueue& mq) { return tolerance_.isAvailable(mq.brokerName); };
        if (auto mq = tpInfo.selectOneMessageQueue(availableFilter, brokerFilter)) {
            return *mq;
        }
        // 2) reachable: brokers still isolating whose channel remains reachable
        std::function<bool(const MessageQueue&)> reachableFilter =
            [this](const MessageQueue& mq) { return tolerance_.isReachable(mq.brokerName); };
        if (auto mq = tpInfo.selectOneMessageQueue(reachableFilter, brokerFilter)) {
            return *mq;
        }
        // 3) all isolated and unreachable: fall back to plain round-robin
        return tpInfo.selectOneMessageQueue();
    }

    // When disabled it degrades to plain round-robin (avoiding lastBrokerName when set; the overload already falls back when every queue shares it)
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
    // Scan from the end of the table towards the front for the first level with latency >= LATENCY_MAX[i]
    for (int i = kLatencyMaxSize - 1; i >= 0; --i) {
        if (currentLatency >= static_cast<double>(kLatencyMax[i])) {
            return kNotAvailableDuration[i];
        }
    }
    return 0;
}

}  // namespace rocketmq
