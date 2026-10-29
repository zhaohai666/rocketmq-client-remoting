#include "rocketmq/client/consumer_stats.h"

#include <chrono>
#include <utility>

#include "rocketmq/common/logging.h"
#include "rocketmq/common/util_all.h"

namespace rocketmq {

namespace {
int64_t nowMillis() { return UtilAll::currentTimeMillis(); }
}  // namespace

StatsSnapshot computeStatsData(const std::deque<std::tuple<int64_t, int64_t, int64_t>>& csList) {
    StatsSnapshot ss;
    if (csList.empty()) return ss;
    const auto& first = csList.front();
    const auto& last = csList.back();
    ss.sum = std::get<1>(last) - std::get<1>(first);
    const int64_t spanMs = std::get<0>(last) - std::get<0>(first);
    if (spanMs > 0) {
        ss.tps = static_cast<double>(ss.sum) * 1000.0 / static_cast<double>(spanMs);
    }
    const int64_t timesDiff = std::get<2>(last) - std::get<2>(first);
    ss.times = timesDiff;
    if (timesDiff > 0) {
        ss.avgpt = static_cast<double>(ss.sum) / static_cast<double>(timesDiff);
    }
    return ss;
}

// ---------------------------------------------------------------- StatsItem
StatsItem::StatsItem(std::string statsName, std::string statsKey)
    : statsName_(std::move(statsName)), statsKey_(std::move(statsKey)) {}

void StatsItem::addValue(int64_t incValue, int64_t incTimes) {
    std::lock_guard<std::mutex> lk(mutex_);
    value_ += incValue;
    times_ += incTimes;
}

void StatsItem::sample() {
    const int64_t now = nowMillis();
    std::lock_guard<std::mutex> lk(mutex_);
    if (minute_.empty()) {
        minute_.emplace_back(now - static_cast<int64_t>(kSamplingIntervalSeconds * 1000), 0, 0);
    }
    minute_.emplace_back(now, value_, times_);
    while (minute_.size() > kMinuteListMax) minute_.pop_front();
}

void StatsItem::sampleHour() {
    const int64_t now = nowMillis();
    std::lock_guard<std::mutex> lk(mutex_);
    if (hour_.empty()) {
        hour_.emplace_back(now - static_cast<int64_t>(kHourSamplingIntervalSeconds * 1000), 0, 0);
    }
    hour_.emplace_back(now, value_, times_);
    while (hour_.size() > kHourListMax) hour_.pop_front();
}

void StatsItem::sampleDay() {
    const int64_t now = nowMillis();
    std::lock_guard<std::mutex> lk(mutex_);
    if (day_.empty()) {
        day_.emplace_back(now - static_cast<int64_t>(kDaySamplingIntervalSeconds * 1000), 0, 0);
    }
    day_.emplace_back(now, value_, times_);
    while (day_.size() > kDayListMax) day_.pop_front();
}

StatsSnapshot StatsItem::getStatsDataInMinute() {
    std::lock_guard<std::mutex> lk(mutex_);
    return computeStatsData(minute_);
}

StatsSnapshot StatsItem::getStatsDataInHour() {
    std::lock_guard<std::mutex> lk(mutex_);
    return computeStatsData(hour_);
}

StatsSnapshot StatsItem::getStatsDataInDay() {
    std::lock_guard<std::mutex> lk(mutex_);
    return computeStatsData(day_);
}

int64_t StatsItem::value() {
    std::lock_guard<std::mutex> lk(mutex_);
    return value_;
}

int64_t StatsItem::times() {
    std::lock_guard<std::mutex> lk(mutex_);
    return times_;
}

// ---------------------------------------------------------------- StatsItemSet
StatsItemSet::StatsItemSet(std::string statsName) : statsName_(std::move(statsName)) {}

std::shared_ptr<StatsItem> StatsItemSet::getAndCreate(const std::string& key) {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = items_.find(key);
    if (it == items_.end()) {
        it = items_.emplace(key, std::make_shared<StatsItem>(statsName_, key)).first;
    }
    return it->second;
}

std::shared_ptr<StatsItem> StatsItemSet::find(const std::string& key) {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = items_.find(key);
    return it == items_.end() ? nullptr : it->second;
}

void StatsItemSet::addValue(const std::string& key, int64_t incValue, int64_t incTimes) {
    getAndCreate(key)->addValue(incValue, incTimes);
}

std::vector<std::string> StatsItemSet::keys() {
    std::lock_guard<std::mutex> lk(mutex_);
    std::vector<std::string> out;
    out.reserve(items_.size());
    for (const auto& kv : items_) out.push_back(kv.first);
    return out;
}

void StatsItemSet::sampleAll() {
    for (const auto& key : keys()) {
        if (auto item = find(key)) item->sample();
    }
}

void StatsItemSet::sampleHourAll() {
    for (const auto& key : keys()) {
        if (auto item = find(key)) item->sampleHour();
    }
}

void StatsItemSet::sampleDayAll() {
    for (const auto& key : keys()) {
        if (auto item = find(key)) item->sampleDay();
    }
}

// ---------------------------------------------------------------- ConsumerStatsManager
ConsumerStatsManager::ConsumerStatsManager()
    : topicAndGroupPullRT_("PULL_RT"),
      topicAndGroupPullTPS_("PULL_TPS"),
      topicAndGroupConsumeRT_("CONSUME_RT"),
      topicAndGroupConsumeOKTPS_("CONSUME_OK_TPS"),
      topicAndGroupConsumeFailedTPS_("CONSUME_FAILED_TPS") {}

ConsumerStatsManager::~ConsumerStatsManager() { shutdown(); }

void ConsumerStatsManager::start() {
    // start() converges on a single sampling thread (sampling could also hang off each StatsItem; not done here),
    // keeping the same 10s resolution.
    std::lock_guard<std::mutex> lk(sampleMutex_);
    if (started_) return;
    stop_ = false;
    started_ = true;
    sampleThread_.reset(new std::thread([this]() { sampleLoop(); }));
}

void ConsumerStatsManager::shutdown() {
    {
        std::lock_guard<std::mutex> lk(sampleMutex_);
        if (!started_) return;
        stop_ = true;
    }
    // The sampling thread parks in wait_for(10s), so it must be woken explicitly; otherwise shutdown waits a full sampling period
    sampleCond_.notify_all();
    if (sampleThread_ && sampleThread_->joinable()) {
        sampleThread_->join();
    }
    sampleThread_.reset();
    std::lock_guard<std::mutex> lk(sampleMutex_);
    started_ = false;
}

void ConsumerStatsManager::sampleLoop() {
    int64_t rounds = 0;
    for (;;) {
        bool stopped = false;
        {
            std::unique_lock<std::mutex> lk(sampleMutex_);
            // wait_for returning true means notify woke it (stop); a timeout means the sampling point is due.
            stopped = sampleCond_.wait_for(lk, std::chrono::duration<double>(kSamplingIntervalSeconds),
                                           [this]() { return stop_; });
        }
        if (stopped) return;
        ++rounds;
        topicAndGroupPullRT_.sampleAll();
        topicAndGroupPullTPS_.sampleAll();
        topicAndGroupConsumeRT_.sampleAll();
        topicAndGroupConsumeOKTPS_.sampleAll();
        topicAndGroupConsumeFailedTPS_.sampleAll();
        if (rounds % 60 == 0) {  // 60 x 10s = 10 minutes
            topicAndGroupPullRT_.sampleHourAll();
            topicAndGroupPullTPS_.sampleHourAll();
            topicAndGroupConsumeRT_.sampleHourAll();
            topicAndGroupConsumeOKTPS_.sampleHourAll();
            topicAndGroupConsumeFailedTPS_.sampleHourAll();
        }
        if (rounds % 360 == 0) {  // 360 x 10s = 1 hour
            topicAndGroupPullRT_.sampleDayAll();
            topicAndGroupPullTPS_.sampleDayAll();
            topicAndGroupConsumeRT_.sampleDayAll();
            topicAndGroupConsumeOKTPS_.sampleDayAll();
            topicAndGroupConsumeFailedTPS_.sampleDayAll();
        }
    }
}

namespace {
std::string statsKey(const std::string& topic, const std::string& group) {
    return topic + "@" + group;
}
}  // namespace

void ConsumerStatsManager::incPullRT(const std::string& group, const std::string& topic, int64_t rt) {
    topicAndGroupPullRT_.addValue(statsKey(topic, group), rt, 1);
}

void ConsumerStatsManager::incPullTPS(const std::string& group, const std::string& topic, int64_t msgs) {
    topicAndGroupPullTPS_.addValue(statsKey(topic, group), msgs, 1);
}

void ConsumerStatsManager::incConsumeRT(const std::string& group, const std::string& topic, int64_t rt) {
    topicAndGroupConsumeRT_.addValue(statsKey(topic, group), rt, 1);
}

void ConsumerStatsManager::incConsumeOKTPS(const std::string& group, const std::string& topic, int64_t msgs) {
    topicAndGroupConsumeOKTPS_.addValue(statsKey(topic, group), msgs, 1);
}

void ConsumerStatsManager::incConsumeFailedTPS(const std::string& group, const std::string& topic, int64_t msgs) {
    topicAndGroupConsumeFailedTPS_.addValue(statsKey(topic, group), msgs, 1);
}

ConsumeStatus ConsumerStatsManager::consumeStatus(const std::string& group, const std::string& topic) {
    ConsumeStatus cs;
    const std::string key = statsKey(topic, group);
    if (auto item = topicAndGroupPullRT_.find(key)) {
        cs.pullRT = item->getStatsDataInMinute().avgpt;
    }
    if (auto item = topicAndGroupPullTPS_.find(key)) {
        cs.pullTPS = item->getStatsDataInMinute().tps;
    }
    if (auto item = topicAndGroupConsumeRT_.find(key)) {
        cs.consumeRT = item->getStatsDataInMinute().avgpt;
    }
    if (auto item = topicAndGroupConsumeOKTPS_.find(key)) {
        cs.consumeOKTPS = item->getStatsDataInMinute().tps;
    }
    if (auto item = topicAndGroupConsumeFailedTPS_.find(key)) {
        cs.consumeFailedTPS = item->getStatsDataInMinute().tps;
        cs.consumeFailedMsgs = item->getStatsDataInHour().sum;
    }
    return cs;
}

}  // namespace rocketmq
