#ifndef ROCKETMQ_CLIENT_CONSUMER_STATS_H
#define ROCKETMQ_CLIENT_CONSUMER_STATS_H

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "rocketmq/remoting/protocol/body.h"

namespace rocketmq {

inline constexpr double kSamplingIntervalSeconds = 10.0;
inline constexpr double kHourSamplingIntervalSeconds = 600.0;
inline constexpr double kDaySamplingIntervalSeconds = 3600.0;
inline constexpr size_t kMinuteListMax = 7;   // seven 10s points ≈ one minute plus the seed
inline constexpr size_t kHourListMax = 7;     // seven 10min points, same window shape
inline constexpr size_t kDayListMax = 25;     // 25 hourly points

struct StatsSnapshot {
    int64_t sum = 0;
    double tps = 0.0;
    double avgpt = 0.0;
    int64_t times = 0;
};

StatsSnapshot computeStatsData(const std::deque<std::tuple<int64_t, int64_t, int64_t>>& csList);

class StatsItem {
public:
    StatsItem(std::string statsName, std::string statsKey);

    void addValue(int64_t incValue, int64_t incTimes);
    void sample();        // minute-level sample point (every 10s)
    void sampleHour();    // hour-level sample point (every 10 minutes)
    void sampleDay();     // day-level sample point (every hour)

    StatsSnapshot getStatsDataInMinute();
    StatsSnapshot getStatsDataInHour();
    StatsSnapshot getStatsDataInDay();

    int64_t value();
    int64_t times();

    // Test only: inject sample points with custom timestamps (two samples from the real clock are ≈0 apart, so tps cannot be measured).
    void appendSampleForTest(int64_t tsMs, int64_t value, int64_t times) {
        std::lock_guard<std::mutex> lk(mutex_);
        minute_.emplace_back(tsMs, value, times);
        hour_.emplace_back(tsMs, value, times);
    }
    // Test only: chain lengths, so their caps are observable without decoding the window math.
    size_t minuteSampleCountForTest() { return sampleCount(minute_); }
    size_t hourSampleCountForTest() { return sampleCount(hour_); }
    size_t daySampleCountForTest() { return sampleCount(day_); }

private:
    size_t sampleCount(const std::deque<std::tuple<int64_t, int64_t, int64_t>>& chain) {
        std::lock_guard<std::mutex> lk(mutex_);
        return chain.size();
    }

    std::string statsName_;
    std::string statsKey_;
    std::mutex mutex_;
    int64_t value_ = 0;
    int64_t times_ = 0;
    std::deque<std::tuple<int64_t, int64_t, int64_t>> minute_;
    std::deque<std::tuple<int64_t, int64_t, int64_t>> hour_;
    std::deque<std::tuple<int64_t, int64_t, int64_t>> day_;
};

class StatsItemSet {
public:
    explicit StatsItemSet(std::string statsName);

    std::shared_ptr<StatsItem> getAndCreate(const std::string& key);
    std::shared_ptr<StatsItem> find(const std::string& key);   // nullptr when absent
    void addValue(const std::string& key, int64_t incValue, int64_t incTimes);
    std::vector<std::string> keys();
    void sampleAll();
    void sampleHourAll();
    void sampleDayAll();

private:
    std::string statsName_;
    std::mutex mutex_;
    std::map<std::string, std::shared_ptr<StatsItem>> items_;
};

// Five StatsItemSet instances, all keyed by topic@group:
// PULL_RT / PULL_TPS / CONSUME_RT / CONSUME_OK_TPS / CONSUME_FAILED_TPS.
// start() launches one shared sampling thread (10s minute-level tick; hour-level every 60 ticks, i.e. 10 minutes).
class ConsumerStatsManager {
public:
    ConsumerStatsManager();
    ~ConsumerStatsManager();

    ConsumerStatsManager(const ConsumerStatsManager&) = delete;
    ConsumerStatsManager& operator=(const ConsumerStatsManager&) = delete;

    void start();
    void shutdown();

    void incPullRT(const std::string& group, const std::string& topic, int64_t rt);
    void incPullTPS(const std::string& group, const std::string& topic, int64_t msgs);
    void incConsumeRT(const std::string& group, const std::string& topic, int64_t rt);
    void incConsumeOKTPS(const std::string& group, const std::string& topic, int64_t msgs);
    void incConsumeFailedTPS(const std::string& group, const std::string& topic, int64_t msgs);

    ConsumeStatus consumeStatus(const std::string& group, const std::string& topic);

    // For tests: append a custom sample to the item of a key inside the chosen set.
    StatsItemSet& topicAndGroupPullRT() { return topicAndGroupPullRT_; }
    StatsItemSet& topicAndGroupPullTPS() { return topicAndGroupPullTPS_; }
    StatsItemSet& topicAndGroupConsumeRT() { return topicAndGroupConsumeRT_; }
    StatsItemSet& topicAndGroupConsumeOKTPS() { return topicAndGroupConsumeOKTPS_; }
    StatsItemSet& topicAndGroupConsumeFailedTPS() { return topicAndGroupConsumeFailedTPS_; }

private:
    void sampleLoop();

    StatsItemSet topicAndGroupPullRT_;
    StatsItemSet topicAndGroupPullTPS_;
    StatsItemSet topicAndGroupConsumeRT_;
    StatsItemSet topicAndGroupConsumeOKTPS_;
    StatsItemSet topicAndGroupConsumeFailedTPS_;

    // Sampling thread (stop_ + condition-variable timed wait, so no sleep polling)
    std::mutex sampleMutex_;
    std::condition_variable sampleCond_;
    bool stop_ = false;
    bool started_ = false;
    std::unique_ptr<std::thread> sampleThread_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_CONSUMER_STATS_H
