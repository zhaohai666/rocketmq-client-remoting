// 消费侧统计（对应 org.apache.rocketmq.client.stat.ConsumerStatsManager 与
// org.apache.rocketmq.common.stats.{StatsItem,StatsItemSet,StatsSnapshot}）。
//
// Java 真实模型（5.5.1 源码逐条核对，Python 参考实现 rocketmq/client/consumer_stats.py
// 有完整注释）：StatsItem 持有**累计值** value/times（只增不减），加两级采样快照链
// （每 10s 采分钟点、每 10 分钟采小时点）。快照计算 computeStatsData（StatsItem.java:53-79）：
//   sum   = last.value - first.value
//   tps   = sum * 1000.0 / (last.ts - first.ts)     // 每秒
//   avgpt = timesDiff > 0 ? sum / timesDiff : 0     // RT 项即平均耗时
// TPS 类计数 addValue(msgs, 1)；RT 类计数 addRTValue(rt, 1)。
//
// 实现差异（语义不变）：Java 给每个 StatsItem 单独排采样任务；这里由 manager 的
// **一个**采样线程统一巡采（10s 一轮，60 轮做一次小时级）——精度相同，线程省。
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

// 采样参数（Java StatsItem.init 的 scheduleAtFixedRate 参数）
inline constexpr double kSamplingIntervalSeconds = 10.0;
inline constexpr size_t kMinuteListMax = 60;   // ≈ 10 分钟窗口
inline constexpr size_t kHourListMax = 60;

// 对应 Java StatsSnapshot：sum / tps / avgpt / times
struct StatsSnapshot {
    int64_t sum = 0;
    double tps = 0.0;
    double avgpt = 0.0;
    int64_t times = 0;
};

// Java StatsItem.computeStatsData（StatsItem.java:53-79）逐条照抄。
// csList 元素 = (timestamp_ms, 累计 value, 累计 times)。
StatsSnapshot computeStatsData(const std::deque<std::tuple<int64_t, int64_t, int64_t>>& csList);

class StatsItem {
public:
    StatsItem(std::string statsName, std::string statsKey);

    void addValue(int64_t incValue, int64_t incTimes);
    void sample();        // 分钟级采样点（每 10s）
    void sampleHour();    // 小时级采样点（每 10 分钟）

    StatsSnapshot getStatsDataInMinute();
    StatsSnapshot getStatsDataInHour();

    int64_t value();
    int64_t times();

    // 仅供测试：注入自定义时间戳的采样点（真实时钟两次 sample 间隔≈0，tps 无从测起）。
    void appendSampleForTest(int64_t tsMs, int64_t value, int64_t times) {
        std::lock_guard<std::mutex> lk(mutex_);
        minute_.emplace_back(tsMs, value, times);
        hour_.emplace_back(tsMs, value, times);
    }

private:
    std::string statsName_;
    std::string statsKey_;
    std::mutex mutex_;
    int64_t value_ = 0;
    int64_t times_ = 0;
    std::deque<std::tuple<int64_t, int64_t, int64_t>> minute_;
    std::deque<std::tuple<int64_t, int64_t, int64_t>> hour_;
};

// key -> StatsItem（对应 Java StatsItemSet；key = topic@group）
class StatsItemSet {
public:
    explicit StatsItemSet(std::string statsName);

    std::shared_ptr<StatsItem> getAndCreate(const std::string& key);
    std::shared_ptr<StatsItem> find(const std::string& key);   // 无则返回 nullptr
    void addValue(const std::string& key, int64_t incValue, int64_t incTimes);
    std::vector<std::string> keys();
    void sampleAll();
    void sampleHourAll();

private:
    std::string statsName_;
    std::mutex mutex_;
    std::map<std::string, std::shared_ptr<StatsItem>> items_;
};

// 五个 StatsItemSet，key 一律 topic@group：
// PULL_RT / PULL_TPS / CONSUME_RT / CONSUME_OK_TPS / CONSUME_FAILED_TPS。
// start() 起统一采样线程（10s 分钟级 + 每 60 轮即 10 分钟小时级）。
class ConsumerStatsManager {
public:
    ConsumerStatsManager();
    ~ConsumerStatsManager();

    ConsumerStatsManager(const ConsumerStatsManager&) = delete;
    ConsumerStatsManager& operator=(const ConsumerStatsManager&) = delete;

    void start();
    void shutdown();

    // ---- 记数（Java ConsumerStatsManager 同名方法，参数顺序一致）----
    void incPullRT(const std::string& group, const std::string& topic, int64_t rt);
    void incPullTPS(const std::string& group, const std::string& topic, int64_t msgs);
    void incConsumeRT(const std::string& group, const std::string& topic, int64_t rt);
    void incConsumeOKTPS(const std::string& group, const std::string& topic, int64_t msgs);
    void incConsumeFailedTPS(const std::string& group, const std::string& topic, int64_t msgs);

    // ---- 查询 ----
    // Java ConsumerStatsManager.consumeStatus：全部取 minute 快照；
    // consumeFailedMsgs 取 failed 的 **hour** 窗口 sum（Java 特意跨窗口，照抄）。
    ConsumeStatus consumeStatus(const std::string& group, const std::string& topic);

    // 供测试注入快照点：对指定 set 里 key 对应的 item 追加自定义采样。
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

    // 采样线程（stop_ + condvar 定时等待，避免 sleep 轮询）
    std::mutex sampleMutex_;
    std::condition_variable sampleCond_;
    bool stop_ = false;
    bool started_ = false;
    std::unique_ptr<std::thread> sampleThread_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_CONSUMER_STATS_H
