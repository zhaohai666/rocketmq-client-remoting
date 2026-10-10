// LitePull 消费者 topic 队列集合变更监听真机验证。
// 用法：rmq_live_lite_topic_queue_change 127.0.0.1:9876
//
// 为什么必须真机：比对趟次每次都现问 nameserver 取订阅队列集合，而普通路由缓存是 30s
// 才刷一次。假 nameserver 能证比对逻辑，只有真集群能证「现查」。所以这里把检查周期压到
// 1s 下限、路由轮询保持默认 30s，再把 topic 真的扩容：过了首查延迟的稳定期里，从
// nameserver 报出新队列数到监听器收到回调只该隔一两趟检查（≤5s）；如果比对读的是 30s
// 缓存，这个窗口会拖到半分钟以上。
//
//   L1  队列没动 ⇒ 监听器不被打扰
//   L1b 首查那趟真的跑过 ⇒ 依旧静默（运行中注册的快照不算变化）
//   L2  扩容 2→4：nameserver 认了之后，回调紧跟几趟检查
//   L3  回调后快照推进 ⇒ 同一套队列不再重复回调
//   L4  缩容 4→2：同样靠现查看到
//   L5  没建过的 topic：空队列集算「查不到」，不伪装成缩到 0 队列
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
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

constexpr int32_t kBaseQueueNum = 2;
constexpr int32_t kScaledQueueNum = 4;
// 检查周期压到下限，好把「每趟现查」和「吃 30s 缓存」在时间上分开。
constexpr int32_t kCheckIntervalMs = 1000;
// 后台比对的首查延迟（与实现里的默认值一致）。
constexpr int64_t kFirstDelayMs = 10000;
// nameserver 见到变化后允许的最大回调间隔。
constexpr int64_t kFreshWindowMs = 5000;

std::string idsText(const std::vector<int32_t>& ids) {
    std::string s;
    for (int32_t id : ids) {
        if (!s.empty()) s += ",";
        s += std::to_string(id);
    }
    return s;
}

/// 只记回调，不改状态；后台线程也会调它，所以自带锁。
class Recorder : public TopicMessageQueueChangeListener {
public:
    void onChanged(const std::string& topic,
                   const std::vector<MessageQueue>& messageQueues) override {
        std::vector<int32_t> ids;
        for (const MessageQueue& mq : messageQueues) ids.push_back(mq.queueId);
        std::sort(ids.begin(), ids.end());
        std::lock_guard<std::mutex> g(gate_);
        events_.emplace_back(topic, ids);
    }
    std::vector<std::pair<std::string, std::vector<int32_t>>> events() {
        std::lock_guard<std::mutex> g(gate_);
        return events_;
    }
    size_t count() { return events().size(); }
    std::vector<int32_t> lastIds() {
        auto ev = events();
        return ev.empty() ? std::vector<int32_t>() : ev.back().second;
    }

private:
    std::mutex gate_;
    std::vector<std::pair<std::string, std::vector<int32_t>>> events_;
};

std::vector<int32_t> seq(int32_t n) {
    std::vector<int32_t> v;
    for (int32_t i = 0; i < n; ++i) v.push_back(i);
    return v;
}

void sleepMs(int64_t ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

/// 现查队列集合，直到报出 want 个为止；返回等待毫秒数，超时返回 -1。
int64_t waitQueueNum(DefaultLitePullConsumer& c, const std::string& topic, int32_t want,
                     int64_t timeoutMs) {
    int64_t waited = 0;
    while (waited < timeoutMs) {
        try {
            if (static_cast<int32_t>(c.fetchMessageQueues(topic).size()) == want) {
                return waited;
            }
        } catch (const std::exception&) {
            // 路由还没更新，继续等
        }
        sleepMs(250);
        waited += 250;
    }
    return -1;
}

/// 等监听器记到 want 条回调；返回等待毫秒数，超时返回 -1。
int64_t waitEvents(Recorder& rec, size_t want, int64_t timeoutMs) {
    int64_t waited = 0;
    while (waited < timeoutMs) {
        if (rec.count() >= want) return waited;
        sleepMs(100);
        waited += 100;
    }
    return -1;
}

void scaleTopic(const std::string& namesrv, const std::string& topic, const std::string& group,
                int32_t queueNum) {
    DefaultMQProducer prod(group);
    prod.setNamesrvAddr(namesrv);
    prod.start();
    try {
        prod.createTopic("TBW102", topic, queueNum);
    } catch (const std::exception& e) {
        std::printf("!! createTopic(%d) failed: %s\n", queueNum, e.what());
    }
    prod.shutdown();
}

}  // namespace

int main(int argc, char** argv) {
    std::string namesrv = argc > 1 ? argv[1] : "127.0.0.1:9876";
    int64_t stamp = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    std::string topic = "LiteQcLive_" + std::to_string(stamp);
    std::string group = "LiteQcG_" + std::to_string(stamp);
    std::string ghost = "LiteQcGhost_" + std::to_string(stamp);
    std::printf("LitePull queue-change live (C++): namesrv=%s topic=%s\n",
                namesrv.c_str(), topic.c_str());

    scaleTopic(namesrv, topic, "PG_QcPrepare_" + std::to_string(stamp), kBaseQueueNum);

    DefaultLitePullConsumer c(group);
    c.setNamesrvAddr(namesrv);
    c.setInstanceName("lite-qc-live");
    c.setTopicMetadataCheckIntervalMillis(kCheckIntervalMs);
    check("L0 检查周期压到 1s 下限", c.topicMetadataCheckIntervalMillis() == kCheckIntervalMs,
          std::to_string(c.topicMetadataCheckIntervalMillis()));
    c.subscribe(topic, "*");
    c.start();
    auto loopStart = std::chrono::steady_clock::now();

    int64_t seen = waitQueueNum(c, topic, kBaseQueueNum, 30000);
    check("L0 路由可见（2 个队列）", seen >= 0, std::to_string(seen) + "ms");

    auto rec = std::make_shared<Recorder>();
    // 运行中注册 ⇒ 立刻记快照 ⇒ 首轮不该回调
    c.registerTopicMessageQueueChangeListener(topic, rec);
    sleepMs(3000);
    check("L1 队列没动 ⇒ 静默", rec->count() == 0, "events=" + std::to_string(rec->count()));

    // 等到首查延迟过去，此后只剩 1s 一趟的稳定期
    int64_t elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - loopStart).count();
    if (elapsed < kFirstDelayMs + 2000) sleepMs(kFirstDelayMs + 2000 - elapsed);
    check("L1b 首查那趟真的跑过 ⇒ 依旧静默", rec->count() == 0,
          "events=" + std::to_string(rec->count()));

    // ---- L2 扩容 2→4
    scaleTopic(namesrv, topic, "PG_QcUp_" + std::to_string(stamp), kScaledQueueNum);
    int64_t nsMs = waitQueueNum(c, topic, kScaledQueueNum, 45000);
    check("L2a nameserver 报出 4 个队列", nsMs >= 0, std::to_string(nsMs) + "ms");
    if (nsMs >= 0) {
        int64_t cb = waitEvents(*rec, 1, kFreshWindowMs);
        check("L2b 比对趟次现查路由（回调紧跟 nameserver，不等 30s 缓存）",
              cb >= 0 && rec->lastIds() == seq(kScaledQueueNum),
              std::to_string(cb) + "ms, ids=" + idsText(rec->lastIds()));
    }

    // ---- L3 快照推进
    sleepMs(3000);
    check("L3 回调后快照推进 ⇒ 不重复回调", rec->count() == 1,
          "events=" + std::to_string(rec->count()));

    // ---- L4 缩容 4→2
    scaleTopic(namesrv, topic, "PG_QcDown_" + std::to_string(stamp), kBaseQueueNum);
    nsMs = waitQueueNum(c, topic, kBaseQueueNum, 45000);
    check("L4a nameserver 报回 2 个队列", nsMs >= 0, std::to_string(nsMs) + "ms");
    if (nsMs >= 0) {
        int64_t cb = waitEvents(*rec, 2, kFreshWindowMs);
        check("L4b 缩容同样靠现查路由看到",
              cb >= 0 && rec->lastIds() == seq(kBaseQueueNum),
              std::to_string(cb) + "ms, ids=" + idsText(rec->lastIds()));
    }

    // ---- L5 未知 topic：空队列集算「查不到」
    std::string raised;
    try {
        auto queues = c.fetchMessageQueues(ghost);
        raised = "no exception, queues=" + std::to_string(queues.size());
    } catch (const std::exception& e) {
        raised = e.what();
    }
    check("L5 未知 topic 取队列抛「查不到」而不是返回空",
          raised.find("Namesrv return empty") != std::string::npos, raised);

    c.shutdown();
    std::printf("\nLitePull queue-change live (C++): PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
