// 路由刷新周期 / 位点落盘周期 真机验证（C++ 参考 python/verify_interval_live.py 的 I1–I3）。
//
// 对应用户要求「基于真实集群测试是否正常」：`pollNameServerInterval`（Java 默认 30s）与
// `persistConsumerOffsetInterval`（Java 默认 5s）都是**以时间为唯一可观测量**的配置项，
// 不接真集群就只能断言「字段被读到了」，证明不了「周期真的按配置走」。这里两段都用真机行为断言：
//
//   I1 路由刷新周期：两个生产者同时 start，周期分别设 1000ms 与 Java 默认 30000ms；两者都
//      用 `MQClientInstance::registerTopicInUse` 把一个**尚未创建**的 topic 登记进周期刷新
//      集合。之后才用 admin 建 topic（NameServer 侧路由立即可见），于是「缓存里何时出现这个
//      topic」只由各自的刷新周期决定：
//        * 1s 组 ≤6s 看到；
//        * 30s 组在那一刻**还看不到**（下一次刷新在启动后 30s）；
//        * 30s 组最终也在 ≤40s 内看到（默认值不是「卡死」，只是慢 30 倍）。
//      观测点必须用**只读缓存探测** `isTopicRouteCached`：`getTopicRouteData` 未命中会立刻
//      拉一次，用它等于自己把缓存填上，周期就不可观测了。
//
//   I2 位点落盘周期：两个消费者（周期 1000ms / 60000ms）消费同一 topic 的 3 条消息后
//      **不 commit、不 shutdown**，broker 侧位点只能由后台周期任务推上去。断言：
//        * 首次落盘发生在 start 后 ~10s（Java `scheduleAtFixedRate` 的 initialDelay
//          1000*10，不是立刻）；
//        * 再发 3 条：1s 组在 5s 内把位点推到 6；此刻 60s 组仍是 3（它的下一次落盘在 ~70s）；
//        * 60s 组 `shutdown()` 时把 6 落盘（Java persistConsumerOffset 的收尾语义），
//          证明它只是「周期没到」，不是坏了。
//
//   I3 透传：生产者/消费者 start 之后，真机实例上的 `pollNameServerIntervalMillis()` 就是调用方
//      设的值（不是只在门面上存着）。
//
// 前置：NameServer + Broker 已起（本仓库 /tmp/rmq_rust_live/broker.conf）。
// 用法：./rmq_live_scheduled_intervals 127.0.0.1:9876
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;
int32_t gSkip = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

void skip(const std::string& name, const std::string& detail) {
    ++gSkip;
    std::printf("  [SKIP] %s  %s\n", name.c_str(), detail.c_str());
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// 真机投递受 broker 长轮询/流控与同机负载影响，固定 sleep 会把「实现没问题」测成假失败。
bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 200) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

std::string secs(int64_t fromMs, int64_t toMs) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%.2fs", static_cast<double>(toMs - fromMs) / 1000.0);
    return std::string(buf);
}

struct BodySink {
    std::mutex mtx;
    std::vector<std::string> bodies;

    size_t size() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies.size();
    }
};

class CollectListener : public MessageListenerConcurrently {
public:
    explicit CollectListener(BodySink& sink) : sink_(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) {
            sink_.bodies.push_back(std::string(m.body.begin(), m.body.end()));
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    BodySink& sink_;
};

// broker 上该 group 这条队列的已提交位点（`setZeroIfNotFound=true` ⇒ 没提交过回 0，
// 与 Python `_broker_offset` 同口径）。判据是「broker 侧真的收到了位点」，所以只能查 broker。
int64_t brokerOffset(DefaultMQAdminExt& admin, const std::string& group, const MessageQueue& mq) {
    int64_t off = 0;
    if (!admin.client().queryConsumerOffset(group, mq, off, 5000, std::string(), true)) {
        return 0;
    }
    return off;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_scheduled_intervals <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    const std::string stamp = std::to_string(nowMs() % 1000000);
    const std::string tPoll = "IntervalPollTopic_" + stamp;
    const std::string tPersist = "IntervalPersistTopic_" + stamp;
    const std::string gPollFast = "G_poll_fast_" + stamp;
    const std::string gPollSlow = "G_poll_slow_" + stamp;
    const std::string gFast = "G_persist_fast_" + stamp;
    const std::string gSlow = "G_persist_slow_" + stamp;
    const int32_t kFastPollMs = 1000;
    const int32_t kSlowPollMs = 30000;  // Java ClientConfig.pollNameServerInterval 默认值
    const int32_t kFastPersistMs = 1000;
    const int32_t kSlowPersistMs = 60000;
    // Java startScheduledTask:423 的 initialDelay = 1000 * 10
    const double kPersistInitialDelaySec = 10.0;

    DefaultMQAdminExt admin;
    admin.setNamesrvAddr(nsAddr);
    admin.start();
    ClusterInfo cluster;
    try {
        cluster = admin.fetchBrokerClusterInfo();
    } catch (const std::exception& e) {
        check("集群探活", false, std::string("fetchBrokerClusterInfo: ") + e.what());
        admin.shutdown();
        return 1;
    }
    const std::vector<std::string> brokerAddrs = cluster.getBrokerAddrs();
    if (brokerAddrs.empty()) {
        check("集群探活", false, "nameServer 无 broker 注册");
        admin.shutdown();
        return 1;
    }
    const std::string brokerAddr = brokerAddrs[0];
    check("集群探活", true, "broker=" + brokerAddr);

    DefaultMQProducer* producer = nullptr;
    std::shared_ptr<DefaultMQProducer> fastPoll;
    std::shared_ptr<DefaultMQProducer> slowPoll;
    std::vector<std::shared_ptr<DefaultMQPushConsumer>> consumers;

    try {
        // ---------------- I1 路由刷新周期 ----------------
        fastPoll = std::make_shared<DefaultMQProducer>(gPollFast);
        fastPoll->setNamesrvAddr(nsAddr);
        fastPoll->setInstanceName("interval-poll-fast-" + stamp);
        fastPoll->setPollNameServerIntervalMillis(kFastPollMs);
        fastPoll->start();

        slowPoll = std::make_shared<DefaultMQProducer>(gPollSlow);
        slowPoll->setNamesrvAddr(nsAddr);
        slowPoll->setInstanceName("interval-poll-slow-" + stamp);
        slowPoll->start();  // 不设周期 = Java 默认 30s

        check("I3 生产者实例拿到配置的刷新周期（1s 组）",
              fastPoll->client().pollNameServerIntervalMillis() == kFastPollMs,
              "client.pollNameServerIntervalMillis=" +
                  std::to_string(fastPoll->client().pollNameServerIntervalMillis()));
        check("I3 生产者实例默认 30s（对照组）",
              slowPoll->client().pollNameServerIntervalMillis() == kSlowPollMs,
              "client.pollNameServerIntervalMillis=" +
                  std::to_string(slowPoll->client().pollNameServerIntervalMillis()));

        // 把一个还没创建的 topic 登记进周期刷新集合：两个生产者都不会给它发消息，
        // 所以缓存里何时出现它，只由各自的刷新周期决定。
        fastPoll->client().registerTopicInUse(tPoll);
        slowPoll->client().registerTopicInUse(tPoll);
        check("I1 两个生产者都把 " + tPoll + " 登记进在用 topic 集合（登记本身不拉取）",
              !fastPoll->client().isTopicRouteCached(tPoll) &&
                  !slowPoll->client().isTopicRouteCached(tPoll),
              "登记后两边缓存都为空");

        // 先让两个实例各自的**首跳**（Java scheduleAtFixedRate 的 initialDelay=10ms）跑完并把
        // 这个还不存在的 topic 拉失败一次，再去建 topic。否则首跳可能落在建 topic 之后：
        // 那一跳对两组都是"第一次拉"，30s 组照样当场拿到路由，两组间隔就退化成一个传输 RTT，
        // 这条对照实验也就失去意义（真机上表现为 30s 组和 1s 组同时命中）。
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));

        const int64_t t0 = nowMs();
        admin.createTopic(MixAll::DEFAULT_TOPIC, tPoll, 1);
        check("I1 admin 建 topic 成功", true, tPoll + " 1 队列");

        const bool fastOk =
            waitUntil([&] { return fastPoll->client().isTopicRouteCached(tPoll); }, 6000, 100);
        const int64_t dtFast = nowMs() - t0;
        check("I1 1s 周期组 ≤6s 从 NameServer 拉到新 topic 路由", fastOk,
              "dt=" + secs(t0, nowMs()) + " 周期=" + std::to_string(kFastPollMs) + "ms");
        check("I1 此刻 30s 周期组**还**没拉到（对照：周期决定时机）",
              !slowPoll->client().isTopicRouteCached(tPoll),
              "dt=" + secs(t0, nowMs()) + " 周期=" + std::to_string(kSlowPollMs) + "ms");

        const bool slowOk =
            waitUntil([&] { return slowPoll->client().isTopicRouteCached(tPoll); }, 40000, 500);
        const int64_t dtSlow = nowMs() - t0;
        check("I1 30s 周期组最终也拉到（默认值只是慢，不是坏）", slowOk,
              "dt=" + secs(t0, nowMs()) + " 周期=" + std::to_string(kSlowPollMs) + "ms");
        check("I1 两组间隔与配置同量级（30s 组至少晚 20s）", (dtSlow - dtFast) >= 20000,
              "fast=" + secs(0, dtFast) + " slow=" + secs(0, dtSlow));
        fastPoll->shutdown();
        slowPoll->shutdown();

        // ---------------- I2 位点落盘周期 ----------------
        admin.createTopic(MixAll::DEFAULT_TOPIC, tPersist, 1);
        const TopicRouteData persistRoute = admin.examineTopicRoute(tPersist);
        const std::vector<MessageQueue> persistQueues = persistRoute.getAllMessageQueue(tPersist);
        if (persistQueues.empty()) {
            throw std::runtime_error("examineTopicRoute 没返回 " + tPersist + " 的队列");
        }
        const MessageQueue mq = persistQueues[0];

        producer = new DefaultMQProducer("G_persist_producer_" + stamp);
        producer->setNamesrvAddr(nsAddr);
        producer->setInstanceName("interval-persist-prod-" + stamp);
        producer->start();
        for (int i = 0; i < 3; ++i) {
            producer->send(Message(tPersist, "batch1-" + std::to_string(i)));
        }

        BodySink sinkFast;
        BodySink sinkSlow;
        auto fastC = std::make_shared<DefaultMQPushConsumer>(gFast);
        fastC->setNamesrvAddr(nsAddr);
        fastC->setInstanceName("interval-persist-fast-" + stamp);
        fastC->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        fastC->setPollNameServerIntervalMillis(2000);
        fastC->setPersistConsumerOffsetIntervalMillis(kFastPersistMs);
        fastC->setMessageListener(std::make_shared<CollectListener>(sinkFast));
        fastC->subscribe(tPersist);

        auto slowC = std::make_shared<DefaultMQPushConsumer>(gSlow);
        slowC->setNamesrvAddr(nsAddr);
        slowC->setInstanceName("interval-persist-slow-" + stamp);
        slowC->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        slowC->setPersistConsumerOffsetIntervalMillis(kSlowPersistMs);
        slowC->setMessageListener(std::make_shared<CollectListener>(sinkSlow));
        slowC->subscribe(tPersist);

        const int64_t tStart = nowMs();
        fastC->start();
        consumers.push_back(fastC);
        check("I3 消费者实例拿到配置的刷新周期",
              fastC->client().pollNameServerIntervalMillis() == 2000,
              "client.pollNameServerIntervalMillis=" +
                  std::to_string(fastC->client().pollNameServerIntervalMillis()));
        slowC->start();
        consumers.push_back(slowC);

        const bool consumed =
            waitUntil([&] { return sinkFast.size() >= 3 && sinkSlow.size() >= 3; }, 30000);
        check("I2 两个消费者都消费到 3 条（尚未 commit / shutdown）", consumed,
              "fast=" + std::to_string(sinkFast.size()) + " slow=" +
                  std::to_string(sinkSlow.size()));

        // 首笔落盘在 start 后 ~10s：消费若在 10s 内完成，此刻两边都还没推上去。
        // 消费本身慢过 10s 时这一格不再有判别力，记为 SKIP 而不是硬报（真机负载导致的
        // 时序漂移不该记成客户端问题）。
        if (nowMs() - tStart < 9000) {
            const int64_t offFastNow = brokerOffset(admin, gFast, mq);
            const int64_t offSlowNow = brokerOffset(admin, gSlow, mq);
            check("I2 消费后 broker 位点还没有立刻被推上去", offFastNow < 3 && offSlowNow < 3,
                  "fast=" + std::to_string(offFastNow) + " slow=" + std::to_string(offSlowNow) +
                      " elapsed=" + secs(tStart, nowMs()));
        } else {
            skip("I2 消费后 broker 位点还没有立刻被推上去",
                 "消费耗时 " + secs(tStart, nowMs()) + " ≥ 10s，已越过首笔落盘窗口");
        }

        const bool fastFirst =
            waitUntil([&] { return brokerOffset(admin, gFast, mq) == 3; }, 20000, 300);
        const int64_t dtFastFirst = nowMs() - tStart;
        const bool slowFirst =
            waitUntil([&] { return brokerOffset(admin, gSlow, mq) == 3; }, 20000, 300);
        const int64_t dtSlowFirst = nowMs() - tStart;
        check("I2 1s 组首次落盘发生在 initialDelay(~10s) 之后", fastFirst,
              "dt=" + secs(0, dtFastFirst) + " 周期=" + std::to_string(kFastPersistMs) + "ms");
        const double fastFirstSec = static_cast<double>(dtFastFirst) / 1000.0;
        check("I2 首笔落盘不早于 Java 的 initialDelay 10s",
              fastFirst && fastFirstSec >= kPersistInitialDelaySec - 0.5,
              "dt=" + secs(0, dtFastFirst));
        check("I2 60s 组同样在 ~10s 完成首笔落盘（周期未到，先走 initialDelay）", slowFirst,
              "dt=" + secs(0, dtSlowFirst) + " 周期=" + std::to_string(kSlowPersistMs) + "ms");

        // 第二批：两组都会消费到，但 broker 侧位点只由各自的周期任务推上去。
        const int64_t t2 = nowMs();
        for (int i = 0; i < 3; ++i) {
            producer->send(Message(tPersist, "batch2-" + std::to_string(i)));
        }
        check("I2 两个消费者都消费到第二批（6 条）",
              waitUntil([&] { return sinkFast.size() >= 6 && sinkSlow.size() >= 6; }, 20000),
              "fast=" + std::to_string(sinkFast.size()) + " slow=" +
                  std::to_string(sinkSlow.size()));
        const bool fastSecond =
            waitUntil([&] { return brokerOffset(admin, gFast, mq) == 6; }, 5000, 200);
        check("I2 1s 组一个周期内把第二批位点推上去", fastSecond,
              "dt=" + secs(t2, nowMs()) + " 周期=" + std::to_string(kFastPersistMs) + "ms");
        const int64_t offSlowBatch2 = brokerOffset(admin, gSlow, mq);
        check("I2 此刻 60s 组仍是 3（周期 60s 远未到，且它确实消费到了 6）",
              offSlowBatch2 == 3 && sinkSlow.size() >= 6,
              "broker=" + std::to_string(offSlowBatch2) + " 已消费=" +
                  std::to_string(sinkSlow.size()) + " 周期=" + std::to_string(kSlowPersistMs) + "ms");
        const int64_t sinceSlowFirst = nowMs() - (tStart + dtSlowFirst);
        check("I2 60s 组的下一次周期还没到（距首笔落盘 < 周期 60s）",
              sinceSlowFirst < kSlowPersistMs, "elapsed=" + secs(0, sinceSlowFirst));

        slowC->shutdown();  // Java persistConsumerOffset 的收尾语义：退出前把内存位点落盘
        consumers.erase(consumers.begin() + 1);
        const bool slowSecond =
            waitUntil([&] { return brokerOffset(admin, gSlow, mq) == 6; }, 10000, 300);
        check("I2 60s 组 shutdown() 时把 6 落盘（Java persistConsumerOffset 收尾）", slowSecond,
              "broker=" + std::to_string(brokerOffset(admin, gSlow, mq)));
    } catch (const std::exception& e) {
        check("验证过程抛出异常", false, e.what());
    }

    // ---------------- 清理 ----------------
    for (auto& c : consumers) {
        try {
            c->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (consumer shutdown 失败: %s)\n", e.what());
        }
    }
    if (producer != nullptr) {
        try {
            producer->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (producer shutdown 失败: %s)\n", e.what());
        }
        delete producer;
    }
    for (const std::shared_ptr<DefaultMQProducer>& p : {fastPoll, slowPoll}) {
        if (!p) continue;
        try {
            p->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (producer shutdown 失败: %s)\n", e.what());
        }
    }
    for (const std::string& t : {tPoll, tPersist}) {
        try {
            admin.deleteTopic(t);
            std::printf("    (deleteTopic(%s) OK)\n", t.c_str());
        } catch (const std::exception& e) {
            std::printf("    (deleteTopic(%s) 失败: %s)\n", t.c_str(), e.what());
        }
    }
    for (const std::string& g : {gPollFast, gPollSlow, gFast, gSlow}) {
        try {
            admin.deleteSubscriptionGroup(brokerAddr, g, true);
        } catch (const std::exception& e) {
            std::printf("    (deleteSubscriptionGroup(%s) 失败: %s)\n", g.c_str(), e.what());
        }
    }
    try {
        admin.shutdown();
    } catch (const std::exception& e) {
        std::printf("    (admin shutdown 失败: %s)\n", e.what());
    }

    std::printf("\nPASS=%d FAIL=%d SKIP=%d\n", gPass, gFail, gSkip);
    return gFail == 0 ? 0 : 1;
}
