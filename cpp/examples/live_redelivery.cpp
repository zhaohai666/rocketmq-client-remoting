// 消费侧补齐真机验证（对齐 Java：回投 / 位点持久化 / 顺序锁 / 广播 / 流控）。
// 用法：rmq_live_redelivery 127.0.0.1:9876
//
// 场景：
//   S1 回投：listener 对 "retry-me" 首次返回 RECONSUME_LATER → 应经 %RETRY%topic
//      （延迟梯度 level3=10s）二次投递且 reconsumeTimes=1；正常消息只投一次。
//   S2 位点持久化：消费 3 条后重启同组消费者 → 旧消息不重复，新消息继续投递。
//   S3 顺序消费：orderly listener 消费正常，且 broker 队列锁（LOCK_BATCH_MQ）生效。
//   S4 广播模式：同组两个消费者各自收全所有消息。
//   S5 流控：阈值 2 + 慢消费 → 流控触发计数 >0，最终消息全部消费。
//   S6 集群多实例 rebalance：同组两实例均分队列（不重不漏），40 条消息无重复消费，
//      并且两侧都收到过 broker 反向推的 NOTIFY_CONSUMER_IDS_CHANGED(40)。
//   S7 优雅注销：shutdown 发 UNREGISTER_CLIENT，broker 端立刻摘除 clientId。
//   S8 命名空间：带 namespace 的生产者/消费者在 "<ns>%<topic>" 上收发成功；不带 namespace
//      的消费者订阅同名裸 topic 收不到（多租户隔离）。
//   S9 死信终态：maxReconsumeTimes=2 ⇒ 恰好投递 3 次（reconsumeTimes 0/1/2），第 3 次回投后
//      broker 改投 %DLQ%<group>（自动建 topic 并注册路由），死信里 reconsumeTimes=3、
//      RETRY_TOPIC 保留业务 topic，且原组不再有第 4 次投递。
//   S10 部分 ack（ackIndex）：CONSUME_SUCCESS + 一批 3 条里只认可第 1 条 ⇒ 尾巴 2 条经
//      %RETRY% 重投（reconsumeTimes>=1、listener 看到业务 topic）、已认可的那条整个窗口
//      只投一次、3 条最终全部消费完、业务队列位点仍整批提交到 3；对照组（不碰 ackIndex）
//      一条都不回投。
//   S11 拉取停摆自愈（Java isPullExpired / PULL_MAX_IDLE_TIME=120s）：把仍归本实例的队列
//      的 lastPull 时刻倒拨到阈值之外 ⇒ 这一趟 rebalance 必须撤掉它（持久化位点）并重建
//      拉取线程，之后同一队列继续消费、307 运行信息里的 lastPullTimestamp 是真值、
//      且前面消费过的 6 条一条都不重投（位点没回退）。
//
// ⚠ 每个场景都必须**先建 topic 再启动消费者**（见 prepareTopic）。消费者不做默认 topic
//   兜底（对齐 Java：只有生产者才会拿 TBW102 为新 topic 合成发布信息），所以 topic 不存在
//   时消费者拿不到路由 → 不分配队列 → 不消费；等它自己发现路由时，CONSUME_FROM_LAST_OFFSET
//   已把位点解析到"发现时刻的最新"，期间生产的消息会被正常跳过（Java 同样）。那是语义
//   正确但测不出东西的假失败，不是客户端 bug。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/route.h"

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

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::string str2bytes(const std::string& s) { return s; }

// 按真实用法先把 topic 建出来，再启动消费者（与 Python verify_redelivery_live.py 的
// prepare_topic 同义）。真实环境里 topic 由管理员或首次发送预先创建；消费者不做默认
// topic 兜底，topic 不存在时拿不到路由、不分配队列。这里显式建出来，避免测出
// "实现没问题但等超时/漏消息"的假失败。
void prepareTopic(DefaultMQProducer& producer, const std::string& topic, int32_t queues = 4) {
    try {
        producer.createTopic("init", topic, queues);
    } catch (const std::exception& e) {
        std::printf("  预建 topic %s 失败（改用自动创建）: %s\n", topic.c_str(), e.what());
    }
    // 等 NameServer 路由传播，否则消费者首轮 rebalance 仍查不到
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

// 轮询等待条件成立：真机投递受 broker 长轮询、流控和同机负载影响，固定 sleep 会在机器
// 忙时把「实现没问题」测成漏消息（实测同一份代码 got=7/10、35/40 抖动）。
bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return pred();
}

// 收集盒：锁和缓冲区绑在一起，主线程读之前必须拿同一把锁。
struct BodySink {
    std::mutex mtx;
    std::vector<std::string> bodies;

    std::vector<std::string> snapshot() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies;
    }

    size_t size() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies.size();
    }

    bool empty() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies.empty();
    }
};

// 便捷监听器：把收到的 body 记进列表，全部 CONSUME_SUCCESS。
// 锁必须跟着缓冲区走：早先每个 listener 各持一把 mtx_，主线程读 vector 时谁都没锁，
// 是数据竞争（实测 S6 计数飘到 71/40、35/40 这种不可能的值）。
class CollectListener : public MessageListenerConcurrently {
public:
    explicit CollectListener(BodySink& sink) : sink_(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) {
            sink_.bodies.push_back(bodyOf(m));
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    BodySink& sink_;
};

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_redelivery <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    const std::string gPrefix = "GapCpp_" + std::to_string(nowMs() % 1000000);

    DefaultMQProducer producer(gPrefix + "_pg");
    producer.setNamesrvAddr(nsAddr);
    producer.start();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // ---------------- S1 回投 ----------------
    {
        const std::string topic = gPrefix + "_Retry";
        prepareTopic(producer, topic);
        auto consumer = std::make_shared<DefaultMQPushConsumer>(gPrefix + "_g1");
        std::mutex mtx;
        struct Arrival {
            std::string body;
            std::string topic;
            int32_t reconsumeTimes;
            int64_t ts;
        };
        std::vector<Arrival> seen;
        class L : public MessageListenerConcurrently {
        public:
            L(std::mutex& mtx, std::vector<Arrival>& seen) : mtx_(mtx), seen_(seen) {}
            ConsumeConcurrentlyStatus consumeMessage(
                const std::vector<MessageExt>& msgs, ConsumeConcurrentlyContext&) override {
                bool needRetry = false;
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    for (const MessageExt& m : msgs) {
                        seen_.push_back({bodyOf(m), m.topic, m.getReconsumeTimes(), nowMs()});
                        if (bodyOf(m) == "retry-me" && m.getReconsumeTimes() == 0) {
                            needRetry = true;
                        }
                    }
                }
                // retry-me 首次投递失败，重投后成功（重投次数走 MessageExt 第 13 字段）
                return needRetry ? ConsumeConcurrentlyStatus::RECONSUME_LATER
                                 : ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            std::mutex& mtx_;
            std::vector<Arrival>& seen_;
        };
        consumer->setMessageListener(std::make_shared<L>(mtx, seen));
        consumer->setNamesrvAddr(nsAddr);
        consumer->subscribe(topic);
        consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        producer.send(Message(topic, str2bytes("retry-me")));
        producer.send(Message(topic, str2bytes("normal-1")));
        std::printf("S1: 已发送，等待回投（延迟梯度 level3≈10s）...\n");
        std::this_thread::sleep_for(std::chrono::seconds(22));
        consumer->shutdown();

        std::vector<Arrival> retryArrivals;
        int32_t normalCount = 0;
        {
            std::lock_guard<std::mutex> lk(mtx);
            for (const Arrival& a : seen) {
                if (a.body == "retry-me") retryArrivals.push_back(a);
                if (a.body == "normal-1") ++normalCount;
            }
        }
        check("S1-retry-me 被投递多次", retryArrivals.size() >= 2,
              "arrivals=" + std::to_string(retryArrivals.size()));
        bool fromRetry = false;
        for (const Arrival& a : retryArrivals) {
            if (a.reconsumeTimes >= 1 || a.topic.rfind("%RETRY%", 0) == 0) fromRetry = true;
        }
        check("S1-重投来自 %RETRY%/reconsumeTimes", fromRetry);
        if (retryArrivals.size() >= 2) {
            const double gapSec =
                static_cast<double>(retryArrivals.back().ts - retryArrivals.front().ts) / 1000.0;
            check("S1-回投有延迟梯度(>=8s)", gapSec >= 8.0,
                  "gap=" + std::to_string(gapSec).substr(0, 5) + "s");
        } else {
            check("S1-回投有延迟梯度(>=8s)", false, "不足两次投递");
        }
        check("S1-正常消息只投一次", normalCount == 1,
              "arrivals=" + std::to_string(normalCount));
    }

    // ---------------- S2 位点持久化 ----------------
    {
        const std::string topic = gPrefix + "_Offset";
        const std::string group = gPrefix + "_g2";
        prepareTopic(producer, topic);
        BodySink round1;
        {
            auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
            consumer->setMessageListener(std::make_shared<CollectListener>(round1));
            consumer->setNamesrvAddr(nsAddr);
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(3));
            for (int i = 0; i < 3; ++i) {
                producer.send(Message(topic, str2bytes("persist-" + std::to_string(i))));
            }
            waitUntil([&] { return round1.size() >= 3; }, 40000);
            consumer->shutdown();  // shutdown 持久化位点
        }
        const std::vector<std::string> seen1 = round1.snapshot();
        int32_t gotFirst = 0;
        for (int i = 0; i < 3; ++i) {
            if (std::find(seen1.begin(), seen1.end(),
                          "persist-" + std::to_string(i)) != seen1.end()) {
                ++gotFirst;
            }
        }
        check("S2-首轮消费 3 条", gotFirst == 3, "got=" + std::to_string(gotFirst));

        BodySink round2;
        {
            auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
            consumer->setMessageListener(std::make_shared<CollectListener>(round2));
            consumer->setNamesrvAddr(nsAddr);
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(3));
            producer.send(Message(topic, str2bytes("persist-new")));
            waitUntil([&] { return round2.size() >= 1; }, 40000);
            consumer->shutdown();
        }
        const std::vector<std::string> seen2 = round2.snapshot();
        bool newSeen = std::find(seen2.begin(), seen2.end(), "persist-new") != seen2.end();
        int32_t oldResent = 0;
        for (const std::string& b : seen2) {
            if (b.rfind("persist-", 0) == 0 && b != "persist-new") ++oldResent;
        }
        check("S2-重启后新消息继续投递", newSeen, newSeen ? "" : "未收到");
        check("S2-重启不重复消费旧消息", oldResent == 0,
              "重复=" + std::to_string(oldResent));
    }

    // ---------------- S3 顺序消费 + broker 锁 ----------------
    {
        const std::string topic = gPrefix + "_Orderly";
        prepareTopic(producer, topic);
        auto consumer = std::make_shared<DefaultMQPushConsumer>(gPrefix + "_g3");
        std::atomic<int32_t> got{0};
        class L : public MessageListenerOrderly {
        public:
            explicit L(std::atomic<int32_t>& got) : got_(got) {}
            ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                ConsumeOrderlyContext&) override {
                got_.fetch_add(static_cast<int32_t>(msgs.size()));
                return ConsumeOrderlyStatus::SUCCESS;
            }

        private:
            std::atomic<int32_t>& got_;
        };
        consumer->setMessageListener(std::make_shared<L>(got));
        consumer->setNamesrvAddr(nsAddr);
        consumer->subscribe(topic);
        consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(5));  // 等 LOCK_BATCH_MQ 首轮生效
        for (int i = 0; i < 4; ++i) {
            producer.send(Message(topic, str2bytes("orderly-" + std::to_string(i))));
        }
        const bool orderlyAll = waitUntil([&] { return got.load() >= 4; }, 30000);
        consumer->shutdown();
        check("S3-顺序消费收全", orderlyAll && got.load() == 4, "got=" + std::to_string(got.load()));
        // lockOK 数通过消费成功 + 无异常间接验证（内部锁集合非空在日志 debug 可见）
        check("S3-顺序消费链路存活", got.load() > 0);
    }

    // ---------------- S4 广播模式 ----------------
    {
        const std::string topic = gPrefix + "_Bc";
        const std::string group = gPrefix + "_g4";
        prepareTopic(producer, topic);
        BodySink gotA;
        BodySink gotB;
        auto ca = std::make_shared<DefaultMQPushConsumer>(group);
        ca->setInstanceName("bc-a");
        ca->setMessageListener(std::make_shared<CollectListener>(gotA));
        ca->setMessageModel(MessageModel::BROADCASTING);
        ca->setNamesrvAddr(nsAddr);
        ca->subscribe(topic);
        ca->start();
        auto cb = std::make_shared<DefaultMQPushConsumer>(group);
        cb->setInstanceName("bc-b");
        cb->setMessageListener(std::make_shared<CollectListener>(gotB));
        cb->setMessageModel(MessageModel::BROADCASTING);
        cb->setNamesrvAddr(nsAddr);
        cb->subscribe(topic);
        cb->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 3; ++i) {
            producer.send(Message(topic, str2bytes("bc-" + std::to_string(i))));
        }
        // 广播模式下两个实例各自收全 3 条
        const bool aAll = waitUntil([&] { return gotA.size() >= 3; }, 30000);
        const bool bAll = waitUntil([&] { return gotB.size() >= 3; }, 30000);
        ca->shutdown();
        cb->shutdown();
        check("S4-广播消费者 A 收全", aAll && gotA.size() == 3,
              "got=" + std::to_string(gotA.size()));
        check("S4-广播消费者 B 收全", bAll && gotB.size() == 3,
              "got=" + std::to_string(gotB.size()));
    }

    // ---------------- S5 流控 ----------------
    {
        const std::string topic = gPrefix + "_Flow";
        prepareTopic(producer, topic);
        auto consumer = std::make_shared<DefaultMQPushConsumer>(gPrefix + "_g5");
        std::atomic<int32_t> got{0};
        class L : public MessageListenerConcurrently {
        public:
            explicit L(std::atomic<int32_t>& got) : got_(got) {}
            ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                     ConsumeConcurrentlyContext&) override {
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
                got_.fetch_add(static_cast<int32_t>(msgs.size()));
                return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            std::atomic<int32_t>& got_;
        };
        consumer->setMessageListener(std::make_shared<L>(got));
        consumer->setPullThresholdForQueue(2);
        consumer->setNamesrvAddr(nsAddr);
        consumer->subscribe(topic);
        consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 10; ++i) {
            producer.send(Message(topic, str2bytes("flow-" + std::to_string(i))));
        }
        // 阈值 2 + 每条睡 300ms：全部落袋才说明「流控只是暂停拉取，不丢消息」。
        // 固定 sleep 在机器忙时会测出 got=7/10 的假失败（同一份代码复跑即绿）。
        const bool allArrived = waitUntil([&] { return got.load() >= 10; }, 45000);
        const int64_t fc = consumer->flowControlTriggered();
        consumer->shutdown();
        check("S5-慢消费下消息全部到达", allArrived && got.load() == 10,
              "got=" + std::to_string(got.load()));
        check("S5-流控触发计数>0", fc > 0, "triggered=" + std::to_string(fc));
    }

    // ---------------- S6 集群多实例 rebalance（队列分配） ----------------
    {
        const std::string topic6 = gPrefix + "_Rebalance";
        const std::string group6 = gPrefix + "_g6";
        // 先把 topic 建出来（8 队列）并等路由传播，否则消费者启动时无路由，
        // 分配要等到下一轮 20s rebalance 才稳定（会读到中间态）
        try {
            producer.createTopic("init", topic6, 8);
            std::this_thread::sleep_for(std::chrono::seconds(3));
        } catch (const std::exception& e) {
            std::printf("S6: 预建 topic 失败（改用自动创建）: %s\n", e.what());
        }
        BodySink recA;
        BodySink recB;
        auto ca = std::make_shared<DefaultMQPushConsumer>(group6);
        ca->setInstanceName("inst-a");
        ca->setMessageListener(std::make_shared<CollectListener>(recA));
        ca->setNamesrvAddr(nsAddr);
        ca->subscribe(topic6);
        ca->start();
        auto cb = std::make_shared<DefaultMQPushConsumer>(group6);
        cb->setInstanceName("inst-b");
        cb->setMessageListener(std::make_shared<CollectListener>(recB));
        cb->setNamesrvAddr(nsAddr);
        cb->subscribe(topic6);
        cb->start();
        // 等分配稳定：交集为空 + 两边都非空 + 主 topic 队列被完整覆盖（最多等 45s）
        std::vector<std::string> keysA, keysB;
        int asgA = 0, asgB = 0;
        std::set<std::string> expectedKeys;
        {
            for (const MessageQueue& mq : ca->fetchSubscribeMessageQueues(topic6)) {
                expectedKeys.insert(mq.topic + mq.brokerName + std::to_string(mq.queueId));
            }
        }
        int64_t deadline = nowMs() + 45000;
        while (nowMs() < deadline) {
            keysA = ca->assignedQueueKeys();
            keysB = cb->assignedQueueKeys();
            asgA = static_cast<int>(keysA.size());
            asgB = static_cast<int>(keysB.size());
            std::set<std::string> ua(keysA.begin(), keysA.end());
            std::set<std::string> ub(keysB.begin(), keysB.end());
            std::set<std::string> inter;
            std::set_intersection(ua.begin(), ua.end(), ub.begin(), ub.end(),
                                  std::inserter(inter, inter.begin()));
            std::set<std::string> uni = ua;
            uni.insert(ub.begin(), ub.end());
            std::set<std::string> covered;
            std::set_intersection(uni.begin(), uni.end(), expectedKeys.begin(), expectedKeys.end(),
                                  std::inserter(covered, covered.begin()));
            if (asgA > 0 && asgB > 0 && inter.empty() && covered == expectedKeys) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
        const int64_t hbA = ca->heartbeatCount();
        std::vector<std::string> cidList = ca->consumerIdListOfGroup(topic6);
        const int n6 = 40;
        for (int i = 0; i < n6; ++i) {
            producer.send(Message(topic6, str2bytes("rb-" + std::to_string(i))));
        }
        std::this_thread::sleep_for(std::chrono::seconds(15));
        // 先给一轮固定投递时间，再等 40 条收齐（机器忙时固定 15s 会测成 35/40 的假失败）；
        // 收齐后再多等一会儿给重复投递露面的机会：这时多出来的条数才是真重复，不是「还没到」。
        const bool allArrived = waitUntil([&] { return recA.size() + recB.size() >= 40; }, 60000);
        std::this_thread::sleep_for(std::chrono::seconds(8));
        const int total6 = static_cast<int>(recA.size() + recB.size());
        std::vector<std::string> all6 = recA.snapshot();
        const std::vector<std::string> b6 = recB.snapshot();
        all6.insert(all6.end(), b6.begin(), b6.end());
        std::set<std::string> uniq(all6.begin(), all6.end());
        int dup6 = static_cast<int>(all6.size()) - static_cast<int>(uniq.size());
        // broker 在组成员变化时沿长连接反向推 40；反向请求用例注入不了，所以计数是
        // 唯一能证明「实例级 40 处理器真的跑过」的落点（必须在 shutdown 之前取样）。
        const size_t notifiedA = ca->client().consumerIdsChangedCount();
        const size_t notifiedB = cb->client().consumerIdsChangedCount();
        ca->shutdown();
        cb->shutdown();
        check("S6-消费者已心跳注册",
              hbA > 0 && cidList.size() == 2,
              "heartbeats=" + std::to_string(hbA)
                  + " brokerCids=" + std::to_string(cidList.size()));
        std::set<std::string> sa(keysA.begin(), keysA.end());
        std::set<std::string> sb(keysB.begin(), keysB.end());
        std::set<std::string> inter2;
        std::set_intersection(sa.begin(), sa.end(), sb.begin(), sb.end(),
                              std::inserter(inter2, inter2.begin()));
        std::set<std::string> uni2 = sa;
        uni2.insert(sb.begin(), sb.end());
        std::set<std::string> covered2;
        std::set_intersection(uni2.begin(), uni2.end(), expectedKeys.begin(), expectedKeys.end(),
                              std::inserter(covered2, covered2.begin()));
        check("S6-队列不重不漏(a=" + std::to_string(asgA) + ",b=" + std::to_string(asgB)
                  + ",交集=" + std::to_string(inter2.size()) + ",覆盖=" + std::to_string(covered2.size())
                  + "/" + std::to_string(expectedKeys.size()) + ")",
              asgA > 0 && asgB > 0 && inter2.empty() && !expectedKeys.empty()
                  && covered2 == expectedKeys);
        check("S6-消息无重复消费", allArrived && dup6 == 0 && total6 == n6,
              "got=" + std::to_string(total6) + "/" + std::to_string(n6)
                  + " dup=" + std::to_string(dup6));
        check("S6-成员变化时收到 broker 的 NOTIFY_CONSUMER_IDS_CHANGED(40)",
              notifiedA > 0 && notifiedB > 0,
              "a=" + std::to_string(notifiedA) + " b=" + std::to_string(notifiedB)
                  + "（0 表示实例级处理器没收到过反向通知）");
    }

    // ---------------- S7 优雅注销 ----------------
    {
        // 复用在 S6 建的 8 队列 topic；shutdown 时应发 UNREGISTER_CLIENT，broker 端立刻摘除，
        // 不必等心跳超时（~120s）。查询用独立的探针客户端（消费者 shutdown 后其内部客户端已关闭）。
        const std::string topic7 = gPrefix + "_Rebalance";
        const std::string group7 = gPrefix + "_g7";
        MQClientInstance probe("probe-" + std::to_string(nowMs()),
                               std::vector<std::string>{nsAddr});
        probe.start();
        auto qc = std::make_shared<DefaultMQPushConsumer>(group7);
        qc->setInstanceName("inst-c");
        BodySink sink;
        qc->setMessageListener(std::make_shared<CollectListener>(sink));
        qc->setNamesrvAddr(nsAddr);
        qc->subscribe(topic7);
        qc->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        const std::string cid7 = qc->clientId();
        std::vector<std::string> listBefore = probe.getConsumerIdListByGroup(topic7, group7);
        qc->shutdown();
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::vector<std::string> listAfter = probe.getConsumerIdListByGroup(topic7, group7);
        probe.shutdown();
        bool beforeHas =
            std::find(listBefore.begin(), listBefore.end(), cid7) != listBefore.end();
        bool afterHas = std::find(listAfter.begin(), listAfter.end(), cid7) != listAfter.end();
        check("S7-shutdown 已注销 clientId",
              beforeHas && !afterHas,
              "before=" + std::to_string(listBefore.size())
                  + " after=" + std::to_string(listAfter.size()));
    }

    // ---------------- S8 命名空间（多租户隔离）----------------
    {
        const std::string ns = "NSCpp" + std::to_string(nowMs() % 100000);
        const std::string topic = gPrefix + "_Ns";
        DefaultMQProducer nsProducer(gPrefix + "_ns_pg");
        nsProducer.setNamesrvAddr(nsAddr);
        nsProducer.setNamespace(ns);
        nsProducer.start();
        // createTopic 也走 namespace 包装：真实建出来的是 "<ns>%<topic>"
        prepareTopic(nsProducer, topic);

        BodySink nsGot;
        {
            auto consumer = std::make_shared<DefaultMQPushConsumer>(gPrefix + "_g8");
            consumer->setNamespace(ns);
            consumer->setMessageListener(std::make_shared<CollectListener>(nsGot));
            consumer->setNamesrvAddr(nsAddr);
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(3));
            for (int i = 0; i < 3; ++i) {
                nsProducer.send(Message(topic, str2bytes("ns-" + std::to_string(i))));
            }
            waitUntil([&] { return nsGot.size() >= 3; }, 30000);
            consumer->shutdown();
        }
        check("S8-带 namespace 生产/消费收全", nsGot.size() == 3,
              "got=" + std::to_string(nsGot.size()));

        // 不带 namespace 的消费者订阅同一个裸 topic → 收不到（证明真实 topic 是 ns%topic）
        BodySink plainGot;
        try {
            auto consumer = std::make_shared<DefaultMQPushConsumer>(gPrefix + "_g8plain");
            consumer->setMessageListener(std::make_shared<CollectListener>(plainGot));
            consumer->setNamesrvAddr(nsAddr);
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(8));
            consumer->shutdown();
        } catch (const std::exception& e) {
            std::printf("  (无 ns 消费者异常，符合隔离预期: %s)\n", e.what());
        }
        check("S8-无 namespace 消费者收不到(隔离)", plainGot.empty(),
              "got=" + std::to_string(plainGot.size()));

        nsProducer.shutdown();
    }

    // ---------------- S9 死信终态（%DLQ%） ----------------
    // 为什么只能真机验：「重试到第几次算用尽」两端各写一半。客户端只把
    // maxReconsumeTimes 塞进 CONSUMER_SEND_MSG_BACK 请求头（Java
    // DefaultMQPushConsumerImpl#sendMessageBack:773，-1 时按 16 传，见
    // #getMaxReconsumeTimes:890）；判定与改投 %DLQ%<group> 全在 broker
    // （AbstractSendMessageProcessor#consumerSendMsgBack:183 用
    // `msgExt.getReconsumeTimes() >= maxReconsumeTimes`，注意是 >= 而不是 >；
    // 转死信时 topic 换成 MixAll::getDlqTopic(group)、顺手建 topic 并注册路由，
    // :226 又给 reconsumeTimes +1）。两种写反都表现为「看起来正常」：客户端漏传
    // header ⇒ broker 用订阅组默认的 16 次，测试等到天荒地老；把 >= 写成 > ⇒
    // 多投一次才进死信。离线单测锁不住任何一边。
    // 这里用 maxReconsumeTimes=2 把终态压到几十秒，逐条钉住投递次数与死信内容。
    {
        const std::string topic = gPrefix + "_Dlq";
        const std::string group = gPrefix + "_g9";
        const int32_t kMaxReconsume = 2;
        prepareTopic(producer, topic, 1);
        std::mutex mtx;
        struct Arrival {
            int32_t reconsumeTimes;
            std::string topic;
            int64_t atMs;  // 相对发送时刻的投递延迟，用来区分「次数不对」和「来得慢」
        };
        std::vector<Arrival> seen;
        int64_t sentAtMs = 0;
        class L : public MessageListenerConcurrently {
        public:
            L(std::mutex& mtx, std::vector<Arrival>& seen, const int64_t& sentAtMs)
                : mtx_(mtx), seen_(seen), sentAtMs_(sentAtMs) {}
            ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                     ConsumeConcurrentlyContext&) override {
                bool mine = false;
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    for (const MessageExt& m : msgs) {
                        seen_.push_back({m.getReconsumeTimes(), m.topic, nowMs() - sentAtMs_});
                        if (bodyOf(m) == "dlq-me") mine = true;
                    }
                }
                return mine ? ConsumeConcurrentlyStatus::RECONSUME_LATER
                            : ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            std::mutex& mtx_;
            std::vector<Arrival>& seen_;
            const int64_t& sentAtMs_;
        };
        auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
        consumer->setMaxReconsumeTimes(kMaxReconsume);
        consumer->setMessageListener(std::make_shared<L>(mtx, seen, sentAtMs));
        consumer->setNamesrvAddr(nsAddr);
        consumer->subscribe(topic);
        consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        sentAtMs = nowMs();
        producer.send(Message(topic, str2bytes("dlq-me")));
        // 回投档位 = 3 + reconsumeTimes ⇒ level3(10s) + level4(30s)：理论 40s 收口。
        // 预算给到 150s：四套真机用例同机并跑时第 3 次投递实测拖到 100s 之后（环境慢，
        // 不是实现错），慢到什么程度由 times=[…]@Ns 直接暴露出来。
        std::vector<Arrival> firstThree;
        {
            int64_t deadline = nowMs() + 150000;
            while (nowMs() < deadline) {
                size_t n;
                {
                    std::lock_guard<std::mutex> lk(mtx);
                    n = seen.size();
                }
                if (n >= 3) break;
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
            std::lock_guard<std::mutex> lk(mtx);
            firstThree = seen;
        }
        std::this_thread::sleep_for(std::chrono::seconds(15));  // 反证：不该有第 4 次
        std::vector<Arrival> finalSeen;
        {
            std::lock_guard<std::mutex> lk(mtx);
            finalSeen = seen;
        }
        consumer->shutdown();
        std::string times;
        for (const Arrival& a : firstThree) {
            times += (times.empty() ? "" : ",") + std::to_string(a.reconsumeTimes) + "@"
                + std::to_string(a.atMs / 1000) + "s";
        }
        bool ladder = firstThree.size() >= 3 && firstThree[0].reconsumeTimes == 0
            && firstThree[1].reconsumeTimes == 1 && firstThree[2].reconsumeTimes == 2;
        check("S9-maxReconsumeTimes=2 ⇒ 投递 3 次（reconsumeTimes 0/1/2）", ladder,
              "times=[" + times + "]");
        check("S9-用尽后不再投递（观察窗口内只有 3 次）", finalSeen.size() == 3,
              "arrivals=" + std::to_string(finalSeen.size()));
        bool topicKept = true;
        for (const Arrival& a : firstThree) {
            if (a.topic != topic) topicKept = false;
        }
        check("S9-重投期间 listener 看到业务 topic（不是 %RETRY%）", topicKept);

        const std::string dlqTopic = MixAll::getDlqTopic(group);
        std::shared_ptr<TopicRouteData> dlqRoute;
        MQClientInstance probe("dlqprobe-" + std::to_string(nowMs()),
                               std::vector<std::string>{nsAddr});
        probe.start();
        for (int i = 0; i < 15; ++i) {
            dlqRoute = probe.getTopicRouteData(dlqTopic);
            if (dlqRoute && !dlqRoute->queueDatas.empty()) break;
            dlqRoute.reset();
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
        check("S9-broker 自动创建并注册了 %DLQ%<group> 路由", dlqRoute != nullptr,
              "dlq=" + dlqTopic);
        probe.shutdown();

        std::vector<MessageExt> dlqMsgs;
        if (dlqRoute) {
            std::vector<MessageQueue> queues;
            for (const QueueData& q : dlqRoute->queueDatas) {
                for (int32_t i = 0; i < q.readQueueNums; ++i) {
                    queues.emplace_back(dlqTopic, q.brokerName, i);
                }
            }
            DefaultLitePullConsumer reader(gPrefix + "_g9dlq");
            reader.setNamesrvAddr(nsAddr);
            // 新消费组 + LAST 会从队尾开始，把已经在死信里的那条跳过 ⇒ 假失败
            reader.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
            reader.assign(queues);
            reader.start();
            for (const MessageQueue& mq : queues) reader.seekToBegin(mq);
            int64_t deadline = nowMs() + 25000;
            while (nowMs() < deadline && dlqMsgs.empty()) {
                std::vector<MessageExt> batch = reader.poll(1000);
                dlqMsgs.insert(dlqMsgs.end(), batch.begin(), batch.end());
            }
            reader.shutdown();
        }
        const bool one = dlqMsgs.size() == 1 && bodyOf(dlqMsgs[0]) == "dlq-me";
        check("S9-消息落在 %DLQ%<group>", one,
              "n=" + std::to_string(dlqMsgs.size()));
        if (one) {
            const MessageExt& d = dlqMsgs.front();
            check("S9-死信 reconsumeTimes = maxReconsumeTimes + 1（broker 存储时 +1）",
                d.getReconsumeTimes() == kMaxReconsume + 1,
                "reconsumeTimes=" + std::to_string(d.getReconsumeTimes()));
            auto retryIt = d.properties.find("RETRY_TOPIC");
            check("S9-死信保留 RETRY_TOPIC=业务 topic，topic 已是 %DLQ%<group>",
                retryIt != d.properties.end() && retryIt->second == topic && d.topic == dlqTopic,
                "retryTopic="
                    + (retryIt == d.properties.end() ? std::string("<missing>") : retryIt->second));
        }
    }

    // ---------------- S10 部分 ack（ackIndex） ----------------
    // Java ConsumeMessageConcurrentlyService#processConsumeResult:207-269：CONSUME_SUCCESS
    // 时 listener 写的 ackIndex 把本批切成「已认可前缀 / 待回投后缀」，尾巴逐条
    // sendMessageBack；默认 Integer.MAX_VALUE 就是整批认可。
    // 离线单测（tests/test_consume_ack_index.cpp）只能锁「回投**失败**时位点不越过它」
    // —— 未 start 的消费者回投必败；「回投成功时尾巴真的被 broker 收下重投、已 ack 的那条
    // 整个窗口只投一次、业务队列位点仍整批前进」只有真 broker 说得了算得了。
    // 两种写错在离线看不出差别：忘记回投（尾巴静默丢失，收到的条数照样对）、
    // 把已 ack 的前缀也回投（看起来"没丢"，其实重复投递）。
    {
        const std::string topic = gPrefix + "_AckIndex";
        prepareTopic(producer, topic, 1);  // 1 队列：一批 3 条才连续且有序

        struct Rec {
            std::string body;
            std::string topic;
            int32_t times;
        };
        struct Sink {
            std::mutex mtx;
            std::vector<Rec> recs;
            std::vector<size_t> batchSizes;

            std::vector<Rec> snapshot() {
                std::lock_guard<std::mutex> lk(mtx);
                return recs;
            }
            size_t batchSize(size_t i) {
                std::lock_guard<std::mutex> lk(mtx);
                return i < batchSizes.size() ? batchSizes[i] : 0;
            }
            size_t count() {
                std::lock_guard<std::mutex> lk(mtx);
                return recs.size();
            }
        };

        // ackFirst < 0 = 完全不碰 ackIndex（对照组）
        class L : public MessageListenerConcurrently {
        public:
            L(Sink& sink, int32_t ackFirst) : sink_(sink), ackFirst_(ackFirst) {}
            ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                     ConsumeConcurrentlyContext& ctx) override {
                std::lock_guard<std::mutex> lk(sink_.mtx);
                for (const MessageExt& m : msgs) {
                    sink_.recs.push_back({bodyOf(m), m.topic, m.getReconsumeTimes()});
                }
                // 只在**首批**收窄 ackIndex：后续批次必须整批认可，否则尾巴永远回投不完
                sink_.batchSizes.push_back(msgs.size());
                if (ackFirst_ >= 0 && sink_.batchSizes.size() == 1) {
                    ctx.ackIndex = ackFirst_;
                }
                return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            Sink& sink_;
            int32_t ackFirst_;
        };

        const std::string group = gPrefix + "_g10";
        const std::string controlGroup = gPrefix + "_g10ctrl";
        Sink partial, control;
        auto mk = [&](const std::string& g, Sink& s, int32_t ackFirst) {
            auto c = std::make_shared<DefaultMQPushConsumer>(g);
            c->setNamesrvAddr(nsAddr);
            // 默认一批 1 条，不收窄到 3 就根本没有「部分」可言
            c->setConsumeMessageBatchMaxSize(3);
            c->subscribe(topic);
            c->setMessageListener(std::make_shared<L>(s, ackFirst));
            return c;
        };
        auto pc = mk(group, partial, 0);
        auto cc = mk(controlGroup, control, -1);
        // 先把 3 条放上去再起消费者：批次怎么切由拉取时机决定，先发才必然是「一整批 3 条」，
        // 否则首批可能只有 1~2 条，ackIndex=0 扣下的尾巴数量就不确定了。
        // 新组在 LAST_OFFSET 下会从分配时刻的最新位点开始，故显式从 FIRST_OFFSET 起消。
        pc->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        cc->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        for (int i = 0; i < 3; ++i) {
            producer.send(Message(topic, str2bytes("ack-" + std::to_string(i))));
        }
        pc->start();
        cc->start();
        waitUntil([&] { return partial.count() >= 3 && control.count() >= 3; }, 40000);

        const std::vector<std::string> tail = {"ack-1", "ack-2"};
        auto redelivered = [&](Sink& s) {
            const std::vector<Rec> seen = s.snapshot();
            size_t hit = 0;
            for (const std::string& b : tail) {
                for (const Rec& r : seen) {
                    if (r.body == b && r.times >= 1 && r.topic == topic) {
                        ++hit;
                        break;
                    }
                }
            }
            return hit;
        };
        // 尾巴要经 %RETRY%（延迟 level 3≈10s）+ 重试 topic 的路由注册 + 下一轮 rebalance
        const bool tailBack = waitUntil([&] { return redelivered(partial) == tail.size(); }, 150000);
        std::this_thread::sleep_for(std::chrono::seconds(10));  // 反证窗口：多余的重复投递会露出来
        const std::vector<Rec> seen = partial.snapshot();

        check("S10-首批确实拿到 3 条（ackIndex=0 才有「部分」可言）",
              partial.batchSize(0) == 3, "firstBatch=" + std::to_string(partial.batchSize(0)));
        check("S10-未认可的尾巴从 %RETRY% 回来（reconsumeTimes>=1 且 topic 是业务 topic）",
              tailBack, "redelivered=" + std::to_string(redelivered(partial)) + "/2");
        size_t ackedHits = 0;
        for (const Rec& r : seen) {
            if (r.body == "ack-0") ++ackedHits;
        }
        check("S10-已认可的那条整个窗口只投一次（没有把前缀也回投）", ackedHits == 1,
              "arrivals=" + std::to_string(ackedHits));
        std::set<std::string> distinct;
        for (const Rec& r : seen) distinct.insert(r.body);
        check("S10-3 条最终全部消费（不丢）", distinct.size() == 3,
              "distinct=" + std::to_string(distinct.size()));

        size_t ctrlRetried = 0;
        const std::vector<Rec> ctrl = control.snapshot();
        for (const Rec& r : ctrl) {
            if (r.times >= 1) ++ctrlRetried;
        }
        check("S10-对照组默认 ackIndex(MAX_VALUE)：一条都不回投",
              ctrl.size() == 3 && ctrlRetried == 0,
              "deliveries=" + std::to_string(ctrl.size()) + " retried=" + std::to_string(ctrlRetried));

        // broker 侧口径：两条组的业务队列位点都必须整批提交到 3（部分 ack 不是「少提交」，
        // 尾巴已交给 broker 重投，本队列没有欠账）
        DefaultMQAdminExt admin(gPrefix + "_admin10");
        admin.setNamesrvAddr(nsAddr);
        try {
            admin.start();
            const std::vector<MessageQueue> queues = pc->fetchSubscribeMessageQueues(topic);
            check("S10-业务 topic 有队列可查位点", !queues.empty(),
                  "queues=" + std::to_string(queues.size()));
            if (!queues.empty()) {
                const MessageQueue mq = queues.front();
                // -1 = broker 还没有该组的位点（QUERY_NOT_FOUND）
                auto readOffset = [&](const std::string& g) -> int64_t {
                    int64_t off = -1;
                    try {
                        if (!admin.examineConsumerOffset(g, mq, off)) return -1;
                    } catch (const std::exception&) {
                        return -1;
                    }
                    return off;
                };
                auto committedIs = [&](const std::string& g, int64_t want) {
                    return readOffset(g) == want;
                };
                const bool p = waitUntil([&] { return committedIs(group, 3); }, 30000);
                check("S10-部分 ack 后业务队列位点仍整批前进到 3", p,
                      "committed=" + std::to_string(readOffset(group)));
                const bool c = waitUntil([&] { return committedIs(controlGroup, 3); }, 30000);
                check("S10-对照组业务队列位点同样到 3", c,
                      "committed=" + std::to_string(readOffset(controlGroup)));
            }
        } catch (const std::exception& e) {
            check("S10-位点查询可用", false, e.what());
        }
        admin.shutdown();
        pc->shutdown();
        cc->shutdown();
    }

    // ---------------- S11 拉取停摆自愈（Java isPullExpired / PULL_MAX_IDLE_TIME）----------------
    // 队列还归本实例、但拉取循环死了或卡住：Java RebalanceImpl:438-461 会在同一趟
    // rebalance 里把它撤掉（持久化位点 + 丢缓冲）再重建。不做这一步的坏法最难发现——
    // 客户端不报错、心跳照发、别的队列照常推进，只有"这个队列的位点永远不动"。
    // 阈值（120s）离线单测锁死（tests/test_pull_expired.cpp），这里锁真机闭环：
    // 注入停摆 → 撤+建 → **同一个队列继续消费**，且位点不回退（前 3 条不重投）。
    {
        const std::string topic11 = gPrefix + "_Heal";
        const std::string group11 = gPrefix + "_g11";
        prepareTopic(producer, topic11, 1);   // 1 队列：注入点唯一，位点判据也唯一

        struct Rec { std::string body; int32_t times; };
        struct Sink {
            std::mutex mtx;
            std::vector<Rec> recs;
            std::vector<Rec> snapshot() {
                std::lock_guard<std::mutex> lk(mtx);
                return recs;
            }
            size_t count() {
                std::lock_guard<std::mutex> lk(mtx);
                return recs.size();
            }
        };
        class L : public MessageListenerConcurrently {
        public:
            explicit L(Sink& s) : sink_(s) {}
            ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                     ConsumeConcurrentlyContext&) override {
                std::lock_guard<std::mutex> lk(sink_.mtx);
                for (const MessageExt& m : msgs) {
                    sink_.recs.push_back({bodyOf(m), m.getReconsumeTimes()});
                }
                return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            Sink& sink_;
        };

        Sink sink;
        auto c = std::make_shared<DefaultMQPushConsumer>(group11);
        c->setNamesrvAddr(nsAddr);
        c->subscribe(topic11);
        c->setMessageListener(std::make_shared<L>(sink));
        c->start();

        const std::vector<MessageQueue> queues = c->fetchSubscribeMessageQueues(topic11);
        check("S11-队列已分配", !queues.empty(), "queues=" + std::to_string(queues.size()));
        if (!queues.empty()) {
            const std::string key = DefaultMQPushConsumer::offsetKey(queues.front());
            auto epochMs = [] {
                return std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch()).count();
            };

            for (int i = 0; i < 3; ++i) {
                producer.send(Message(topic11, str2bytes("heal-1-" + std::to_string(i))));
            }
            const bool base = waitUntil([&] { return sink.count() >= 3; }, 40000);
            check("S11-基线：3 条被消费", base, "arrivals=" + std::to_string(sink.count()));

            // 307 应答里必须看得见这个时刻（Java ProcessQueue.fillOutRunningInfo:456）；
            // 写死 0 就等于把停摆判据的现场证据全丢了。
            const int64_t injected = epochMs() - kPullMaxIdleTime - 5000;
            c->setLastPullAt(key, injected);
            std::string body;
            try {
                const Bytes encoded = c->consumerRunningInfo().encode();
                body.assign(reinterpret_cast<const char*>(encoded.data()), encoded.size());
            } catch (const std::exception& e) {
                body = std::string("threw: ") + e.what();
            }
            check("S11-运行信息把 lastPullTimestamp 报成真值",
                  body.find("\"lastPullTimestamp\":" + std::to_string(injected)) != std::string::npos,
                  "want=" + std::to_string(injected));
            check("S11-倒拨超过 120s 即判停摆", c->pullStalled(key), "key=" + key);

            c->syncPullThreads();   // Java updateProcessQueueTableInRebalance 的撤+建
            const bool healed = waitUntil([&] {
                const int64_t now = c->lastPullAt(key);
                return now > injected && !c->pullStalled(key);
            }, 30000);
            check("S11-停摆队列被撤掉重建，新循环重新盖章", healed,
                  "lastPullAt=" + std::to_string(c->lastPullAt(key)));

            for (int i = 0; i < 3; ++i) {
                producer.send(Message(topic11, str2bytes("heal-2-" + std::to_string(i))));
            }
            const bool resumed = waitUntil([&] { return sink.count() >= 6; }, 40000);
            check("S11-自愈后同一个队列继续消费（重建不是空转）", resumed,
                  "arrivals=" + std::to_string(sink.count()));

            std::set<std::string> distinct;
            int32_t redelivered = 0;
            for (const Rec& r : sink.snapshot()) {
                distinct.insert(r.body);
                if (r.times >= 1) ++redelivered;
            }
            check("S11-6 条各只投一次（撤走前持久化了位点，重建后从 broker 续拉）",
                  distinct.size() == 6 && redelivered == 0,
                  "distinct=" + std::to_string(distinct.size())
                      + " redelivered=" + std::to_string(redelivered));
        }
        c->shutdown();
    }

    producer.shutdown();
    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}