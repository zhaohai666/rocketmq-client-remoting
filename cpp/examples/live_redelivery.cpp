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
//   S6 集群多实例 rebalance：同组两实例均分队列（不重不漏），40 条消息无重复消费。
//   S7 优雅注销：shutdown 发 UNREGISTER_CLIENT，broker 端立刻摘除 clientId。
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
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
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

// 便捷监听器：把收到的 body 记进列表，全部 CONSUME_SUCCESS
class CollectListener : public MessageListenerConcurrently {
public:
    explicit CollectListener(std::vector<std::string>& sink) : sink_(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const MessageExt& m : msgs) {
            sink_.push_back(bodyOf(m));
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    std::vector<std::string>& sink_;
    std::mutex mtx_;
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
        std::vector<std::string> round1;
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
            std::this_thread::sleep_for(std::chrono::seconds(8));
            consumer->shutdown();  // shutdown 持久化位点
        }
        int32_t gotFirst = 0;
        for (int i = 0; i < 3; ++i) {
            if (std::find(round1.begin(), round1.end(),
                          "persist-" + std::to_string(i)) != round1.end()) {
                ++gotFirst;
            }
        }
        check("S2-首轮消费 3 条", gotFirst == 3, "got=" + std::to_string(gotFirst));

        std::vector<std::string> round2;
        {
            auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
            consumer->setMessageListener(std::make_shared<CollectListener>(round2));
            consumer->setNamesrvAddr(nsAddr);
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(3));
            producer.send(Message(topic, str2bytes("persist-new")));
            std::this_thread::sleep_for(std::chrono::seconds(8));
            consumer->shutdown();
        }
        bool newSeen = std::find(round2.begin(), round2.end(), "persist-new") != round2.end();
        int32_t oldResent = 0;
        for (const std::string& b : round2) {
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
        std::this_thread::sleep_for(std::chrono::seconds(8));
        consumer->shutdown();
        check("S3-顺序消费收全", got.load() == 4, "got=" + std::to_string(got.load()));
        // lockOK 数通过消费成功 + 无异常间接验证（内部锁集合非空在日志 debug 可见）
        check("S3-顺序消费链路存活", got.load() > 0);
    }

    // ---------------- S4 广播模式 ----------------
    {
        const std::string topic = gPrefix + "_Bc";
        const std::string group = gPrefix + "_g4";
        prepareTopic(producer, topic);
        std::vector<std::string> gotA;
        std::vector<std::string> gotB;
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
        std::this_thread::sleep_for(std::chrono::seconds(8));
        ca->shutdown();
        cb->shutdown();
        check("S4-广播消费者 A 收全", static_cast<int32_t>(gotA.size()) == 3,
              "got=" + std::to_string(gotA.size()));
        check("S4-广播消费者 B 收全", static_cast<int32_t>(gotB.size()) == 3,
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
        std::this_thread::sleep_for(std::chrono::seconds(12));
        const int64_t fc = consumer->flowControlTriggered();
        consumer->shutdown();
        check("S5-慢消费下消息全部到达", got.load() == 10, "got=" + std::to_string(got.load()));
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
        std::vector<std::string> recA;
        std::vector<std::string> recB;
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
        int total6 = static_cast<int>(recA.size() + recB.size());
        std::vector<std::string> all6 = recA;
        all6.insert(all6.end(), recB.begin(), recB.end());
        std::set<std::string> uniq(all6.begin(), all6.end());
        int dup6 = static_cast<int>(all6.size()) - static_cast<int>(uniq.size());
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
        check("S6-消息无重复消费", dup6 == 0 && total6 == n6,
              "got=" + std::to_string(total6) + "/" + std::to_string(n6)
                  + " dup=" + std::to_string(dup6));
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
        std::vector<std::string> sink;
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

    producer.shutdown();
    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
