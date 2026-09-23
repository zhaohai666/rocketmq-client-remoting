// 拉取前流控（Java ProcessQueue 五个阈值）真机验证。
// 用法：rmq_live_flow_control 127.0.0.1:9876
//
// 与 Python 的 verify_flow_control_live.py、Rust 的 live_flow_control.rs、.NET 的
// LiveFlowControl.cs 一一对应（S0~S5）。
//
// 离线单测（tests/test_flow_control.cpp）锁的是**判据本身**；这里锁真机上两件离线
// 永远锁不住的事：
//   A. 闸门在真实 broker 上**确实会命中**（单位错一位、阈值读错一个字段，离线拿 mock
//      缓冲照样"能命中"，真机上却永远不命中或永远命中）；
//   B. 命中之后**一条消息都不许丢**：流控只是"暂停拉取"，不是"丢弃/跳过"。暂停期间
//      位点不许越过还没消费完的消息，恢复后同一个队列必须继续消费到末尾。
//
// S1 队列级字节闸门   —— 条数闸门压到 Java 上界(65535)，只剩 size=1MiB 这道可能命中
// S2 位点跨度闸门     —— 条数/字节都压到不命中，只剩 maxSpan=2 这道可能命中
// S3 topic 级条数闸门 —— 队列级三条全压到不命中，只有一台实例上**跨队列累计**才可能命中
// S4 命中之后恢复     —— 同一组再来一批大消息，闸门仍会命中且新消息照单全收
// S5 配置数值闸门     —— Java checkConfig(:1099-1209) 的区间边界值真机能启动并收全消息；
//                        越界配置在本地就被拒，且 broker 侧查不到这个消费组（没留下僵尸
//                        clientId 把 cidAll 撑歪）
//
// ⚠ 大消息必须是**不可压缩**的随机字节：生产者对超过压缩阈值的 body 先试压，全同字节
//   的 payload 会被压到几百字节，broker 落盘的 storeSize 跟着变成几百字节，"size 闸门
//   永不命中"就成了夹具问题而不是实现问题（Python 侧第一次跑就是这么踩到的）。
// ⚠ 每个场景都必须**先建 topic 再启动消费者**：消费者不做默认 topic 兜底，没路由就
//   不分配队列；S1 还必须建**单队列** topic，否则 8 条 400KB 摊到 4 条队列上每条才
//   800KB，永远够不到 1MiB 这道队列级闸门。
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::printf("  [%s] %s%s\n", ok ? "PASS" : "FAIL", name.c_str(),
                detail.empty() ? "" : ("  " + detail).c_str());
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// 轮询等待条件成立：真机投递受长轮询、流控退避和同机负载影响，固定 sleep 会在机器忙时
// 把"实现没问题"测成漏消息。
bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return pred();
}

std::string num(int64_t v) { return std::to_string(v); }

// 400KB 不可压缩消息体：6 字节前缀 + 随机尾巴（前缀只便于日志辨认，判据一律用条数）
Bytes bigBody(int32_t tag) {
    static std::mt19937_64 rng(std::random_device{}());
    std::string body = "FCBIG-" + std::to_string(tag);
    body.reserve(400 * 1024);
    std::uniform_int_distribution<int> byteDist(1, 255);
    while (body.size() < 400u * 1024u) {
        body.push_back(static_cast<char>(byteDist(rng)));
    }
    return body;
}

Bytes bodyBytes(const std::string& s) { return Bytes(s); }

void prepareTopic(DefaultMQProducer& producer, const std::string& topic, int32_t queues) {
    try {
        producer.createTopic("init", topic, queues);
    } catch (const std::exception& e) {
        std::printf("  预建 topic %s 失败（改用自动创建）: %s\n", topic.c_str(), e.what());
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

// 收消息用的 listener：可选地每批睡 slowMs（制造"已拉未消费"的堆积）
class Sink : public MessageListenerConcurrently {
public:
    explicit Sink(int32_t slowMs) : slowMs_(slowMs) {}

    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        if (slowMs_ > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(slowMs_));
        }
        std::lock_guard<std::mutex> lk(mtx_);
        for (const MessageExt& m : msgs) {
            bodies_.insert(m.body);
            ++got_;
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    int32_t got() {
        std::lock_guard<std::mutex> lk(mtx_);
        return got_;
    }

    size_t distinct() {
        std::lock_guard<std::mutex> lk(mtx_);
        return bodies_.size();
    }

private:
    int32_t slowMs_;
    std::mutex mtx_;
    int32_t got_ = 0;
    std::set<std::string> bodies_;
};

struct Case {
    std::shared_ptr<DefaultMQPushConsumer> consumer;
    std::shared_ptr<Sink> sink;
};

Case startConsumer(const std::string& nsAddr, const std::string& group,
                   const std::string& topic, int32_t slowMs) {
    Case c;
    c.sink = std::make_shared<Sink>(slowMs);
    c.consumer = std::make_shared<DefaultMQPushConsumer>(group);
    c.consumer->setMessageListener(c.sink);
    c.consumer->setNamesrvAddr(nsAddr);
    c.consumer->subscribe(topic);
    return c;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string nsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    const std::string prefix = "FcCpp_" + num(nowMs());

    DefaultMQProducer producer(prefix + "_pg");
    producer.setNamesrvAddr(nsAddr);
    try {
        producer.start();
    } catch (const std::exception& e) {
        std::printf("生产者启动失败：%s\n", e.what());
        return 2;
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // "把这道闸门压到不命中"的合法写法：Java checkConfig 的**上界**
    // （pullThresholdForQueue / consumeConcurrentlyMaxSpan 都是 [1, 65535]，
    // pullThresholdSizeForQueue 是 [1, 1024] MiB），**不是 0**。0 会被 S5 的启动期
    // 闸门拒（Java :1134/:1152）；运行期虽有 max(1,n) 兜底，但那道兜底不该拿来
    // 越过校验 —— 真机上写 0 的后果是"每轮都判成超限"，队列永久停拉。
    constexpr int32_t kOffCount = 65535;
    constexpr int32_t kOffSizeMiB = 1024;

    // ---------------- S0 默认闸门 + 快消费：不该命中 ----------------
    {
        const std::string topic = prefix + "_Defaults";
        prepareTopic(producer, topic, 4);
        Case c = startConsumer(nsAddr, prefix + "_g0", topic, 0);
        c.consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 12; ++i) {
            producer.send(Message(topic, bodyBytes("ok-" + std::to_string(i))));
        }
        const bool all = waitUntil([&] { return c.sink->got() >= 12; }, 30000);
        const int64_t fc = c.consumer->flowControlTriggered();
        c.consumer->shutdown();
        check("S0-默认闸门不命中", fc == 0, "triggered=" + num(fc));
        check("S0-默认闸门下全部到达", all && c.sink->got() == 12, "got=" + num(c.sink->got()));
        check("S0-一条不重不丢", c.sink->distinct() == 12, "distinct=" + num(c.sink->distinct()));
    }

    // ---------------- S1 队列级字节闸门（单队列 topic）----------------
    {
        const std::string topic = prefix + "_Size";
        prepareTopic(producer, topic, 1);
        Case c = startConsumer(nsAddr, prefix + "_g1", topic, 300);
        c.consumer->setPullThresholdForQueue(kOffCount);
        c.consumer->setPullThresholdSizeForQueue(1);  // 1 MiB
        c.consumer->setConsumeConcurrentlyMaxSpan(kOffCount);
        c.consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 8; ++i) {
            Message m(topic, bodyBytes("big-" + std::to_string(i)));
            m.body = bigBody(i);
            producer.send(m);
        }
        const bool hit = waitUntil([&] { return c.consumer->flowControlTriggered() > 0; }, 30000);
        const bool all = waitUntil([&] { return c.sink->got() >= 8; }, 40000);
        const int64_t fc = c.consumer->flowControlTriggered();
        c.consumer->shutdown();
        check("S1-队列级字节闸门真机命中", hit, "triggered=" + num(fc));
        check("S1-大消息一条不丢", all && c.sink->got() == 8, "got=" + num(c.sink->got()));
        check("S1-8 条 400KB 消息全不重复", c.sink->distinct() == 8,
              "distinct=" + num(c.sink->distinct()));
    }

    // ---------------- S2 位点跨度闸门 ----------------
    {
        const std::string topic = prefix + "_Span";
        prepareTopic(producer, topic, 4);
        Case c = startConsumer(nsAddr, prefix + "_g2", topic, 300);
        c.consumer->setPullThresholdForQueue(kOffCount);
        c.consumer->setPullThresholdSizeForQueue(kOffSizeMiB);
        c.consumer->setConsumeConcurrentlyMaxSpan(2);
        c.consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 14; ++i) {
            producer.send(Message(topic, bodyBytes("s-" + std::to_string(i))));
        }
        const bool hit = waitUntil([&] { return c.consumer->flowControlTriggered() > 0; }, 30000);
        const bool all = waitUntil([&] { return c.sink->got() >= 14; }, 40000);
        const int64_t fc = c.consumer->flowControlTriggered();
        c.consumer->shutdown();
        check("S2-跨度闸门真机命中", hit, "triggered=" + num(fc));
        check("S2-跨度过限后仍全部消费", all && c.sink->got() == 14, "got=" + num(c.sink->got()));
    }

    // ---------------- S3 topic 级条数闸门（跨队列累计）----------------
    {
        const std::string topic = prefix + "_Topic";
        prepareTopic(producer, topic, 4);
        Case c = startConsumer(nsAddr, prefix + "_g3", topic, 300);
        c.consumer->setPullThresholdForQueue(kOffCount);
        c.consumer->setPullThresholdSizeForQueue(kOffSizeMiB);
        c.consumer->setConsumeConcurrentlyMaxSpan(kOffCount);
        c.consumer->setPullThresholdForTopic(4);
        c.consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 16; ++i) {
            producer.send(Message(topic, bodyBytes("t-" + std::to_string(i))));
        }
        const bool hit = waitUntil([&] { return c.consumer->flowControlTriggered() > 0; }, 30000);
        const bool all = waitUntil([&] { return c.sink->got() >= 16; }, 40000);
        const int64_t fc = c.consumer->flowControlTriggered();
        c.consumer->shutdown();
        check("S3-topic 级条数闸门真机命中", hit, "triggered=" + num(fc));
        check("S3-跨队列累计后仍全部消费", all && c.sink->got() == 16,
              "got=" + num(c.sink->got()));
    }

    // ---------------- S4 命中过流控的队列恢复后继续消费 ----------------
    // 复用 S1 的组与 topic（位点已由 S1 提交到 broker 末尾）。这一条锁的是"暂停 100ms"
    // 被写成"退出拉取循环"的错误 —— 那条队列会永久停摆，而 S1 已消费完的消息看不出差别。
    {
        const std::string topic = prefix + "_Size";
        Case c = startConsumer(nsAddr, prefix + "_g1", topic, 300);
        c.consumer->setPullThresholdForQueue(kOffCount);
        c.consumer->setPullThresholdSizeForQueue(1);
        c.consumer->setConsumeConcurrentlyMaxSpan(kOffCount);
        c.consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 6; ++i) {
            Message m(topic, bodyBytes("r-" + std::to_string(i)));
            m.body = bigBody(100 + i);
            producer.send(m);
        }
        const bool all = waitUntil([&] { return c.sink->got() >= 6; }, 40000);
        const bool hit = waitUntil([&] { return c.consumer->flowControlTriggered() > 0; }, 10000);
        const int64_t fc = c.consumer->flowControlTriggered();
        c.consumer->shutdown();
        check("S4-触发过流控的队列恢复后继续消费", all && c.sink->got() == 6,
              "got=" + num(c.sink->got()));
        check("S4-恢复批次仍然命中流控（闸门不会命中一次后失效）", hit,
              "triggered=" + num(fc));
        check("S4-恢复批次不重复", c.sink->distinct() == 6, "distinct=" + num(c.sink->distinct()));
    }

    // ---------------- S5 配置数值闸门（Java checkConfig :1099-1209）----------------
    // 离线单测（tests/test_consumer_check_config.cpp）锁的是区间与文案；这里补两件
    // 只有真集群能锁死的事：
    //   1. 落在 Java 区间**边界**上的配置在 broker 上真能把消费者跑起来并收全消息 ——
    //      闸门写歪最常见的方式是"比 Java 还严"，把合法配置也拒了，用户直接起不来；
    //   2. 越界的配置**没有打到 broker 上**。写成"先注册再校验"的话，broker 的
    //      ConsumerManager 会留下一堆永不心跳的僵尸 clientId，把 rebalance 用的
    //      cidAll 撑歪（真机表现为队列分配不均），而客户端日志里只有启动失败那一条。
    {
        const std::string topic = prefix + "_Config";
        prepareTopic(producer, topic, 4);
        const std::string goodGroup = prefix + "_g5";
        const std::string badGroup = prefix + "_g6";

        Case c = startConsumer(nsAddr, goodGroup, topic, 0);
        // 每条闸门都取 Java 区间的端点值：pullBatchSize=1024、popInvisibleTime=300000
        // 这类"贴着上限"的写法在生产里就是"实际不拦"，误拒等于把用户挡在门外。
        c.consumer->setConsumeThreadMin(1);
        c.consumer->setConsumeThreadMax(2);
        c.consumer->setConsumeConcurrentlyMaxSpan(kOffCount);
        c.consumer->setPullThresholdForQueue(kOffCount);
        c.consumer->setPullThresholdForTopic(-1);
        c.consumer->setPullThresholdSizeForQueue(kOffSizeMiB);
        c.consumer->setPullThresholdSizeForTopic(-1);
        c.consumer->setPullIntervalMillis(0);
        c.consumer->setConsumeMessageBatchMaxSize(1);
        c.consumer->setPullBatchSize(1024);
        c.consumer->setPopInvisibleTime(300000);
        c.consumer->setPopBatchNums(32);
        bool boundaryStarted = true;
        try {
            c.consumer->start();
        } catch (const std::exception& e) {
            boundaryStarted = false;
            check("S5-边界值配置能启动", false, e.what());
        }
        if (boundaryStarted) {
            check("S5-边界值配置能启动", true, "isStarted=" + num(c.consumer->isStarted()));
            std::this_thread::sleep_for(std::chrono::seconds(3));
            for (int i = 0; i < 10; ++i) {
                producer.send(Message(topic, bodyBytes("c-" + std::to_string(i))));
            }
            const bool all = waitUntil([&] { return c.sink->got() >= 10; }, 30000);
            check("S5-边界值配置下 10 条全到达",
                  all && c.sink->got() == 10 && c.sink->distinct() == 10,
                  "got=" + num(c.sink->got()) + " distinct=" + num(c.sink->distinct()));
        }

        // 越界配置：本地拒（文案逐字对 Java）+ 失败后不留半启动实例。
        // 每条都取"刚刚越界"的值：差 1 就够，越界幅度大不代表更可信。
        struct BadCase {
            const char* want;
            std::function<void(DefaultMQPushConsumer&)> apply;
        };
        const BadCase kBad[] = {
            {"pullThresholdSizeForQueue Out of range [1, 1024]",
             [](DefaultMQPushConsumer& x) { x.setPullThresholdSizeForQueue(0); }},
            {"pullBatchSize Out of range [1, 1024]",
             [](DefaultMQPushConsumer& x) { x.setPullBatchSize(1025); }},
            {"popInvisibleTime Out of range [5000, 300000]",
             [](DefaultMQPushConsumer& x) { x.setPopInvisibleTime(4999); }},
            {"popBatchNums Out of range [1, 32]",
             [](DefaultMQPushConsumer& x) { x.setPopBatchNums(33); }},
            {"consumeThreadMin (8) is larger than consumeThreadMax (4)",
             [](DefaultMQPushConsumer& x) {
                 x.setConsumeThreadMin(8);
                 x.setConsumeThreadMax(4);
             }},
        };
        for (const BadCase& b : kBad) {
            auto sink = std::make_shared<Sink>(0);
            DefaultMQPushConsumer bad(badGroup);
            bad.setMessageListener(sink);
            bad.setNamesrvAddr(nsAddr);
            bad.subscribe(topic);
            b.apply(bad);
            bool rejected = false;
            std::string actual;
            try {
                bad.start();
                actual = "start() 居然成功了";
                bad.shutdown();
            } catch (const MQClientException& e) {
                rejected = true;
                actual = e.what();
            } catch (const std::exception& e) {
                actual = std::string("别的异常: ") + e.what();
            }
            check(std::string("S5-越界配置被拒: ") + b.want, rejected && actual == b.want,
                  "实际=" + actual);
            check(std::string("S5-越界配置没留下半启动实例: ") + b.want, !bad.isStarted());
        }

        // broker 侧反证：被拒的组查不到、边界值组查得到。
        // 必须用**裸**的 getConsumerListByGroup —— getConsumerIdListByGroup 内部吞异常
        // 返回空列表，"被拒绝"和"没注册"在调用方看来一模一样。
        if (boundaryStarted) {
            MQClientInstance probe("FC_CPP_PROBE_" + num(nowMs()), {nsAddr});
            probe.start();
            std::string addr;
            auto route = probe.getTopicRouteData(topic);
            if (route != nullptr && !route->brokerDatas.empty()) {
                addr = route->brokerDatas[0].selectBrokerAddr();
            }
            check("S5-拿到 broker 地址用于查消费组", !addr.empty(), "addr=" + addr);
            if (!addr.empty()) {
                // 从未注册过的组：broker 的 GET_CONSUMER_LIST_BY_GROUP 不回空列表，而是
                // 直接甩 code=1 "no consumer for this group"。两种形态都算"查无此组"，
                // 但**绝不能**返回任何 clientId。
                bool absent = false;
                std::string detail;
                try {
                    auto ids = probe.getConsumerListByGroup(badGroup, addr, 5000);
                    absent = ids.consumerIdList.empty();
                    detail = "ids=" + num(ids.consumerIdList.size());
                } catch (const MQBrokerException& e) {
                    absent = true;
                    detail = "broker 直接拒绝: code=" + num(e.getResponseCode()) + " "
                           + e.getResponseMessage();
                } catch (const std::exception& e) {
                    absent = false;
                    detail = std::string("探测请求失败: ") + e.what();
                }
                check("S5-broker 侧不知道被拒的消费组", absent, detail);

                std::string goodDetail;
                size_t goodN = 0;
                try {
                    goodN = probe.getConsumerListByGroup(goodGroup, addr, 5000)
                                .consumerIdList.size();
                } catch (const std::exception& e) {
                    goodDetail = std::string("探测失败: ") + e.what();
                }
                check("S5-broker 侧认下了边界值消费者", goodN == 1,
                      "n=" + num(static_cast<int32_t>(goodN))
                          + (goodDetail.empty() ? "" : " " + goodDetail));
            }
            probe.shutdown();
        }
        if (boundaryStarted) {
            c.consumer->shutdown();
        }
    }

    producer.shutdown();
    std::printf("flow control live: %d PASS / %d FAIL\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
