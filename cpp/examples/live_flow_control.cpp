// 拉取前流控（Java ProcessQueue 五个阈值）真机验证。
// 用法：rmq_live_flow_control 127.0.0.1:9876
//
// 与 Python 的 verify_flow_control_live.py、Rust 的 live_flow_control.rs、.NET 的
// LiveFlowControl.cs 一一对应（S0~S4）。
//
// 离线单测（tests/test_flow_control.cpp）锁的是**判据本身**；这里锁真机上两件离线
// 永远锁不住的事：
//   A. 闸门在真实 broker 上**确实会命中**（单位错一位、阈值读错一个字段，离线拿 mock
//      缓冲照样"能命中"，真机上却永远不命中或永远命中）；
//   B. 命中之后**一条消息都不许丢**：流控只是"暂停拉取"，不是"丢弃/跳过"。暂停期间
//      位点不许越过还没消费完的消息，恢复后同一个队列必须继续消费到末尾。
//
// S1 队列级字节闸门   —— 条数闸门放到 int 上限，只剩 size=1MiB 这道可能命中
// S2 位点跨度闸门     —— 条数/字节都关掉，只剩 maxSpan=2 这道可能命中
// S3 topic 级条数闸门 —— 队列级三条全关掉，只有一台实例上**跨队列累计**才可能命中
// S4 命中之后恢复     —— 同一组再来一批大消息，闸门仍会命中且新消息照单全收
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

    constexpr int32_t kHuge = 2000000000;  // 远大于任何真机缓冲，等价"这条闸门关掉"

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
        c.consumer->setPullThresholdForQueue(kHuge);
        c.consumer->setPullThresholdSizeForQueue(1);  // 1 MiB
        c.consumer->setConsumeConcurrentlyMaxSpan(kHuge);
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
        c.consumer->setPullThresholdForQueue(kHuge);
        c.consumer->setPullThresholdSizeForQueue(0);  // 0 = 这条闸门关闭
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
        c.consumer->setPullThresholdForQueue(kHuge);
        c.consumer->setPullThresholdSizeForQueue(0);
        c.consumer->setConsumeConcurrentlyMaxSpan(kHuge);
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
        c.consumer->setPullThresholdForQueue(kHuge);
        c.consumer->setPullThresholdSizeForQueue(1);
        c.consumer->setConsumeConcurrentlyMaxSpan(kHuge);
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

    producer.shutdown();
    std::printf("flow control live: %d PASS / %d FAIL\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
