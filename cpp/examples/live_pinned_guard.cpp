// 定点发送 topic 一致性守卫真机验证（Java `DefaultMQProducerImpl:1234-1236` / `:1277-1278`）。
// 用法：rmq_live_pinned_guard 127.0.0.1:9876
//
// 与 `python/verify_pinned_guard_live.py`（S1..S6）、`rust/examples/live_pinned_guard.rs`、
// `dotnet/examples/RocketMQ.Examples/LivePinnedGuard.cs` 同题。
//
// 前置：NameServer + Broker 已起，``autoCreateTopicEnable=true``。
//
// 离线单测（`tests/test_send_retry.cpp::pinnedTopicGuard` / `test_producer_async.cpp::
// pinnedTopicGuardAsync`）用假端点证明的是「拒了、且报文没上线」；真机这一趟证明**另一面**：
//   S1/S6 守卫不误伤真业务：从真路由取到的队列在同步单条/同步批量/异步单条/异步批量四条
//         入口上照常 SEND_OK，消息按 keys 一条不少地被消费到；
//   S2/S4 拒的时候守在本端：亚毫秒、无 broker 码，而且 broker 上的 maxOffset 一动不动
//         （真机版的 wire 反证：拒绝不留痕，也不会事后偷发）；
//   S3    命名空间的比较用 Java `queueWithNamespace` 的幂等：`ns%topic` 与裸 topic 都不误拒，
//         `ns2%topic` 才拒；
//   S5    单向定点**没有**守卫（Java `:1303-1310` 有意留的口子）：报文按 msg 自己的 topic
//         落库 —— 用「A 收到、B 的 maxOffset 还是 0」把这条语义钉死。
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/heartbeat.h"

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

bool waitUntil(const std::function<bool()>& fn, int timeoutMillis) {
    const int64_t deadline = nowMs() + timeoutMillis;
    while (nowMs() < deadline) {
        if (fn()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return fn();
}

/// 一次异步发送的终态。
class SendLatch : public SendCallback {
public:
    void onSuccess(const SendResult& result) override {
        std::lock_guard<std::mutex> lk(m_);
        ++successCount;
        status = result.sendStatus;
        done = true;
    }
    void onException(const std::exception_ptr& error) override {
        std::lock_guard<std::mutex> lk(m_);
        ++exceptionCount;
        try {
            std::rethrow_exception(error);
        } catch (const std::exception& e) {
            message = e.what();
        } catch (...) {
            message = "<non-std exception>";
        }
        done = true;
    }

    bool waitDone(int timeoutMillis) { return waitUntil([this] { return done; }, timeoutMillis); }

    int successCount = 0;
    int exceptionCount = 0;
    SendStatus status = SendStatus::SEND_OK;
    std::string message;

private:
    std::mutex m_;
    volatile bool done = false;
};

/// 消费到的 keys 收集器。
struct KeySink {
    std::mutex mtx;
    std::vector<std::string> keys;

    std::vector<std::string> snapshot() {
        std::lock_guard<std::mutex> lk(mtx);
        return keys;
    }
};

class KeyListener : public MessageListenerConcurrently {
public:
    explicit KeyListener(KeySink& sink) : sink_(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) {
            const auto it = m.properties.find(MessageConst::PROPERTY_KEYS);
            if (it != m.properties.end()) {
                sink_.keys.push_back(it->second);
            } else {
                sink_.keys.push_back(std::string(m.body.begin(), m.body.end()));
            }
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    KeySink& sink_;
};

Message keyed(const std::string& topic, const Bytes& body, const std::string& key) {
    Message m(topic, body);
    if (!key.empty()) m.setKeys(key);
    return m;
}

std::string join(const std::vector<std::string>& v) {
    std::string s = "[";
    for (const std::string& x : v) {
        if (s.size() > 1) s += ", ";
        s += "\"" + x + "\"";
    }
    return s + "]";
}

/// 6*2=一个号段，stamp 用毫秒尾部，够本次跑完不重名。
std::string stamp() { return std::to_string(nowMs() % 1000000000); }

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_pinned_guard <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    const std::string tag = stamp();
    const std::string topicA = "PinGuardA_" + tag;
    const std::string topicB = "PinGuardB_" + tag;
    const std::string wTopic = "ns1%" + topicA;
    const std::string group = "G_pin_guard_" + tag;
    const std::string syncWording = "message's topic not equal mq's topic";
    const std::string asyncWording = "Topic of the message does not match its target message queue";
    const double localBudgetMs = 50.0;

    DefaultMQAdminExt admin;
    admin.setNamesrvAddr(nsAddr);
    admin.start();
    std::string brokerAddr;
    try {
        const ClusterInfo cluster = admin.fetchBrokerClusterInfo();
        const std::vector<std::string> addrs = cluster.getBrokerAddrs();
        if (addrs.empty()) {
            check("集群探活", false, "nameServer 无 broker 注册");
            admin.shutdown();
            return 1;
        }
        brokerAddr = addrs[0];
    } catch (const std::exception& e) {
        check("集群探活", false, std::string("fetchBrokerClusterInfo: ") + e.what());
        admin.shutdown();
        return 1;
    }
    check("集群探活", true, "broker=" + brokerAddr);

    std::shared_ptr<DefaultMQProducer> producer;
    std::shared_ptr<DefaultMQProducer> nsProducer;
    std::shared_ptr<DefaultMQPushConsumer> consumer;
    KeySink sink;

    try {
        admin.createTopic(topicA, topicA, 4, 0);
        admin.createTopic(topicB, topicB, 4, 0);
        admin.createTopic(wTopic, wTopic, 4, 0);

        producer = std::make_shared<DefaultMQProducer>("PG_pin_guard_" + tag);
        producer->setNamesrvAddr(nsAddr);
        producer->setInstanceName("pin-guard-" + tag);
        producer->start();

        // ---------------- S0 路由 + 消费者就位 ----------------
        MessageQueue mqA;
        MessageQueue mqB;
        bool routed = waitUntil(
            [&] {
                const std::vector<MessageQueue> qsA = producer->fetchPublishMessageQueues(topicA);
                const std::vector<MessageQueue> qsB = producer->fetchPublishMessageQueues(topicB);
                for (const MessageQueue& q : qsA) {
                    if (q.queueId == 0) mqA = q;
                }
                for (const MessageQueue& q : qsB) {
                    if (q.queueId == 0) mqB = q;
                }
                return !mqA.brokerName.empty() && !mqB.brokerName.empty();
            },
            20000);
        check("S0 两条 topic 的路由都可用（反腿用真队列，拒的才一定是 topic 而不是地址）",
              routed && mqA.brokerName == mqB.brokerName,
              "A=" + mqA.brokerName + "/" + std::to_string(mqA.queueId) + " B=" + mqB.brokerName +
                  "/" + std::to_string(mqB.queueId));
        if (!routed) {
            check("S0 前置失败，后续场景无法继续", false);
            producer->shutdown();
            admin.shutdown();
            std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
            return 1;
        }

        // 消费者先起（CONSUME_FROM_FIRST_OFFSET），不和发送抢 rebalance 的时间点
        consumer = std::make_shared<DefaultMQPushConsumer>(group);
        consumer->setNamesrvAddr(nsAddr);
        consumer->setInstanceName("pin-guard-live-" + tag);
        consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        consumer->setMessageListener(std::make_shared<KeyListener>(sink));
        consumer->subscribe(topicA, "*");
        consumer->start();

        // ---------------- S1 正腿：真路由队列上的定点发送 ----------------
        const std::string kSingle = "pinned-single-" + tag;
        const std::string kB1 = "pinned-b1-" + tag;
        const std::string kB2 = "pinned-b2-" + tag;
        {
            const SendResult r = producer->send(keyed(topicA, Bytes{'s', '1'}, kSingle), mqA, 5000);
            check("S1a 同步单条定点 SEND_OK 且落在 queue 0",
                  r.sendStatus == SendStatus::SEND_OK && r.messageQueue.queueId == mqA.queueId &&
                      r.messageQueue.topic == topicA,
                  "status=" + std::to_string(static_cast<int>(r.sendStatus)) +
                      " mq=" + r.messageQueue.topic + "/" + std::to_string(r.messageQueue.queueId));
        }
        {
            const SendResult r = producer->sendBatch(
                {keyed(topicA, Bytes{'b', '1'}, kB1), keyed(topicA, Bytes{'b', '2'}, kB2)},
                mqA, 5000);
            check("S1b 同步批量定点 SEND_OK 且同一队列",
                  r.sendStatus == SendStatus::SEND_OK && r.messageQueue.queueId == mqA.queueId,
                  "status=" + std::to_string(static_cast<int>(r.sendStatus)));
        }
        // 批量在消费队列上按**子消息**逐条落位（broker 收到 inner-batch 后拆开写）⇒ 2 条子消息涨 2
        int64_t offAfterS1 = -1;
        waitUntil(
            [&] {
                offAfterS1 = producer->maxOffset(mqA);
                return offAfterS1 >= 3;
            },
            10000);
        offAfterS1 = producer->maxOffset(mqA);
        check("S1c 三笔子消息都真落库（maxOffset = 单条 1 + 批量子消息 2）", offAfterS1 == 3,
              "maxOffset=" + std::to_string(offAfterS1));

        // ---------------- S2 反腿：同步拒绝，本端、无痕 ----------------
        {
            const int64_t began = nowMs();
            std::string msg;
            bool threw = false;
            try {
                producer->send(keyed(topicA, Bytes{'r', '1'}, "pinned-refused-" + tag), mqB, 5000);
            } catch (const std::exception& e) {
                threw = true;
                msg = e.what();
            }
            const double ms = static_cast<double>(nowMs() - began);
            check("S2a 同步单条拒（Java 原文案）", threw && msg == syncWording,
                  std::to_string(ms) + "ms " + (threw ? msg : "没有抛异常"));
            check("S2b 拒在本端：亚毫秒（不是超时、不是 broker remark）",
                  threw && ms < localBudgetMs, std::to_string(ms) + "ms");
        }
        {
            const int64_t began = nowMs();
            std::string msg;
            bool threw = false;
            try {
                producer->sendBatch({Message(topicA, Bytes{'r', '2'}), Message(topicA, Bytes{'r', '3'})},
                                    mqB, 5000);
            } catch (const std::exception& e) {
                threw = true;
                msg = e.what();
            }
            const double ms = static_cast<double>(nowMs() - began);
            check("S2c 同步批量共用同一处守卫与同一句文案",
                  threw && msg == syncWording && ms < localBudgetMs,
                  std::to_string(ms) + "ms " + (threw ? msg : "没有抛异常"));
        }
        {
            const int64_t aNow = producer->maxOffset(mqA);
            const int64_t bNow = producer->maxOffset(mqB);
            check("S2d wire 反证：A 的 maxOffset 一动没动，B 上一条都没有",
                  aNow == offAfterS1 && bNow == 0,
                  "A=" + std::to_string(aNow) + " B=" + std::to_string(bNow));
        }

        // ---------------- S3 命名空间：wrap 幂等，只拒真的不同名 ----------------
        nsProducer = std::make_shared<DefaultMQProducer>("PG_pin_guard_ns_" + tag);
        nsProducer->setNamesrvAddr(nsAddr);
        nsProducer->setInstanceName("pin-guard-ns-" + tag);
        nsProducer->setNamespace("ns1");
        nsProducer->start();
        MessageQueue mqw;
        const bool nsRouted = waitUntil(
            [&] {
                for (const MessageQueue& q : nsProducer->fetchPublishMessageQueues(wTopic)) {
                    if (q.queueId == 0) mqw = q;
                }
                return !mqw.brokerName.empty();
            },
            20000);
        check("S3a 带前缀 topic 的路由可用", nsRouted, "topic=" + wTopic);
        {
            const SendResult r =
                nsProducer->send(keyed(topicA, Bytes{'n', '1'}, "pinned-ns-q-" + tag), mqw, 5000);
            check("S3b 队列 topic 已带 ns 前缀：wrap 幂等，不误拒",
                  r.sendStatus == SendStatus::SEND_OK,
                  "status=" + std::to_string(static_cast<int>(r.sendStatus)));
        }
        {
            const SendResult r =
                nsProducer->send(keyed(wTopic, Bytes{'n', '2'}, "pinned-ns-m-" + tag), mqw, 5000);
            check("S3c 消息 topic 自己已带前缀同样放行",
                  r.sendStatus == SendStatus::SEND_OK,
                  "status=" + std::to_string(static_cast<int>(r.sendStatus)));
        }
        {
            const MessageQueue ns2Mq("ns2%" + topicA, mqw.brokerName, 0);
            const int64_t began = nowMs();
            std::string msg;
            bool threw = false;
            try {
                nsProducer->send(Message(topicA, Bytes{'n', '3'}), ns2Mq, 5000);
            } catch (const std::exception& e) {
                threw = true;
                msg = e.what();
            }
            const double ms = static_cast<double>(nowMs() - began);
            check("S3d 换成 ns2% 前缀才拒（对照腿：拒的是名字，不是「有前缀」）",
                  threw && msg == syncWording && ms < localBudgetMs,
                  std::to_string(ms) + "ms " + (threw ? msg : "没有抛异常"));
        }
        {
            int64_t nsOff = -1;
            waitUntil(
                [&] {
                    nsOff = nsProducer->maxOffset(mqw);
                    return nsOff >= 2;
                },
                10000);
            nsOff = nsProducer->maxOffset(mqw);
            check("S3e 两条放行腿真落进 ns1%topic（maxOffset=2）", nsOff == 2,
                  "maxOffset=" + std::to_string(nsOff));
        }

        // ---------------- S4 异步：回调里是异步那处文案 ----------------
        {
            auto refused = std::make_shared<SendLatch>();
            producer->sendAsync(keyed(topicA, Bytes{'a', 'r'}, "pinned-async-refused-" + tag), mqB,
                                refused, 5000);
            const bool done = refused->waitDone(10000);
            check("S4a 单条异步拒绝走回调、文案是异步那处",
                  done && refused->exceptionCount == 1 && refused->successCount == 0 &&
                      refused->message == asyncWording,
                  "msg=" + refused->message);
            const int64_t aNow = producer->maxOffset(mqA);
            check("S4b 拒后 maxOffset 仍不动（异步也没偷发）", aNow == offAfterS1,
                  "maxOffset=" + std::to_string(aNow));
        }
        {
            auto refused = std::make_shared<SendLatch>();
            producer->sendBatchAsync({Message(topicA, Bytes{'a', 'r', '1'}),
                                      Message(topicA, Bytes{'a', 'r', '2'})},
                                     mqB, refused, 5000);
            const bool done = refused->waitDone(10000);
            check("S4c 批量异步共用同一处文案与同一条拒绝路径",
                  done && refused->exceptionCount == 1 && refused->message == asyncWording,
                  "msg=" + refused->message);
        }
        {
            auto singleOk = std::make_shared<SendLatch>();
            auto batchOk = std::make_shared<SendLatch>();
            producer->sendAsync(keyed(topicA, Bytes{'a', 'o'}, "pinned-async-single-" + tag), mqA,
                                singleOk, 5000);
            producer->sendBatchAsync({keyed(topicA, Bytes{'a', 'b', '1'}, "pinned-async-b1-" + tag),
                                       keyed(topicA, Bytes{'a', 'b', '2'}, "pinned-async-b2-" + tag)},
                                      mqA, batchOk, 5000);
            const bool okSingle = singleOk->waitDone(10000);
            const bool okBatch = batchOk->waitDone(10000);
            check("S4d 两条放行腿都 SEND_OK",
                  okSingle && singleOk->successCount == 1 && okBatch &&
                      batchOk->successCount == 1,
                  "single_err=" + singleOk->message + " batch_err=" + batchOk->message);
            int64_t offAfterAsync = -1;
            waitUntil(
                [&] {
                    offAfterAsync = producer->maxOffset(mqA);
                    return offAfterAsync >= offAfterS1 + 3;
                },
                10000);
            offAfterAsync = producer->maxOffset(mqA);
            check("S4e 异步放行腿同样真落库（单条 1 + 批量子消息 2，maxOffset 再涨 3）",
                  offAfterAsync == offAfterS1 + 3, "maxOffset=" + std::to_string(offAfterAsync));

            // ---------------- S5 单向：Java 有意没有守卫 ----------------
            const std::string kOw = "pinned-oneway-" + tag;
            producer->sendOneway(keyed(topicA, Bytes{'o', 'w'}, kOw), mqB);
            int64_t offAfterOw = -1;
            waitUntil(
                [&] {
                    offAfterOw = producer->maxOffset(mqA);
                    return offAfterOw >= offAfterAsync + 1;
                },
                10000);
            offAfterOw = producer->maxOffset(mqA);
            check("S5a 单向定点没有守卫：报文按 msg 自己的 topic 落进 A",
                  offAfterOw == offAfterAsync + 1,
                  "A maxOffset=" + std::to_string(offAfterOw));
            const int64_t bNow = producer->maxOffset(mqB);
            check("S5b 目标队列所在的 B 一条都没有（是 Java 的口子，不是漏发）", bNow == 0,
                  "B maxOffset=" + std::to_string(bNow));

            // ---------------- S6 正腿收尾：消息一条不少 ----------------
            const std::set<std::string> expected = {
                kSingle, kB1, kB2, "pinned-async-single-" + tag, "pinned-async-b1-" + tag,
                "pinned-async-b2-" + tag, kOw};
            const bool all = waitUntil(
                [&] {
                    const std::vector<std::string> got = sink.snapshot();
                    for (const std::string& k : expected) {
                        if (std::find(got.begin(), got.end(), k) == got.end()) return false;
                    }
                    return true;
                },
                40000);
            const std::vector<std::string> got = sink.snapshot();
            check("S6 正腿消息一条不少地被消费到", all, "received=" + join(got));
        }
    } catch (const std::exception& e) {
        check("用例执行中未预期异常", false, e.what());
    }

    if (consumer) consumer->shutdown();
    if (nsProducer) nsProducer->shutdown();
    if (producer) producer->shutdown();
    try {
        admin.deleteTopic(topicA, topicA);
        admin.deleteTopic(topicB, topicB);
        admin.deleteTopic(wTopic, wTopic);
    } catch (const std::exception& e) {
        std::printf("  [WARN] 清理 topic 失败: %s\n", e.what());
    }
    admin.shutdown();

    std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
