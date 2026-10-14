// Live-cluster verification of the pinned (specified-queue) send guard (assert every scenario S1..S6 one by one).
//
// Prerequisites: NameServer + Broker already running, with ``autoCreateTopicEnable=true``.
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

/// Terminal state of one async send.
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

/// Collector of the keys that were consumed.
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

/// 6*2 = one id range; the stamp uses the low digits of epoch millis, unique enough that this run has no collisions.
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
            check("cluster reachability probe", false, "no broker registered on the name server");
            admin.shutdown();
            return 1;
        }
        brokerAddr = addrs[0];
    } catch (const std::exception& e) {
        check("cluster reachability probe", false, std::string("fetchBrokerClusterInfo: ") + e.what());
        admin.shutdown();
        return 1;
    }
    check("cluster reachability probe", true, "broker=" + brokerAddr);

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

        // ---------------- S0 routes + consumer in place ----------------
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
        check("S0 routes of both topics available (the negative legs use real queues, so a rejection must be about the topic, not the address)",
              routed && mqA.brokerName == mqB.brokerName,
              "A=" + mqA.brokerName + "/" + std::to_string(mqA.queueId) + " B=" + mqB.brokerName +
                  "/" + std::to_string(mqB.queueId));
        if (!routed) {
            check("S0 prerequisite failed, later scenarios cannot continue", false);
            producer->shutdown();
            admin.shutdown();
            std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
            return 1;
        }

        // Start the consumer first (CONSUME_FROM_FIRST_OFFSET); do not race the rebalance moment with the sends
        consumer = std::make_shared<DefaultMQPushConsumer>(group);
        consumer->setNamesrvAddr(nsAddr);
        consumer->setInstanceName("pin-guard-live-" + tag);
        consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        consumer->setMessageListener(std::make_shared<KeyListener>(sink));
        consumer->subscribe(topicA, "*");
        consumer->start();

        // ---------------- S1 positive legs: pinned sends onto real routed queues ----------------
        const std::string kSingle = "pinned-single-" + tag;
        const std::string kB1 = "pinned-b1-" + tag;
        const std::string kB2 = "pinned-b2-" + tag;
        {
            const SendResult r = producer->send(keyed(topicA, Bytes{'s', '1'}, kSingle), mqA, 5000);
            check("S1a sync single pinned send SEND_OK and lands on queue 0",
                  r.sendStatus == SendStatus::SEND_OK && r.messageQueue.queueId == mqA.queueId &&
                      r.messageQueue.topic == topicA,
                  "status=" + std::to_string(static_cast<int>(r.sendStatus)) +
                      " mq=" + r.messageQueue.topic + "/" + std::to_string(r.messageQueue.queueId));
        }
        {
            const SendResult r = producer->sendBatch(
                {keyed(topicA, Bytes{'b', '1'}, kB1), keyed(topicA, Bytes{'b', '2'}, kB2)},
                mqA, 5000);
            check("S1b sync batch pinned send SEND_OK and the same queue",
                  r.sendStatus == SendStatus::SEND_OK && r.messageQueue.queueId == mqA.queueId,
                  "status=" + std::to_string(static_cast<int>(r.sendStatus)));
        }
        // A batch lands on the consume queue **sub-message by sub-message** (the broker splits the inner-batch when writing) => 2 sub-messages advance the offset by 2
        int64_t offAfterS1 = -1;
        waitUntil(
            [&] {
                offAfterS1 = producer->maxOffset(mqA);
                return offAfterS1 >= 3;
            },
            10000);
        offAfterS1 = producer->maxOffset(mqA);
        check("S1c all three sub-messages really persisted (maxOffset = 1 single + 2 batch sub-messages)", offAfterS1 == 3,
              "maxOffset=" + std::to_string(offAfterS1));

        // ---------------- S2 negative legs: sync rejection, local-side, no trace ----------------
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
            check("S2a sync single send rejected (exact protocol wording)", threw && msg == syncWording,
                  std::to_string(ms) + "ms " + (threw ? msg : "no exception thrown"));
            check("S2b rejection is local-side: sub-millisecond (not a timeout, not a broker remark)",
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
            check("S2c sync batch shares the same guard site and the same wording",
                  threw && msg == syncWording && ms < localBudgetMs,
                  std::to_string(ms) + "ms " + (threw ? msg : "no exception thrown"));
        }
        {
            const int64_t aNow = producer->maxOffset(mqA);
            const int64_t bNow = producer->maxOffset(mqB);
            check("S2d wire-level counter-evidence: A's maxOffset did not move and B received nothing",
                  aNow == offAfterS1 && bNow == 0,
                  "A=" + std::to_string(aNow) + " B=" + std::to_string(bNow));
        }

        // ---------------- S3 namespace: wrap is idempotent, only genuinely different names are rejected ----------------
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
        check("S3a route of the prefixed topic available", nsRouted, "topic=" + wTopic);
        {
            const SendResult r =
                nsProducer->send(keyed(topicA, Bytes{'n', '1'}, "pinned-ns-q-" + tag), mqw, 5000);
            check("S3b queue topic already carries the ns prefix: wrap is idempotent, no false rejection",
                  r.sendStatus == SendStatus::SEND_OK,
                  "status=" + std::to_string(static_cast<int>(r.sendStatus)));
        }
        {
            const SendResult r =
                nsProducer->send(keyed(wTopic, Bytes{'n', '2'}, "pinned-ns-m-" + tag), mqw, 5000);
            check("S3c a message topic carrying the prefix itself also passes",
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
            check("S3d rejected only after switching to the ns2% prefix (control leg: what is rejected is the name, not 'having a prefix')",
                  threw && msg == syncWording && ms < localBudgetMs,
                  std::to_string(ms) + "ms " + (threw ? msg : "no exception thrown"));
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
            check("S3e both passing legs really land in ns1%topic (maxOffset=2)", nsOff == 2,
                  "maxOffset=" + std::to_string(nsOff));
        }

        // ---------------- S4 async: the callback carries the async-site wording ----------------
        {
            auto refused = std::make_shared<SendLatch>();
            producer->sendAsync(keyed(topicA, Bytes{'a', 'r'}, "pinned-async-refused-" + tag), mqB,
                                refused, 5000);
            const bool done = refused->waitDone(10000);
            check("S4a single async rejection goes through the callback with the async-site wording",
                  done && refused->exceptionCount == 1 && refused->successCount == 0 &&
                      refused->message == asyncWording,
                  "msg=" + refused->message);
            const int64_t aNow = producer->maxOffset(mqA);
            check("S4b maxOffset still unchanged after the rejection (async did not sneak a send)", aNow == offAfterS1,
                  "maxOffset=" + std::to_string(aNow));
        }
        {
            auto refused = std::make_shared<SendLatch>();
            producer->sendBatchAsync({Message(topicA, Bytes{'a', 'r', '1'}),
                                      Message(topicA, Bytes{'a', 'r', '2'})},
                                     mqB, refused, 5000);
            const bool done = refused->waitDone(10000);
            check("S4c batch async shares the same wording and the same rejection path",
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
            check("S4d both passing legs return SEND_OK",
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
            check("S4e async passing legs also really persist (1 single + 2 batch sub-messages, maxOffset advances by 3 again)",
                  offAfterAsync == offAfterS1 + 3, "maxOffset=" + std::to_string(offAfterAsync));

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
            check("S5a oneway pinned has no guard: the message lands in A under the msg's own topic",
                  offAfterOw == offAfterAsync + 1,
                  "A maxOffset=" + std::to_string(offAfterOw));
            const int64_t bNow = producer->maxOffset(mqB);
            check("S5b queue B, the pinned target, received nothing (a deliberate design gap, not a lost send)", bNow == 0,
                  "B maxOffset=" + std::to_string(bNow));

            // ---------------- S6 positive-leg wrap-up: not one message missing ----------------
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
            check("S6 every positive-leg message was consumed", all, "received=" + join(got));
        }
    } catch (const std::exception& e) {
        check("unexpected exception while running the case", false, e.what());
    }

    if (consumer) consumer->shutdown();
    if (nsProducer) nsProducer->shutdown();
    if (producer) producer->shutdown();
    try {
        admin.deleteTopic(topicA, topicA);
        admin.deleteTopic(topicB, topicB);
        admin.deleteTopic(wTopic, wTopic);
    } catch (const std::exception& e) {
        std::printf("  [WARN] topic cleanup failed: %s\n", e.what());
    }
    admin.shutdown();

    std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
