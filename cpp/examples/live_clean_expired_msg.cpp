// Live-cluster verification of clean-expired-msg and redelivery (assert every scenario A0-A5 one by one).
//
// The offline unit test (tests/test_clean_expired_msg.cpp) locks down the **decision rules** (selection/threshold/cap/removal gate);
// what this locks down are two things offline cannot prove, which need a real cluster:
//   A. **the sweep really fires**: with a listener blocked and not returning beyond consumeTimeout minutes, the sweep
//      thread must sendMessageBack this message (delayLevel 3) -- along the whole chain the client must neither lose it nor deliver it twice.
//   B. **redelivery really returns to the broker**: the pull loop of %RETRY%<group> is independent of the blocked consumer thread, and the message must
//      reappear in the local buffer and be delivered a second time with reconsumeTimes=1 (the broker-side counter).
//
// This path fails **silently** when broken: the stuck message pins the queue offset and the dispatch loop together, with no visible
// exception or timeout; the only way to infer it is "the message was sent but a second delivery never arrives". So this must run on a real cluster and produce the
// full evidence chain of "hang -> sweep -> redelivery arrives"; "the client raised no error" proves nothing by itself.
//
//
// Usage: ./rmq_live_clean_expired_msg 127.0.0.1:9876   (about 4 minutes, waits for two sweep cycles)
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/route.h"

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

bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 500) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

// Hangs without returning on the first delivery; after release, returns CONSUME_SUCCESS normally.
class HungListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        bool first = false;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            ++calls_;
            first = (calls_ == 1);
            for (const MessageExt& m : msgs) {
                arrivals_.emplace_back(std::string(m.body),
                                       m.getReconsumeTimes(),
                                       static_cast<int64_t>(nowMs()));
            }
        }
        if (first) {
            std::unique_lock<std::mutex> lk(mtx_);
            firstSeen_ = true;
            cv_.notify_all();
            // Hang window: wait for the sweep to act plus release by the verifying script (300 s cap as fallback so a crashed script cannot wedge the thread)
            releaseCv_.wait_for(lk, std::chrono::seconds(300), [this] { return release_; });
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    bool waitFirst(int64_t timeoutMs) {
        std::unique_lock<std::mutex> lk(mtx_);
        return cv_.wait_for(lk, std::chrono::milliseconds(timeoutMs), [this] { return firstSeen_; });
    }

    void release() {
        std::lock_guard<std::mutex> lk(mtx_);
        release_ = true;
        releaseCv_.notify_all();
    }

    int calls() {
        std::lock_guard<std::mutex> lk(mtx_);
        return calls_;
    }

    // Arrival records of a body as [(reconsumeTimes, timestamp)]
    std::vector<std::pair<int32_t, int64_t>> times(const std::string& body) {
        std::lock_guard<std::mutex> lk(mtx_);
        std::vector<std::pair<int32_t, int64_t>> out;
        for (const auto& a : arrivals_) {
            if (std::get<0>(a) == body) out.emplace_back(std::get<1>(a), std::get<2>(a));
        }
        return out;
    }

private:
    std::mutex mtx_;
    std::condition_variable cv_;
    std::condition_variable releaseCv_;
    bool firstSeen_ = false;
    bool release_ = false;
    int calls_ = 0;
    std::vector<std::tuple<std::string, int32_t, int64_t>> arrivals_;
};

bool containsBody(const std::vector<MessageExt>& msgs, const std::string& body) {
    for (const MessageExt& m : msgs) {
        if (m.body == body) return true;
    }
    return false;
}

struct Fixture {
    std::string namesrv;
    std::string stamp;
    std::string brokerAddr;
    DefaultMQAdminExt admin;
    std::shared_ptr<DefaultMQProducer> producer;

    void start(const std::string& ns) {
        namesrv = ns;
        stamp = std::to_string(nowMs() % 1000000);
        admin.setNamesrvAddr(ns);
        admin.start();
        ClusterInfo cluster = admin.fetchBrokerClusterInfo();
        const std::vector<std::string> addrs = cluster.getBrokerAddrs();
        if (addrs.empty()) {
            throw std::runtime_error("no broker registered on the name server");
        }
        brokerAddr = addrs[0];
        producer = std::make_shared<DefaultMQProducer>("GID_cpp_live_ce_pg_" + stamp);
        producer->setNamesrvAddr(ns);
        producer->setInstanceName("cpp-live-ce-prod-" + stamp);
        producer->start();
    }

    std::string topic() const { return "CppLiveCe" + stamp; }

    // Sum of committed offsets (the business topic has only 1 queue; query every queue from the route and
    // do not force the assumption that 'the main topic has 4 queues'). No record on the broker -> return -1.
    int64_t committedOffset(const std::string& group, const std::string& topicName) {
        TopicRouteData route = admin.examineTopicRoute(topicName);
        const std::vector<MessageQueue> mqs = route.getAllMessageQueue(topicName);
        int64_t total = 0;
        for (const MessageQueue& mq : mqs) {
            int64_t off = 0;
            bool found = false;
            try {
                found = admin.client().queryConsumerOffset(group, mq, off, 5000, brokerAddr,
                                                           /*setZeroIfNotFound=*/false);
            } catch (const std::exception&) {
                found = false;
            }
            total += found ? off : -1;
        }
        return mqs.empty() ? -1 : total;
    }

    void shutdown() {
        if (producer) {
            try {
                producer->shutdown();
            } catch (const std::exception&) {
            }
        }
        try {
            admin.shutdown();
        } catch (const std::exception&) {
        }
    }
};

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_clean_expired_msg <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    Fixture fx;
    try {
        fx.start(nsAddr);
    } catch (const std::exception& e) {
        check("cluster reachability probe", false, e.what());
        return 1;
    }
    check("cluster reachability probe", true, "broker=" + fx.brokerAddr);

    const std::string topic = fx.topic();
    const std::string group = "GID_cpp_live_ce_" + fx.stamp;
    const std::string retryTopic = MixAll::getRetryTopic(group);

    std::shared_ptr<DefaultMQPushConsumer> consumer;
    auto listener = std::make_shared<HungListener>();
    const std::string body = "ce-cpp-1";

    try {
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic, 1);
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::printf("topic=%s group=%s\n", topic.c_str(), group.c_str());

        consumer = std::make_shared<DefaultMQPushConsumer>(group);
        consumer->setNamesrvAddr(nsAddr);
        consumer->setInstanceName("cpp-live-ce-" + fx.stamp);
        consumer->setMessageListener(listener);
        consumer->setConsumeTimeout(1);
        consumer->subscribe(topic, "*");
        consumer->start();

        // ---------- A0 business queue (exclude %RETRY%) ----------
        std::string bizKey;
        const bool assigned = waitUntil(
            [&] {
                for (const std::string& k : consumer->assignedQueueKeys()) {
                    if (k.rfind("%RETRY%", 0) != 0) {
                        bizKey = k;
                        return true;
                    }
                }
                return false;
            },
            30000);
        check("A0-business queue assigned (%RETRY% queues do not count)", assigned, "key=" + bizKey);
        if (!assigned) {
            throw std::runtime_error("business queue not assigned");
        }

        // ---------- A1 baseline: the first delivery hangs ----------
        Message msg(topic, body);
        fx.producer->send(msg);
        const bool firstOk = listener->waitFirst(30000);
        check("A1-first delivery arrived within 30 s and hung", firstOk, "calls=" + std::to_string(listener->calls()));
        const auto first = listener->times(body);
        check("A1-first delivery has reconsumeTimes=0", !first.empty() && first[0].first == 0,
              "times=" + std::to_string(first.empty() ? -1 : first[0].first));
        bool registered = false;
        std::string stampSeen;
        for (const MessageExt& m : consumer->processQueueEntries(bizKey)) {
            if (m.body == body) {
                registered = true;
                stampSeen = m.getProperty(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP);
            }
        }
        check("A1-message registered while the listener hangs (still in its hands)", registered);
        check("A1-registered copy carries CONSUME_START_TIME of this round (the sweep uses it to detect expiry)", !stampSeen.empty(),
              "stamp=" + stampSeen);

        // ---------- A2 sweep hits: the listener still hangs, but the registry no longer holds the message ----------
        const int64_t t0 = nowMs();
        const bool swept = waitUntil(
            [&] { return !containsBody(consumer->processQueueEntries(bizKey), body); },
            210000, 1000);
        const double elapsed = static_cast<double>(nowMs() - t0) / 1000.0;
        check("A2-sweep took the message away while the listener was still hung (about start+120 s)", swept,
              "elapsed=" + std::to_string(elapsed) + "s calls=" +
                  std::to_string(listener->calls()));
        check("A2-removal happened later than one sweep threshold (>60 s, rules out other paths acting)", elapsed > 60.0,
              "elapsed=" + std::to_string(elapsed) + "s");

        // ---------- A3 the back-send really reached the broker: it shows up in the %RETRY% buffer ----------
        // The hung listener occupies the dispatch thread, so the redelivered message can only sit in the local buffer of the %RETRY% queue.
        std::string retryKeyFound;
        const bool back = waitUntil(
            [&] {
                for (const std::string& k : consumer->assignedQueueKeys()) {
                    if (k.rfind("%RETRY%", 0) != 0) continue;
                    retryKeyFound = k;
                    if (containsBody(consumer->pendingMessages(k), body)) return true;
                }
                return false;
            },
            40000, 1000);
        check("A3-redelivered message appears in the local buffer of " + retryTopic +
                  " (the broker really received sendMessageBack)",
              back, "retryKey=" + retryKeyFound);

        // ---------- A4 release: the redelivery is consumed again ----------
        listener->release();
        const bool second = waitUntil([&] { return listener->times(body).size() >= 2; }, 60000, 1000);
        auto times = listener->times(body);
        check("A4-reconsumed after release (reconsumeTimes=1)", second,
              "times=" + std::to_string(times.size()));
        check("A4-second delivery has reconsumeTimes=1 (broker-side redelivery counter)",
              times.size() >= 2 && times[1].first == 1,
              "times=" + std::to_string(times.size() >= 2 ? times[1].first : -1));
        check("A4-the same body arrives exactly twice overall (one clean redelivery, no duplicate delivery)", times.size() == 2,
              "times=" + std::to_string(times.size()));
        check("A4-second delivery is more than 60 s after the first (not a redelivery caused by the listener itself)",
              times.size() >= 2 && (times[1].second - times[0].second) > 60000,
              "gap=" + std::to_string(times.size() >= 2
                                          ? (times[1].second - times[0].second) / 1000
                                          : -1) +
                  "s");

        // ---------- A5 offset wrap-up ----------
        const bool offsetOk = waitUntil([&] { return fx.committedOffset(group, topic) == 1; },
                                        30000, 1000);
        check("A5-business queue offset advances to 1 after the hung listener returns (removeMessage includes swept entries)",
              offsetOk, "offset=" + std::to_string(fx.committedOffset(group, topic)));
    } catch (const std::exception& e) {
        check("the verification threw an exception", false, e.what());
    }

    // ---------------- cleanup ----------------
    listener->release();
    if (consumer) {
        try {
            consumer->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (consumer shutdown failed: %s)\n", e.what());
        }
    }
    try {
        fx.admin.deleteTopic(topic);
        std::printf("    (deleteTopic(%s) OK)\n", topic.c_str());
    } catch (const std::exception& e) {
        std::printf("    (deleteTopic(%s) failed: %s)\n", topic.c_str(), e.what());
    }
    try {
        fx.admin.deleteSubscriptionGroup(fx.brokerAddr, group, true);
    } catch (const std::exception& e) {
        std::printf("    (deleteSubscriptionGroup(%s) failed: %s)\n", group.c_str(), e.what());
    }
    fx.shutdown();

    std::printf("\nCleanExpiredMsg(cpp): PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
