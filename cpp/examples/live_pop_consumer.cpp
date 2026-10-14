// Difference from rmq_live_pop (protocol plumbing): this verifies the **consume loop** --
//   POP pops -> deliver to the listener -> ack on success, extend the invisible time on failure -> no duplicate delivery.
// The consume loop neither queries nor commits consumer offsets; progress is tracked entirely by the broker's checkpoint.
//
// Scenarios:
//   S1 POP consuming: start the consumer -> send 12 messages -> all received, no duplicates, body sets match
//   S2 ack really works: after receiving them all keep observing for a while (> invisibleTime) -> no redelivery expected
//   S3 RECONSUME_LATER: the listener keeps returning RECONSUME_LATER -> redelivery per delay level
//   S4 multi-queue: messages really land on several queues and all are consumed (POP pops queue by queue)
//   S5 pull statistics: the POP path must also record pullRT/pullTPS into the status table (307 statusTable non-zero)
//
// ⚠ S2's observation window must be > popInvisibleTime, otherwise even "the ack never went out" would show no duplicate delivery
//   (the message has not reached its revival time yet) -- this is the false green most easily disguised as a pass.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;
std::mutex gMtx;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(),
                    detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(),
                    detail.empty() ? "" : ("  " + detail).c_str());
    }
}

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

int64_t stamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool waitUntil(const std::function<bool()>& pred, int32_t timeoutMs) {
    const int64_t deadline = stamp() + timeoutMs;
    while (stamp() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return pred();
}

void prepareTopic(DefaultMQProducer& producer, const std::string& topic, int32_t queueNum) {
    try {
        producer.createTopic("TBW102", topic, queueNum);
    } catch (const std::exception& e) {
        std::printf("  !! createTopic(%s) failed: %s\n", topic.c_str(), e.what());
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_pop_consumer <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    const std::string pfx = "PopConsCpp_" + std::to_string(stamp() % 1000000);
    const std::string topic = pfx + "_T";
    const std::string group = pfx + "_G";
    const std::string topicLater = pfx + "_Later";
    const std::string groupLater = pfx + "_GLater";
    const std::string topicStats = pfx + "_Stats";
    const std::string groupStats = pfx + "_GStats";
    const int32_t nMsg = 12;

    DefaultMQProducer prep(pfx + "_prep");
    prep.setNamesrvAddr(nsAddr);
    prep.start();
    prepareTopic(prep, topic, 4);
    prepareTopic(prep, topicLater, 4);
    prepareTopic(prep, topicStats, 4);
    prep.shutdown();

    // ---------------- S1 / S2 / S4: normal consuming + ack ----------------
    {
        std::printf("=== S1 POP consuming (all received + no duplicates) ===\n");
        std::mutex mtx;
        std::vector<std::string> got;
        std::set<int32_t> queues;

        class L : public MessageListenerConcurrently {
        public:
            L(std::mutex& m, std::vector<std::string>& got, std::set<int32_t>& q)
                : m_(m), got_(got), q_(q) {}
            ConsumeConcurrentlyStatus consumeMessage(
                const std::vector<MessageExt>& msgs, ConsumeConcurrentlyContext&) override {
                std::lock_guard<std::mutex> lk(m_);
                for (const MessageExt& m : msgs) {
                    got_.push_back(bodyOf(m));
                    q_.insert(m.queueId);
                }
                return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            std::mutex& m_;
            std::vector<std::string>& got_;
            std::set<int32_t>& q_;
        };

        auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
        consumer->setNamesrvAddr(nsAddr);
        consumer->setPopMode(true);
        consumer->setConsumeThreadNums(4);
        consumer->setConsumeMessageBatchMaxSize(4);
        // ⚠ deliberately squeezed to 10 s: so that "no ack -> revival redelivery when invisibleTime expires" has time to surface inside the observation window
        consumer->setPopInvisibleTime(10000);
        consumer->setMessageListener(std::make_shared<L>(mtx, got, queues));
        consumer->subscribe(topic);
        // Start the consumer before sending the messages (see the project convention)
        consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(1));

        DefaultMQProducer producer(pfx + "_pg");
        producer.setNamesrvAddr(nsAddr);
        producer.start();
        std::set<std::string> sent;
        for (int32_t i = 0; i < nMsg; i++) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "pop-cons-%02d", i);
            Message m(topic, Bytes(buf, buf + std::strlen(buf)));
            producer.send(m);
            sent.insert(std::string(buf));
        }
        std::printf("  sent %d msgs\n", nMsg);

        bool all = waitUntil(
            [&]() {
                std::lock_guard<std::mutex> lk(mtx);
                return static_cast<int32_t>(got.size()) >= nMsg;
            },
            30000);
        // Must be > popInvisibleTime (10 s), otherwise a missing ack would show no duplicate delivery
        std::this_thread::sleep_for(std::chrono::seconds(16));

        std::set<std::string> gotSet;
        {
            std::lock_guard<std::mutex> lk(mtx);
            gotSet.insert(got.begin(), got.end());
            check("S1a all messages were consumed", all && static_cast<int32_t>(got.size()) >= nMsg,
                  "received=" + std::to_string(got.size()) + "/" + std::to_string(nMsg));
            check("S1b the body set matches what was sent", gotSet == sent,
                  "got=" + std::to_string(gotSet.size()) + " sent=" + std::to_string(sent.size()));
            check("S2a no duplicate delivery", gotSet.size() == got.size(),
                  "received=" + std::to_string(got.size())
                      + " unique=" + std::to_string(gotSet.size()));
            check("S2b no new deliveries during the observation window", static_cast<int32_t>(got.size()) == nMsg,
                  "received=" + std::to_string(got.size()));
            check("S4 messages spread over multiple queues and all were consumed", queues.size() > 1,
                  "queues=" + std::to_string(queues.size()));
        }
        consumer->shutdown();
        producer.shutdown();
    }

    // ---------------- S3: RECONSUME_LATER -> redelivery after a delay ----------------
    {
        std::printf("=== S3 RECONSUME_LATER -> redelivery after a delay ===\n");
        std::mutex mtx;
        std::vector<std::string> keys;

        class L2 : public MessageListenerConcurrently {
        public:
            L2(std::mutex& m, std::vector<std::string>& k) : m_(m), k_(k) {}
            ConsumeConcurrentlyStatus consumeMessage(
                const std::vector<MessageExt>& msgs, ConsumeConcurrentlyContext&) override {
                std::lock_guard<std::mutex> lk(m_);
                for (const MessageExt& m : msgs) {
                    k_.push_back(m.getKeys().empty() ? m.msgId : m.getKeys());
                }
                return ConsumeConcurrentlyStatus::RECONSUME_LATER;
            }

        private:
            std::mutex& m_;
            std::vector<std::string>& k_;
        };

        auto c2 = std::make_shared<DefaultMQPushConsumer>(groupLater);
        c2->setNamesrvAddr(nsAddr);
        c2->setPopMode(true);
        c2->setConsumeThreadNums(2);
        c2->setPopInvisibleTime(5000);
        c2->setMessageListener(std::make_shared<L2>(mtx, keys));
        c2->subscribe(topicLater);
        c2->start();
        std::this_thread::sleep_for(std::chrono::seconds(1));

        DefaultMQProducer p2(pfx + "_pg2");
        p2.setNamesrvAddr(nsAddr);
        p2.start();
        for (int32_t i = 0; i < 3; i++) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "later-%d", i);
            Message m(topicLater, Bytes(buf, buf + std::strlen(buf)));
            m.setKeys(std::string("pl") + std::to_string(i));
            p2.send(m);
        }

        bool first = waitUntil(
            [&]() {
                std::lock_guard<std::mutex> lk(mtx);
                return keys.size() >= 3;
            },
            30000);
        size_t firstRound = 0;
        {
            std::lock_guard<std::mutex> lk(mtx);
            firstRound = keys.size();
        }
        check("S3a the first round delivered 3 messages", first && firstRound >= 3, "count=" + std::to_string(firstRound));

        // ⚠ the assertion is "every message was redelivered", not "3 more deliveries overall": judged by raw count,
        // one message redelivered 3 times while another never came back would still pass.
        auto redeliveredKinds = [&]() {
            std::map<std::string, int32_t> seen;
            for (const std::string& k : keys) ++seen[k];
            size_t n = 0;
            for (const auto& e : seen) {
                if (e.second >= 2) ++n;
            }
            return n;
        };

        bool again = waitUntil(
            [&]() {
                std::lock_guard<std::mutex> lk(mtx);
                return redeliveredKinds() >= 3;
            },
            40000);
        std::string joined;
        size_t kinds = 0;
        {
            std::lock_guard<std::mutex> lk(mtx);
            kinds = redeliveredKinds();
            for (const std::string& k : keys) {
                joined += (joined.empty() ? "" : ",") + k;
            }
        }
        check("S3b every failed message was redelivered (extending the invisible time works)", again,
              "first=" + std::to_string(firstRound) + " redelivered=" + std::to_string(kinds)
                  + " deliveries=" + joined);
        c2->shutdown();
        p2.shutdown();
    }

    {
        std::printf("=== S5 the POP loop writes pullRT/pullTPS into the 307 status table ===\n");
        std::mutex mtx5;
        int32_t delivered5 = 0;

        class L5 : public MessageListenerConcurrently {
        public:
            L5(std::mutex& m, int32_t& n) : m_(m), n_(n) {}
            ConsumeConcurrentlyStatus consumeMessage(
                const std::vector<MessageExt>& msgs, ConsumeConcurrentlyContext&) override {
                std::lock_guard<std::mutex> lk(m_);
                n_ += static_cast<int32_t>(msgs.size());
                return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            std::mutex& m_;
            int32_t& n_;
        };
        auto l5 = std::make_shared<L5>(mtx5, delivered5);

        auto c5 = std::make_shared<DefaultMQPushConsumer>(groupStats);
        c5->setNamesrvAddr(nsAddr);
        c5->setPopMode(true);
        c5->setConsumeThreadNums(2);
        c5->setMessageListener(l5);
        c5->subscribe(topicStats);
        c5->start();
        std::this_thread::sleep_for(std::chrono::seconds(1));

        DefaultMQProducer p5(pfx + "_pg5");
        p5.setNamesrvAddr(nsAddr);
        p5.start();
        const int64_t began5 = stamp();
        int32_t rounds = 0;
        while (stamp() - began5 < 26000) {  // about 3 sampling periods
            for (int32_t i = 0; i < 4; i++) {
                char buf[32];
                std::snprintf(buf, sizeof(buf), "s5-%d-%d", rounds, i);
                Message m(topicStats, Bytes(buf, buf + std::strlen(buf)));
                p5.send(m);
            }
            rounds++;
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
        const int32_t sent5 = rounds * 4;

        DefaultMQAdminExt admin(pfx + "_admin5");
        admin.setNamesrvAddr(nsAddr);
        admin.start();
        // 307 routed through the broker to the consumer itself; a failure is treated as an empty table (the assertions below make it visible)
        JsonValue status = JsonValue::makeObject();
        try {
            ConsumerRunningInfo ri = admin.examineConsumerRunningInfo(groupStats, c5->clientId());
            status = ri.statusTable;
        } catch (const std::exception& e) {
            std::printf("  !! 307 failed: %s\n", e.what());
        }
        const JsonValue& cs = status.get(topicStats);
        const double pullRT = cs.get("pullRT").doubleValue();
        const double pullTPS = cs.get("pullTPS").doubleValue();
        const double consumeOKTPS = cs.get("consumeOKTPS").doubleValue();
        std::printf("  sent=%d pullRT=%.2f pullTPS=%.4f consumeOKTPS=%.4f\n", sent5, pullRT,
                    pullTPS, consumeOKTPS);
        check("S5a pullRT is non-zero (every POP FOUND records one pull duration)", pullRT > 0.0,
              "pullRT=" + std::to_string(pullRT));
        check("S5b pullTPS is non-zero (counted by the number of popped messages)", pullTPS > 0.0,
              "pullTPS=" + std::to_string(pullTPS));
        // The pull cell and the consume cell report independently: only consumeOKTPS having a value while pull* are all 0 is exactly
        // the signature of a POP loop that forgets the pull statistics.
        check("S5c consumeOKTPS is non-zero at the same time (both cells reported independently)", consumeOKTPS > 0.0,
              "consumeOKTPS=" + std::to_string(consumeOKTPS));
        // A non-zero status table only proves "the counters were called"; we must also confirm the traffic behind these statistics really got
        // consumed: otherwise pullTPS could be inflated by repeatedly popping messages that are never acked -- the dashboard looks good while it spins.
        bool consumed = waitUntil(
            [&]() {
                std::lock_guard<std::mutex> lk(mtx5);
                return delivered5 >= sent5;
            },
            20000);
        {
            std::lock_guard<std::mutex> lk(mtx5);
            check("S5d the traffic behind the status table was really consumed", consumed,
                  "delivered=" + std::to_string(delivered5) + " sent=" + std::to_string(sent5));
        }
        c5->shutdown();
        p5.shutdown();
        admin.shutdown();
    }

    std::printf("########################################\n");
    std::printf("PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
