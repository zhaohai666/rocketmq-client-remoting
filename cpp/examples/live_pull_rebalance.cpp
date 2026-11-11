// Live verification of the pull-side rebalance (Java DefaultMQPullConsumer.enableRebalance),
// against a real RocketMQ 5.5.1 cluster.
//
// Java's pull consumer rebalances: DefaultMQPullConsumer.java:97 defaults enableRebalance
// to true, and the shared RebalanceService drives tryRebalance() every 20s, calling
// MessageQueueListener.messageQueueChanged(topic, mqAll, mqDivided) when the allocation
// changed. This probe proves against a real broker:
//
//   L1  two instances of one group split a multi-queue topic -- disjoint shares whose
//       union is the whole topic (only possible if both read the same broker-side
//       consumer list and run the same allocation formula);
//   L2  the listener really fired with mqAll = the topic's full queue set;
//   L3  enableRebalance=false stops the loop (no callbacks, empty table) while
//       fetchMessageQueuesInBalance keeps computing on the spot.
//
// Single broker, one topic with 8 queues -- enough for all three: the split is across
// queues within one broker, not across brokers.
//
// Usage: ./live_pull_rebalance <namesrv>        (e.g. 127.0.0.1:9876)
// Exits non-zero on any FAIL. Every run uses a fresh topic and group.
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
#include "rocketmq/client/pull_consumer.h"

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
    std::printf("  %s %s%s\n", ok ? "[PASS]" : "[FAIL]", name.c_str(),
                detail.empty() ? "" : ("  " + detail).c_str());
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 200) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

// Records messageQueueChanged(topic, mqAll, mqDivided) calls. The rebalance fires the
// listener on its own thread, so the recorder takes a lock and stores copies.
class Recorder : public MessageQueueListener {
public:
    void messageQueueChanged(const std::string& topic, const std::vector<MessageQueue>& mqAll,
                             const std::vector<MessageQueue>& mqDivided) override {
        std::lock_guard<std::mutex> lk(mtx_);
        calls_ += 1;
        lastTopic_ = topic;
        lastAll_ = mqAll;
        lastDivided_ = mqDivided;
    }

    int64_t count() {
        std::lock_guard<std::mutex> lk(mtx_);
        return calls_;
    }

    int64_t lastAllSize() {
        std::lock_guard<std::mutex> lk(mtx_);
        return static_cast<int64_t>(lastAll_.size());
    }

    int64_t lastDividedSize() {
        std::lock_guard<std::mutex> lk(mtx_);
        return static_cast<int64_t>(lastDivided_.size());
    }

    std::set<std::string> lastDividedIds() {
        std::lock_guard<std::mutex> lk(mtx_);
        return idSet(lastDivided_);
    }

private:
    static std::set<std::string> idSet(const std::vector<MessageQueue>& mqs) {
        std::set<std::string> out;
        for (const MessageQueue& mq : mqs) {
            out.insert(mq.brokerName + "#" + std::to_string(mq.queueId));
        }
        return out;
    }

    std::mutex mtx_;
    int64_t calls_ = 0;
    std::string lastTopic_;
    std::vector<MessageQueue> lastAll_;
    std::vector<MessageQueue> lastDivided_;
};

std::set<std::string> idSetOf(const std::vector<MessageQueue>& mqs) {
    std::set<std::string> out;
    for (const MessageQueue& mq : mqs) {
        out.insert(mq.brokerName + "#" + std::to_string(mq.queueId));
    }
    return out;
}

std::shared_ptr<DefaultMQPullConsumer> makeConsumer(const std::string& ns, const std::string& inst,
                                                    const std::string& group,
                                                    const std::string& topic,
                                                    const std::shared_ptr<Recorder>& rec,
                                                    bool enable) {
    auto c = std::make_shared<DefaultMQPullConsumer>(group);
    // A distinct instanceName gives a distinct clientId; the clientId is what the broker's
    // GET_CONSUMER_LIST_BY_GROUP returns, so without this both facades would be one client.
    c->setInstanceName(inst);
    c->setNamesrvAddr(ns);
    // Shorter than Java's 20s so the probe finishes quickly; the loop shape is the point.
    c->setRebalanceWaitMillis(3000);
    c->setRebalanceMinIntervalMillis(500);
    if (rec) c->registerMessageQueueListener(topic, rec);
    c->setEnableRebalance(enable);
    c->start();
    return c;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: live_pull_rebalance <namesrv>\n");
        return 2;
    }
    const std::string ns = argv[1];
    const std::string prefix = "PullRebLiveCpp_" + std::to_string(nowMs() % 1000000000);
    const std::string topic = prefix + "Topic";
    const std::string group = prefix + "Group";
    const int32_t kQueueNum = 8;

    std::printf("%s: pull-side rebalance live (ns=%s, topic=%s, %d queues)\n", prefix.c_str(),
                ns.c_str(), topic.c_str(), kQueueNum);

    // Create the topic up front: a consumer gets no route for an uncreated topic (TBW102
    // is a producer-side fallback only). Then wait until the name server really serves
    // the topic's own route with all 8 read queues.
    {
        DefaultMQAdminExt admin("PullRebCppAdmin");
        admin.setNamesrvAddr(ns);
        admin.start();
        try {
            admin.createTopic("TBW102", topic, kQueueNum, 0);
        } catch (const std::exception& e) {
            std::printf("  !! createTopic(%s) failed: %s\n", topic.c_str(), e.what());
            return 2;
        }
        const bool ready = waitUntil(
            [&] {
                try {
                    const TopicRouteData route = admin.examineTopicRoute(topic);
                    int32_t queues = 0;
                    for (const QueueData& qd : route.queueDatas) {
                        queues += qd.readQueueNums;
                    }
                    return queues >= kQueueNum;
                } catch (const std::exception&) {
                    return false;
                }
            },
            30000, 500);
        admin.shutdown();
        if (!ready) {
            std::printf("FATAL route for %s never showed %d queues\n", topic.c_str(), kQueueNum);
            return 2;
        }
    }
    std::printf("  created %s with %d queues\n", topic.c_str(), kQueueNum);

    auto recA = std::make_shared<Recorder>();
    auto recB = std::make_shared<Recorder>();
    auto a = makeConsumer(ns, prefix + "InstA", group, topic, recA, true);
    auto b = makeConsumer(ns, prefix + "InstB", group, topic, recB, true);
    std::shared_ptr<DefaultMQPullConsumer> c;
    try {
        std::printf("  started two instances, waiting for the first rebalance rounds...\n");
        const auto table = [&](const std::shared_ptr<DefaultMQPullConsumer>& cc) {
            const auto t = cc->getRebalanceTable();
            auto it = t.find(topic);
            return it == t.end() ? std::vector<MessageQueue>() : it->second;
        };

        // L1: both must end up with a non-empty share of the topic.
        const bool got = waitUntil([&] { return !table(a).empty() && !table(b).empty(); }, 30000);
        const std::vector<MessageQueue> shareA = table(a);
        const std::vector<MessageQueue> shareB = table(b);
        std::printf("  INFO share A=%zu queues, share B=%zu queues\n", shareA.size(), shareB.size());
        check("L1-both instances ended up with a share of the topic", got,
              "A=" + std::to_string(shareA.size()) + " B=" + std::to_string(shareB.size()));

        const std::set<std::string> idsA = idSetOf(shareA);
        const std::set<std::string> idsB = idSetOf(shareB);
        int overlap = 0;
        for (const std::string& id : idsA) {
            if (idsB.count(id)) ++overlap;
        }
        check("L1-the two shares are disjoint (one queue is never owned twice)", overlap == 0,
              "overlap=" + std::to_string(overlap));
        std::set<std::string> unionIds = idsA;
        unionIds.insert(idsB.begin(), idsB.end());
        bool full = static_cast<int64_t>(unionIds.size()) == kQueueNum;
        for (int32_t i = 0; i < kQueueNum && full; ++i) {
            if (!unionIds.count("broker-a#" + std::to_string(i))) full = false;
        }
        check("L1-the two shares cover every queue of the topic exactly once", full,
              "union=" + std::to_string(unionIds.size()) + " of " + std::to_string(kQueueNum));

        // L2: the listener really ran, and mqAll was the topic's full queue set -- proof
        // that the allocation was computed against the real route, not invented locally.
        check("L2-the allocation change was reported to the listener",
              recA->count() >= 1 && recB->count() >= 1,
              "A calls=" + std::to_string(recA->count()) + " B calls=" + std::to_string(recB->count()));
        check("L2-messageQueueChanged carried the topic's full queue set as mqAll",
              recA->lastAllSize() == kQueueNum, "mqAll=" + std::to_string(recA->lastAllSize()));
        {
            const std::set<std::string> divided = recA->lastDividedIds();
            bool same = static_cast<int64_t>(divided.size()) == static_cast<int64_t>(idsA.size());
            for (const std::string& id : divided) {
                if (!idsA.count(id)) same = false;
            }
            check("L2-mqDivided matches what this instance reports as its share", same,
                  "mqDivided=" + std::to_string(divided.size()) + " table=" + std::to_string(idsA.size()));
        }

        // The table is the background rebalance's product, so fetchMessageQueuesInBalance
        // must now answer from it (Java reads the process-queue keys).
        const std::vector<MessageQueue> inBalance = a->fetchMessageQueuesInBalance(topic);
        const std::set<std::string> inBalanceIds = idSetOf(inBalance);
        bool same = static_cast<int64_t>(inBalanceIds.size()) == static_cast<int64_t>(idsA.size());
        for (const std::string& id : inBalanceIds) {
            if (!idsA.count(id)) same = false;
        }
        check("L1-fetch_message_queues_in_balance answers from the rebalance table", same,
              "got=" + std::to_string(inBalanceIds.size()) + " expect=" + std::to_string(idsA.size()));

        // L3: the switch really gates the loop.
        auto recC = std::make_shared<Recorder>();
        c = makeConsumer(ns, prefix + "InstC", group, topic, recC, false);
        std::printf("  started a third instance with enableRebalance=false, waiting 2 rounds...\n");
        std::this_thread::sleep_for(std::chrono::milliseconds(3000 * 2 + 4000));
        check("L3-enableRebalance=false stops the loop: no callback at all", recC->count() == 0,
              "calls=" + std::to_string(recC->count()));
        check("L3-enableRebalance=false leaves the rebalance table empty",
              c->getRebalanceTable().empty(),
              "table size=" + std::to_string(c->getRebalanceTable().size()));
        const std::vector<MessageQueue> onTheSpot = c->fetchMessageQueuesInBalance(topic);
        check("L3-with the loop off, fetch_message_queues_in_balance still computes on the spot",
              !onTheSpot.empty(), "got=" + std::to_string(onTheSpot.size()) + " queues");
    } catch (const std::exception& e) {
        std::printf("FATAL %s\n", e.what());
        gFail += 1;
    }

    try {
        if (c) c->shutdown();
    } catch (const std::exception&) {
    }
    try {
        b->shutdown();
    } catch (const std::exception&) {
    }
    try {
        a->shutdown();
    } catch (const std::exception&) {
    }

    std::printf("\n%s: %d passed, %d failed\n", prefix.c_str(), gPass, gFail);
    return gFail > 0 ? 1 : 0;
}
