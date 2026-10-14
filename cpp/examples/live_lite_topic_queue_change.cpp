// Live-cluster verification of the LitePull consumer's topic queue-set change listener.
// Usage: rmq_live_lite_topic_queue_change 127.0.0.1:9876
//
// Why a real cluster is required: every comparison pass queries the nameserver freshly for the subscribed queue set, while the ordinary
// route cache only refreshes every 30 s. A fake nameserver can prove the comparison logic; only a real cluster can prove the "fresh query".
// So here the check interval is squeezed to the 1 s floor, route polling stays at the default 30 s, and the topic really gets
// scaled out: in the steady period past the first-query delay, from the nameserver reporting the new queue count to the listener receiving the callback
// there should be only one or two check passes (<=5 s); if the comparison read the 30 s cache, this window would stretch beyond half a minute.
//
//   L1  queues unchanged => the listener is not disturbed
//   L1b the first-query pass really ran => still silent (a snapshot registered while running is not a change)
//   L2  scale out 2→4: once the nameserver agrees, the callback follows within a few check passes
//   L3  snapshot advances after the callback => the same queue set is not reported again
//   L4  scale in 4→2: likewise observed through the fresh query
//   L5  a topic never created: an empty queue set counts as "not found", not disguised as scaled down to 0 queues
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
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

constexpr int32_t kBaseQueueNum = 2;
constexpr int32_t kScaledQueueNum = 4;
// Squeeze the check interval to the floor so "a fresh query each pass" and "eating the 30 s cache" separate in time.
constexpr int32_t kCheckIntervalMs = 1000;
// First-query delay of the background comparison (matches the default in the implementation).
constexpr int64_t kFirstDelayMs = 10000;
// Largest allowed callback gap after the nameserver has seen the change.
constexpr int64_t kFreshWindowMs = 5000;

std::string idsText(const std::vector<int32_t>& ids) {
    std::string s;
    for (int32_t id : ids) {
        if (!s.empty()) s += ",";
        s += std::to_string(id);
    }
    return s;
}

/// Only records callbacks, changes no state; the background thread calls it too, so it carries its own lock.
class Recorder : public TopicMessageQueueChangeListener {
public:
    void onChanged(const std::string& topic,
                   const std::vector<MessageQueue>& messageQueues) override {
        std::vector<int32_t> ids;
        for (const MessageQueue& mq : messageQueues) ids.push_back(mq.queueId);
        std::sort(ids.begin(), ids.end());
        std::lock_guard<std::mutex> g(gate_);
        events_.emplace_back(topic, ids);
    }
    std::vector<std::pair<std::string, std::vector<int32_t>>> events() {
        std::lock_guard<std::mutex> g(gate_);
        return events_;
    }
    size_t count() { return events().size(); }
    std::vector<int32_t> lastIds() {
        auto ev = events();
        return ev.empty() ? std::vector<int32_t>() : ev.back().second;
    }

private:
    std::mutex gate_;
    std::vector<std::pair<std::string, std::vector<int32_t>>> events_;
};

std::vector<int32_t> seq(int32_t n) {
    std::vector<int32_t> v;
    for (int32_t i = 0; i < n; ++i) v.push_back(i);
    return v;
}

void sleepMs(int64_t ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

/// Queries the queue set fresh until it reports want queues; returns the waited milliseconds, -1 on timeout.
int64_t waitQueueNum(DefaultLitePullConsumer& c, const std::string& topic, int32_t want,
                     int64_t timeoutMs) {
    int64_t waited = 0;
    while (waited < timeoutMs) {
        try {
            if (static_cast<int32_t>(c.fetchMessageQueues(topic).size()) == want) {
                return waited;
            }
        } catch (const std::exception&) {
            // the route has not updated yet, keep waiting
        }
        sleepMs(250);
        waited += 250;
    }
    return -1;
}

/// Waits until the listener records want callbacks; returns the waited milliseconds, -1 on timeout.
int64_t waitEvents(Recorder& rec, size_t want, int64_t timeoutMs) {
    int64_t waited = 0;
    while (waited < timeoutMs) {
        if (rec.count() >= want) return waited;
        sleepMs(100);
        waited += 100;
    }
    return -1;
}

void scaleTopic(const std::string& namesrv, const std::string& topic, const std::string& group,
                int32_t queueNum) {
    DefaultMQProducer prod(group);
    prod.setNamesrvAddr(namesrv);
    prod.start();
    try {
        prod.createTopic("TBW102", topic, queueNum);
    } catch (const std::exception& e) {
        std::printf("!! createTopic(%d) failed: %s\n", queueNum, e.what());
    }
    prod.shutdown();
}

}  // namespace

int main(int argc, char** argv) {
    std::string namesrv = argc > 1 ? argv[1] : "127.0.0.1:9876";
    int64_t stamp = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    std::string topic = "LiteQcLive_" + std::to_string(stamp);
    std::string group = "LiteQcG_" + std::to_string(stamp);
    std::string ghost = "LiteQcGhost_" + std::to_string(stamp);
    std::printf("LitePull queue-change live (C++): namesrv=%s topic=%s\n",
                namesrv.c_str(), topic.c_str());

    scaleTopic(namesrv, topic, "PG_QcPrepare_" + std::to_string(stamp), kBaseQueueNum);

    DefaultLitePullConsumer c(group);
    c.setNamesrvAddr(namesrv);
    c.setInstanceName("lite-qc-live");
    c.setTopicMetadataCheckIntervalMillis(kCheckIntervalMs);
    check("L0 check interval squeezed to the 1 s floor", c.topicMetadataCheckIntervalMillis() == kCheckIntervalMs,
          std::to_string(c.topicMetadataCheckIntervalMillis()));
    c.subscribe(topic, "*");
    c.start();
    auto loopStart = std::chrono::steady_clock::now();

    int64_t seen = waitQueueNum(c, topic, kBaseQueueNum, 30000);
    check("L0 route visible (2 queues)", seen >= 0, std::to_string(seen) + "ms");

    auto rec = std::make_shared<Recorder>();
    // Registering while running => the snapshot is recorded at once => the first round must not call back
    c.registerTopicMessageQueueChangeListener(topic, rec);
    sleepMs(3000);
    check("L1 queues unchanged => silent", rec->count() == 0, "events=" + std::to_string(rec->count()));

    // Wait past the first-query delay; from here on only the stable period of one pass per second remains
    int64_t elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - loopStart).count();
    if (elapsed < kFirstDelayMs + 2000) sleepMs(kFirstDelayMs + 2000 - elapsed);
    check("L1b the first-query pass really ran => still silent", rec->count() == 0,
          "events=" + std::to_string(rec->count()));

    // ---- L2 scale out 2→4
    scaleTopic(namesrv, topic, "PG_QcUp_" + std::to_string(stamp), kScaledQueueNum);
    int64_t nsMs = waitQueueNum(c, topic, kScaledQueueNum, 45000);
    check("L2a nameserver reports 4 queues", nsMs >= 0, std::to_string(nsMs) + "ms");
    if (nsMs >= 0) {
        int64_t cb = waitEvents(*rec, 1, kFreshWindowMs);
        check("L2b comparison passes query the route fresh (the callback follows the nameserver, not the 30 s cache)",
              cb >= 0 && rec->lastIds() == seq(kScaledQueueNum),
              std::to_string(cb) + "ms, ids=" + idsText(rec->lastIds()));
    }

    // ---- L3 snapshot advances
    sleepMs(3000);
    check("L3 snapshot advances after the callback => no repeated callback", rec->count() == 1,
          "events=" + std::to_string(rec->count()));

    // ---- L4 scale in 4→2
    scaleTopic(namesrv, topic, "PG_QcDown_" + std::to_string(stamp), kBaseQueueNum);
    nsMs = waitQueueNum(c, topic, kBaseQueueNum, 45000);
    check("L4a nameserver reports back 2 queues", nsMs >= 0, std::to_string(nsMs) + "ms");
    if (nsMs >= 0) {
        int64_t cb = waitEvents(*rec, 2, kFreshWindowMs);
        check("L4b the scale-in is likewise seen through the fresh route query",
              cb >= 0 && rec->lastIds() == seq(kBaseQueueNum),
              std::to_string(cb) + "ms, ids=" + idsText(rec->lastIds()));
    }

    // ---- L5 unknown topic: an empty queue set counts as "not found"
    std::string raised;
    try {
        auto queues = c.fetchMessageQueues(ghost);
        raised = "no exception, queues=" + std::to_string(queues.size());
    } catch (const std::exception& e) {
        raised = e.what();
    }
    check("L5 fetching queues of an unknown topic throws 'not found' instead of returning an empty set",
          raised.find("Namesrv return empty") != std::string::npos, raised);

    c.shutdown();
    std::printf("\nLitePull queue-change live (C++): PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
