// Active-pull consumer (DefaultMQPullConsumer) live-cluster verification.
// Usage: rmq_live_pull 127.0.0.1:9876
//
// Scenarios (the core of pull mode is "the caller pulls itself and manages offsets itself"; every assertion revolves around that):
//   S1 create topic + fetchSubscribeMessageQueues → get 4 queues
//   S1b fetchMessageQueuesInBalance → an instance that **never pulled** derives its own share from the real route + 38
//   S2 produce 12 messages → per-queue min/max offset difference = 3 (messages land evenly on the 4 queues)
//   S3 manual pull: per queue from min offset to max offset → all 12 received and bodies match the sent set
//   S4 manual offset commit: updateConsumeOffset → fetchConsumeOffset reads back the same value (broker round trip)
//   S5 the caller owns the offset: pull again from the committed offset → NO_NEW_MSG; rewind the offset to min and pull → FOUND
//      (push mode cannot do this — precisely why pull mode exists)
//   S6 searchOffset(now) / earliestMsgStoreTime → both > 0
//   S7 sendMessageBack → the message lands in %RETRY%group and can be pulled (the redelivery path really works)
//
// ⚠ Two traps already stepped in (do not "optimize them away"):
//   1. **create the topic before fetching queues**: the consumer does no default-topic fallback, so a nonexistent topic yields no route;
//   2. **do not query maxOffset right after producing**: the broker's consumequeue dispatches asynchronously and
//      would read 0 (measured with a probe); poll until the sum of per-queue max-min reaches the target.
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <typeinfo>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/util_all.h"

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

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::vector<std::string> splitAddrs(const std::string& addr) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : addr) {
        if (c == ';' || c == ',') {
            if (!cur.empty()) out.push_back(cur);
            cur.clear();
        } else if (c != ' ') {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

// MessageQueue must be comparable to serve as a map key: use the offsetKey string as key, for set-style deduplication.
std::string qKey(const MessageQueue& q) {
    return q.brokerName + ":" + std::to_string(q.queueId);
}

std::string join(const std::set<std::string>& s) {
    std::string out;
    for (const std::string& v : s) {
        out += v + " ";
    }
    return out;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string namesrv = argc > 1 ? argv[1] : std::string("127.0.0.1:9876");
    const std::vector<std::string> nsAddrs = splitAddrs(namesrv);

    const std::string stamp = std::to_string(nowMs());
    const std::string topic = "PullLiveCpp_" + stamp;
    const std::string group = "PG_PullLiveCpp_" + stamp;
    const std::string retryTopic = MixAll::getRetryTopic(group);
    const int32_t nMsg = 12;
    const int32_t queueNum = 4;
    const int32_t perQueue = nMsg / queueNum;

    std::printf("======================================================================\n");
    std::printf("PullConsumer live (C++): namesrv=%s topic=%s group=%s\n", namesrv.c_str(),
                topic.c_str(), group.c_str());
    std::printf("======================================================================\n");

    // ---------------- create the topic ----------------
    {
        DefaultMQProducer prep("PG_PrepareCpp_" + stamp);
        prep.setNamesrvAddr(namesrv);
        prep.start();
        try {
            prep.createTopic("TBW102", topic, queueNum);
        } catch (const std::exception& e) {
            std::printf("!! createTopic failed: %s\n", e.what());
        }
        prep.shutdown();
    }

    DefaultMQPullConsumer consumer(group);
    consumer.setNamesrvAddr(namesrv);

    // ---------------- S1 queues ----------------
    std::printf("\nS1 create topic + fetchSubscribeMessageQueues\n");
    std::vector<MessageQueue> routes;
    {
        consumer.start();
        const int64_t deadline = nowMs() + 20000;
        std::string last = "not tried";
        while (nowMs() < deadline) {
            try {
                routes = consumer.fetchSubscribeMessageQueues(topic);
                if (static_cast<int32_t>(routes.size()) >= queueNum) break;
                last = std::to_string(routes.size()) + " queues";
            } catch (const std::exception& e) {
                last = std::string(typeid(e).name()) + ": " + e.what();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        if (static_cast<int32_t>(routes.size()) != queueNum) {
            check("S1 got " + std::to_string(queueNum) + " queues", false,
                  "got=" + std::to_string(routes.size()) + " last=" + last);
            std::printf("\nPullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
            return 1;
        }
    }
    check("S1 got " + std::to_string(queueNum) + " queues", true,
          "got=" + std::to_string(routes.size()));
    {
        bool namesOk = true;
        for (const MessageQueue& q : routes) {
            if (q.topic != topic || q.brokerName.empty()) namesOk = false;
        }
        check("S1 queue topics and broker names are non-empty", namesOk,
              "sample=" + routes[0].brokerName + ":" + std::to_string(routes[0].queueId));
    }

    std::printf("\nS1b fetchMessageQueuesInBalance\n");
    {
        consumer.registerMessageQueueListener(topic, nullptr);
        const int32_t hbOk = consumer.sendHeartbeatToAllBroker();
        check("S1b heartbeat went out after registering the topic (prerequisite for 38 to be visible)", hbOk > 0,
              "heartbeat_ok=" + std::to_string(hbOk));
        std::vector<MessageQueue> mine;
        const int64_t balanceDeadline = nowMs() + 20000;
        while (nowMs() < balanceDeadline) {
            try {
                mine = consumer.fetchMessageQueuesInBalance(topic);
                if (static_cast<int32_t>(mine.size()) == queueNum) break;
            } catch (const std::exception& e) {
                std::printf("  [diag] fetchMessageQueuesInBalance: %s\n", e.what());
            }
            consumer.sendHeartbeatToAllBroker();
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        check("S1b balance view: the exclusive group got all " + std::to_string(queueNum) + " queues"
              " (never pulled ⇒ can only have been computed from route + 38)",
              static_cast<int32_t>(mine.size()) == queueNum,
              "got=" + std::to_string(mine.size()) + " of " + std::to_string(queueNum));
        std::set<std::string> mineKeys, wholeKeys;
        for (const MessageQueue& q : mine) mineKeys.insert(qKey(q));
        for (const MessageQueue& q : routes) wholeKeys.insert(qKey(q));
        bool subset = true;
        for (const std::string& k : mineKeys) {
            if (wholeKeys.find(k) == wholeKeys.end()) subset = false;
        }
        check("S1b balance view is a subset of the subscribe view", subset, "mine=" + join(mineKeys));
    }
    // ---------------- S2 produce ----------------
    std::printf("\nS2 produce %d messages\n", nMsg);
    std::set<std::string> sent;
    {
        DefaultMQProducer prod("PG_PullLiveCpp_" + stamp);
        prod.setNamesrvAddr(namesrv);
        prod.start();
        int32_t sentOk = 0;
        for (int i = 0; i < nMsg; ++i) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "pull-%02d", i);
            const std::string body(buf);
            try {
                Message msg(topic, body);
                msg.setKeys(std::string("pull-key-") + buf);
                SendResult r = prod.send(msg);
                if (r.getSendStatus() == SendStatus::SEND_OK) {
                    ++sentOk;
                    sent.insert(body);
                }
            } catch (const std::exception& e) {
                std::printf("   send %d failed: %s\n", i, e.what());
            }
        }
        check("S2 produced " + std::to_string(nMsg) + " messages successfully", sentOk == nMsg,
              "sentOk=" + std::to_string(sentOk));
        prod.shutdown();
    }

    // Poll until the consumequeue dispatch lands (maxOffset must not be queried right after sending)
    std::map<std::string, int64_t> lo;
    std::map<std::string, int64_t> hi;
    {
        const int64_t deadline = nowMs() + 25000;
        while (nowMs() < deadline) {
            lo.clear();
            hi.clear();
            int64_t total = 0;
            for (const MessageQueue& q : routes) {
                lo[qKey(q)] = consumer.minOffset(q);
                hi[qKey(q)] = consumer.maxOffset(q);
                total += std::max<int64_t>(0, hi[qKey(q)] - lo[qKey(q)]);
            }
            if (total >= nMsg) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }
    for (const MessageQueue& q : routes) {
        const int64_t diff = hi[qKey(q)] - lo[qKey(q)];
        check("S2 queue " + qKey(q) + " holds " + std::to_string(perQueue) + " messages", diff == perQueue,
              "min=" + std::to_string(lo[qKey(q)]) + " max=" + std::to_string(hi[qKey(q)]));
    }

    // ---------------- S3 manual pull ----------------
    std::printf("\nS3 manual pull (per queue min -> max)\n");
    std::set<std::string> got;
    {
        for (const MessageQueue& q : routes) {
            int64_t offset = lo[qKey(q)];
            const int64_t end = hi[qKey(q)];
            int guard = 0;
            while (offset < end && guard < 64) {
                ++guard;
                PullResult r;
                try {
                    r = consumer.pull(q, "*", offset, 32, 5000);
                } catch (const std::exception& e) {
                    check("S3 pull " + qKey(q) + " threw an exception", false, e.what());
                    break;
                }
                if (r.status == PullStatus::FOUND) {
                    for (const MessageExt& m : r.msgFoundList) {
                        got.insert(bodyOf(m));
                    }
                    if (r.nextBeginOffset <= offset) break;
                    offset = r.nextBeginOffset;
                } else if (r.status == PullStatus::NO_NEW_MSG) {
                    break;
                } else if (r.status == PullStatus::OFFSET_ILLEGAL) {
                    break;
                } else {
                    check("S3 pull " + qKey(q) + " returned an unexpected status", false,
                          std::string("status=") + pullStatusName(r.status));
                    break;
                }
            }
        }
        std::set<std::string> missing;
        std::set<std::string> extra;
        std::set_difference(sent.begin(), sent.end(), got.begin(), got.end(),
                            std::inserter(missing, missing.begin()));
        std::set_difference(got.begin(), got.end(), sent.begin(), sent.end(),
                            std::inserter(extra, extra.begin()));
        check("S3 manual pull collected all " + std::to_string(nMsg) + " messages with matching content", got == sent,
              "got=" + std::to_string(got.size()) + " missing=[" + join(missing) + "] extra=[" +
                  join(extra) + "]");
    }

    if (got.empty()) {
        std::printf("\n!! nothing was pulled, skipping the remaining scenarios\n");
        std::printf("\nPullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
        consumer.shutdown();
        return 1;
    }

    // ---------------- S4 manual offset commit ----------------
    std::printf("\nS4 commit the offset manually and read it back\n");
    {
        const MessageQueue& q0 = routes[0];
        const int64_t target = hi[qKey(q0)];
        consumer.updateConsumeOffset(q0, target);
        int64_t back = -1;
        const bool haveOffset = consumer.fetchConsumeOffset(q0, back);
        check("S4 offset reads back the same after commit", haveOffset && back == target,
              "committed=" + std::to_string(back) + " target=" + std::to_string(target));
    }

    // ---------------- S5 the caller owns the offset ----------------
    std::printf("\nS5 the caller owns the offset\n");
    {
        const MessageQueue& q0 = routes[0];
        const int64_t committed = hi[qKey(q0)];
        PullResult r1 = consumer.pull(q0, "*", committed, 32, 5000);
        check("S5 pulling again from the committed offset = NO_NEW_MSG",
              r1.status == PullStatus::NO_NEW_MSG && r1.msgFoundList.empty(),
              std::string("status=") + pullStatusName(r1.status) +
                  " n=" + std::to_string(r1.msgFoundList.size()));

        PullResult r2 = consumer.pull(q0, "*", lo[qKey(q0)], 32, 5000);
        check("S5 after rewinding the offset to min the messages can be re-pulled (the core capability of pull mode)",
              r2.status == PullStatus::FOUND && !r2.msgFoundList.empty(),
              std::string("status=") + pullStatusName(r2.status) +
                  " n=" + std::to_string(r2.msgFoundList.size()));
    }

    // ---------------- S6 offset queries ----------------
    std::printf("\nS6 searchOffset / earliestMsgStoreTime / min/max\n");
    {
        const MessageQueue& q0 = routes[0];
        const int64_t so = consumer.searchOffset(q0, UtilAll::currentTimeMillis());
        check("S6 searchOffset(now) > 0", so > 0, "searchOffset=" + std::to_string(so));
        const int64_t emst = consumer.earliestMsgStoreTime(q0);
        check("S6 earliestMsgStoreTime > 0", emst > 0, "earliest=" + std::to_string(emst));
        check("S6 minOffset <= maxOffset", lo[qKey(q0)] <= hi[qKey(q0)],
              "min=" + std::to_string(lo[qKey(q0)]) + " max=" + std::to_string(hi[qKey(q0)]));
    }

    // ---------------- S7 redelivery ----------------
    std::printf("\nS7 sendMessageBack -> %%RETRY%%group is pullable\n");
    {
        MessageExt sample;
        bool haveSample = false;
        for (const MessageQueue& q : routes) {
            PullResult r = consumer.pull(q, "*", lo[qKey(q)], 1, 5000);
            if (!r.msgFoundList.empty()) {
                sample = r.msgFoundList[0];
                haveSample = true;
                break;
            }
        }
        if (!haveSample) {
            check("S7 take a sample message", false, "no message available");
        } else {
            const std::string want = bodyOf(sample);
            try {
                consumer.sendMessageBack(sample, 0);
                check("S7 redelivery request accepted by the broker", true,
                      "offset=" + std::to_string(sample.commitLogOffset) + " body=" + want);
            } catch (const std::exception& e) {
                check("S7 redelivery request accepted by the broker", false, e.what());
                std::printf("\nPullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
                consumer.shutdown();
                return gFail == 0 ? 0 : 1;
            }

            bool found = false;
            std::string detail = "not tried";
            const int64_t deadline = nowMs() + 40000;
            while (nowMs() < deadline && !found) {
                std::vector<MessageQueue> rqs;
                try {
                    rqs = consumer.fetchSubscribeMessageQueues(retryTopic);
                } catch (const std::exception& e) {
                    detail = std::string("retry topic not routable yet: ") + e.what();
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    continue;
                }
                for (const MessageQueue& rq : rqs) {
                    int64_t rlo = 0;
                    int64_t rhi = 0;
                    try {
                        rlo = consumer.minOffset(rq);
                        rhi = consumer.maxOffset(rq);
                    } catch (const std::exception& e) {
                        detail = e.what();
                        continue;
                    }
                    if (rhi <= rlo) continue;
                    PullResult r = consumer.pull(rq, "*", rlo, 32, 5000);
                    for (const MessageExt& m : r.msgFoundList) {
                        if (bodyOf(m) == want) {
                            found = true;
                            detail = "queue=" + std::to_string(rq.queueId) +
                                     " reconsumeTimes=" + std::to_string(m.reconsumeTimes) +
                                     " body=" + bodyOf(m);
                            break;
                        }
                    }
                    if (found) break;
                }
                if (!found) std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            check("S7 the %RETRY% topic served the redelivered message", found, detail);
        }
    }

    consumer.shutdown();

    std::printf("\nPullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
