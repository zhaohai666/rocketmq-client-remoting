// The offline unit test (tests/test_lite_pull_cursor.cpp) only proves that the
// nextBeginOffset the stub returns gets followed. Only a real broker makes both statements
// below true at once: it is **broker-computed**, and following it **really delivers messages**.
//
// Scenarios:
//   S1 control group: pin 1 message per queue -> assign + seek(0) + `*` -> all 4 received (the
//      path has to work, and the maxOffset == 1 yardstick has to hold too).
//   S2 NO_MATCHED_MSG: replace the assign expression with a Tag that never matches, then
//      seek(0). With the whole run filtered out by expression the nextBeginOffset returned
//      is **already past the whole run** (= maxOffset): every queue's pull cursor must get
//      there; advancing only on FOUND (old code) would sit at 0 rescanning. Zero delivery.
//   S3 OFFSET_ILLEGAL self-healing (decisive case): seek(maxOffset + 1000) per queue.
//      The broker corrects it -> the cursor must return to maxOffset; pin 1 more message per
//      queue and all 4 must **be received**. Old cursor: stuck on the out-of-range value, the
//      same correction every round, nothing new -- the consumer **silently** stops receiving.
//
// Usage: ./rmq_live_lite_pull_cursor 127.0.0.1:9876
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
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

bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 200) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

const int32_t kQueues = 4;
const char* kNeverMatch = "TagLiteCursorNeverMatch";
const int64_t kBigAhead = 1000;

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
        producer = std::make_shared<DefaultMQProducer>("GID_cpp_live_lite_cursor_pg_" + stamp);
        producer->setNamesrvAddr(ns);
        producer->setInstanceName("cpp-live-lite-cursor-prod-" + stamp);
        producer->start();
    }

    std::vector<MessageQueue> queues(const std::string& topicName) {
        TopicRouteData route = admin.examineTopicRoute(topicName);
        return route.getAllMessageQueue(topicName);
    }

    int64_t maxOffset(const MessageQueue& mq) {
        try {
            return admin.client().getMaxOffset(mq, 5000, brokerAddr);
        } catch (const std::exception&) {
            return -1;
        }
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

std::string cursorRow(DefaultLitePullConsumer& c, const std::vector<MessageQueue>& mqs) {
    std::string s;
    for (const MessageQueue& mq : mqs) {
        if (!s.empty()) s += " ";
        s += "q" + std::to_string(mq.queueId) + ":" + std::to_string(c.pullCursorOf(mq));
    }
    return s;
}

/// Poll repeatedly until expect messages arrive (or the timeout); return the bodies received.
std::vector<std::string> drain(DefaultLitePullConsumer& c, size_t expect, int64_t timeoutMs) {
    std::vector<std::string> out;
    const int64_t deadline = nowMs() + timeoutMs;
    while (out.size() < expect && nowMs() < deadline) {
        for (const MessageExt& m : c.poll(500)) {
            out.push_back(m.getBody());
        }
    }
    return out;
}

/// Watch the buffer during the given window (used to assert that nothing is delivered).
std::vector<std::string> pollQuiet(DefaultLitePullConsumer& c, int64_t windowMs) {
    std::vector<std::string> out;
    const int64_t deadline = nowMs() + windowMs;
    while (nowMs() < deadline) {
        for (const MessageExt& m : c.poll(200)) {
            out.push_back(m.getBody());
        }
    }
    return out;
}

std::string join(const std::vector<std::string>& v) {
    std::string s = "[";
    for (size_t i = 0; i < v.size(); ++i) {
        if (i != 0) s += ", ";
        s += v[i];
    }
    return s + "]";
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_lite_pull_cursor <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    Fixture fx;
    try {
        fx.start(nsAddr);
    } catch (const std::exception& e) {
        std::printf("fixture start failed: %s\n", e.what());
        return 2;
    }

    const std::string topic = "CppLiveLiteCursor" + fx.stamp;
    const std::string group = "GID_CppLiteCursorLive_" + fx.stamp;
    std::unique_ptr<DefaultLitePullConsumer> c;

    try {
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic, kQueues);
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::printf("topic=%s queues=%d group=%s\n", topic.c_str(), kQueues, group.c_str());

        std::vector<MessageQueue> mqs = fx.queues(topic);
        check("route visible: 4 queues", mqs.size() == static_cast<size_t>(kQueues),
              "got=" + std::to_string(mqs.size()));
        if (mqs.size() != static_cast<size_t>(kQueues)) {
            fx.shutdown();
            return 1;
        }

        // ---------------- S1 control group ----------------
        for (size_t i = 0; i < mqs.size(); ++i) {
            Message m(topic, "lc-s1-" + std::to_string(i));
            m.setTags("TagA");
            fx.producer->send(m, mqs[i]);
        }

        c = std::make_unique<DefaultLitePullConsumer>(group);
        c->setNamesrvAddr(fx.namesrv);
        c->setInstanceName("cpp-live-lite-cursor-" + fx.stamp);
        c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        // Out-of-range leg must not commit the bad offset: auto-commit off, watch the cursors only.
        c->setAutoCommit(false);
        c->setPullIntervalMillis(200);
        c->assign(mqs);
        c->start();
        std::this_thread::sleep_for(std::chrono::seconds(1));
        for (const MessageQueue& mq : mqs) {
            c->seek(mq, 0);
        }

        std::vector<std::string> got1 = drain(*c, static_cast<size_t>(kQueues), 30000);
        check("S1-control group: all 4 queue-pinned messages received under the * subscription",
              got1.size() == static_cast<size_t>(kQueues),
              "got=" + std::to_string(got1.size()) + " bodies=" + join(got1));

        std::vector<int64_t> maxes;
        const bool maxOk = waitUntil(
            [&] {
                maxes.clear();
                for (const MessageQueue& mq : mqs) {
                    maxes.push_back(fx.maxOffset(mq));
                }
                for (int64_t m : maxes) {
                    if (m != 1) return false;
                }
                return true;
            },
            10000);
        std::string maxDetail;
        for (int64_t m : maxes) maxDetail += std::to_string(m) + " ";
        check("S1-maxOffset == 1 on every queue (yardstick for the two later legs)", maxOk, maxDetail);

        // ---------------- S2 NO_MATCHED_MSG ----------------
        c->setSubExpressionForAssign(topic, kNeverMatch);
        for (const MessageQueue& mq : mqs) {
            c->seek(mq, 0);
        }
        const bool s2 = waitUntil(
            [&] {
                for (const MessageQueue& mq : mqs) {
                    if (c->pullCursorOf(mq) != 1) return false;
                }
                return true;
            },
            20000);
        check("S2-pull cursor passes the whole non-matching run after NO_MATCHED_MSG (== maxOffset=1)", s2,
              cursorRow(*c, mqs));
        std::vector<std::string> quiet = pollQuiet(*c, 1000);
        check("S2-zero delivery while the responses stay empty", quiet.empty(), join(quiet));

        // ---------------- S3 OFFSET_ILLEGAL ----------------
        std::string ahead;
        for (const MessageQueue& mq : mqs) {
            const int64_t maxOff = fx.maxOffset(mq);
            ahead += std::to_string(maxOff) + " ";
            c->seek(mq, maxOff + kBigAhead);
        }
        const bool s3 = waitUntil(
            [&] {
                for (const MessageQueue& mq : mqs) {
                    if (c->pullCursorOf(mq) != 1) return false;
                }
                return true;
            },
            20000);
        check("S3-cursor returns to maxOffset after the broker corrects the out-of-range offset (self-healing)", s3,
              cursorRow(*c, mqs) + " seekedTo=" + ahead);

        // Put back the never-matching expression set in S2, else the broker filters out the 4 TagA messages below.
        c->setSubExpressionForAssign(topic, "*");
        for (size_t i = 0; i < mqs.size(); ++i) {
            Message m(topic, "lc-s3-" + std::to_string(i));
            m.setTags("TagA");
            fx.producer->send(m, mqs[i]);
        }

        std::vector<std::string> got3 = drain(*c, static_cast<size_t>(kQueues), 40000);
        check("S3-all new messages delivered after self-healing (old code: cursor stuck at +1000, nothing visible)",
              got3.size() == static_cast<size_t>(kQueues),
              "got=" + std::to_string(got3.size()) + " bodies=" + join(got3));
        bool bodiesOk = got3.size() == static_cast<size_t>(kQueues);
        for (int32_t i = 0; bodiesOk && i < kQueues; ++i) {
            const std::string want = "lc-s3-" + std::to_string(i);
            bool found = false;
            for (const std::string& got : got3) {
                if (got == want) found = true;
            }
            bodiesOk = found;
        }
        check("S3-exactly the 4 messages pinned after the out-of-range seek were received", bodiesOk, join(got3));

        c->shutdown();
    } catch (const std::exception& e) {
        check("scenario threw", false, e.what());
    }

    fx.shutdown();
    std::printf("\nlite pull cursor live: %d passed, %d failed\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
