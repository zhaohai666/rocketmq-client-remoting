// lite-pull **拉取游标**（#105，Java `DefaultLitePullConsumerImpl#PullTaskImpl.run:982-998`）
// 真机验证。与 `python/verify_lite_pull_cursor_live.py`、`rust/examples/live_lite_pull_cursor.rs`、
// csharp 的对应场景同题、逐条对应。
//
// 离线单测（tests/test_lite_pull_cursor.cpp）只能证明「脚本回的 nextBeginOffset 被跟了」；
// 真 broker 才能让下面两件事同时成立：那个 nextBeginOffset 是 **broker 自己算的**，而且
// 跟过去以后**真的能收到消息**。
//
// 场景：
//   S1 对照组：每条队列钉 1 条 → assign + seek(0) + `*` → 4 条全收（链路要通，
//      maxOffset == 1 这个标尺也要立住）。
//   S2 NO_MATCHED_MSG：把 assign 表达式换成永不匹配的 Tag 再 seek(0)。broker 按表达式把
//      整段滤掉后回的 nextBeginOffset **已经越过整段**（= maxOffset）。断言每条队列的拉取
//      游标都到 maxOffset（旧实现只在 FOUND 时推游标，这里会永远停在 0，每轮重扫同一段）。
//      零投递。
//   S3 OFFSET_ILLEGAL 自愈（决定性一条）：每条队列 seek(maxOffset + 1000)。broker 回纠正值
//      → 游标必须回到 maxOffset；随后每条队列再钉 1 条，4 条必须**全部收到**。旧实现的游标
//      永远卡在越界值上：每轮收到同一个「越界纠正」，新消息一条也看不到 —— 越界之后消费者
//      会**静默**地永远收不到消息。
//
// 用法：./rmq_live_lite_pull_cursor 127.0.0.1:9876
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
            throw std::runtime_error("nameServer 无 broker 注册");
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

/// 反复 poll 直到收齐 expect 条（或超时），返回收到的 body。
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

/// 给定窗口里盯住缓冲（断言「不该有交付」时用）。
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
        check("路由可见：4 条队列", mqs.size() == static_cast<size_t>(kQueues),
              "got=" + std::to_string(mqs.size()));
        if (mqs.size() != static_cast<size_t>(kQueues)) {
            fx.shutdown();
            return 1;
        }

        // ---------------- S1 对照组 ----------------
        for (size_t i = 0; i < mqs.size(); ++i) {
            Message m(topic, "lc-s1-" + std::to_string(i));
            m.setTags("TagA");
            fx.producer->send(m, mqs[i]);
        }

        c = std::make_unique<DefaultLitePullConsumer>(group);
        c->setNamesrvAddr(fx.namesrv);
        c->setInstanceName("cpp-live-lite-cursor-" + fx.stamp);
        c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        // 位点越界那一腿绝不能把越界值提交上去：关掉自动提交，只看拉取/交付两条游标。
        c->setAutoCommit(false);
        c->setPullIntervalMillis(200);
        c->assign(mqs);
        c->start();
        std::this_thread::sleep_for(std::chrono::seconds(1));
        for (const MessageQueue& mq : mqs) {
            c->seek(mq, 0);
        }

        std::vector<std::string> got1 = drain(*c, static_cast<size_t>(kQueues), 30000);
        check("S1-对照组：* 订阅下 4 条钉到队列的消息全收",
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
        check("S1-每条队列 maxOffset == 1（后面两条腿的标尺）", maxOk, maxDetail);

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
        check("S2-NO_MATCHED_MSG 后拉取游标越过整段不匹配区间（== maxOffset=1）", s2,
              cursorRow(*c, mqs));
        std::vector<std::string> quiet = pollQuiet(*c, 1000);
        check("S2-空应答期间零投递", quiet.empty(), join(quiet));

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
        check("S3-越界位点被 broker 纠正后游标回到 maxOffset（越界自愈）", s3,
              cursorRow(*c, mqs) + " seekedTo=" + ahead);

        // S2 换上的永不匹配表达式要换回来，否则下面 4 条 TagA 会被 broker 原样滤掉。
        c->setSubExpressionForAssign(topic, "*");
        for (size_t i = 0; i < mqs.size(); ++i) {
            Message m(topic, "lc-s3-" + std::to_string(i));
            m.setTags("TagA");
            fx.producer->send(m, mqs[i]);
        }

        std::vector<std::string> got3 = drain(*c, static_cast<size_t>(kQueues), 40000);
        check("S3-自愈后新消息全部送达（旧实现：游标卡在 +1000，一条都看不到）",
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
        check("S3-收到的正是越界之后钉进去的那 4 条", bodiesOk, join(got3));

        c->shutdown();
    } catch (const std::exception& e) {
        check("场景异常", false, e.what());
    }

    fx.shutdown();
    std::printf("\nlite pull cursor live: %d passed, %d failed\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
