// 轻量拉取消费者（DefaultLitePullConsumer）真机验证。
// 用法：rmq_live_lite_pull 127.0.0.1:9876
//
// 与 Python verify_lite_pull_live.py 完全同场景（三语言对拍用同一套断言）。
//
// 场景（围绕 Lite 相对 Pull 的本质区别：调用方不用管位点，poll 从本地缓冲拿消息）：
//   S1 建 topic + subscribe 模式等待 rebalance 分到位点（4/4 队列）
//   S2 先起消费者、再发 12 条（交替 TagA/TagB）→ subscribe + poll 收全 12 条且内容一致
//   S3 auto-commit：消费后 committed 位点 > 0，且 commit() 后可回读
//   S4 assign 模式：显式 assign 全部队列 + seek 到队首 → poll 重新收全 12 条
//   S5 订阅 TagA：subscribe(T, "TagA") 只收 TagA 的 6 条（订阅级 tag 过滤）
//   S6 CONSUME_FROM_TIMESTAMP：consumeTimestamp 按 Java 的 14 位本地墙钟解释
//      S6a 新组 + 起点=30 分钟前 → 收全 12 条
//      S6b 墙钟→队列位置映射：30 分钟前 → 各队列队首（Σ=0）；10 分钟后 → 越过全部消息（Σ=12）
//      （旧实现把 "20260919072530" 当 epoch 秒 → 公元 2611 年 → 两个方向同时翻转）
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
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

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

std::string join(const std::set<std::string>& s) {
    std::string out;
    for (const std::string& v : s) {
        out += v + " ";
    }
    return out;
}

std::vector<MessageQueue> waitAssignment(DefaultLitePullConsumer& c, int32_t timeoutMs = 20000) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        std::vector<MessageQueue> a = c.assignment();
        if (!a.empty()) return a;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return {};
}

std::vector<MessageExt> drain(DefaultLitePullConsumer& c, size_t expect, int32_t timeoutMs = 30000) {
    std::vector<MessageExt> collected;
    const int64_t deadline = nowMs() + timeoutMs;
    while (collected.size() < expect && nowMs() < deadline) {
        std::vector<MessageExt> batch = c.poll(1000);
        for (MessageExt& m : batch) collected.push_back(std::move(m));
    }
    return collected;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string namesrv = argc > 1 ? argv[1] : std::string("127.0.0.1:9876");
    const std::string stamp = std::to_string(nowMs());
    const std::string topic = "LiteLiveCpp_" + stamp;
    const std::string group1 = "LitePG1Cpp_" + stamp;
    const std::string group2 = "LitePG2Cpp_" + stamp;
    const std::string group3 = "LitePG3Cpp_" + stamp;
    const std::string group4 = "LitePG4Cpp_" + stamp;
    const std::string group5 = "LitePG5Cpp_" + stamp;
    const int32_t nMsg = 12;
    const int32_t queueNum = 4;

    std::printf("======================================================================\n");
    std::printf("LitePullConsumer live (C++): namesrv=%s topic=%s group=%s\n", namesrv.c_str(),
                topic.c_str(), group1.c_str());
    std::printf("======================================================================\n");

    // ---------------- 建 topic ----------------
    {
        DefaultMQProducer prep("PG_PrepareLiteCpp_" + stamp);
        prep.setNamesrvAddr(namesrv);
        prep.start();
        try {
            prep.createTopic("TBW102", topic, queueNum);
        } catch (const std::exception& e) {
            std::printf("!! createTopic failed: %s\n", e.what());
        }
        prep.shutdown();
    }

    // ---------------- S1 subscribe 模式：先起消费者再发消息 ----------------
    std::printf("\nS1 subscribe 模式启动 + 等待 rebalance 分到位点\n");
    DefaultLitePullConsumer c1(group1);
    c1.setNamesrvAddr(namesrv);
    c1.setPollTimeoutMillis(1000);
    c1.subscribe(topic, "*");
    c1.start();

    std::vector<MessageQueue> assigned = waitAssignment(c1);
    check("S1 rebalance 分配到 " + std::to_string(queueNum) + " 个队列",
          static_cast<int32_t>(assigned.size()) == queueNum,
          "assigned=" + std::to_string(assigned.size()));
    if (assigned.empty()) {
        std::printf("!! 未分配到队列，后续跳过\n");
        c1.shutdown();
        std::printf("\nLitePullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
        return 1;
    }

    // ---------------- S2 生产 + poll 收全 ----------------
    std::printf("\nS2 生产 %d 条（交替 TagA/TagB）\n", nMsg);
    std::set<std::string> sent;
    {
        DefaultMQProducer prod("PG_LiteLiveCpp_" + stamp);
        prod.setNamesrvAddr(namesrv);
        prod.start();
        int32_t sentOk = 0;
        for (int i = 0; i < nMsg; ++i) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "lite-%02d", i);
            const std::string body(buf);
            try {
                Message msg(topic, body);
                msg.setKeys(std::string("lite-key-") + buf);
                msg.setTags(i % 2 == 0 ? "TagA" : "TagB");
                SendResult r = prod.send(msg);
                if (r.getSendStatus() == SendStatus::SEND_OK) {
                    ++sentOk;
                    sent.insert(body);
                }
            } catch (const std::exception& e) {
                std::printf("   send %d failed: %s\n", i, e.what());
            }
        }
        check("S2 生产 " + std::to_string(nMsg) + " 条成功", sentOk == nMsg,
              "sentOk=" + std::to_string(sentOk));
        prod.shutdown();
    }

    std::vector<MessageExt> got = drain(c1, static_cast<size_t>(nMsg));
    {
        std::set<std::string> gotSet;
        for (const MessageExt& m : got) gotSet.insert(bodyOf(m));
        std::set<std::string> missing;
        std::set<std::string> extra;
        std::set_difference(sent.begin(), sent.end(), gotSet.begin(), gotSet.end(),
                            std::inserter(missing, missing.begin()));
        std::set_difference(gotSet.begin(), gotSet.end(), sent.begin(), sent.end(),
                            std::inserter(extra, extra.begin()));
        check("S2 subscribe+poll 收全 " + std::to_string(nMsg) + " 条且内容一致", gotSet == sent,
              "got=" + std::to_string(gotSet.size()) + " missing=[" + join(missing) +
                  "] extra=[" + join(extra) + "]");
    }

    // ---------------- S3 auto-commit 位点 ----------------
    std::printf("\nS3 auto-commit 位点\n");
    {
        bool allPositive = true;
        std::string detail;
        for (const MessageQueue& mq : assigned) {
            const int64_t v = c1.committed(mq);
            detail += std::to_string(v) + " ";
            if (v <= 0) allPositive = false;
        }
        check("S3 各队列 committed 位点 > 0", allPositive, "committed=" + detail);
    }

    // ---------------- S4 assign 模式：assign 全部队列 + seek 到队首重新收全 ----------------
    std::printf("\nS4 assign 模式：assign 全部队列 + seek 到队首重新收全\n");
    {
        DefaultLitePullConsumer c2(group2);
        c2.setNamesrvAddr(namesrv);
        c2.setPollTimeoutMillis(1000);
        c2.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        std::vector<MessageQueue> allQueues;
        try {
            allQueues = c1.fetchMessageQueues(topic);
        } catch (const std::exception& e) {
            check("S4 fetchMessageQueues", false, e.what());
        }
        c2.assign(allQueues);
        c2.start();
        for (const MessageQueue& mq : allQueues) {
            c2.seekToBegin(mq);
        }
        std::vector<MessageExt> got2 = drain(c2, static_cast<size_t>(nMsg));
        std::set<std::string> got2Set;
        for (const MessageExt& m : got2) got2Set.insert(bodyOf(m));
        check("S4 assign+seek+poll 重新收全 " + std::to_string(nMsg) + " 条", got2Set == sent,
              "got=" + std::to_string(got2Set.size()));
        c2.shutdown();
    }

    // ---------------- S5 订阅级 tag 过滤 ----------------
    std::printf("\nS5 订阅 TagA：只收 TagA 的 6 条\n");
    {
        DefaultLitePullConsumer c3(group3);
        c3.setNamesrvAddr(namesrv);
        c3.setPollTimeoutMillis(1000);
        c3.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c3.subscribe(topic, "TagA");
        c3.start();
        waitAssignment(c3);
        std::vector<MessageExt> got3 = drain(c3, 6);
        std::set<std::string> got3Set;
        for (const MessageExt& m : got3) got3Set.insert(bodyOf(m));
        bool onlyA = got3Set.size() == 6;
        for (const std::string& b : got3Set) {
            // body 形如 lite-00..lite-11：末位（个位）数字为偶数才是 TagA
            if (b.size() == 7 && (b[6] - '0') % 2 != 0) onlyA = false;
        }
        check("S5 仅收 TagA 且恰好 6 条", onlyA, "got=" + std::to_string(got3Set.size()));
        c3.shutdown();
    }

    // ---------------- S6 CONSUME_FROM_TIMESTAMP：14 位本地墙钟 ----------------
    std::printf("\nS6 CONSUME_FROM_TIMESTAMP：墙钟起点收全 + 时间戳→位点映射\n");
    {
        const int64_t wall = UtilAll::currentTimeMillis();

        DefaultLitePullConsumer c4(group4);
        c4.setNamesrvAddr(namesrv);
        c4.setPollTimeoutMillis(1000);
        c4.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_TIMESTAMP);
        c4.setConsumeTimestamp(UtilAll::timeMillisToHumanString3(wall - 30 * 60 * 1000));
        c4.subscribe(topic, "*");
        c4.start();
        waitAssignment(c4);
        std::vector<MessageExt> got4 = drain(c4, static_cast<size_t>(nMsg));
        std::set<std::string> got4Set;
        for (const MessageExt& m : got4) got4Set.insert(bodyOf(m));
        check("S6a 起点=" + c4.consumeTimestamp() + " 早于全部消息 → 收全 " +
                  std::to_string(nMsg) + " 条",
              got4Set == sent, "got=" + std::to_string(got4Set.size()));
        c4.shutdown();

        DefaultLitePullConsumer c5(group5);
        c5.setNamesrvAddr(namesrv);
        c5.setPollTimeoutMillis(1000);
        c5.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_TIMESTAMP);
        c5.setConsumeTimestamp(UtilAll::timeMillisToHumanString3(wall + 10 * 60 * 1000));
        c5.subscribe(topic, "*");
        c5.start();
        std::vector<MessageQueue> q5 = waitAssignment(c5);
        // 「未来时间戳的消费者收不到消息」在 Java 里不成立：新消费组只要队首仍在 commitlog 内，
        // broker 就直接回已提交位点 0（见 ConsumerManageProcessor + RebalanceLitePullImpl 的
        // readOffset 优先），consumeFromWhere 根本不参与。所以这里断言的是墙钟真正影响的量：
        // 时间戳 → 队列位置的映射。旧的 epoch 误解析会把两个方向同时翻转。
        int64_t sumPast = 0, sumFuture = 0;
        for (const MessageQueue& mq : q5) {
            sumPast += c5.offsetForTimestamp(mq, wall - 30 * 60 * 1000);
            sumFuture += c5.offsetForTimestamp(mq, wall + 10 * 60 * 1000);
        }
        check("S6b 30 分钟前 → 各队列队首", sumPast == 0,
              "sumOffset=" + std::to_string(sumPast));
        check("S6b 10 分钟后 → 越过全部 " + std::to_string(nMsg) + " 条",
              sumFuture == nMsg, "sumOffset=" + std::to_string(sumFuture));
        c5.shutdown();
    }

    c1.shutdown();

    std::printf("\nLitePullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
