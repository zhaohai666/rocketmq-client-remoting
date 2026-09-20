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
//   S7 可插拔分配策略（对应 Java setAllocateMessageQueueStrategy）
//      S7a 默认策略名 AVG；传 nullptr 不改（Java 会 NPE，这里挡在门口）
//      S7b AVG_BY_CIRCLE：同组两实例把 4 个队列按下标取模交叉切开，不重不漏
//      S7c CONFIG：只分配配置进去的 2 个队列 → assignment 恰为其一，
//          且 poll 到的消息 queueId 全部落在配置队列内（策略真的驱动了 rebalance）
//      S7d CONSISTENT_HASH：用**真实 clientId** 建环，线上 assignment 必须收敛到
//          「真实 mqAll/cidAll 离线跑同一策略」的预测。环可能一边 4 条一边 0 条
//          （Java 同款落点偏斜），所以判定只看「不重不漏 + 等于预测」
//      S7e MACHINE_ROOM_NEARBY：真实集群只有一个机房 ⇒ 装饰器必须原样透传内层策略；
//          resolver 的调用记录同时证明 rebalance 真的逐个问过队列/客户端的机房
//      S7f MACHINE_ROOM：真实 brokerName 不含 '@'，白名单怎么写都筛不出队列 ——
//          验的是「配错机房安静饿死」（分不到队列、poll 不到消息、不打崩重平衡）
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/allocate_strategy.h"
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

std::string join(const std::vector<std::string>& v) {
    std::string out;
    for (const std::string& s : v) out += s + " ";
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

// 按**时间**排空缓冲（不等条数）：策略只分到部分队列时，收条数是未知的
std::vector<MessageExt> drainFor(DefaultLitePullConsumer& c, int32_t millis) {
    std::vector<MessageExt> collected;
    const int64_t deadline = nowMs() + millis;
    while (nowMs() < deadline) {
        std::vector<MessageExt> batch = c.poll(1000);
        for (MessageExt& m : batch) collected.push_back(std::move(m));
    }
    return collected;
}

// 队列集 → key 集合（"brokerName#queueId"）
std::set<std::string> queueKeys(const std::vector<MessageQueue>& qs) {
    std::set<std::string> keys;
    for (const MessageQueue& q : qs) keys.insert(q.brokerName + "#" + std::to_string(q.queueId));
    return keys;
}

std::string keySetText(const std::set<std::string>& keys) {
    std::string out;
    for (const std::string& k : keys) out += k + " ";
    return out;
}

// 等两个实例把队列**分完**（分配收敛要两边各跑一轮心跳 + rebalance），最多 timeoutMs。
std::pair<std::vector<MessageQueue>, std::vector<MessageQueue>> waitSplitAssignment(
    DefaultLitePullConsumer& a, DefaultLitePullConsumer& b, size_t total, int32_t timeoutMs = 25000) {
    const int64_t deadline = nowMs() + timeoutMs;
    std::vector<MessageQueue> va, vb;
    while (nowMs() < deadline) {
        va = a.assignment();
        vb = b.assignment();
        std::set<std::string> sa = queueKeys(va), sb = queueKeys(vb);
        std::set<std::string> both, overlap;
        std::set_union(sa.begin(), sa.end(), sb.begin(), sb.end(), std::inserter(both, both.begin()));
        std::set_intersection(sa.begin(), sa.end(), sb.begin(), sb.end(),
                              std::inserter(overlap, overlap.begin()));
        // 分完 = 并集覆盖全部队列且两边无交集
        if (both.size() == total && overlap.empty() && !sa.empty() && !sb.empty()) return {va, vb};
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return {va, vb};
}

// 给 lite 消费者装上策略 + 订阅（未 start）。S7d~S7f 三个场景都用同一套配置面。
void setupConsumer(DefaultLitePullConsumer& c, const std::string& namesrv,
                   const std::string& instanceName,
                   const std::shared_ptr<AllocateMessageQueueStrategy>& strategy,
                   const std::string& topic) {
    c.setNamesrvAddr(namesrv);
    c.setInstanceName(instanceName);
    c.setPollTimeoutMillis(1000);
    // 存量消息在 S2 就发完了，新组默认 LAST 会跳过它们 → 收不到任何一条。
    // 这里要验的是「策略真的驱动了收发」，所以从队首起消费。
    c.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c.setAllocateMessageQueueStrategy(strategy);
    c.subscribe(topic, "*");
}

std::string snapshotText(const std::vector<DefaultLitePullConsumer*>& asserted,
                         const std::vector<std::set<std::string>>& live,
                         const std::vector<std::set<std::string>>& expected,
                         const std::vector<std::string>& cidAll) {
    std::string out;
    for (size_t i = 0; i < asserted.size(); ++i) {
        out += "cid=" + asserted[i]->clientId() + " live=[" + keySetText(live[i]) + "] predict=[" +
               keySetText(expected[i]) + "] | ";
    }
    out += "cidAll=[" + join(cidAll) + "]";
    return out;
}

/// 用**真实**输入（路由给的 mqAll + cidMembers 的真实 clientId 当 cidAll）离线跑策略，
/// 并**等线上 assignment() 收敛到这份预测**。
///
/// 为什么以预测为收敛条件，而不是「先等不重不漏、再比预测」：哈希环完全可能把 4 个
/// 队列全分给一个实例，另一边在首轮重平衡之前 assignment() 本来就是空 —— 那种初始态
/// 同样满足「不重不漏」，比出来的其实是「一边还没算」的快照。（Rust 版第一版就在这里翻车。）
///
/// Java RebalanceImpl#rebalanceByTopic 调策略前会把 mqAll、cidAll 都 Collections.sort，
/// 所以这里也得自己排：mqAll 由调用方排好，clientId 按字典序（同 String#compareTo）。
/// strategies 与 asserted 一一对应 —— S7f 要故意让两边配不同策略。
bool waitUntilPredictionConverged(const std::string& group, const std::vector<MessageQueue>& mqAll,
                                 const std::vector<DefaultLitePullConsumer*>& cidMembers,
                                 const std::vector<DefaultLitePullConsumer*>& asserted,
                                 const std::vector<std::shared_ptr<AllocateMessageQueueStrategy>>& strategies,
                                 std::string& detail, int32_t timeoutMs = 45000) {
    std::vector<std::string> cidAll;
    cidAll.reserve(cidMembers.size());
    for (DefaultLitePullConsumer* c : cidMembers) cidAll.push_back(c->clientId());
    std::sort(cidAll.begin(), cidAll.end());

    std::vector<std::set<std::string>> expected;
    expected.reserve(asserted.size());
    for (size_t i = 0; i < asserted.size(); ++i) {
        expected.push_back(queueKeys(strategies[i]->allocate(
            group, asserted[i]->clientId(), mqAll, cidAll)));
    }

    const int64_t deadline = nowMs() + timeoutMs;
    for (;;) {
        std::vector<std::set<std::string>> live;
        live.reserve(asserted.size());
        for (DefaultLitePullConsumer* c : asserted) live.push_back(queueKeys(c->assignment()));
        if (live == expected) {
            detail = snapshotText(asserted, live, expected, cidAll);
            return true;
        }
        if (nowMs() >= deadline) {
            detail = snapshotText(asserted, live, expected, cidAll);
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

// NEARBY 的落点：真实集群只有一个 broker，把队列和客户端都记成同一个机房，
// 于是 NEARBY 必定走「自己机房」那条分支、等价于内层策略。
// 同时留调用记录 —— 证明 rebalance 真的逐个问过队列和客户端的机房，
// 而不是策略对象换了个名字却没参与分配。
const char kRoom[] = "room1";

class OneRoom : public MachineRoomResolver {
public:
    std::string brokerDeployIn(const MessageQueue& messageQueue) override {
        std::lock_guard<std::mutex> lk(lock_);
        brokerCalls_.push_back(messageQueue.brokerName);
        return kRoom;
    }
    std::string consumerDeployIn(const std::string& clientId) override {
        std::lock_guard<std::mutex> lk(lock_);
        consumerCalls_.push_back(clientId);
        return kRoom;
    }
    std::vector<std::string> brokerCalls() const {
        std::lock_guard<std::mutex> lk(lock_);
        return brokerCalls_;
    }
    std::vector<std::string> consumerCalls() const {
        std::lock_guard<std::mutex> lk(lock_);
        return consumerCalls_;
    }

private:
    mutable std::mutex lock_;
    std::vector<std::string> brokerCalls_, consumerCalls_;
};

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
    const std::string group6 = "LitePG6Cpp_" + stamp;
    const std::string group7 = "LitePG7Cpp_" + stamp;
    const std::string group8 = "LitePG8Cpp_" + stamp;
    const std::string group9 = "LitePG9Cpp_" + stamp;
    const std::string group10 = "LitePG10Cpp_" + stamp;  // S7d 一致性哈希环
    const std::string group11 = "LitePG11Cpp_" + stamp;  // S7e 机房就近
    const std::string group12 = "LitePG12Cpp_" + stamp;  // S7f 机房配错
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

    // ---------------- S7 可插拔队列分配策略 ----------------
    std::printf("\nS7 可插拔队列分配策略（对应 Java setAllocateMessageQueueStrategy）\n");
    {
        std::vector<MessageQueue> allQueues;
        try {
            allQueues = c1.fetchMessageQueues(topic);
        } catch (const std::exception& e) {
            check("S7 fetchMessageQueues", false, e.what());
        }
        std::sort(allQueues.begin(), allQueues.end());
        const size_t totalQueues = allQueues.size();

        // S7a 默认策略 / null 守卫（Java 在 checkConfig 里拒绝 null）
        DefaultLitePullConsumer probe(group6);
        probe.setNamesrvAddr(namesrv);
        check("S7a 默认策略名 = AVG",
              probe.allocateMessageQueueStrategy() != nullptr
                  && probe.allocateMessageQueueStrategy()->getName() == "AVG",
              "name=" + (probe.allocateMessageQueueStrategy()
                             ? probe.allocateMessageQueueStrategy()->getName()
                             : std::string("<null>")));
        probe.setAllocateMessageQueueStrategy(nullptr);
        bool nullRejected = false;
        try {
            probe.subscribe(topic, "*");
            probe.start();
        } catch (const std::exception& e) {
            nullRejected =
                std::string(e.what()).find("allocateMessageQueueStrategy is null") != std::string::npos;
            check("S7a 策略为 null 时 start() 报 Java 同款文案", nullRejected, e.what());
        }
        if (!nullRejected) probe.shutdown();

        // S7b AVG_BY_CIRCLE：同组两实例交叉切分，不重不漏
        auto circle = std::make_shared<AllocateMessageQueueAveragelyByCircle>();
        DefaultLitePullConsumer ca(group7), cb(group7);
        ca.setNamesrvAddr(namesrv);
        cb.setNamesrvAddr(namesrv);
        ca.setInstanceName("s7ca");
        cb.setInstanceName("s7cb");
        ca.setAllocateMessageQueueStrategy(circle);
        cb.setAllocateMessageQueueStrategy(circle);
        check("S7b 替换后策略名 = AVG_BY_CIRCLE",
              ca.allocateMessageQueueStrategy()->getName() == "AVG_BY_CIRCLE");
        ca.subscribe(topic, "*");
        cb.subscribe(topic, "*");
        ca.start();
        cb.start();
        std::pair<std::vector<MessageQueue>, std::vector<MessageQueue>> split =
            waitSplitAssignment(ca, cb, totalQueues);
        std::set<std::string> ka = queueKeys(split.first), kb = queueKeys(split.second);
        std::set<std::string> overlap, both;
        std::set_intersection(ka.begin(), ka.end(), kb.begin(), kb.end(),
                              std::inserter(overlap, overlap.begin()));
        std::set_union(ka.begin(), ka.end(), kb.begin(), kb.end(), std::inserter(both, both.begin()));
        check("S7b 两实例分配无交集", overlap.empty(), "overlap=[" + keySetText(overlap) + "]");
        check("S7b 并集覆盖全部 " + std::to_string(totalQueues) + " 个队列", both == queueKeys(allQueues),
              "a=" + std::to_string(ka.size()) + " b=" + std::to_string(kb.size()));
        // 环形分配的签名：拿到的是「按下标取模」的交叉队列而非连续段
        // （4 队列 / 2 实例 → 各 2 条且下标步长为 2；AVG 会给连续两段）。
        std::set<int32_t> posA;
        for (size_t i = 0; i < allQueues.size(); ++i) {
            if (ka.count(allQueues[i].brokerName + "#" + std::to_string(allQueues[i].queueId))) {
                posA.insert(static_cast<int32_t>(i));
            }
        }
        bool circleShape = posA.size() == 2;
        int32_t prev = -1;
        for (int32_t p : posA) {
            if (prev >= 0 && (p - prev) % 2 != 0) circleShape = false;
            prev = p;
        }
        check("S7b 分配形状是交叉（步长 2），不是 AVG 的连续段",
              totalQueues != 4 || circleShape, "posA=" + std::to_string(posA.size()));
        ca.shutdown();
        cb.shutdown();

        // S7c CONFIG：只分配配置进去的一半队列，poll 到的消息也只能来自这些队列
        std::vector<MessageQueue> halfA(allQueues.begin(), allQueues.begin() + totalQueues / 2);
        std::vector<MessageQueue> halfB(allQueues.begin() + totalQueues / 2, allQueues.end());
        auto cfgA = std::make_shared<AllocateMessageQueueByConfig>(halfA);
        auto cfgB = std::make_shared<AllocateMessageQueueByConfig>(halfB);
        DefaultLitePullConsumer c6(group8), c7(group9);
        c6.setNamesrvAddr(namesrv);
        c7.setNamesrvAddr(namesrv);
        c6.setPollTimeoutMillis(1000);
        c7.setPollTimeoutMillis(1000);
        c6.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c7.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c6.setAllocateMessageQueueStrategy(cfgA);
        c7.setAllocateMessageQueueStrategy(cfgB);
        c6.subscribe(topic, "*");
        c7.subscribe(topic, "*");
        c6.start();
        c7.start();
        std::vector<MessageQueue> a6 = waitAssignment(c6);
        std::vector<MessageQueue> a7 = waitAssignment(c7);
        check("S7c CONFIG 只给配置进去的队列（无视 mqAll/cidAll）",
              queueKeys(a6) == queueKeys(halfA) && queueKeys(a7) == queueKeys(halfB),
              "a=" + keySetText(queueKeys(a6)) + " b=" + keySetText(queueKeys(a7)));
        // 两个 CONFIG 消费者各看一半：并集恰好是全部 12 条、交集为空 → 策略真的驱动了收发
        std::vector<MessageExt> g6 = drainFor(c6, 8000);
        std::vector<MessageExt> g7 = drainFor(c7, 8000);
        std::set<std::string> b6, b7, onCfgA, onCfgB;
        std::set<std::string> cfgKeysA = queueKeys(halfA), cfgKeysB = queueKeys(halfB);
        for (const MessageExt& m : g6) {
            b6.insert(bodyOf(m));
            if (cfgKeysA.count(m.getBrokerName() + "#" + std::to_string(m.getQueueId()))) {
                onCfgA.insert(bodyOf(m));
            }
        }
        for (const MessageExt& m : g7) {
            b7.insert(bodyOf(m));
            if (cfgKeysB.count(m.getBrokerName() + "#" + std::to_string(m.getQueueId()))) {
                onCfgB.insert(bodyOf(m));
            }
        }
        check("S7c CONFIG 消费者只收到自己配置队列里的消息", b6 == onCfgA && b7 == onCfgB,
              "a=" + std::to_string(b6.size()) + " aOnCfg=" + std::to_string(onCfgA.size()) + " b="
                  + std::to_string(b7.size()) + " bOnCfg=" + std::to_string(onCfgB.size()));
        std::set<std::string> unionBodies, interBodies;
        std::set_union(b6.begin(), b6.end(), b7.begin(), b7.end(),
                       std::inserter(unionBodies, unionBodies.begin()));
        std::set_intersection(b6.begin(), b6.end(), b7.begin(), b7.end(),
                              std::inserter(interBodies, interBodies.begin()));
        check("S7c 两半合起来恰好覆盖全部 " + std::to_string(nMsg) + " 条且互不重叠",
              unionBodies == sent && interBodies.empty(),
              "union=" + std::to_string(unionBodies.size()) + " inter=" + std::to_string(interBodies.size()));
        c6.shutdown();
        c7.shutdown();

        // S7d CONSISTENT_HASH：用**真实 clientId** 建环，线上分配要收敛到离线预测。
        // 判定只看「不重不漏」：环的落点由 clientId 的哈希决定，真实集群上完全可能
        // 一边 4 条、另一边 0 条（Java 同款偏斜），所以不能要求两边都非空。
        {
            auto ch = std::make_shared<AllocateMessageQueueConsistentHash>();
            DefaultLitePullConsumer d1(group10), d2(group10);
            setupConsumer(d1, namesrv, "s7da", ch, topic);
            setupConsumer(d2, namesrv, "s7db", ch, topic);
            check("S7d 替换后策略名 = CONSISTENT_HASH",
                  d1.allocateMessageQueueStrategy()->getName() == "CONSISTENT_HASH",
                  d1.allocateMessageQueueStrategy()->getName());
            d1.start();
            d2.start();
            std::string detail;
            const bool converged = waitUntilPredictionConverged(
                group10, allQueues, {&d1, &d2}, {&d1, &d2}, {ch, ch}, detail);
            check("S7d 线上分配收敛到一致性环的离线预测", converged, detail);
            std::set<std::string> k1 = queueKeys(d1.assignment()), k2 = queueKeys(d2.assignment());
            std::set<std::string> hit, both;
            std::set_intersection(k1.begin(), k1.end(), k2.begin(), k2.end(),
                                  std::inserter(hit, hit.begin()));
            std::set_union(k1.begin(), k1.end(), k2.begin(), k2.end(), std::inserter(both, both.begin()));
            check("S7d 两实例分配无交集", hit.empty(), "overlap=[" + keySetText(hit) + "]");
            check("S7d 并集覆盖全部 " + std::to_string(totalQueues) + " 个队列",
                  both == queueKeys(allQueues),
                  "a=" + std::to_string(k1.size()) + " b=" + std::to_string(k2.size()));
            std::vector<MessageExt> g1 = drainFor(d1, 8000), g2 = drainFor(d2, 8000);
            std::set<std::string> b1, b2;
            for (const MessageExt& m : g1) b1.insert(bodyOf(m));
            for (const MessageExt& m : g2) b2.insert(bodyOf(m));
            std::set<std::string> joined;
            std::set_union(b1.begin(), b1.end(), b2.begin(), b2.end(),
                           std::inserter(joined, joined.begin()));
            check("S7d 两实例合起来收到全部 " + std::to_string(nMsg) + " 条（环真的在驱动收发）",
                  joined == sent, "union=" + std::to_string(joined.size()));
            d1.shutdown();
            d2.shutdown();
        }

        // S7e MACHINE_ROOM_NEARBY：真实集群只有一个机房 ⇒ 装饰器必须原样透传内层策略。
        {
            auto inner = std::make_shared<AllocateMessageQueueConsistentHash>();
            auto resolver = std::make_shared<OneRoom>();
            auto nearby = std::make_shared<AllocateMachineRoomNearby>(inner, resolver);
            DefaultLitePullConsumer e1(group11), e2(group11);
            setupConsumer(e1, namesrv, "s7ea", nearby, topic);
            setupConsumer(e2, namesrv, "s7eb", nearby, topic);
            check("S7e 装饰后的策略名 = MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
                  e1.allocateMessageQueueStrategy()->getName() ==
                      "MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
                  e1.allocateMessageQueueStrategy()->getName());
            e1.start();
            e2.start();
            std::string detail;
            const bool converged = waitUntilPredictionConverged(
                group11, allQueues, {&e1, &e2}, {&e1, &e2}, {inner, inner}, detail);
            check("S7e NEARBY 的线上分配 == 内层环的离线预测", converged, detail);
            std::set<std::string> k1 = queueKeys(e1.assignment()), k2 = queueKeys(e2.assignment());
            std::set<std::string> hit, both;
            std::set_intersection(k1.begin(), k1.end(), k2.begin(), k2.end(),
                                  std::inserter(hit, hit.begin()));
            std::set_union(k1.begin(), k1.end(), k2.begin(), k2.end(), std::inserter(both, both.begin()));
            check("S7e NEARBY 两实例分配无交集且不漏",
                  hit.empty() && both == queueKeys(allQueues),
                  "a=" + std::to_string(k1.size()) + " b=" + std::to_string(k2.size()) +
                      " overlap=" + std::to_string(hit.size()));
            // resolver 真的被 rebalance 调用过，且看到的是真实 brokerName + 两个真实 clientId。
            const std::vector<std::string> brokerCalls = resolver->brokerCalls();
            const std::vector<std::string> consumerCalls = resolver->consumerCalls();
            std::set<std::string> seenBrokers(brokerCalls.begin(), brokerCalls.end());
            std::set<std::string> realBrokers;
            for (const MessageQueue& mq : allQueues) realBrokers.insert(mq.brokerName);
            check("S7e resolver 被逐个队列问过机房（" + std::to_string(brokerCalls.size()) + " 次）",
                  !brokerCalls.empty() && seenBrokers == realBrokers,
                  "brokers=[" + join(seenBrokers) + "]");
            std::set<std::string> callSet(consumerCalls.begin(), consumerCalls.end());
            check("S7e resolver 被问过两个真实 clientId",
                  callSet.count(e1.clientId()) == 1 && callSet.count(e2.clientId()) == 1,
                  "calls=[" + join(std::vector<std::string>(callSet.begin(), callSet.end())) + "]");
            e1.shutdown();
            e2.shutdown();
        }

        // S7f MACHINE_ROOM：真实 brokerName 是 broker-a，Java 的 split("@") 只切出 1 段
        // ⇒ 白名单怎么写都筛不出队列。要验的是「配错机房安静饿死」，不是打崩 rebalance。
        {
            auto room = std::make_shared<AllocateMessageQueueByMachineRoom>(
                std::set<std::string>{kRoom});
            check("S7f 策略名 = MACHINE_ROOM 且白名单能读回",
                  room->getName() == "MACHINE_ROOM" && room->getConsumeridcs().count(kRoom) == 1,
                  "idcs=[" + join(room->getConsumeridcs()) + "]");
            // 对照组：同组另一个消费者用默认 AVG。两边各自算策略（Java 就是各算各的），
            // 对照组能分到队列 ⇒ 这一组的心跳注册 + 重平衡确实跑起来了，
            // 于是「f1 为空」只能归因于机房筛选，而不是链路没通。
            auto avg = std::make_shared<AllocateMessageQueueAveragely>();
            DefaultLitePullConsumer f1(group12), f2(group12);
            setupConsumer(f1, namesrv, "s7fa", room, topic);
            setupConsumer(f2, namesrv, "s7fb", avg, topic);
            f1.start();
            f2.start();
            std::vector<MessageQueue> ctrl = waitAssignment(f2);
            check("S7f 同组对照组（AVG）正常分到队列", !ctrl.empty(),
                  "ctrl=[" + keySetText(queueKeys(ctrl)) + "]");
            check("S7f 机房不匹配真实 brokerName → 一条都不分（不报错也不误吃）",
                  f1.assignment().empty(), "assignment=[" + keySetText(queueKeys(f1.assignment())) + "]");
            // 对照组按**两个** cid 算 AVG 只拿到自己那半边 —— 它没替配错的那位兜底（Java 同语义）。
            std::string detail;
            const bool converged = waitUntilPredictionConverged(
                group12, allQueues, {&f1, &f2}, {&f1, &f2}, {room, avg}, detail);
            check("S7f 两边线上分配各自收敛到自己策略的离线预测", converged, detail);
            std::vector<MessageExt> starved = drainFor(f1, 5000);
            check("S7f 被饿死的一方 poll 不到消息也不抛错", starved.empty(),
                  "got=" + std::to_string(starved.size()));
            f1.shutdown();
            f2.shutdown();
        }
    }

    c1.shutdown();

    std::printf("\nLitePullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
