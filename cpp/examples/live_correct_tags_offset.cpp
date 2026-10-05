// correctTagsOffset（Java `DefaultMQPushConsumerImpl:713-717`，调用点 `:394-401`）真机验证。
//
// 与 `python/verify_correct_tags_offset_live.py`、`rust/examples/live_correct_tags_offset.rs`、
// C# 的对应场景同题、逐条对应（S1–S4）。
//
// 离线单测（tests/test_correct_tags_offset.cpp）锁的是**判据**；这里锁真机上两件离线锁不住的事：
//   A. **修正确实走到了 broker**：位点最终由 UPDATE_CONSUMER_OFFSET 落盘，只有真集群能证明
//      broker 上的已提交位点前移了。
//   B. **是在零投递的前提下前移的**：订阅表达式永不匹配时，broker 侧按组订阅过滤
//      （PullMessageProcessor 拿 heartbeat 注册的 SubscriptionData）→ PULL_RETRY_IMMEDIATELY
//      → 客户端 NO_MATCHED_MSG。没有修正时这条队列的位点永远停在"未提交"。
//
// 场景：
//   S1 对照组：TagA 订阅正常消费 5 条 —— 证明消息确实在队列里，且"已提交位点 == 各队列
//      maxOffset"这个数值口径就是常规消费的落点（排除 S2 的假绿）。
//   S2 NO_MATCHED_MSG：TagB 订阅（永不匹配）。断言 listener 0 条 + 每条队列的已提交位点
//      == 该队列 maxOffset。
//   S3 NO_NEW_MSG：同一个消费者启动时自动补上 %RETRY%<group>（该队列空）→ PULL_NOT_FOUND
//      → NO_NEW_MSG。断言 broker 上出现值 == maxOffset(0) 的记录。
//   S4 收尾：再等一个静默窗口，listener 依旧是 0 条（修正不会凭空投递）。
//
// 两个离线测不出、写错又很安静的口径：
//   * 队列列表只能按**路由**查：`%RETRY%<group>` 是 broker 建的，只有 1 个队列；照主 topic
//     的 4 个队列去查会把"只有 q0 有记录"误报成失败。
//   * 查位点用 `setZeroIfNotFound=false` 才能区分「没记录」与「记录是 0」：broker 对前者回
//     QUERY_NOT_FOUND（本函数返回 false），对后者回 SUCCESS + offset=0。用 true 会让 S3
//     在"记录没建出来"时照样通过。
//
// 用法：./rmq_live_correct_tags_offset 127.0.0.1:9876
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/mq_client.h"
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

// 真机投递受 broker 长轮询/流控与同机负载影响，固定 sleep 会把「实现没问题」测成假失败。
bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 250) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

const int32_t kQueues = 4;
const int32_t kMsgs = 5;
// 位点落盘首跳 10s + 周期 5s（默认值），再留一个周期的余量。
const int64_t kPersistWindowMs = 45000;

// 只记到达条数与 tag：这一趟关心的是"一条都不该来"，不是消息内容。
struct TagSink {
    std::mutex mtx;
    std::vector<std::string> tags;

    size_t size() {
        std::lock_guard<std::mutex> lk(mtx);
        return tags.size();
    }
};

class TagListener : public MessageListenerConcurrently {
public:
    explicit TagListener(TagSink& sink) : sink_(sink) {}

    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) {
            sink_.tags.push_back(m.getTags());
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    TagSink& sink_;
};

struct Row {
    MessageQueue mq;
    int64_t maxOffset = -1;
    bool hasCommitted = false;  // false = broker 上查无记录（QUERY_NOT_FOUND）
    int64_t committed = -1;
};

bool allCommittedEqMax(const std::vector<Row>& rows) {
    if (rows.empty()) return false;
    for (const Row& r : rows) {
        if (!r.hasCommitted || r.committed != r.maxOffset) return false;
    }
    return true;
}

std::string fmtRows(const std::vector<Row>& rows) {
    std::string s;
    for (const Row& r : rows) {
        if (!s.empty()) s += " ";
        s += "q" + std::to_string(r.mq.queueId) + ":" +
             (r.hasCommitted ? std::to_string(r.committed) : std::string("None")) + "/" +
             std::to_string(r.maxOffset);
    }
    return s;
}

int64_t sumMax(const std::vector<Row>& rows) {
    int64_t s = 0;
    for (const Row& r : rows) s += r.maxOffset;
    return s;
}

int64_t sumCommitted(const std::vector<Row>& rows) {
    int64_t s = 0;
    for (const Row& r : rows) s += r.hasCommitted ? r.committed : -1;
    return s;
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
            throw std::runtime_error("nameServer 无 broker 注册");
        }
        brokerAddr = addrs[0];
        producer = std::make_shared<DefaultMQProducer>("GID_cpp_live_cto_pg_" + stamp);
        producer->setNamesrvAddr(ns);
        producer->setInstanceName("cpp-live-cto-prod-" + stamp);
        producer->start();
    }

    std::string topic() const { return "CppLiveCto" + stamp; }

    // 该 topic 在路由上的真实队列列表。
    //
    // 不能用「主 topic 有几个队列」当通用假设：`%RETRY%<group>` 是 broker 建的，
    // 只有 1 个队列（Python/Rust 参考实现同样按路由查）。照 4 个去查会把"只有 q0 有记录"
    // 误报成失败。
    std::vector<MessageQueue> queues(const std::string& topicName) {
        TopicRouteData route = admin.examineTopicRoute(topicName);
        return route.getAllMessageQueue(topicName);
    }

    // [(mq, maxOffset, committed)]；hasCommitted=false 表示 broker 上查无记录。
    //
    // `setZeroIfNotFound=false`：broker 查无记录回 QUERY_NOT_FOUND（返回 false），
    // 有记录才回 SUCCESS + offset —— 空队列记录 0 与「没记录」必须能分开，否则 S3 是假绿。
    std::vector<Row> offsets(const std::string& group, const std::string& topicName) {
        std::vector<Row> out;
        for (const MessageQueue& mq : queues(topicName)) {
            Row r;
            r.mq = mq;
            try {
                r.maxOffset = admin.client().getMaxOffset(mq, 5000, brokerAddr);
            } catch (const std::exception&) {
                r.maxOffset = -1;
            }
            int64_t off = 0;
            try {
                r.hasCommitted =
                    admin.client().queryConsumerOffset(group, mq, off, 5000, brokerAddr,
                                                       /*setZeroIfNotFound=*/false);
                r.committed = off;
            } catch (const std::exception&) {
                r.hasCommitted = false;
            }
            out.push_back(r);
        }
        return out;
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

std::shared_ptr<DefaultMQPushConsumer> startConsumer(Fixture& fx, const std::string& group,
                                                     const std::string& role,
                                                     const std::string& topic,
                                                     const std::string& expression, TagSink& sink,
                                                     const char* fromWhere) {
    auto c = std::make_shared<DefaultMQPushConsumer>(group);
    c->setNamesrvAddr(fx.namesrv);
    c->setInstanceName("cpp-live-cto-" + role + "-" + fx.stamp);
    if (fromWhere != nullptr) {
        c->setConsumeFromWhere(fromWhere);
    }
    c->setConsumeMessageBatchMaxSize(3);
    c->setMessageListener(std::make_shared<TagListener>(sink));
    c->subscribe(topic, expression);
    c->start();
    return c;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_correct_tags_offset <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    Fixture fx;
    try {
        fx.start(nsAddr);
    } catch (const std::exception& e) {
        check("集群探活", false, e.what());
        return 1;
    }
    check("集群探活", true, "broker=" + fx.brokerAddr);

    const std::string topic = fx.topic();
    const std::string gCtrl = "GID_cpp_live_cto_ctrl_" + fx.stamp;
    const std::string gTest = "GID_cpp_live_cto_test_" + fx.stamp;
    const std::string retryTopic = MixAll::getRetryTopic(gTest);

    std::shared_ptr<DefaultMQPushConsumer> ctrl;
    std::shared_ptr<DefaultMQPushConsumer> test;
    std::vector<Row> ctrlRows;
    std::vector<Row> testRows;
    std::vector<Row> retryRows;

    try {
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic, kQueues);
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::printf("topic=%s queues=%d\n", topic.c_str(), kQueues);

        // ---------------- S1 对照组 ----------------
        TagSink ctrlSink;
        ctrl = startConsumer(fx, gCtrl, "ctrl", topic, "TagA", ctrlSink,
                             ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        for (int32_t i = 0; i < kMsgs; ++i) {
            Message m(topic, "cto-" + std::to_string(i));
            m.setTags("TagA");
            fx.producer->send(m);
        }
        std::printf("S1: 已发送 %d 条 TagA，等对照组消费...\n", kMsgs);
        const bool got = waitUntil([&] { return ctrlSink.size() >= static_cast<size_t>(kMsgs); }, 30000);
        check("S1-对照组（TagA）收齐 " + std::to_string(kMsgs) + " 条 —— 消息确实在队列里",
              got && ctrlSink.size() == static_cast<size_t>(kMsgs),
              "arrivals=" + std::to_string(ctrlSink.size()));

        const bool ctrlOk = waitUntil(
            [&] {
                try {
                    ctrlRows = fx.offsets(gCtrl, topic);
                } catch (const std::exception& e) {
                    std::printf("  [WARN] read offsets: %s\n", e.what());
                    return false;
                }
                return allCommittedEqMax(ctrlRows);
            },
            25000, 500);
        check("S1-对照组的已提交位点 == 各队列 maxOffset（数值口径）", ctrlOk,
              fmtRows(ctrlRows));
        check("S1-对照组确实把消息推进了队列（maxOffset 总和 > 0）", sumMax(ctrlRows) > 0,
              "maxSum=" + std::to_string(sumMax(ctrlRows)));
        ctrl->shutdown();
        ctrl.reset();

        // ---------------- S2 NO_MATCHED_MSG：永不匹配的订阅，零投递但位点要走 ----------------
        TagSink testSink;
        test = startConsumer(fx, gTest, "test", topic, "TagB", testSink, nullptr);
        std::printf("S2: 消费者（TagB，永不匹配）已启动，等空应答修正落盘（首跳 10s + 周期 5s）...\n");
        const bool testOk = waitUntil(
            [&] {
                try {
                    testRows = fx.offsets(gTest, topic);
                } catch (const std::exception& e) {
                    std::printf("  [WARN] read offsets: %s\n", e.what());
                    return false;
                }
                return allCommittedEqMax(testRows);
            },
            kPersistWindowMs, 500);
        check("S2-零投递（listener 一条都没收到）", testSink.size() == 0,
              "arrivals=" + std::to_string(testSink.size()));
        check("S2-每条队列的已提交位点都 == 该队列 maxOffset（空应答修正生效）", testOk,
              fmtRows(testRows));
        check("S2-修正后的位点总和 == 对照组（同一条队列的最大位点）",
              sumCommitted(testRows) == sumMax(ctrlRows),
              "test=" + std::to_string(sumCommitted(testRows)) + " ctrl=" +
                  std::to_string(sumMax(ctrlRows)));

        // ---------------- S3 NO_NEW_MSG：%RETRY%<group> 空队列也要留下位点记录 ----------------
        std::printf("S3: 等 %s 的位点记录（空队列 NO_NEW_MSG）...\n", retryTopic.c_str());
        const bool retryOk = waitUntil(
            [&] {
                try {
                    retryRows = fx.offsets(gTest, retryTopic);
                } catch (const std::exception& e) {
                    std::printf("  [WARN] read retry offsets: %s\n", e.what());
                    return false;
                }
                return allCommittedEqMax(retryRows);
            },
            kPersistWindowMs, 500);
        check("S3-" + retryTopic + " 上出现位点记录且等于 maxOffset", retryOk,
              fmtRows(retryRows));
        bool allZero = !retryRows.empty();
        for (const Row& r : retryRows) {
            if (!r.hasCommitted || r.committed != 0) allZero = false;
        }
        check("S3-该位点确实是 0（空队列的 nextBeginOffset）", allZero, fmtRows(retryRows));

        // ---------------- S4 收尾：静默窗口内仍然零投递 ----------------
        std::this_thread::sleep_for(std::chrono::seconds(6));
        check("S4-整轮下来 listener 依旧是 0 条（修正不会凭空投递）", testSink.size() == 0,
              "arrivals=" + std::to_string(testSink.size()));
    } catch (const std::exception& e) {
        check("验证过程抛出异常", false, e.what());
    }

    // ---------------- 清理 ----------------
    for (const std::shared_ptr<DefaultMQPushConsumer>& c : {ctrl, test}) {
        if (!c) continue;
        try {
            c->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (consumer shutdown 失败: %s)\n", e.what());
        }
    }
    try {
        fx.admin.deleteTopic(topic);
        std::printf("    (deleteTopic(%s) OK)\n", topic.c_str());
    } catch (const std::exception& e) {
        std::printf("    (deleteTopic(%s) 失败: %s)\n", topic.c_str(), e.what());
    }
    for (const std::string& g : {gCtrl, gTest}) {
        try {
            fx.admin.deleteSubscriptionGroup(fx.brokerAddr, g, true);
        } catch (const std::exception& e) {
            std::printf("    (deleteSubscriptionGroup(%s) 失败: %s)\n", g.c_str(), e.what());
        }
    }
    fx.shutdown();

    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
