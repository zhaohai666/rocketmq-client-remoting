// OFFSET_ILLEGAL 纠错分支（Java `DefaultMQPushConsumerImpl:402-427`）真机验证。
//
// 与 `python/verify_offset_illegal_live.py`、`rust/examples/live_offset_illegal.rs`、
// C# 的对应场景同题、逐条对应（S1/S2）。
//
// 这条分支做四件事：位点改用 broker 给的修正值（`setNextOffset`）→ 丢掉这条队列上已取回
// 未消费的消息（`ProcessQueue.setDropped(true)`）→ 把修正位点**立刻**落盘
//（`updateAndFreezeOffset` + `persist`）→ 撤掉队列让 rebalance 按修正位点重建
//（`removeProcessQueue` + `rebalanceImmediately`）。离线单测（tests/test_offset_illegal_recover.cpp）
// 只能锁住本地状态怎么清、哪个 ack 被作废，下面两件事只有真集群能证明：
//
//   S1「丢队列」——broker 判定位点非法时，队列上**已取回但还没消费/没 ack** 的消息必须整批
//      作废。做法：让 listener 卡住第一条（在途 1 条、缓冲里 2 条），再用
//      resetOffsetByQueueId 把位点重置到 3（服务端重置 ⇒ 下一笔 pull 被
//      PullMessageProcessor:539-545 短路成 OFFSET_RESET ⇒ 客户端 OFFSET_ILLEGAL）。
//      修复前：缓冲里的第 1、2 条照常投递（listener 实收 3 条）；修复后：只剩在途的第 0 条，
//      且它的 ack 因队列已被丢（Java ConsumeMessageConcurrentlyService:267）而作废。最后再发
//      第 4 条，验证重建后的队列从修正位点续跑、冻结已随重建解除（新消息的 ack 让 broker 上
//      的位点继续前进到 4）。
//
//   S2「立刻落盘」——纠错后的位点必须马上推给 broker，不能等周期落盘。做法：利用
//      resetOffsetByQueueId 两笔 RPC 非原子（第 1 笔 commitOffset 无区间校验先落库、第 2 笔
//      222 被 resetOffsetInner 拒绝，见 admin.h 里记的 5.5.1 实测）的既有行为，把 broker 上的
//      已提交位点做成非法值 103，再让一个 persistConsumerOffsetIntervalMillis=60000 的新消费者
//      从 103 起拉。窗口内唯一能把 103 写回 3（maxOffset）的路径就是纠错分支自带的那次 persist，
//      且全程零投递。
//
// 说明：发现延迟 = 客户端在途长轮询的返回时间 + broker 侧的巡检周期。本端口按下发
// suspendTimeoutMillis=20000（Java `PullAPIWrapper.brokerSuspendMaxTimeMillis` 默认值 20s）
// 请求挂起，broker 的 PullRequestHoldService 每 5s 巡检一次到期请求，命中前那笔 pull 不会
// 重读 resetOffsetTable。Python 侧实测 24.3s，与 Java 同构（长轮询语义如此，不是缺陷）
// —— 这里给 45s 余量。
//
// 用法：./rmq_live_offset_illegal 127.0.0.1:9876
#include <chrono>
#include <condition_variable>
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

bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 200) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

const int32_t kMsgs = 3;
const int64_t kIllegalTarget = 103;

std::string fmtOffsets(const std::vector<int64_t>& v) {
    std::string s = "[";
    for (size_t i = 0; i < v.size(); ++i) {
        if (i != 0) s += ",";
        s += std::to_string(v[i]);
    }
    return s + "]";
}

bool contains(const std::vector<int64_t>& v, int64_t x) {
    for (int64_t y : v) {
        if (y == x) return true;
    }
    return false;
}

// 第 1 批卡在闸门上（维持"1 条在途 + 其余在缓冲"的窗口），放行后照常返回。
class GatedListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            for (const MessageExt& m : msgs) {
                arrivals_.push_back(m.queueOffset);
            }
        }
        std::unique_lock<std::mutex> lk(mtx_);
        // 上限只防死锁：正常路径下由 release() 显式放行，且必须晚于纠错
        cv_.wait_for(lk, std::chrono::seconds(90), [this] { return released_; });
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    void release() {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            released_ = true;
        }
        cv_.notify_all();
    }

    std::vector<int64_t> arrivals() {
        std::lock_guard<std::mutex> lk(mtx_);
        return arrivals_;
    }

private:
    std::mutex mtx_;
    std::condition_variable cv_;
    bool released_ = false;
    std::vector<int64_t> arrivals_;
};

class RecordingListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const MessageExt& m : msgs) {
            arrivals_.push_back(m.queueOffset);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    std::vector<int64_t> arrivals() {
        std::lock_guard<std::mutex> lk(mtx_);
        return arrivals_;
    }

private:
    std::mutex mtx_;
    std::vector<int64_t> arrivals_;
};

struct Row {
    MessageQueue mq;
    int64_t maxOffset = -1;
    bool hasCommitted = false;  // false = broker 上查无记录（QUERY_NOT_FOUND）
    int64_t committed = -1;
};

std::string fmtRows(const std::vector<Row>& rows) {
    std::string s;
    for (const Row& r : rows) {
        if (!s.empty()) s += " ";
        s += "q" + std::to_string(r.mq.queueId) + ":" +
             (r.hasCommitted ? std::to_string(r.committed) : std::string("None")) + "/max" +
             std::to_string(r.maxOffset);
    }
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
        admin.setTimeoutMillis(10000);
        admin.start();
        ClusterInfo cluster = admin.fetchBrokerClusterInfo();
        const std::vector<std::string> addrs = cluster.getBrokerAddrs();
        if (addrs.empty()) {
            throw std::runtime_error("nameServer 无 broker 注册");
        }
        brokerAddr = addrs[0];
        producer = std::make_shared<DefaultMQProducer>("GID_cpp_live_ilo_pg_" + stamp);
        producer->setNamesrvAddr(ns);
        producer->setInstanceName("cpp-live-ilo-prod-" + stamp);
        producer->start();
    }

    std::vector<MessageQueue> queues(const std::string& topicName) {
        TopicRouteData route = admin.examineTopicRoute(topicName);
        return route.getAllMessageQueue(topicName);
    }

    // [(mq, maxOffset, committed)]；hasCommitted=false 表示 broker 上查无记录。
    // setZeroIfNotFound=false：空记录 0 与「没记录」必须能分开。
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

// 消费者视角下该 topic 的队列 key（用路由上的 mq 拼，与 offsetKey 同口径）。
std::string queueKeyOf(DefaultMQPushConsumer& c, const std::string& topic) {
    for (const MessageQueue& mq : c.fetchSubscribeMessageQueues(topic)) {
        if (mq.queueId == 0) {
            return DefaultMQPushConsumer::offsetKey(mq);
        }
    }
    return std::string();
}

std::shared_ptr<DefaultMQPushConsumer> startConsumer(
    Fixture& fx, const std::string& group, const std::string& role, const std::string& topic,
    std::shared_ptr<MessageListener> listener, int32_t batchSize, int32_t persistIntervalMs) {
    auto c = std::make_shared<DefaultMQPushConsumer>(group);
    c->setNamesrvAddr(fx.namesrv);
    c->setInstanceName("cpp-live-ilo-" + role + "-" + fx.stamp);
    c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c->setConsumeMessageBatchMaxSize(batchSize);
    if (persistIntervalMs > 0) {
        c->setPersistConsumerOffsetIntervalMillis(persistIntervalMs);
    }
    c->setMessageListener(listener);
    c->subscribe(topic, "*");
    c->start();
    return c;
}

// ------------------------------------------------ S1：整批作废 + 按修正位点重建

void s1DropAndRebuild(Fixture& fx, const std::string& topic, const std::string& group) {
    std::printf("\n--- S1：OFFSET_ILLEGAL 整批作废在途/缓冲消息并按修正位点重建 ---\n");
    auto listener = std::make_shared<GatedListener>();
    std::shared_ptr<DefaultMQPushConsumer> c =
        startConsumer(fx, group, "s1", topic, listener, /*batchSize=*/1, /*persist=*/0);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    for (int32_t i = 0; i < kMsgs; ++i) {
        Message m(topic, "ilo-" + std::to_string(i));
        fx.producer->send(m);
    }
    std::printf("S1: 已发送 %d 条（单队列），等 listener 卡住第 1 条...\n", kMsgs);

    std::string key = queueKeyOf(*c, topic);
    if (key.empty()) {
        check("S1-取到队列 key", false, "路由里没有 q0");
        listener->release();
        c->shutdown();
        return;
    }

    size_t pending = 0;
    const bool arranged = waitUntil(
        [&] {
            pending = c->pendingMessages(key).size();
            const size_t arrived = listener->arrivals().size();
            return pending == static_cast<size_t>(kMsgs - 1) && arrived == 1;
        },
        30000);
    check("S1-窗口就绪：1 条在途（listener 被闸住）+ " + std::to_string(kMsgs - 1) +
              " 条留在缓冲",
          arranged, "pending=" + std::to_string(pending) +
                        " arrivals=" + fmtOffsets(listener->arrivals()));

    // 服务端重置到 maxOffset：RPC2 写 resetOffsetTable，下一笔 pull 命中
    // PullMessageProcessor:539-545 被短路成 OFFSET_RESET（nextBeginOffset=修正值）
    fx.admin.resetOffsetByQueueId(fx.brokerAddr, group, topic, 0, kMsgs);
    std::printf("S1: 已发出 resetOffsetByQueueId(->%d)，等下一笔 pull 取走服务端重置...\n", kMsgs);

    const bool dropped = waitUntil([&] { return c->queueEpoch(key) >= 1; }, 45000);
    check("S1-broker 判定位点非法后本端丢弃该队列（ProcessQueue.setDropped：代号 +1）", dropped,
          "epoch=" + std::to_string(c->queueEpoch(key)) +
              " arrivals=" + fmtOffsets(listener->arrivals()));

    const bool atCorrected = waitUntil(
        [&] {
            const std::vector<Row> rows = fx.offsets(group, topic);
            for (const Row& r : rows) {
                if (!r.hasCommitted || r.committed != kMsgs) return false;
            }
            return !rows.empty();
        },
        20000, 500);
    check("S1-broker 上的位点停在修正值 " + std::to_string(kMsgs) +
              "（本场景两笔重置 RPC 已先写过一次，弱断言）",
          atCorrected, fmtRows(fx.offsets(group, topic)));

    listener->release();
    std::this_thread::sleep_for(std::chrono::seconds(6));
    std::vector<int64_t> arr = listener->arrivals();
    check("S1-缓冲里已取回的 " + std::to_string(kMsgs - 1) + " 条被整批作废（第 1、2 条永不投递）",
          !arr.empty() && !contains(arr, 1) && !contains(arr, 2), "arrivals=" + fmtOffsets(arr));

    Message after(topic, "ilo-after");
    fx.producer->send(after);
    const bool gotNew = waitUntil(
        [&] {
            const std::vector<int64_t> a = listener->arrivals();
            return contains(a, kMsgs);
        },
        20000, 500);
    arr = listener->arrivals();
    check("S1-重建后的队列从修正位点续拉（第 " + std::to_string(kMsgs) +
              " 条新消息正常投递，历史拿过的不重投）",
          gotNew && !contains(arr, 1) && !contains(arr, 2), "arrivals=" + fmtOffsets(arr));

    const bool advanced = waitUntil(
        [&] {
            const std::vector<Row> rows = fx.offsets(group, topic);
            for (const Row& r : rows) {
                if (!r.hasCommitted || r.committed != kMsgs + 1) return false;
            }
            return !rows.empty();
        },
        25000, 1000);
    check("S1-冻结随重建解除（新消息的 ack 让 broker 位点继续前进到 " +
              std::to_string(kMsgs + 1) + "）",
          advanced, fmtRows(fx.offsets(group, topic)));
    c->shutdown();
}

// ------------------------------------------------ S2：纠错把修正位点立刻落盘

void s2ImmediatePersist(Fixture& fx, const std::string& topic, const std::string& group) {
    std::printf("\n--- S2：纠错把修正位点立刻落盘（不等周期落盘） ---\n");
    auto l1 = std::make_shared<RecordingListener>();
    std::shared_ptr<DefaultMQPushConsumer> c =
        startConsumer(fx, group, "s2a", topic, l1, /*batchSize=*/1, /*persist=*/0);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    for (int32_t i = 0; i < kMsgs; ++i) {
        Message m(topic, "ilo2-" + std::to_string(i));
        fx.producer->send(m);
    }
    const bool consumed = waitUntil(
        [&] { return l1->arrivals().size() >= static_cast<size_t>(kMsgs); }, 30000, 500);
    check("S2-前置：消费者先正常消费掉 " + std::to_string(kMsgs) + " 条", consumed,
          "arrivals=" + fmtOffsets(l1->arrivals()));
    // shutdown 会同步 persist 一次，此后 broker 上的位点是 3；必须先停掉它，否则它的
    // 周期/关停落盘会把下面种进去的非法值覆盖回去。
    c->shutdown();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    bool rejected = false;
    std::string remark;
    try {
        fx.admin.resetOffsetByQueueId(fx.brokerAddr, group, topic, 0, kIllegalTarget);
    } catch (const std::exception& e) {
        rejected = true;
        remark = std::string(e.what()).substr(0, 140);
    }
    check("S2-前置：越界目标被 resetOffsetInner 拒绝（第 2 笔 RPC）", rejected, remark);

    std::vector<Row> rows = fx.offsets(group, topic);
    bool allIllegal = !rows.empty();
    for (const Row& r : rows) {
        if (!r.hasCommitted || r.committed != kIllegalTarget) allIllegal = false;
    }
    check("S2-前置：第 1 笔 commitOffset 已把非法位点 " + std::to_string(kIllegalTarget) +
              " 落库（两笔 RPC 非原子）",
          allIllegal, fmtRows(rows));

    auto l2 = std::make_shared<RecordingListener>();
    // 周期落盘拉长到 60s：窗口内唯一能改写 broker 位点的路径是纠错分支自带的立即 persist
    std::shared_ptr<DefaultMQPushConsumer> c2 =
        startConsumer(fx, group, "s2b", topic, l2, /*batchSize=*/1, /*persist=*/60000);
    std::printf("S2: 新消费者从非法位点 %lld 起拉，等纠错把 broker 位点写回 %d（周期落盘=60s）...\n",
                static_cast<long long>(kIllegalTarget), kMsgs);
    const int64_t t0 = nowMs();

    const bool back = waitUntil(
        [&] {
            const std::vector<Row> rs = fx.offsets(group, topic);
            for (const Row& r : rs) {
                if (!r.hasCommitted || r.committed != kMsgs) return false;
            }
            return !rs.empty();
        },
        20000, 500);
    const double elapsedS = (nowMs() - t0) / 1000.0;
    char detail[160];
    std::snprintf(detail, sizeof(detail), "elapsed=%.1fs %s", elapsedS,
                  fmtRows(fx.offsets(group, topic)).c_str());
    check("S2-broker 位点由 " + std::to_string(kIllegalTarget) + " 纠回 " +
              std::to_string(kMsgs) + "（纠错分支自带的那次 persist）",
          back, detail);

    // 再等一个静默窗口：位点被纠回后不会回头重投 0..2，也不会再被改写
    std::this_thread::sleep_for(std::chrono::seconds(6));
    check("S2-全程零投递（修正位点落在历史消息之后，一条都不下发）",
          l2->arrivals().empty(), "arrivals=" + fmtOffsets(l2->arrivals()));
    bool still = true;
    rows = fx.offsets(group, topic);
    for (const Row& r : rows) {
        if (!r.hasCommitted || r.committed != kMsgs) still = false;
    }
    check("S2-静默窗口后位点仍停在 " + std::to_string(kMsgs), still && !rows.empty(),
          fmtRows(rows));
    c2->shutdown();
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_offset_illegal <namesrv>\n");
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

    const std::string topic1 = "CppLiveIloDrop" + fx.stamp;
    const std::string topic2 = "CppLiveIloPersist" + fx.stamp;
    const std::string g1 = "GID_cpp_live_ilo_drop_" + fx.stamp;
    const std::string g2 = "GID_cpp_live_ilo_persist_" + fx.stamp;

    try {
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic1, 1);
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic2, 1);
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::printf("topic1=%s topic2=%s（各 1 队列，%d 条消息）\n", topic1.c_str(),
                    topic2.c_str(), kMsgs);

        s1DropAndRebuild(fx, topic1, g1);
        s2ImmediatePersist(fx, topic2, g2);
    } catch (const std::exception& e) {
        check("验证过程抛出异常", false, e.what());
    }

    for (const std::string& t : {topic1, topic2}) {
        try {
            fx.admin.deleteTopic(t);
            std::printf("    (deleteTopic(%s) OK)\n", t.c_str());
        } catch (const std::exception& e) {
            std::printf("    (deleteTopic(%s) 失败: %s)\n", t.c_str(), e.what());
        }
    }
    for (const std::string& g : {g1, g2}) {
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
