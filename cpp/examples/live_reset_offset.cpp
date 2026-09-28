// 220 RESET_CONSUMER_CLIENT_OFFSET（Java MQClientInstance.resetOffset:1403-1450）真机验证。
//
// 与 python/verify_reset_offset_live.py、rust/examples/live_reset_offset.rs、.NET 的对应
// 场景同题、逐条对应。
//
// 220 是 broker 推给**消费端**的重置指令；管理端那笔 INVOKE_BROKER_TO_RESET_OFFSET(222)
// 的响应只是一张「每个队列重置到哪」的表，真正让消费端改位点的是 broker 随后 oneway 推的
// 220（Broker2Client.resetOffset:181-238）。broker 只在 useServerSideResetOffset=false
// 时才走这条推送路径（默认 true 在 AdminBrokerProcessor:2255-2263 直接服务端改位点、一台
// 消费端都不通知），所以本验证先把该开关热改成 false（回读确认），跑完还原。
//
// 离线单测（tests/test_reset_offset.cpp）锁死了请求体两种形状与「撤队列 + 代号 +1 + 新位点
// 经撤销尾巴落盘」的本地状态；下面这些事只有真集群能证明：
//
//   S1「回退重置立刻生效 + 在途批次作废」——位点从 10 往回重置到 3，三段判据：
//      a) broker 上的已提交位点在 ~2s 内变成 3：窗口内**只有**重置路径那次 persist 会写它
//         （周期落盘已拉长到 60s），只写内存表的实现在这里原地不动（broker 停在 10）；
//      b) listener 里卡着的旧批次（重置前取回的 offset 10）放行后，它的 ack 必须整批作废
//         —— 采样点：放行旧批次、新队列的**第一批**已进 listener 且还没 ack 时，本地已消费
//         位点必须还是"没有记录 / ≤3"；没有代号闸门的实现这时会跳到 11；
//      c) 队列被真正重建：3..14 每一批都**重投一次**（旧缓冲里没 ack 的 11..14 也随之
//         作废，只能作为重投的一部分出现），放行一轮断言一轮。
//
//   S2「前跳 + 恢复」——timestamp=-1 重置到 maxOffset：位点直接跳到 10，中间 4..9 一条都不
//      投；随后新消息照常消费、位点继续前进。
//
// 说明：为什么必须在 listener 里逐批设闸——"重置生效"的失败模式在真机上只表现为"位点没动、
// 消息照旧不重投"，admin 侧完全看不出异常；而把时序判断塞进离线单测又依赖不了真 broker 的
// 推送与 offset 表。逐批可控才有"旧批次已放行、新队列第一批还没 ack"这个采样点。
//
// 用法：./rmq_live_reset_offset 127.0.0.1:9876
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
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
#include "rocketmq/common/types.h"
#include "rocketmq/common/util_all.h"
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

// 墙钟毫秒（Unix epoch）。**按时间戳重置只用它，不用 nowMs()**：broker 的
// `getOffsetInQueueByTime` 拿它跟消息的 storeTimestamp（broker 的墙钟）比大小，而
// steady_clock 在 macOS 上是"开机以来的毫秒"（比真实 epoch 小三个数量级）——用错时钟的
// 症状是静默的：目标位点恒定 0（时间戳落在所有消息之前），重置"成功"但把整条队列从头发。
int64_t wallMs() {
    return UtilAll::currentTimeMillis();
}

bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 200) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

// 用于"很快就要发生"的断言：采样比 waitUntil 密。
bool waitBefore(const std::function<bool()>& pred, int64_t windowMs) {
    return waitUntil(pred, windowMs, 50);
}

const int32_t kMsgs = 10;
const int64_t kBackTarget = 3;
const char* kSwitchKey = "useServerSideResetOffset";

std::string fmtOffsets(const std::vector<int64_t>& v) {
    std::string s = "[";
    for (size_t i = 0; i < v.size(); ++i) {
        if (i != 0) s += ",";
        s += std::to_string(v[i]);
    }
    return s + "]";
}

std::string fmtTable(const std::map<MessageQueue, int64_t>& table) {
    std::string s = "{";
    for (const auto& kv : table) {
        if (s.size() > 1) s += ", ";
        s += "q" + std::to_string(kv.first.queueId) + ":" + std::to_string(kv.second);
    }
    return s + "}";
}

// 逐批设闸：每一批都停在闸门上，由测试逐批放行 —— 批与批之间的本地状态因此可以被采样。
//
// S1 的关键采样点（旧批次已放行、新队列第一批还没 ack）只有在"逐批可控"时才存在；一次性
// 放行的 listener 会把 11（旧批次 ack）与随后的重投混在一个瞬间里。
class SteppingListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            std::vector<int64_t> batch;
            for (const MessageExt& m : msgs) {
                batch.push_back(m.queueOffset);
            }
            batches_.push_back(batch);
        }
        std::unique_lock<std::mutex> lk(mtx_);
        // 上限只防死锁：正常路径由测试逐批 release()
        cv_.wait_for(lk, std::chrono::seconds(90), [this] { return released_; });
        released_ = false;  // 一批一放：与 Python Event.set()+clear() 同构
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    void release() {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            released_ = true;
        }
        cv_.notify_all();
    }

    size_t batchCount() {
        std::lock_guard<std::mutex> lk(mtx_);
        return batches_.size();
    }

    std::vector<int64_t> offsets() {
        std::lock_guard<std::mutex> lk(mtx_);
        std::vector<int64_t> out;
        for (const std::vector<int64_t>& b : batches_) {
            out.insert(out.end(), b.begin(), b.end());
        }
        return out;
    }

private:
    std::mutex mtx_;
    std::condition_variable cv_;
    bool released_ = false;
    std::vector<std::vector<int64_t>> batches_;
};

bool waitBatches(SteppingListener& l, size_t n, int64_t timeoutMs) {
    return waitUntil([&] { return l.batchCount() >= n; }, timeoutMs, 50);
}

class RecordingListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const MessageExt& m : msgs) {
            offsets_.push_back(m.queueOffset);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    std::vector<int64_t> offsets() {
        std::lock_guard<std::mutex> lk(mtx_);
        return offsets_;
    }

private:
    std::mutex mtx_;
    std::vector<int64_t> offsets_;
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
        producer = std::make_shared<DefaultMQProducer>("GID_cpp_live_ro_pg_" + stamp);
        producer->setNamesrvAddr(ns);
        producer->setInstanceName("cpp-live-ro-prod-" + stamp);
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

std::string readFlag(DefaultMQAdminExt& admin, const std::string& addr, const std::string& key) {
    try {
        PropertyMap cfg = admin.getBrokerConfig(addr);
        auto it = cfg.find(key);
        return it == cfg.end() ? std::string("<missing>") : it->second;
    } catch (const std::exception& e) {
        return std::string("<") + e.what() + ">";
    }
}

bool setServerSideReset(Fixture& fx, const std::string& value) {
    PropertyMap props;
    props[kSwitchKey] = value;
    try {
        fx.admin.updateBrokerConfig(fx.brokerAddr, props);
    } catch (const std::exception& e) {
        std::printf("  [diag] updateBrokerConfig(%s=%s) failed: %s\n", kSwitchKey, value.c_str(),
                    e.what());
        return false;
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    return readFlag(fx.admin, fx.brokerAddr, kSwitchKey) == value;
}

// 消费者视角下该 topic 的队列 key（用路由上的 mq 拼，与 offsetKey 同口径）。
std::string queueKeyOf(DefaultMQPushConsumer& c, const std::string& topic) {
    for (const MessageQueue& mq : c.fetchSubscribeMessageQueues(topic)) {
        if (mq.queueId == 0) {
            return DefaultMQPushConsumer::offsetKey(mq);
        }
    }
    return std::string();
}

// 本端口内存里的「已消费位点」；nullopt = 该队列在表里没有记录。
//
// 220 的重置语义（Java removeOffset）就是"重置后表里没有旧位点"，所以 nullopt 与 0 必须
// 能分开：只有 nullopt 才能证明旧批次的 ack 没有把位点推回去。
std::optional<int64_t> localOffset(DefaultMQPushConsumer& c, const std::string& topic) {
    const std::map<MessageQueue, int64_t> status = c.getConsumerStatus(topic);
    for (const auto& kv : status) {
        if (kv.first.queueId == 0) return kv.second;
    }
    return std::nullopt;
}

std::string offText(const std::optional<int64_t>& v) {
    return v ? std::to_string(*v) : "None";
}

std::shared_ptr<DefaultMQPushConsumer> startConsumer(
    Fixture& fx, const std::string& group, const std::string& role, const std::string& topic,
    std::shared_ptr<MessageListener> listener, int32_t batchSize, int32_t persistIntervalMs) {
    auto c = std::make_shared<DefaultMQPushConsumer>(group);
    c->setNamesrvAddr(fx.namesrv);
    c->setInstanceName("cpp-live-ro-" + role + "-" + fx.stamp);
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

bool brokerIs(Fixture& fx, const std::string& group, const std::string& topic, int64_t want) {
    const std::vector<Row> rows = fx.offsets(group, topic);
    if (rows.empty()) return false;
    for (const Row& r : rows) {
        if (!r.hasCommitted || r.committed != want) return false;
    }
    return true;
}

// ------------------------------------------------ S1：回退重置 + 在途批次作废

void s1BackwardReset(Fixture& fx, const std::string& topic, const std::string& group) {
    std::printf("\n--- S1：回退重置（10 → 3）立刻落盘 + 在途批次整批作废 ---\n");
    // 前置：先用普通 listener 把 broker 上的位点做成 10（reset 要求该组在 broker 上有记录）
    auto l0 = std::make_shared<RecordingListener>();
    std::shared_ptr<DefaultMQPushConsumer> c0 =
        startConsumer(fx, group, "s1seed", topic, l0, /*batchSize=*/1, /*persist=*/0);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    int64_t tMid = 0;
    for (int32_t i = 0; i < kMsgs; ++i) {
        Message m(topic, "rb-" + std::to_string(i));
        fx.producer->send(m);
        if (i == 2) {
            tMid = wallMs();
            // 让第 3 条（offset 3）与前面三条拉开存储时间，后面按时间戳重置才能稳定命中 3
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }
    const bool seeded =
        waitUntil([&] { return l0->offsets().size() == static_cast<size_t>(kMsgs); }, 30000, 200);
    check("S1-前置：消费者消费掉 " + std::to_string(kMsgs) + " 条", seeded,
          "arrivals=" + fmtOffsets(l0->offsets()));

    bool primed = waitUntil([&] { return brokerIs(fx, group, topic, kMsgs); }, 20000, 500);
    check("S1-前置：broker 位点周期落盘到 " + std::to_string(kMsgs) +
              "（reset 要求组在 broker 上有记录）",
          primed, fmtRows(fx.offsets(group, topic)));
    c0->shutdown();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 主力消费者：逐批闸住 + 周期落盘 60s ⇒ 窗口内唯一能改 broker 位点的路径是重置自带的那次
    auto listener = std::make_shared<SteppingListener>();
    std::shared_ptr<DefaultMQPushConsumer> c =
        startConsumer(fx, group, "s1", topic, listener, /*batchSize=*/1, /*persist=*/60000);
    const int64_t tStart = nowMs();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    for (int32_t i = kMsgs; i < kMsgs + 5; ++i) {
        Message m(topic, "rb-" + std::to_string(i));
        fx.producer->send(m);
    }

    std::string key;
    size_t pending = 0;
    const bool arranged = waitUntil(
        [&] {
            if (key.empty()) key = queueKeyOf(*c, topic);
            if (key.empty()) return false;
            pending = c->pendingMessages(key).size();
            return listener->batchCount() >= 1 && pending == 4;
        },
        30000, 50);
    const std::vector<int64_t> firstArrivals = listener->offsets();
    check("S1-窗口就绪：1 条在途（offset 10 卡在 listener）+ 4 条留在缓冲",
          arranged && !firstArrivals.empty() && firstArrivals[0] == kMsgs,
          "arrivals=" + fmtOffsets(firstArrivals) + " pending=" + std::to_string(pending));
    if (!arranged || key.empty()) {
        listener->release();
        c->shutdown();
        return;
    }

    // 等周期落盘的**首跳**过去（Java initialDelay＝start 后 10s，之后才是 60s 周期）。
    // 不等它，重置后那次 persist 会与首跳混在一起，"broker 位点之所以是 3" 就说不清。
    waitUntil([&] { return nowMs() - tStart > 12000; }, 15000, 200);
    check("S1-首跳周期落盘已过（此后 60s 内不再有周期写）",
          nowMs() - tStart > 12000 && brokerIs(fx, group, topic, kMsgs),
          fmtRows(fx.offsets(group, topic)));

    // 按时间戳重置到 3：tMid 落在第 3 条与第 4 条之间 ⇒ getOffsetInQueueByTime 命中 3，
    // 3 < consumerOffset(10) 且 isForce=true ⇒ broker 推 {mq: 3}
    const int64_t resetTs = tMid + 500;
    const int64_t t0 = nowMs();
    std::map<MessageQueue, int64_t> table =
        fx.admin.resetOffsetByTimestamp(topic, group, resetTs, /*isForce=*/true);
    check("S1-222 响应里的目标位点就是 3",
          table.size() == 1 && table.begin()->second == kBackTarget, fmtTable(table));

    const bool in2s = waitBefore([&] { return brokerIs(fx, group, topic, kBackTarget); }, 2000);
    char detail[160];
    std::snprintf(detail, sizeof(detail), "elapsed=%.1fs %s", (nowMs() - t0) / 1000.0,
                  fmtRows(fx.offsets(group, topic)).c_str());
    check("S1-broker 位点 ~2s 内变成 3（重置路径自带的那次 persist，周期落盘=60s）", in2s,
          detail);

    std::optional<int64_t> off = localOffset(*c, topic);
    check("S1-重置后本地表里没有旧位点（Java removeOffset；新位点经撤销尾巴出去）",
          !off.has_value(),
          "local_offset=" + offText(off) + " epoch=" + std::to_string(c->queueEpoch(key)));

    // 放行旧批次：它的 ack 属于已被撤销的 ProcessQueue，必须作废
    listener->release();
    const bool second = waitBatches(*listener, 2, 20000);
    off = localOffset(*c, topic);
    check("S1-旧批次 ack 作废（放行后新队列第一批 offset 3 已在途时，本地位点仍未越过 3）",
          second && (!off.has_value() || *off <= kBackTarget),
          "local_offset=" + offText(off) + " arrivals=" + fmtOffsets(listener->offsets()));

    // 逐批放行走完重投：3..14 每条重投一次（旧缓冲里 11..14 也已作废，只能作为重投出现）
    std::vector<int64_t> expected;
    expected.push_back(kMsgs);
    for (int64_t i = kBackTarget; i < kMsgs + 5; ++i) {
        expected.push_back(i);
    }
    for (size_t n = 2; n <= expected.size(); ++n) {
        listener->release();
        if (n < expected.size()) {
            waitBatches(*listener, n + 1, 15000);
        }
    }
    const std::vector<int64_t> arr = listener->offsets();
    check("S1-队列被真正重建：重投序列是 " + fmtOffsets(expected), arr == expected,
          "arrivals=" + fmtOffsets(arr));
    waitUntil([&] { return localOffset(*c, topic) == std::optional<int64_t>(kMsgs + 5); }, 10000,
              50);

    std::vector<Row> rows = fx.offsets(group, topic);
    bool allBack = !rows.empty();
    for (const Row& r : rows) {
        if (!r.hasCommitted || r.committed != kBackTarget) allBack = false;
    }
    check("S1-窗口内只有重置那次写 broker（位点仍停在 3，周期落盘=60s）", allBack,
          fmtRows(rows));

    // 关停会同步落盘一次：重投的 ack 才是最终值（15 = 最后一条 14 的 +1）
    c->shutdown();
    const bool advanced = waitUntil([&] { return brokerIs(fx, group, topic, kMsgs + 5); }, 15000,
                                    500);
    check("S1-关停落盘把重投的 ack 写回 broker（位点前进到 15）", advanced,
          fmtRows(fx.offsets(group, topic)));
}

// ------------------------------------------------ S2：前跳重置 + 恢复

void s2ForwardSkip(Fixture& fx, const std::string& topic, const std::string& group) {
    std::printf("\n--- S2：前跳重置（timestamp=-1 → maxOffset）不重投 + 新消息照常消费 ---\n");
    auto listener = std::make_shared<SteppingListener>();
    // 周期落盘用默认 5s：本场景要先靠首跳（start 后 10s）在 broker 上给这个组建记录
    //（Broker2Client.resetOffset 对 queryOffset==-1 的组直接回 SYSTEM_ERROR），
    // 再等一个 >5s 的窗口做重置 —— 重置那笔 persist 与周期写不同刻，判据仍然干净。
    std::shared_ptr<DefaultMQPushConsumer> c =
        startConsumer(fx, group, "s2", topic, listener, /*batchSize=*/1, /*persist=*/0);
    const int64_t tStart = nowMs();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    for (int32_t i = 0; i < kMsgs; ++i) {
        Message m(topic, "rs-" + std::to_string(i));
        fx.producer->send(m);
    }

    // 逐批放行前 3 条：第 4 条（offset 3）留在 listener 里当"在途批次"
    for (int n = 1; n <= 4; ++n) {
        if (!waitBatches(*listener, static_cast<size_t>(n), 30000)) break;
        if (n < 4) listener->release();
    }
    std::vector<int64_t> arr = listener->offsets();
    const bool arranged = listener->batchCount() >= 4 && arr.size() >= 4 && arr[0] == 0 &&
                          arr[1] == 1 && arr[2] == 2 && arr[3] == 3;
    check("S2-窗口就绪：前 3 条已 ack、第 4 条卡在 listener", arranged,
          "arrivals=" + fmtOffsets(arr));
    if (!arranged) {
        listener->release();
        c->shutdown();
        return;
    }

    // 等首跳落盘把组建出来（本地已消费位点 3 = 前三条的 ack），并留出 >5s 的静默窗口
    const bool primed = waitUntil(
        [&] { return nowMs() - tStart > 12000 && brokerIs(fx, group, topic, kBackTarget); },
        20000, 500);
    check("S2-前置：broker 上该组有记录（q0:3），且下一笔周期写还在 5s 之外", primed,
          fmtRows(fx.offsets(group, topic)));

    const int64_t t0 = nowMs();
    std::map<MessageQueue, int64_t> table =
        fx.admin.resetOffsetByTimestamp(topic, group, /*timestamp=*/-1, /*isForce=*/true);
    check("S2-222 响应里的目标位点就是 maxOffset(10)",
          table.size() == 1 && table.begin()->second == kMsgs, fmtTable(table));

    const bool in2s = waitBefore([&] { return brokerIs(fx, group, topic, kMsgs); }, 2000);
    char detail[160];
    std::snprintf(detail, sizeof(detail), "elapsed=%.1fs %s", (nowMs() - t0) / 1000.0,
                  fmtRows(fx.offsets(group, topic)).c_str());
    check("S2-broker 位点 ~2s 内前跳到 10（重置路径自带的那次 persist）", in2s, detail);

    listener->release();
    std::this_thread::sleep_for(std::chrono::seconds(3));
    arr = listener->offsets();
    const std::vector<int64_t> seedArrivals = {0, 1, 2, 3};
    check("S2-被跳过的 4..9 一条都不投（在途那条的 ack 也作废）", arr == seedArrivals,
          "arrivals=" + fmtOffsets(arr));

    Message after(topic, "rs-new");
    fx.producer->send(after);
    const bool gotNew = waitBatches(*listener, 5, 20000);
    listener->release();
    arr = listener->offsets();
    const std::vector<int64_t> wantArrivals = {0, 1, 2, 3, kMsgs};
    check("S2-重建后的队列从队尾续跑（新消息 offset 10 正常投递）",
          gotNew && arr == wantArrivals, "arrivals=" + fmtOffsets(arr));

    // release() 只让 listener 返回；ack 是消费线程随后落的。不等本地位点真的推到 11 就
    // 关停，关停那次 persist 可能跑在 ack 之前（写回的还是 10）——这是断言竞态，不是语义问题。
    waitUntil([&] { return localOffset(*c, topic) == std::optional<int64_t>(kMsgs + 1); }, 10000,
              50);
    c->shutdown();
    const bool advanced = waitUntil([&] { return brokerIs(fx, group, topic, kMsgs + 1); }, 15000,
                                    500);
    check("S2-关停落盘把新消息的 ack 写回 broker（位点前进到 11）", advanced,
          fmtRows(fx.offsets(group, topic)));
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_reset_offset <namesrv>\n");
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

    const std::string topic1 = "CppLiveRoBack" + fx.stamp;
    const std::string topic2 = "CppLiveRoSkip" + fx.stamp;
    const std::string g1 = "GID_cpp_live_ro_back_" + fx.stamp;
    const std::string g2 = "GID_cpp_live_ro_skip_" + fx.stamp;

    try {
        // 开关热改：useServerSideResetOffset=false（220 推送路径的前提）
        check("开关热改：useServerSideResetOffset=false（220 推送路径的前提）",
              setServerSideReset(fx, "false"),
              "回读 useServerSideResetOffset=" + readFlag(fx.admin, fx.brokerAddr, kSwitchKey));

        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic1, 1);
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic2, 1);
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::printf("topic1=%s topic2=%s（各 1 队列，各 %d 条消息）\n", topic1.c_str(),
                    topic2.c_str(), kMsgs);

        s1BackwardReset(fx, topic1, g1);
        s2ForwardSkip(fx, topic2, g2);
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
    check("还原 useServerSideResetOffset=true", setServerSideReset(fx, "true"),
          "回读 useServerSideResetOffset=" + readFlag(fx.admin, fx.brokerAddr, kSwitchKey));
    fx.shutdown();

    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
