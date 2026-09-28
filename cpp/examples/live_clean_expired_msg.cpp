// cleanExpiredMsg 挂起逃生口真机验证（Java `ConsumeMessageConcurrentlyService:68-88/192-200`
// ＋ `ProcessQueue.cleanExpiredMsg:75-127`）。
//
// 与 `python/verify_clean_expired_msg_live.py`、`rust/examples/live_clean_expired_msg.rs`、
// .NET 的对应场景同题、逐条对应（A0–A5）。
//
// 离线单测（tests/test_clean_expired_msg.cpp）锁的是**判据**（选条/阈值/上限/摘除闸门）；
// 这里锁真机上两件离线锁不住的事：
//   A. **清扫真的会开火**：需要一个 listener 挂着不返回超过 consumeTimeout 分钟，清扫线程
//      把这条消息 sendMessageBack（delayLevel 3）——整条链路上客户端不能丢它、也不能投两次。
//   B. **重投真的回到 broker**：%RETRY%<group> 的拉取循环独立于挂住的消费线程，消息必须
//      重新出现在本地缓冲、被第二次投递且 reconsumeTimes=1（broker 侧计数）。
//
// 这条路径坏掉的样子是**静默**的：卡住的消息把该队列位点与分发循环一起钉死，没有任何异常
// 或超时可见，只能从"消息发了却永远不来第二次"反推。所以必须真机跑出「挂起 → 清扫 →
// 重投到达」的完整证据链，光靠"客户端不报错"什么都证明不了。
//
// 场景（`setConsumeTimeout(1)`，清扫周期与阈值都是 1 分钟；Java 的过期判据是**严格大于**，
// 所以清扫在第二个 tick 命中，约 start+120s）：
//   A0 业务队列已分配（排除自动订阅的 %RETRY%<group>）。
//   A1 首投在 30s 内到达并挂住；reconsumeTimes=0；期间在册视图里能看到这条消息
//      且带 CONSUME_START_TIME（本轮盖章）。
//   A2 清扫命中（**核心判据**）：listener 仍挂着时轮询在册视图 —— 消息从里面消失即清扫
//      回投并摘除；距今必须 >60s（排除别的路径动手）。
//   A3 重投真的到了 broker 侧：%RETRY% 队列的本地缓冲里出现这条消息（≤40s）。
//   A4 放行后重新消费：reconsumeTimes=1、同一条 body 全程只到两次、与首投相隔 >60s。
//   A5 位点收尾：挂住的 listener 返回后业务队列已提交位点走到 1
//      （Java removeMessage 的列表仍含这条已被清扫的消息）。
//
// 用法：./rmq_live_clean_expired_msg 127.0.0.1:9876   （约 4 分钟，等两个清扫周期）
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
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

bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 500) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

// 第一次投递就挂住不返回；放行后恢复正常返回 CONSUME_SUCCESS。
class HungListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        bool first = false;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            ++calls_;
            first = (calls_ == 1);
            for (const MessageExt& m : msgs) {
                arrivals_.emplace_back(std::string(m.body),
                                       m.getReconsumeTimes(),
                                       static_cast<int64_t>(nowMs()));
            }
        }
        if (first) {
            std::unique_lock<std::mutex> lk(mtx_);
            firstSeen_ = true;
            cv_.notify_all();
            // 挂起窗口：等清扫动手 + 验证脚本放行（300s 上限兜底，防止脚本崩了卡死线程）
            releaseCv_.wait_for(lk, std::chrono::seconds(300), [this] { return release_; });
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    bool waitFirst(int64_t timeoutMs) {
        std::unique_lock<std::mutex> lk(mtx_);
        return cv_.wait_for(lk, std::chrono::milliseconds(timeoutMs), [this] { return firstSeen_; });
    }

    void release() {
        std::lock_guard<std::mutex> lk(mtx_);
        release_ = true;
        releaseCv_.notify_all();
    }

    int calls() {
        std::lock_guard<std::mutex> lk(mtx_);
        return calls_;
    }

    // 某 body 的到达记录 [(reconsumeTimes, 时刻)]
    std::vector<std::pair<int32_t, int64_t>> times(const std::string& body) {
        std::lock_guard<std::mutex> lk(mtx_);
        std::vector<std::pair<int32_t, int64_t>> out;
        for (const auto& a : arrivals_) {
            if (std::get<0>(a) == body) out.emplace_back(std::get<1>(a), std::get<2>(a));
        }
        return out;
    }

private:
    std::mutex mtx_;
    std::condition_variable cv_;
    std::condition_variable releaseCv_;
    bool firstSeen_ = false;
    bool release_ = false;
    int calls_ = 0;
    std::vector<std::tuple<std::string, int32_t, int64_t>> arrivals_;
};

bool containsBody(const std::vector<MessageExt>& msgs, const std::string& body) {
    for (const MessageExt& m : msgs) {
        if (m.body == body) return true;
    }
    return false;
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
        producer = std::make_shared<DefaultMQProducer>("GID_cpp_live_ce_pg_" + stamp);
        producer->setNamesrvAddr(ns);
        producer->setInstanceName("cpp-live-ce-prod-" + stamp);
        producer->start();
    }

    std::string topic() const { return "CppLiveCe" + stamp; }

    // 已提交位点之和（业务 topic 只有 1 个队列；路由上查全，别拿"主 topic 有 4 个队列"
    // 的假设硬套）。broker 查无记录 → 返回 -1。
    int64_t committedOffset(const std::string& group, const std::string& topicName) {
        TopicRouteData route = admin.examineTopicRoute(topicName);
        const std::vector<MessageQueue> mqs = route.getAllMessageQueue(topicName);
        int64_t total = 0;
        for (const MessageQueue& mq : mqs) {
            int64_t off = 0;
            bool found = false;
            try {
                found = admin.client().queryConsumerOffset(group, mq, off, 5000, brokerAddr,
                                                           /*setZeroIfNotFound=*/false);
            } catch (const std::exception&) {
                found = false;
            }
            total += found ? off : -1;
        }
        return mqs.empty() ? -1 : total;
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

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_clean_expired_msg <namesrv>\n");
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
    const std::string group = "GID_cpp_live_ce_" + fx.stamp;
    const std::string retryTopic = MixAll::getRetryTopic(group);

    std::shared_ptr<DefaultMQPushConsumer> consumer;
    auto listener = std::make_shared<HungListener>();
    const std::string body = "ce-cpp-1";

    try {
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic, 1);
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::printf("topic=%s group=%s\n", topic.c_str(), group.c_str());

        consumer = std::make_shared<DefaultMQPushConsumer>(group);
        consumer->setNamesrvAddr(nsAddr);
        consumer->setInstanceName("cpp-live-ce-" + fx.stamp);
        consumer->setMessageListener(listener);
        consumer->setConsumeTimeout(1);  // 清扫周期与阈值都变成 1 分钟（Java 同字段同语义）
        consumer->subscribe(topic, "*");
        consumer->start();

        // ---------- A0 业务队列（排除 %RETRY%） ----------
        std::string bizKey;
        const bool assigned = waitUntil(
            [&] {
                for (const std::string& k : consumer->assignedQueueKeys()) {
                    if (k.rfind("%RETRY%", 0) != 0) {
                        bizKey = k;
                        return true;
                    }
                }
                return false;
            },
            30000);
        check("A0-业务队列已分配（%RETRY% 队列不算）", assigned, "key=" + bizKey);
        if (!assigned) {
            throw std::runtime_error("业务队列未分配");
        }

        // ---------- A1 基线：第一投挂住 ----------
        Message msg(topic, body);
        fx.producer->send(msg);
        const bool firstOk = listener->waitFirst(30000);
        check("A1-首投在 30s 内到达并挂住", firstOk, "calls=" + std::to_string(listener->calls()));
        const auto first = listener->times(body);
        check("A1-首投 reconsumeTimes=0", !first.empty() && first[0].first == 0,
              "times=" + std::to_string(first.empty() ? -1 : first[0].first));
        bool registered = false;
        std::string stampSeen;
        for (const MessageExt& m : consumer->processQueueEntries(bizKey)) {
            if (m.body == body) {
                registered = true;
                stampSeen = m.getProperty(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP);
            }
        }
        check("A1-挂住期间消息登记在册（在 listener 手里）", registered);
        check("A1-在册副本带本轮 CONSUME_START_TIME（清扫靠它判过期）", !stampSeen.empty(),
              "stamp=" + stampSeen);

        // ---------- A2 清扫命中：listener 还挂着，登记里已经没了 ----------
        const int64_t t0 = nowMs();
        const bool swept = waitUntil(
            [&] { return !containsBody(consumer->processQueueEntries(bizKey), body); },
            210000, 1000);
        const double elapsed = static_cast<double>(nowMs() - t0) / 1000.0;
        check("A2-清扫在 listener 仍挂起时收走了这条消息（约 start+120s）", swept,
              "elapsed=" + std::to_string(elapsed) + "s calls=" +
                  std::to_string(listener->calls()));
        check("A2-收走时间晚于一个清扫阈值（>60s，排除别的路径动手）", elapsed > 60.0,
              "elapsed=" + std::to_string(elapsed) + "s");

        // ---------- A3 回投真的到了 broker：%RETRY% 缓冲里出现 ----------
        // 挂起的 listener 把分发线程占住，重投消息只能停在 %RETRY% 队列的本地缓冲里。
        std::string retryKeyFound;
        const bool back = waitUntil(
            [&] {
                for (const std::string& k : consumer->assignedQueueKeys()) {
                    if (k.rfind("%RETRY%", 0) != 0) continue;
                    retryKeyFound = k;
                    if (containsBody(consumer->pendingMessages(k), body)) return true;
                }
                return false;
            },
            40000, 1000);
        check("A3-回投消息出现在 " + retryTopic +
                  " 的本地缓冲（broker 真收到了 sendMessageBack）",
              back, "retryKey=" + retryKeyFound);

        // ---------- A4 放行：重投被重新消费 ----------
        listener->release();
        const bool second = waitUntil([&] { return listener->times(body).size() >= 2; }, 60000, 1000);
        auto times = listener->times(body);
        check("A4-放行后重新消费到（reconsumeTimes=1）", second,
              "times=" + std::to_string(times.size()));
        check("A4-第二次投递 reconsumeTimes=1（broker 侧重投计数）",
              times.size() >= 2 && times[1].first == 1,
              "times=" + std::to_string(times.size() >= 2 ? times[1].first : -1));
        check("A4-同一条 body 全程只到两次（清算一次回投，无重复投递）", times.size() == 2,
              "times=" + std::to_string(times.size()));
        check("A4-第二次投递与首投相隔 >60s（不是 listener 自己造成的重投）",
              times.size() >= 2 && (times[1].second - times[0].second) > 60000,
              "gap=" + std::to_string(times.size() >= 2
                                          ? (times[1].second - times[0].second) / 1000
                                          : -1) +
                  "s");

        // ---------- A5 位点收尾 ----------
        const bool offsetOk = waitUntil([&] { return fx.committedOffset(group, topic) == 1; },
                                        30000, 1000);
        check("A5-挂住的 listener 返回后业务队列位点走到 1（Java removeMessage 含已清扫条目）",
              offsetOk, "offset=" + std::to_string(fx.committedOffset(group, topic)));
    } catch (const std::exception& e) {
        check("验证过程抛出异常", false, e.what());
    }

    // ---------------- 清理 ----------------
    listener->release();
    if (consumer) {
        try {
            consumer->shutdown();
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
    try {
        fx.admin.deleteSubscriptionGroup(fx.brokerAddr, group, true);
    } catch (const std::exception& e) {
        std::printf("    (deleteSubscriptionGroup(%s) 失败: %s)\n", group.c_str(), e.what());
    }
    fx.shutdown();

    std::printf("\nCleanExpiredMsg(cpp): PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
