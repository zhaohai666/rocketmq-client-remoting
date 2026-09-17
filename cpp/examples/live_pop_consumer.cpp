// POP 消费侧真机验证（对齐 Java ConsumeMessagePopConcurrentlyService）。
// 用法：rmq_live_pop_consumer 127.0.0.1:9876
//
// 与 rmq_live_pop（协议管道）的区别：这里验证**消费循环** ——
//   POP 弹出 → 投递 listener → 成功则 ack、失败则延长不可见时间 → 不重复投递。
// 消费循环不查/不提交消费位点，进度完全由 broker 的 checkpoint 跟踪。
//
// 场景：
//   S1 POP 消费：起消费者 → 发 12 条 → 全部收到、无重复、body 集合一致
//   S2 ack 生效：收满后再观察一段时间（> invisibleTime）→ 不应被重复投递
//   S3 RECONSUME_LATER：listener 持续返回 RECONSUME_LATER → 按延迟档位重新投递
//   S4 多队列：消息确实落到了多个队列且都被消费（POP 是逐队列弹的）
//
// ⚠ S2 的观测窗口必须 > popInvisibleTime，否则"ack 完全没发出去"也看不出重复投递
//   （消息还没到复活时间）——这是最容易伪装成通过的假绿。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;
std::mutex gMtx;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(),
                    detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(),
                    detail.empty() ? "" : ("  " + detail).c_str());
    }
}

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

int64_t stamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool waitUntil(const std::function<bool()>& pred, int32_t timeoutMs) {
    const int64_t deadline = stamp() + timeoutMs;
    while (stamp() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return pred();
}

void prepareTopic(DefaultMQProducer& producer, const std::string& topic, int32_t queueNum) {
    try {
        producer.createTopic("TBW102", topic, queueNum);
    } catch (const std::exception& e) {
        std::printf("  !! createTopic(%s) failed: %s\n", topic.c_str(), e.what());
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_pop_consumer <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    const std::string pfx = "PopConsCpp_" + std::to_string(stamp() % 1000000);
    const std::string topic = pfx + "_T";
    const std::string group = pfx + "_G";
    const std::string topicLater = pfx + "_Later";
    const std::string groupLater = pfx + "_GLater";
    const int32_t nMsg = 12;

    DefaultMQProducer prep(pfx + "_prep");
    prep.setNamesrvAddr(nsAddr);
    prep.start();
    prepareTopic(prep, topic, 4);
    prepareTopic(prep, topicLater, 4);
    prep.shutdown();

    // ---------------- S1 / S2 / S4：正常消费 + ack ----------------
    {
        std::printf("=== S1 POP 消费（全收 + 无重复）===\n");
        std::mutex mtx;
        std::vector<std::string> got;
        std::set<int32_t> queues;

        class L : public MessageListenerConcurrently {
        public:
            L(std::mutex& m, std::vector<std::string>& got, std::set<int32_t>& q)
                : m_(m), got_(got), q_(q) {}
            ConsumeConcurrentlyStatus consumeMessage(
                const std::vector<MessageExt>& msgs, ConsumeConcurrentlyContext&) override {
                std::lock_guard<std::mutex> lk(m_);
                for (const MessageExt& m : msgs) {
                    got_.push_back(bodyOf(m));
                    q_.insert(m.queueId);
                }
                return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            std::mutex& m_;
            std::vector<std::string>& got_;
            std::set<int32_t>& q_;
        };

        auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
        consumer->setNamesrvAddr(nsAddr);
        consumer->setPopMode(true);
        consumer->setConsumeThreadNums(4);
        consumer->setConsumeMessageBatchMaxSize(4);
        // ⚠ 故意压到 10s：让"没 ack → invisibleTime 到期复活重投"在观测窗口内来得及暴露
        consumer->setPopInvisibleTime(10000);
        consumer->setMessageListener(std::make_shared<L>(mtx, got, queues));
        consumer->subscribe(topic);
        // 先起消费者再发消息（见项目约定）
        consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(1));

        DefaultMQProducer producer(pfx + "_pg");
        producer.setNamesrvAddr(nsAddr);
        producer.start();
        std::set<std::string> sent;
        for (int32_t i = 0; i < nMsg; i++) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "pop-cons-%02d", i);
            Message m(topic, Bytes(buf, buf + std::strlen(buf)));
            producer.send(m);
            sent.insert(std::string(buf));
        }
        std::printf("  sent %d msgs\n", nMsg);

        bool all = waitUntil(
            [&]() {
                std::lock_guard<std::mutex> lk(mtx);
                return static_cast<int32_t>(got.size()) >= nMsg;
            },
            30000);
        // 必须 > popInvisibleTime(10s)，否则 ack 没发也看不出重复投递
        std::this_thread::sleep_for(std::chrono::seconds(16));

        std::set<std::string> gotSet;
        {
            std::lock_guard<std::mutex> lk(mtx);
            gotSet.insert(got.begin(), got.end());
            check("S1a 全部消息被消费", all && static_cast<int32_t>(got.size()) >= nMsg,
                  "received=" + std::to_string(got.size()) + "/" + std::to_string(nMsg));
            check("S1b body 集合与发送一致", gotSet == sent,
                  "got=" + std::to_string(gotSet.size()) + " sent=" + std::to_string(sent.size()));
            check("S2a 无重复投递", gotSet.size() == got.size(),
                  "received=" + std::to_string(got.size())
                      + " unique=" + std::to_string(gotSet.size()));
            check("S2b 观察期内没有新增投递", static_cast<int32_t>(got.size()) == nMsg,
                  "received=" + std::to_string(got.size()));
            check("S4 消息分布在多个队列且都被消费", queues.size() > 1,
                  "queues=" + std::to_string(queues.size()));
        }
        consumer->shutdown();
        producer.shutdown();
    }

    // ---------------- S3：RECONSUME_LATER → 延迟后重投 ----------------
    {
        std::printf("=== S3 RECONSUME_LATER → 延迟后重投 ===\n");
        std::mutex mtx;
        std::vector<std::string> keys;

        class L2 : public MessageListenerConcurrently {
        public:
            L2(std::mutex& m, std::vector<std::string>& k) : m_(m), k_(k) {}
            ConsumeConcurrentlyStatus consumeMessage(
                const std::vector<MessageExt>& msgs, ConsumeConcurrentlyContext&) override {
                std::lock_guard<std::mutex> lk(m_);
                for (const MessageExt& m : msgs) {
                    k_.push_back(m.getKeys().empty() ? m.msgId : m.getKeys());
                }
                return ConsumeConcurrentlyStatus::RECONSUME_LATER;
            }

        private:
            std::mutex& m_;
            std::vector<std::string>& k_;
        };

        auto c2 = std::make_shared<DefaultMQPushConsumer>(groupLater);
        c2->setNamesrvAddr(nsAddr);
        c2->setPopMode(true);
        c2->setConsumeThreadNums(2);
        c2->setPopInvisibleTime(5000);
        c2->setMessageListener(std::make_shared<L2>(mtx, keys));
        c2->subscribe(topicLater);
        c2->start();
        std::this_thread::sleep_for(std::chrono::seconds(1));

        DefaultMQProducer p2(pfx + "_pg2");
        p2.setNamesrvAddr(nsAddr);
        p2.start();
        for (int32_t i = 0; i < 3; i++) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "later-%d", i);
            Message m(topicLater, Bytes(buf, buf + std::strlen(buf)));
            m.setKeys(std::string("pl") + std::to_string(i));
            p2.send(m);
        }

        bool first = waitUntil(
            [&]() {
                std::lock_guard<std::mutex> lk(mtx);
                return keys.size() >= 3;
            },
            30000);
        size_t firstRound = 0;
        {
            std::lock_guard<std::mutex> lk(mtx);
            firstRound = keys.size();
        }
        check("S3a 首轮投递 3 条", first && firstRound >= 3, "count=" + std::to_string(firstRound));

        bool again = waitUntil(
            [&]() {
                std::lock_guard<std::mutex> lk(mtx);
                return keys.size() >= firstRound + 3;
            },
            40000);
        size_t total = 0;
        {
            std::lock_guard<std::mutex> lk(mtx);
            total = keys.size();
        }
        check("S3b 消费失败后被重新投递（延长不可见时间生效）", again,
              "first=" + std::to_string(firstRound) + " total=" + std::to_string(total));
        c2->shutdown();
        p2.shutdown();
    }

    std::printf("########################################\n");
    std::printf("PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
