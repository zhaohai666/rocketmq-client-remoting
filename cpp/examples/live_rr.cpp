// Request-Reply（5.x）真机验证（与 python/verify_request_reply_live.py 的 S1–S8 同套场景）。
// 用法：rmq_live_rr 127.0.0.1:9876
//
// 链路：
//     请求方 producer.request(msg, timeout)             应答方 push consumer
//     msg.props[CORRELATION_ID]=uuid
//     msg.props[REPLY_TO_CLIENT]=clientId ─────────►   收到请求（broker 已写 CLUSTER）
//     msg.props[TTL]=timeoutMillis                      createReplyMessage(req, body)
//                                                         → topic=<CLUSTER>_REPLY_TOPIC
//                                                         → MSG_TYPE="reply"
//     ◄── PUSH_REPLY_MESSAGE_TO_CLIENT(326) ◄─────      producer.send(reply) → 325
//
// 错误码两条硬断言（Java 同款，缺了就是两类故障糊成一条）：
//   S5 无应答方 → RequestTimeoutException 带 10006 REQUEST_TIMEOUT_EXCEPTION；
//   S8 拿客户端手里那份（没有 broker 写的 CLUSTER）造应答 → MQClientException 带
//      10007 CREATE_REPLY_MESSAGE_EXCEPTION，文案点到缺失的 CLUSTER。
//
// ⚠ 两个真机必踩点：
//   1) <cluster>_REPLY_TOPIC 是 broker 启动时注册的**系统 topic**，客户端 createTopic 它
//      会被 INVALID_PARAMETER(29)「conflict with system topic」拒绝 —— 不要去"预建"它。
//   2) 请求方必须先在 broker 上登记（心跳），否则 broker 按 REPLY_TO_CLIENT 找不到 channel。
//      request() 内部已补一次心跳（对齐 Java prepareSendRequest）。
#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/request_reply.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/codes.h"

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

std::string bodyOf(const MessageExt& m) { return m.body; }

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

void prepareTopic(DefaultMQProducer& producer, const std::string& topic, int32_t queues = 4) {
    try {
        producer.createTopic("TBW102", topic, queues);
    } catch (const std::exception& e) {
        std::printf("  预建 topic %s 失败（改用自动创建）: %s\n", topic.c_str(), e.what());
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

// 应答方收到的请求记录：(body, correlationId, replyTo, ttl, cluster)
struct RequestRecord {
    std::string body;
    std::string correlationId;
    std::string replyTo;
    std::string ttl;
    // broker 在存储时补的 CLUSTER（S8 要用它证明应答必须建立在投递到的那份消息上）
    std::string cluster;
};

// 应答方：普通 push 消费者 + 用来发应答的生产者（Java 文档里 Request-Reply 的标准写法）
class Replier {
public:
    Replier(const std::string& nsAddr, const std::string& group, const std::string& topic)
        : producer_{"PG_RRReplier"} {
        producer_.setNamesrvAddr(nsAddr);
        producer_.start();
        auto self = this;
        consumer_ = std::make_shared<DefaultMQPushConsumer>(group);
        consumer_->setNamesrvAddr(nsAddr);
        consumer_->subscribe(topic);
        consumer_->setMessageListener(std::make_shared<Listener>(self));
        consumer_->start();
    }

    void shutdown() {
        consumer_->shutdown();
        producer_.shutdown();
    }

    std::mutex mtx;
    std::vector<RequestRecord> received;
    std::vector<std::string> replied;
    std::vector<std::string> replyErrors;

private:
    class Listener : public MessageListenerConcurrently {
    public:
        explicit Listener(Replier* owner) : owner_(owner) {}
        ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                 ConsumeConcurrentlyContext&) override {
            for (const MessageExt& m : msgs) {
                RequestRecord rec{bodyOf(m),
                                  m.getProperty(MessageConst::PROPERTY_CORRELATION_ID),
                                  m.getProperty(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT),
                                  m.getProperty(MessageConst::PROPERTY_MESSAGE_TTL),
                                  m.getProperty(MessageConst::PROPERTY_CLUSTER)};
                try {
                    Message reply = createReplyMessage(m, "reply:" + m.body);
                    owner_->producer_.send(reply);
                    std::lock_guard<std::mutex> lk(owner_->mtx);
                    owner_->received.push_back(rec);
                    owner_->replied.push_back("reply:" + m.body);
                } catch (const std::exception& e) {
                    std::lock_guard<std::mutex> lk(owner_->mtx);
                    owner_->received.push_back(rec);
                    owner_->replyErrors.push_back(e.what());
                }
            }
            return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
        }

    private:
        Replier* owner_;
    };

    DefaultMQProducer producer_;
    std::shared_ptr<DefaultMQPushConsumer> consumer_;
};

template <typename Pred>
bool waitUntil(Pred pred, int timeoutMs) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return pred();
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_rr <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    const int64_t stamp = nowMs() % 100000000;
    const std::string topic = "RequestReply_" + std::to_string(stamp);
    const std::string silentTopic = topic + "_NoReplier";
    // broker.conf 里 brokerClusterName=DefaultCluster
    const std::string replyTopic = MixAll::getReplyTopic("DefaultCluster");
    const int32_t requestTimeout = 8000;

    std::printf("== Request-Reply 真机验证  namesrv=%s  topic=%s ==\n", nsAddr.c_str(),
                topic.c_str());
    std::printf("   应答 topic = %s\n", replyTopic.c_str());

    DefaultMQProducer prep{"PG_RRPrep_" + std::to_string(stamp)};
    prep.setNamesrvAddr(nsAddr);
    prep.start();
    prepareTopic(prep, topic);

    // ---------------- S1 应答 topic 是 broker 系统 topic，客户端不能建 ----------------
    {
        bool rejected = false;
        std::string detail;
        try {
            prep.createTopic("TBW102", replyTopic, 4);
            detail = "竟然建成功了";
        } catch (const std::exception& e) {
            // createTopic 会把「逐个 broker 都失败」包成 MQClientException（message 里带着
            // broker 的 CODE/DESC，C++ 异常没有 cause 链可解，与 python 脚本的
            // broker_error() 解 cause 链等价）：匹配 broker 的 INVALID_PARAMETER(29) 文本。
            detail = e.what();
            rejected = detail.find("conflict with system topic") != std::string::npos
                       || detail.find("CODE: 29") != std::string::npos;
        }
        check("S1 客户端 createTopic(应答 topic) 被拒（broker 系统 topic）", rejected, detail);
    }

    DefaultMQProducer requester{"PG_RRReq_" + std::to_string(stamp)};
    requester.setNamesrvAddr(nsAddr);
    requester.start();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    Replier replier(nsAddr, "CG_RRReply_" + std::to_string(stamp), topic);

    // watcher：独立订阅 <cluster>_REPLY_TOPIC，是 S4 的硬证据
    std::mutex watchMtx;
    std::vector<std::string> watched;
    class Watcher : public MessageListenerConcurrently {
    public:
        Watcher(std::mutex& mtx, std::vector<std::string>& sink) : mtx_(mtx), sink_(sink) {}
        ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                 ConsumeConcurrentlyContext&) override {
            std::lock_guard<std::mutex> lk(mtx_);
            for (const MessageExt& m : msgs) sink_.push_back(bodyOf(m));
            return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
        }

    private:
        std::mutex& mtx_;
        std::vector<std::string>& sink_;
    };
    auto watcher = std::make_shared<DefaultMQPushConsumer>("CG_RRWatch_" + std::to_string(stamp));
    watcher->setNamesrvAddr(nsAddr);
    watcher->subscribe(replyTopic);
    watcher->setMessageListener(std::make_shared<Watcher>(watchMtx, watched));
    watcher->start();

    try {
        // ---------------- S2/S3 一次完整往返 ----------------
        std::printf("\nS2/S3 一次完整 request → reply 往返\n");
        Message ping(topic, "ping-1");
        const int64_t t0 = nowMs();
        Message reply = requester.request(ping, requestTimeout);
        const std::string want = "reply:ping-1";
        check("S3 request() 拿到应答且 body 正确", reply.body == want,
              "got=" + reply.body + " want=" + want);
        check("S3 应答 topic 是 <cluster>_REPLY_TOPIC", reply.topic == replyTopic,
              "topic=" + reply.topic);
        const std::string arrive =
            reply.getProperty(MessageConst::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME);
        check("S3 应答带 REPLY_MESSAGE_ARRIVE_TIME", !arrive.empty(), "value=" + arrive);

        const std::string requesterClientId = requester.clientId();
        bool gotRequest = waitUntil(
            [&replier]() {
                std::lock_guard<std::mutex> lk(replier.mtx);
                return !replier.received.empty();
            },
            15000);
        check("S2 应答方收到请求消息", gotRequest, "received=1");
        RequestRecord rec;
        {
            std::lock_guard<std::mutex> lk(replier.mtx);
            if (!replier.received.empty()) rec = replier.received[0];
        }
        check("S2 请求消息带 CORRELATION_ID（uuid 36 字符）", rec.correlationId.size() == 36,
              "corr=" + rec.correlationId);
        check("S2 请求消息带 REPLY_TO_CLIENT=请求方 clientId", rec.replyTo == requesterClientId,
              "replyTo=" + rec.replyTo + " want=" + requesterClientId);
        check("S2 请求消息带 TTL=timeout", rec.ttl == std::to_string(requestTimeout),
              "ttl=" + rec.ttl);
        const int64_t roundTrip = nowMs() - t0;
        check("S3 往返耗时合理（<timeout）", roundTrip < requestTimeout,
              "elapsedMs=" + std::to_string(roundTrip));

        // ---------------- S4 应答确实走了 325 且落到 REPLY_TOPIC ----------------
        std::printf("\nS4 应答走 SEND_REPLY_MESSAGE_V2(325) 且落到 %s\n", replyTopic.c_str());
        bool replied = waitUntil(
            [&replier]() {
                std::lock_guard<std::mutex> lk(replier.mtx);
                return !replier.replied.empty() && replier.replyErrors.empty();
            },
            15000);
        std::string s4Detail;
        {
            std::lock_guard<std::mutex> lk(replier.mtx);
            for (const auto& e : replier.replyErrors) s4Detail += e + "; ";
        }
        check("S4 应答方成功发出了应答消息", replied, s4Detail);
        bool seen = waitUntil(
            [&watched, &watchMtx]() {
                std::lock_guard<std::mutex> lk(watchMtx);
                for (const auto& b : watched) {
                    if (b == "reply:ping-1") return true;
                }
                return false;
            },
            15000);
        check("S4 订阅应答 topic 的独立消费者能看到该应答", seen, "");

        // ---------------- S5 无应答方 → RequestTimeoutException ----------------
        std::printf("\nS5 无应答方时 request() 抛 RequestTimeoutException\n");
        prepareTopic(prep, silentTopic);
        const int64_t s5Begin = nowMs();
        bool raisedTimeout = false;
        int32_t raisedCode = 0;
        std::string raisedWhat;
        try {
            Message silent(silentTopic, "nobody-home");
            requester.request(silent, 3000);
        } catch (const RequestTimeoutException& e) {
            raisedTimeout = true;
            raisedCode = e.getResponseCode();
            raisedWhat = e.what();
        } catch (const std::exception& e) {
            raisedWhat = std::string("wrong exception: ") + e.what();
        }
        const int64_t s5Elapsed = nowMs() - s5Begin;
        check("S5 抛的是 RequestTimeoutException", raisedTimeout, raisedWhat);
        // Java 抛的是 RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, msg)：
        // 光有类型不够，10006 也要带上 —— 调用方按码分流时才知道"请求已投出去、只是没等到应答"。
        check("S5 异常带 10006 REQUEST_TIMEOUT_EXCEPTION",
              raisedCode == ClientErrorCode::REQUEST_TIMEOUT_EXCEPTION,
              "code=" + std::to_string(raisedCode));
        check("S5 超时时长接近设定值（2s~12s）", s5Elapsed >= 2000 && s5Elapsed <= 12000,
              "elapsedMs=" + std::to_string(s5Elapsed));

        // ---------------- S6 并发请求不串台 ----------------
        std::printf("\nS6 并发 3 个 request，各自拿到自己的应答\n");
        std::mutex resMtx;
        int32_t okCount = 0;
        int32_t concTimeouts = 0;
        std::vector<std::thread> threads;
        for (int i = 0; i < 3; ++i) {
            threads.emplace_back([&, i]() {
                try {
                    Message m(topic, "ping-conc-" + std::to_string(i));
                    Message r = requester.request(m, requestTimeout);
                    std::string expectBody = "reply:ping-conc-" + std::to_string(i);
                    std::lock_guard<std::mutex> lk(resMtx);
                    if (r.body == expectBody) ++okCount;
                } catch (const std::exception&) {
                    std::lock_guard<std::mutex> lk(resMtx);
                    ++concTimeouts;
                }
            });
        }
        for (std::thread& t : threads) t.join();
        check("S6 3 个并发 request 全部拿到自己的应答", okCount == 3 && concTimeouts == 0,
              "ok=" + std::to_string(okCount) + " fail=" + std::to_string(concTimeouts));

        // ---------------- S7 消费者未被 Reply 流量破坏 ----------------
        std::printf("\nS7 应答方的消费者仍然完好（普通 push 消费不受 Reply 影响）\n");
        requester.send(Message(topic, "plain-after-replies"));
        bool plain = waitUntil(
            [&replier]() {
                std::lock_guard<std::mutex> lk(replier.mtx);
                for (const auto& r : replier.received) {
                    if (r.body == "plain-after-replies") return true;
                }
                return false;
            },
            15000);
        check("S7 后续普通消息仍被消费", plain, "");

        // ---------------- S8 CLUSTER 由 broker 写入；造不出应答时报 10007 ----------------
        // Java MessageUtil.createReplyMessage（:46/49）抛的是
        // MQClientException(CREATE_REPLY_MESSAGE_EXCEPTION=10007, ...)。这条既验"错误码带上了"，
        // 也验它**为什么**存在：CLUSTER 是 broker 存储时补的（SendMessageProcessor:318/614），
        // 客户端手里那份永远没有 ⇒ 应答必须建立在**投递到的**那条消息上，用错对象就撞上 10007。
        std::printf("\nS8 CLUSTER 由 broker 写入；造不出应答时报 10007 而不是别的错\n");
        RequestRecord pingRec;
        {
            std::lock_guard<std::mutex> lk(replier.mtx);
            for (const auto& r : replier.received) {
                if (r.body == "ping-1") pingRec = r;
            }
        }
        check("S8 投递到的请求消息带 broker 写入的 CLUSTER=DefaultCluster",
              pingRec.cluster == "DefaultCluster", "cluster=" + pingRec.cluster);
        check("S8 客户端手里那份请求消息**没有** CLUSTER（属性确实是 broker 补的）",
              ping.getProperty(MessageConst::PROPERTY_CLUSTER).empty(),
              "value=" + ping.getProperty(MessageConst::PROPERTY_CLUSTER));
        int32_t replyCode = 0;
        std::string replyWhat;
        try {
            createReplyMessage(ping, "pong");
        } catch (const MQClientException& e) {
            replyCode = e.getResponseCode();
            replyWhat = e.what();
        } catch (const std::exception& e) {
            replyWhat = std::string("wrong exception: ") + e.what();
        }
        check("S8 拿本地那份请求消息造应答 → MQClientException 带 10007",
              replyCode == ClientErrorCode::CREATE_REPLY_MESSAGE_EXCEPTION,
              "code=" + std::to_string(replyCode) + " what=" + replyWhat);
        check("S8 10007 的文案点到缺失的 CLUSTER 属性（Java 原文）",
              replyWhat.find("property[CLUSTER] is null.") != std::string::npos,
              "what=" + replyWhat);
    } catch (const std::exception& e) {
        std::printf("  [FATAL] 场景执行异常: %s\n", e.what());
        ++gFail;
    }

    requester.shutdown();
    replier.shutdown();
    watcher->shutdown();
    prep.shutdown();

    std::printf("\n== 结果: PASS=%d FAIL=%d ==\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
