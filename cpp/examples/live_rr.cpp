// Request-Reply (5.x) verified against a real cluster (scenarios S1-S8 asserted one by one).
// Usage: rmq_live_rr 127.0.0.1:9876
//
// Chain:
//     Requester producer.request(msg, timeout)            Responder push consumer
//     msg.props[CORRELATION_ID]=uuid
//     msg.props[REPLY_TO_CLIENT]=clientId ─────────►   receives request (broker already wrote CLUSTER)
//     msg.props[TTL]=timeoutMillis                      createReplyMessage(req, body)
//                                                         → topic=<CLUSTER>_REPLY_TOPIC
//                                                         → MSG_TYPE="reply"
//     ◄── PUSH_REPLY_MESSAGE_TO_CLIENT(326) ◄─────      producer.send(reply) → 325
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
        std::printf("  pre-creating topic %s failed (falling back to auto-create): %s\n", topic.c_str(), e.what());
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

// Requests received by the responder: (body, correlationId, replyTo, ttl, cluster)
struct RequestRecord {
    std::string body;
    std::string correlationId;
    std::string replyTo;
    std::string ttl;
    // CLUSTER added by the broker when storing (S8 uses it to prove the reply must be built on the delivered copy)
    std::string cluster;
};

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
    // broker.conf sets brokerClusterName=DefaultCluster
    const std::string replyTopic = MixAll::getReplyTopic("DefaultCluster");
    const int32_t requestTimeout = 8000;

    std::printf("== Request-Reply real-cluster verification  namesrv=%s  topic=%s ==\n", nsAddr.c_str(),
                topic.c_str());
    std::printf("   reply topic = %s\n", replyTopic.c_str());

    DefaultMQProducer prep{"PG_RRPrep_" + std::to_string(stamp)};
    prep.setNamesrvAddr(nsAddr);
    prep.start();
    prepareTopic(prep, topic);

    // ---------------- S1 the reply topic is a broker system topic; clients cannot create it ----------------
    {
        bool rejected = false;
        std::string detail;
        try {
            prep.createTopic("TBW102", replyTopic, 4);
            detail = "it unexpectedly created successfully";
        } catch (const std::exception& e) {
            // createTopic wraps "every broker failed" into an MQClientException whose message carries
            // the broker's CODE/DESC (C++ exceptions have no cause chain to unwrap): match the
            // broker's INVALID_PARAMETER(29) text directly.
            detail = e.what();
            rejected = detail.find("conflict with system topic") != std::string::npos
                       || detail.find("CODE: 29") != std::string::npos;
        }
        check("S1 client createTopic(reply topic) rejected (broker system topic)", rejected, detail);
    }

    DefaultMQProducer requester{"PG_RRReq_" + std::to_string(stamp)};
    requester.setNamesrvAddr(nsAddr);
    requester.start();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    Replier replier(nsAddr, "CG_RRReply_" + std::to_string(stamp), topic);

    // watcher: independently subscribes <cluster>_REPLY_TOPIC, the hard evidence for S4
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
        // ---------------- S2/S3 one full round trip ----------------
        std::printf("\nS2/S3 one full request → reply round trip\n");
        Message ping(topic, "ping-1");
        const int64_t t0 = nowMs();
        Message reply = requester.request(ping, requestTimeout);
        const std::string want = "reply:ping-1";
        check("S3 request() got the reply with the correct body", reply.body == want,
              "got=" + reply.body + " want=" + want);
        check("S3 reply topic is <cluster>_REPLY_TOPIC", reply.topic == replyTopic,
              "topic=" + reply.topic);
        const std::string arrive =
            reply.getProperty(MessageConst::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME);
        check("S3 reply carries REPLY_MESSAGE_ARRIVE_TIME", !arrive.empty(), "value=" + arrive);

        const std::string requesterClientId = requester.clientId();
        bool gotRequest = waitUntil(
            [&replier]() {
                std::lock_guard<std::mutex> lk(replier.mtx);
                return !replier.received.empty();
            },
            15000);
        check("S2 responder received the request message", gotRequest, "received=1");
        RequestRecord rec;
        {
            std::lock_guard<std::mutex> lk(replier.mtx);
            if (!replier.received.empty()) rec = replier.received[0];
        }
        check("S2 request message carries CORRELATION_ID (uuid, 36 chars)", rec.correlationId.size() == 36,
              "corr=" + rec.correlationId);
        check("S2 request message carries REPLY_TO_CLIENT=requester clientId", rec.replyTo == requesterClientId,
              "replyTo=" + rec.replyTo + " want=" + requesterClientId);
        check("S2 request message carries TTL=timeout", rec.ttl == std::to_string(requestTimeout),
              "ttl=" + rec.ttl);
        const int64_t roundTrip = nowMs() - t0;
        check("S3 round-trip time is reasonable (<timeout)", roundTrip < requestTimeout,
              "elapsedMs=" + std::to_string(roundTrip));

        // ---------------- S4 the reply really went through 325 and landed on REPLY_TOPIC ----------------
        std::printf("\nS4 reply sent via SEND_REPLY_MESSAGE_V2(325) and lands on %s\n", replyTopic.c_str());
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
        check("S4 the responder successfully sent the reply message", replied, s4Detail);
        bool seen = waitUntil(
            [&watched, &watchMtx]() {
                std::lock_guard<std::mutex> lk(watchMtx);
                for (const auto& b : watched) {
                    if (b == "reply:ping-1") return true;
                }
                return false;
            },
            15000);
        check("S4 an independent consumer subscribed to the reply topic sees the reply", seen, "");

        // ---------------- S5 no responder -> RequestTimeoutException ----------------
        std::printf("\nS5 with no responder, request() throws RequestTimeoutException\n");
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
        check("S5 the exception thrown is RequestTimeoutException", raisedTimeout, raisedWhat);
        check("S5 exception carries 10006 REQUEST_TIMEOUT_EXCEPTION",
              raisedCode == ClientErrorCode::REQUEST_TIMEOUT_EXCEPTION,
              "code=" + std::to_string(raisedCode));
        check("S5 the wait is close to the configured timeout (2s~12s)", s5Elapsed >= 2000 && s5Elapsed <= 12000,
              "elapsedMs=" + std::to_string(s5Elapsed));

        // ---------------- S6 concurrent requests do not cross-talk ----------------
        std::printf("\nS6 3 concurrent requests, each getting its own reply\n");
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
        check("S6 all 3 concurrent requests got their own replies", okCount == 3 && concTimeouts == 0,
              "ok=" + std::to_string(okCount) + " fail=" + std::to_string(concTimeouts));

        // ---------------- S7 the consumer survives Reply traffic ----------------
        std::printf("\nS7 the responder's consumer is still healthy (plain push consumption unaffected by Reply)\n");
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
        check("S7 later plain messages are still consumed", plain, "");

        std::printf("\nS8 CLUSTER is written by the broker; an unbuildable reply reports 10007, not some other error\n");
        RequestRecord pingRec;
        {
            std::lock_guard<std::mutex> lk(replier.mtx);
            for (const auto& r : replier.received) {
                if (r.body == "ping-1") pingRec = r;
            }
        }
        check("S8 the delivered request message carries broker-written CLUSTER=DefaultCluster",
              pingRec.cluster == "DefaultCluster", "cluster=" + pingRec.cluster);
        check("S8 the request message copy in the client's hand has **no** CLUSTER (the property really is added by the broker)",
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
        check("S8 building a reply from the local request copy -> MQClientException with 10007",
              replyCode == ClientErrorCode::CREATE_REPLY_MESSAGE_EXCEPTION,
              "code=" + std::to_string(replyCode) + " what=" + replyWhat);
        check("S8 the 10007 text points at the missing CLUSTER property (protocol wording)",
              replyWhat.find("property[CLUSTER] is null.") != std::string::npos,
              "what=" + replyWhat);
    } catch (const std::exception& e) {
        std::printf("  [FATAL] scenario execution error: %s\n", e.what());
        ++gFail;
    }

    requester.shutdown();
    replier.shutdown();
    watcher->shutdown();
    prep.shutdown();

    std::printf("\n== Result: PASS=%d FAIL=%d ==\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
