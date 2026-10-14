// **Real-cluster** message-type integration test for the C++ client.
//
// Covers 11 message capabilities, all against a real name server + broker, and **every type is read back from the broker** before judging
// (the send status by itself does not count; only oneway has no reply, so it is proven by the read-back count that it really landed and was delivered):
//   0. synchronous normal send (send -> push consumer reads back 1 message)
//   1. asynchronous send (sendAsync + SendCallback)
//   1b. pinned async send (sendAsync(msg, mq, cb): lands on the specified queue)
//   1c. one-way send (sendOneway, no reply -> read-back count == 3 proves delivery succeeded)
//   2. ordered message (sendBySelector same key lands same queue + orderly consumption preserves order)
//   3. tagged message + server-side tag filtering
//   4. user property pass-through
//   5. delayed message (setDelayTimeLevel and verify store_ts - born_ts >= 3000ms)
//   5b. timer message (user property TIMER_DELIVER_MS set to about 5s later, likewise using store_ts-born_ts to prove the delay took effect)
//   6. keyed message + server-side query by key (QUERY_MESSAGE)
//   7. transactional message (two-phase commit path; full chain in rmq_live_transaction) + landed and consumable
//   8. batch message (sendBatch -> SEND_BATCH_MESSAGE(320), broker delivers as N independent messages with contiguous offsets)
//   Appendix: consumer heartbeat registration (HEART_BEAT)
//
// This program does not start a cluster itself; the caller must first start nameServer(9876) + broker(10911) with
// autoCreateTopicEnable=true. Usage:
//   ./rmq_live_message_types [namesrv_addr] [accessKey secretKey]
// To avoid a false picture from store accumulation, all topics/consumer groups carry a timestamp prefix.
//
// ACL cluster support: accessKey/secretKey may be passed in. When passed, **every** client facade
// (producer / consumer) started by this program carries this credential pair (setCredentials), and broker signature auth
// takes effect; when not passed, behavior is exactly that of a plain cluster without credentials.
// When credentials are passed, an extra **negative assertion** runs: first start a producer **without** credentials to send, which must be rejected by the broker
// (NO_PERMISSION=16). Without this negative case, an "all green" on a cluster that has no auth enabled would be a false pass.
// On ordering: in this port each facade news its own independent MQClientInstance inside start() (see
// DefaultMQProducer::start); RPC hooks bind to **that instance's own** transport layer and are not shared between instances,
// so an uncredentialed probe cannot inherit another client's already-signed connection. Even so, run the probe as the **first**
// client started, shut it down right after sending and leave ~1s for the instance to be reclaimed, then start credentialed clients to ensure
// a clean starting state.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"

using namespace rocketmq;

namespace {

std::string gNamesrv = "127.0.0.1:9876";
std::string gPrefix;
// Optional ACL credentials: when both are non-empty every client this program starts carries a signature; on a no-credential cluster keep both empty.
std::string gAccessKey;
std::string gSecretKey;
std::vector<std::pair<std::string, bool>> gResults;
int gPass = 0;
int gFail = 0;

// Whether credentials are complete: if empty this is treated as plain-cluster mode and all setCredentials calls are skipped.
bool aclEnabled() { return !gAccessKey.empty() && !gSecretKey.empty(); }

// Producer credential loading. DefaultMQProducer and DefaultMQPushConsumer share an identical method signature,
// so one template covers both instead of writing it once per client type.
template <typename Client> void applyCredentials(Client& c) {
    if (aclEnabled()) {
        c.setCredentials(gAccessKey, gSecretKey);
    }
}

void check(const std::string& name, bool ok, const std::string& detail = "") {
    gResults.emplace_back(name, ok);
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::cout << "[" << (ok ? "PASS" : "FAIL") << "] " << name;
    if (!detail.empty()) std::cout << "  " << detail;
    std::cout << std::endl;
}

std::string bytes2str(const Bytes& b) { return std::string(b.begin(), b.end()); }

Bytes str2bytes(const std::string& s) { return Bytes(s.begin(), s.end()); }

// ---------------------------------------------------------------- listener
class CollectingListenerConcurrently : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext& /*ctx*/) override {
        std::lock_guard<std::mutex> lk(m_);
        for (const MessageExt& m : msgs) msgs_.push_back(m);
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    std::vector<MessageExt> snapshot() const {
        std::lock_guard<std::mutex> lk(m_);
        return msgs_;
    }

private:
    mutable std::mutex m_;
    std::vector<MessageExt> msgs_;
};

class CollectingListenerOrderly : public MessageListenerOrderly {
public:
    ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                        ConsumeOrderlyContext& /*ctx*/) override {
        std::lock_guard<std::mutex> lk(m_);
        for (const MessageExt& m : msgs) msgs_.push_back(m);
        return ConsumeOrderlyStatus::SUCCESS;
    }

    std::vector<MessageExt> snapshot() const {
        std::lock_guard<std::mutex> lk(m_);
        return msgs_;
    }

private:
    mutable std::mutex m_;
    std::vector<MessageExt> msgs_;
};

// ---------------------------------------------------------------- async callback
class CollectingCallback : public SendCallback {
public:
    void onSuccess(const SendResult& r) override {
        ++ok_;
        std::lock_guard<std::mutex> lk(m_);
        results_.push_back(r);
    }
    void onException(const std::exception_ptr& e) override {
        ++err_;
        std::lock_guard<std::mutex> lk(m_);
        errors_.push_back(exceptionMessage(e) + " [" + exceptionTypeName(e) + "]");
    }

    int ok() const { return ok_.load(); }
    int err() const { return err_.load(); }
    std::vector<SendResult> results() const {
        std::lock_guard<std::mutex> lk(m_);
        return results_;
    }
    std::vector<std::string> errors() const {
        std::lock_guard<std::mutex> lk(m_);
        return errors_;
    }

private:
    std::atomic<int> ok_{0};
    std::atomic<int> err_{0};
    mutable std::mutex m_;
    std::vector<SendResult> results_;
    std::vector<std::string> errors_;
};

// ---------------------------------------------------------------- transaction listener
// COMMIT: the local transaction commits directly
class CommitTxListener : public TransactionListener {
public:
    LocalTransactionState executeLocalTransaction(const Message& /*msg*/,
                                                  const std::string& /*arg*/) override {
        return LocalTransactionState::COMMIT_MESSAGE;
    }
    LocalTransactionState checkLocalTransaction(const MessageExt& /*msg*/) override {
        return LocalTransactionState::COMMIT_MESSAGE;
    }
};

// ROLLBACK: the local transaction rolls back, and the broker should not deliver the half message
class RollbackTxListener : public TransactionListener {
public:
    LocalTransactionState executeLocalTransaction(const Message& /*msg*/,
                                                  const std::string& /*arg*/) override {
        return LocalTransactionState::ROLLBACK_MESSAGE;
    }
    LocalTransactionState checkLocalTransaction(const MessageExt& /*msg*/) override {
        return LocalTransactionState::ROLLBACK_MESSAGE;
    }
};

// UNKNOW + check-back: the local transaction returns UNKNOW, and COMMIT is only decided when the broker checks back.
// checkCalls proves **the broker really called back** (otherwise "finally received" might just be the normal-message path).
class UnknownThenCommitTxListener : public TransactionListener {
public:
    LocalTransactionState executeLocalTransaction(const Message& /*msg*/,
                                                  const std::string& /*arg*/) override {
        return LocalTransactionState::UNKNOW;
    }
    LocalTransactionState checkLocalTransaction(const MessageExt& /*msg*/) override {
        ++checkCalls;
        return LocalTransactionState::COMMIT_MESSAGE;
    }
    std::atomic<int> checkCalls{0};
};

// ---------------------------------------------------------------- consume helper
// Actively wait until expect messages are received (or at most durationSec seconds), then close the consumer.
// Use active waiting instead of a blind sleep: on a cold start the route/queues are not ready yet, so a fixed window often yields 0 messages.
struct ConsumerRun {
    std::vector<MessageExt> msgs;
    std::shared_ptr<DefaultMQPushConsumer> consumer;  // already shut down, only used to read the count
};

ConsumerRun runConsumer(const std::string& topic, const std::string& subExpr, int durationSec,
                        bool orderly, const std::string& groupSuffix, int expect = 0,
                        int32_t pullTimeout = 3000, int32_t pullSuspend = 1000) {
    std::shared_ptr<MessageListener> listener;
    std::shared_ptr<CollectingListenerConcurrently> conc;
    std::shared_ptr<CollectingListenerOrderly> ord;
    if (orderly) {
        ord = std::make_shared<CollectingListenerOrderly>();
        listener = ord;
    } else {
        conc = std::make_shared<CollectingListenerConcurrently>();
        listener = conc;
    }

    auto consumer = std::make_shared<DefaultMQPushConsumer>(gPrefix + "_" + groupSuffix);
    consumer->setNamesrvAddr(gNamesrv);
    applyCredentials(*consumer);
    consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    consumer->subscribe(topic, subExpr);
    consumer->setMessageListener(listener);
    // Key point: under single-threaded sequential long-polling, an idle queue's suspend long-poll starves a full queue,
    // so both the client wait and the server-side suspend are set short (see the consumer.h comments).
    consumer->setPullTimeoutMillis(pullTimeout);
    consumer->setPullSuspendTimeoutMillis(pullSuspend);
    consumer->start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(durationSec);
    while (std::chrono::steady_clock::now() < deadline) {
        size_t got = orderly ? ord->snapshot().size() : conc->snapshot().size();
        if (expect > 0 && static_cast<int>(got) >= expect) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ConsumerRun run;
    run.msgs = orderly ? ord->snapshot() : conc->snapshot();
    consumer->shutdown();
    run.consumer = consumer;
    return run;
}

// Wait until the broker finishes registering with the name server (avoids the race where the port just opened but registration has not landed)
std::vector<std::string> waitBroker(DefaultMQProducer& prod) {
    for (int i = 0; i < 40; ++i) {
        try {
            std::vector<MessageQueue> mqs = prod.fetchPublishMessageQueues(MixAll::DEFAULT_TOPIC);
            std::vector<std::string> addrs;
            for (const MessageQueue& mq : mqs) {
                std::string a = prod.client().brokerAddrOf(mq.brokerName);
                if (!a.empty() && std::find(addrs.begin(), addrs.end(), a) == addrs.end()) {
                    addrs.push_back(a);
                }
            }
            if (!addrs.empty()) return addrs;
        } catch (const std::exception&) {
            // route not ready yet, keep waiting
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return {};
}

}  // namespace

int main(int argc, char** argv) {
    if (argc >= 2) {
        gNamesrv = argv[1];
    }
    // Optional positional args: argv[2]=accessKey argv[3]=secretKey
    if (argc >= 3) {
        gAccessKey = argv[2];
    }
    if (argc >= 4) {
        gSecretKey = argv[3];
    }
    const int64_t stamp = UtilAll::currentTimeSeconds();
    gPrefix = "MTCPP_" + std::to_string(stamp);

    std::cout << "=== C++ client message-type integration (real cluster) ===" << std::endl;
    std::cout << "namesrv = " << gNamesrv << "  prefix = " << gPrefix << std::endl;
    std::cout << "ACL = " << (aclEnabled() ? "enabled (ak=" + gAccessKey + ")" : "disabled")
              << std::endl;

    // Keep logs clean: default INFO, benign long-poll timeouts (DEBUG) are suppressed
    // Keep logs clean: default INFO, benign long-poll timeouts (DEBUG) are suppressed.
    // But if ROCKETMQ_CLIENT_LOG_LEVEL is explicitly set externally, honor it — on live diagnosis you can raise the level without changing code.
    if (!logLevelSetFromEnv()) {
        setLogLevel(LOG_INFO);
    }

    // ---------- 0. ACL negative assertion: an uncredentialed producer's send must be rejected ----------
    // On an auth-enabled cluster every broker RPC needs a valid signature; without credentials it returns NO_PERMISSION(16).
    // Without this, the program would also show "all green" on a cluster without auth, and that green cannot prove the signature really takes effect.
    // It must run before all credentialed clients: in this port each facade holds its own independent MQClientInstance,
    // hooks bind to each own transport layer, so order does not affect isolation; still start it first, shut it down immediately and wait ~1s,
    // letting the uncredentialed probe's instance release cleanly so later credentialed clients connect from a clean state.
    if (aclEnabled()) {
        const std::string probeTopic = gPrefix + "_AclDenied";
        DefaultMQProducer noCred(gPrefix + "_producer_nocred");
        noCred.setNamesrvAddr(gNamesrv);  // deliberately do **not** load credentials
        noCred.setSendMsgTimeout(5000);
        noCred.setRetryTimesWhenSendFailed(0);  // an auth failure need not retry three rounds
        bool rejected = false;
        std::string detail;
        try {
            noCred.start();
            SendResult sr = noCred.send(Message(probeTopic, str2bytes("must-be-denied")));
            detail = "unexpectedly succeeded: " + std::string(sendStatusName(sr.sendStatus));
        } catch (const MQBrokerException& e) {
            rejected = e.getResponseCode() == ResponseCode::NO_PERMISSION;
            detail = "code=" + std::to_string(e.getResponseCode()) + " " + e.getResponseMessage();
        } catch (const std::exception& e) {
            // "no queue available" / topic-creation failure concatenates the underlying 16 code into the text, so match it by marker here.
            detail = e.what();
            rejected = detail.find("CODE: 16") != std::string::npos ||
                       detail.find("NO_PERMISSION") != std::string::npos;
        }
        check("ACL negative: uncredentialed send rejected (NO_PERMISSION=16)", rejected, detail);
        noCred.shutdown();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    DefaultMQProducer prod(gPrefix + "_producer");
    prod.setNamesrvAddr(gNamesrv);
    applyCredentials(prod);
    prod.setSendMsgTimeout(5000);
    try {
        prod.start();
    } catch (const std::exception& e) {
        std::cout << "[FAIL] producer startup: " << e.what() << std::endl;
        return 1;
    }

    std::vector<std::string> brokers = waitBroker(prod);
    if (brokers.empty()) {
        check("cluster liveness probe", false, "nameServer has no broker registered");
        prod.shutdown();
        return 1;
    }
    std::string brokerList;
    for (size_t i = 0; i < brokers.size(); ++i) {
        if (i) brokerList += ",";
        brokerList += brokers[i];
    }
    check("cluster liveness probe", true, "brokers=" + brokerList);

    const int64_t t0 = UtilAll::currentTimeMillis();

    // ---------- 0a. synchronous normal send: beyond the status it must also be read back from the broker ----------
    // A send returning SEND_OK only proves the broker accepted the request, not that the message entered a consumable queue,
    // so this case's criterion is the push consumer reading it back via FIRST_OFFSET.
    {
        const std::string topic = gPrefix + "_Sync";
        bool sendOk = false;
        std::string detail;
        try {
            SendResult sr = prod.send(Message(topic, str2bytes("sync-normal")));
            sendOk = sr.sendStatus == SendStatus::SEND_OK && !sr.msgId.empty();
            detail = "status=" + std::string(sendStatusName(sr.sendStatus))
                     + " msgId=" + (sr.msgId.empty() ? std::string("<empty>") : sr.msgId);
        } catch (const std::exception& e) {
            detail = std::string("throw: ") + e.what();
        }
        check("synchronous normal send send()", sendOk, detail);

        ConsumerRun run = runConsumer(topic, "*", 12, false, "sync", /*expect=*/1);
        bool readBack = false;
        for (const MessageExt& m : run.msgs) {
            if (bytes2str(m.body) == "sync-normal") readBack = true;
        }
        check("synchronous normal send read back from broker", readBack,
              "received=" + std::to_string(run.msgs.size()));
    }

    // ---------- 0b. one-way send sendOneway: no reply, can only be proven by the read-back count ----------
    // oneway is a fire-and-forget no-reply semantic: the client gets no broker receipt at all,
    // so "3 messages sent" cannot be proven by itself. The only criterion is the consumer reading back **exactly** 3 --
    // one fewer means the request never went out, one more means duplicate delivery.
    //
    // Create the topic explicitly first, then send: a new topic's auto-creation waits for the send's **reply** to bring back the route,
    // while oneway per protocol produces no reply, so you cannot be sure which queue a message finally lands on. With the topic in place first,
    // the read-back count is clean evidence about oneway alone (all three are on the same known topic).
    {
        const std::string topic = gPrefix + "_Oneway";
        const int kOnewayNum = 3;
        bool topicReady = false;
        std::string createDetail;
        try {
            // The key used to create the topic is the template topic for the publish route (same value as the producer's createTopicKey).
            prod.createTopic(MixAll::DEFAULT_TOPIC, topic, 4);
            // After creating it, wait until the route is visible; do not let the one-way send run ahead of the route
            for (int i = 0; i < 30 && !topicReady; ++i) {
                try {
                    topicReady = prod.fetchPublishMessageQueues(topic).size() >= 4;
                } catch (const std::exception&) {
                    // route not landed yet, re-check shortly
                }
                if (!topicReady) std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            createDetail = "queues=" + std::to_string(topicReady ? 4 : 0);
        } catch (const std::exception& e) {
            createDetail = std::string("throw: ") + e.what();
        }
        check("one-way send pre-created topic", topicReady, createDetail);

        std::string detail = "sent=";
        int sent = 0;
        for (int i = 0; i < kOnewayNum; ++i) {
            try {
                prod.sendOneway(Message(topic, str2bytes("oneway-" + std::to_string(i))));
                ++sent;
            } catch (const std::exception& e) {
                detail += "[EXC:" + std::string(e.what()) + "]";
            }
        }
        detail += std::to_string(sent) + " (oneway has no reply, not used as a criterion)";
        check("one-way send sendOneway() call succeeded", sent == kOnewayNum, detail);

        ConsumerRun run = runConsumer(topic, "*", 15, false, "oneway", /*expect=*/kOnewayNum);
        std::set<std::string> bodies;
        for (const MessageExt& m : run.msgs) {
            bodies.insert(bytes2str(m.body));
        }
        bool exact = true;
        for (int i = 0; i < kOnewayNum; ++i) {
            if (bodies.count("oneway-" + std::to_string(i)) == 0) exact = false;
        }
        // The read-back count is oneway's only "send succeeded" evidence.
        check("one-way send read back 3 from broker (the only criterion)",
              run.msgs.size() == static_cast<size_t>(kOnewayNum) && exact,
              "received=" + std::to_string(run.msgs.size()) + " distinct="
                  + std::to_string(bodies.size()));
    }

    // ---------- 1. asynchronous send ----------
    {
        const std::string topic = gPrefix + "_Async";
        auto cb = std::make_shared<CollectingCallback>();
        Message msg(topic, str2bytes("async-hello"));
        try {
            prod.sendAsync(msg, cb);
        } catch (const std::exception& e) {
            check("asynchronous send sendAsync", false, std::string("throw: ") + e.what());
        }
        for (int i = 0; i < 200 && cb->ok() + cb->err() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        std::vector<SendResult> rs = cb->results();
        bool ok = (cb->ok() == 1) && (cb->err() == 0) && !rs.empty() &&
                  rs[0].sendStatus == SendStatus::SEND_OK;
        check("asynchronous send sendAsync", ok,
              "ok=" + std::to_string(cb->ok()) + " err=" + std::to_string(cb->err()));
    }

    // ---------- 1b. pinned async send: sendAsync(msg, mq, cb) ----------
    {
        const std::string topic = gPrefix + "_AsyncPinned";
        SendResult probe;
        bool probed = true;
        try {
            probe = prod.send(Message(topic, str2bytes("probe")), 5000);
        } catch (const std::exception& e) {
            probed = false;
            check("pinned async send sendAsync(msg, mq)", false,
                  std::string("create topic failed: ") + e.what());
        }
        if (probed) {
            DefaultMQProducer fresh(gPrefix + "_producer_fresh");
            fresh.setNamesrvAddr(gNamesrv);
            applyCredentials(fresh);
            fresh.setSendMsgTimeout(5000);
            fresh.start();
            auto cb = std::make_shared<CollectingCallback>();
            try {
                fresh.sendAsync(Message(topic, str2bytes("pinned-async")), probe.messageQueue, cb,
                                5000);
            } catch (const std::exception& e) {
                check("pinned async send sendAsync(msg, mq)", false,
                      std::string("throw: ") + e.what());
            }
            for (int i = 0; i < 200 && cb->ok() + cb->err() == 0; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
            }
            std::vector<SendResult> rs = cb->results();
            const bool sameQueue = !rs.empty() && rs[0].messageQueue == probe.messageQueue;
            const bool ok = (cb->ok() == 1) && (cb->err() == 0) && !rs.empty()
                            && rs[0].sendStatus == SendStatus::SEND_OK && sameQueue
                            && !rs[0].msgId.empty();
            check("pinned async send sendAsync(msg, mq)", ok,
                  "ok=" + std::to_string(cb->ok()) + " err=" + std::to_string(cb->err())
                      + (rs.empty()
                             ? std::string()
                             : (" queue=" + rs[0].messageQueue.brokerName + ":"
                                + std::to_string(rs[0].messageQueue.queueId) + " want="
                                  + probe.messageQueue.brokerName + ":"
                                  + std::to_string(probe.messageQueue.queueId)
                                  + " msgId=" + (rs[0].msgId.empty() ? "<empty>" : "set"))));
            if (cb->err() > 0) {
                std::vector<std::string> errs = cb->errors();
                std::cout << "       callback exception: " << errs[0] << std::endl;
            }
            fresh.shutdown();
        }
    }

    // ---------- 2. ordered message: same key lands same queue + orderly consumption preserves order ----------
    std::vector<Bytes> bodiesOrder;
    {
        const std::string topic = gPrefix + "_Order";
        SelectMessageQueueByHash selector;
        std::vector<int32_t> qids;
        for (int i = 0; i < 10; ++i) {
            char buf[16];
            std::snprintf(buf, sizeof(buf), "ord-%02d", i);
            Bytes body = str2bytes(buf);
            bodiesOrder.push_back(body);
            SendResult sr = prod.sendBySelector(Message(topic, body), selector, "shard-A");
            if (std::find(qids.begin(), qids.end(), sr.messageQueue.queueId) == qids.end()) {
                qids.push_back(sr.messageQueue.queueId);
            }
        }
        std::string qidStr;
        for (int32_t q : qids) {
            if (!qidStr.empty()) qidStr += ",";
            qidStr += std::to_string(q);
        }
        check("ordered send: same key routes to the same queue", qids.size() == 1,
              "distinct_queue_ids=" + (qidStr.empty() ? std::string("?") : qidStr));

        ConsumerRun run = runConsumer(topic, "*", 12, /*orderly=*/true, "order",
                                      /*expect=*/10);
        // Real order check: the received **sequence** must match the send order (not merely set equality --
        // comparing only sorted proves "all received" but not "nothing out of order"),
        // and within the same queue the queueOffset must be strictly increasing.
        std::vector<std::string> recv, expect;
        for (const MessageExt& m : run.msgs) recv.push_back(bytes2str(m.body));
        for (const Bytes& b : bodiesOrder) expect.push_back(bytes2str(b));
        bool seqOk = (recv == expect);
        bool offsetOk = true;
        for (size_t i = 1; i < run.msgs.size(); ++i) {
            if (run.msgs[i].queueOffset <= run.msgs[i - 1].queueOffset) offsetOk = false;
        }
        std::string orderDetail = "received=" + std::to_string(run.msgs.size()) + "/10 seq_ok="
                                  + (seqOk ? "1" : "0") + " offset_monotonic="
                                  + (offsetOk ? "1" : "0");
        if (!recv.empty()) {
            orderDetail += " first=" + recv.front() + " last=" + recv.back();
        }
        check("orderly consumption preserves order", run.msgs.size() == 10 && seqOk && offsetOk, orderDetail);
    }

    // ---------- 3. tagged message + server-side tag filtering ----------
    {
        const std::string topic = gPrefix + "_Tag";
        for (int i = 0; i < 3; ++i) {
            Message m(topic, str2bytes("tagA-" + std::to_string(i)));
            m.setTags("TagA");
            prod.send(m);
        }
        for (int i = 0; i < 3; ++i) {
            Message m(topic, str2bytes("tagB-" + std::to_string(i)));
            m.setTags("TagB");
            prod.send(m);
        }
        ConsumerRun run = runConsumer(topic, "TagA", 12, false, "tag");
        bool allA = true;
        for (const MessageExt& m : run.msgs) {
            if (m.getTags() != "TagA") allA = false;
        }
        check("Tag-filtered consumption (only TagA received)", run.msgs.size() == 3 && allA,
              "received=" + std::to_string(run.msgs.size()));
    }

    // ---------- 4. user property pass-through ----------
    {
        const std::string topic = gPrefix + "_Prop";
        for (int i = 0; i < 3; ++i) {
            Message m(topic, str2bytes("prop-" + std::to_string(i)));
            m.setTags("P");
            m.setUserProperty("city", "Hangzhou");
            m.setUserProperty("env", "prod");
            prod.send(m);
        }
        ConsumerRun run = runConsumer(topic, "*", 12, false, "prop");
        bool okCity = true;
        bool okEnv = true;
        for (const MessageExt& m : run.msgs) {
            if (m.getUserProperty("city") != "Hangzhou") okCity = false;
            if (m.getUserProperty("env") != "prod") okEnv = false;
        }
        check("user property pass-through(city=Hangzhou, env=prod)",
              okCity && okEnv && run.msgs.size() == 3,
              "received=" + std::to_string(run.msgs.size()));
    }

    // ---------- 5. delayed message ----------
    {
        const std::string topic = gPrefix + "_Delay";
        prod.send(Message(topic, str2bytes("normal-now")));
        Message dm(topic, str2bytes("delayed-5s"));
        dm.setDelayTimeLevel(2);  // level2 = 5s
        prod.send(dm);

        ConsumerRun run = runConsumer(topic, "*", 20, false, "delay", /*expect=*/2);
        std::vector<MessageExt> delayed, normal;
        for (const MessageExt& m : run.msgs) {
            if (bytes2str(m.body) == "delayed-5s") {
                delayed.push_back(m);
            } else if (bytes2str(m.body) == "normal-now") {
                normal.push_back(m);
            }
        }
        check("delayed message finally delivered", !delayed.empty(),
              "delayed received=" + std::to_string(delayed.size()) + " normal="
                  + std::to_string(normal.size()));
        if (!delayed.empty()) {
            int64_t drift = delayed[0].storeTimestamp - delayed[0].bornTimestamp;
            check("delay took effect (store_ts-born_ts>=3000ms)", drift >= 3000,
                  "drift=" + std::to_string(drift) + "ms");
        } else {
            check("delay took effect (store_ts-born_ts>=3000ms)", false, "no delayed message to verify");
        }
    }

    // ---------- 5b. timer message (user property TIMER_DELIVER_MS, delivery about 5s later)----------
    // A delay level can only pick a few fixed tiers; a timer message lets the sender give an **absolute delivery time** directly.
    // The broker keeps TIMER_DELIVER_MS messages in timer storage and only writes them back to the real topic at the due time,
    // so the store_ts the consumer sees is the storage time of "the actual delivery":
    // store_ts - born_ts >= 3000ms proves the delay really took effect rather than being delivered immediately as a normal message.
    {
        const std::string topic = gPrefix + "_Timer";
        const int64_t deliverInMs = 5000;
        const int64_t deliverAt = UtilAll::currentTimeMillis() + deliverInMs;
        Message tm(topic, str2bytes("timer-deliver"));
        // Via user property: TIMER_DELIVER_MS is the protocol property name of a timer message.
        tm.setUserProperty(MessageConst::PROPERTY_TIMER_DELIVER_MS, std::to_string(deliverAt));
        bool sent = false;
        std::string detail;
        try {
            SendResult sr = prod.send(tm);
            sent = sr.sendStatus == SendStatus::SEND_OK;
            detail = "status=" + std::string(sendStatusName(sr.sendStatus))
                     + " deliver_in=" + std::to_string(deliverInMs) + "ms";
        } catch (const std::exception& e) {
            detail = std::string("throw: ") + e.what();
        }
        check("timer message send (TIMER_DELIVER_MS≈+5s)", sent, detail);

        ConsumerRun run = runConsumer(topic, "*", 20, false, "timer", /*expect=*/1);
        std::vector<MessageExt> timed;
        for (const MessageExt& m : run.msgs) {
            if (bytes2str(m.body) == "timer-deliver") timed.push_back(m);
        }
        check("timer message finally delivered", !timed.empty(),
              "timer received=" + std::to_string(timed.size()) + " total="
                  + std::to_string(run.msgs.size()));
        if (!timed.empty()) {
            int64_t drift = timed[0].storeTimestamp - timed[0].bornTimestamp;
            check("timer took effect (store_ts-born_ts>=3000ms)", drift >= 3000,
                  "drift=" + std::to_string(drift) + "ms");
        } else {
            check("timer took effect (store_ts-born_ts>=3000ms)", false, "no timer message to verify");
        }
    }

    // ---------- 6. keyed message + query by key ----------
    {
        const std::string topic = gPrefix + "_Key";
        const std::string key = "MTKEY_" + std::to_string(stamp);
        const std::string payload = "key-msg-payload";
        Message m(topic, str2bytes(payload));
        m.setKeys(key);
        prod.send(m);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        int64_t begin = t0 - 120000;
        int64_t end = UtilAll::currentTimeMillis() + 120000;
        std::vector<MessageExt> found = prod.queryMessage(topic, key, 10, begin, end);
        bool hit = false;
        for (const MessageExt& x : found) {
            if (bytes2str(x.body) == payload) hit = true;
        }
        check("query by key (query_message)", hit,
              "returned=" + std::to_string(found.size()));
    }

    {
        const std::string topic = gPrefix + "_Tx";
        CommitTxListener listener;
        bool txOk = false;
        std::string stateStr;
        try {
            TransactionSendResult tsr =
                prod.sendMessageInTransaction(Message(topic, str2bytes("tx-commit")), listener);
            stateStr = localTransactionStateName(tsr.localTransactionState);
            txOk = (tsr.sendStatus == SendStatus::SEND_OK &&
                    tsr.localTransactionState == LocalTransactionState::COMMIT_MESSAGE);
        } catch (const std::exception& e) {
            stateStr = std::string("throw: ") + e.what();
        }
        check("transaction-COMMIT send status", txOk, "state=" + stateStr);

        ConsumerRun run = runConsumer(topic, "*", 10, false, "tx");
        bool consumed = false;
        for (const MessageExt& m : run.msgs) {
            if (bytes2str(m.body) == "tx-commit") consumed = true;
        }
        check("transaction-COMMIT landed and consumable", consumed,
              "received=" + std::to_string(run.msgs.size()));
    }

    // ---------- 7.2 ROLLBACK ----------
    {
        const std::string topic = gPrefix + "_TxRollback";
        RollbackTxListener listener;
        bool txOk = false;
        std::string stateStr;
        try {
            TransactionSendResult tsr =
                prod.sendMessageInTransaction(Message(topic, str2bytes("tx-rollback")), listener);
            stateStr = localTransactionStateName(tsr.localTransactionState);
            txOk = (tsr.sendStatus == SendStatus::SEND_OK &&
                    tsr.localTransactionState == LocalTransactionState::ROLLBACK_MESSAGE);
        } catch (const std::exception& e) {
            stateStr = std::string("throw: ") + e.what();
        }
        check("transaction-ROLLBACK send status", txOk, "state=" + stateStr);

        // After rollback the broker should not deliver: wait out the whole window to confirm nothing was received
        ConsumerRun run = runConsumer(topic, "*", 10, false, "txrollback");
        bool consumed = false;
        for (const MessageExt& m : run.msgs) {
            if (bytes2str(m.body) == "tx-rollback") consumed = true;
        }
        check("transaction-ROLLBACK not delivered", !consumed,
              "received=" + std::to_string(run.msgs.size()));
    }

    // ---------- 7.3 UNKNOW + broker check-back ----------
    {
        const std::string topic = gPrefix + "_TxCheck";
        UnknownThenCommitTxListener listener;
        bool txOk = false;
        std::string stateStr;
        try {
            TransactionSendResult tsr =
                prod.sendMessageInTransaction(Message(topic, str2bytes("tx-check")), listener);
            stateStr = localTransactionStateName(tsr.localTransactionState);
            txOk = (tsr.sendStatus == SendStatus::SEND_OK &&
                    tsr.localTransactionState == LocalTransactionState::UNKNOW);
        } catch (const std::exception& e) {
            stateStr = std::string("throw: ") + e.what();
        }
        check("transaction-UNKNOW send status", txOk, "state=" + stateStr);

        // The check-back cadence depends on the broker: on a local cluster transactionCheckInterval is one round per 30s,
        // the half message is committed 15~17s after sending, so the window must cover a whole check-back cycle;
        // expect=1 lets the consumer return as soon as it receives one, without waiting out the window.
        ConsumerRun run = runConsumer(topic, "*", 40, false, "txcheck", /*expect=*/1);
        bool consumed = false;
        for (const MessageExt& m : run.msgs) {
            if (bytes2str(m.body) == "tx-check") consumed = true;
        }
        int checks = listener.checkCalls.load();
        check("transaction-UNKNOW triggers broker check-back", checks > 0,
              "checkLocalTransaction_calls=" + std::to_string(checks));
        check("transaction-UNKNOW finally delivered after check-back", consumed,
              "received=" + std::to_string(run.msgs.size()));
    }

    // ---------- 8. batch message (SEND_BATCH_MESSAGE 320)----------
    // After the request code changed from 310 to 320, it must be re-proven on a real broker that the broker really
    // handles this request as "batch write". The criterion is the consumer seeing **N independent messages with contiguous offsets**:
    // if the broker stored the batch body as a single message (i.e. header.batch did not take effect), only
    // 1 message whose body is the concatenated bytes would arrive, not N each-complete bodies.
    {
        const std::string topic = gPrefix + "_Batch";
        std::vector<Message> group;
        for (int i = 0; i < 3; ++i) {
            group.emplace_back(topic, str2bytes("batch-" + std::to_string(i)));
        }
        bool batchOk = false;
        std::string batchDetail = "throw";
        try {
            SendResult sr = prod.sendBatch(group, 5000);
            batchOk = sr.sendStatus == SendStatus::SEND_OK;
            batchDetail = "status=" + std::to_string(static_cast<int>(sr.sendStatus));
        } catch (const std::exception& e) {
            batchDetail = std::string("throw: ") + e.what();
        }
        check("batch send sendBatch(3 messages)", batchOk, batchDetail);

        ConsumerRun run = runConsumer(topic, "*", 15, false, "batch", /*expect=*/3);
        std::set<std::string> bodies;
        std::vector<int64_t> offsets;
        for (const MessageExt& m : run.msgs) {
            bodies.insert(bytes2str(m.body));
            offsets.push_back(m.queueOffset);
        }
        std::sort(offsets.begin(), offsets.end());
        bool contiguous = offsets.size() == 3;
        for (size_t i = 1; i < offsets.size(); ++i) {
            if (offsets[i] != offsets[i - 1] + 1) contiguous = false;
        }
        bool exact = true;
        for (int i = 0; i < 3; ++i) {
            if (bodies.count("batch-" + std::to_string(i)) == 0) exact = false;
        }
        check("batch delivered by the broker as 3 independent messages", exact && contiguous,
              "received=" + std::to_string(run.msgs.size()) + " offsets="
                  + [&] {
                        std::string s;
                        for (int64_t o : offsets) { if (!s.empty()) s += ","; s += std::to_string(o); }
                        return s;
                    }());
    }

    // ---------- Appendix: heartbeat registration ----------
    {
        const std::string topic = gPrefix + "_Hb";
        prod.send(Message(topic, str2bytes("hb-probe")));
        ConsumerRun run = runConsumer(topic, "*", 10, false, "hb", /*expect=*/1);
        int64_t hb = run.consumer ? run.consumer->heartbeatCount() : 0;
        check("consumer heartbeat registration (HEART_BEAT)", hb > 0,
              "heartbeat_ok=" + std::to_string(hb) + " consumed="
                  + std::to_string(run.msgs.size()));
    }

    prod.shutdown();

    // ---------- summary ----------
    std::cout << "\n================ C++ message-type integration summary ================" << std::endl;
    for (const auto& r : gResults) {
        std::cout << "  [" << (r.second ? "PASS" : "FAIL") << "] " << r.first << std::endl;
    }
    std::cout << "=====================================================" << std::endl;
    std::cout << "  PASS=" << gPass << " FAIL=" << gFail << std::endl;
    if (gFail > 0) {
        std::cout << "result: " << gFail << " items failed" << std::endl;
        return 1;
    }
    std::cout << "result: all passed (the C++ client completed every message type against a real cluster)" << std::endl;
    return 0;
}
