// Prerequisites: name server + broker already up, with ``autoCreateTopicEnable=true``.
//
//
// ! Three script disciplines (the working rules proven during the live_backpressure live run):
//   (1) topic names always carry the stamp -- successive runs land on the same broker, a fixed name inherits the previous round's message count;
//   (2) "did it land" is judged only by the broker-side sum of maxOffset-minOffset over the queues, never by client callbacks;
//   (3) a new topic waits for the broker to register its topicConfig into the name server (seconds up to tens of seconds), so every
//       broker-side reconciliation polls until timeout; one failed route read is not a failure.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/hook.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;
std::vector<std::string> gFailed;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        gFailed.push_back(name);
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

std::string num(int64_t v) { return std::to_string(v); }

int64_t stamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool waitUntil(const std::function<bool()>& pred, int32_t timeoutMs) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return pred();
}

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

bool startsWith(const std::string& s, const std::string& prefix) {
    return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

class Recorder : public SendCallback {
public:
    void onSuccess(const SendResult& r) override {
        std::lock_guard<std::mutex> lk(m_);
        results_.push_back(r);
        threads_.push_back(currentThreadName());
    }

    void onException(const std::exception_ptr& e) override {
        std::lock_guard<std::mutex> lk(m_);
        errors_.push_back(exceptionMessage(e));
        errorTypes_.push_back(exceptionTypeName(e));
        threads_.push_back(currentThreadName());
    }

    size_t done() {
        std::lock_guard<std::mutex> lk(m_);
        return results_.size() + errors_.size();
    }

    size_t ok() {
        std::lock_guard<std::mutex> lk(m_);
        size_t n = 0;
        for (const SendResult& r : results_) {
            if (r.sendStatus == SendStatus::SEND_OK) ++n;
        }
        return n;
    }

    size_t errorCount() {
        std::lock_guard<std::mutex> lk(m_);
        return errors_.size();
    }

    std::vector<SendResult> results() {
        std::lock_guard<std::mutex> lk(m_);
        return results_;
    }

    // Thread name looked up by index (which callback), out of range returns an empty string
    std::string threadAt(size_t i) {
        std::lock_guard<std::mutex> lk(m_);
        return i < threads_.size() ? threads_[i] : std::string();
    }

    std::string firstError() {
        std::lock_guard<std::mutex> lk(m_);
        return errors_.empty() ? std::string() : errors_.front();
    }

    std::string firstErrorType() {
        std::lock_guard<std::mutex> lk(m_);
        return errorTypes_.empty() ? std::string() : errorTypes_.front();
    }

    std::string summary() {
        std::lock_guard<std::mutex> lk(m_);
        return "ok=" + std::to_string(results_.size()) + " err=" + std::to_string(errors_.size())
             + (errors_.empty() ? ""
                                : " " + errors_.front()
                                      + (errorTypes_.empty() ? "" : " ["
                                                                       + errorTypes_.front()
                                                                       + "]"));
    }

private:
    std::mutex m_;
    std::vector<SendResult> results_;
    std::vector<std::string> errors_;
    std::vector<std::string> errorTypes_;
    std::vector<std::string> threads_;
};

// The before hook parks for a while: only a slowed-down preparation stage can prove that the caller did not wait for it.
// It also records two things: which thread the hook runs on, and how many times before/after each ran.
class TracingHook : public SendMessageHook {
public:
    explicit TracingHook(int64_t parkMillis) : parkMillis_(parkMillis) {}

    std::string hookName() const override { return "async-tracing"; }

    void sendMessageBefore(SendMessageContext&) override {
        {
            std::lock_guard<std::mutex> lk(m_);
            ++before_;
            beforeThread_ = currentThreadName();
        }
        const int64_t ms = parkMillis_.load();
        if (ms > 0) std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }

    void sendMessageAfter(SendMessageContext&) override {
        std::lock_guard<std::mutex> lk(m_);
        ++after_;
    }

    int32_t before() {
        std::lock_guard<std::mutex> lk(m_);
        return before_;
    }

    int32_t after() {
        std::lock_guard<std::mutex> lk(m_);
        return after_;
    }

    std::string beforeThread() {
        std::lock_guard<std::mutex> lk(m_);
        return beforeThread_;
    }

private:
    std::mutex m_;
    std::atomic<int64_t> parkMillis_;
    int32_t before_ = 0;
    int32_t after_ = 0;
    std::string beforeThread_;
};

// Rejects only the messages tagged forbidden, and records the CommunicationMode it sees
class ForbiddenTagHook : public CheckForbiddenHook {
public:
    std::string hookName() const override { return "async-forbidden"; }

    void checkForbidden(CheckForbiddenContext& ctx) override {
        std::lock_guard<std::mutex> lk(m_);
        ++calls_;
        mode_ = ctx.communicationMode;
        if (ctx.message != nullptr && ctx.message->getTags() == "forbidden") {
            throw MQClientException("live test: tag forbidden is not allowed");
        }
    }

    int32_t calls() {
        std::lock_guard<std::mutex> lk(m_);
        return calls_;
    }

    CommunicationMode mode() {
        std::lock_guard<std::mutex> lk(m_);
        return mode_;
    }

private:
    std::mutex m_;
    int32_t calls_ = 0;
    CommunicationMode mode_ = CommunicationMode::SYNC;
};

// ---------------------------------------------------------------- environment
struct Env {
    std::string nsAddr;
    int64_t s = 0;
    std::unique_ptr<DefaultMQAdminExt> admin;

    std::string topic(const std::string& prefix) const {
        return prefix + "_" + std::to_string(s);
    }
};

// Hooks must be attached before start() (same discipline as live_backpressure)
std::shared_ptr<DefaultMQProducer> makeProducer(Env& env, const std::string& instance,
                                                std::shared_ptr<SendMessageHook> sendHook = nullptr,
                                                std::shared_ptr<CheckForbiddenHook> forbidHook =
                                                    nullptr) {
    auto p = std::make_shared<DefaultMQProducer>();
    p->setProducerGroup("PID_rmq_async_cpp_" + std::to_string(env.s));
    p->setNamesrvAddr(env.nsAddr);
    p->setInstanceName("async-cpp-" + instance + "-" + std::to_string(env.s));
    if (sendHook) p->registerSendMessageHook(std::move(sendHook));
    if (forbidHook) p->registerCheckForbiddenHook(std::move(forbidHook));
    p->start();
    return p;
}

Message msg(const std::string& topic, const std::string& body) { return Message(topic, body); }

// How many messages this topic has landed on the broker (sum of maxOffset-minOffset over all queues); -1 when the route cannot be read
int64_t landedCount(DefaultMQAdminExt& admin, const std::string& topic) {
    std::vector<MessageQueue> queues;
    try {
        queues = admin.examineTopicRoute(topic).getAllMessageQueue(topic);
    } catch (const std::exception&) {
        return -1;  // the route is not registered yet
    }
    int64_t total = 0;
    for (const MessageQueue& mq : queues) {
        try {
            total += admin.maxOffset(mq) - admin.minOffset(mq);
        } catch (const std::exception&) {
            // this queue was just created and has not been written to yet
        }
    }
    return total;
}

// Wait until at least expected messages show up on the broker, return the last reading (a new topic reaches the name server with second-scale delay)
int64_t waitLanded(Env& env, const std::string& topic, int64_t expected, int seconds = 30) {
    const int64_t deadline = nowMs() + seconds * 1000LL;
    int64_t landed = landedCount(*env.admin, topic);
    while (landed < expected && nowMs() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        landed = landedCount(*env.admin, topic);
    }
    return landed;
}

int64_t queueLanded(DefaultMQAdminExt& admin, const MessageQueue& mq) {
    try {
        return admin.maxOffset(mq) - admin.minOffset(mq);
    } catch (const std::exception&) {
        return -1;
    }
}

std::string brokerAddr(Env& env) {
    try {
        const ClusterInfo info = env.admin->fetchBrokerClusterInfo();
        const std::vector<std::string> addrs = info.getBrokerAddrs();
        if (!addrs.empty()) return addrs[0];
    } catch (const std::exception&) {
    }
    return "127.0.0.1:10911";
}

// ------------------------------------------------- A1 immediate return + thread identity + read-back
void a1NonBlockingAndReadBack(Env& env, const std::string& t) {
    auto hook = std::make_shared<TracingHook>(400);
    auto p = makeProducer(env, "a1", hook);
    auto rec = std::make_shared<Recorder>();
    const std::string body = "async-a1-readback";
    const int64_t began = nowMs();
    p->sendAsync(msg(t, body), rec, 5000);
    const int64_t callerTook = nowMs() - began;
    check("A1 the caller returned before the preparation stage (the hook sleeps 400ms)",
          callerTook < 200 && hook->before() <= 1,
          "caller_elapsed=" + num(callerTook) + "ms before_ran=" + num(hook->before()));
    check("A1 exactly one callback and SEND_OK",
          waitUntil([&]() { return rec->done() >= 1; }, 20000) && rec->ok() == 1
              && rec->errorCount() == 0,
          rec->summary());
    check("A1 the before hook runs on AsyncSenderExecutor_N",
          startsWith(hook->beforeThread(), "AsyncSenderExecutor_"),
          "thread=" + hook->beforeThread());
    check("A1 the user callback runs on NettyClientPublicExecutor_N",
          startsWith(rec->threadAt(0), "NettyClientPublicExecutor_"),
          "thread=" + rec->threadAt(0));
    check("A1 before/after each ran once", hook->before() == 1 && hook->after() == 1,
          "before=" + num(hook->before()) + " after=" + num(hook->after()));
    check("A1 the broker landed this one message", waitLanded(env, t, 1) == 1);
    const std::vector<SendResult> rs = rec->results();
    if (rs.empty()) {
        p->shutdown();
        return;
    }
    const SendResult& r = rs[0];
    // offsetMsgId is handed out by the broker and only it decodes to the commitLog offset => read the message back and compare the body
    MessageExt back;
    const bool read = waitUntil(
        [&]() {
            try {
                back = env.admin->viewMessage(t, r.offsetMsgId);
                return true;
            } catch (const std::exception&) {
                return false;  // the commitLog is not flushed out yet
            }
        },
        15000);
    check("A1 the offsetMsgId from the callback reads this message back", read && bodyOf(back) == body,
          "body=" + bodyOf(back));
    int64_t maxOffset = -1;
    try {
        maxOffset = env.admin->maxOffset(r.messageQueue);
    } catch (const std::exception&) {
    }
    check("A1 the queueOffset in the callback is exactly where the message landed", r.queueOffset == maxOffset - 1,
          "queueOffset=" + num(r.queueOffset) + " maxOffset=" + num(maxOffset));
    // msgId is the UNIQ_KEY filled in by the client (32 hex digits), not the broker's own id
    check("A1 msgId is the client UNIQ_KEY and differs from the broker's offsetMsgId",
          r.msgId.size() == 32 && r.msgId != r.offsetMsgId,
          "msgId=" + r.msgId + " offsetMsgId=" + r.offsetMsgId);
    p->shutdown();
}

// ------------------------------------------------- A2 concurrent sends do not cross-talk
void a2BurstExactlyOnceEach(Env& env, const std::string& t) {
    const int32_t burst = 30;
    auto p = makeProducer(env, "a2");
    std::vector<std::shared_ptr<Recorder>> each;
    for (int32_t i = 0; i < burst; ++i) each.push_back(std::make_shared<Recorder>());
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < burst; ++i) {
        threads.emplace_back([p, t, each, i]() { p->sendAsync(msg(t, "async-burst-" + num(i)),
                                                              each[static_cast<size_t>(i)], 8000); });
    }
    for (std::thread& th : threads) th.join();
    check("A2 each of the 30 concurrent async sends reached a terminal state",
          waitUntil([&]() {
              for (const std::shared_ptr<Recorder>& r : each) {
                  if (r->done() < 1) return false;
              }
              return true;
          },
                    20000),
          num([&]() {
              size_t n = 0;
              for (const std::shared_ptr<Recorder>& r : each) n += r->done();
              return static_cast<int64_t>(n);
          }()));
    int32_t multi = 0;
    int32_t notOk = 0;
    std::set<std::string> slots;
    std::set<std::string> uniq;
    for (const std::shared_ptr<Recorder>& r : each) {
        if (r->done() != 1) ++multi;
        const std::vector<SendResult> rs = r->results();
        if (rs.size() != 1 || rs[0].sendStatus != SendStatus::SEND_OK) ++notOk;
        for (const SendResult& s : rs) {
            slots.insert(s.messageQueue.brokerName + "#" + num(s.messageQueue.queueId) + "@"
                         + num(s.queueOffset));
            uniq.insert(s.msgId);
        }
    }
    check("A2 every send has exactly one terminal state (no more, no less)", multi == 0, "sends_with_extra_callbacks=" + num(multi));
    check("A2 all SEND_OK", notOk == 0, each[0]->summary());
    check("A2 exactly 30 messages landed on the broker", waitLanded(env, t, burst) == burst);
    check("A2 the (broker, queueId, queueOffset) slots of the sends never overlap",
          static_cast<int32_t>(slots.size()) == burst, "distinct=" + num(static_cast<int64_t>(slots.size())));
    check("A2 every send's UNIQ_KEY is different", static_cast<int32_t>(uniq.size()) == burst);
    p->shutdown();
}

// ------------------------------------------------- A3 pinned-queue send
void a3PinnedQueue(Env& env, const std::string& t) {
    auto p = makeProducer(env, "a3");
    auto rec = std::make_shared<Recorder>();
    // First grow the topic (one synchronous send so the broker creates all queues), then pick one queue and pin to it.
    // ! Before taking the baseline, wait until the warm-up message is already readable on the broker: a just-acked
    // report reaches the consumeQueue with a delay, so a baseline read of 0 that turns into 1 during reconciliation would invent one extra message.
    p->send(msg(t, "async-a3-warmup"), 5000);
    check("A3 the warm-up message is already readable on the broker", waitLanded(env, t, 1) >= 1,
          "landed=" + num(landedCount(*env.admin, t)));
    const std::vector<MessageQueue> queues = p->fetchPublishMessageQueues(t);
    check("A3 fetched the publish queues", !queues.empty(), "queues=" + num(static_cast<int64_t>(queues.size())));
    if (queues.empty()) {
        p->shutdown();
        return;
    }
    const MessageQueue aimed = queues[0];
    const int64_t before = queueLanded(*env.admin, aimed);
    int64_t othersBefore = 0;
    for (size_t i = 1; i < queues.size(); ++i) othersBefore += std::max<int64_t>(0, queueLanded(*env.admin, queues[i]));
    p->sendAsync(msg(t, "async-a3-pinned"), aimed, rec, 5000);
    check("A3 the pinned async send reached a terminal state with SEND_OK",
          waitUntil([&]() { return rec->done() >= 1; }, 20000) && rec->ok() == 1,
          rec->summary());
    const std::vector<SendResult> rs = rec->results();
    if (rs.empty()) {
        p->shutdown();
        return;
    }
    const SendResult& r = rs[0];
    check("A3 the result landed on the queue that was specified",
          r.messageQueue.brokerName == aimed.brokerName
              && r.messageQueue.queueId == aimed.queueId,
          "broker=" + r.messageQueue.brokerName + " queueId=" + num(r.messageQueue.queueId));
    check("A3 that queue grew by exactly 1 message",
          waitUntil([&]() { return queueLanded(*env.admin, aimed) == before + 1; }, 20000),
          "landed=" + num(queueLanded(*env.admin, aimed)) + " before=" + num(before));
    int64_t othersAfter = 0;
    for (size_t i = 1; i < queues.size(); ++i) othersAfter += std::max<int64_t>(0, queueLanded(*env.admin, queues[i]));
    check("A3 no other queue gained a single message", othersAfter == othersBefore,
          "other_queues " + num(othersBefore) + " -> " + num(othersAfter));
    p->shutdown();
}

// ------------------------------------------------- A4 checkForbidden hook
void a4ForbiddenHook(Env& env, const std::string& t) {
    auto forbidden = std::make_shared<ForbiddenTagHook>();
    auto p = makeProducer(env, "a4", nullptr, forbidden);
    auto rejected = std::make_shared<Recorder>();
    auto passed = std::make_shared<Recorder>();
    Message bad = msg(t, "async-a4-rejected");
    bad.setTags("forbidden");
    p->sendAsync(bad, rejected, 5000);
    check("A4 the exception the hook threw reaches the callback unchanged",
          waitUntil([&]() { return rejected->done() >= 1; }, 10000)
              && rejected->firstError().find("tag forbidden is not allowed") != std::string::npos,
          rejected->summary());
    check("A4 the rejection exception type is MQClientException (exactly what checkForbidden throws)",
          rejected->firstErrorType() == "MQClientException", rejected->summary());
    check("A4 the intercepting hook sees ASYNC", forbidden->mode() == CommunicationMode::ASYNC,
          "mode=" + std::string(forbidden->mode() == CommunicationMode::ASYNC      ? "ASYNC"
                                : forbidden->mode() == CommunicationMode::ONEWAY ? "ONEWAY"
                                                                                 : "SYNC"));
    // This topic holds nothing but the rejected send => either the reading is 0 or the route is not even
    // registered (-1). autoCreate only builds the route once the first message reaches the broker,
    // so "the route cannot be read" is itself evidence that the broker never received a request.
    const int64_t afterReject = landedCount(*env.admin, t);
    check("A4 the rejected send left no trace on the broker", afterReject <= 0,
          "landed=" + num(afterReject) + " (-1 = the route was not created, i.e. the broker received nothing)");

    p->sendAsync(msg(t, "async-a4-ok"), passed, 5000);
    check("A4 the same producer with another tag still lands (the rejection did not break the pool)",
          waitUntil([&]() { return passed->done() >= 1; }, 20000) && passed->ok() == 1,
          passed->summary());
    const int64_t landed = waitLanded(env, t, 1);
    check("A4 exactly 1 message landed on the broker", landed == 1, "landed=" + num(landed));
    check("A4 the hook was called 2 times in total (one rejected, one passed)", forbidden->calls() == 2,
          "calls=" + num(forbidden->calls()));
    p->shutdown();
}

// ------------------------------------------------- A5 batch async (sendBatchAsync)

// Total messages landed on all queues except index 0 (a pinned batch must prove that no other queue gained a message).
int64_t othersLanded(DefaultMQAdminExt& admin, const std::vector<MessageQueue>& queues) {
    int64_t total = 0;
    for (size_t i = 1; i < queues.size(); ++i) {
        total += std::max<int64_t>(0, queueLanded(admin, queues[i]));
    }
    return total;
}

void a5BatchAsync(Env& env, const std::string& t) {
    auto p = makeProducer(env, "a5");

    // (1) A batch of 5: exactly one callback, SEND_OK, and the broker really holds 5 messages.
    //     Watching only the callback is fooled by "the callback reported OK but the request never left" -- that one needs broker-side reconciliation.
    auto rec = std::make_shared<Recorder>();
    std::vector<Message> batch;
    for (int32_t i = 0; i < 5; ++i) batch.push_back(msg(t, "async-a5-" + num(i)));
    p->sendBatchAsync(batch, rec, 10000);
    check("A5 a batch of 5: exactly one callback with SEND_OK",
          waitUntil([&]() { return rec->done() >= 1; }, 20000) && rec->done() == 1
              && rec->ok() == 1 && rec->errorCount() == 0
              && !rec->results().empty() && !rec->results()[0].msgId.empty(),
          rec->summary());
    const int64_t landed = waitLanded(env, t, 5);
    check("A5 the broker really landed 5 messages (not just the callback claiming success)", landed == 5,
          "landed=" + num(landed) + " (-1 = the route was not created yet)");
    check("*  A5 request code = SEND_BATCH_MESSAGE(320) is proven by the offline unit test", true,
          "the live machine cannot see the outgoing report; tests/test_producer_async.cpp proves this one in-process");

    const std::vector<SendResult> batchResults = rec->results();
    const SendResult batchResult = batchResults.empty() ? SendResult{} : batchResults[0];
    // Batch msgId semantics: the UNIQ_KEY the client generated for the whole batch, not the broker's offsetMsgId string
    check("A5 the batch msgId is the client's 32-character id, not the broker's offsetMsgId",
          batchResult.msgId.size() == 32 && batchResult.msgId.find(',') == std::string::npos
              && batchResult.msgId != batchResult.offsetMsgId,
          "msgId=" + batchResult.msgId + " offsetMsgId=" + batchResult.offsetMsgId);
    // The offsetMsgId of the batch response is the string the broker returns per sub-message (comma separated, one commitLog
    // offset each), which is itself the evidence that this batch was split and stored as 5 messages
    std::vector<std::string> subOffsets;
    for (size_t pos = 0; pos <= batchResult.offsetMsgId.size();) {
        size_t comma = batchResult.offsetMsgId.find(',', pos);
        if (comma == std::string::npos) comma = batchResult.offsetMsgId.size();
        if (comma > pos) subOffsets.push_back(batchResult.offsetMsgId.substr(pos, comma - pos));
        pos = comma + 1;
    }
    check("A5 the broker returned 5 per-message commitLog offsets (the batch really was split on storage)",
          subOffsets.size() == 5, "offsetMsgId=" + batchResult.offsetMsgId);
    MessageExt storedSub;
    const bool readSub =
        !subOffsets.empty()
        && waitUntil(
               [&]() {
                   try {
                       storedSub = env.admin->viewMessage(t, subOffsets.front());
                       return true;
                   } catch (const std::exception&) {
                       return false;  // the commitLog is not flushed out yet
                   }
               },
               15000);
    const std::string storedUniq =
        readSub ? storedSub.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                : std::string();
    check("A5 the sub-message stored on the broker carries the client UNIQ_KEY (the per-message id is encoded in the body)",
          storedUniq.size() == 32, "stored UNIQ_KEY=" + storedUniq);

    // (2) Pinned batch: the mq argument really reaches the batch core instead of being dropped and a queue picked on its own.
    const std::vector<MessageQueue> queues = p->fetchPublishMessageQueues(t);
    if (queues.empty()) {
        check("A5 the pinned batch fetched the publish queues", false, "queues=0");
        p->shutdown();
        return;
    }
    const MessageQueue aimed = queues[0];
    const int64_t aimedBefore = queueLanded(*env.admin, aimed);
    const int64_t othersBefore = othersLanded(*env.admin, queues);
    auto pinned = std::make_shared<Recorder>();
    std::vector<Message> pinBatch;
    for (int32_t i = 0; i < 3; ++i) pinBatch.push_back(msg(t, "async-a5-pin-" + num(i)));
    p->sendBatchAsync(pinBatch, aimed, pinned, 10000);
    const bool pinnedOk = waitUntil([&]() { return pinned->done() >= 1; }, 20000)
                        && pinned->done() == 1 && pinned->ok() == 1;
    const std::vector<SendResult> pr = pinned->results();
    const bool landedOnAimed = !pr.empty() && pr[0].messageQueue.brokerName == aimed.brokerName
                            && pr[0].messageQueue.queueId == aimed.queueId;
    int64_t aimedAfter = queueLanded(*env.admin, aimed);
    check("A5 the pinned batch's queue grew by exactly 3 messages",
          waitUntil([&]() { return (aimedAfter = queueLanded(*env.admin, aimed)) == aimedBefore + 3; },
                    20000),
          "this_queue " + num(aimedBefore) + " -> " + num(aimedAfter));
    const int64_t othersAfter = othersLanded(*env.admin, queues);
    check("A5 the pinned batch lands on the specified queue and no other queue gains a message",
          pinnedOk && landedOnAimed && aimedAfter == aimedBefore + 3 && othersAfter == othersBefore,
          "ok=" + num(static_cast<int64_t>(pinned->ok())) + " landed_on_aimed=" + std::string(landedOnAimed ? "1" : "0")
              + " this_queue " + num(aimedBefore) + "->" + num(aimedAfter)
              + " others " + num(othersBefore) + "->" + num(othersAfter));

    auto mixed = std::make_shared<Recorder>();
    p->sendBatchAsync({msg(t, "async-a5-mixed-1"), msg(env.topic("BatchOther"), "async-a5-mixed-2")},
                      mixed, 5000);
    check("A5 a batch mixing topics reports in the callback (local validation is not bypassed by the async path)",
          waitUntil([&]() { return mixed->done() >= 1; }, 10000) && mixed->done() == 1
              && mixed->ok() == 0
              && mixed->firstError().find("should be the same") != std::string::npos,
          mixed->summary());

    // (4) Empty batch: same contract -- the error goes into the callback, not a single callback is left owing.
    auto empty = std::make_shared<Recorder>();
    p->sendBatchAsync(std::vector<Message>(), empty, 5000);
    check("A5 an empty batch also gets exactly one failing callback, nothing is silently swallowed",
          waitUntil([&]() { return empty->done() >= 1; }, 10000) && empty->done() == 1
              && empty->ok() == 0
              && empty->firstError().find("message list is empty") != std::string::npos,
          empty->summary());

    // (5) Backpressure charges the whole batch's bytes, not "one message": with the byte gate at the floor (1 MiB), a batch of 2x600 KiB
    //     must be blocked (1.2 MiB total > 1 MiB); a batch of 2x100 KiB must pass again. Charging only the first message
    //     (600 KiB) or not charging at all would flip both of these outcomes at once.
    p->setEnableBackpressureForAsyncMode(true);
    p->setBackPressureForAsyncSendNum(1);
    p->setBackPressureForAsyncSendSize(1024 * 1024);
    auto gated = std::make_shared<Recorder>();
    p->sendBatchAsync({msg(t, std::string(600 * 1024, 'x')), msg(t, std::string(600 * 1024, 'x'))},
                      gated, 2000);
    check("A5 the 1.2MiB whole batch is blocked by the 1MiB byte gate (the callback gets TOO_MUCH_REQUEST)",
          waitUntil([&]() { return gated->done() >= 1; }, 15000) && gated->done() == 1
              && gated->ok() == 0
              && gated->firstError().find("semaphoreAsyncSize timeout") != std::string::npos,
          gated->summary());
    // A small batch under the same gate must still land: proof that the block came from the whole-batch size, not from batches being rejected as a class
    auto passed = std::make_shared<Recorder>();
    p->sendBatchAsync({msg(t, std::string(100 * 1024, 'y')), msg(t, std::string(100 * 1024, 'y'))},
                      passed, 10000);
    check("A5 a 200KiB batch under the same gate still gets SEND_OK",
          waitUntil([&]() { return passed->done() >= 1; }, 20000) && passed->done() == 1
              && passed->ok() == 1,
          passed->summary());
    // Permits must be returned exactly as taken: leaking one turns into "every async send times out" after a long run, invisible to single sends.
    const int32_t numTotal = p->getBackPressureForAsyncSendNum();
    const int32_t sizeTotal = p->getBackPressureForAsyncSendSize();
    check("A5 the batch path returns both permits exactly (free amounts back to the total, including the gated send)",
          waitUntil(
              [&]() {
                  return p->getSemaphoreAsyncSendNumAvailablePermits() == numTotal
                      && p->getSemaphoreAsyncSendSizeAvailablePermits() == sizeTotal;
              },
              10000),
          "free_num=" + num(p->getSemaphoreAsyncSendNumAvailablePermits()) + " total=" + num(numTotal)
              + " free_bytes=" + num(p->getSemaphoreAsyncSendSizeAvailablePermits())
              + " total=" + num(sizeTotal));
    p->shutdown();
}

// ------------------------------------------------- A6 shutting the pool down drains the in-flight preparation stage
void a6ShutdownDrains(Env& env, const std::string& t) {
    const int32_t cores = std::max<int32_t>(1, static_cast<int32_t>(std::thread::hardware_concurrency()));
    const int32_t sends = cores * 3;
    auto hook = std::make_shared<TracingHook>(100);
    auto p = makeProducer(env, "a6", hook);
    auto rec = std::make_shared<Recorder>();
    for (int32_t i = 0; i < sends; ++i) p->sendAsync(msg(t, "async-a6-" + num(i)), rec, 8000);
    p->shutdown();
    const int64_t landed = waitLanded(env, t, sends, 40);
    check("A6 shutdown drained the queue: every accepted send went on the wire", landed == sends,
          "landed=" + num(landed) + " sent=" + num(sends));
    check("A6 a send gets at most one terminal callback", rec->done() <= static_cast<size_t>(sends),
          "done=" + num(static_cast<int64_t>(rec->done())) + " sent=" + num(sends));

    auto p2 = makeProducer(env, "a6-after");
    auto after = std::make_shared<Recorder>();
    p2->sendAsync(msg(t, "async-a6-after"), after, 8000);
    check("A6 the shut-down pool is not reused by another producer (a new producer can still send)",
          waitUntil([&]() { return after->done() >= 1; }, 20000) && after->ok() == 1,
          after->summary());
    p2->shutdown();
}

}  // namespace

int main(int argc, char** argv) {
    const std::string nsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    Env env;
    env.nsAddr = nsAddr;
    env.s = stamp();
    env.admin = std::make_unique<DefaultMQAdminExt>("ADMIN_async");
    env.admin->setNamesrvAddr(nsAddr);
    env.admin->setTimeoutMillis(10000);
    const std::string t1 = env.topic("AsyncReadBack");
    const std::string t2 = env.topic("AsyncBurst");
    const std::string t3 = env.topic("AsyncPinned");
    const std::string t4 = env.topic("AsyncForbidden");
    const std::string t5 = env.topic("AsyncBatch");
    const std::string t6 = env.topic("AsyncDrain");

    std::printf("############ C++ async send core live verification (namesrv=%s stamp=%lld)\n",
                nsAddr.c_str(), static_cast<long long>(env.s));
    try {
        env.admin->start();
        a1NonBlockingAndReadBack(env, t1);
        a2BurstExactlyOnceEach(env, t2);
        a3PinnedQueue(env, t3);
        a4ForbiddenHook(env, t4);
        a5BatchAsync(env, t5);
        a6ShutdownDrains(env, t6);
    } catch (const std::exception& e) {
        check("whole script executed", false, e.what());
    }

    const std::string addr = brokerAddr(env);
    for (const std::string& topic : {t1, t2, t3, t4, t5, t6}) {
        try {
            env.admin->deleteTopicInBroker(addr, topic);
        } catch (const std::exception&) {
            // cleanup failure does not affect the conclusion
        }
    }
    env.admin->shutdown();

    std::printf("\n############ PASS=%d FAIL=%d ############\n", gPass, gFail);
    for (const std::string& name : gFailed) std::printf("  FAILED: %s\n", name.c_str());
    return gFail == 0 ? 0 : 1;
}
