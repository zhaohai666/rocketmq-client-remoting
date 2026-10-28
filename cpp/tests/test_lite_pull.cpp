// Unit tests for the lite pull consumer (DefaultLitePullConsumer) + 220/221/309 bodies.
#include <cstdint>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/remoting/protocol/body.h"

using namespace rocketmq;

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg)                                                       \
    do {                                                                       \
        if (cond) {                                                            \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << "\n";                           \
        }                                                                      \
    } while (0)

#define CHECK_EQ(a, b, msg)                                                    \
    do {                                                                       \
        if ((a) == (b)) {                                                      \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << " (actual=" << (a)             \
                      << ", expect=" << (b) << ")\n";                          \
        }                                                                      \
    } while (0)

// Runs the call and hands back what it threw ("none" when nothing did): a guard that
// stays silent is not a guard, so every refusal below also names the message.
template <typename Fn>
static std::string threwMessage(Fn fn) {
    try {
        fn();
    } catch (const std::exception& e) {
        return std::string(e.what());
    }
    return "none";
}

static bool refusedWith(const std::string& threw, const std::string& needle) {
    return threw.find(needle) != std::string::npos;
}

// ---------------------------------------------------------------- the surface refuses misuse
static void testLitePullPreStartBehavior() {
    DefaultLitePullConsumer c;
    CHECK_EQ(c.consumerGroup(), std::string(MixAll::DEFAULT_CONSUMER_GROUP),
             "LitePull default consumer group = DEFAULT_CONSUMER");
    CHECK(!c.isStarted(), "not started after construction");

    // no namesrv configured / no subscription before start: start() must error explicitly (it cannot silently spin up idle threads)
    bool threw = false;
    try {
        c.start();
    } catch (const MQClientException&) {
        threw = true;
    } catch (...) {
        threw = true;
    }
    CHECK(threw, "start() throws without a subscription and without a namesrv");

    // An empty assign would revoke every queue held so far, which is what an empty route
    // lookup does by accident; a blank topic would be queried on the wire as a topic
    // literally named ""; a blank assign expression would quietly mean "everything".
    CHECK(refusedWith(threwMessage([&] { c.assign({}); }), "can not be null or empty"),
          "assign({}) is refused instead of revoking every held queue");
    CHECK(c.assignment().empty(), "a refused assign leaves the assignment alone");
    CHECK(refusedWith(threwMessage([&] { c.subscribe("", "TagA"); }), "Topic can not be null or empty"),
          "subscribe(\"\") is refused");
    CHECK(refusedWith(threwMessage([&] { c.subscribe("   ", "TagA"); }), "Topic can not be null or empty"),
          "a blank topic is refused whatever the padding");
    CHECK(refusedWith(threwMessage([&] { c.setSubExpressionForAssign("MyTopic", "  "); }),
                    "subExpression can not be null or empty"),
          "a blank assign expression is refused instead of meaning \"everything\"");
    CHECK(refusedWith(threwMessage([&] { c.setSubExpressionForAssign("  ", "TagA"); }),
                    "Topic can not be null or empty"),
          "a blank topic is refused as an assignment key too");

    c.subscribe("MyTopic", "TagA || TagB");

    // assign (resolveInitialOffset fails while not started but must be swallowed, no crash)
    MessageQueue mq("MyTopic", "broker-a", 3);
    c.assign({mq});
    CHECK_EQ(c.assignment().size(), static_cast<size_t>(1), "assignment holds 1 queue after assign");
    CHECK_EQ(c.assignment()[0].queueId, 3, "assignment queue id matches");

    // An empty poll and "no offset yet" are exactly what a queue at its tail looks like,
    // so a consumer that never started refuses instead of answering: this is how a missing
    // start() stays hidden forever.
    CHECK(refusedWith(threwMessage([&] { c.committed(mq); }), "not running"),
          "committed() on a consumer that never started refuses");
    CHECK(refusedWith(threwMessage([&] { c.poll(20); }), "not running"),
          "poll() on a consumer that never started refuses");
    // The state check comes first: an unstarted consumer is reported as that, not as a
    // timeout problem.
    CHECK(refusedWith(threwMessage([&] { c.poll(-1); }), "not running"),
          "the running check precedes the timeout check");

    // seek must still record the offset while not started (no broker query)
    c.seek(mq, 42);
    CHECK_EQ(c.assignment().size(), static_cast<size_t>(1), "assignment unchanged after seek");

    // pause / resume do not crash while not started
    c.pause({mq});
    c.resume({mq});

    c.subscribe("MyTopic", "TagC");

    // Running (the flag only: none of what follows reads the wire): the timeout is the
    // caller's to get right, and the assign expression is a pre-start setting because the
    // broker was told the old one at start.
    c.setAutoCommit(false);
    c.markRunningForTest();
    CHECK(refusedWith(threwMessage([&] { c.poll(-1); }), "Timeout must not be negative"),
          "a running consumer still refuses a negative timeout");
    CHECK(refusedWith(threwMessage([&] { c.setSubExpressionForAssign("MyTopic", "TagA"); }),
                    "only can be called before start"),
          "even a real expression is refused once the consumer runs");
    CHECK(threwMessage([&] { c.poll(20); }) == "none",
          "a running consumer with an empty buffer answers with an empty batch");
    CHECK_EQ(c.committed(mq), static_cast<int64_t>(-1),
             "a running consumer with no client says -1, not an error");
    // passing means no crash (the expressions live in private tables, covered by real-cluster verification)
    ++g_pass;
}

static void testLitePullOffsetTable() {
    DefaultLitePullConsumer c("LitePGOffsetTable");
    const MessageQueue q0("MyTopic", "broker-a", 0);
    const MessageQueue q1("MyTopic", "broker-a", 1);
    const MessageQueue stranger("MyTopic", "broker-a", 7);
    c.assign({q0, q1});
    // committed() only answers for a consumer that runs; the tables themselves are local,
    // so the running flag is set without a cluster.
    c.markRunningForTest();

    CHECK_EQ(c.pullCursorOf(q0), -1, "pull cursor = -1 before the start offset is resolved");
    CHECK_EQ(c.consumeCursorOf(q0), -1, "consume cursor = -1 when nothing was delivered");

    c.commit();
    CHECK_EQ(c.committed(q0), -1, "commitAll does not commit queues that were never consumed");

    // commit(map): a caller-specified offset only writes the commit target; neither cursor moves
    std::map<MessageQueue, int64_t> specified;
    specified[q0] = 5;
    specified[q1] = 8;
    c.commit(specified, false);
    CHECK_EQ(c.committed(q0), 5, "specified offset enters the in-memory offset table");
    CHECK_EQ(c.committed(q1), 8, "specified offset enters the in-memory offset table (q1)");
    CHECK_EQ(c.pullCursorOf(q0), -1, "committing an offset does not move the pull cursor");
    CHECK_EQ(c.consumeCursorOf(q0), -1, "committing an offset does not move the consume cursor");

    std::map<MessageQueue, int64_t> guarded;
    guarded[q0] = -1;
    guarded[stranger] = 3;
    c.commit(guarded, false);
    CHECK_EQ(c.committed(q0), 5, "offset == -1 only logs, does not overwrite an existing offset");
    CHECK_EQ(c.committed(stranger), -1, "a queue that was not assigned is not committed on its behalf");

    c.commit(std::map<MessageQueue, int64_t>(), false);
    CHECK_EQ(c.committed(q0), 5, "an empty map ignores this commit");
    c.commit(std::set<MessageQueue>(), false);
    CHECK_EQ(c.committed(q0), 5, "an empty set ignores this commit");

    // commit(set) uses the consume cursor, not an arbitrary value: nothing delivered means the guard blocks it
    std::set<MessageQueue> only0;
    only0.insert(q0);
    c.commit(only0, false);
    CHECK_EQ(c.committed(q0), 5, "commit(Set) does not write -1 without a delivery record");

    c.seek(q0, 2);
    CHECK_EQ(c.pullCursorOf(q0), 2, "seek moves the pull cursor");
    CHECK_EQ(c.consumeCursorOf(q0), 2, "seek must also move the consume cursor, else a replayed stretch would be skipped by the old offset");

    c.assign({q0});
    CHECK_EQ(c.pullCursorOf(q1), -1, "the pull cursor disappears after assign revokes the queue");
    CHECK_EQ(c.consumeCursorOf(q1), -1, "the consume cursor disappears after assign revokes the queue");
    CHECK_EQ(c.committed(q1), 8, "assign mode never touches offsetStore");
    // a revoked queue must be blocked when specified for commit again (it is no longer held by this instance)
    std::map<MessageQueue, int64_t> late;
    late[q1] = 99;
    c.commit(late, false);
    CHECK_EQ(c.committed(q1), 8, "a revoked queue gets no offset written");

    c.commit(only0, true);
    // this q0 commit uses the current consume cursor (the seek above moved it and the pull cursor to 2 together)
    CHECK_EQ(c.committed(q0), 2, "commit(Set) commits the consume cursor");
    CHECK_EQ(c.committed(q1), -1, "persistAll drops unnamed queues from the in-memory table");
}

static void testLitePullConsumeTimestamp() {
    // the group must be valid and must not be the reserved DEFAULT_CONSUMER: checkConfig runs before
    // start-offset validation, so the default group would only hit the group-name error (see the Validators::checkGroup wiring).
    DefaultLitePullConsumer c("LitePullTsGroup");
    CHECK_EQ(c.consumeTimestamp().size(), static_cast<size_t>(14), "default consumeTimestamp is 14 digits");
    bool allDigit = !c.consumeTimestamp().empty();
    for (char ch : c.consumeTimestamp()) {
        if (ch < '0' || ch > '9') allDigit = false;
    }
    CHECK(allDigit, "default consumeTimestamp is all digits");

    c.setNamesrvAddr("127.0.0.1:1");
    c.subscribe("MyTopic", "*");
    // a bare epoch-millis number must be rejected (the old implementation parsed it as epoch and silently computed a wrong start offset)
    c.setConsumeTimestamp("1700000000000");
    std::string what;
    bool threw = false;
    try {
        c.start();
    } catch (const MQClientException& e) {
        threw = true;
        what = e.what();
    } catch (...) {
        threw = true;
        what = "<not MQClientException>";
    }
    CHECK(threw, "start() throws on an invalid consumeTimestamp");
    CHECK(what.find("consumeTimestamp is invalid") != std::string::npos,
          "the exception wording matches the local checkConfig message");

    // a valid value must not be killed by this guard (an unreachable namesrv is a separate matter)
    DefaultLitePullConsumer ok;
    ok.setNamesrvAddr("127.0.0.1:1");
    ok.subscribe("MyTopic", "*");
    ok.setConsumeTimestamp("20230101000000");
    std::string okWhat;
    try {
        ok.start();
    } catch (const MQClientException& e) {
        okWhat = e.what();
    } catch (...) {
    }
    CHECK(okWhat.find("consumeTimestamp is invalid") == std::string::npos,
          "a valid consumeTimestamp is not misfired on");
    ok.shutdown();
}

// The auto-commit interval has a floor, and the floor is an acceptance rule rather than a clamp: a
// shorter setting is ignored outright, so the interval that was already running keeps running.
static void testLitePullAutoCommitIntervalFloor() {
    DefaultLitePullConsumer c;
    CHECK_EQ(c.autoCommitIntervalMillis(), 5000, "auto-commit interval starts at its default");
    for (int32_t rejected : {999, 0, -1}) {
        c.setAutoCommitIntervalMillis(rejected);
        CHECK_EQ(c.autoCommitIntervalMillis(), 5000,
                 "a setting below the floor is ignored, not clamped to it");
    }
    c.setAutoCommitIntervalMillis(kMinAutoCommitIntervalMillis);
    CHECK_EQ(c.autoCommitIntervalMillis(), 1000, "the floor itself is accepted");
    c.setAutoCommitIntervalMillis(2500);
    c.setAutoCommitIntervalMillis(500);
    CHECK_EQ(c.autoCommitIntervalMillis(), 2500,
             "a rejected setting leaves the accepted interval running");
}

// ---------------------------------------------------------------- 221 GetConsumerStatusBody
static void testGetConsumerStatusBodyWireShape() {
    GetConsumerStatusBody b;
    b.messageQueueTable[MessageQueue("MyTopic", "broker-a", 3)] = 42;
    b.messageQueueTable[MessageQueue("MyTopic", "broker-a", 1)] = 7;

    std::string json = b.encode();
    // the key is fastjson2-style inline MessageQueue JSON (alphabetical: brokerName, queueId, topic),
    // serialized as an **escaped string key** (the same encoding as the real-cluster-verified 307 mqTable)
    CHECK(json.find("\"{\\\"brokerName\\\":\\\"broker-a\\\",\\\"queueId\\\":3,"
                    "\\\"topic\\\":\\\"MyTopic\\\"}\":42") != std::string::npos,
          "221 messageQueueTable inline object-key shape");
    CHECK(json.find("\"consumerTable\":{}") != std::string::npos,
          "221 consumerTable empty object must be serialized");
    CHECK(json.find("\"messageQueueTable\":") != std::string::npos,
          "221 top-level key name messageQueueTable");
}

// ---------------------------------------------------------------- 309 ConsumeMessageDirectlyResult
static void testConsumeMessageDirectlyResult() {
    ConsumeMessageDirectlyResult def;
    CHECK(!def.order, "309 default order=false");
    CHECK(def.autoCommit, "309 default autoCommit=true");

    std::string json = def.encode();
    CHECK(json.find("\"order\":false") != std::string::npos, "309 order field name");
    CHECK(json.find("\"autoCommit\":true") != std::string::npos, "309 autoCommit field name");
    CHECK(json.find("\"consumeResult\":null") != std::string::npos, "309 empty consumeResult serializes to null");
    CHECK(json.find("\"remark\":null") != std::string::npos, "309 empty remark serializes to null");
    CHECK(json.find("\"spentTimeMills\":0") != std::string::npos, "309 spentTimeMills field name");

    // the receipt of a successful consume
    ConsumeMessageDirectlyResult ok;
    ok.consumeResult = "CR_SUCCESS";
    ok.spentTimeMills = 12;
    std::string okJson = ok.encode();
    CHECK(okJson.find("\"consumeResult\":\"CR_SUCCESS\"") != std::string::npos,
          "309 CR_SUCCESS");

    // the exception receipt + round trip
    ConsumeMessageDirectlyResult bad;
    bad.order = true;
    bad.consumeResult = "CR_THROW_EXCEPTION";
    bad.remark = "std::exception: boom";
    bad.spentTimeMills = 33;
    ConsumeMessageDirectlyResult back;
    CHECK(ConsumeMessageDirectlyResult::decode(bad.encode(), back), "309 round-trip decode");
    CHECK(back.order, "round trip keeps order=true");
    CHECK(back.autoCommit, "round trip keeps autoCommit at the default true");
    CHECK_EQ(back.consumeResult, std::string("CR_THROW_EXCEPTION"), "round trip consumeResult");
    CHECK_EQ(back.remark, std::string("std::exception: boom"), "round trip remark");
    CHECK_EQ(back.spentTimeMills, static_cast<int64_t>(33), "round trip spentTimeMills");
}

int main() {
    testLitePullPreStartBehavior();
    testLitePullOffsetTable();
    testLitePullConsumeTimestamp();
    testLitePullAutoCommitIntervalFloor();
    testGetConsumerStatusBodyWireShape();
    testConsumeMessageDirectlyResult();
    std::cout << "lite_pull: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
