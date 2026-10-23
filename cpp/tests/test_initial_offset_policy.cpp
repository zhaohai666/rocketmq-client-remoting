// The start point of a freshly assigned queue is decided by one table: the stored offset first, then
// consumeFromWhere -- never the subscription type and never the offset *storage mode*.
//
// Why this must be pinned offline: the wrong shapes fail silently on real hardware. Short-circuiting
// SQL92 subscriptions to the queue tail drops the whole backlog (send-first + CONSUME_FROM_FIRST_OFFSET
// received nothing; consumer-first only passed because the queues were empty). Broadcasting used to
// return before the timestamp branch, so CONSUME_FROM_TIMESTAMP degraded to "start now" for exactly
// the mode that has no broker-side offset at all. And a first-time %RETRY% queue that starts at the
// tail never replays the messages that triggered the retry.
//
// The table is separated from the I/O that gathers its inputs (local file / broker commit table for
// the stored offset, maxOffset and searchOffset RPCs for the two computed starts), so every cell is
// reachable without a cluster.
#include <cstdio>
#include <stdexcept>
#include <string>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/util_all.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

const char* kPlanNames[] = {"stored", "zero", "tail", "search"};

const char* planName(StartOffsetPlan p) {
    return kPlanNames[static_cast<int>(p)];
}

void expectPlan(StartOffsetPlan actual, StartOffsetPlan want, const std::string& name) {
    ++checks;
    if (actual != want) {
        ++fails;
        std::printf("FAIL %s (want=%s got=%s)\n", name.c_str(), planName(want), planName(actual));
    }
}

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

const std::string kFirst = ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET;
const std::string kLast = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;
const std::string kTs = ConsumeFromWhere::CONSUME_FROM_TIMESTAMP;
// The deprecated/alias variants share the LAST_OFFSET cell of the table.
const std::string kLastOrMin = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST;
const std::string kMin = ConsumeFromWhere::CONSUME_FROM_MIN_OFFSET;
const std::string kMax = ConsumeFromWhere::CONSUME_FROM_MAX_OFFSET;

const std::string kBusiness = "InitialOffsetPolicyTopic";
std::string retryTopic() { return MixAll::getRetryTopic("GID_InitialOffsetPolicy"); }

// ------------------------------------------------- a committed offset always wins

void testStoredOffsetWinsOverEveryPolicy() {
    for (const std::string& where : {kFirst, kLast, kTs, kLastOrMin, kMin, kMax}) {
        expectPlan(planInitialOffset(0, where, false), StartOffsetPlan::kStored,
                   "stored.zero beats " + where);
        expectPlan(planInitialOffset(7, where, true), StartOffsetPlan::kStored,
                   "stored.on retry queue beats " + where);
        // Paired control: with nothing committed the same policy must not report a stored start,
        // else the two assertions above would pass on an implementation that always returns kStored.
        expect(planInitialOffset(-1, where, false) != StartOffsetPlan::kStored,
               "nothing committed is not a stored start: " + where);
    }
}

void testNoStoredOffsetLeavesTheNegativeSentinelAlone() {
    // 0 is a real offset ("start at the first message"); only a negative answer means "never consumed".
    expectPlan(planInitialOffset(0, kLast, false), StartOffsetPlan::kStored, "offset 0 is a commit");
    expectPlan(planInitialOffset(-1, kLast, false), StartOffsetPlan::kQueueTail, "-1 is no commit");
}

// ------------------------------------------------- FIRST_OFFSET replays from 0

void testFirstOffsetNeverQueriesTheFloor() {
    expectPlan(planInitialOffset(-1, kFirst, false), StartOffsetPlan::kFromZero,
               "FIRST starts at the literal 0");
    // Same cell for a retry queue: FIRST is already the earliest possible start.
    expectPlan(planInitialOffset(-1, kFirst, true), StartOffsetPlan::kFromZero,
               "FIRST on a retry queue is still 0");
}

// ------------------------------------------------- TIMESTAMP: search, except on the retry queue

void testTimestampStartSearchesTheConfiguredInstant() {
    expectPlan(planInitialOffset(-1, kTs, false), StartOffsetPlan::kSearchAtConfiguredTime,
               "TIMESTAMP searches the configured instant");
    // Paired control: a %RETRY% queue is created by the first redelivery, long after that instant, so
    // searching there answers nothing -- it must take the tail instead. If the retry case were
    // dropped, these two cells would collapse to the same plan and one of them goes red.
    expectPlan(planInitialOffset(-1, kTs, true), StartOffsetPlan::kQueueTail,
               "TIMESTAMP on a retry queue takes the tail");
}

// ------------------------------------------------- LAST (and its aliases): tail, except on the retry queue

void testLastOffsetSkipsTheBacklogButRedeliveryDoesNot() {
    for (const std::string& where : {kLast, kLastOrMin, kMin, kMax}) {
        expectPlan(planInitialOffset(-1, where, false), StartOffsetPlan::kQueueTail,
                   where + " starts at the tail");
        expectPlan(planInitialOffset(-1, where, true), StartOffsetPlan::kFromZero,
                   where + " on a retry queue replays from 0");
    }
}

// ------------------------------------------------- the retry topic really is the one the client writes

void testRetryTopicClassificationMatchesTheWriter() {
    expect(MixAll::isRetryTopic(retryTopic()), "the group's retry topic classifies as retry");
    expect(!MixAll::isRetryTopic(kBusiness), "a business topic does not");
}

// ------------------------------------------------- consumeTimestamp is a hard format gate

void testConsumeTimestampParsesRoundTrippedInstants() {
    const int64_t t = UtilAll::parseConsumeTimestamp("20230101000000");
    expect(t % 1000 == 0, "a parsed instant is whole-second", std::to_string(t));
    expect(UtilAll::timeMillisToHumanString3(t) == "20230101000000",
           "parse and format round-trip", UtilAll::timeMillisToHumanString3(t));
    expect(UtilAll::parseConsumeTimestamp("20230101000001") - t == 1000,
           "one second later is 1000ms apart");
    expect(UtilAll::parseConsumeTimestamp("20230101010000") > t, "a later hour sorts later");
}

std::string parseError(const std::string& ts) {
    try {
        UtilAll::parseConsumeTimestamp(ts);
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

void testMalformedConsumeTimestampIsRejected() {
    for (const std::string& bad : {std::string(), std::string("2023010100000"),
                                   std::string("2023010100000a"), std::string("abcdefghijklmn"),
                                   std::string("202301010000000")}) {
        const std::string what = parseError(bad);
        expect(what.find("consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received " +
                             bad) == 0,
               "malformed timestamp rejected", "input=<" + bad + "> what=" + what);
    }
    expect(parseError("20230101000000").empty(), "a valid timestamp is not rejected");
}

void testStartupChecksTheTimestampWhateverThePolicyIs() {
    DefaultMQPushConsumer c("GID_InitialOffsetPolicyCheckConfig");
    // The default is a real instant (boot time minus 30 minutes), so start() must not fire on it --
    // an empty default would silently turn CONSUME_FROM_TIMESTAMP into "from now".
    expect(c.consumeTimestamp().size() == 14, "default consumeTimestamp is 14 digits",
           c.consumeTimestamp());
    bool defaultOk = true;
    try {
        c.checkConfigRanges();
    } catch (const std::exception& e) {
        defaultOk = false;
        std::printf("  default config rejected: %s\n", e.what());
    }
    expect(defaultOk, "the default configuration passes checkConfig");

    c.setConsumeTimestamp("2023010100000");
    const std::string what = [&c] {
        try {
            c.checkConfigRanges();
        } catch (const std::exception& e) {
            return std::string(e.what());
        }
        return std::string();
    }();
    expect(what.find("consumeTimestamp is invalid") != std::string::npos,
           "a malformed consumeTimestamp fails at startup", what);

    // Paired control: the same consumer with a well-formed value passes, so the rejection above is
    // the format gate and not a constant throw.
    c.setConsumeTimestamp("20200101000000");
    bool fixed = true;
    try {
        c.checkConfigRanges();
    } catch (const std::exception&) {
        fixed = false;
    }
    expect(fixed, "a corrected consumeTimestamp starts again");
}

}  // namespace

int main() {
    testStoredOffsetWinsOverEveryPolicy();
    testNoStoredOffsetLeavesTheNegativeSentinelAlone();
    testFirstOffsetNeverQueriesTheFloor();
    testTimestampStartSearchesTheConfiguredInstant();
    testLastOffsetSkipsTheBacklogButRedeliveryDoesNot();
    testRetryTopicClassificationMatchesTheWriter();
    testConsumeTimestampParsesRoundTrippedInstants();
    testMalformedConsumeTimestampIsRejected();
    testStartupChecksTheTimestampWhateverThePolicyIs();
    std::printf("initial offset policy: %d checks, %d failed\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
