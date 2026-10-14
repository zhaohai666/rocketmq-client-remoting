// This locks the threshold arithmetic and the per-queue stamp schedule; "consumption really resumes after removal" needs real threads and a real broker
// accepting offsets, verifiable only on a live cluster (the S11 stall self-healing scenario in examples/live_redelivery.cpp).
#include <cstdio>
#include <string>

#include "rocketmq/client/consumer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/util_all.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

const char* kGroup = "GID_PullExpiredCppUnit";
const char* kTopic = "PullExpiredCppUnitTopic";
const char* kBroker = "broker-a";

std::string queueKey(int32_t queueId) {
    return DefaultMQPushConsumer::offsetKey(MessageQueue(kTopic, kBroker, queueId));
}

void testStallThresholdDefault() {
    expect(kPullMaxIdleTime == 120000, "threshold.120s", std::to_string(kPullMaxIdleTime));
}

void testNoStampIsNotStalled() {
    // A freshly assigned loop that has not reached the stamping point is not stalled, otherwise "start thread -> removed at once" would thrash back and forth.
    DefaultMQPushConsumer c(kGroup);
    const std::string key = queueKey(0);
    expect(c.lastPullAt(key) == -1, "table.absent");
    expect(!c.pullStalled(key), "stamp.missing.notStalled");
}

void testFreshStampIsNotStalled() {
    DefaultMQPushConsumer c(kGroup);
    const std::string key = queueKey(0);
    c.setLastPullAt(key, UtilAll::currentTimeMillis());
    expect(!c.pullStalled(key), "stamp.fresh.notStalled");
    expect(c.lastPullAt(key) > 0, "stamp.written");
}

void testThresholdBoundaryIsStrict() {
    DefaultMQPushConsumer c(kGroup);
    const std::string key = queueKey(0);
    const int64_t now = UtilAll::currentTimeMillis();
    c.setLastPullAt(key, now - kPullMaxIdleTime);
    expect(!c.pullStalled(key), "stamp.exactlyAtLimit.notStalled");
    c.setLastPullAt(key, now - kPullMaxIdleTime - 1);
    expect(c.pullStalled(key), "stamp.oneMsPast.stalled");
}

void testOldEnoughStampAlwaysStalls() {
    DefaultMQPushConsumer c(kGroup);
    const std::string key = queueKey(0);
    c.setLastPullAt(key, UtilAll::currentTimeMillis() - (kPullMaxIdleTime + 5000));
    expect(c.pullStalled(key), "stamp.stale.stalled");
}

void testLoopExitMarker() {
    // The pull loop returned by itself yet still owns the queue (exception punched through / subscription lost): the thread wrapper stamps the time as 0.
    // A dead std::thread cannot answer is_alive() (joinable() is still true), so it must report in by itself.
    DefaultMQPushConsumer c(kGroup);
    const std::string key = queueKey(0);
    c.setLastPullAt(key, UtilAll::currentTimeMillis());
    expect(!c.pullStalled(key), "marker.beforeExit.notStalled");
    c.setLastPullAt(key, 0);
    expect(c.pullStalled(key), "marker.afterExit.stalled");
}

void testStampIsPerQueue() {
    // The stall check is **per queue**: one stuck queue must not get other queues of the same topic removed along with it (that is what manufactures duplicates).
    DefaultMQPushConsumer c(kGroup);
    const std::string k0 = queueKey(0);
    const std::string k1 = queueKey(1);
    const int64_t now = UtilAll::currentTimeMillis();
    c.setLastPullAt(k0, now - kPullMaxIdleTime - 1000);
    c.setLastPullAt(k1, now);
    expect(c.pullStalled(k0), "perQueue.k0.stalled");
    expect(!c.pullStalled(k1), "perQueue.k1.notStalled");
}

}  // namespace

int main() {
    testStallThresholdDefault();
    testNoStampIsNotStalled();
    testFreshStampIsNotStalled();
    testThresholdBoundaryIsStrict();
    testOldEnoughStampAlwaysStalls();
    testLoopExitMarker();
    testStampIsPerQueue();
    std::printf("pull expired: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
