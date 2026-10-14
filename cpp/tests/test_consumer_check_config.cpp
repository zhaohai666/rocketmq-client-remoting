#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/common/message.h"

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

std::string num(int64_t v) { return std::to_string(v); }

struct Gate {
    std::string field;
    int64_t lo;
    int64_t hi;
    std::string msg;
    std::function<void(DefaultMQPushConsumer&, int64_t)> set;
    bool lowClampedBySetter = false;
    bool hasMinusOneSentinel = false;
};

// Every consumer starts from a legal config differing only in numbers; each case changes one field.
DefaultMQPushConsumer fresh() {
    return DefaultMQPushConsumer("GID_CheckConfigCppUnit");
}

// Capture the message from checkConfigRanges(); returns an empty string when nothing is thrown.
std::string gateMessage(DefaultMQPushConsumer& c) {
    try {
        c.checkConfigRanges();
    } catch (const MQClientException& e) {
        return std::string(e.what());
    } catch (const std::exception& e) {
        return std::string("WRONG_TYPE: ") + e.what();
    }
    return std::string();
}

std::vector<Gate> gates() {
    std::vector<Gate> v;
    v.push_back({"consumeThreadMin", 1, 1000, "consumeThreadMin Out of range [1, 1000]",
                 [](DefaultMQPushConsumer& c, int64_t n) {
                     c.setConsumeThreadMax(1000);   // otherwise "min is larger than max" fires first
                     c.setConsumeThreadMin(static_cast<int32_t>(n));
                 },
                 true, false});
    v.push_back({"consumeThreadMax", 1, 1000, "consumeThreadMax Out of range [1, 1000]",
                 [](DefaultMQPushConsumer& c, int64_t n) {
                     c.setConsumeThreadMin(1);      // default min=20, pushing max to 1 hits that gate first
                     c.setConsumeThreadMax(static_cast<int32_t>(n));
                 },
                 true, false});
    v.push_back({"consumeConcurrentlyMaxSpan", 1, 65535,
                 "consumeConcurrentlyMaxSpan Out of range [1, 65535]",
                 [](DefaultMQPushConsumer& c, int64_t n) { c.setConsumeConcurrentlyMaxSpan(n); },
                 false, false});
    v.push_back({"pullThresholdForQueue", 1, 65535,
                 "pullThresholdForQueue Out of range [1, 65535]",
                 [](DefaultMQPushConsumer& c, int64_t n) {
                     c.setPullThresholdForQueue(static_cast<int32_t>(n));
                 },
                 false, false});
    v.push_back({"pullThresholdForTopic", 1, 6553500,
                 "pullThresholdForTopic Out of range [1, 6553500]",
                 [](DefaultMQPushConsumer& c, int64_t n) {
                     c.setPullThresholdForTopic(static_cast<int32_t>(n));
                 },
                 false, true});
    v.push_back({"pullThresholdSizeForQueue", 1, 1024,
                 "pullThresholdSizeForQueue Out of range [1, 1024]",
                 [](DefaultMQPushConsumer& c, int64_t n) {
                     c.setPullThresholdSizeForQueue(static_cast<int32_t>(n));
                 },
                 false, false});
    v.push_back({"pullThresholdSizeForTopic", 1, 102400,
                 "pullThresholdSizeForTopic Out of range [1, 102400]",
                 [](DefaultMQPushConsumer& c, int64_t n) {
                     c.setPullThresholdSizeForTopic(static_cast<int32_t>(n));
                 },
                 false, true});
    v.push_back({"pullInterval", 0, 65535, "pullInterval Out of range [0, 65535]",
                 [](DefaultMQPushConsumer& c, int64_t n) {
                     c.setPullIntervalMillis(static_cast<int32_t>(n));
                 },
                 false, false});
    v.push_back({"consumeMessageBatchMaxSize", 1, 1024,
                 "consumeMessageBatchMaxSize Out of range [1, 1024]",
                 [](DefaultMQPushConsumer& c, int64_t n) {
                     c.setConsumeMessageBatchMaxSize(static_cast<int32_t>(n));
                 },
                 true, false});
    v.push_back({"pullBatchSize", 1, 1024, "pullBatchSize Out of range [1, 1024]",
                 [](DefaultMQPushConsumer& c, int64_t n) {
                     c.setPullBatchSize(static_cast<int32_t>(n));
                 },
                 false, false});
    v.push_back({"popInvisibleTime", 5000, 300000,
                 "popInvisibleTime Out of range [5000, 300000]",
                 [](DefaultMQPushConsumer& c, int64_t n) { c.setPopInvisibleTime(n); },
                 false, false});
    v.push_back({"popBatchNums", 1, 32, "popBatchNums Out of range [1, 32]",
                 [](DefaultMQPushConsumer& c, int64_t n) {
                     c.setPopBatchNums(static_cast<int32_t>(n));
                 },
                 false, false});
    return v;
}

// ---------------- Default values ----------------

void testDefaultsClearEveryGate() {
    DefaultMQPushConsumer c = fresh();
    expect(gateMessage(c).empty(), "defaults.pass", gateMessage(c));
    expect(c.getConsumeThreadMin() == 20 && c.getConsumeThreadMax() == 20, "defaults.threadNums",
           num(c.getConsumeThreadMin()) + "/" + num(c.getConsumeThreadMax()));
    expect(c.consumeConcurrentlyMaxSpan() == 2000, "defaults.maxSpan",
           num(c.consumeConcurrentlyMaxSpan()));
    expect(c.pullThresholdForQueue() == 1000 && c.pullThresholdSizeForQueue() == 100,
           "defaults.queueThresholds",
           num(c.pullThresholdForQueue()) + "/" + num(c.pullThresholdSizeForQueue()));
    expect(c.pullThresholdForTopic() == -1 && c.pullThresholdSizeForTopic() == -1,
           "defaults.topicThresholdsAreSentinel",
           num(c.pullThresholdForTopic()) + "/" + num(c.pullThresholdSizeForTopic()));
    expect(c.popInvisibleTime() == 60000 && c.popBatchNums() == 32, "defaults.pop",
           num(c.popInvisibleTime()) + "/" + num(c.popBatchNums()));
}

// ---------------- Ranges: bounds legal, out-of-range rejected ----------------

void testBoundsAndRejection() {
    for (const Gate& g : gates()) {
        // The bounds **themselves** must pass: only this cell catches writing < lo as <= lo
        for (int64_t bound : {g.lo, g.hi}) {
            DefaultMQPushConsumer c = fresh();
            g.set(c, bound);
            expect(gateMessage(c).empty(), g.field + ".bound" + num(bound), gateMessage(c));
        }
        // Upper bound + 1 is always rejected, and the message must match character for character
        {
            DefaultMQPushConsumer c = fresh();
            g.set(c, g.hi + 1);
            expect(gateMessage(c) == g.msg, g.field + ".aboveHi",
                   "want=[" + g.msg + "] got=[" + gateMessage(c) + "]");
        }
        // Lower bound - 1: rejected when reachable; fields whose setter clamps to 1 lock that in
        {
            DefaultMQPushConsumer c = fresh();
            g.set(c, g.lo - 1);
            if (g.lowClampedBySetter) {
                expect(gateMessage(c).empty(), g.field + ".belowLoClampedToLegal",
                       gateMessage(c));
                DefaultMQPushConsumer d = fresh();
                g.set(d, 0);
                const int64_t got = g.field == "consumeThreadMin" ? d.getConsumeThreadMin()
                              : g.field == "consumeThreadMax" ? d.getConsumeThreadMax()
                                                              : d.consumeMessageBatchMaxSize();
                expect(got == 1, g.field + ".setterClampsToOne", num(got));
            } else {
                expect(gateMessage(c) == g.msg, g.field + ".belowLo",
                       "want=[" + g.msg + "] got=[" + gateMessage(c) + "]");
            }
        }
        if (!g.lowClampedBySetter && !g.hasMinusOneSentinel && g.lo != 0) {
            DefaultMQPushConsumer c = fresh();
            g.set(c, 0);
            expect(gateMessage(c) == g.msg, g.field + ".zeroRejected",
                   "want=[" + g.msg + "] got=[" + gateMessage(c) + "]");
        }
        // Sentinel: only -1 passes, -2 falls into the range comparison
        if (g.hasMinusOneSentinel) {
            DefaultMQPushConsumer c = fresh();
            g.set(c, -1);
            expect(gateMessage(c).empty(), g.field + ".sentinelPasses", gateMessage(c));
            DefaultMQPushConsumer d = fresh();
            g.set(d, -2);
            expect(gateMessage(d) == g.msg, g.field + ".minusTwoRejected",
                   "want=[" + g.msg + "] got=[" + gateMessage(d) + "]");
        }
    }
}

// ---------------- Relative relationship between min and max ----------------

void testThreadMinNotLargerThanMax() {
    DefaultMQPushConsumer c = fresh();
    c.setConsumeThreadMin(64);
    c.setConsumeThreadMax(32);
    const std::string want = "consumeThreadMin (64) is larger than consumeThreadMax (32)";
    expect(gateMessage(c) == want, "minLargerThanMax",
           "want=[" + want + "] got=[" + gateMessage(c) + "]");

    // Strict > : min == max is a legal single-thread pool configuration
    DefaultMQPushConsumer e = fresh();
    e.setConsumeThreadMin(1);
    e.setConsumeThreadMax(1);
    expect(gateMessage(e).empty(), "minEqualsMax", gateMessage(e));
    DefaultMQPushConsumer f = fresh();
    f.setConsumeThreadMin(32);
    f.setConsumeThreadMax(32);
    expect(gateMessage(f).empty(), "minEqualsMax32", gateMessage(f));

    // The two range checks run before the relative check: an out-of-range min reports min's own
    // range message instead of stuffing both numbers into the "is larger than" text
    DefaultMQPushConsumer g = fresh();
    g.setConsumeThreadMax(1);
    g.setConsumeThreadMin(2000);
    expect(gateMessage(g) == "consumeThreadMin Out of range [1, 1000]", "rangeBeatsRelative",
           gateMessage(g));
}

// ---------------- Gate order ----------------

void testGateCheckOrder() {
    // With two gates broken at once the earlier one reports: one error at a time is actionable.
    DefaultMQPushConsumer c = fresh();
    c.setConsumeThreadMax(1001);
    c.setPullBatchSize(0);
    expect(gateMessage(c) == "consumeThreadMax Out of range [1, 1000]", "firstGateWins",
           gateMessage(c));
    // Only after the first one is fixed does the second surface (this goes red if the order flips)
    c.setConsumeThreadMax(64);
    expect(gateMessage(c) == "pullBatchSize Out of range [1, 1024]", "secondGateAfterFix",
           gateMessage(c));
}

// ---------------- Gates are local behaviour, ahead of any network access ----------------

class NullListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

// The address points at a port **nobody listens on**: if the gate runs before construction, start()
// must report the config text, not a connection failure; this locks out leaking a started thread.
void testStartRejectsBeforeTouchingNetwork() {
    const std::string dead = "127.0.0.1:1";
    struct Case {
        std::string name;
        std::string want;
        std::function<void(DefaultMQPushConsumer&)> set;
    };
    const std::vector<Case> cases = {
        {"size", "pullThresholdSizeForQueue Out of range [1, 1024]",
         [](DefaultMQPushConsumer& c) { c.setPullThresholdSizeForQueue(0); }},
        {"batch", "pullBatchSize Out of range [1, 1024]",
         [](DefaultMQPushConsumer& c) { c.setPullBatchSize(1025); }},
        {"pop", "popInvisibleTime Out of range [5000, 300000]",
         [](DefaultMQPushConsumer& c) { c.setPopInvisibleTime(4999); }},
        {"popNums", "popBatchNums Out of range [1, 32]",
         [](DefaultMQPushConsumer& c) { c.setPopBatchNums(33); }},
        {"threadNums", "consumeThreadMin (8) is larger than consumeThreadMax (4)",
         [](DefaultMQPushConsumer& c) {
             c.setConsumeThreadMin(8);
             c.setConsumeThreadMax(4);
         }},
    };
    for (const Case& kase : cases) {
        DefaultMQPushConsumer c = fresh();
        c.setNamesrvAddr(dead);
        c.setMessageListener(std::make_shared<NullListener>());
        c.subscribe("CheckConfigCppUnitTopic");
        kase.set(c);
        std::string got = "NO_THROW";
        try {
            c.start();
        } catch (const MQClientException& e) {
            got = e.what();
        } catch (const std::exception& e) {
            got = std::string("WRONG_TYPE: ") + e.what();
        }
        expect(got == kase.want, "start.rejects." + kase.name,
               "want=[" + kase.want + "] got=[" + got + "]");
        expect(!c.isStarted(), "start.notStarted." + kase.name);
    }

    {
        DefaultMQPushConsumer c("bad group!!");
        c.setNamesrvAddr(dead);
        c.setMessageListener(std::make_shared<NullListener>());
        c.subscribe("CheckConfigCppUnitTopic");
        c.setPullBatchSize(0);
        std::string got = "NO_THROW";
        try {
            c.start();
        } catch (const MQClientException& e) {
            got = e.what();
        } catch (const std::exception& e) {
            got = std::string("WRONG_TYPE: ") + e.what();
        }
        expect(got.find("group") != std::string::npos, "start.groupBeatsRanges", got);
        expect(!c.isStarted(), "start.groupBeatsRanges.notStarted");
    }
}

}  // namespace

int main() {
    testDefaultsClearEveryGate();
    testBoundsAndRejection();
    testThreadMinNotLargerThanMax();
    testGateCheckOrder();
    testStartRejectsBeforeTouchingNetwork();
    std::printf("consumer_check_config: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
