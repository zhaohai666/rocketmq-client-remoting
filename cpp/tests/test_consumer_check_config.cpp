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

// 每个消费者都从"只差数值"的合法配置起步，逐条只改一个字段。
DefaultMQPushConsumer fresh() {
    return DefaultMQPushConsumer("GID_CheckConfigCppUnit");
}

// 抓 checkConfigRanges() 的文案；没抛返回空串。
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
                     c.setConsumeThreadMax(1000);   // 否则先撞上 "min is larger than max"
                     c.setConsumeThreadMin(static_cast<int32_t>(n));
                 },
                 true, false});
    v.push_back({"consumeThreadMax", 1, 1000, "consumeThreadMax Out of range [1, 1000]",
                 [](DefaultMQPushConsumer& c, int64_t n) {
                     c.setConsumeThreadMin(1);      // 默认 min=20，压 max 到 1 会先撞那道门
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

// ---------------- 默认值 ----------------

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

// ---------------- 区间：边界合法、越界被拒 ----------------

void testBoundsAndRejection() {
    for (const Gate& g : gates()) {
        // 上下界**本身**必须放行：把 < lo 写成 <= lo 只有这一格能抓到
        for (int64_t bound : {g.lo, g.hi}) {
            DefaultMQPushConsumer c = fresh();
            g.set(c, bound);
            expect(gateMessage(c).empty(), g.field + ".bound" + num(bound), gateMessage(c));
        }
        // 上界 +1 一律被拒，且文案一字不差
        {
            DefaultMQPushConsumer c = fresh();
            g.set(c, g.hi + 1);
            expect(gateMessage(c) == g.msg, g.field + ".aboveHi",
                   "want=[" + g.msg + "] got=[" + gateMessage(c) + "]");
        }
        // 下界 -1：够得着就被拒；setter 先夹到 1 的字段锁"夹到 1"这个行为
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
        // 哨兵：只有 -1 放行，-2 落进区间判断
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

// ---------------- min / max 的相对关系 ----------------

void testThreadMinNotLargerThanMax() {
    DefaultMQPushConsumer c = fresh();
    c.setConsumeThreadMin(64);
    c.setConsumeThreadMax(32);
    const std::string want = "consumeThreadMin (64) is larger than consumeThreadMax (32)";
    expect(gateMessage(c) == want, "minLargerThanMax",
           "want=[" + want + "] got=[" + gateMessage(c) + "]");

    // 严格 > ：min == max 是合法的单线程池配置
    DefaultMQPushConsumer e = fresh();
    e.setConsumeThreadMin(1);
    e.setConsumeThreadMax(1);
    expect(gateMessage(e).empty(), "minEqualsMax", gateMessage(e));
    DefaultMQPushConsumer f = fresh();
    f.setConsumeThreadMin(32);
    f.setConsumeThreadMax(32);
    expect(gateMessage(f).empty(), "minEqualsMax32", gateMessage(f));

    // 两道区间排在相对关系之前：min 自身越界时报的是 min 的区间，不是把两个数字塞进
    // "is larger than" 文案
    DefaultMQPushConsumer g = fresh();
    g.setConsumeThreadMax(1);
    g.setConsumeThreadMin(2000);
    expect(gateMessage(g) == "consumeThreadMin Out of range [1, 1000]", "rangeBeatsRelative",
           gateMessage(g));
}

// ---------------- 闸门顺序 ----------------

void testGateCheckOrder() {
    // 同时写坏两道时报靠前的那道：一次只吐一个错才修得动。
    DefaultMQPushConsumer c = fresh();
    c.setConsumeThreadMax(1001);
    c.setPullBatchSize(0);
    expect(gateMessage(c) == "consumeThreadMax Out of range [1, 1000]", "firstGateWins",
           gateMessage(c));
    // 修掉前者后，后者才露出来（顺序反了的话这条会红）
    c.setConsumeThreadMax(64);
    expect(gateMessage(c) == "pullBatchSize Out of range [1, 1024]", "secondGateAfterFix",
           gateMessage(c));
}

// ---------------- 闸门是本地行为，排在网络之前 ----------------

class NullListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

// 地址指向一个**没人监听**的端口：闸门若真的在建实例之前，start() 报的一定是配置文案，
// 而不是连接失败。这条锁的是"起了后台线程再抛错"泄漏线程的那类错法。
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
