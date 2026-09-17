// POP **消费侧**（DefaultMQPushConsumer::setPopMode(true)）单测 —— 不需要集群。
//
// 为什么单独一个文件：协议管道（test_pop.cpp）过了 ≠ 消费循环对。消费侧有自己一套
// 容易错的语义，而且错得**很安静** —— 比如 ackIndex 默认值不对，CONSUME_SUCCESS 会
// 一条都不 ack，消息在 invisibleTime 到期后被 broker 复活重投；如果观测窗口比
// invisibleTime 短，真机看起来还是"全过"。所以能离线锁的必须先锁死。
//
// 覆盖：
//   - PopProcessQueue 计数与 dropped 语义
//   - POP_CK → (topic, brokerName, queueId, offset) 的还原，含 retry topic 反解
//   - isPopTimeout 判定
//   - 默认值与 Java 对齐（关掉时走原路径、poll < timeout、batchNums ≤ 32）
//   - 延迟档位表（单位秒）
#include <cstdio>
#include <string>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/extra_info.h"

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

const char* kGroup = "GID_PopCppUnit";
const char* kTopic = "PopCppUnitTopic";
const char* kBroker = "broker-a";

// 手工拼 8 段 CK：buildExtraInfo 的 retryFlag 由 topic 推出，而这里要显式指定 retry 段
std::string ckSeg(int32_t queueId = 0, int64_t offset = 0, const std::string& retry = "0",
                  int64_t popTime = 1000, int64_t invisible = 60000) {
    char buf[256];
    std::snprintf(buf, sizeof(buf), "0 %lld %lld 0 %s %s %d %lld",
                  static_cast<long long>(popTime), static_cast<long long>(invisible),
                  retry.c_str(), kBroker, queueId, static_cast<long long>(offset));
    return std::string(buf);
}

MessageExt makeMsg(const std::string& topic = kTopic, int32_t queueId = 0,
                   int64_t queueOffset = 0, const std::string& popCk = std::string()) {
    MessageExt m;
    m.topic = topic;
    m.queueId = queueId;
    m.queueOffset = queueOffset;
    m.bornTimestamp = 0;
    if (!popCk.empty()) {
        m.properties[MessageConst::PROPERTY_POP_CK] = popCk;
    }
    return m;
}

void testProcessQueue() {
    PopProcessQueue pq;
    expect(pq.waitAckCount() == 0, "pq.initial");
    pq.incFoundMsg(3);
    expect(pq.waitAckCount() == 3, "pq.inc");
    pq.ack();
    pq.ack();
    expect(pq.waitAckCount() == 1, "pq.ack");
    // Java 传负数（decFoundMsg(-size)），这里按"减多少"理解
    pq.decFoundMsg(-1);
    expect(pq.waitAckCount() == 0, "pq.dec");
    expect(!pq.isDropped(), "pq.notDropped");
    pq.setDropped(true);
    expect(pq.isDropped(), "pq.dropped");
}

void testDefaults() {
    DefaultMQPushConsumer c(kGroup);
    // 关掉时必须完全走原来的 pull 路径
    expect(!c.popMode(), "defaults.popModeOff");
    // Java popInvisibleTime=60000 / popBatchNums=32 / popThresholdForQueue=96
    expect(c.popInvisibleTime() == 60000, "defaults.invisible",
           std::to_string(c.popInvisibleTime()));
    expect(c.popBatchNums() == 32, "defaults.batchNums", std::to_string(c.popBatchNums()));
    expect(c.popThresholdForQueue() == 96, "defaults.threshold",
           std::to_string(c.popThresholdForQueue()));
    // broker 侧 maxMsgNums > 32 会回 INVALID_PARAMETER
    expect(c.popBatchNums() <= 32, "defaults.batchNumsLe32");
    // 长轮询挂起时长必须 < 请求超时，否则客户端先超时、每次都空转
    expect(c.popPollTimeMillis() < c.popTimeoutMillis(), "defaults.pollLtTimeout");
    // 档位表单位秒，首档 10s（send 的延迟档位首档是 1s，别混）
    const std::vector<int32_t>& t = popDelayLevelTable();
    expect(t[0] == 10, "defaults.levelFirst10");
    expect(t.back() == 7200, "defaults.levelLast7200");
    for (size_t i = 1; i < t.size(); i++) {
        expect(t[i - 1] < t[i], "defaults.levelAscending", std::to_string(i));
    }
    expect(kMinPopInvisibleTime == 5000 && kMaxPopInvisibleTime == 300000,
           "defaults.invisibleBounds");
}

void testPopCkTarget() {
    DefaultMQPushConsumer c(kGroup);

    // 普通 topic：原样还原
    {
        MessageExt m = makeMsg(kTopic, 3, 0, ckSeg(3, 7));
        auto t = c.popCkTarget(m);
        expect(t.has_value(), "ck.normal.hasValue");
        if (t) {
            expect(t->topic == kTopic, "ck.normal.topic", t->topic);
            expect(t->brokerName == kBroker, "ck.normal.broker", t->brokerName);
            expect(t->queueId == 3, "ck.normal.queueId", std::to_string(t->queueId));
            expect(t->offset == 7, "ck.normal.offset", std::to_string(t->offset));
        }
    }
    // retryFlag=1 → 真实 topic 是 %RETRY%<group>_<topic>（V1 下划线）
    {
        MessageExt m = makeMsg(kTopic, 1, 2, ckSeg(1, 2, "1"));
        auto t = c.popCkTarget(m);
        expect(t.has_value(), "ck.retry1.hasValue");
        if (t) {
            expect(t->topic == std::string("%RETRY%") + kGroup + "_" + kTopic,
                   "ck.retry1.topic", t->topic);
        }
    }
    // retryFlag=2 → V2 用 '+' 分隔
    {
        MessageExt m = makeMsg(kTopic, 0, 0, ckSeg(0, 0, "2"));
        auto t = c.popCkTarget(m);
        expect(t.has_value(), "ck.retry2.hasValue");
        if (t) {
            expect(t->topic == std::string("%RETRY%") + kGroup + "+" + kTopic,
                   "ck.retry2.topic", t->topic);
        }
    }
    // 没有 CK → nullopt（放弃 ack，交给 broker 复活）
    expect(!c.popCkTarget(makeMsg()).has_value(), "ck.missing");
    // 段数不足（7 段）→ nullopt，不能抛
    {
        std::string seven = extra_info::buildExtraInfo(0, 1, 2, 3, kTopic, kBroker, 4);
        expect(!c.popCkTarget(makeMsg(kTopic, 4, 0, seven)).has_value(), "ck.short7");
    }
    // 垃圾串 → nullopt
    expect(!c.popCkTarget(makeMsg(kTopic, 0, 0, "only two")).has_value(), "ck.garbage");
}

void testIsPopTimeout() {
    // 解析不出 popTime/invisibleTime 时按超时处理（Java isPopTimeout）
    expect(DefaultMQPushConsumer::isPopTimeout(0, 0), "timeout.bothZero");
    expect(DefaultMQPushConsumer::isPopTimeout(0, 60000), "timeout.popTimeZero");
    expect(DefaultMQPushConsumer::isPopTimeout(1000, 0), "timeout.invisibleZero");
    // 窗口内不算超时（用真实当前时间，别硬编码时间戳）
    const int64_t now = UtilAll::currentTimeMillis();
    expect(!DefaultMQPushConsumer::isPopTimeout(now, 60000), "timeout.within");
    expect(DefaultMQPushConsumer::isPopTimeout(now - 60001, 60000), "timeout.past");
}

}  // namespace

int main() {
    testProcessQueue();
    testDefaults();
    testPopCkTarget();
    testIsPopTimeout();
    std::printf("pop consumer: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
