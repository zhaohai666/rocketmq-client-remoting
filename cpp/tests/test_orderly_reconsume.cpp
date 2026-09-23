// 顺序消费的重投闸门（checkReconsumeTimes / 回投）单测 —— 不需要集群。
//
// 对齐 Java ConsumeMessageOrderlyService（:236-362）：
//   - processConsumeResult:254-266 —— SUSPEND 先过 checkReconsumeTimes，返回 true 才
//     makeMessageToConsumeAgain + 延后重试，返回 false 走 commit()（位点前进）
//   - getMaxReconsumeTimes:313-320 —— 顺序侧 -1 读成 Integer.MAX_VALUE，
//     **不是**并发侧（DefaultMQPushConsumerImpl#getMaxReconsumeTimes:890）的 16
//   - checkReconsumeTimes:322-339 —— 没用尽就本地 +1 并挂起；用尽则回投，
//     **只有回投失败**才继续挂起
//   - sendMessageBack:341-362 —— 走内部生产者当**普通消息**发到 %RETRY%<group>
//
// 外加 DefaultMQProducerImpl.sendKernelImpl:1004-1018 的那次「抬进请求头」：broker 的
// handleRetryAndDLQ（SendMessageProcessor:197-210）判死信读的是 requestHeader 的
// reconsumeTimes / maxReconsumeTimes，不是报文属性。
//
// 为什么三处都要离线锁死：坏掉全是**静默**的。少 +1 永远到不了阈值；抬错字段 broker
// 就退回订阅组默认的 16；回投成功后仍挂起，则一条毒消息永久占住那条队列 —— 真机上
// 看起来跟「消费者挂了」一模一样。
//
// ⚠ 离线只能锁「回投失败」那一支：未 start 的消费者拿不到内部生产者，
// orderlySendMessageBack 必返 false。「回投成功 → 位点前进、队列不再被堵住」由真机
// examples/live_redelivery.cpp 的 S12 证明。与 Python 的 tests/test_orderly_reconsume.py
// 逐条对应。
#include <cstdint>
#include <cstdio>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

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

const char* kGroup = "GID_OrderlyReconsumeCpp";
const char* kTopic = "OrderlyReconsumeCppTopic";
const char* kBroker = "broker-a";
const int32_t kIntMax = std::numeric_limits<int32_t>::max();

std::string retryTopic() { return std::string("%RETRY%") + kGroup; }

MessageQueue queue0() { return MessageQueue(kTopic, kBroker, 0); }

MessageExt ext(int64_t queueOffset, int32_t reconsumeTimes = 0) {
    MessageExt m;
    m.topic = kTopic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.reconsumeTimes = reconsumeTimes;
    m.msgId = "offset-msg-id-" + std::to_string(queueOffset);
    m.body = "body-" + std::to_string(queueOffset);
    return m;
}

class OrderlyListener : public MessageListenerOrderly {
public:
    explicit OrderlyListener(ConsumeOrderlyStatus status) : status_(status) {}

    ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>&,
                                        ConsumeOrderlyContext&) override {
        return status_;
    }

private:
    ConsumeOrderlyStatus status_;
};

// 不碰网络的顺序消费者：位点与待发缓冲手动搭好。
class Harness {
public:
    explicit Harness(int32_t maxReconsumeTimes) : consumer_(kGroup), key_(
        DefaultMQPushConsumer::offsetKey(queue0())) {
        consumer_.setMaxReconsumeTimes(maxReconsumeTimes);
        consumer_.setSuspendCurrentQueueTimeMillis(0);  // 单测不干等 1s
        consumer_.setPendingMessages(key_, {});
    }

    bool run(const std::vector<MessageExt>& batch,
             ConsumeOrderlyStatus status = ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT) {
        consumer_.setMessageListener(std::make_shared<OrderlyListener>(status));
        return consumer_.consumeBatch(key_, queue0(), batch);
    }

    std::vector<MessageExt> pending() const { return consumer_.pendingMessages(key_); }
    std::optional<int64_t> offset() const { return consumer_.consumeOffset(key_); }
    DefaultMQPushConsumer& c() { return consumer_; }

    std::string pendingShape() const {
        std::string s = "[";
        for (size_t i = 0; i < pending().size(); i++) {
            if (i) s += ",";
            s += std::to_string(pending()[i].queueOffset) + ":" +
                 std::to_string(pending()[i].reconsumeTimes);
        }
        return s + "]";
    }

private:
    DefaultMQPushConsumer consumer_;
    std::string key_;
};

// ------------------------------------------------ -1 的两套含义（顺序 vs 并发）

void testMinusOneMeansUnlimitedForOrderly() {
    DefaultMQPushConsumer c(kGroup);
    c.setMaxReconsumeTimes(-1);
    expect(c.orderlyMaxReconsumeTimes() == kIntMax, "orderly.minusOne.maxValue",
           std::to_string(c.orderlyMaxReconsumeTimes()));
    // 并发回投兜底仍是 broker 默认的 16：两者并成一个常量就等于给顺序消费凭空造死信，
    // 或让并发消息无限重投。
    expect(c.maxReconsumeTimesOrDefault() == 16, "concurrent.minusOne.still16",
           std::to_string(c.maxReconsumeTimesOrDefault()));
    c.setMaxReconsumeTimes(3);
    expect(c.orderlyMaxReconsumeTimes() == 3, "orderly.explicitUsedAsIs",
           std::to_string(c.orderlyMaxReconsumeTimes()));
}

void testDefaultCapNeverJudgesExhausted() {
    // 默认配置（-1）下毒消息不该被判定「用尽」：本地计数永远追不上 MAX_VALUE。
    Harness h(-1);
    std::vector<MessageExt> batch;
    batch.push_back(ext(0, kIntMax - 1));
    expect(h.c().checkOrderlyReconsumeTimes(batch), "defaultCap.suspend");
    expect(batch[0].reconsumeTimes == kIntMax, "defaultCap.countedUp",
           std::to_string(batch[0].reconsumeTimes));
}

// ------------------------------------------------ checkReconsumeTimes 三条分支

void testBelowCapCountsLocallyAndSuspends() {
    Harness h(3);
    std::vector<MessageExt> batch;
    batch.push_back(ext(0, 0));
    batch.push_back(ext(1, 2));
    expect(h.c().checkOrderlyReconsumeTimes(batch), "belowCap.suspend");
    // broker 侧没记这次失败，客户端不就地 +1 就永远到不了阈值
    expect(batch[0].reconsumeTimes == 1 && batch[1].reconsumeTimes == 3, "belowCap.countedUp",
           std::to_string(batch[0].reconsumeTimes) + "/" +
               std::to_string(batch[1].reconsumeTimes));
}

void testAtCapWithFailedSendBackStillSuspends() {
    // 未 start → 回投必败：Java :328-331 这时 suspend=true 并且再 +1，下一轮再来。
    Harness h(2);
    std::vector<MessageExt> batch;
    batch.push_back(ext(7, 2));
    expect(h.c().checkOrderlyReconsumeTimes(batch), "atCap.failedSendBack.suspend");
    expect(batch[0].reconsumeTimes == 3, "atCap.failedSendBack.countedUp",
           std::to_string(batch[0].reconsumeTimes));
}

void testEmptyBatchDoesNotSuspend() {
    Harness h(0);
    std::vector<MessageExt> empty;
    expect(!h.c().checkOrderlyReconsumeTimes(empty), "empty.noSuspend");
}

// ------------------------------------------------ 回投那条消息长什么样

void testRetryMessageFields() {
    Harness h(2);
    MessageExt poison = ext(7, 2);
    poison.putProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED, "true");
    poison.putProperty(MessageConst::PROPERTY_KEYS, "k7");
    const Message m = h.c().buildRetryMessage(poison, h.c().orderlyMaxReconsumeTimes());
    expect(m.topic == retryTopic(), "retryMessage.topic", m.topic);
    expect(m.body == poison.body, "retryMessage.body", m.body);
    expect(m.getProperty(MessageConst::PROPERTY_KEYS) == "k7", "retryMessage.copyProps");
    expect(m.getProperty(MessageConst::PROPERTY_RETRY_TOPIC) == kTopic, "retryMessage.retryTopic",
           m.getProperty(MessageConst::PROPERTY_RETRY_TOPIC));
    expect(m.getProperty(MessageConst::PROPERTY_RECONSUME_TIME) == "3", "retryMessage.reconsumeTime",
           m.getProperty(MessageConst::PROPERTY_RECONSUME_TIME));
    expect(m.getProperty(MessageConst::PROPERTY_MAX_RECONSUME_TIMES) == "2",
           "retryMessage.maxReconsumeTimes",
           m.getProperty(MessageConst::PROPERTY_MAX_RECONSUME_TIMES));
    expect(m.getProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL) == "5", "retryMessage.delayLevel",
           m.getProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL));  // 3 + 2
    expect(m.getProperty(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID) == poison.msgId,
           "retryMessage.originMsgId", m.getProperty(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID));
    // 半消息标记必须清掉，否则 broker 会把它再当回查消息处理
    expect(m.getProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED).empty(),
           "retryMessage.transactionCleared",
           m.getProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED));
    expect(!m.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX).empty(),
           "retryMessage.hasUniqKey");
}

// ------------------------------------------------ 消费循环里的接线

void testSuspendRequeuesBatchWhileBelowCap() {
    Harness h(3);
    std::vector<MessageExt> batch;
    batch.push_back(ext(0, 0));
    batch.push_back(ext(1, 1));
    // 队尾还有下一条（offset 2），本批要按原顺序插到它前面
    MessageExt tail = ext(2, 0);
    h.c().setPendingMessages(DefaultMQPushConsumer::offsetKey(queue0()), {tail});
    expect(!h.run(batch), "wire.requeue.notAdvanced");
    expect(!h.offset().has_value(), "wire.requeue.offsetFrozen",
           h.offset() ? std::to_string(*h.offset()) : "none");
    expect(h.pendingShape() == "[0:1,1:2,2:0]", "wire.requeue.order", h.pendingShape());
}

void testSuccessPathSkipsTheReconsumeGate() {
    // 阈值 0：一旦走到判据就会回投，所以这里断言「一次都没回投」= 判据没被调用。
    Harness h(0);
    std::vector<MessageExt> batch;
    batch.push_back(ext(0, 0));
    expect(h.run(batch, ConsumeOrderlyStatus::SUCCESS), "wire.success.advanced");
    expect(h.offset().has_value() && *h.offset() == 1, "wire.success.offset",
           h.offset() ? std::to_string(*h.offset()) : "none");
    expect(h.pending().empty(), "wire.success.noRequeue", h.pendingShape());
}

// ------------------------------------------------ 抬进请求头（sendKernelImpl:1004-1018）

// 只用建头那一段：建请求不碰网络，抬字段的判据全在这一步的产物里。
std::shared_ptr<SendMessageRequestHeaderV2> buildHeader(MQClientInstance& inst,
                                                       const std::string& topic,
                                                       const PropertyMap& props,
                                                       PropertyMap* wire) {
    Message m;
    m.topic = topic;
    m.body = "b";
    m.properties = props;
    RemotingCommand request =
        inst.buildSendRequest("PG", m, MessageQueue(topic, kBroker, 0), /*sysFlag=*/0,
                             /*unitMode=*/false);
    // createRequestCommand 只挂 customHeader，extFields 要到 encode 那一步才填；
    // 这里显式展开，断言的才是线上真正的报文。
    request.makeCustomHeaderToNet();
    if (wire != nullptr) {
        *wire = request.extFields;
    }
    return std::static_pointer_cast<SendMessageRequestHeaderV2>(request.customHeader);
}

void testRetryTopicPropertiesAreLiftedIntoTheHeader() {
    MQClientInstance inst("lift-unit-test", {});
    PropertyMap props;
    props[MessageConst::PROPERTY_RECONSUME_TIME] = "4";
    props[MessageConst::PROPERTY_MAX_RECONSUME_TIMES] = "6";
    PropertyMap wire;
    const auto header = buildHeader(inst, retryTopic(), props, &wire);
    // broker 判死信看的是请求头；少了这一步它退回订阅组默认的 retryMaxTimes(16)
    expect(header->reconsumeTimes && *header->reconsumeTimes == 4, "lift.reconsumeTimes",
           header->reconsumeTimes ? std::to_string(*header->reconsumeTimes) : "none");
    expect(header->maxReconsumeTimes && *header->maxReconsumeTimes == 6, "lift.maxReconsumeTimes",
           header->maxReconsumeTimes ? std::to_string(*header->maxReconsumeTimes) : "none");
    // V2 的短字段名：j=reconsumeTimes、l=maxReconsumeTimes、i=properties
    expect(wire["j"] == "4", "lift.wireJ", wire["j"]);
    expect(wire["l"] == "6", "lift.wireL", wire["l"]);
    // ⚠ 线上属性里 RECONSUME_TIME **仍在**：Java 先序列化 properties 再抬字段。
    // 改成「先抬后清」会让消费端读不到 broker 写的重试次数。
    expect(wire["i"].find("RECONSUME_TIME") != std::string::npos, "lift.wireKeepsProperty",
           wire["i"]);
}

void testOrdinaryTopicSendIsNotLifted() {
    MQClientInstance inst("lift-unit-test-2", {});
    PropertyMap props;
    props[MessageConst::PROPERTY_RECONSUME_TIME] = "4";
    props[MessageConst::PROPERTY_MAX_RECONSUME_TIMES] = "6";
    PropertyMap wire;
    const auto header = buildHeader(inst, kTopic, props, &wire);
    expect(header->reconsumeTimes && *header->reconsumeTimes == 0, "noLift.reconsumeTimes",
           header->reconsumeTimes ? std::to_string(*header->reconsumeTimes) : "none");
    expect(wire["j"] == "0", "noLift.wireJ", wire["j"]);
    // 非 %RETRY% 发送不下发 maxReconsumeTimes：broker ≥V3_4_9 无条件采信它，
    // 固定发 0 会让首投就判定 reconsumeTimes(0) >= 0 直接进 %DLQ%。
    expect(wire.find("l") == wire.end(), "noLift.wireLAbsent", wire["l"]);
}

}  // namespace

int main() {
    testMinusOneMeansUnlimitedForOrderly();
    testDefaultCapNeverJudgesExhausted();
    testBelowCapCountsLocallyAndSuspends();
    testAtCapWithFailedSendBackStillSuspends();
    testEmptyBatchDoesNotSuspend();
    testRetryMessageFields();
    testSuspendRequeuesBatchWhileBelowCap();
    testSuccessPathSkipsTheReconsumeGate();
    testRetryTopicPropertiesAreLiftedIntoTheHeader();
    testOrdinaryTopicSendIsNotLifted();
    std::printf("orderly reconsume: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
