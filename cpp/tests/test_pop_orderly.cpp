// POP + 顺序监听器（Java ConsumeMessagePopOrderlyService，5.5.0 未完成骨架）单测 —— 不需要集群。
//
// Java DefaultMQPushConsumerImpl:960-990 按 listener 类型选服务：顺序监听器 + POP 走
// ConsumeMessagePopOrderlyService。上游 5.5.0 那是个未完成骨架（:533 POPTODO）：请求
// 去重入队后 run() 拿到队列锁就返回 —— 消息**不消费、不 ack**，invisibleTime 到期由
// broker 复活重投，宏观表现是「顺序 + POP 收不到消息且积压不消」。此前本端口会把顺序
// 监听器 static_cast 成并发监听器照常消费（还是 UB），与 Java 行为脱节。
//
// 覆盖（对齐 ConsumeMessagePopOrderlyService.java）：
//   - submitPopConsumeRequest:161-166 —— 分派进顺序骨架，listener 不被调、不 ack
//     （pq 的 waitAckCount 不动 = 积压只增不减，交给流控把 pop 循环压停）
//   - submitConsumeRequest:178-191 —— 去重集按 (pq 引用, mq) 判等：同队列重复提交
//     只入队一次；rebalance 换了**新** PopProcessQueue（指针身份不同）后照常入队
//   - ConsumeRequest.run:315-324 —— pq 被撤销才摘请求；活队列上是 no-op
//   - 并发分支不受分流影响（回归护栏：改坏分流 = 要么顺序消息被消费、要么并发的
//     POP 消息静默消失）
//
// 离线锁不住的部分：lockLoop/shutdown/onQueuesRevoked 在 POP 模式下不发
// LOCK/UNLOCK_BATCH_MQ（Java 的 lockAll/unlockAll 只读 processQueueTable），需要真机
// 抓包证明 —— 由 examples 真机验证与四端互查覆盖。
#include <cstdio>
#include <memory>
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

const char* kGroup = "GID_PopOrderlyCpp";
const char* kTopic = "PopOrderlyCppTopic";
const char* kBroker = "broker-a";

MessageQueue queue0() { return MessageQueue(kTopic, kBroker, 0); }

MessageExt ext(int64_t queueOffset) {
    MessageExt m;
    m.topic = kTopic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.msgId = "pop-orderly-" + std::to_string(queueOffset);
    m.body = "body-" + std::to_string(queueOffset);
    return m;
}

/// 带上"刚弹出"的 POP_CK：consumePopBatch 的 isPopTimeout 判据靠它，
/// 没有它并发分支会直接按超时丢弃（listener 不被调）。
void stampFreshPopCk(MessageExt& m) {
    m.putProperty(MessageConst::PROPERTY_POP_CK,
                  extra_info::buildExtraInfo(/*ckQueueOffset=*/m.queueOffset,
                                             /*popTime=*/UtilAll::currentTimeMillis(),
                                             /*invisibleTime=*/60000, /*reviveQid=*/0,
                                             kTopic, kBroker, /*queueId=*/0,
                                             /*msgQueueOffset=*/m.queueOffset));
}

class CountingOrderlyListener : public MessageListenerOrderly {
public:
    int* calls;
    explicit CountingOrderlyListener(int* sink) : calls(sink) {}
    ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>&,
                                        ConsumeOrderlyContext&) override {
        ++*calls;
        return ConsumeOrderlyStatus::SUCCESS;
    }
};

class CountingConcurrentListener : public MessageListenerConcurrently {
public:
    int* calls;
    explicit CountingConcurrentListener(int* sink) : calls(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        ++*calls;
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

std::vector<MessageExt> batch2() { return {ext(0), ext(1)}; }

// ------------------------------------------------ 分派：顺序进骨架，不调 listener

void testOrderlyDispatchNeverInvokesListener() {
    DefaultMQPushConsumer c(kGroup);
    int calls = 0;
    c.setMessageListener(std::make_shared<CountingOrderlyListener>(&calls));
    auto pq = std::make_shared<PopProcessQueue>();
    // queuePopLoop 在分派前已 incFoundMsg：积压先涨上去
    pq->incFoundMsg(2);
    c.submitPopConsumeRequest(batch2(), pq, queue0());
    expect(calls == 0, "dispatch.orderly.listenerNotCalled", std::to_string(calls));
    // 不 ack、不延长：积压原样留在 pq 上（broker 侧等 invisibleTime 到期复活重投）
    expect(pq->waitAckCount() == 2, "dispatch.orderly.neverAcked",
           std::to_string(pq->waitAckCount()));
    // 请求留在集合里（Java：非 dropped 分支不 removeConsumeRequest）
    expect(c.popOrderlyRequestCount() == 1, "dispatch.orderly.requestKept",
           std::to_string(c.popOrderlyRequestCount()));
}

// ------------------------------------------------ 分流：并发分支照常消费（回归护栏）

void testConcurrentDispatchStillConsumes() {
    DefaultMQPushConsumer c(kGroup);
    int calls = 0;
    c.setMessageListener(std::make_shared<CountingConcurrentListener>(&calls));
    auto pq = std::make_shared<PopProcessQueue>();
    pq->incFoundMsg(1);
    std::vector<MessageExt> msgs{ext(0)};
    stampFreshPopCk(msgs[0]);
    c.submitPopConsumeRequest(msgs, pq, queue0());
    expect(calls == 1, "dispatch.concurrent.listenerCalled", std::to_string(calls));
    // CONSUME_SUCCESS 整批 ack：waitAckCount 回到 0（ackPopMsg 在未 start 时发不出去，
    // 但 pq->ack() 照走 —— 真机上 ack 由 broker 承认，这里只锁 pq 计数这一半）
    expect(pq->waitAckCount() == 0, "dispatch.concurrent.acked",
           std::to_string(pq->waitAckCount()));
}

// ------------------------------------------------ 去重集的判等（pq 引用 + mq）

void testRequestDedup() {
    DefaultMQPushConsumer c(kGroup);
    int calls = 0;
    c.setMessageListener(std::make_shared<CountingOrderlyListener>(&calls));
    auto pq = std::make_shared<PopProcessQueue>();
    // 同 (pq, mq) 重复提交：ConcurrentSet.add 返回 false，不重复入队
    c.submitPopOrderlyRequest(pq, queue0(), /*force=*/false);
    c.submitPopOrderlyRequest(pq, queue0(), /*force=*/false);
    expect(c.popOrderlyRequestCount() == 1, "dedup.samePqSameMq.once",
           std::to_string(c.popOrderlyRequestCount()));
    // mq 不同 → 新请求
    c.submitPopOrderlyRequest(pq, MessageQueue(kTopic, kBroker, 1), false);
    expect(c.popOrderlyRequestCount() == 2, "dedup.differentMq.counted",
           std::to_string(c.popOrderlyRequestCount()));
    // 同 mq 但**新** PopProcessQueue（rebalance 撤走再分回来）→ 新请求
    auto fresh = std::make_shared<PopProcessQueue>();
    c.submitPopOrderlyRequest(fresh, queue0(), false);
    expect(c.popOrderlyRequestCount() == 3, "dedup.newPq.counted",
           std::to_string(c.popOrderlyRequestCount()));
    // force=true：照常执行，但集合仍只此一份（Java：先 add 再判 force||isNew）
    c.submitPopOrderlyRequest(pq, queue0(), /*force=*/true);
    expect(c.popOrderlyRequestCount() == 3, "dedup.force.staysSingle",
           std::to_string(c.popOrderlyRequestCount()));
}

// ------------------------------------------------ 摘请求：只在 pq 被撤销时

void testDroppedRequestIsRemoved() {
    DefaultMQPushConsumer c(kGroup);
    int calls = 0;
    c.setMessageListener(std::make_shared<CountingOrderlyListener>(&calls));
    auto pq = std::make_shared<PopProcessQueue>();
    c.submitPopOrderlyRequest(pq, queue0(), false);
    expect(c.popOrderlyRequestCount() == 1, "dropped.primed",
           std::to_string(c.popOrderlyRequestCount()));
    // rebalance 撤走 → pq 标 dropped → 请求跑到 dropped 分支：摘掉自己 + 不调 listener
    pq->setDropped(true);
    c.runPopOrderlyRequest(pq, queue0());
    expect(c.popOrderlyRequestCount() == 0, "dropped.removed",
           std::to_string(c.popOrderlyRequestCount()));
    expect(calls == 0, "dropped.listenerStillNotCalled", std::to_string(calls));
    // 撤销队列的 submit 路径同样自摘（run 在 dropped 分支返回前擦除）
    c.submitPopOrderlyRequest(pq, queue0(), false);
    expect(c.popOrderlyRequestCount() == 0, "dropped.submitAlsoCleans",
           std::to_string(c.popOrderlyRequestCount()));
}

// ------------------------------------------------ 活队列上的 run 是 no-op

void testRunOnLiveQueueIsANoOp() {
    DefaultMQPushConsumer c(kGroup);
    int calls = 0;
    c.setMessageListener(std::make_shared<CountingOrderlyListener>(&calls));
    auto pq = std::make_shared<PopProcessQueue>();
    pq->incFoundMsg(3);
    c.submitPopOrderlyRequest(pq, queue0(), false);
    c.runPopOrderlyRequest(pq, queue0());
    expect(c.popOrderlyRequestCount() == 1, "noop.requestKept",
           std::to_string(c.popOrderlyRequestCount()));
    expect(pq->waitAckCount() == 3, "noop.untouched", std::to_string(pq->waitAckCount()));
    expect(calls == 0, "noop.listenerNotCalled", std::to_string(calls));
}

}  // namespace

int main() {
    testOrderlyDispatchNeverInvokesListener();
    testConcurrentDispatchStillConsumes();
    testRequestDedup();
    testDroppedRequestIsRemoved();
    testRunOnLiveQueueIsANoOp();
    std::printf("pop orderly: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
