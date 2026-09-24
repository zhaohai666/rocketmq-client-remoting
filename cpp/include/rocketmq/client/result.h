// 客户端结果类型与回调/监听器接口。
//
// 对应：
//   org.apache.rocketmq.client.producer.{SendResult, SendStatus, TransactionSendResult,
//                                       MessageQueueSelector, LocalTransactionState,
//                                       TransactionListener, SendCallback}
//   org.apache.rocketmq.client.consumer.listener.{ConsumeConcurrentlyStatus, ...}
//   org.apache.rocketmq.client.consumer.PullResult / PullStatus
//   Python: client/send_result.py, client/consumer_result.py, client/producer.py
#ifndef ROCKETMQ_CLIENT_RESULT_H
#define ROCKETMQ_CLIENT_RESULT_H

#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_decoder.h"

namespace rocketmq {

// ---------------------------------------------------------------- 发送结果
enum class SendStatus {
    SEND_OK = 0,
    FLUSH_DISK_TIMEOUT = 1,
    FLUSH_SLAVE_TIMEOUT = 2,
    SLAVE_NOT_AVAILABLE = 3,
};

inline const char* sendStatusName(SendStatus s) {
    switch (s) {
        case SendStatus::SEND_OK: return "SEND_OK";
        case SendStatus::FLUSH_DISK_TIMEOUT: return "FLUSH_DISK_TIMEOUT";
        case SendStatus::FLUSH_SLAVE_TIMEOUT: return "FLUSH_SLAVE_TIMEOUT";
        case SendStatus::SLAVE_NOT_AVAILABLE: return "SLAVE_NOT_AVAILABLE";
    }
    return "UNKNOWN";
}

struct SendResult {
    SendStatus sendStatus = SendStatus::SEND_OK;
    std::string msgId;
    std::string offsetMsgId;
    MessageQueue messageQueue;
    int64_t queueOffset = 0;
    std::string transactionId;
    std::string regionId;
    // broker 是否开启轨迹（由 SEND 响应头 TRACE_ON 解析，默认 true）。
    // 轨迹钩子据此决定是否落轨迹（traceOn=false 或 regionId 为空则不落）。
    bool traceOn = true;
    // 对应 Java SendResult.recallHandle：只有定时/延迟消息才有（broker 在 SEND 响应头
    // 里挂回 recallHandle），普通消息恒为 nullopt。原样传给 recallMessage() 即可撤回。
    std::optional<std::string> recallHandle;

    SendStatus getSendStatus() const { return sendStatus; }
    const std::string& getMsgId() const { return msgId; }
    const MessageQueue& getMessageQueue() const { return messageQueue; }
    int64_t getQueueOffset() const { return queueOffset; }
    const std::string& getTransactionId() const { return transactionId; }
    void setTransactionId(const std::string& id) { transactionId = id; }
    const std::optional<std::string>& getRecallHandle() const { return recallHandle; }

    std::string toString() const {
        return std::string("SendResult [sendStatus=") + sendStatusName(sendStatus)
             + ", msgId=" + msgId + ", offsetMsgId=" + offsetMsgId
             + ", messageQueue=" + messageQueue.toString()
             + ", queueOffset=" + std::to_string(queueOffset)
             + ", transactionId=" + transactionId + "]";
    }
};

// ---------------------------------------------------------------- 事务
enum class LocalTransactionState {
    COMMIT_MESSAGE = 0,
    ROLLBACK_MESSAGE = 1,
    UNKNOW = 2,
};

inline const char* localTransactionStateName(LocalTransactionState s) {
    switch (s) {
        case LocalTransactionState::COMMIT_MESSAGE: return "COMMIT_MESSAGE";
        case LocalTransactionState::ROLLBACK_MESSAGE: return "ROLLBACK_MESSAGE";
        case LocalTransactionState::UNKNOW: return "UNKNOW";
    }
    return "UNKNOWN";
}

struct TransactionSendResult : public SendResult {
    LocalTransactionState localTransactionState = LocalTransactionState::UNKNOW;
    LocalTransactionState getLocalTransactionState() const { return localTransactionState; }
};

class TransactionListener {
public:
    virtual ~TransactionListener() = default;
    // 执行本地事务，返回提交/回滚/未知
    virtual LocalTransactionState executeLocalTransaction(const Message& msg,
                                                          const std::string& arg) = 0;
    // broker 回查本地事务状态
    virtual LocalTransactionState checkLocalTransaction(const MessageExt& msg) = 0;
};

// ---------------------------------------------------------------- 异步回调
class SendCallback {
public:
    virtual ~SendCallback() = default;
    virtual void onSuccess(const SendResult& sendResult) = 0;
    // 对应 Java ``onException(Throwable e)``：交出去的是**异常对象**，类型即失败原因
    // —— broker 明确回错是 MQBrokerException（带 responseCode，调用方按码分流）、
    // 背压闸/预算耗尽拒绝是 RemotingTooMuchRequestException、其余传输失败是
    // MQClientException（Java operationFail 三包装的文案）。只给字符串等于把"哪一类失败"
    // 丢掉，调用方只能对着文案做正则。
    //
    // 与 Java 的差异（结构性）：Java 在包装 MQClientException 时会把原始 Throwable 挂成
    // cause，本端口跨不了这道边界 —— remoting 层按设计不抛异常（InvokeError 是纯数据），
    // 所以回调拿到的是**没有 cause 的** MQClientException。文案与类型仍逐字对位。
    virtual void onException(const std::exception_ptr& error) = 0;
};

// ---------------------------------------------------------------- 拉取结果
enum class PullStatus {
    FOUND = 0,
    NO_NEW_MSG = 1,
    NO_MATCHED_MSG = 2,
    OFFSET_ILLEGAL = 3,
};

inline const char* pullStatusName(PullStatus s) {
    switch (s) {
        case PullStatus::FOUND: return "FOUND";
        case PullStatus::NO_NEW_MSG: return "NO_NEW_MSG";
        case PullStatus::NO_MATCHED_MSG: return "NO_MATCHED_MSG";
        case PullStatus::OFFSET_ILLEGAL: return "OFFSET_ILLEGAL";
    }
    return "UNKNOWN";
}

struct PullResult {
    PullStatus status = PullStatus::NO_NEW_MSG;
    int64_t nextBeginOffset = 0;
    int64_t minOffset = 0;
    int64_t maxOffset = 0;
    std::vector<MessageExt> msgFoundList;

    bool isFound() const { return status == PullStatus::FOUND; }
    bool isNoNewMsg() const { return status == PullStatus::NO_NEW_MSG; }
};

// ---------------------------------------------------------------- POP 模式

// 对应 Java org.apache.rocketmq.client.consumer.PopStatus
enum class PopStatus {
    FOUND = 0,
    NO_NEW_MSG = 1,
    POLLING_FULL = 2,
    POLLING_NOT_FOUND = 3,
};

inline const char* popStatusName(PopStatus s) {
    switch (s) {
        case PopStatus::FOUND: return "FOUND";
        case PopStatus::NO_NEW_MSG: return "NO_NEW_MSG";
        case PopStatus::POLLING_FULL: return "POLLING_FULL";
        case PopStatus::POLLING_NOT_FOUND: return "POLLING_NOT_FOUND";
    }
    return "UNKNOWN";
}

// POP 响应（对应 Java PopResult）。
// startOffsetInfo / msgOffsetInfo / orderCountInfo 保留 broker 原样字符串，
// 解析交给 remoting::protocol::extra_info；msgFoundList 里每条消息都已盖好
// POP_CK（客户端反构）与 1ST_POP_TIME 属性。
struct PopResult {
    PopStatus status = PopStatus::NO_NEW_MSG;
    std::vector<MessageExt> msgFoundList;
    int64_t restNum = 0;
    int64_t popTime = 0;
    int64_t invisibleTime = 0;
    int32_t reviveQid = 0;
    std::string startOffsetInfo;
    std::string msgOffsetInfo;
    std::string orderCountInfo;

    bool isFound() const { return status == PopStatus::FOUND; }
};

// changeInvisibleTime 的结果。extraInfo 是用响应里**新的** popTime/invisibleTime/
// reviveQid 重建的 8 段 CK 串，后续 ACK 要用它（不是请求时传进去的那个旧串）。
struct ChangeInvisibleTimeResult {
    int32_t responseCode = 0;
    int64_t popTime = 0;
    int64_t invisibleTime = 0;
    int32_t reviveQid = 0;
    std::string extraInfo;

    bool success() const { return responseCode == 0; }
};

// ---------------------------------------------------------------- 消费状态
enum class ConsumeConcurrentlyStatus {
    CONSUME_SUCCESS = 0,
    RECONSUME_LATER = 1,
};

enum class ConsumeOrderlyStatus {
    // 声明顺序与 Java ``ConsumeOrderlyStatus`` 逐字对齐（Java 里 ROLLBACK/COMMIT 带
    // @Deprecated，注释写明 "only for binlog consumption"）：两侧数字一致才不会在
    // 「按序号写死」的调用方手里错位。
    SUCCESS = 0,
    ROLLBACK = 1,
    COMMIT = 2,
    SUSPEND_CURRENT_QUEUE_A_MOMENT = 3,
};

/// Java 枚举名原文（钩子 status / 轨迹 contextCode 用它，不要用 C++ 的枚举拼写）。
inline const char* consumeOrderlyStatusName(ConsumeOrderlyStatus s) {
    switch (s) {
        case ConsumeOrderlyStatus::SUCCESS:
            return "SUCCESS";
        case ConsumeOrderlyStatus::ROLLBACK:
            return "ROLLBACK";
        case ConsumeOrderlyStatus::COMMIT:
            return "COMMIT";
        case ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT:
            return "SUSPEND_CURRENT_QUEUE_A_MOMENT";
    }
    return "UNKNOWN";
}

struct ConsumeConcurrentlyContext {
    MessageQueue messageQueue;
    int32_t delayLevelWhenNextConsume = 0;
    // 对应 Java ConsumeConcurrentlyContext.ackIndex（默认 Integer.MAX_VALUE）：
    // 「listener 认可到第几条」，下标含自身，其后的消息按状态回投/丢弃。
    // 默认值是「全批认可」，只有 listener 主动调小才会部分 ack。
    int32_t ackIndex = std::numeric_limits<int32_t>::max();
    explicit ConsumeConcurrentlyContext(const MessageQueue& mq = MessageQueue()) : messageQueue(mq) {}
};

struct ConsumeOrderlyContext {
    MessageQueue messageQueue;
    // true 时由客户端按 listener 的状态提交位点（正常路径）；listener 置 false 拿走提交权
    //（Java 的 binlog 用法）：只有 COMMIT 才提交、ROLLBACK 回滚重投。
    bool autoCommit = true;
    // 对应 Java ConsumeOrderlyContext.suspendCurrentQueueTimeMillis，**默认 -1**（Java 就是
    // 这个默认值）：-1 表示「没指定」，挂起时长回落到消费者配置的
    // ``suspendCurrentQueueTimeMillis``（默认 1000）；解析出来的值再由调度侧钳到
    // [10, 30000]（Java ConsumeMessageOrderlyService#submitConsumeRequestLater:216-225）。
    int32_t suspendCurrentQueueTimeMillis = -1;
    explicit ConsumeOrderlyContext(const MessageQueue& mq = MessageQueue()) : messageQueue(mq) {}
};

class MessageListener {
public:
    virtual ~MessageListener() = default;
    // true 表示顺序消费（对应 Python 的 MessageListenerOrderly）
    virtual bool orderly() const = 0;
};

class MessageListenerConcurrently : public MessageListener {
public:
    bool orderly() const override { return false; }
    virtual ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                     ConsumeConcurrentlyContext& context) = 0;
};

class MessageListenerOrderly : public MessageListener {
public:
    bool orderly() const override { return true; }
    virtual ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                ConsumeOrderlyContext& context) = 0;
};

// ---------------------------------------------------------------- 队列选择器
class MessageQueueSelector {
public:
    virtual ~MessageQueueSelector() = default;
    virtual MessageQueue select(const std::vector<MessageQueue>& mqs, const Message& msg,
                                const std::string& arg) const = 0;
};

// 对应 Java SelectMessageQueueByHash：arg 的 Java String.hashCode() 取模
class SelectMessageQueueByHash : public MessageQueueSelector {
public:
    MessageQueue select(const std::vector<MessageQueue>& mqs, const Message& msg,
                        const std::string& arg) const override;
};

// 对应 Java SelectMessageQueueByRandom
class SelectMessageQueueByRandom : public MessageQueueSelector {
public:
    MessageQueue select(const std::vector<MessageQueue>& mqs, const Message& msg,
                        const std::string& arg) const override;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_RESULT_H
