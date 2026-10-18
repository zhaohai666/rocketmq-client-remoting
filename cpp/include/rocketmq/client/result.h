// Client result types and callback/listener interfaces.
#ifndef ROCKETMQ_CLIENT_RESULT_H
#define ROCKETMQ_CLIENT_RESULT_H

#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_decoder.h"

namespace rocketmq {

// ---------------------------------------------------------------- send result
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
    // Whether the broker has tracing enabled (parsed from the TRACE_ON header of the SEND response, default true).
    // The trace hook uses it to decide whether to emit trace data (nothing emitted when traceOn=false or regionId is empty).
    bool traceOn = true;
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

// ---------------------------------------------------------------- transaction
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
    // Execute the local transaction, returning commit/rollback/unknown.
    virtual LocalTransactionState executeLocalTransaction(const Message& msg,
                                                          const std::string& arg) = 0;
    // Check back the local transaction state when the broker asks.
    virtual LocalTransactionState checkLocalTransaction(const MessageExt& msg) = 0;
};

// ---------------------------------------------------------------- async send callback
class SendCallback {
public:
    virtual ~SendCallback() = default;
    virtual void onSuccess(const SendResult& sendResult) = 0;
    virtual void onException(const std::exception_ptr& error) = 0;
};

// ---------------------------------------------------------------- pull result
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
    std::optional<int64_t> suggestWhichBrokerId;

    bool isFound() const { return status == PullStatus::FOUND; }
    bool isNoNewMsg() const { return status == PullStatus::NO_NEW_MSG; }
};

// ---------------------------------------------------------------- async pull callback
// Java's org.apache.rocketmq.client.consumer.PullCallback (DefaultMQPullConsumer.pull(..., PullCallback, timeout));
// shaped exactly like SendCallback above: one of the two fires, exactly once.
class PullCallback {
public:
    virtual ~PullCallback() = default;
    virtual void onSuccess(const PullResult& pullResult) = 0;
    virtual void onException(const std::exception_ptr& error) = 0;
};

// ---------------------------------------------------------------- POP mode

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

// Result of changeInvisibleTime. extraInfo is the 8-segment CK string rebuilt from the
// **new** popTime/invisibleTime/reviveQid in the response; later ACKs must use it, not the old one passed in the request.
struct ChangeInvisibleTimeResult {
    int32_t responseCode = 0;
    int64_t popTime = 0;
    int64_t invisibleTime = 0;
    int32_t reviveQid = 0;
    std::string extraInfo;

    bool success() const { return responseCode == 0; }
};

// ---------------------------------------------------------------- consume status
enum class ConsumeConcurrentlyStatus {
    CONSUME_SUCCESS = 0,
    RECONSUME_LATER = 1,
};

enum class ConsumeOrderlyStatus {
    SUCCESS = 0,
    ROLLBACK = 1,
    COMMIT = 2,
    SUSPEND_CURRENT_QUEUE_A_MOMENT = 3,
};

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
    int32_t ackIndex = std::numeric_limits<int32_t>::max();
    explicit ConsumeConcurrentlyContext(const MessageQueue& mq = MessageQueue()) : messageQueue(mq) {}
};

struct ConsumeOrderlyContext {
    MessageQueue messageQueue;
    bool autoCommit = true;
    int32_t suspendCurrentQueueTimeMillis = -1;
    explicit ConsumeOrderlyContext(const MessageQueue& mq = MessageQueue()) : messageQueue(mq) {}
};

class MessageListener {
public:
    virtual ~MessageListener() = default;
    // true means orderly consumption, false means concurrent consumption
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

// ---------------------------------------------------------------- queue selector
class MessageQueueSelector {
public:
    virtual ~MessageQueueSelector() = default;
    virtual MessageQueue select(const std::vector<MessageQueue>& mqs, const Message& msg,
                                const std::string& arg) const = 0;
};

class SelectMessageQueueByHash : public MessageQueueSelector {
public:
    MessageQueue select(const std::vector<MessageQueue>& mqs, const Message& msg,
                        const std::string& arg) const override;
};

class SelectMessageQueueByRandom : public MessageQueueSelector {
public:
    MessageQueue select(const std::vector<MessageQueue>& mqs, const Message& msg,
                        const std::string& arg) const override;
};

class SelectMessageQueueByMachineRoom : public MessageQueueSelector {
public:
    MessageQueue select(const std::vector<MessageQueue>& mqs, const Message& msg,
                        const std::string& arg) const override;

    const std::set<std::string>& getConsumeridcs() const { return consumeridcs_; }
    void setConsumeridcs(std::set<std::string> consumeridcs) {
        consumeridcs_ = std::move(consumeridcs);
    }

private:
    std::set<std::string> consumeridcs_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_RESULT_H
