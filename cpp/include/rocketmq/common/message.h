#ifndef ROCKETMQ_COMMON_MESSAGE_H
#define ROCKETMQ_COMMON_MESSAGE_H

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/types.h"

namespace rocketmq {

// ---------------------------------------------------------------- MessageQueue
struct MessageQueue {
    std::string topic;
    std::string brokerName;
    int32_t queueId = 0;

    MessageQueue() = default;
    MessageQueue(const std::string& topic, const std::string& brokerName, int32_t queueId)
        : topic(topic), brokerName(brokerName), queueId(queueId) {}

    const std::string& getTopic() const { return topic; }
    void setTopic(const std::string& t) { topic = t; }

    const std::string& getBrokerName() const { return brokerName; }
    void setBrokerName(const std::string& b) { brokerName = b; }

    int32_t getQueueId() const { return queueId; }
    void setQueueId(int32_t q) { queueId = q; }

    std::string getQueueIdStr() const { return std::to_string(queueId); }

    int32_t hashCode() const;

    bool operator==(const MessageQueue& other) const {
        return topic == other.topic && brokerName == other.brokerName && queueId == other.queueId;
    }

    bool operator!=(const MessageQueue& other) const { return !(*this == other); }

    bool operator<(const MessageQueue& other) const { return compareTo(other) < 0; }

    int compareTo(const MessageQueue& other) const {
        if (topic != other.topic) return topic < other.topic ? -1 : 1;
        if (brokerName != other.brokerName) return brokerName < other.brokerName ? -1 : 1;
        if (queueId != other.queueId) return queueId < other.queueId ? -1 : 1;
        return 0;
    }

    std::string toString() const;
};

// ---------------------------------------------------------------- Message
struct Message {
    std::string topic;
    int32_t flag = 0;
    PropertyMap properties;
    Bytes body;
    bool hasBody = true;
    std::string transactionId;
    // Whether this is a batch message body (set by MessageBatch::generateFromList).
    // On sending it maps to SendMessageRequestHeader.batch — the broker's
    // SendMessageProcessor uses requestHeader.isBatch() to choose between sendBatchMessage
    // and sendMessage, so this flag is required.
    // Remark: dynamic_cast is not used because Message is not a polymorphic type (no virtual functions), so RTTI is unavailable.
    bool isBatch = false;

    Message() = default;
    Message(const std::string& topic, const Bytes& body) : topic(topic), body(body) {}
    Message(const std::string& topic, const std::string& tags, const std::string& keys,
            const Bytes& body)
        : topic(topic), body(body) {
        if (!tags.empty()) setTags(tags);
        if (!keys.empty()) setKeys(keys);
    }

    // ---- property shortcuts ----
    void setTags(const std::string& tags) { properties[MessageConst::PROPERTY_TAGS] = tags; }
    std::string getTags() const;
    void setKeys(const std::string& keys) { properties[MessageConst::PROPERTY_KEYS] = keys; }
    std::string getKeys() const;
    void setDelayTimeLevel(int32_t level) {
        properties[MessageConst::PROPERTY_DELAY_TIME_LEVEL] = std::to_string(level);
    }
    int32_t getDelayTimeLevel() const;
    void setDelayTimeSec(int64_t sec) {
        properties[MessageConst::PROPERTY_TIMER_DELAY_SEC] = std::to_string(sec);
    }
    int64_t getDelayTimeSec() const;
    void setDelayTimeMs(int64_t timeMs) {
        properties[MessageConst::PROPERTY_TIMER_DELAY_MS] = std::to_string(timeMs);
    }
    int64_t getDelayTimeMs() const;
    void setDeliverTimeMs(int64_t timeMs) {
        properties[MessageConst::PROPERTY_TIMER_DELIVER_MS] = std::to_string(timeMs);
    }
    int64_t getDeliverTimeMs() const;
    void setWaitStoreMsgOk(bool ok) {
        properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK] = ok ? "true" : "false";
    }
    bool isWaitStoreMsgOk() const;
    std::string getWaitStoreMsgOkStr() const;

    void setUserProperty(const std::string& name, const std::string& value) { properties[name] = value; }
    std::string getUserProperty(const std::string& name) const;
    void putProperty(const std::string& name, const std::string& value) { properties[name] = value; }
    void removeProperty(const std::string& name) { properties.erase(name); }
    std::string getProperty(const std::string& name) const;
    void clearProperty() { properties.clear(); }

    const std::string& getTopic() const { return topic; }
    void setTopic(const std::string& t) { topic = t; }

    const Bytes& getBody() const { return body; }
    void setBody(const Bytes& b) { body = b; hasBody = true; }

    int32_t getFlag() const { return flag; }
    void setFlag(int32_t f) { flag = f; }

    const PropertyMap& getProperties() const { return properties; }
    void setProperties(const PropertyMap& p) { properties = p; }

    const std::string& getTransactionId() const { return transactionId; }
    void setTransactionId(const std::string& id) { transactionId = id; }

    std::string toString() const;
};

// ---------------------------------------------------------------- MessageExt
struct MessageExt : public Message {
    int32_t queueId = 0;
    int32_t storeSize = 0;
    int64_t queueOffset = 0;
    int32_t sysFlag = 0;
    int64_t bornTimestamp = 0;
    std::string bornHost;
    int32_t bornHostPort = 0;
    int64_t storeTimestamp = 0;
    std::string storeHost;
    int32_t storeHostPort = 0;
    std::string msgId;
    int64_t commitLogOffset = 0;
    uint32_t bodyCrc = 0;
    int32_t reconsumeTimes = 0;
    int64_t preparedTransactionOffset = 0;
    std::string brokerName;
    std::string offsetMsgId;
    std::string msgType;

    MessageExt() = default;
    explicit MessageExt(const Message& m) : Message(m) {}

    int32_t getQueueId() const { return queueId; }
    void setQueueId(int32_t q) { queueId = q; }

    int32_t getStoreSize() const { return storeSize; }
    void setStoreSize(int32_t s) { storeSize = s; }

    int64_t getQueueOffset() const { return queueOffset; }
    void setQueueOffset(int64_t o) { queueOffset = o; }

    int32_t getSysFlag() const { return sysFlag; }
    void setSysFlag(int32_t f) { sysFlag = f; }

    int64_t getBornTimestamp() const { return bornTimestamp; }
    void setBornTimestamp(int64_t t) { bornTimestamp = t; }

    const std::string& getBornHost() const { return bornHost; }
    void setBornHost(const std::string& h) { bornHost = h; }

    int64_t getStoreTimestamp() const { return storeTimestamp; }
    void setStoreTimestamp(int64_t t) { storeTimestamp = t; }

    const std::string& getStoreHost() const { return storeHost; }
    void setStoreHost(const std::string& h) { storeHost = h; }

    const std::string& getMsgId() const { return msgId; }
    void setMsgId(const std::string& id) { msgId = id; }

    int64_t getCommitLogOffset() const { return commitLogOffset; }
    void setCommitLogOffset(int64_t o) { commitLogOffset = o; }

    uint32_t getBodyCrc() const { return bodyCrc; }
    void setBodyCrc(uint32_t crc) { bodyCrc = crc; }

    int32_t getReconsumeTimes() const { return reconsumeTimes; }
    void setReconsumeTimes(int32_t n) { reconsumeTimes = n; }

    int64_t getPreparedTransactionOffset() const { return preparedTransactionOffset; }
    void setPreparedTransactionOffset(int64_t o) { preparedTransactionOffset = o; }

    const std::string& getBrokerName() const { return brokerName; }
    void setBrokerName(const std::string& n) { brokerName = n; }

    const std::string& getOffsetMsgId() const { return offsetMsgId; }
    void setOffsetMsgId(const std::string& id) { offsetMsgId = id; }

    const std::string& getMsgType() const { return msgType; }
    void setMsgType(const std::string& t) { msgType = t; }

    std::string getBornHostString() const;
    std::string getStoreHostString() const;

    std::string toString() const;
};

struct MessageBatch : public Message {
    std::vector<Message> messages;

    MessageBatch() = default;
    explicit MessageBatch(const std::vector<Message>& msgs) : messages(msgs) {}

    Bytes encode() const;

    size_t size() const { return messages.size(); }

    static MessageBatch generateFromList(const std::vector<Message>& msgs);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_MESSAGE_H
