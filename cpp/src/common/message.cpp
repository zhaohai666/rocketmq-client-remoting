// 消息模型实现（对应 org.apache.rocketmq.common.message.*）。
//
// 对齐 python/common/message.py：
//   - MessageQueue.hashCode 与 Java 逐位一致（32 位有符号回绕）；
//   - 属性快捷方式（TAGS/KEYS/DELAY/WAIT）读写 properties；
//   - MessageBatch.generateFromList 的约束：非空 / 同 topic / 同 waitStoreMsgOK / 禁延时 / 禁重试 topic。
#include "rocketmq/common/message.h"

#include <stdexcept>

#include "rocketmq/common/message_decoder.h"

namespace rocketmq {

// ---------------------------------------------------------------- MessageQueue

int32_t MessageQueue::hashCode() const {
    int64_t brokerHash = javaStringHash(brokerName);
    int64_t topicHash = javaStringHash(topic);
    int64_t v = ((31 + brokerHash) * 31 + queueId) * 31 + topicHash;
    return static_cast<int32_t>(static_cast<uint32_t>(v & 0xFFFFFFFFLL));
}

std::string MessageQueue::toString() const {
    return "MessageQueue [topic=" + topic + ", brokerName=" + brokerName +
           ", queueId=" + std::to_string(queueId) + "]";
}

// ---------------------------------------------------------------- Message

std::string Message::getTags() const {
    auto it = properties.find(MessageConst::PROPERTY_TAGS);
    return it == properties.end() ? std::string() : it->second;
}

std::string Message::getKeys() const {
    auto it = properties.find(MessageConst::PROPERTY_KEYS);
    return it == properties.end() ? std::string() : it->second;
}

int32_t Message::getDelayTimeLevel() const {
    auto it = properties.find(MessageConst::PROPERTY_DELAY_TIME_LEVEL);
    if (it == properties.end() || it->second.empty()) return 0;
    try {
        return std::stoi(it->second);
    } catch (...) {
        return 0;
    }
}

bool Message::isWaitStoreMsgOk() const {
    // Java Message.isWaitStoreMsgOK()：属性**缺省即 true**，其余走 Boolean.parseBoolean
    // —— 只有忽略大小写的 "true" 为真。
    // ⚠ 别把缺省判成 false：本类（同 Python / Rust / C#）**不**在构造时预写 WAIT，
    // 所以"属性缺省"是常态。缺省判成 false 会让 MessageBatch::generateFromList 攒出
    // WAIT=false 的批量，broker 不等刷盘就回 SEND_OK（持久性静默降级）。
    auto it = properties.find(MessageConst::PROPERTY_WAIT_STORE_MSG_OK);
    if (it == properties.end()) return true;
    const std::string& s = it->second;
    if (s.size() != 4) return false;
    return (s[0] == 't' || s[0] == 'T') && (s[1] == 'r' || s[1] == 'R') &&
           (s[2] == 'u' || s[2] == 'U') && (s[3] == 'e' || s[3] == 'E');
}

std::string Message::getWaitStoreMsgOkStr() const {
    auto it = properties.find(MessageConst::PROPERTY_WAIT_STORE_MSG_OK);
    return it == properties.end() ? std::string("true") : it->second;
}

std::string Message::getUserProperty(const std::string& name) const {
    auto it = properties.find(name);
    return it == properties.end() ? std::string() : it->second;
}

std::string Message::getProperty(const std::string& name) const {
    auto it = properties.find(name);
    return it == properties.end() ? std::string() : it->second;
}

std::string Message::toString() const {
    std::string props;
    for (const auto& kv : properties) {
        if (!props.empty()) props += ",";
        props += kv.first + "=" + kv.second;
    }
    return "Message [topic=" + topic + ", flag=" + std::to_string(flag) +
           ", properties=" + props + ", body=" +
           std::to_string(static_cast<long long>(body.size())) + " bytes]";
}

// ---------------------------------------------------------------- MessageExt

std::string MessageExt::getBornHostString() const {
    if (!bornHost.empty() && bornHostPort != 0) {
        return bornHost + ":" + std::to_string(bornHostPort);
    }
    return bornHost;
}

std::string MessageExt::getStoreHostString() const {
    if (!storeHost.empty() && storeHostPort != 0) {
        return storeHost + ":" + std::to_string(storeHostPort);
    }
    return storeHost;
}

std::string MessageExt::toString() const {
    return "MessageExt [queueId=" + std::to_string(queueId) +
           ", storeSize=" + std::to_string(storeSize) +
           ", queueOffset=" + std::to_string(static_cast<long long>(queueOffset)) +
           ", sysFlag=" + std::to_string(sysFlag) +
           ", msgId=" + msgId +
           ", topic=" + topic + "]";
}

// ---------------------------------------------------------------- MessageBatch

Bytes MessageBatch::encode() const { return encodeMessages(messages); }

MessageBatch MessageBatch::generateFromList(const std::vector<Message>& msgs) {
    if (msgs.empty()) {
        throw std::invalid_argument("messages must not be null or empty");
    }
    const Message* first = nullptr;
    for (const auto& m : msgs) {
        if (m.getDelayTimeLevel() > 0) {
            throw std::invalid_argument("Delayed messages are not supported for batching");
        }
        if (m.getTopic().rfind(MixAll::RETRY_GROUP_TOPIC_PREFIX, 0) == 0) {
            throw std::invalid_argument("Retry Group is not supported for batching");
        }
        if (first == nullptr) {
            first = &m;
        } else {
            if (first->getTopic() != m.getTopic()) {
                throw std::invalid_argument("The topic of the messages in one batch should be the same");
            }
            if (first->getWaitStoreMsgOkStr() != m.getWaitStoreMsgOkStr()) {
                throw std::invalid_argument("The waitStoreMsgOK of the messages in one batch should be the same");
            }
        }
    }

    MessageBatch batch;
    batch.messages = msgs;
    batch.setTopic(first->getTopic());
    batch.setWaitStoreMsgOk(first->isWaitStoreMsgOk());
    batch.setBody(batch.encode());
    batch.isBatch = true;  // 让发送侧把 SendMessageRequestHeader.batch 置 true
    return batch;
}

}  // namespace rocketmq
