// 句柄不是客户端自己造的：发送带 TIMER_DELIVER_MS / TIMER_DELAY_MS / TIMER_DELAY_SEC
// 的定时消息时，broker 在 SendMessageProcessor#attachRecallHandle 里把这个句柄挂到
// SEND 响应头的 recallHandle 字段上返回，客户端只负责原样带回去调用 recallMessage。
// 普通消息的响应里没有这个字段。
#ifndef ROCKETMQ_COMMON_RECALL_MESSAGE_HANDLE_H
#define ROCKETMQ_COMMON_RECALL_MESSAGE_HANDLE_H

#include <string>

namespace rocketmq {

struct HandleV1 {
    std::string topic;
    std::string brokerName;
    std::string timestampStr;
    std::string messageId;

    bool operator==(const HandleV1& other) const {
        return topic == other.topic && brokerName == other.brokerName &&
               timestampStr == other.timestampStr && messageId == other.messageId;
    }
};

std::string buildRecallHandle(const std::string& topic, const std::string& brokerName,
                              const std::string& timestampStr, const std::string& messageId);

HandleV1 decodeRecallHandle(const std::string& handle);

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_RECALL_MESSAGE_HANDLE_H
