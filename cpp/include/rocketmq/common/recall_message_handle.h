// The handle is not produced by the client: when sending a timed message with TIMER_DELIVER_MS /
// TIMER_DELAY_MS / TIMER_DELAY_SEC, the broker attaches this handle to the recallHandle field of the
// SEND response header in SendMessageProcessor#attachRecallHandle; the client only carries it back
// verbatim when calling recallMessage. Normal messages have no such field in the response.
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
