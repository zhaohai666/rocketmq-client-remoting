// 客户端层异常（对应 org.apache.rocketmq.client.exception.* 与 Python client/exception.py）。
#ifndef ROCKETMQ_CLIENT_EXCEPTION_H
#define ROCKETMQ_CLIENT_EXCEPTION_H

#include <cstdint>
#include <stdexcept>
#include <string>

namespace rocketmq {

struct MQClientException : public std::runtime_error {
    // 对应 Python MQClientException(message, response_code)；默认 1（UNKNOWN）。
    // 管理端靠它把 broker 响应码透传出来——例如 resetOffsetNew 需要区分
    // CONSUMER_NOT_ONLINE 才能退化到 resetOffsetByTimestampOld。
    int32_t responseCode = 1;

    explicit MQClientException(const std::string& msg, int32_t code = 1)
        : std::runtime_error(msg), responseCode(code) {}

    int32_t getResponseCode() const { return responseCode; }
};

// 对应 Java MQBrokerException：带 broker 返回的 responseCode
struct MQBrokerException : public std::runtime_error {
    // 对应 Java ResponseCode.SYSTEM_ERROR / Python 的 UNKNOWN
    static constexpr int32_t UNKNOWN = 1;

    int32_t responseCode;
    std::string responseMessage;

    MQBrokerException(int32_t code, const std::string& msg)
        : std::runtime_error("CODE: " + std::to_string(code) + " DESC: " + msg),
          responseCode(code), responseMessage(msg) {}

    int32_t getResponseCode() const { return responseCode; }
    const std::string& getResponseMessage() const { return responseMessage; }
};

// 对应 Java org.apache.rocketmq.client.exception.RequestTimeoutException
// （extends MQClientException）：request() 里「请求消息已发出但等应答超时」时抛。
struct RequestTimeoutException : public MQClientException {
    explicit RequestTimeoutException(const std::string& msg) : MQClientException(msg) {}
};

// 对应 Java MQClientException 的 "no route info" 等语义化子类（此处仅作语义标记）
struct MQClientNoRouteException : public MQClientException {
    explicit MQClientNoRouteException(const std::string& topic)
        : MQClientException("No route info of this topic: " + topic) {}
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_EXCEPTION_H
