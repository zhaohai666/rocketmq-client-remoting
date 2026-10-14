#ifndef ROCKETMQ_CLIENT_EXCEPTION_H
#define ROCKETMQ_CLIENT_EXCEPTION_H

#include <cstdint>
#include <exception>
#include <stdexcept>
#include <string>

#include "rocketmq/remoting/exception.h"

namespace rocketmq {

struct ClientErrorCode {
    static constexpr int32_t CONNECT_BROKER_EXCEPTION = 10001;
    static constexpr int32_t ACCESS_BROKER_TIMEOUT = 10002;
    static constexpr int32_t BROKER_NOT_EXIST_EXCEPTION = 10003;
    static constexpr int32_t NO_NAME_SERVER_EXCEPTION = 10004;
    static constexpr int32_t NOT_FOUND_TOPIC_EXCEPTION = 10005;
    static constexpr int32_t REQUEST_TIMEOUT_EXCEPTION = 10006;
    static constexpr int32_t CREATE_REPLY_MESSAGE_EXCEPTION = 10007;
};

struct MQClientException : public std::runtime_error {
    // Constructor arguments (message, response_code); the default is 1 (UNKNOWN).
    // The admin side relies on it to pass the broker response code through — for instance,
    // resetOffsetNew must be able to tell CONSUMER_NOT_ONLINE apart in order to fall back to resetOffsetByTimestampOld.
    int32_t responseCode = 1;

    explicit MQClientException(const std::string& msg, int32_t code = 1)
        : std::runtime_error(msg), responseCode(code) {}

    int32_t getResponseCode() const { return responseCode; }
};

struct MQBrokerException : public std::runtime_error {
    static constexpr int32_t UNKNOWN = 1;

    int32_t responseCode;
    std::string responseMessage;

    MQBrokerException(int32_t code, const std::string& msg)
        : std::runtime_error("CODE: " + std::to_string(code) + " DESC: " + msg),
          responseCode(code), responseMessage(msg) {}

    int32_t getResponseCode() const { return responseCode; }
    const std::string& getResponseMessage() const { return responseMessage; }
};

struct RequestTimeoutException : public MQClientException {
    explicit RequestTimeoutException(const std::string& msg)
        : MQClientException(msg, ClientErrorCode::REQUEST_TIMEOUT_EXCEPTION) {}
};

struct MQClientNoRouteException : public MQClientException {
    explicit MQClientNoRouteException(const std::string& topic)
        : MQClientException("No route info of this topic: " + topic) {}
};

inline std::string exceptionMessage(const std::exception_ptr& e) {
    if (e == nullptr) {
        return "";
    }
    try {
        std::rethrow_exception(e);
    } catch (const std::exception& ex) {
        return ex.what();
    } catch (...) {
        return "unknown exception";
    }
}

inline std::string exceptionTypeName(const std::exception_ptr& e) {
    if (e == nullptr) {
        return "";
    }
    try {
        std::rethrow_exception(e);
    } catch (const MQBrokerException&) {
        return "MQBrokerException";
    } catch (const RequestTimeoutException&) {
        return "RequestTimeoutException";
    } catch (const MQClientNoRouteException&) {
        return "MQClientNoRouteException";
    } catch (const MQClientException&) {
        return "MQClientException";
    } catch (const RemotingTooMuchRequestException&) {
        return "RemotingTooMuchRequestException";
    } catch (const RemotingConnectException&) {
        return "RemotingConnectException";
    } catch (const RemotingSendRequestException&) {
        return "RemotingSendRequestException";
    } catch (const RemotingTimeoutException&) {
        return "RemotingTimeoutException";
    } catch (const RemotingCommandException&) {
        return "RemotingCommandException";
    } catch (const RemotingException&) {
        return "RemotingException";
    } catch (const std::exception&) {
        return "std::exception";
    } catch (...) {
        return "unknown";
    }
}

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_EXCEPTION_H
