// 异常类型（对应 org.apache.rocketmq.remoting.exception.*）。
#ifndef ROCKETMQ_REMOTING_EXCEPTION_H
#define ROCKETMQ_REMOTING_EXCEPTION_H

#include <stdexcept>
#include <string>

namespace rocketmq {

struct RemotingException : public std::runtime_error {
    explicit RemotingException(const std::string& msg) : std::runtime_error(msg) {}
};

struct RemotingConnectException : public RemotingException {
    explicit RemotingConnectException(const std::string& msg) : RemotingException(msg) {}
};

struct RemotingSendRequestException : public RemotingException {
    explicit RemotingSendRequestException(const std::string& msg) : RemotingException(msg) {}
};

struct RemotingTimeoutException : public RemotingException {
    explicit RemotingTimeoutException(const std::string& msg) : RemotingException(msg) {}
};

struct RemotingCommandException : public RemotingException {
    explicit RemotingCommandException(const std::string& msg) : RemotingException(msg) {}
};

struct RemotingTooMuchRequestException : public RemotingException {
    explicit RemotingTooMuchRequestException(const std::string& msg) : RemotingException(msg) {}
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_EXCEPTION_H
