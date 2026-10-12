// 协议回顾（与 python/client/request_reply.py 同形）：
//
//     请求方 (producer.request)                    应答方 (push consumer)
//     ─────────────────────────                    ──────────────────────
//     msg.properties[CORRELATION_ID] = uuid
//     msg.properties[REPLY_TO_CLIENT] = clientId ─► 收到请求消息（broker 已写入 CLUSTER）
//     msg.properties[TTL] = timeoutMillis           createReplyMessage(requestMsg, body):
//                                                      topic       = <CLUSTER>_REPLY_TOPIC
//                                                      CORRELATION_ID / REPLY_TO_CLIENT / TTL 原样带回
//                                                      MSG_TYPE    = "reply"
//     ◄── PUSH_REPLY_MESSAGE_TO_CLIENT(326) ──────  producer.send(reply) → SEND_REPLY_MESSAGE_V2(325)
//         由 broker 按 REPLY_TO_CLIENT 找到请求方连接推回
//
// 两个关键点（错了真机就不通）：
// 1. 应答消息必须带 MSG_TYPE == "reply"，客户端据此把请求码从 SEND_MESSAGE_V2(310) 换成
//    SEND_REPLY_MESSAGE_V2(325)；broker 的 ReplyMessageProcessor 只注册在 324/325 上。
// 2. REPLY_TO_CLIENT 是**请求方的 clientId**，broker 用它在 ProducerManager 的
//    clientChannelTable 里反查 channel —— 所以请求方必须先发过心跳（已注册为 producer）。
#ifndef ROCKETMQ_CLIENT_REQUEST_REPLY_H
#define ROCKETMQ_CLIENT_REQUEST_REPLY_H

#include <condition_variable>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

namespace rocketmq {

extern const int32_t DEFAULT_REQUEST_TIMEOUT_MILLIS;

class RequestCallback {
public:
    virtual ~RequestCallback() = default;
    virtual void onSuccess(const MessageExt& responseMessage) = 0;
    virtual void onException(const std::exception_ptr& e) = 0;
};

class RequestResponseFuture {
public:
    RequestResponseFuture(std::string correlationId, int32_t timeoutMillis,
                          std::shared_ptr<RequestCallback> callback = nullptr);

    const std::string& correlationId() const { return correlationId_; }
    int32_t timeoutMillis() const { return timeoutMillis_; }

    bool waitResponseMessage(int32_t timeoutMillis);

    void putResponseMessage(const MessageExt& responseMessage);
    // 无应答的唤醒（发送阶段就失败时用它，避免等待方白等到超时）。
    void putResponseMessage();

    bool hasResponse() const;
    MessageExt responseMessage() const;  // 未收到时返回空 MessageExt

    void setSendRequestOk(bool ok) { sendRequestOk_ = ok; }
    bool isSendRequestOk() const { return sendRequestOk_; }
    void setCause(const std::exception_ptr& e) { cause_ = e; }
    const std::exception_ptr& cause() const { return cause_; }

    bool isTimeout() const;

    void executeRequestCallback();

private:
    std::string correlationId_;
    int32_t timeoutMillis_;
    int64_t beginTimestamp_;
    std::shared_ptr<RequestCallback> callback_;

    mutable std::mutex m_;
    std::condition_variable cv_;
    bool hasResponse_ = false;
    MessageExt responseMessage_;

    bool sendRequestOk_ = true;
    std::exception_ptr cause_;

    std::mutex callbackMutex_;
    bool callbackFired_ = false;
};

class RequestFutureHolder {
public:
    static RequestFutureHolder& getInstance();

    void putRequest(const std::string& correlationId, std::shared_ptr<RequestResponseFuture> future);
    std::shared_ptr<RequestResponseFuture> getRequest(const std::string& correlationId);
    std::shared_ptr<RequestResponseFuture> removeRequest(const std::string& correlationId);

    std::shared_ptr<RequestResponseFuture> putResponse(const std::string& correlationId,
                                                       const MessageExt& responseMessage);

    size_t size();

    void startScheduledTask();
    void shutdownScheduledTask();

private:
    void scanExpiredRequest();

    RequestFutureHolder() = default;
    std::mutex m_;
    std::unordered_map<std::string, std::shared_ptr<RequestResponseFuture>> table_;

    int producerCount_ = 0;
    bool sweepRunning_ = false;
    bool sweepStopped_ = false;
    std::thread sweepThread_;
    std::mutex sweepMutex_;
    std::condition_variable sweepCv_;
};

std::string createCorrelationId();

Message createReplyMessage(const Message& requestMessage, const Bytes& body);

// MSG_TYPE == "reply" 的发送要走 SEND_REPLY_MESSAGE_V2(325)。
bool isReplyMessage(const Message& msg);

std::optional<RemotingCommand> processReplyMessage(const RemotingCommand& cmd,
                                                   const std::string& addr);

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_REQUEST_REPLY_H
