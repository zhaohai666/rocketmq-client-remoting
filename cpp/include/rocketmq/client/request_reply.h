// Protocol recap:
//
//     Requester (producer.request)                   Responder (push consumer)
//     ─────────────────────────                      ──────────────────────
//     msg.properties[CORRELATION_ID] = uuid
//     msg.properties[REPLY_TO_CLIENT] = clientId ─► request message received (broker already wrote to CLUSTER)
//     msg.properties[TTL] = timeoutMillis           createReplyMessage(requestMsg, body):
//                                                      topic       = <CLUSTER>_REPLY_TOPIC
//                                                      CORRELATION_ID / REPLY_TO_CLIENT / TTL carried back unchanged
//                                                      MSG_TYPE    = "reply"
//     ◄── PUSH_REPLY_MESSAGE_TO_CLIENT(326) ──────  producer.send(reply) → SEND_REPLY_MESSAGE_V2(325)
//         the broker finds the requester's connection by REPLY_TO_CLIENT and pushes the reply back
//
// Two critical points (get them wrong and it will not work against a real broker):
// 1. The reply message must carry MSG_TYPE == "reply"; the client uses it to switch the request code from SEND_MESSAGE_V2(310) to
//    SEND_REPLY_MESSAGE_V2(325); the broker's ReplyMessageProcessor is only registered on 324/325.
// 2. REPLY_TO_CLIENT is **the requester's clientId**; the broker uses it to look up the channel in ProducerManager's
//    clientChannelTable — so the requester must have sent a heartbeat first (registered as a producer).
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
    // Wake-up without a response (used when the send stage already failed, so the waiter does not wait until timeout for nothing).
    void putResponseMessage();

    bool hasResponse() const;
    MessageExt responseMessage() const;  // returns an empty MessageExt when none was received

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

// Sending a MSG_TYPE == "reply" message must go through SEND_REPLY_MESSAGE_V2(325).
bool isReplyMessage(const Message& msg);

std::optional<RemotingCommand> processReplyMessage(const RemotingCommand& cmd,
                                                   const std::string& addr);

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_REQUEST_REPLY_H
