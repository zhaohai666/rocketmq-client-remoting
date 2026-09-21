// Request-Reply（5.x）客户端侧支撑，对应 Java：
//   org.apache.rocketmq.client.producer.RequestResponseFuture
//   org.apache.rocketmq.client.producer.RequestFutureHolder
//   org.apache.rocketmq.client.utils.MessageUtil#createReplyMessage
//   org.apache.rocketmq.client.impl.ClientRemotingProcessor#receiveReplyMessage
//
// 协议回顾（与 python/rocketmq/client/request_reply.py 同形）：
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
#include <unordered_map>

#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

namespace rocketmq {

//: 默认请求超时（对应 Java DefaultMQProducer 的 sendMsgTimeout 兜底 3000ms）
extern const int32_t DEFAULT_REQUEST_TIMEOUT_MILLIS;

// 对应 Java org.apache.rocketmq.client.producer.RequestCallback。
// 回调在**读线程**里执行（应答由 remoting 读线程分发），实现需自行保证线程安全。
class RequestCallback {
public:
    virtual ~RequestCallback() = default;
    virtual void onSuccess(const MessageExt& responseMessage) = 0;
    // Java 用 Throwable；C++ 跨线程只能传 exception_ptr（可能为空，表示无具体异常）。
    virtual void onException(const std::exception_ptr& e) = 0;
};

// 对应 Java RequestResponseFuture：一次 request 的等待槽。
//
// 与 Java 的差异（有意）：Java 额外起了一个 scanExpiredRequest 定时线程清理超时项；
// 本实现由 request() 的收尾逻辑保证移除，故不需要后台扫描线程。
class RequestResponseFuture {
public:
    RequestResponseFuture(std::string correlationId, int32_t timeoutMillis,
                          std::shared_ptr<RequestCallback> callback = nullptr);

    const std::string& correlationId() const { return correlationId_; }
    int32_t timeoutMillis() const { return timeoutMillis_; }

    // 等待应答（对应 Java waitResponseMessage）。超时返回 false，此时尚未写入应答。
    bool waitResponseMessage(int32_t timeoutMillis);

    // 写入应答并唤醒等待者（对应 Java putResponseMessage，允许多次调用）。
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

    // 对应 Java executeRequestCallback：回调只允许触发一次。
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

// 对应 Java RequestFutureHolder：correlationId -> 等待槽 的**进程内单例**表。
//
// Java 里是跨 producer 共享的单例（RequestFutureHolder.getInstance()），因为应答由
// clientId 级别的 remoting 通道推回，与具体 producer 实例无关。
class RequestFutureHolder {
public:
    static RequestFutureHolder& getInstance();

    void putRequest(const std::string& correlationId, std::shared_ptr<RequestResponseFuture> future);
    std::shared_ptr<RequestResponseFuture> getRequest(const std::string& correlationId);
    std::shared_ptr<RequestResponseFuture> removeRequest(const std::string& correlationId);

    // 接收侧入口（对齐 Java ClientRemotingProcessor#processReplyMessage）：
    // **先原子移除再填充** —— 让「应答到达」与「超时清理」两条路径只有一个能生效。
    // 返回被填充的 future；查不到（已超时/已移除）返回 nullptr，调用方据此只记日志。
    std::shared_ptr<RequestResponseFuture> putResponse(const std::string& correlationId,
                                                       const MessageExt& responseMessage);

    size_t size();

private:
    RequestFutureHolder() = default;
    std::mutex m_;
    std::unordered_map<std::string, std::shared_ptr<RequestResponseFuture>> table_;
};

// 对应 Java CorrelationIdUtil.createCorrelationId（随机 UUID 字符串，36 字符小写）。
std::string createCorrelationId();

// 对应 Java MessageUtil.createReplyMessage：由请求消息派生出应答消息。
//
// CLUSTER 属性由 **broker** 在投递时写入（SendMessageProcessor），拿不到就说明这条消息
// 不是经 broker 转发过来的（或 topic 配得不对），与 Java 一样直接抛错，
// 而不是造一条投不出去的应答。
Message createReplyMessage(const Message& requestMessage, const Bytes& body);

// MSG_TYPE == "reply" 的发送要走 SEND_REPLY_MESSAGE_V2(325)。
bool isReplyMessage(const Message& msg);

// PUSH_REPLY_MESSAGE_TO_CLIENT(326) 处理器（对齐 Java
// ClientRemotingProcessor#receiveReplyMessage）：按 CORRELATION_ID 把应答投进等待槽，
// 并**必须回一个响应**（broker 侧 Broker2Client.callClient 是 invokeSync，不回它就超时）。
// 正常回 SUCCESS；解析失败回 SYSTEM_ERROR（绝不让读线程死掉）；
// 查不到等待槽（迟到/重复应答）只记 warn 仍回 SUCCESS。addr 仅用于日志。
// 供 MQClientInstance 在构造时注册、单测直接调用。
std::optional<RemotingCommand> processReplyMessage(const RemotingCommand& cmd,
                                                   const std::string& addr);

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_REQUEST_REPLY_H
