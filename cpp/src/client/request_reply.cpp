#include "rocketmq/client/request_reply.h"

#include <chrono>
#include <random>

#include "rocketmq/client/exception.h"
#include "rocketmq/common/compression.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/headers.h"

namespace rocketmq {

const int32_t DEFAULT_REQUEST_TIMEOUT_MILLIS = 3000;

namespace {

int64_t nowMillis() {
    return UtilAll::currentTimeMillis();
}

std::mt19937_64 makeRng() {
    std::random_device rd;
    // 无硬件熵源时 random_device 可能退化为确定性序列，故再掺入高精度时钟。
    std::seed_seq seq{rd(), rd(),
                      static_cast<unsigned>(std::chrono::high_resolution_clock::now()
                                                .time_since_epoch()
                                                .count())};
    return std::mt19937_64(seq);
}

// UUID v4（RFC 4122）：8-4-4-4-12 小写十六进制，与 Java UUID.randomUUID().toString() 同形。
std::string randomUuid() {
    static thread_local std::mt19937_64 rng = makeRng();
    uint64_t hi = rng();
    uint64_t lo = rng();
    // 版本位 = 4，变体位 = 10xx（Java 的 UUID.randomUUID 也是这两处固定）
    hi = (hi & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;
    lo = (lo & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;

    static const char* hex = "0123456789abcdef";
    std::string s;
    s.reserve(36);
    auto push = [&](uint64_t v, int nibbles, bool dash) {
        for (int i = nibbles - 1; i >= 0; --i) {
            s.push_back(hex[(v >> (i * 4)) & 0xF]);
        }
        if (dash) {
            s.push_back('-');
        }
    };
    push(hi >> 32, 8, true);            // time_low
    push((hi >> 16) & 0xFFFF, 4, true); // time_mid
    push(hi & 0xFFFF, 4, true);         // time_hi_and_version
    push(lo >> 48, 4, true);            // clock_seq
    push(lo & 0xFFFFFFFFFFFFULL, 12, false);  // node
    return s;
}

}  // namespace

// ---------------------------------------------------------------- RequestResponseFuture

RequestResponseFuture::RequestResponseFuture(std::string correlationId, int32_t timeoutMillis,
                                             std::shared_ptr<RequestCallback> callback)
    : correlationId_(std::move(correlationId)),
      timeoutMillis_(timeoutMillis),
      beginTimestamp_(nowMillis()),
      callback_(std::move(callback)) {}

bool RequestResponseFuture::waitResponseMessage(int32_t timeoutMillis) {
    std::unique_lock<std::mutex> lk(m_);
    cv_.wait_for(lk, std::chrono::milliseconds(timeoutMillis < 0 ? 0 : timeoutMillis),
                 [this] { return hasResponse_; });
    return hasResponse_;
}

void RequestResponseFuture::putResponseMessage(const MessageExt& responseMessage) {
    {
        std::lock_guard<std::mutex> lk(m_);
        responseMessage_ = responseMessage;
        hasResponse_ = true;
    }
    cv_.notify_all();
}

void RequestResponseFuture::putResponseMessage() {
    {
        std::lock_guard<std::mutex> lk(m_);
        hasResponse_ = true;
    }
    cv_.notify_all();
}

bool RequestResponseFuture::hasResponse() const {
    std::lock_guard<std::mutex> lk(m_);
    return hasResponse_;
}

MessageExt RequestResponseFuture::responseMessage() const {
    std::lock_guard<std::mutex> lk(m_);
    return responseMessage_;
}

bool RequestResponseFuture::isTimeout() const {
    return nowMillis() - beginTimestamp_ > timeoutMillis_;
}

void RequestResponseFuture::executeRequestCallback() {
    if (callback_ == nullptr) {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(callbackMutex_);
        if (callbackFired_) {
            return;
        }
        callbackFired_ = true;
    }
    if (sendRequestOk_ && cause_ == nullptr) {
        callback_->onSuccess(responseMessage());
    } else {
        callback_->onException(cause_);
    }
}

// ---------------------------------------------------------------- RequestFutureHolder

RequestFutureHolder& RequestFutureHolder::getInstance() {
    static RequestFutureHolder instance;
    return instance;
}

void RequestFutureHolder::putRequest(const std::string& correlationId,
                                     std::shared_ptr<RequestResponseFuture> future) {
    std::lock_guard<std::mutex> lk(m_);
    table_[correlationId] = std::move(future);
}

std::shared_ptr<RequestResponseFuture> RequestFutureHolder::getRequest(
    const std::string& correlationId) {
    std::lock_guard<std::mutex> lk(m_);
    auto it = table_.find(correlationId);
    return it == table_.end() ? nullptr : it->second;
}

std::shared_ptr<RequestResponseFuture> RequestFutureHolder::removeRequest(
    const std::string& correlationId) {
    std::lock_guard<std::mutex> lk(m_);
    auto it = table_.find(correlationId);
    if (it == table_.end()) {
        return nullptr;
    }
    auto future = it->second;
    table_.erase(it);
    return future;
}

std::shared_ptr<RequestResponseFuture> RequestFutureHolder::putResponse(
    const std::string& correlationId, const MessageExt& responseMessage) {
    auto future = removeRequest(correlationId);
    if (future == nullptr) {
        return nullptr;
    }
    future->putResponseMessage(responseMessage);
    // 对齐 Java：成功路径也走 executeRequestCallback，让「只回调一次」的守卫生效；
    // 同步调用方（callback 为空）靠 putResponseMessage 唤醒。
    future->executeRequestCallback();
    return future;
}

size_t RequestFutureHolder::size() {
    std::lock_guard<std::mutex> lk(m_);
    return table_.size();
}

// ---------------------------------------------------------------- 便利函数

std::string createCorrelationId() {
    return randomUuid();
}

Message createReplyMessage(const Message& requestMessage, const Bytes& body) {
    // 对应 Java ``MessageUtil.createReplyMessage``。Java 签名是 ``throws MQClientException``
    // 且带 10007：应答方通常写在业务 listener 里，按 responseCode 分流才能把"这条请求
    // 回不了话"和别的本地故障分开。C++ 这边 requestMessage 是引用，"为 null" 那条分支
    // 在类型上就不存在（调用方拿不到引用只能传值），所以只有 CLUSTER 缺失这一个入口。
    const std::string cluster = requestMessage.getProperty(MessageConst::PROPERTY_CLUSTER);
    if (cluster.empty()) {
        throw MQClientException(
            std::string("create reply message fail, requestMessage error, property[") +
            MessageConst::PROPERTY_CLUSTER + "] is null.",
            ClientErrorCode::CREATE_REPLY_MESSAGE_EXCEPTION);
    }
    Message reply;
    reply.topic = MixAll::getReplyTopic(cluster);
    reply.setBody(body);
    reply.putProperty(MessageConst::PROPERTY_MESSAGE_TYPE, MixAll::REPLY_MESSAGE_FLAG);
    reply.putProperty(MessageConst::PROPERTY_CORRELATION_ID,
                      requestMessage.getProperty(MessageConst::PROPERTY_CORRELATION_ID));
    reply.putProperty(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT,
                      requestMessage.getProperty(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT));
    reply.putProperty(MessageConst::PROPERTY_MESSAGE_TTL,
                      requestMessage.getProperty(MessageConst::PROPERTY_MESSAGE_TTL));
    return reply;
}

bool isReplyMessage(const Message& msg) {
    return msg.getProperty(MessageConst::PROPERTY_MESSAGE_TYPE) == MixAll::REPLY_MESSAGE_FLAG;
}

// ---------------------------------------------------------------- 326 处理器

std::optional<RemotingCommand> processReplyMessage(const RemotingCommand& cmd,
                                                   const std::string& addr) {
    try {
        ReplyMessageRequestHeader header;
        header.fromExtFields(cmd.extFields);
        Bytes body = cmd.body;
        const int32_t sysFlag = header.sysFlag.value_or(0);
        // sysFlag 带压缩标志时先解压：326 推的是裸包，不走消息解码路径
        // （对齐 Java receiveReplyMessage 同处的 Compressor 分支）。
        if (MessageSysFlag::isCompressed(sysFlag) && !body.empty()) {
            body = CompressorFactory::decompress(body, MessageSysFlag::getCompressionType(sysFlag));
        }
        MessageExt msg;
        msg.topic = header.topic.value_or("");
        msg.setBody(body);
        msg.queueId = header.queueId.value_or(0);
        msg.storeTimestamp = header.storeTimestamp.value_or(0);
        msg.flag = header.flag.value_or(0);
        msg.bornTimestamp = header.bornTimestamp.value_or(0);
        msg.reconsumeTimes = header.reconsumeTimes.value_or(0);
        msg.sysFlag = sysFlag;
        msg.bornHost = header.bornHost.value_or("");
        msg.storeHost = header.storeHost.value_or("");
        msg.properties = stringToMessageProperties(header.properties.value_or(""));
        // 客户端收到应答的时刻（对应 Java RECEIVE_REPLY_MESSAGE 的同名属性）
        msg.putProperty(MessageConst::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME,
                        std::to_string(UtilAll::currentTimeMillis()));

        const std::string correlationId =
            msg.getProperty(MessageConst::PROPERTY_CORRELATION_ID);
        if (RequestFutureHolder::getInstance().putResponse(correlationId, msg) == nullptr) {
            // 查不到是正常情况（请求已超时 / 应答重复），Java 此处也是 warn
            logger_warn("receive reply message, but not matched any request, CorrelationId: "
                        + correlationId + ", reply from host: " + addr);
        }
        return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, std::string());
    } catch (const std::exception& e) {
        logger_warn(std::string("unknown err when receiveReplyMsg: ") + e.what());
        return RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR,
                                                      "process reply message fail");
    }
}

}  // namespace rocketmq
