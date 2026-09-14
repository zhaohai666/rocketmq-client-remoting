// 生产者实现（对应 Python client/producer.py 的 DefaultMQProducer）。
#include "rocketmq/client/producer.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/exception.h"

namespace rocketmq {

namespace {

std::string trim(const std::string& s) {
    size_t b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return std::string();
    size_t e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

std::vector<std::string> splitSemicolon(const std::string& addr) {
    std::vector<std::string> out;
    size_t start = 0;
    while (start <= addr.size()) {
        size_t pos = addr.find(';', start);
        std::string piece = addr.substr(start, pos == std::string::npos ? std::string::npos
                                                                       : pos - start);
        std::string t = trim(piece);
        if (!t.empty()) out.push_back(t);
        if (pos == std::string::npos) break;
        start = pos + 1;
    }
    return out;
}

}  // namespace

DefaultMQProducer::DefaultMQProducer(const std::string& producerGroup) {
    if (UtilAll::isBlank(producerGroup)) {
        throw MQClientException("producerGroup is empty");
    }
    producerGroup_ = producerGroup;
}

DefaultMQProducer::~DefaultMQProducer() {
    try {
        shutdown();
    } catch (...) {
        // 析构不抛
    }
}

// ---------------------------------------------------------------- 配置
void DefaultMQProducer::setNamesrvAddr(const std::string& addr) {
    nameServerAddrs_ = splitSemicolon(addr);
}

void DefaultMQProducer::setNameServerAddresses(const std::vector<std::string>& addrs) {
    nameServerAddrs_ = addrs;
}

std::string DefaultMQProducer::getNamesrvAddr() const {
    std::string out;
    for (size_t i = 0; i < nameServerAddrs_.size(); ++i) {
        if (i) out += ";";
        out += nameServerAddrs_[i];
    }
    return out;
}

void DefaultMQProducer::setProducerGroup(const std::string& g) {
    if (started_) {
        throw MQClientException("producerGroup cannot be changed after startup");
    }
    producerGroup_ = g;
}

// ---------------------------------------------------------------- 生命周期
void DefaultMQProducer::start() {
    std::lock_guard<std::mutex> lk(lock_);
    if (started_) {
        return;
    }
    if (nameServerAddrs_.empty()) {
        throw MQClientException("name server address is not set");
    }
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_);
    }
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_));
    mqClient_->start();
    started_ = true;
    logger_info("DefaultMQProducer[" + producerGroup_ + "] started, clientId=" + clientId_);
}

void DefaultMQProducer::shutdown() {
    std::vector<std::thread> threads;
    {
        std::lock_guard<std::mutex> lk(lock_);
        if (!started_) {
            return;
        }
        started_ = false;
        threads.swap(asyncThreads_);
    }
    // 先回收异步线程（它们内部持有 mqClient_ 引用），再关客户端
    for (std::thread& t : threads) {
        if (t.joinable()) t.join();
    }
    if (mqClient_) {
        mqClient_->shutdown();
    }
}

MQClientInstance& DefaultMQProducer::client() {
    if (!started_ || mqClient_ == nullptr) {
        throw MQClientException("producer not started, call start() first");
    }
    return *mqClient_;
}

// ---------------------------------------------------------------- 校验
void DefaultMQProducer::checkMessage(const Message& msg) const {
    if (msg.topic.empty()) {
        throw MQClientException("message topic is empty");
    }
    if (static_cast<int32_t>(msg.body.size()) > maxMessageSize_) {
        throw MQClientException("message body size " + std::to_string(msg.body.size())
                                + " exceeds maxMessageSize " + std::to_string(maxMessageSize_));
    }
}

// 对应 Java DefaultMQProducerImpl.tryToCompressMessage + sendKernelImpl 的 sysFlag 组装。
//
// 语义逐条对齐 Java：
//   * 批量消息（MessageBatch）**永不压缩**；
//   * body 长度 >= compressMsgBodyOverHowmuch（默认 4096）才压缩；
//   * 压缩失败按 Java 的做法**降级为不压缩**并记日志，而不是让发送失败；
//   * 压缩后不比较体积（Java 也不比较：即使压完更大也照发）。
int32_t DefaultMQProducer::prepareForSend(Message& msg) const {
    if (msg.isBatch) {
        return 0;
    }
    if (static_cast<int32_t>(msg.body.size()) < compressMsgBodyOverHowmuch_) {
        return 0;
    }

    Bytes compressed;
    try {
        compressed = CompressorFactory::compress(msg.body, compressType_, compressLevel_);
    } catch (const std::exception& e) {
        logger_warn(std::string("tryToCompressMessage failed, send uncompressed: ") + e.what());
        return 0;
    }
    if (compressed.empty() && !msg.body.empty()) {
        return 0;
    }

    msg.body = compressed;
    msg.hasBody = true;
    int32_t sysFlag = MessageSysFlag::COMPRESSED_FLAG;
    sysFlag |= CompressionType::getCompressionFlag(compressType_);
    return sysFlag;
}

// ---------------------------------------------------------------- 同步发送
SendResult DefaultMQProducer::send(const Message& msg, int32_t timeoutMillis) {
    MQClientInstance& c = client();
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    checkMessage(msg);
    Message outbound = msg;
    const int32_t sysFlag = prepareForSend(outbound);

    std::string lastError;
    for (int32_t attempt = 0; attempt <= retryTimesWhenSendFailed_; ++attempt) {
        try {
            std::shared_ptr<TopicPublishInfo> publish = c.getTopicPublishInfo(outbound.topic);
            MessageQueue selected = publish->selectOneMessageQueue();
            return c.sendMessage(producerGroup_, outbound, selected, timeout, sysFlag);
        } catch (const MQClientException& e) {
            lastError = e.what();
        } catch (const MQBrokerException& e) {
            lastError = e.what();
        } catch (const RemotingException& e) {
            lastError = e.what();
        }
    }
    throw MQClientException("send failed after " + std::to_string(retryTimesWhenSendFailed_ + 1)
                            + " attempts, last error: " + lastError);
}

SendResult DefaultMQProducer::send(const Message& msg, const MessageQueue& mq,
                                   int32_t timeoutMillis) {
    MQClientInstance& c = client();
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    checkMessage(msg);
    Message outbound = msg;
    const int32_t sysFlag = prepareForSend(outbound);
    return c.sendMessage(producerGroup_, outbound, mq, timeout, sysFlag);
}

SendResult DefaultMQProducer::sendBySelector(const Message& msg,
                                             const MessageQueueSelector& selector,
                                             const std::string& arg, int32_t timeoutMillis) {
    MQClientInstance& c = client();
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    checkMessage(msg);
    std::shared_ptr<TopicPublishInfo> publish = c.getTopicPublishInfo(msg.topic);
    MessageQueue selected = selector.select(publish->msgQueueList, msg, arg);
    // 选择器用的是原始消息（topic/业务字段），压缩只影响 body
    Message outbound = msg;
    const int32_t sysFlag = prepareForSend(outbound);
    return c.sendMessage(producerGroup_, outbound, selected, timeout, sysFlag);
}

// ---------------------------------------------------------------- 异步 / 单向
void DefaultMQProducer::sendAsync(const Message& msg, std::shared_ptr<SendCallback> callback,
                                  int32_t timeoutMillis) {
    // 先确认已启动（与 Python 一致：未启动立即抛，而不是在后台线程里静默失败）
    (void)client();
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    std::thread th([this, msg, callback, timeout]() {
        try {
            SendResult result = send(msg, timeout);
            if (callback) callback->onSuccess(result);
        } catch (const std::exception& e) {
            if (callback) callback->onException(e.what());
        } catch (...) {
            if (callback) callback->onException("unknown error");
        }
    });
    std::lock_guard<std::mutex> lk(lock_);
    asyncThreads_.push_back(std::move(th));
}

void DefaultMQProducer::sendOneway(const Message& msg) {
    MQClientInstance& c = client();
    checkMessage(msg);
    std::shared_ptr<TopicPublishInfo> publish = c.getTopicPublishInfo(msg.topic);
    MessageQueue selected = publish->selectOneMessageQueue();
    Message outbound = msg;
    const int32_t sysFlag = prepareForSend(outbound);
    c.sendMessageOneway(producerGroup_, outbound, selected, sendMsgTimeout_, sysFlag);
}

// ---------------------------------------------------------------- 批量
SendResult DefaultMQProducer::sendBatch(const std::vector<Message>& msgs, int32_t timeoutMillis) {
    MQClientInstance& c = client();
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    if (msgs.empty()) {
        throw MQClientException("message list is empty");
    }
    MessageBatch batch = MessageBatch::generateFromList(msgs);
    checkMessage(batch);
    std::shared_ptr<TopicPublishInfo> publish = c.getTopicPublishInfo(batch.topic);
    MessageQueue selected = publish->selectOneMessageQueue();
    // MessageBatch 的 isBatch 为 true，prepareForSend 会直接返回 0（不压缩）
    const int32_t sysFlag = prepareForSend(batch);
    return c.sendMessage(producerGroup_, batch, selected, timeout, sysFlag);
}

// ---------------------------------------------------------------- 事务消息
TransactionSendResult DefaultMQProducer::sendMessageInTransaction(const Message& msg,
                                                                  TransactionListener& listener,
                                                                  const std::string& arg) {
    MQClientInstance& c = client();
    checkMessage(msg);
    std::shared_ptr<TopicPublishInfo> publish = c.getTopicPublishInfo(msg.topic);
    MessageQueue selected = publish->selectOneMessageQueue();

    // 简化单阶段：先发消息，再执行本地事务，按结果回填状态。
    // 未实现 broker 半消息 + 回查 + END_TRANSACTION 两阶段提交。
    // 压缩与普通发送一致（Java 的事务发送同样走 sendKernelImpl）。
    Message outbound = msg;
    const int32_t sysFlag = prepareForSend(outbound);
    SendResult sendResult =
        c.sendMessage(producerGroup_, outbound, selected, sendMsgTimeout_, sysFlag);
    TransactionSendResult tsr;
    static_cast<SendResult&>(tsr) = sendResult;
    tsr.localTransactionState = listener.executeLocalTransaction(msg, arg);
    return tsr;
}

// ---------------------------------------------------------------- 查询 / 管理
std::vector<MessageExt> DefaultMQProducer::queryMessage(const std::string& topic,
                                                        const std::string& key, int32_t maxNum,
                                                        int64_t beginTimestamp,
                                                        int64_t endTimestamp) {
    MQClientInstance& c = client();
    Bytes body;
    bool found = c.queryMessage(topic, key, maxNum, beginTimestamp, endTimestamp, body, 15000);
    if (!found || body.empty()) {
        return {};
    }
    return decodeMessages(body);
}

std::vector<MessageQueue> DefaultMQProducer::fetchPublishMessageQueues(const std::string& topic) {
    MQClientInstance& c = client();
    std::shared_ptr<TopicPublishInfo> publish = c.getTopicPublishInfo(topic);
    return publish->msgQueueList;
}

void DefaultMQProducer::createTopic(const std::string& key, const std::string& newTopic,
                                    int32_t queueNum) {
    MQClientInstance& c = client();
    constexpr int32_t perm = 6;  // PERM_READ | PERM_WRITE
    (void)key;
    c.createTopicInRoute(newTopic, queueNum, queueNum, perm);
}

int64_t DefaultMQProducer::searchOffset(const MessageQueue& mq, int64_t timestamp) {
    return client().searchOffsetByTimestamp(mq, timestamp);
}

int64_t DefaultMQProducer::maxOffset(const MessageQueue& mq) { return client().getMaxOffset(mq); }

int64_t DefaultMQProducer::minOffset(const MessageQueue& mq) { return client().getMinOffset(mq); }

// ---------------------------------------------------------------- TransactionMQProducer
TransactionSendResult TransactionMQProducer::sendMessageInTransaction(const Message& msg,
                                                                      const std::string& arg) {
    if (transactionListener_ == nullptr) {
        throw MQClientException("transaction listener is not set");
    }
    return DefaultMQProducer::sendMessageInTransaction(msg, *transactionListener_, arg);
}

}  // namespace rocketmq
