// 生产者实现（对应 Python client/producer.py 的 DefaultMQProducer）。
#include "rocketmq/client/producer.h"

#include <algorithm>
#include <atomic>
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

// 异步发送线程的递增序号，用于线程命名（对齐 Java 线程工厂 "AsyncSenderThread_" + n 的后缀）。
// 进程级递增，与 Java 的 ThreadFactoryImpl 计数器语义一致。
int nextAsyncSenderSeq() {
    static std::atomic<int> seq{0};
    return seq.fetch_add(1, std::memory_order_relaxed);
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
    // 注册 broker 主动请求处理器：事务回查 CHECK_TRANSACTION_STATE(39)。
    // 不注册的话 broker 回查会被传输层当成"未知请求"丢弃，事务消息永远停留在 UNKNOW。
    mqClient_->remotingClient().registerProcessor(
        RequestCode::CHECK_TRANSACTION_STATE,
        [this](const RemotingCommand& cmd, const std::string& addr) {
            this->checkTransactionState(cmd, addr);
        });
    started_ = true;
    logger_info("DefaultMQProducer[" + producerGroup_ + "] started, clientId=" + clientId_);

    // 心跳线程：周期性向 broker 注册 ProducerData。没有它 broker 无法主动回查事务。
    heartbeatRunning_.store(true);
    heartbeatThread_ = std::thread([this]() {
        setThreadName("ProducerHeartbeatThread");
        // 启动后立刻发一次：让 broker 尽快登记 channel，避免首条事务消息错过回查窗口
        while (heartbeatRunning_.load()) {
            try {
                sendHeartbeatToAllBroker();
            } catch (const std::exception& e) {
                logger_debug("producer heartbeat failed: " + std::string(e.what()));
            }
            for (int i = 0; i < heartbeatIntervalMillis_ / 100 && heartbeatRunning_.load(); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    });
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
    // 先停心跳线程（它内部持有 mqClient_ 引用），再回收其它线程
    heartbeatRunning_.store(false);
    if (heartbeatThread_.joinable()) {
        heartbeatThread_.join();
    }
    {
        std::vector<std::thread> txThreads;
        {
            std::lock_guard<std::mutex> tl(txThreadsMutex_);
            txThreads.swap(txThreads_);
        }
        for (std::thread& t : txThreads) {
            if (t.joinable()) t.join();
        }
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
            std::shared_ptr<TopicPublishInfo> publish =
                c.getTopicPublishInfo(outbound.topic, /*isDefault=*/true);
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
    std::shared_ptr<TopicPublishInfo> publish = c.getTopicPublishInfo(msg.topic, /*isDefault=*/true);
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
        // 线程名对齐 Java 的 ThreadFactoryImpl("AsyncSenderThread_")
        setThreadName("AsyncSenderThread_" + std::to_string(nextAsyncSenderSeq()));
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
    std::shared_ptr<TopicPublishInfo> publish = c.getTopicPublishInfo(msg.topic, /*isDefault=*/true);
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
    std::shared_ptr<TopicPublishInfo> publish =
        c.getTopicPublishInfo(batch.topic, /*isDefault=*/true);
    MessageQueue selected = publish->selectOneMessageQueue();
    // MessageBatch 的 isBatch 为 true，prepareForSend 会直接返回 0（不压缩）
    const int32_t sysFlag = prepareForSend(batch);
    return c.sendMessage(producerGroup_, batch, selected, timeout, sysFlag);
}

// ---------------------------------------------------------------- 心跳
int32_t DefaultMQProducer::sendHeartbeatToAllBroker() {
    if (mqClient_ == nullptr) {
        return 0;
    }
    std::vector<std::string> addrs;
    try {
        addrs = mqClient_->knownBrokerAddrs();
    } catch (const std::exception& e) {
        logger_warn("producer heartbeat: gather brokers failed: " + std::string(e.what()));
        return 0;
    }
    if (addrs.empty()) {
        return 0;
    }

    // 只带 ProducerData：对齐 Java MQClientInstance 里 producerTable 的注册内容。
    // broker 会把该 group 登记到 ProducerManager（事务回查即通过该 channel 反向联系）。
    HeartbeatData hb(clientId_);
    ProducerData pd;
    pd.groupName = producerGroup_;
    hb.heartbeatFingerprint = 0;  // 走 V1 注册路径，最稳妥
    hb.addProducerData(pd);

    int32_t okCount = 0;
    for (const std::string& addr : addrs) {
        try {
            mqClient_->sendHeartbeat(addr, hb, 5000);
            ++okCount;
            heartbeatCount_.fetch_add(1);
        } catch (const std::exception& e) {
            logger_warn("producer heartbeat to " + addr + " failed: " + e.what());
        }
    }
    return okCount;
}

// ---------------------------------------------------------------- 事务消息
//
// 对齐 Java DefaultMQProducerImpl 的两阶段实现：
//   半消息(TRAN_MSG/PGROUP + sysFlag TRANSACTION_PREPARED) -> 本地事务 ->
//   END_TRANSACTION(37, oneway)；UNKNOW 时由 broker 回查 CHECK_TRANSACTION_STATE(39)。
static int32_t transactionFlagOf(LocalTransactionState state) {
    switch (state) {
        case LocalTransactionState::COMMIT_MESSAGE:
            return MessageSysFlag::TRANSACTION_COMMIT_TYPE;    // 0x2 << 2
        case LocalTransactionState::ROLLBACK_MESSAGE:
            return MessageSysFlag::TRANSACTION_ROLLBACK_TYPE;  // 0x3 << 2
        default:
            return MessageSysFlag::TRANSACTION_NOT_TYPE;       // UNKNOW
    }
}

void DefaultMQProducer::endTransaction(const Message& msg, const SendResult& sendResult,
                                       LocalTransactionState state, bool hasLocalException,
                                       const std::string& localExceptionText, bool fromCheck,
                                       const CheckTransactionStateRequestHeader* checkHeader,
                                       const MessageExt* checkMsg, const std::string& brokerAddr) {
    MQClientInstance& c = client();

    EndTransactionRequestHeader header;
    header.producerGroup = producerGroup_;
    header.commitOrRollback = transactionFlagOf(state);
    header.fromTransactionCheck = fromCheck;

    std::string addr;
    if (fromCheck) {
        // 回查收尾：偏移 / 事务号来自 broker 的回查请求（sendResult 此时不可用）
        header.topic = checkHeader->topic.value_or("");
        header.commitLogOffset = checkHeader->commitLogOffset;
        header.tranStateTableOffset = checkHeader->tranStateTableOffset;
        header.transactionId = checkHeader->transactionId;
        header.bname = checkHeader->bname;
        // Java: uniqueKey = msg 属性 UNIQ_KEY，取不到才用 msgId
        std::string uniqueKey;
        if (checkMsg != nullptr) {
            uniqueKey = checkMsg->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
            if (uniqueKey.empty()) {
                uniqueKey = checkMsg->msgId;
            }
        }
        header.msgId = uniqueKey.empty() ? std::optional<std::string>() : uniqueKey;
        addr = brokerAddr;
    } else {
        // Java: id = decodeMessageId(offsetMsgId != null ? offsetMsgId : msgId)
        const std::string& idText = sendResult.offsetMsgId.empty() ? sendResult.msgId
                                                                   : sendResult.offsetMsgId;
        std::string idIp;
        int32_t idPort = 0;
        int64_t idOffset = 0;
        if (!decodeMessageId(idText, idIp, idPort, idOffset)) {
            throw MQClientException("unrecognized msgId: " + idText);
        }
        header.topic = msg.topic;
        header.commitLogOffset = idOffset;
        header.tranStateTableOffset = sendResult.queueOffset;
        header.transactionId = sendResult.transactionId;
        header.bname = sendResult.messageQueue.brokerName;
        header.msgId = sendResult.msgId;
        addr = c.brokerAddrForMq(sendResult.messageQueue);
    }

    RemotingCommand request = RemotingCommand::createRequestCommand(
        RequestCode::END_TRANSACTION, std::make_shared<EndTransactionRequestHeader>(header));
    if (hasLocalException) {
        request.remark = "executeLocalTransactionBranch exception: " + localExceptionText;
    }
    // Java 走 endTransactionOneway：单向发送，不等 broker 响应
    c.remotingClient().invokeOneway(addr, request);
}

void DefaultMQProducer::checkTransactionState(const RemotingCommand& cmd, const std::string& addr) {
    CheckTransactionStateRequestHeader header;
    header.fromExtFields(cmd.extFields);

    // broker 把整条 MessageExt 编码后放在 body 里（Java Broker2Client.checkProducerTransactionState）
    MessageExt msgExt;
    if (cmd.body.empty() || !decodeMessage(cmd.body, msgExt)) {
        logger_warn("checkTransactionState: decode message failed");
        return;
    }

    const std::string group =
        msgExt.getProperty(MessageConst::PROPERTY_PRODUCER_GROUP);
    if (group != producerGroup_) {
        logger_debug("checkTransactionState: group " + group + " is not mine (" + producerGroup_ +
                     ")");
        return;
    }

    TransactionListener* listener = txListener_;
    if (listener == nullptr) {
        logger_warn("checkTransactionState: no transaction listener for group " + producerGroup_);
        return;
    }

    // Java 在独立线程里执行回查回调，避免阻塞读线程
    MessageExt captured = std::move(msgExt);
    CheckTransactionStateRequestHeader capturedHeader = header;
    std::thread th([this, captured, capturedHeader, addr, listener]() {
        setThreadName("TransactionCheckThread");
        LocalTransactionState state = LocalTransactionState::UNKNOW;
        bool hasException = false;
        std::string exceptionText;
        try {
            state = listener->checkLocalTransaction(captured);
        } catch (const std::exception& e) {
            logger_error(std::string("Broker call checkTransactionState, but "
                                     "checkLocalTransactionState exception: ") +
                         e.what());
            hasException = true;
            exceptionText = e.what();
        } catch (...) {
            logger_error("Broker call checkTransactionState, but checkLocalTransactionState "
                         "threw unknown exception");
            hasException = true;
            exceptionText = "unknown exception";
        }
        try {
            static const Message emptyMsg;
            static const SendResult emptyResult;
            endTransaction(emptyMsg, emptyResult, state, hasException, exceptionText, true,
                           &capturedHeader, &captured, addr);
        } catch (const std::exception& e) {
            logger_warn("checkTransactionState: end transaction failed: " + std::string(e.what()));
        }
    });
    {
        std::lock_guard<std::mutex> tl(txThreadsMutex_);
        txThreads_.push_back(std::move(th));
    }
}

TransactionSendResult DefaultMQProducer::sendMessageInTransaction(const Message& msg,
                                                                  TransactionListener& listener,
                                                                  const std::string& arg) {
    // Java ensureNotDelayedForTransactional：事务消息不支持延迟投递
    if (msg.getProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL).size() > 0) {
        throw MQClientException("Transactional messages do not support delayed delivery");
    }

    MQClientInstance& c = client();
    checkMessage(msg);

    // 半消息标记：broker 据此把消息写入 RMQ_SYS_TRANS_HALF_TOPIC，等待 END_TRANSACTION
    Message outbound = msg;
    outbound.putProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED, "true");
    outbound.putProperty(MessageConst::PROPERTY_PRODUCER_GROUP, producerGroup_);
    txListener_ = &listener;

    std::shared_ptr<TopicPublishInfo> publish =
        c.getTopicPublishInfo(outbound.topic, /*isDefault=*/true);
    MessageQueue selected = publish->selectOneMessageQueue();

    // 压缩与普通发送一致；再叠加事务类型位（Java sendKernelImpl 检测 TRAN_MSG 后置 PREPARED）
    int32_t sysFlag = prepareForSend(outbound);
    sysFlag = MessageSysFlag::resetTransactionValue(sysFlag,
                                                    MessageSysFlag::TRANSACTION_PREPARED_TYPE);
    SendResult sendResult;
    try {
        sendResult = c.sendMessage(producerGroup_, outbound, selected, sendMsgTimeout_, sysFlag);
    } catch (const std::exception& e) {
        throw MQClientException(std::string("send message Exception: ") + e.what());
    }

    LocalTransactionState state = LocalTransactionState::UNKNOW;
    bool hasLocalException = false;
    std::string localExceptionText;
    if (sendResult.sendStatus == SendStatus::SEND_OK) {
        if (!sendResult.transactionId.empty()) {
            outbound.putProperty("__transactionId__", sendResult.transactionId);
        }
        std::string uniq = outbound.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if (!uniq.empty()) {
            outbound.transactionId = uniq;
        }
        try {
            LocalTransactionState ret = listener.executeLocalTransaction(outbound, arg);
            state = ret;  // Java：null 视为 UNKNOW → C++ 枚举已覆盖三态
        } catch (const std::exception& e) {
            logger_error("executeLocalTransactionBranch exception, topic=" + outbound.topic +
                         ": " + e.what());
            hasLocalException = true;
            localExceptionText = e.what();
        } catch (...) {
            logger_error("executeLocalTransactionBranch threw unknown exception, topic=" +
                         outbound.topic);
            hasLocalException = true;
            localExceptionText = "unknown exception";
        }
    } else if (sendResult.sendStatus == SendStatus::FLUSH_DISK_TIMEOUT ||
               sendResult.sendStatus == SendStatus::FLUSH_SLAVE_TIMEOUT ||
               sendResult.sendStatus == SendStatus::SLAVE_NOT_AVAILABLE) {
        state = LocalTransactionState::ROLLBACK_MESSAGE;
    }

    try {
        endTransaction(outbound, sendResult, state, hasLocalException, localExceptionText, false,
                       nullptr, nullptr, "");
    } catch (const std::exception& e) {
        // Java：end broker transaction 失败只 warn，不影响返回结果
        logger_warn("local transaction execute " + std::string(localTransactionStateName(state)) +
                    ", but end broker transaction failed: " + e.what());
    }

    TransactionSendResult tsr;
    static_cast<SendResult&>(tsr) = sendResult;
    tsr.localTransactionState = state;
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
    std::shared_ptr<TopicPublishInfo> publish =
        c.getTopicPublishInfo(topic, /*isDefault=*/true);
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
