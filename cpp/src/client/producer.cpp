// 生产者实现（DefaultMQProducer，走经典 remoting 协议）。
#include "rocketmq/client/producer.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/trace_context.h"
#include "rocketmq/client/trace_hook.h"
#include "rocketmq/client/validators.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/namespace_util.h"
#include "rocketmq/common/recall_message_handle.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/exception.h"
#include "schedule_util.h"

namespace rocketmq {

namespace {

std::string trim(const std::string& s) {
    size_t b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return std::string();
    size_t e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

// 把 broker 名列表拼成 "a, b, c"，用于发送失败信息里的 brokersSent
std::string joinStrings(const std::vector<std::string>& parts) {
    std::string out;
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) out += ", ";
        out += parts[i];
    }
    return out;
}

// 用单调钟算耗时：一次本地 broker 往返可能不到 1ms，整数毫秒差会在容错表里记成
// latency=0，那这条 broker 就永远不会被延迟阈值判到。
double elapsedMillis(const std::chrono::steady_clock::time_point& from) {
    return std::chrono::duration<double, std::milli>(
               std::chrono::steady_clock::now() - from)
        .count();
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

// 发送前确保消息带有 UNIQ_KEY（32 位十六进制唯一 ID）：缺失才生成，已存在则保留（幂等）。
// UNIQ_KEY 是经典 remoting 协议里的客户端消息 ID，broker 侧的事务回查与轨迹都按它关联。
// 轨迹钩子用它在 SendResult.msgId 里回填 UNIQ_KEY，
// 从而让发送侧轨迹的 msgId == UNIQ_KEY（非 offsetMsgId）。
void ensureUniqId(Message& msg) {
    std::string uniq = msg.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    if (uniq.empty()) {
        uniq = InnerIdGenerator::createUniqId();
        msg.putProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniq);
    }
}

// 建背压信号量时的初始许可：配置**不高于**地板值时用地板值，并记一条 info ——
// 分支是 `cfg > floor ? max(cfg, floor) : floor`，所以恰好等于地板值时也会打这条日志。
int64_t backPressurePermits(int32_t configured, int64_t floor, const char* name) {
    if (static_cast<int64_t>(configured) > floor) {
        return configured;
    }
    logger_info(std::string(name) + " can not be smaller than " + std::to_string(floor) + ".");
    return floor;
}

// 扣多少「字节」许可：取 body 长度，body 为空时按 1 计。不给空 body 算 1 就等于不限流。
int64_t backPressureMsgLen(const Message& msg) {
    const Bytes& body = msg.getBody();
    return body.empty() ? 1 : static_cast<int64_t>(body.size());
}

// 批量异步扣多少「字节」许可：逐条累加、空 body 也算 1，整批为空算 1。批量发送走
// SEND_BATCH、整批编码成一个 body，没有现成的批长度可依——不给它按批累加，就等于一批
// 整体只占 1 份容量，字节闸对批量形同虚设。
int64_t batchBackPressureMsgLen(const std::vector<Message>& msgs) {
    int64_t total = 0;
    for (const Message& m : msgs) {
        total += backPressureMsgLen(m);
    }
    return total == 0 ? 1 : total;
}

// 攒批判定要看四个延时属性的最大值（缺省 0）：`DELAY`（getDelayTimeLevel）+
// `TIMER_DELAY_MS` / `TIMER_DELAY_SEC` / `TIMER_DELIVER_MS`，这四个属性名都由经典
// remoting 协议定义。
//
// 一个 `MessageBatch` 只能有一个延时属性，所以任何一个 > 0 都不攒批。
//
// ⚠ `MessageConst` 提供了这三个常量（PROPERTY_TIMER_DELAY_SEC/MS/DELIVER_MS），
// 这里直接用常量，四个属性一份清单一起看。
//
// 取值口径：属性缺失返回 0；**值非法时不吞**，数字解析的异常直接抛给调用方。
int64_t maxDelayValue(const Message& msg) {
    int64_t result = msg.getDelayTimeLevel();
    static const char* kTimerDelayProperties[] = {
        MessageConst::PROPERTY_TIMER_DELAY_MS, MessageConst::PROPERTY_TIMER_DELAY_SEC,
        MessageConst::PROPERTY_TIMER_DELIVER_MS};
    for (const char* name : kTimerDelayProperties) {
        auto it = msg.properties.find(name);
        if (it == msg.properties.end() || it->second.empty()) {
            continue;
        }
        const int64_t value = std::stoll(it->second);
        if (value > result) {
            result = value;
        }
    }
    return result;
}

}  // namespace

DefaultMQProducer::DefaultMQProducer(const std::string& producerGroup)
    // 两个公平信号量在构造时就建好，而不是等 start()：背压开关允许运行时才打开，
    // 那时闸必须已经在了。
    : semaphoreAsyncSendNum_(backPressurePermits(backPressureForAsyncSendNum_, kMinAsyncSendNum,
                                                 "semaphoreAsyncSendNum")),
      semaphoreAsyncSendSize_(backPressurePermits(backPressureForAsyncSendSize_,
                                                  kMinAsyncSendSize, "semaphoreAsyncSendSize")) {
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
    // 生产者组也拼命名空间（`setProducerGroup(withNamespace(producerGroup))`），
    // broker 侧按带前缀的组名登记。
    // wrapNamespace 自带"已带前缀则原样返回"的幂等守卫，重复 start 不会套两层。
    if (!namespace_.empty()) {
        producerGroup_ = NamespaceUtil::wrapNamespace(namespace_, producerGroup_);
    }
    // 启动先做配置校验（checkConfig）：它排在 withNamespace 之后（组名要按带命名空间前缀的
    // 结果校验），并且要挡住
    // DEFAULT_PRODUCER —— 多进程共用默认组会互相踢下线。
    // ⚠ checkConfig 是 start() 的第一步，所以这里领先于 name server 地址检查：
    // 配置非法时不该先报"没配地址"，也不该建出客户端实例。
    Validators::checkGroup(producerGroup_);
    if (producerGroup_ == MixAll::DEFAULT_PRODUCER_GROUP) {
        throw MQClientException(
            "producerGroup can not equal DEFAULT_PRODUCER, please specify another one.");
    }
    // 静态地址与动态取址（ROCKETMQ_NAMESRV_DOMAIN）二选一必须可用
    if (nameServerAddrs_.empty() && !DefaultTopAddressing::isConfigured()) {
        // 码值用 10004（validateNameServerSetting 对同一个故障给的码）：
        // 这道检查提前到 start()（更可预期），否则故障要等第一次发送才以 10004 冒出来，
        // 但不能让调用方看到两种不同的码。
        throw MQClientException("name server address is not set",
                                ClientErrorCode::NO_NAME_SERVER_EXCEPTION);
    }
    // 启动的两步：先 `changeInstanceNameToPID`（本实现没有内部生产者，所以无条件
    // 执行），再拼 clientId：
    // `<本机 IP>@<instanceName>[@unitName][@STREAM]` —— broker 侧按这个串识别客户端连接。
    // instanceName 就地写回：第二次 start() 复用同一个 clientId。
    instanceName_ = changeInstanceNameToPID(instanceName_);
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_, unitName_, enableStreamRequestType_);
    }
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                         /*connectTimeoutMillis=*/3000,
                                         /*invokeTimeoutMillis=*/15000,
                                         tlsEnable_, unitName_,
                                         pollNameServerIntervalMillis_));
    // TLS 严格校验选项（caCert 非空 = 严格校验）：转发给实例与底层传输层
    if (tlsOptionsSet_) {
        mqClient_->setTlsOptions(tlsOptions_);
    }
    if (socksProxySet_) {
        mqClient_->setSocksProxy(socksProxy_);
    }
    if (std::shared_ptr<RPCHook> requestHook =
            composeRequestHooks(enableStreamRequestType_, rpcHook_,
                                [this] { return namespaceV2_; })) {
        if (!mqClient_->registerRPCHook(requestHook)) {
            logger_warn("producer rpc hook ignored: MQClientInstance already has one (clientId="
                        + clientId_ + ")");
        }
    }
    mqClient_->start();
    // 动态 name server：实例启动时可能已从地址服务器拿到地址，回填到本生产者
    if (nameServerAddrs_.empty() && !mqClient_->nameServerAddrs().empty()) {
        nameServerAddrs_ = mqClient_->nameServerAddrs();
    }
    // 注册 broker 主动请求处理器：事务回查 CHECK_TRANSACTION_STATE(39)。
    // 不注册的话 broker 回查会被传输层当成"未知请求"丢弃，事务消息永远停留在 UNKNOW。
    mqClient_->remotingClient().registerProcessor(
        RequestCode::CHECK_TRANSACTION_STATE,
        [this](const RemotingCommand& cmd, const std::string& addr)
            -> std::optional<RemotingCommand> {
            this->checkTransactionState(cmd, addr);
            return std::nullopt;
        });
    // 异步发送的两个线程池：必须在 started_ 置位前建好，否则 sendAsync 会看到
    // "已启动但池子为空"。
    createAsyncExecutors();
    started_ = true;
    logger_info("DefaultMQProducer[" + producerGroup_ + "] started, clientId=" + clientId_);

    initProduceAccumulator();
    if (produceAccumulator_ != nullptr) {
        produceAccumulator_->start();
    }

    // 消息轨迹：enableTrace=true 时建 AsyncTraceDispatcher 并注册 Send/EndTransaction 钩子。
    // 必须在心跳线程之前完成 —— 分发器内部生产者要先把轨迹 topic 的路由拉起来。
    startTraceDispatcher();

    RequestFutureHolder::getInstance().startScheduledTask();

    // 心跳线程：周期性向 broker 注册 ProducerData。没有它 broker 无法主动回查事务。
    heartbeatStop_.store(false);
    heartbeatThread_ = std::thread([this]() {
        setThreadName("ProducerHeartbeatThread");
        // 启动后立刻发一次：让 broker 尽快登记 channel，避免首条事务消息错过回查窗口。
        // 之后固定速率：每跳锚定在 heartbeatIntervalMillis_（见 schedule_util.h）——
        // "干完再按 100ms 切片睡满 30s"每轮会多出十几毫秒，心跳周期被越拖越长。
        auto next = std::chrono::steady_clock::now();
        while (!heartbeatStop_.load()) {
            sleepUntilDeadline(heartbeatStop_, next);
            next += std::chrono::milliseconds(heartbeatIntervalMillis_);
            try {
                sendHeartbeatToAllBroker();
            } catch (const std::exception& e) {
                logger_debug("producer heartbeat failed: " + std::string(e.what()));
            }
        }
    });

    mqFaultStrategy_.latencyFaultTolerance().setResolver(
        [this](const std::string& brokerName) -> std::string {
            return mqClient_ ? mqClient_->findBrokerAddressInPublish(brokerName) : std::string();
        });
    mqFaultStrategy_.latencyFaultTolerance().setServiceDetector(
        [this](const std::string& addr, int32_t timeoutMillis) {
            return detectBrokerByMaxOffset(addr, timeoutMillis);
        });
    detectorStop_.store(false);
    detectorThread_ = std::thread([this]() { detectorLoop(); });
}

void DefaultMQProducer::shutdown() {
    if (produceAccumulator_ != nullptr) {
        produceAccumulator_->shutdown();
        produceAccumulator_->detachSender();
    }
    std::shared_ptr<ConsumeExecutor> asyncSender;
    std::shared_ptr<ConsumeExecutor> callbackPool;
    {
        std::lock_guard<std::mutex> lk(lock_);
        if (!started_) {
            return;
        }
        // 先把两个池摘掉：之后的 sendAsync 会看到 "producer already shutdown"。
        // ⚠ started_ 要等队列排空之后再翻 —— 排队里的异步发送还要用 client()，
        //   提前置 false 会让它们以"producer not started"失败，等于静默丢消息。
        asyncSender.swap(asyncSenderExecutor_);
        callbackPool.swap(callbackExecutor_);
    }
    if (asyncSender) {
        asyncSender->shutdown(true);
    }
    if (callbackPool) {
        callbackPool->shutdown(true);
    }
    {
        std::lock_guard<std::mutex> lk(lock_);
        started_ = false;
    }
    // 先关轨迹分发器：它会把队列里剩余的轨迹强刷出去，再关内部生产者。
    // 必须在 mqClient_->shutdown() 之前 —— 刷写要发消息，得有自己的传输层。
    if (traceDispatcher_) {
        try {
            traceDispatcher_->shutdown();
        } catch (const std::exception& e) {
            logger_warn(std::string("trace dispatcher shutdown failed: ") + e.what());
        }
        traceDispatcher_.reset();
    }
    // 先停心跳线程（它内部持有 mqClient_ 引用），再回收其它线程
    heartbeatStop_.store(true);
    if (heartbeatThread_.joinable()) {
        heartbeatThread_.join();
    }
    detectorStop_.store(true);
    if (detectorThread_.joinable()) {
        detectorThread_.join();
    }
    RequestFutureHolder::getInstance().shutdownScheduledTask();
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
        try {
            mqClient_->unregisterClientAllBrokers(clientId_, producerGroup_, "");
        } catch (const std::exception& e) {
            logger_debug("unregister on shutdown failed: " + std::string(e.what()));
        } catch (...) {
            logger_debug("unregister on shutdown failed: unknown exception");
        }
    }
    // 这一步 join 掉连接的读线程与超时清理线程：返回后不可能再有异步回调打进本对象，
    // 生产者的析构才是安全的（executeOnCallbackThread 在池子没了之后就地跑）。
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

void DefaultMQProducer::checkMessage(const Message& msg) const {
    Validators::checkMessage(msg, maxMessageSize_);
}

Message DefaultMQProducer::withNamespace(const Message& msg) const {
    if (namespace_.empty()) return msg;
    Message out = msg;
    out.topic = NamespaceUtil::wrapNamespace(namespace_, msg.topic);
    return out;
}

namespace {
constexpr const char* kPinnedTopicMismatchSync = "message's topic not equal mq's topic";
constexpr const char* kPinnedTopicMismatchAsync =
    "Topic of the message does not match its target message queue";
}  // namespace

void DefaultMQProducer::checkPinnedTopic(const std::string& msgTopic, const MessageQueue& mq,
                                         const std::string& message) const {
    if (NamespaceUtil::wrapNamespace(namespace_, msgTopic) !=
        NamespaceUtil::wrapNamespace(namespace_, mq.topic)) {
        throw MQClientException(message);
    }
}

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

// ---------------------------------------------------------------- 消息轨迹 / 钩子
SendMessageContext DefaultMQProducer::buildSendMessageContext(
    const Message& msg, const MessageQueue& mq, const std::string& brokerAddr) const {
    SendMessageContext context;
    context.producerGroup = producerGroup_;
    context.message = &msg;
    context.mq = mq;
    context.brokerAddr = brokerAddr;
    context.ns = namespace_;
    context.msgType = static_cast<int32_t>(TraceMessageType::NORMAL);
    if (msg.getProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED) == "true") {
        context.msgType = static_cast<int32_t>(TraceMessageType::TRANS);
    }
    if (!msg.getProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL).empty()) {
        context.msgType = static_cast<int32_t>(TraceMessageType::DELAY);
    }
    return context;
}

void DefaultMQProducer::executeSendMessageHookBefore(SendMessageContext& context) {
    for (auto& hook : sendMessageHookList_) {
        try {
            hook->sendMessageBefore(context);
        } catch (const std::exception& e) {
            logger_warn(std::string("failed to executeSendMessageHookBefore: ") + e.what());
        } catch (...) {
            logger_warn("failed to executeSendMessageHookBefore: unknown error");
        }
    }
}

void DefaultMQProducer::executeSendMessageHookAfter(SendMessageContext& context) {
    for (auto& hook : sendMessageHookList_) {
        try {
            hook->sendMessageAfter(context);
        } catch (const std::exception& e) {
            logger_warn(std::string("failed to executeSendMessageHookAfter: ") + e.what());
        } catch (...) {
            logger_warn("failed to executeSendMessageHookAfter: unknown error");
        }
    }
}

void DefaultMQProducer::executeCheckForbiddenHook(CheckForbiddenContext& context) {
    for (auto& hook : checkForbiddenHookList_) {
        hook->checkForbidden(context);
    }
}

void DefaultMQProducer::runCheckForbidden(const Message& msg, const MessageQueue& mq,
                                          const std::string& brokerAddr, const std::string* arg,
                                          CommunicationMode mode) {
    if (checkForbiddenHookList_.empty()) return;
    CheckForbiddenContext context;
    context.nameSrvAddr = nameServerAddrs_.empty() ? "" : nameServerAddrs_.front();
    context.group = producerGroup_;
    context.message = &msg;
    context.mq = mq;
    context.brokerAddr = brokerAddr;
    context.communicationMode = mode;
    context.arg = arg;
    context.unitMode = unitMode_;
    executeCheckForbiddenHook(context);
}

SendResult DefaultMQProducer::sendWithHooks(MQClientInstance& client, const Message& msg,
                                            const MessageQueue& mq, int32_t timeout,
                                            int32_t sysFlag, const std::string* arg,
                                            CommunicationMode mode) {
    // W3C traceparent 透传（opt-in）：没有就注入根上下文，已有值不覆盖。
    // ⚠ 必须放在 hasSendInterceptors() 早退之前，否则无钩子时注入被跳过。
    if (enableTraceContext_) {
        injectTraceContext(const_cast<Message*>(&msg));
    }
    if (!hasSendInterceptors()) {
        // 无钩子时零开销透传；client.sendMessage 内部同样走发布地址，失败抛同一句。
        return client.sendMessage(producerGroup_, msg, mq, timeout, sysFlag, unitMode_,
                                    createTopicKey_, defaultTopicQueueNums_);
    }
    const std::string brokerAddr = client.publishAddrFor(mq.brokerName, mq.topic);
    runCheckForbidden(msg, mq, brokerAddr, arg, mode);
    if (sendMessageHookList_.empty()) {
        return client.sendMessage(producerGroup_, msg, mq, timeout, sysFlag, unitMode_,
                                    createTopicKey_, defaultTopicQueueNums_);
    }
    SendMessageContext context = buildSendMessageContext(msg, mq, brokerAddr);
    executeSendMessageHookBefore(context);

    SendResult result;
    try {
        result = client.sendMessage(producerGroup_, msg, mq, timeout, sysFlag, unitMode_,
                          createTopicKey_, defaultTopicQueueNums_);
    } catch (const std::exception& e) {
        context.exception = e.what();
        executeSendMessageHookAfter(context);
        throw;
    } catch (...) {
        context.exception = "unknown error";
        executeSendMessageHookAfter(context);
        throw;
    }
    context.sendResult = &result;
    executeSendMessageHookAfter(context);
    return result;
}

void DefaultMQProducer::executeEndTransactionHook(const Message& msg,
                                                  const std::string& brokerAddr,
                                                  const std::string& msgId,
                                                  const std::string& transactionId,
                                                  const std::string& transactionState,
                                                  bool fromTransactionCheck) {
    if (endTransactionHookList_.empty()) return;
    EndTransactionContext context;
    context.producerGroup = producerGroup_;
    context.message = &msg;
    context.brokerAddr = brokerAddr;
    context.msgId = msgId;
    context.transactionId = transactionId;
    context.transactionState = transactionState;
    context.fromTransactionCheck = fromTransactionCheck;
    context.ns = namespace_;
    for (auto& hook : endTransactionHookList_) {
        try {
            hook->endTransaction(context);
        } catch (const std::exception& e) {
            logger_warn(std::string("failed to executeEndTransactionHook: ") + e.what());
        } catch (...) {
            logger_warn("failed to executeEndTransactionHook: unknown error");
        }
    }
}

void DefaultMQProducer::startTraceDispatcher() {
    if (!enableTrace_) return;
    try {
        auto dispatcher = std::make_shared<AsyncTraceDispatcher>(
            producerGroup_, TraceDispatcherType::PRODUCE, traceMsgBatchNum_, traceTopic_, rpcHook_);
        dispatcher->setNamespaceV2(namespaceV2_);
        dispatcher->setHostProducer(this);
        dispatcher->setHostClientId(clientId_);
        dispatcher->start(getNamesrvAddr());
        traceDispatcher_ = dispatcher;
        registerSendMessageHook(std::make_shared<SendMessageTraceHook>(dispatcher.get()));
        registerEndTransactionHook(std::make_shared<EndTransactionTraceHook>(dispatcher.get()));
        logger_info("producer trace enabled, traceTopic=" + dispatcher->getTraceTopicName());
    } catch (const std::exception& e) {
        logger_warn(std::string("start trace dispatcher failed: ") + e.what());
    }
}

std::shared_ptr<TopicPublishInfo> DefaultMQProducer::topicPublishInfo(MQClientInstance& c,
                                                                     const std::string& topic) {
    try {
        return c.getTopicPublishInfo(topic, /*isDefault=*/false);
    } catch (const MQClientException&) {
        try {
            return c.getTopicPublishInfo(topic, /*isDefault=*/true);
        } catch (const MQClientException&) {
            validateNameServerSetting(c);
            throw;
        }
    }
}

void DefaultMQProducer::validateNameServerSetting(MQClientInstance& c) {
    if (c.nameServerAddrs().empty()) {
        throw MQClientException("No name server address, please set it.",
                                ClientErrorCode::NO_NAME_SERVER_EXCEPTION);
    }
}


bool DefaultMQProducer::getAutoBatch() const {
    if (produceAccumulator_ == nullptr) {
        return false;
    }
    return autoBatch_;
}

void DefaultMQProducer::initProduceAccumulator() {
    produceAccumulator_ = getOrCreateProduceAccumulator(clientId_, this);
    if (batchMaxDelayMs_ > -1) {
        produceAccumulator_->setBatchMaxDelayMs(batchMaxDelayMs_);
    }
    if (batchMaxBytes_ > -1) {
        produceAccumulator_->setBatchMaxBytes(static_cast<int32_t>(batchMaxBytes_));
    }
    if (totalBatchMaxBytes_ > -1) {
        produceAccumulator_->setTotalBatchMaxBytes(static_cast<int32_t>(totalBatchMaxBytes_));
    }
}

void DefaultMQProducer::setBatchMaxDelayMs(int32_t holdMs) {
    batchMaxDelayMs_ = holdMs;
    if (produceAccumulator_ != nullptr) {
        produceAccumulator_->setBatchMaxDelayMs(holdMs);
    }
}

int32_t DefaultMQProducer::getBatchMaxDelayMs() const {
    if (produceAccumulator_ == nullptr) {
        return batchMaxDelayMs_;
    }
    return produceAccumulator_->getBatchMaxDelayMs();
}

void DefaultMQProducer::setBatchMaxBytes(int64_t holdSize) {
    batchMaxBytes_ = holdSize;
    if (produceAccumulator_ != nullptr) {
        produceAccumulator_->setBatchMaxBytes(static_cast<int32_t>(holdSize));
    }
}

int64_t DefaultMQProducer::getBatchMaxBytes() const {
    if (produceAccumulator_ == nullptr) {
        return batchMaxBytes_;
    }
    return produceAccumulator_->getBatchMaxBytes();
}

void DefaultMQProducer::setTotalBatchMaxBytes(int64_t totalHoldBytes) {
    totalBatchMaxBytes_ = totalHoldBytes;
    if (produceAccumulator_ != nullptr) {
        produceAccumulator_->setTotalBatchMaxBytes(static_cast<int32_t>(totalHoldBytes));
    }
}

int64_t DefaultMQProducer::getTotalBatchMaxBytes() const {
    if (produceAccumulator_ == nullptr) {
        return totalBatchMaxBytes_;
    }
    return produceAccumulator_->getTotalBatchMaxBytes();
}

bool DefaultMQProducer::canBatch(const Message& msg) const {
    if (produceAccumulator_ == nullptr || !produceAccumulator_->tryAddMessage(msg)) {
        return false;
    }
    // 延时/定时消息不攒批：一个 MessageBatch 只能有一个延时属性
    if (maxDelayValue(msg) > 0) {
        return false;
    }
    // 重试消息不攒批：%RETRY% topic 的位点语义特殊
    if (msg.getTopic().rfind(MixAll::RETRY_GROUP_TOPIC_PREFIX, 0) == 0) {
        return false;
    }
    // 已带 PGROUP 的消息（事务半消息那种）不攒批：broker 侧要按组找连接
    if (msg.properties.count(MessageConst::PROPERTY_PRODUCER_GROUP) > 0) {
        return false;
    }
    return true;
}

std::optional<SendResult> DefaultMQProducer::sendDirect(const Message& msg, const MessageQueue* mq,
                                                        std::shared_ptr<SendCallback> callback) {
    if (callback == nullptr) {
        return mq == nullptr ? sendDefaultImpl(msg, -1) : sendToMqImpl(msg, *mq, -1);
    }
    if (mq == nullptr) {
        sendAsyncImpl(msg, std::move(callback), -1);
    } else {
        sendAsyncToMqImpl(msg, *mq, std::move(callback), -1);
    }
    return std::nullopt;
}

std::optional<SendResult> DefaultMQProducer::sendByAccumulator(
    const Message& msg, const MessageQueue* mq, std::shared_ptr<SendCallback> callback) {
    if (!canBatch(msg)) {
        return sendDirect(msg, mq, std::move(callback));
    }
    checkMessage(msg);
    Message stamped = msg;
    ensureUniqId(stamped);
    if (callback == nullptr) {
        return mq == nullptr ? produceAccumulator_->send(stamped)
                             : produceAccumulator_->sendWithMq(stamped, *mq);
    }
    if (mq == nullptr) {
        produceAccumulator_->sendAsync(stamped, std::move(callback));
    } else {
        produceAccumulator_->sendAsyncWithMq(stamped, *mq, std::move(callback));
    }
    return std::nullopt;
}

// ---------------- AccumulatorSender 实现 ----------------

SendResult DefaultMQProducer::sendDirectBlocking(const MessageBatch& batch,
                                                 const MessageQueue* mq) {
    return sendPreparedBatch(batch, mq, -1);
}

void DefaultMQProducer::sendDirectAsync(const MessageBatch& batch, const MessageQueue* mq,
                                        std::shared_ptr<SendCallback> callback) {
    if (mq == nullptr) {
        sendBatchAsync(batch.messages, std::move(callback), -1);
    } else {
        sendBatchAsync(batch.messages, *mq, std::move(callback), -1);
    }
}

SendResult DefaultMQProducer::sendPreparedBatch(const MessageBatch& batch, const MessageQueue* pinned,
                                               int32_t timeoutMillis) {
    MQClientInstance& c = client();
    const int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    checkMessage(batch);
    MessageBatch outbound = batch;
    // 累加器的 AggregateKey.topic 已经是拼过命名空间的（生产者的公开入口先 withNamespace
    // 再分流），`wrapNamespace` 幂等，所以这一步只是补齐「直接调 sender」这条路径。
    if (!namespace_.empty()) {
        outbound.topic = NamespaceUtil::wrapNamespace(namespace_, outbound.topic);
    }
    MessageQueue target;
    if (pinned != nullptr) {
        checkPinnedTopic(outbound.topic, *pinned, kPinnedTopicMismatchSync);
        target = *pinned;
    } else {
        std::shared_ptr<TopicPublishInfo> publish = topicPublishInfo(c, outbound.topic);
        target = publish->selectOneMessageQueue();
    }
    // isBatch ⇒ prepareForSend 直接返回 0（批量永不压缩），但 batch.body 已经是编码结果，
    // 不需要（也不该）再 encode 一次。
    const int32_t sysFlag = prepareForSend(outbound);
    return sendWithHooks(c, outbound, target, timeout, sysFlag);
}

SendResult DefaultMQProducer::send(const Message& msg, int32_t timeoutMillis) {
    if (timeoutMillis < 0 && getAutoBatch() && !msg.isBatch) {
        return *sendByAccumulator(withNamespace(msg), nullptr, nullptr);
    }
    return sendDefaultImpl(msg, timeoutMillis);
}

SendResult DefaultMQProducer::sendDefaultImpl(const Message& msg, int32_t timeoutMillis) {
    MQClientInstance& c = client();
    const int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    checkMessage(msg);
    Message outbound = withNamespace(msg);
    ensureUniqId(outbound);
    const int32_t sysFlag = prepareForSend(outbound);

    std::shared_ptr<TopicPublishInfo> publish;
    try {
        publish = topicPublishInfo(c, outbound.topic);
    } catch (const MQClientException& e) {
        // 已经定过性的 10004（压根没配 name server）必须原样透传，否则两种故障又糊成一条
        throw MQClientException(e.what(), e.responseCode == ClientErrorCode::NO_NAME_SERVER_EXCEPTION
                                               ? e.responseCode
                                               : ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION);
    }

    const int32_t timesTotal = retryTimesWhenSendFailed_ + 1;
    const int64_t beginFirst = UtilAll::currentTimeMillis();
    std::vector<std::string> brokersSent;
    std::string lastBrokerName;
    SendResult result;
    bool gotResult = false;
    bool callTimeout = false;
    rememberDetectTopic(outbound.topic);
    // 最后一次失败的原因，用于循环结束后的错误码映射
    enum class Cause { NONE, BROKER, CONNECT, TIMEOUT, CLIENT, OTHER };
    Cause cause = Cause::NONE;
    int32_t brokerCode = 0;
    std::string lastError;

    for (int32_t times = 0; times < timesTotal; ++times) {
        MessageQueue selected;
        int64_t beginPrev = UtilAll::currentTimeMillis();
        std::chrono::steady_clock::time_point attemptBegan = std::chrono::steady_clock::now();
        try {
            selected = mqFaultStrategy_.selectOneMessageQueue(*publish, lastBrokerName,
                                                              /*resetIndex=*/times > 0);
            lastBrokerName = selected.brokerName;
            brokersSent.push_back(selected.brokerName);

            beginPrev = UtilAll::currentTimeMillis();
            const int64_t costTime = beginPrev - beginFirst;
            if (timeout < costTime) {
                callTimeout = true;
                break;
            }
            int32_t curTimeout = static_cast<int32_t>(timeout - costTime);
            const bool canRetryAgain = times + 1 < timesTotal;
            if (sendMsgMaxTimeoutPerRequest_ > -1 && canRetryAgain
                && curTimeout > sendMsgMaxTimeoutPerRequest_) {
                curTimeout = sendMsgMaxTimeoutPerRequest_;
            }
            attemptBegan = std::chrono::steady_clock::now();
            result = sendWithHooks(c, outbound, selected, curTimeout, sysFlag);
            gotResult = true;
            // 记录发送延迟；超出阈值会把该 broker 隔离一段时间
            mqFaultStrategy_.updateFaultItem(selected.brokerName, elapsedMillis(attemptBegan),
                                             false, true);
            if (result.sendStatus != SendStatus::SEND_OK && retryAnotherBrokerWhenNotStoreOK_) {
                continue;
            }
            return result;
        } catch (const MQBrokerException& e) {
            // broker 明确回了错误码：隔离该 broker（可达性不动），只有可重试码才换一台
            if (!selected.brokerName.empty()) {
                mqFaultStrategy_.updateFaultItem(selected.brokerName,
                                                 elapsedMillis(attemptBegan), true, false);
            }
            lastError = e.what();
            cause = Cause::BROKER;
            brokerCode = e.getResponseCode();
            if (isRetryResponseCode(brokerCode)) continue;
            if (gotResult) return result;
            throw;
        } catch (const RemotingException& e) {
            if (!selected.brokerName.empty()) {
                mqFaultStrategy_.updateFaultItem(selected.brokerName, elapsedMillis(attemptBegan),
                                                 true, !mqFaultStrategy_.isStartDetectorEnable());
            }
            lastError = e.what();
            if (dynamic_cast<const RemotingConnectException*>(&e) != nullptr) {
                cause = Cause::CONNECT;
            } else if (dynamic_cast<const RemotingTimeoutException*>(&e) != nullptr) {
                cause = Cause::TIMEOUT;
            } else {
                cause = Cause::OTHER;
            }
        } catch (const MQClientException& e) {
            if (!selected.brokerName.empty()) {
                mqFaultStrategy_.updateFaultItem(selected.brokerName,
                                                 elapsedMillis(attemptBegan), false, true);
            }
            lastError = e.what();
            cause = Cause::CLIENT;
        }
    }

    if (gotResult) return result;
    const int32_t costTotal = static_cast<int32_t>(UtilAll::currentTimeMillis() - beginFirst);
    if (callTimeout) {
        throw RemotingTooMuchRequestException("sendDefaultImpl call timeout");
    }
    std::string info = "Send [" + std::to_string(brokersSent.size())
                       + "] times, still failed, cost [" + std::to_string(costTotal)
                       + "]ms, Topic: " + outbound.topic + ", BrokersSent: ["
                       + joinStrings(brokersSent) + "], last error: " + lastError;
    int32_t responseCode = 1;
    switch (cause) {
        case Cause::BROKER: responseCode = brokerCode; break;
        case Cause::CONNECT: responseCode = ClientErrorCode::CONNECT_BROKER_EXCEPTION; break;
        case Cause::TIMEOUT: responseCode = ClientErrorCode::ACCESS_BROKER_TIMEOUT; break;
        case Cause::CLIENT: responseCode = ClientErrorCode::BROKER_NOT_EXIST_EXCEPTION; break;
        default: break;
    }
    throw MQClientException(info, responseCode);
}

SendResult DefaultMQProducer::send(const Message& msg, const MessageQueue& mq,
                                   int32_t timeoutMillis) {
    if (timeoutMillis < 0 && getAutoBatch() && !msg.isBatch) {
        MessageQueue target = mq;
        return *sendByAccumulator(withNamespace(msg), &target, nullptr);
    }
    return sendToMqImpl(msg, mq, timeoutMillis);
}

SendResult DefaultMQProducer::sendToMqImpl(const Message& msg, const MessageQueue& mq,
                                           int32_t timeoutMillis) {
    MQClientInstance& c = client();
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    checkMessage(msg);
    checkPinnedTopic(msg.topic, mq, kPinnedTopicMismatchSync);
    Message outbound = withNamespace(msg);
    ensureUniqId(outbound);
    const int32_t sysFlag = prepareForSend(outbound);
    return sendWithHooks(c, outbound, mq, timeout, sysFlag);
}

SendResult DefaultMQProducer::sendBySelector(const Message& msg,
                                             const MessageQueueSelector& selector,
                                             const std::string& arg, int32_t timeoutMillis) {
    MQClientInstance& c = client();
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    checkMessage(msg);
    Message outbound = withNamespace(msg);
    ensureUniqId(outbound);
    std::shared_ptr<TopicPublishInfo> publish = topicPublishInfo(c, outbound.topic);
    MessageQueue selected = selector.select(publish->msgQueueList, outbound, arg);
    if (timeoutMillis < 0 && getAutoBatch() && !outbound.isBatch) {
        return *sendByAccumulator(outbound, &selected, nullptr);
    }
    // 选择器用的是原始消息（topic/业务字段），压缩只影响 body
    const int32_t sysFlag = prepareForSend(outbound);
    return sendWithHooks(c, outbound, selected, timeout, sysFlag, &arg, CommunicationMode::SYNC);
}

namespace {

int32_t elapsedMs(const std::chrono::steady_clock::time_point& from) {
    return static_cast<int32_t>(elapsedMillis(from));
}

void classifyAsyncFailure(const InvokeError& error, int32_t cost, InvokeError* wrapped,
                          bool* needRetry) {
    switch (error.kind) {
        case InvokeError::Kind::SEND_REQUEST:
            *wrapped = InvokeError(InvokeError::Kind::OTHER, "send request failed");
            *needRetry = true;
            return;
        case InvokeError::Kind::TIMEOUT:
            *wrapped = InvokeError(InvokeError::Kind::OTHER,
                                   "wait response timeout, cost=" + std::to_string(cost));
            *needRetry = true;
            return;
        case InvokeError::Kind::RESPONSE_FAILED:
            *wrapped = error;  // 原样交给回调，不包装
            *needRetry = false;
            return;
        case InvokeError::Kind::TOO_MUCH_REQUEST:
            *wrapped = InvokeError(InvokeError::Kind::OTHER, "unknown reason");
            *needRetry = false;
            return;
        default:
            *wrapped = InvokeError(InvokeError::Kind::OTHER, "unknown reason");
            *needRetry = true;
            return;
    }
}

std::exception_ptr makeSendException(const InvokeError& error) {
    switch (error.kind) {
        case InvokeError::Kind::TOO_MUCH_REQUEST:
            return std::make_exception_ptr(RemotingTooMuchRequestException(error.message));
        case InvokeError::Kind::RESPONSE_FAILED:
            if (error.responseCode != 0) {
                return std::make_exception_ptr(
                    MQBrokerException(error.responseCode, error.message));
            }
            return std::make_exception_ptr(RemotingCommandException(error.message));
        case InvokeError::Kind::TIMEOUT:
            return std::make_exception_ptr(RemotingTimeoutException(error.message));
        case InvokeError::Kind::SEND_REQUEST:
            return std::make_exception_ptr(RemotingSendRequestException(error.message));
        case InvokeError::Kind::CONNECT:
            return std::make_exception_ptr(RemotingConnectException(error.message));
        default:
            return std::make_exception_ptr(MQClientException(error.message));
    }
}

}  // namespace

void DefaultMQProducer::setBackPressureForAsyncSendNum(int32_t num) {
    backPressureForAsyncSendNum_ = std::max(num, static_cast<int32_t>(kMinAsyncSendNum));
    semaphoreAsyncSendNum_.setTotalPermits(backPressureForAsyncSendNum_);
}

void DefaultMQProducer::setBackPressureForAsyncSendSize(int32_t size) {
    backPressureForAsyncSendSize_ = std::max(size, static_cast<int32_t>(kMinAsyncSendSize));
    semaphoreAsyncSendSize_.setTotalPermits(backPressureForAsyncSendSize_);
}

int64_t DefaultMQProducer::getSemaphoreAsyncSendNumAvailablePermits() const {
    return semaphoreAsyncSendNum_.availablePermits();
}

int64_t DefaultMQProducer::getSemaphoreAsyncSendSizeAvailablePermits() const {
    return semaphoreAsyncSendSize_.availablePermits();
}

void DefaultMQProducer::createAsyncExecutors() {
    int32_t cores = static_cast<int32_t>(std::thread::hardware_concurrency());
    if (cores <= 0) {
        cores = 1;  // 取不到 CPU 数时按 1 核算，不能建出一个跑不动任务的池
    }
    asyncSenderExecutor_ = std::make_shared<ConsumeExecutor>(
        cores, cores, 60.0, "AsyncSenderExecutor", asyncSenderQueueCapacity_, "_", 1);
    int32_t callbackThreads = clientCallbackExecutorThreads_;
    if (callbackThreads <= 0) {
        callbackThreads = cores;
    }
    callbackExecutor_ = std::make_shared<ConsumeExecutor>(
        callbackThreads, callbackThreads, 60.0, "NettyClientPublicExecutor", 0, "_", 1);
}

void DefaultMQProducer::sendAsync(const Message& msg, std::shared_ptr<SendCallback> callback,
                                  int32_t timeoutMillis) {
    if (timeoutMillis < 0 && getAutoBatch() && !msg.isBatch) {
        sendByAccumulator(withNamespace(msg), nullptr, std::move(callback));
        return;
    }
    sendAsyncImpl(msg, std::move(callback), timeoutMillis);
}

void DefaultMQProducer::sendAsyncImpl(const Message& msg, std::shared_ptr<SendCallback> callback,
                                      int32_t timeoutMillis) {
    auto state = std::make_shared<AsyncSendState>();
    state->msg = msg;
    state->callback = std::move(callback);
    enqueueAsync(state, nullptr, timeoutMillis);
}

void DefaultMQProducer::sendAsync(const Message& msg, const MessageQueue& mq,
                                  std::shared_ptr<SendCallback> callback, int32_t timeoutMillis) {
    if (timeoutMillis < 0 && getAutoBatch() && !msg.isBatch) {
        MessageQueue target = mq;
        sendByAccumulator(withNamespace(msg), &target, std::move(callback));
        return;
    }
    sendAsyncToMqImpl(msg, mq, std::move(callback), timeoutMillis);
}

void DefaultMQProducer::sendAsyncToMqImpl(const Message& msg, const MessageQueue& mq,
                                          std::shared_ptr<SendCallback> callback,
                                          int32_t timeoutMillis) {
    auto state = std::make_shared<AsyncSendState>();
    state->msg = msg;
    state->callback = std::move(callback);
    enqueueAsync(state, &mq, timeoutMillis);
}

void DefaultMQProducer::sendBatchAsync(const std::vector<Message>& msgs,
                                       std::shared_ptr<SendCallback> callback,
                                       int32_t timeoutMillis) {
    auto state = std::make_shared<AsyncSendState>();
    state->batch = true;
    state->batchMsgs = msgs;
    state->callback = std::move(callback);
    enqueueAsync(state, nullptr, timeoutMillis);
}

void DefaultMQProducer::sendBatchAsync(const std::vector<Message>& msgs, const MessageQueue& mq,
                                       std::shared_ptr<SendCallback> callback,
                                       int32_t timeoutMillis) {
    auto state = std::make_shared<AsyncSendState>();
    state->batch = true;
    state->batchMsgs = msgs;
    state->callback = std::move(callback);
    enqueueAsync(state, &mq, timeoutMillis);
}

void DefaultMQProducer::enqueueAsync(const std::shared_ptr<AsyncSendState>& state,
                                     const MessageQueue* pinned, int32_t timeoutMillis) {
    // 先确认已启动（未启动立即抛，而不是在后台线程里静默失败）
    (void)client();
    std::shared_ptr<ConsumeExecutor> pool;
    {
        std::lock_guard<std::mutex> lk(lock_);
        pool = asyncSenderExecutor_;
    }
    if (pool == nullptr) {
        throw MQClientException("producer already shutdown");
    }
    const int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    state->timeout = timeout;
    state->msgLen =
        state->batch ? batchBackPressureMsgLen(state->batchMsgs) : backPressureMsgLen(state->msg);
    if (pinned != nullptr) {
        state->mq = *pinned;
        state->pinned = true;
    }
    const std::chrono::steady_clock::time_point began = std::chrono::steady_clock::now();
    const std::function<void()> runnable = [this, state, began, timeout]() {
        const int32_t cost = elapsedMs(began);
        if (timeout <= cost) {
            const InvokeError tooMuch(InvokeError::Kind::TOO_MUCH_REQUEST,
                                      "DEFAULT ASYNC send call timeout");
            completeAsync(state, nullptr, &tooMuch);
            return;
        }
        state->timeout = timeout - cost;
        try {
            if (state->batch) {
                sendBatchAsyncInner(state, state->pinned ? &state->mq : nullptr);
            } else {
                sendAsyncInner(state, state->pinned ? &state->mq : nullptr);
            }
        } catch (const std::exception& e) {
            const InvokeError err(InvokeError::Kind::OTHER, e.what());
            completeAsync(state, nullptr, &err);
        } catch (...) {
            const InvokeError err(InvokeError::Kind::OTHER, "unknown error");
            completeAsync(state, nullptr, &err);
        }
    };
    executeAsyncMessageSend(state, runnable, timeout, began, pool);
}

void DefaultMQProducer::executeAsyncMessageSend(const std::shared_ptr<AsyncSendState>& state,
                                                const std::function<void()>& runnable,
                                                int32_t timeout,
                                                const std::chrono::steady_clock::time_point& began,
                                                const std::shared_ptr<ConsumeExecutor>& pool) {
    if (enableBackpressureForAsyncMode_) {
        const int32_t numBudget = timeout - elapsedMs(began);
        state->numAcquired =
            numBudget > 0 && semaphoreAsyncSendNum_.tryAcquire(1, numBudget);
        if (!state->numAcquired) {
            const InvokeError tooMuch(InvokeError::Kind::TOO_MUCH_REQUEST,
                                       "send message tryAcquire semaphoreAsyncNum timeout");
            completeAsync(state, nullptr, &tooMuch);
            return;
        }
        const int32_t sizeBudget = timeout - elapsedMs(began);
        state->sizeAcquired =
            sizeBudget > 0 && semaphoreAsyncSendSize_.tryAcquire(state->msgLen, sizeBudget);
        if (!state->sizeAcquired) {
            const InvokeError tooMuch(InvokeError::Kind::TOO_MUCH_REQUEST,
                                      "send message tryAcquire semaphoreAsyncSize timeout");
            completeAsync(state, nullptr, &tooMuch);
            return;
        }
    }
    try {
        pool->submit(runnable);
    } catch (const RejectedExecutionError&) {
        if (enableBackpressureForAsyncMode_) {
            runnable();
        } else {
            throw MQClientException("executor rejected");
        }
    }
}

void DefaultMQProducer::releaseBackPressure(const std::shared_ptr<AsyncSendState>& state) {
    if (state->permitsReleased) {
        return;
    }
    state->permitsReleased = true;
    if (state->sizeAcquired) {
        semaphoreAsyncSendSize_.release(state->msgLen);
    }
    if (state->numAcquired) {
        semaphoreAsyncSendNum_.release(1);
    }
}

void DefaultMQProducer::sendBatchAsyncInner(const std::shared_ptr<AsyncSendState>& state,
                                            const MessageQueue* pinned) {
    // 批量分支：批量在本实现里只有同步内核，所以「异步」= 在
    // AsyncSenderExecutor 线程里同步发一批，结果挪到回调线程交付。对调用方语义没差别。
    // 校验、组批、拼 namespace、选队列、钩子全在内核里，这里不重复做。
    const int32_t timeout = state->timeout;
    try {
        const SendResult result = sendBatchKernel(state->batchMsgs, pinned, timeout,
                                                  kPinnedTopicMismatchAsync);
        executeOnCallbackThread([this, state, result]() { completeAsync(state, &result, nullptr); });
    } catch (const MQBrokerException& e) {
        executeOnCallbackThread([this, state, msg = std::string(e.what()), code = e.responseCode]() {
            const InvokeError err(InvokeError::Kind::RESPONSE_FAILED, msg, code);
            completeAsync(state, nullptr, &err);
        });
    } catch (const std::exception& e) {
        // 内核的失败（路由拿不到、校验不过、重试用尽）原样交付回调，与单条异步一致。
        executeOnCallbackThread([this, state, text = std::string(e.what())]() {
            const InvokeError err(InvokeError::Kind::OTHER, text);
            completeAsync(state, nullptr, &err);
        });
    }
}

void DefaultMQProducer::sendAsyncInner(const std::shared_ptr<AsyncSendState>& state,
                                       const MessageQueue* pinned) {
    MQClientInstance& c = client();
    checkMessage(state->msg);
    if (pinned != nullptr) {
        checkPinnedTopic(state->msg.topic, *pinned, kPinnedTopicMismatchAsync);
    }
    state->msg = withNamespace(state->msg);
    ensureUniqId(state->msg);
    // 与同步发送同理：压缩在重试链之外做一次，避免重试时把已压缩的 body 再压一遍
    state->sysFlag = prepareForSend(state->msg);
    if (pinned != nullptr) {
        sendKernelAsync(state);
        return;
    }
    try {
        state->publish = topicPublishInfo(c, state->msg.topic);
    } catch (const MQClientException& e) {
        // 同 sendDefaultImpl：10004 不能被改写成 10005，异步链的定性和同步链不能分叉
        throw MQClientException(e.what(), e.responseCode == ClientErrorCode::NO_NAME_SERVER_EXCEPTION
                                               ? e.responseCode
                                               : ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION);
    }
    // ASYNC 的 timesTotal 固定为 1：外层只跑一次，换 broker 全在 onSendException 里
    MessageQueue selected =
        mqFaultStrategy_.selectOneMessageQueue(*state->publish, std::string(), false);
    if (selected.brokerName.empty()) {
        throw MQClientException("Send [0] times, still failed, Topic: " + state->msg.topic
                                + ", BrokersSent: []");
    }
    state->mq = MessageQueue(state->msg.topic, selected.brokerName, selected.queueId);
    sendKernelAsync(state);
}

void DefaultMQProducer::sendKernelAsync(const std::shared_ptr<AsyncSendState>& state) {
    MQClientInstance& c = client();
    const std::chrono::steady_clock::time_point beginKernel = std::chrono::steady_clock::now();
    state->brokerName = state->mq.brokerName;

    const std::string brokerAddr = c.publishAddrFor(state->mq.brokerName, state->mq.topic);
    if (hasCheckForbiddenHook()) {
        runCheckForbidden(state->msg, state->mq, brokerAddr, nullptr, CommunicationMode::ASYNC);
    }
    if (enableTraceContext_) {
        injectTraceContext(&state->msg);
    }
    state->request =
        c.buildSendRequest(producerGroup_, state->msg, state->mq, state->sysFlag, unitMode_,
                           createTopicKey_, defaultTopicQueueNums_);
    if (hasSendMessageHook()) {
        state->context = std::make_shared<SendMessageContext>(
            buildSendMessageContext(state->msg, state->mq, brokerAddr));
        executeSendMessageHookBefore(*state->context);
    }
    const int32_t costAsync = elapsedMs(beginKernel);
    if (state->timeout < costAsync) {
        const InvokeError tooMuch(InvokeError::Kind::TOO_MUCH_REQUEST,
                                  "sendKernelImpl call timeout");
        completeAsync(state, nullptr, &tooMuch);
        return;
    }
    state->timeout -= costAsync;
    sendAttempt(state, brokerAddr);
}

void DefaultMQProducer::sendAttempt(const std::shared_ptr<AsyncSendState>& state,
                                    const std::string& addr) {
    state->attemptBegan = std::chrono::steady_clock::now();
    try {
        mqClient_->sendMessageAsync(addr, state->request, state->msg, state->mq, state->timeout,
                                    [this, state](const SendResult& result,
                                                  const InvokeError& error) {
                                        // 回调处理挪到 NettyClientPublicExecutor：
                                        // 这里是连接读线程/超时清理线程，不能跑业务代码
                                        SendResult copy = result;
                                        InvokeError err = error;
                                        executeOnCallbackThread([this, state, copy, err]() {
                                            onAttemptComplete(state, copy, err);
                                        });
                                    });
    } catch (const RemotingConnectException& e) {
        onTransportThrow(state, e.what(), InvokeError::Kind::CONNECT);
    } catch (const RemotingSendRequestException& e) {
        onTransportThrow(state, e.what(), InvokeError::Kind::SEND_REQUEST);
    } catch (const RemotingTimeoutException& e) {
        onTransportThrow(state, e.what(), InvokeError::Kind::TIMEOUT);
    } catch (const std::exception& e) {
        onTransportThrow(state, e.what(), InvokeError::Kind::OTHER);
    } catch (...) {
        onTransportThrow(state, "unknown error", InvokeError::Kind::OTHER);
    }
}

void DefaultMQProducer::onTransportThrow(const std::shared_ptr<AsyncSendState>& state,
                                         const std::string& message, InvokeError::Kind kind) {
    const int32_t cost = elapsedMs(state->attemptBegan);
    mqFaultStrategy_.updateFaultItem(state->brokerName, cost, true, false);
    state->timeout -= cost;
    onSendException(state, InvokeError(kind, message), true);
}

void DefaultMQProducer::onAttemptComplete(const std::shared_ptr<AsyncSendState>& state,
                                          const SendResult& result, const InvokeError& error) {
    const int32_t cost = elapsedMs(state->attemptBegan);
    if (error.empty()) {
        mqFaultStrategy_.updateFaultItem(state->brokerName, cost, false, true);
        completeAsync(state, &result, nullptr);
        return;
    }
    mqFaultStrategy_.updateFaultItem(state->brokerName, cost, true, true);
    InvokeError wrapped;
    bool needRetry = false;
    classifyAsyncFailure(error, cost, &wrapped, &needRetry);
    logger_warn("async send to broker " + state->brokerName + " failed: " + error.message
                + " (reported as " + wrapped.message + ")");
    // 超时预算是**所有尝试共享**的剩余时间，不是每次尝试各给一份
    state->timeout -= cost;
    onSendException(state, wrapped, needRetry);
}

void DefaultMQProducer::onSendException(const std::shared_ptr<AsyncSendState>& state,
                                        const InvokeError& error, bool needRetry) {
    state->times += 1;
    if (needRetry && state->times <= retryTimesWhenSendAsyncFailed_ && state->timeout > 0) {
        std::string retryBroker = state->brokerName;
        if (state->publish != nullptr) {
            // 第三个参数是 false：按 lastBrokerName 避开刚失败的那台
            MessageQueue selected = mqFaultStrategy_.selectOneMessageQueue(
                *state->publish, state->brokerName, false);
            if (!selected.brokerName.empty()) {
                state->mq = MessageQueue(state->msg.topic, selected.brokerName, selected.queueId);
                retryBroker = selected.brokerName;
            }
        }
        const std::string addr = client().findBrokerAddressInPublish(retryBroker);
        if (addr.empty()) {
            // 路由里连这台 broker 的主地址都没了，再试也没有意义
            const InvokeError noBroker(InvokeError::Kind::OTHER,
                                       "The broker[" + retryBroker + "] not exist");
            completeAsync(state, nullptr, &noBroker);
            return;
        }
        logger_warn("async send msg by retry " + std::to_string(state->times) + " times. topic="
                    + state->msg.topic + ", brokerAddr=" + addr + ", brokerName=" + retryBroker
                    + ": " + error.message);
        state->brokerName = retryBroker;
        // 换新 opaque：旧请求还挂在在途表里等超时，复用会把两次尝试的应答串台
        state->request.opaque = RemotingCommand::nextOpaque();
        sendAttempt(state, addr);
        return;
    }
    completeAsync(state, nullptr, &error);
}

void DefaultMQProducer::executeOnCallbackThread(std::function<void()> fn) {
    std::shared_ptr<ConsumeExecutor> pool;
    {
        std::lock_guard<std::mutex> lk(lock_);
        pool = callbackExecutor_;
    }
    if (pool == nullptr) {
        fn();
        return;
    }
    try {
        pool->submit(std::move(fn));
    } catch (const RejectedExecutionError&) {
        fn();
    }
}

void DefaultMQProducer::completeAsync(const std::shared_ptr<AsyncSendState>& state,
                                      const SendResult* result, const InvokeError* error) {
    // 异步链的终点：先跑 SendMessageHook.after，再回调用户
    if (state->context != nullptr) {
        if (result != nullptr) {
            state->context->sendResult = result;
        } else if (error != nullptr) {
            state->context->exception = error->message;
        }
        executeSendMessageHookAfter(*state->context);
    }
    releaseBackPressure(state);
    if (state->callback == nullptr) {
        return;
    }
    try {
        if (error != nullptr) {
            state->callback->onException(makeSendException(*error));
        } else {
            state->callback->onSuccess(*result);
        }
    } catch (...) {
        logger_warn("sendAsync callback threw an exception");
    }
}

void DefaultMQProducer::sendOneway(const Message& msg) {
    MQClientInstance& c = client();
    checkMessage(msg);
    Message outbound = withNamespace(msg);
    std::shared_ptr<TopicPublishInfo> publish = topicPublishInfo(c, outbound.topic);
    MessageQueue selected = publish->selectOneMessageQueue();
    const int32_t sysFlag = prepareForSend(outbound);
    const std::string brokerAddr = c.publishAddrFor(selected.brokerName, selected.topic);
    runCheckForbidden(outbound, selected, brokerAddr, nullptr, CommunicationMode::ONEWAY);
    // W3C traceparent 透传（opt-in）：单向发送同样注入
    if (enableTraceContext_) {
        injectTraceContext(&outbound);
    }
    c.sendMessageOneway(producerGroup_, outbound, selected, sendMsgTimeout_, sysFlag,
                          unitMode_, createTopicKey_, defaultTopicQueueNums_);
}

void DefaultMQProducer::sendOneway(const Message& msg, const MessageQueue& mq) {
    MQClientInstance& c = client();
    checkMessage(msg);
    Message outbound = withNamespace(msg);
    const int32_t sysFlag = prepareForSend(outbound);
    const std::string brokerAddr = c.publishAddrFor(mq.brokerName, mq.topic);
    runCheckForbidden(outbound, mq, brokerAddr, nullptr, CommunicationMode::ONEWAY);
    if (enableTraceContext_) {
        injectTraceContext(&outbound);
    }
    c.sendMessageOneway(producerGroup_, outbound, mq, sendMsgTimeout_, sysFlag,
                        unitMode_, createTopicKey_, defaultTopicQueueNums_);
}

// ---------------------------------------------------------------- Request-Reply

namespace {

struct RequestFutureRemover {
    const std::string& correlationId;
    ~RequestFutureRemover() { RequestFutureHolder::getInstance().removeRequest(correlationId); }
};

}  // namespace

Message DefaultMQProducer::request(const Message& msg, int32_t timeoutMillis) {
    MQClientInstance& c = client();
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : requestTimeoutMillis_;
    checkMessage(msg);
    Message outbound = withNamespace(msg);
    std::shared_ptr<TopicPublishInfo> publish = topicPublishInfo(c, outbound.topic);
    MessageQueue selected = publish->selectOneMessageQueue();
    return requestWithQueue(outbound, selected, timeout);
}

Message DefaultMQProducer::request(const Message& msg, const MessageQueue& mq,
                                   int32_t timeoutMillis) {
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : requestTimeoutMillis_;
    checkMessage(msg);
    Message outbound = withNamespace(msg);
    return requestWithQueue(outbound, mq, timeout);
}

Message DefaultMQProducer::requestWithQueue(Message& outbound, const MessageQueue& mq,
                                            int32_t timeout) {
    MQClientInstance& c = client();

    const std::string correlationId = createCorrelationId();
    outbound.putProperty(MessageConst::PROPERTY_CORRELATION_ID, correlationId);
    outbound.putProperty(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT, clientId_);
    outbound.putProperty(MessageConst::PROPERTY_MESSAGE_TTL, std::to_string(timeout));

    const int64_t begin = UtilAll::currentTimeMillis();
    // 先确认路由已知，再补一次心跳：没在 broker 上登记为 producer，broker 就找不到
    // channel 把应答推回来（REPLY_TO_CLIENT 反查 ProducerManager 的 clientChannelTable）。
    try {
        (void)topicPublishInfo(c, outbound.topic);
        sendHeartbeatToAllBroker();
    } catch (const std::exception& e) {
        // 拿不到路由就让下面的发送路径自己报错
        logger_debug(std::string("request: prepare route/heartbeat failed: ") + e.what());
    }

    auto future = std::make_shared<RequestResponseFuture>(correlationId, timeout);
    RequestFutureHolder::getInstance().putRequest(correlationId, future);
    RequestFutureRemover remover{correlationId};
    (void)remover;

    const int64_t cost = UtilAll::currentTimeMillis() - begin;
    try {
        const int32_t sysFlag = prepareForSend(outbound);
        c.sendMessage(producerGroup_, outbound, mq, timeout, sysFlag, unitMode_,
                      createTopicKey_, defaultTopicQueueNums_);
    } catch (...) {
        future->setSendRequestOk(false);
        future->setCause(std::current_exception());
        future->putResponseMessage();
    }
    return waitRequestResponse(outbound, timeout, future, cost);
}

Message DefaultMQProducer::waitRequestResponse(const Message& outbound, int32_t timeout,
                                               const std::shared_ptr<RequestResponseFuture>& future,
                                               int64_t costMillis) {
    const int64_t remain = static_cast<int64_t>(timeout) - costMillis;
    future->waitResponseMessage(remain > 0 ? static_cast<int32_t>(remain) : 0);
    if (!future->hasResponse()) {
        throw RequestTimeoutException("send request message to <" + outbound.topic
                                      + "> OK, but wait reply message timeout, "
                                      + std::to_string(timeout) + " ms.");
    }
    if (!future->isSendRequestOk() || future->cause() != nullptr) {
        std::string detail = "send request message to <" + outbound.topic + "> fail";
        if (future->cause() != nullptr) {
            try {
                std::rethrow_exception(future->cause());
            } catch (const std::exception& e) {
                detail += ": ";
                detail += e.what();
            } catch (...) {
            }
        }
        throw MQClientException(detail);
    }
    // 应答在 MessageExt 里带全属性（CORRELATION_ID / REPLY_MESSAGE_ARRIVE_TIME 等）
    return future->responseMessage();
}

// ---------------------------------------------------------------- 批量
SendResult DefaultMQProducer::sendBatch(const std::vector<Message>& msgs, int32_t timeoutMillis) {
    return sendBatchKernel(msgs, nullptr, timeoutMillis, kPinnedTopicMismatchSync);
}

SendResult DefaultMQProducer::sendBatch(const std::vector<Message>& msgs, const MessageQueue& mq,
                                        int32_t timeoutMillis) {
    return sendBatchKernel(msgs, &mq, timeoutMillis, kPinnedTopicMismatchSync);
}

SendResult DefaultMQProducer::sendBatchKernel(const std::vector<Message>& msgs,
                                              const MessageQueue* pinned,
                                              int32_t timeoutMillis,
                                              const std::string& pinnedGuardMessage) {
    MQClientInstance& c = client();
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    if (msgs.empty()) {
        throw MQClientException("message list is empty");
    }
    std::vector<Message> subs;
    subs.reserve(msgs.size());
    for (const Message& m : msgs) {
        Validators::checkMessage(m, maxMessageSize_);
        Message sub = m;
        ensureUniqId(sub);
        if (!namespace_.empty()) {
            sub.topic = NamespaceUtil::wrapNamespace(namespace_, sub.topic);
        }
        subs.push_back(std::move(sub));
    }
    MessageBatch batch = MessageBatch::generateFromList(subs);
    ensureUniqId(batch);
    batch.body = batch.encode();
    checkMessage(batch);
    if (!namespace_.empty()) {
        batch.topic = NamespaceUtil::wrapNamespace(namespace_, batch.topic);
    }
    if (pinned != nullptr) {
        checkPinnedTopic(batch.topic, *pinned, pinnedGuardMessage);
    }
    MessageQueue target;
    if (pinned != nullptr) {
        target = *pinned;
    } else {
        std::shared_ptr<TopicPublishInfo> publish = topicPublishInfo(c, batch.topic);
        target = publish->selectOneMessageQueue();
    }
    // MessageBatch 的 isBatch 为 true，prepareForSend 会直接返回 0（不压缩）
    const int32_t sysFlag = prepareForSend(batch);
    return sendWithHooks(c, batch, target, timeout, sysFlag);
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

// ---------------------------------------------------------------- 故障规避主动探测
void DefaultMQProducer::detectorLoop() {
    setThreadName("FaultDetectorThread");
    auto next = std::chrono::steady_clock::now();
    // 注意：sleepUntilDeadline 的第一参是"停止标志"（false=继续睡，true=立刻返回），
    // 与 consumer 的 stop_、mq_client 的 routeRefreshStop_ 同一约定。此前把运行中标志
    // detectorRunning_ 直接传进来，极性反了：运行期 sleepUntilDeadline 立即返回，
    // 循环变成零节流热轮询，next 每轮无条件 +3s 被推到实际时刻几十小时之外；
    // shutdown 置停后线程反而进入"睡到 next"分支，join 被拖住数小时（ctest 挂死根因）。
    while (!detectorStop_.load()) {
        sleepUntilDeadline(detectorStop_, next);
        next += std::chrono::milliseconds(3000);
        if (!mqFaultStrategy_.isStartDetectorEnable()) {
            continue;
        }
        try {
            mqFaultStrategy_.latencyFaultTolerance().detectByOneRound();
        } catch (const std::exception& e) {
            logger_debug(std::string("fault detector round failed: ") + e.what());
        } catch (...) {
            logger_debug("fault detector round failed: unknown exception");
        }
    }
}

std::string DefaultMQProducer::pickDetectTopic() {
    std::lock_guard<std::mutex> lk(detectTopicsMutex_);
    return detectTopics_.empty() ? std::string() : *detectTopics_.begin();
}

void DefaultMQProducer::rememberDetectTopic(const std::string& topic) {
    if (topic.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lk(detectTopicsMutex_);
    detectTopics_.insert(topic);
}

bool DefaultMQProducer::detectBrokerByMaxOffset(const std::string& addr, int32_t timeoutMillis) {
    if (addr.empty()) {
        return false;
    }
    const std::string topic = pickDetectTopic();
    if (topic.empty()) {
        return false;
    }
    try {
        MessageQueue mq(topic, /*brokerName=*/std::string(), /*queueId=*/0);
        client().getMaxOffset(mq, timeoutMillis, addr);
        return true;
    } catch (const std::exception& e) {
        logger_debug("detector: GET_MAX_OFFSET probe to " + addr + " failed: " + std::string(e.what()));
        return false;
    }
}

// ---------------------------------------------------------------- 事务消息
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
        addr = c.findBrokerAddressInPublish(sendResult.messageQueue.brokerName);
    }

    RemotingCommand request = RemotingCommand::createRequestCommand(
        RequestCode::END_TRANSACTION, std::make_shared<EndTransactionRequestHeader>(header));
    if (hasLocalException) {
        request.remark = "executeLocalTransactionBranch exception: " + localExceptionText;
    }
    if (addr.empty()) {
        throw MQClientException("no broker address for end transaction");
    }
    c.remotingClient().invokeOneway(addr, request);

    executeEndTransactionHook(msg, addr, header.msgId.value_or(std::string()),
                              msg.transactionId, localTransactionStateName(state), fromCheck);
}

void DefaultMQProducer::checkTransactionState(const RemotingCommand& cmd, const std::string& addr) {
    CheckTransactionStateRequestHeader header;
    header.fromExtFields(cmd.extFields);

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
    if (msg.getProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL).size() > 0) {
        throw MQClientException("Transactional messages do not support delayed delivery");
    }

    MQClientInstance& c = client();
    checkMessage(msg);

    // 半消息标记：broker 据此把消息写入 RMQ_SYS_TRANS_HALF_TOPIC，等待 END_TRANSACTION
    Message outbound = withNamespace(msg);
    outbound.putProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED, "true");
    outbound.putProperty(MessageConst::PROPERTY_PRODUCER_GROUP, producerGroup_);
    txListener_ = &listener;

    std::shared_ptr<TopicPublishInfo> publish = topicPublishInfo(c, outbound.topic);
    MessageQueue selected = publish->selectOneMessageQueue();

    ensureUniqId(outbound);
    int32_t sysFlag = prepareForSend(outbound);
    sysFlag = MessageSysFlag::resetTransactionValue(sysFlag,
                                                    MessageSysFlag::TRANSACTION_PREPARED_TYPE);
    SendResult sendResult;
    try {
        sendResult = sendWithHooks(c, outbound, selected, sendMsgTimeout_, sysFlag);
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
            state = ret;
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
    const std::string realTopic = namespace_.empty() ? topic : NamespaceUtil::wrapNamespace(namespace_, topic);
    bool found = c.queryMessage(realTopic, key, maxNum, beginTimestamp, endTimestamp, body, 15000);
    if (!found || body.empty()) {
        return {};
    }
    return decodeMessages(body);
}

std::vector<MessageQueue> DefaultMQProducer::fetchPublishMessageQueues(const std::string& topic) {
    MQClientInstance& c = client();
    const std::string realTopic = namespace_.empty() ? topic : NamespaceUtil::wrapNamespace(namespace_, topic);
    std::shared_ptr<TopicPublishInfo> publish = topicPublishInfo(c, realTopic);
    return publish->msgQueueList;
}

void DefaultMQProducer::createTopic(const std::string& key, const std::string& newTopic,
                                    int32_t queueNum) {
    MQClientInstance& c = client();
    (void)key;
    Validators::checkTopic(newTopic);
    Validators::isSystemTopic(newTopic);
    constexpr int32_t perm = 6;  // PERM_READ | PERM_WRITE
    const std::string realTopic = namespace_.empty() ? newTopic : NamespaceUtil::wrapNamespace(namespace_, newTopic);
    c.createTopicInRoute(realTopic, queueNum, queueNum, perm);
}

std::string DefaultMQProducer::recallMessage(const std::string& topic,
                                             const std::string& recallHandle) {
    MQClientInstance& c = client();
    const std::string realTopic =
        namespace_.empty() ? topic : NamespaceUtil::wrapNamespace(namespace_, topic);
    Validators::checkTopic(realTopic);
    if (MixAll::isRetryTopic(realTopic) || MixAll::isDlqTopic(realTopic)) {
        throw MQClientException("topic is not supported");
    }
    const HandleV1 handle = decodeRecallHandle(recallHandle);

    c.registerTopicInUse(realTopic);
    (void)topicPublishInfo(c, realTopic);

    std::string addr = c.findBrokerAddressInPublish(handle.brokerName);
    if (addr.empty()) {
        addr = c.findBrokerAddrByTopic(realTopic);
    }
    if (addr.empty()) {
        logger_warn(std::string("can't find broker service address. ") + handle.brokerName);
        throw MQClientException("The broker service address not found");
    }

    RecallMessageRequestHeader header;
    header.producerGroup = producerGroup_;
    header.topic = realTopic;
    header.recallHandle = recallHandle;
    header.bname = handle.brokerName;
    return c.recallMessage(addr, header, sendMsgTimeout_);
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
