// Producer implementation (DefaultMQProducer, over the classic remoting protocol).
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

// Join broker names into "a, b, c"; used for brokersSent in send-failure messages
std::string joinStrings(const std::vector<std::string>& parts) {
    std::string out;
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) out += ", ";
        out += parts[i];
    }
    return out;
}

// Duration uses the monotonic clock: a local broker round trip may be under 1ms, and an integer
// millisecond delta records latency=0 in the fault table, so the broker is never caught by the latency threshold.
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

// Ensure the message carries UNIQ_KEY (a 32-hex-digit unique ID) before sending: generate only when
// missing, keep an existing one (idempotent). UNIQ_KEY is the client message ID in the classic remoting
// protocol; broker-side transaction check-back and tracing both correlate on it. The trace hook uses it
// to backfill UNIQ_KEY into SendResult.msgId, so the send-side trace msgId == UNIQ_KEY (not offsetMsgId).
void ensureUniqId(Message& msg) {
    std::string uniq = msg.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    if (uniq.empty()) {
        uniq = InnerIdGenerator::createUniqId();
        msg.putProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniq);
    }
}

// Initial permits for a backpressure semaphore: a config not above the floor uses the floor and logs
// info -- the branch is `cfg > floor ? max(cfg, floor) : floor`, so exactly the floor value also logs.
int64_t backPressurePermits(int32_t configured, int64_t floor, const char* name) {
    if (static_cast<int64_t>(configured) > floor) {
        return configured;
    }
    logger_info(std::string(name) + " can not be smaller than " + std::to_string(floor) + ".");
    return floor;
}

// Byte permits deducted: the body length, counted as 1 when empty; not charging empty bodies 1 means no throttling.
int64_t backPressureMsgLen(const Message& msg) {
    const Bytes& body = msg.getBody();
    return body.empty() ? 1 : static_cast<int64_t>(body.size());
}

// Byte permits deducted for a batched async send: summed per message, an empty body counts as 1, an
// empty batch counts as 1. Batch sending uses SEND_BATCH and encodes the whole batch into one body, so
// there is no ready-made batch length -- without summing per message, a batch occupies only 1 unit and the byte gate is a no-op for batches.
int64_t batchBackPressureMsgLen(const std::vector<Message>& msgs) {
    int64_t total = 0;
    for (const Message& m : msgs) {
        total += backPressureMsgLen(m);
    }
    return total == 0 ? 1 : total;
}

// The batching decision looks at the maximum of four delay properties (default 0): `DELAY`
// (getDelayTimeLevel) plus `TIMER_DELAY_MS` / `TIMER_DELAY_SEC` / `TIMER_DELIVER_MS`; all four
// property names are defined by the classic remoting protocol.
//
// A `MessageBatch` can carry only one delay property, so any value > 0 disables batching.
//
// ⚠ `MessageConst` provides those three constants (PROPERTY_TIMER_DELAY_SEC/MS/DELIVER_MS);
// the constants are used directly here, all four properties checked in one list.
//
// Value semantics: a missing property yields 0; invalid values are not swallowed -- number parse exceptions propagate to the caller.
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
    // The two fair semaphores are created in the constructor, not in start(): backpressure may be
    // enabled only at runtime, and the gate must already exist by then.
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
        // Destructors do not throw
    }
}

// ---------------------------------------------------------------- Configuration
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

// ---------------------------------------------------------------- Lifecycle
void DefaultMQProducer::start() {
    std::lock_guard<std::mutex> lk(lock_);
    if (started_) {
        return;
    }
    // The producer group is namespace-prefixed too (`setProducerGroup(withNamespace(producerGroup))`);
    // the broker registers the group under the prefixed name.
    // wrapNamespace has its own idempotent guard ("already prefixed, return unchanged"), so a repeated start does not nest two prefixes.
    if (!namespace_.empty()) {
        producerGroup_ = NamespaceUtil::wrapNamespace(namespace_, producerGroup_);
    }
    // start() validates configuration first (checkConfig): it runs after withNamespace (the group
    // name must be validated against the namespace-prefixed result), and it must reject
    // DEFAULT_PRODUCER -- processes sharing the default group kick each other offline.
    // ⚠ checkConfig is the first step of start(), so it precedes the name server address check:
    // with invalid configuration we must not report "no address set" first, nor create a client instance.
    Validators::checkGroup(producerGroup_);
    if (producerGroup_ == MixAll::DEFAULT_PRODUCER_GROUP) {
        throw MQClientException(
            "producerGroup can not equal DEFAULT_PRODUCER, please specify another one.");
    }
    // Either the static addresses or dynamic addressing (ROCKETMQ_NAMESRV_DOMAIN) must be available
    if (nameServerAddrs_.empty() && !DefaultTopAddressing::isConfigured()) {
        // Error code 10004 (the code validateNameServerSetting uses for this same fault):
        // this check is hoisted into start() (more predictable), otherwise the fault would only surface
        // as 10004 on the first send, but callers must not see two different codes.
        throw MQClientException("name server address is not set",
                                ClientErrorCode::NO_NAME_SERVER_EXCEPTION);
    }
    // Two startup steps: first `changeInstanceNameToPID` (this implementation has no internal
    // producer, so it runs unconditionally), then build the clientId:
    // `<local IP>@<instanceName>[@unitName][@STREAM]` -- the broker identifies client connections by this string.
    // instanceName is written back in place: a second start() reuses the same clientId.
    instanceName_ = changeInstanceNameToPID(instanceName_);
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_, unitName_, enableStreamRequestType_);
    }
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                         /*connectTimeoutMillis=*/3000,
                                         /*invokeTimeoutMillis=*/15000,
                                         tlsEnable_, unitName_,
                                         pollNameServerIntervalMillis_));
    // Broker VIP channel: forwarded to the instance that owns the broker RPCs (off = plain address).
    mqClient_->setVipChannelEnabled(vipChannelEnabled_);
    mqClient_->setDecodeReadBody(decodeReadBody_);
    mqClient_->setMqClientApiTimeout(mqClientApiTimeout_);
    mqClient_->setEnableConcurrentHeartbeat(enableConcurrentHeartbeat_);
    mqClient_->setConcurrentHeartbeatThreadPoolSize(concurrentHeartbeatThreadPoolSize_);
    // TLS strict-verification option (non-empty caCert = strict verification): forwarded to the instance and the underlying transport
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
    // Dynamic name server: the instance may already have resolved addresses from the address server at start-up; backfill them into this producer
    if (nameServerAddrs_.empty() && !mqClient_->nameServerAddrs().empty()) {
        nameServerAddrs_ = mqClient_->nameServerAddrs();
    }
    // Register the broker-initiated request handler: transaction check-back CHECK_TRANSACTION_STATE(39).
    // Without it, broker check-backs are dropped by the transport as "unknown request" and transactional messages stay UNKNOW forever.
    mqClient_->remotingClient().registerProcessor(
        RequestCode::CHECK_TRANSACTION_STATE,
        [this](const RemotingCommand& cmd, const std::string& addr)
            -> std::optional<RemotingCommand> {
            this->checkTransactionState(cmd, addr);
            return std::nullopt;
        });
    // The two async-send thread pools must be created before started_ is set, otherwise sendAsync
    // would see "started but the pools are null".
    createAsyncExecutors();
    started_ = true;
    logger_info("DefaultMQProducer[" + producerGroup_ + "] started, clientId=" + clientId_);

    initProduceAccumulator();
    if (produceAccumulator_ != nullptr) {
        produceAccumulator_->start();
    }

    // Message tracing: with enableTrace=true an AsyncTraceDispatcher is built and the Send/EndTransaction hooks registered.
    // This must finish before the heartbeat thread -- the dispatcher's internal producer first needs the trace topic route.
    startTraceDispatcher();

    RequestFutureHolder::getInstance().startScheduledTask();

    // Heartbeat thread: periodically registers ProducerData with every broker. Without it the broker cannot initiate transaction check-backs.
    heartbeatStop_.store(false);
    heartbeatThread_ = std::thread([this]() {
        setThreadName("ProducerHeartbeatThread");
        // Send once immediately after start: the broker registers the channel early, so the first transactional message does not miss the check-back window.
        // Then a fixed rate: each tick is anchored to heartbeatIntervalMillis_ (see schedule_util.h) --
        // "work, then sleep 30s in 100ms slices" adds tens of milliseconds per round and stretches the heartbeat period.
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
        // Detach the two pools first: subsequent sendAsync calls then see "producer already shutdown".
        // ⚠ started_ must flip only after the queues drain -- queued async sends still need client();
        //   setting it false early makes them fail with "producer not started", i.e. silently dropped messages.
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
    // Close the trace dispatcher first: it flushes the remaining queued trace records, then closes its internal producer.
    // This must happen before mqClient_->shutdown() -- flushing sends messages and needs its own transport.
    if (traceDispatcher_) {
        try {
            traceDispatcher_->shutdown();
        } catch (const std::exception& e) {
            logger_warn(std::string("trace dispatcher shutdown failed: ") + e.what());
        }
        traceDispatcher_.reset();
    }
    // Stop the heartbeat thread first (it holds mqClient_ references), then reclaim the other threads
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
    // This step joins the connection reader threads and the timeout-cleanup thread: afterwards no async callback can
    // reach this object, which makes the producer's destruction safe (executeOnCallbackThread runs inline once the pools are gone).
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

// ---------------------------------------------------------------- Message tracing / hooks
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
    // W3C traceparent propagation (opt-in): inject the root context when absent; never overwrite an existing value.
    // ⚠ Must run before the hasSendInterceptors() early return, otherwise injection is skipped when there are no hooks.
    if (enableTraceContext_) {
        injectTraceContext(const_cast<Message*>(&msg));
    }
    if (!hasSendInterceptors()) {
        // Zero-overhead passthrough when there are no hooks; client.sendMessage internally uses the publish address too and throws the same message on failure.
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
    // Delayed/timer messages are not batched: a MessageBatch can carry only one delay property
    if (maxDelayValue(msg) > 0) {
        return false;
    }
    // Retry messages are not batched: %RETRY% topics have special offset semantics
    if (msg.getTopic().rfind(MixAll::RETRY_GROUP_TOPIC_PREFIX, 0) == 0) {
        return false;
    }
    // Messages already carrying PGROUP (transaction half messages) are not batched: the broker looks the connection up by group
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

// ---------------- AccumulatorSender implementation ----------------

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
    // The accumulator's AggregateKey.topic is already namespace-prefixed (the producer's public
    // entry points apply withNamespace before dispatching); `wrapNamespace` is idempotent, so this step only covers the "call the sender directly" path.
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
    // isBatch => prepareForSend returns 0 immediately (batches are never compressed), and batch.body is already the
    // encoded result -- a second encode is neither needed nor desirable.
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
        // A 10004 already classified (no name server configured at all) must pass through unchanged, otherwise the two faults blur into one
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
    // The last failure's cause, used for error-code mapping after the loop ends
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
            // Record send latency; exceeding the threshold isolates this broker for a while
            mqFaultStrategy_.updateFaultItem(selected.brokerName, elapsedMillis(attemptBegan),
                                             false, true);
            if (result.sendStatus != SendStatus::SEND_OK && retryAnotherBrokerWhenNotStoreOK_) {
                continue;
            }
            return result;
        } catch (const MQBrokerException& e) {
            // The broker replied with an explicit error code: isolate it (reachability unchanged); only retryable codes switch to another broker
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
    // The selector sees the original message (topic/business fields); compression only affects the body
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
            *wrapped = error;  // Pass to the callback unchanged, no wrapping
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
        cores = 1;  // Assume 1 core when the CPU count is unavailable; never build a pool that cannot run tasks
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
    // Verify started state first (throw immediately instead of failing silently on a background thread)
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
    // Batch branch: batching only has a synchronous kernel in this implementation, so "async" means
    // sending one batch synchronously on the AsyncSenderExecutor thread and delivering the result on the callback thread. Caller-visible semantics are unchanged.
    // Validation, batch assembly, namespace prefixing, queue selection and hooks all live in the kernel; nothing is repeated here.
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
        // Kernel failures (route unavailable, validation failed, retries exhausted) are delivered to the callback as they are, matching single-message async.
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
    // Same reasoning as synchronous send: compression happens once outside the retry chain so a retry never re-compresses an already compressed body
    state->sysFlag = prepareForSend(state->msg);
    if (pinned != nullptr) {
        sendKernelAsync(state);
        return;
    }
    try {
        state->publish = topicPublishInfo(c, state->msg.topic);
    } catch (const MQClientException& e) {
        // Same as sendDefaultImpl: 10004 must not be rewritten into 10005, the async chain's classification must not diverge from the sync chain
        throw MQClientException(e.what(), e.responseCode == ClientErrorCode::NO_NAME_SERVER_EXCEPTION
                                               ? e.responseCode
                                               : ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION);
    }
    // ASYNC timesTotal is fixed at 1: the outer loop runs once, broker switching all happens inside onSendException
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
                                        // Callback handling moves to NettyClientPublicExecutor:
                                        // this is the connection reader / timeout-cleanup thread, no business code may run here
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
    // The timeout budget is the remaining time **shared by all attempts**, not a fresh budget per attempt
    state->timeout -= cost;
    onSendException(state, wrapped, needRetry);
}

void DefaultMQProducer::onSendException(const std::shared_ptr<AsyncSendState>& state,
                                        const InvokeError& error, bool needRetry) {
    state->times += 1;
    if (needRetry && state->times <= retryTimesWhenSendAsyncFailed_ && state->timeout > 0) {
        std::string retryBroker = state->brokerName;
        if (state->publish != nullptr) {
            // The third argument is false: avoid the just-failed broker via lastBrokerName
            MessageQueue selected = mqFaultStrategy_.selectOneMessageQueue(
                *state->publish, state->brokerName, false);
            if (!selected.brokerName.empty()) {
                state->mq = MessageQueue(state->msg.topic, selected.brokerName, selected.queueId);
                retryBroker = selected.brokerName;
            }
        }
        const std::string addr = client().findBrokerAddressInPublish(retryBroker);
        if (addr.empty()) {
            // The route no longer even has this broker's master address; retrying is pointless
            const InvokeError noBroker(InvokeError::Kind::OTHER,
                                       "The broker[" + retryBroker + "] not exist");
            completeAsync(state, nullptr, &noBroker);
            return;
        }
        logger_warn("async send msg by retry " + std::to_string(state->times) + " times. topic="
                    + state->msg.topic + ", brokerAddr=" + addr + ", brokerName=" + retryBroker
                    + ": " + error.message);
        state->brokerName = retryBroker;
        // Use a fresh opaque: the old request is still in the in-flight table waiting to time out; reusing it would cross the two attempts' responses
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
    // End of the async chain: run SendMessageHook.after first, then invoke the user callback
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
    // W3C traceparent propagation (opt-in): oneway sends inject it too
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
    // Confirm the route is known first, then send one more heartbeat: unless registered as a producer on
    // the broker, the broker cannot find the channel to push the reply back (REPLY_TO_CLIENT is looked up in ProducerManager's clientChannelTable).
    try {
        (void)topicPublishInfo(c, outbound.topic);
        sendHeartbeatToAllBroker();
    } catch (const std::exception& e) {
        // Without a route, let the send path below report its own error
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
    // The reply carries the full properties in MessageExt (CORRELATION_ID / REPLY_MESSAGE_ARRIVE_TIME etc.)
    return future->responseMessage();
}

// ---------------------------------------------------------------- Batch
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
    // MessageBatch has isBatch true, so prepareForSend returns 0 directly (no compression)
    const int32_t sysFlag = prepareForSend(batch);
    return sendWithHooks(c, batch, target, timeout, sysFlag);
}

// ---------------------------------------------------------------- Heartbeat
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
    hb.heartbeatFingerprint = 0;  // Take the V1 registration path, the safest
    hb.addProducerData(pd);

    int32_t okCount = 0;
    if (mqClient_->isEnableConcurrentHeartbeat()) {
        okCount = mqClient_->sendHeartbeatConcurrently(hb);
        if (okCount > 0) heartbeatCount_.fetch_add(okCount);
        return okCount;
    }
    for (const std::string& addr : addrs) {
        try {
            mqClient_->sendHeartbeat(addr, hb);
            ++okCount;
            heartbeatCount_.fetch_add(1);
        } catch (const std::exception& e) {
            logger_warn("producer heartbeat to " + addr + " failed: " + e.what());
        }
    }
    return okCount;
}

// ---------------------------------------------------------------- Fault-avoidance active detection
void DefaultMQProducer::detectorLoop() {
    setThreadName("FaultDetectorThread");
    auto next = std::chrono::steady_clock::now();
    // sleepUntilDeadline's first argument is the *stop* flag (false = keep sleeping, true = return
    // at once), the same convention as consumer's stop_ and mq_client's routeRefreshStop_. Passing the
    // running flag detectorRunning_ inverted the polarity: while running, sleepUntilDeadline returned
    // at once, the loop became an unthrottled hot poll, and next grew +3s per round until it was tens of
    // hours ahead of real time; after shutdown the thread took the "sleep until next" branch and blocked join for hours (the ctest hang).
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

// ---------------------------------------------------------------- Transactional messages
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
        // Check-back wrap-up: the offset / transaction id come from the broker's check-back request (sendResult is unusable here)
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

    // Half-message markers: the broker writes the message to RMQ_SYS_TRANS_HALF_TOPIC accordingly and waits for END_TRANSACTION
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

// ---------------------------------------------------------------- Query / administration
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
