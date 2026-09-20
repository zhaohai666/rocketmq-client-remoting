// 生产者实现（对应 Python client/producer.py 的 DefaultMQProducer）。
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

// 对应 Java 失败信息里的 Arrays.toString(brokersSent)
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

// 异步发送线程的递增序号，用于线程命名（对齐 Java 线程工厂 "AsyncSenderThread_" + n 的后缀）。
// 进程级递增，与 Java 的 ThreadFactoryImpl 计数器语义一致。
int nextAsyncSenderSeq() {
    static std::atomic<int> seq{0};
    return seq.fetch_add(1, std::memory_order_relaxed);
}

// 发送前确保消息带有 UNIQ_KEY（32 位十六进制唯一 ID），与 Java MessageClientIDSetter.setUniqID
// 对齐：缺失才生成，已存在则保留（幂等）。轨迹钩子用它在 SendResult.msgId 里回填 UNIQ_KEY，
// 从而让发送侧轨迹的 msgId == UNIQ_KEY（非 offsetMsgId）。
void ensureUniqId(Message& msg) {
    std::string uniq = msg.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    if (uniq.empty()) {
        uniq = InnerIdGenerator::createUniqId();
        msg.putProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniq);
    }
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
    // 生产者组也拼命名空间（对齐 Java DefaultMQProducer.start():375 / Python：
    // setProducerGroup(withNamespace(producerGroup))），broker 侧按带前缀的组名登记。
    // wrapNamespace 自带"已带前缀则原样返回"的幂等守卫，重复 start 不会套两层。
    if (!namespace_.empty()) {
        producerGroup_ = NamespaceUtil::wrapNamespace(namespace_, producerGroup_);
    }
    // 对应 Java DefaultMQProducerImpl.checkConfig(:295)：它排在 withNamespace 之后
    // （Java 也是 start() 先 withNamespace 再 impl.start()），并且要挡住
    // DEFAULT_PRODUCER —— 多进程共用默认组会互相踢下线。
    // ⚠ checkConfig 是 Java start() 的第一步，所以这里领先于 name server 地址检查：
    // 配置非法时不该先报"没配地址"，也不该建出客户端实例。
    Validators::checkGroup(producerGroup_);
    if (producerGroup_ == MixAll::DEFAULT_PRODUCER_GROUP) {
        throw MQClientException(
            "producerGroup can not equal DEFAULT_PRODUCER, please specify another one.");
    }
    // 静态地址与动态取址（ROCKETMQ_NAMESRV_DOMAIN）二选一必须可用
    if (nameServerAddrs_.empty() && !DefaultTopAddressing::isConfigured()) {
        throw MQClientException("name server address is not set");
    }
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_);
    }
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                         /*connectTimeoutMillis=*/3000,
                                         /*invokeTimeoutMillis=*/15000,
                                         tlsEnable_));
    mqClient_->start();
    // 动态 name server：实例启动时可能已从地址服务器拿到地址，回填到本生产者
    if (nameServerAddrs_.empty() && !mqClient_->nameServerAddrs().empty()) {
        nameServerAddrs_ = mqClient_->nameServerAddrs();
    }
    // ACL 鉴权钩子：必须在任何请求发出之前绑定（路由拉取、心跳都会带签名）。
    if (rpcHook_) {
        if (!mqClient_->registerRPCHook(rpcHook_)) {
            logger_warn("producer rpc hook ignored: MQClientInstance already has one (clientId="
                        + clientId_ + ")");
        }
    }
    // 注册 broker 主动请求处理器：事务回查 CHECK_TRANSACTION_STATE(39)。
    // 不注册的话 broker 回查会被传输层当成"未知请求"丢弃，事务消息永远停留在 UNKNOW。
    mqClient_->remotingClient().registerProcessor(
        RequestCode::CHECK_TRANSACTION_STATE,
        [this](const RemotingCommand& cmd, const std::string& addr)
            -> std::optional<RemotingCommand> {
            this->checkTransactionState(cmd, addr);
            // 不回响应：broker 用 invokeOneway 发回查，与 Java checkTransactionState 返回 null 一致。
            return std::nullopt;
        });
    started_ = true;
    logger_info("DefaultMQProducer[" + producerGroup_ + "] started, clientId=" + clientId_);

    // 消息轨迹：enableTrace=true 时建 AsyncTraceDispatcher 并注册 Send/EndTransaction 钩子。
    // 必须在心跳线程之前完成 —— 分发器内部生产者要先把轨迹 topic 的路由拉起来。
    startTraceDispatcher();

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
// 对应 Java DefaultMQProducerImpl 发送前的 Validators.checkMessage(msg, this)：
// 全部判定收敛到 Validators（topic blank/长度/字符表 → 禁发 topic → body 三档 →
// INNER_MULTI_DISPATCH 分隔符），文案与顺序以 python/rocketmq/client/validators.py 为准。
void DefaultMQProducer::checkMessage(const Message& msg) const {
    Validators::checkMessage(msg, maxMessageSize_);
}

Message DefaultMQProducer::withNamespace(const Message& msg) const {
    if (namespace_.empty()) return msg;
    Message out = msg;
    out.topic = NamespaceUtil::wrapNamespace(namespace_, msg.topic);
    return out;
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

// ---------------------------------------------------------------- 消息轨迹 / 钩子
//
// 对应 Java DefaultMQProducerImpl 的 sendMessageHookList / executeSendMessageHook*，
// 以及 DefaultMQProducer.start() 里的 traceDispatcher 装配（Java 5.x :380-405）。
// 钩子异常一律吞掉并记 warn —— 轨迹挂了绝不能影响正常收发。
SendMessageContext DefaultMQProducer::buildSendMessageContext(
    const Message& msg, const MessageQueue& mq, const std::string& brokerAddr) const {
    SendMessageContext context;
    context.producerGroup = producerGroup_;
    context.message = &msg;
    context.mq = mq;
    context.brokerAddr = brokerAddr;
    context.ns = namespace_;
    context.msgType = static_cast<int32_t>(TraceMessageType::NORMAL);
    // 判定顺序照抄 Java DefaultMQProducerImpl:975-990：
    //   TRAN_MSG == "true"      -> Trans_Msg_Half(1)
    //   带 DELAY 属性            -> Delay_Msg(3)（覆盖前者，与 Java 的两段 if 顺序一致）
    if (msg.getProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED) == "true") {
        context.msgType = static_cast<int32_t>(TraceMessageType::TRANS);
    }
    // Java 判的是 "属性存在"（null 判定）；C++ 的 getProperty 对缺失返回空串，
    // 故用非空判定 —— 值为空的 DELAY 属性本身也没有语义。
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
    // ⚠ 与 send/consume 钩子**相反**：这里不吞异常（Java 签名就是 throws MQClientException）。
    // 异常会沿 sendDefaultImpl 的重试链向上传播 —— 这正是"禁止发送"的实现方式。
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
    // 本项目无 unit mode（Java isUnitMode() 恒为 false）
    context.unitMode = false;
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
    // 没有任何拦截/钩子时零开销透传
    if (!hasSendInterceptors()) {
        return client.sendMessage(producerGroup_, msg, mq, timeout, sysFlag);
    }
    std::string brokerAddr;
    try {
        brokerAddr = client.brokerAddrOf(mq.brokerName);
    } catch (...) {
        brokerAddr.clear();
    }
    // 顺序严格照抄 Java sendKernelImpl:956-990：
    //   1. CheckForbiddenHook（每次尝试都跑；异常不吞）  2. SendMessageHook.before
    //   3. 发请求  4. SendMessageHook.after
    runCheckForbidden(msg, mq, brokerAddr, arg, mode);
    if (sendMessageHookList_.empty()) {
        return client.sendMessage(producerGroup_, msg, mq, timeout, sysFlag);
    }
    SendMessageContext context = buildSendMessageContext(msg, mq, brokerAddr);
    executeSendMessageHookBefore(context);

    SendResult result;
    try {
        result = client.sendMessage(producerGroup_, msg, mq, timeout, sysFlag);
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
        dispatcher->setHostProducer(this);
        dispatcher->setHostClientId(clientId_);
        dispatcher->start(getNamesrvAddr());
        traceDispatcher_ = dispatcher;
        // 顺序与 Java DefaultMQProducer.start() 一致：先 SendMessageTraceHook，再 EndTransactionTraceHook
        registerSendMessageHook(std::make_shared<SendMessageTraceHook>(dispatcher.get()));
        registerEndTransactionHook(std::make_shared<EndTransactionTraceHook>(dispatcher.get()));
        logger_info("producer trace enabled, traceTopic=" + dispatcher->getTraceTopicName());
    } catch (const std::exception& e) {
        // 轨迹挂了不能影响正常发送（对齐 Java 的 try/catch 语义）
        logger_warn(std::string("start trace dispatcher failed: ") + e.what());
    }
}

// ---------------------------------------------------------------- 同步发送
// 逐条对齐 Java DefaultMQProducerImpl#sendDefaultImpl：
//   * timesTotal = 1 + retryTimesWhenSendFailed（只有同步发送有重试）；
//   * 每次尝试先算 costTime，总超时已用完则整体放弃（→ RemotingTooMuchRequestException）；
//     还剩重试机会时，单次请求超时被 sendMsgMaxTimeoutPerRequest 压住，把余量留给后面的 broker；
//   * 异常按类型分档写容错表，且只有 retryResponseCodes 里的 broker 响应码才继续重试；
//   * 全部失败时把原因映射成 ClientErrorCode 塞进 MQClientException。
SendResult DefaultMQProducer::send(const Message& msg, int32_t timeoutMillis) {
    MQClientInstance& c = client();
    const int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    checkMessage(msg);
    Message outbound = withNamespace(msg);
    ensureUniqId(outbound);
    const int32_t sysFlag = prepareForSend(outbound);

    // 路由完全取不到时 Java 在循环外就抛 NOT_FOUND_TOPIC，不会把重试次数空转掉。
    // 与 Java 一致：整条重试链只用这一份发布信息，中途路由刷新不会换掉候选队列。
    std::shared_ptr<TopicPublishInfo> publish;
    try {
        publish = c.getTopicPublishInfo(outbound.topic, /*isDefault=*/true);
    } catch (const MQClientException& e) {
        throw MQClientException(e.what(), ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION);
    }

    const int32_t timesTotal = retryTimesWhenSendFailed_ + 1;
    const int64_t beginFirst = UtilAll::currentTimeMillis();
    std::vector<std::string> brokersSent;
    std::string lastBrokerName;
    SendResult result;
    bool gotResult = false;
    bool callTimeout = false;
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
            // 故障规避：开启时按 broker 延迟/隔离状态选队列（Java MQFaultStrategy）；
            // 关闭时退化为普通轮询（策略内部判断）。重试时 resetIndex 让轮询从头开始，
            // 从而能避开 lastBrokerName 选到别的 broker。
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
            // Java：非 SEND_OK 且开了 retryAnotherBrokerWhenNotStoreOK 才换 broker，
            // 否则把这个"存了但没存好"的结果原样返回
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
            // 连不上/超时/发不出去：隔离该 broker。本项目无后台可达性探测线程，
            // 所以 Java 的 reachable=!isStartDetectorEnable() 恒为 true。
            if (!selected.brokerName.empty()) {
                mqFaultStrategy_.updateFaultItem(selected.brokerName,
                                                 elapsedMillis(attemptBegan), true, true);
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
            // 客户端自己的问题（选不到队列、路由没了…）：Java 同样只记延迟、不隔离
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
    MQClientInstance& c = client();
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    checkMessage(msg);
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
    std::shared_ptr<TopicPublishInfo> publish =
        c.getTopicPublishInfo(outbound.topic, /*isDefault=*/true);
    MessageQueue selected = selector.select(publish->msgQueueList, outbound, arg);
    // 选择器用的是原始消息（topic/业务字段），压缩只影响 body
    const int32_t sysFlag = prepareForSend(outbound);
    // arg 透传给 CheckForbiddenHook（Java CheckForbiddenContext.arg 就是它）
    return sendWithHooks(c, outbound, selected, timeout, sysFlag, &arg, CommunicationMode::SYNC);
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
    Message outbound = withNamespace(msg);
    std::shared_ptr<TopicPublishInfo> publish =
        c.getTopicPublishInfo(outbound.topic, /*isDefault=*/true);
    MessageQueue selected = publish->selectOneMessageQueue();
    const int32_t sysFlag = prepareForSend(outbound);
    // 单向发送在 Java 里同样走 sendKernelImpl → CheckForbiddenHook 必须生效
    std::string brokerAddr;
    try {
        brokerAddr = c.brokerAddrOf(selected.brokerName);
    } catch (...) {
        brokerAddr.clear();
    }
    runCheckForbidden(outbound, selected, brokerAddr, nullptr, CommunicationMode::ONEWAY);
    // W3C traceparent 透传（opt-in）：单向发送同样注入
    if (enableTraceContext_) {
        injectTraceContext(&outbound);
    }
    c.sendMessageOneway(producerGroup_, outbound, selected, sendMsgTimeout_, sysFlag);
}

// ---------------------------------------------------------------- Request-Reply

namespace {

// RAII：无论正常返回还是异常，都把等待槽摘掉（对应 Java request() 的 finally remove）。
// putResponse 已经先原子 remove 了，这里再摘一次是幂等的。
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
    std::shared_ptr<TopicPublishInfo> publish =
        c.getTopicPublishInfo(outbound.topic, /*isDefault=*/true);
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

    // 请求方三件事（对齐 Java prepareSendRequest / DefaultMQProducerImpl#request）：
    // CORRELATION_ID（随机 UUID）、REPLY_TO_CLIENT（本客户端 clientId）、TTL（= timeout）。
    // REPLY_TO_CLIENT 是 broker 反查 channel 的键 —— 撞名会把应答推到别人的连接上。
    const std::string correlationId = createCorrelationId();
    outbound.putProperty(MessageConst::PROPERTY_CORRELATION_ID, correlationId);
    outbound.putProperty(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT, clientId_);
    outbound.putProperty(MessageConst::PROPERTY_MESSAGE_TTL, std::to_string(timeout));

    const int64_t begin = UtilAll::currentTimeMillis();
    // 先确认路由已知，再补一次心跳：没在 broker 上登记为 producer，broker 就找不到
    // channel 把应答推回来（REPLY_TO_CLIENT 反查 ProducerManager 的 clientChannelTable）。
    try {
        (void)c.getTopicPublishInfo(outbound.topic, /*isDefault=*/true);
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
        c.sendMessage(producerGroup_, outbound, mq, timeout, sysFlag);
    } catch (...) {
        // 发送失败三件事：标 !sendRequestOk + 空唤醒（别让等待方白等满 timeout）+ 记 cause。
        // 与 Java 的匿名 SendCallback#onException 完全一致。
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
    MQClientInstance& c = client();
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : sendMsgTimeout_;
    if (msgs.empty()) {
        throw MQClientException("message list is empty");
    }
    // 对应 Java DefaultMQProducer.batch() / Python _send_batch：**每条子消息**都过一遍
    // Validators.checkMessage（在拼命名空间之前、用原始 topic），再 MessageBatch
    // .generateFromList 查同质性。少这一步等于批量路径绕过了所有本地校验——
    // 超长/空 body/非法 topic 都能发出去。
    for (const Message& m : msgs) {
        Validators::checkMessage(m, maxMessageSize_);
    }
    MessageBatch batch = MessageBatch::generateFromList(msgs);
    checkMessage(batch);
    if (!namespace_.empty()) {
        batch.topic = NamespaceUtil::wrapNamespace(namespace_, batch.topic);
    }
    std::shared_ptr<TopicPublishInfo> publish =
        c.getTopicPublishInfo(batch.topic, /*isDefault=*/true);
    MessageQueue selected = publish->selectOneMessageQueue();
    // MessageBatch 的 isBatch 为 true，prepareForSend 会直接返回 0（不压缩）
    const int32_t sysFlag = prepareForSend(batch);
    return sendWithHooks(c, batch, selected, timeout, sysFlag);
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

    // 事务收尾轨迹（对应 Java DefaultMQProducerImpl:1561 / :442 的 doExecuteEndTransactionHook）：
    // 客户端主动提交与 broker 回查两条路径都会产生 EndTransaction 轨迹。
    // msgId 取 header 里的（本地路径 = SendResult.msgId 即 UNIQ_KEY；回查路径 = 消息 UNIQ_KEY）；
    // transactionId 取消息自身的（Java 用的是 msg.getTransactionId()，不是 header 的）。
    executeEndTransactionHook(msg, addr, header.msgId.value_or(std::string()),
                              msg.transactionId, localTransactionStateName(state), fromCheck);
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
    Message outbound = withNamespace(msg);
    outbound.putProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED, "true");
    outbound.putProperty(MessageConst::PROPERTY_PRODUCER_GROUP, producerGroup_);
    txListener_ = &listener;

    std::shared_ptr<TopicPublishInfo> publish =
        c.getTopicPublishInfo(outbound.topic, /*isDefault=*/true);
    MessageQueue selected = publish->selectOneMessageQueue();

    // 压缩与普通发送一致；再叠加事务类型位（Java sendKernelImpl 检测 TRAN_MSG 后置 PREPARED）
    ensureUniqId(outbound);   // 对齐 Java sendKernelImpl：非批量消息发送前注入 UNIQ_KEY
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
    std::shared_ptr<TopicPublishInfo> publish =
        c.getTopicPublishInfo(realTopic, /*isDefault=*/true);
    return publish->msgQueueList;
}

void DefaultMQProducer::createTopic(const std::string& key, const std::string& newTopic,
                                    int32_t queueNum) {
    MQClientInstance& c = client();
    // 对应 Java DefaultMQProducerImpl.createTopic：先 checkTopic（blank/长度/字符表），
    // 再 isSystemTopic —— 建与 broker 内部资源重名的 topic 会静默篡改系统流水。
    (void)key;
    Validators::checkTopic(newTopic);
    Validators::isSystemTopic(newTopic);
    constexpr int32_t perm = 6;  // PERM_READ | PERM_WRITE
    const std::string realTopic = namespace_.empty() ? newTopic : NamespaceUtil::wrapNamespace(namespace_, newTopic);
    c.createTopicInRoute(realTopic, queueNum, queueNum, perm);
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
