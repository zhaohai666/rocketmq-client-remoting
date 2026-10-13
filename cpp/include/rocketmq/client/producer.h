// 能力覆盖：同步发送（轮询选队列 / 定点发送）、按选择器发送（顺序消息）、
// 异步发送、单向发送、批量发送、事务消息（两阶段：半消息 → 本地事务 → END_TRANSACTION → broker 回查）、
// **自动攒批（autoBatch + ProduceAccumulator，见 client/produce_accumulator.h）**、
// 按 Key 查询、
// offset 查询、建 topic。
#ifndef ROCKETMQ_CLIENT_PRODUCER_H
#define ROCKETMQ_CLIENT_PRODUCER_H

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/hook.h"
#include "rocketmq/client/backpressure.h"
#include "rocketmq/client/consume_executor.h"
#include "rocketmq/client/latency.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/produce_accumulator.h"
#include "rocketmq/client/request_reply.h"
#include "rocketmq/client/result.h"
#include "rocketmq/client/trace_context.h"
#include "rocketmq/client/trace_dispatcher.h"
#include "rocketmq/common/compression.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/namespace_util.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

namespace rocketmq {

class DefaultMQProducer : public AccumulatorSender {
public:
    explicit DefaultMQProducer(const std::string& producerGroup = MixAll::DEFAULT_PRODUCER_GROUP);

    DefaultMQProducer(const DefaultMQProducer&) = delete;
    DefaultMQProducer& operator=(const DefaultMQProducer&) = delete;
    virtual ~DefaultMQProducer();

    // ---------------- 配置 ----------------
    // 分号分隔的 nameServer 地址串，如 "127.0.0.1:9876"
    void setNamesrvAddr(const std::string& addr);
    void setNameServerAddresses(const std::vector<std::string>& addrs);
    std::string getNamesrvAddr() const;
    void setInstanceName(const std::string& name) { instanceName_ = name; }
    void setTlsEnable(bool b) { tlsEnable_ = b; }
    // TLS 严格校验选项（caCert 非空 = 证书链 + 主机名严格校验；clientCert/Key = mTLS）。
    // 调用后自动开启 TLS；须在 start() 之前设置。
    void setTlsOptions(const TlsOptions& opts) {
        tlsOptions_ = opts;
        tlsOptionsSet_ = true;
        tlsEnable_ = true;
    }
    void setSocksProxy(const SocksProxyConfig& proxy) {
        socksProxy_ = proxy;
        socksProxySet_ = true;
    }
    // W3C traceparent 透传（opt-in；缺省读 env ROCKETMQ_TRACE_CONTEXT_ENABLE）
    void setEnableTraceContext(bool b) { enableTraceContext_ = b; }
    void setSendMsgTimeout(int32_t millis) { sendMsgTimeout_ = millis; }
    void setRetryTimesWhenSendFailed(int32_t n) { retryTimesWhenSendFailed_ = n; }
    void setRetryAnotherBrokerWhenNotStoreOK(bool b) { retryAnotherBrokerWhenNotStoreOK_ = b; }
    bool isRetryAnotherBrokerWhenNotStoreOK() const { return retryAnotherBrokerWhenNotStoreOK_; }
    void setSendMsgMaxTimeoutPerRequest(int32_t millis) { sendMsgMaxTimeoutPerRequest_ = millis; }
    int32_t getSendMsgMaxTimeoutPerRequest() const { return sendMsgMaxTimeoutPerRequest_; }
    void addRetryResponseCode(int32_t responseCode) { retryResponseCodes_.insert(responseCode); }
    const std::set<int32_t>& getRetryResponseCodes() const { return retryResponseCodes_; }
    bool isRetryResponseCode(int32_t responseCode) const {
        return retryResponseCodes_.count(responseCode) > 0;
    }
    void setMaxMessageSize(int32_t bytes) { maxMessageSize_ = bytes; }
    void setAutoBatch(bool autoBatch) { autoBatch_ = autoBatch; }
    bool getAutoBatch() const;
    // 单个批次的最长攒批时间，合法区间 (0, 30s]（默认 10ms）
    void setBatchMaxDelayMs(int32_t holdMs);
    int32_t getBatchMaxDelayMs() const;
    // 单个批次的 body 上限，合法区间 (0, 2MB]（默认 32KB）
    void setBatchMaxBytes(int64_t holdSize);
    int64_t getBatchMaxBytes() const;
    // 累加器的在途总量上限，必须 > 0（默认 32MB）
    void setTotalBatchMaxBytes(int64_t totalHoldBytes);
    int64_t getTotalBatchMaxBytes() const;
    void initProduceAccumulator();
    std::shared_ptr<ProduceAccumulator> produceAccumulator() const { return produceAccumulator_; }

    void setRetryTimesWhenSendAsyncFailed(int32_t n) { retryTimesWhenSendAsyncFailed_ = n; }
    int32_t getRetryTimesWhenSendAsyncFailed() const { return retryTimesWhenSendAsyncFailed_; }
    void setAsyncSenderQueueCapacity(int32_t n) { asyncSenderQueueCapacity_ = n; }
    int32_t getAsyncSenderQueueCapacity() const { return asyncSenderQueueCapacity_; }
    void setEnableBackpressureForAsyncMode(bool b) { enableBackpressureForAsyncMode_ = b; }
    bool isEnableBackpressureForAsyncMode() const { return enableBackpressureForAsyncMode_; }
    void setBackPressureForAsyncSendNum(int32_t num);
    int32_t getBackPressureForAsyncSendNum() const { return backPressureForAsyncSendNum_; }
    void setBackPressureForAsyncSendSize(int32_t size);
    int32_t getBackPressureForAsyncSendSize() const { return backPressureForAsyncSendSize_; }
    int64_t getSemaphoreAsyncSendNumAvailablePermits() const;
    int64_t getSemaphoreAsyncSendSizeAvailablePermits() const;
    void setClientCallbackExecutorThreads(int32_t n) { clientCallbackExecutorThreads_ = n; }
    int32_t getClientCallbackExecutorThreads() const { return clientCallbackExecutorThreads_; }
    void setDefaultTopicQueueNums(int32_t n) { defaultTopicQueueNums_ = n; }
    void setCreateTopicKey(const std::string& key) { createTopicKey_ = key; }
    void setProducerGroup(const std::string& g);
    void setNamespace(const std::string& ns) { namespace_ = ns; }
    const std::string& namespaceOf() const { return namespace_; }
    void setNamespaceV2(const std::string& ns) { namespaceV2_ = ns; }
    const std::string& namespaceV2() const { return namespaceV2_; }

    void setUnitName(const std::string& unitName) { unitName_ = unitName; }
    const std::string& unitName() const { return unitName_; }
    void setUnitMode(bool unitMode) { unitMode_ = unitMode; }
    bool isUnitMode() const { return unitMode_; }
    void setEnableStreamRequestType(bool enable) { enableStreamRequestType_ = enable; }
    bool isEnableStreamRequestType() const { return enableStreamRequestType_; }
    void setPollNameServerIntervalMillis(int32_t millis) { pollNameServerIntervalMillis_ = millis; }
    int32_t pollNameServerIntervalMillis() const { return pollNameServerIntervalMillis_; }

    void setSendLatencyFaultEnable(bool enable) { mqFaultStrategy_.setSendLatencyFaultEnable(enable); }
    bool isSendLatencyFaultEnable() const { return mqFaultStrategy_.isSendLatencyFaultEnable(); }
    MQFaultStrategy& mqFaultStrategy() { return mqFaultStrategy_; }

    void setStartDetectorEnable(bool enable) { mqFaultStrategy_.setStartDetectorEnable(enable); }
    bool isStartDetectorEnable() const { return mqFaultStrategy_.isStartDetectorEnable(); }
    void setDetectTimeout(int32_t millis) {
        mqFaultStrategy_.latencyFaultTolerance().setDetectTimeout(millis);
    }
    int32_t getDetectTimeout() const {
        return mqFaultStrategy_.latencyFaultTolerance().detectTimeout();
    }
    void setDetectInterval(int32_t millis) {
        mqFaultStrategy_.latencyFaultTolerance().setDetectInterval(millis);
    }
    int32_t getDetectInterval() const {
        return mqFaultStrategy_.latencyFaultTolerance().detectInterval();
    }

    void setRPCHook(std::shared_ptr<RPCHook> hook) { rpcHook_ = std::move(hook); }
    // 便捷入口：用 accessKey/secretKey（可选 securityToken）构造 AclClientRPCHook。
    void setCredentials(const std::string& accessKey, const std::string& secretKey,
                        const std::string& securityToken = std::string()) {
        rpcHook_ = std::make_shared<AclClientRPCHook>(
            SessionCredentials(accessKey, secretKey, securityToken));
    }
    const std::shared_ptr<RPCHook>& rpcHook() const { return rpcHook_; }

    void setCompressMsgBodyOverHowmuch(int32_t bytes) { compressMsgBodyOverHowmuch_ = bytes; }
    int32_t getCompressMsgBodyOverHowmuch() const { return compressMsgBodyOverHowmuch_; }
    void setCompressLevel(int32_t level) { compressLevel_ = level; }
    int32_t getCompressLevel() const { return compressLevel_; }
    void setCompressType(int32_t type) { compressType_ = type; }
    int32_t getCompressType() const { return compressType_; }

    void setEnableTrace(bool enable) { enableTrace_ = enable; }
    bool isEnableTrace() const { return enableTrace_; }
    // 自定义轨迹 topic（默认 RMQ_SYS_TRACE_TOPIC）
    void setTraceTopic(const std::string& topic) { traceTopic_ = topic; }
    const std::string& getTraceTopic() const { return traceTopic_; }
    void setTraceMsgBatchNum(int32_t n) { traceMsgBatchNum_ = n; }
    int32_t getTraceMsgBatchNum() const { return traceMsgBatchNum_; }

    void registerSendMessageHook(std::shared_ptr<SendMessageHook> hook) {
        if (hook) sendMessageHookList_.push_back(std::move(hook));
    }
    void registerEndTransactionHook(std::shared_ptr<EndTransactionHook> hook) {
        if (hook) endTransactionHookList_.push_back(std::move(hook));
    }
    bool hasSendMessageHook() const { return !sendMessageHookList_.empty(); }
    size_t sendMessageHookCount() const { return sendMessageHookList_.size(); }

    void registerCheckForbiddenHook(std::shared_ptr<CheckForbiddenHook> hook) {
        if (hook) checkForbiddenHookList_.push_back(std::move(hook));
    }
    bool hasCheckForbiddenHook() const { return !checkForbiddenHookList_.empty(); }
    size_t checkForbiddenHookCount() const { return checkForbiddenHookList_.size(); }
    // 不需要走「带拦截/钩子」发送内核时返回 false（零开销快路径）
    bool hasSendInterceptors() const {
        return !sendMessageHookList_.empty() || !checkForbiddenHookList_.empty();
    }
    void executeCheckForbiddenHook(CheckForbiddenContext& context);
    // 轨迹分发器（未开轨迹时为空），供联调脚本读取丢弃计数等状态
    std::shared_ptr<AsyncTraceDispatcher> traceDispatcher() const { return traceDispatcher_; }

    const std::string& producerGroup() const { return producerGroup_; }
    const std::string& clientId() const { return clientId_; }
    int32_t sendMsgTimeout() const { return sendMsgTimeout_; }
    int32_t maxMessageSize() const { return maxMessageSize_; }
    bool isStarted() const { return started_; }

    bool canBatch(const Message& msg) const;

    std::optional<SendResult> sendDirect(const Message& msg, const MessageQueue* mq,
                                         std::shared_ptr<SendCallback> callback);

    std::optional<SendResult> sendByAccumulator(const Message& msg, const MessageQueue* mq,
                                                std::shared_ptr<SendCallback> callback);

    SendResult sendDirectBlocking(const MessageBatch& batch, const MessageQueue* mq) override;
    void sendDirectAsync(const MessageBatch& batch, const MessageQueue* mq,
                         std::shared_ptr<SendCallback> callback) override;

    // ---------------- 生命周期 ----------------
    void start();
    void shutdown();

    MQClientInstance& client();

    // ---------------- 同步发送 ----------------
    // 不指定队列：轮询选择，失败按 retryTimesWhenSendFailed 重试
    SendResult send(const Message& msg, int32_t timeoutMillis = -1);
    // 定点发送到指定队列
    SendResult send(const Message& msg, const MessageQueue& mq, int32_t timeoutMillis = -1);

    // 按选择器发送（顺序消息：同一 arg 落到同一队列）
    SendResult sendBySelector(const Message& msg, const MessageQueueSelector& selector,
                              const std::string& arg, int32_t timeoutMillis = -1);

    void sendAsync(const Message& msg, std::shared_ptr<SendCallback> callback,
                   int32_t timeoutMillis = -1);
    void sendAsync(const Message& msg, const MessageQueue& mq, std::shared_ptr<SendCallback> callback,
                   int32_t timeoutMillis = -1);
    void sendOneway(const Message& msg);
    void sendOneway(const Message& msg, const MessageQueue& mq);

    void setRequestTimeout(int32_t millis) { requestTimeoutMillis_ = millis; }
    int32_t requestTimeout() const { return requestTimeoutMillis_; }
    // 不指定队列：轮询选择
    Message request(const Message& msg, int32_t timeoutMillis = -1);
    // 定点发送请求到指定队列
    Message request(const Message& msg, const MessageQueue& mq, int32_t timeoutMillis = -1);

    // ---------------- 批量 ----------------
    SendResult sendBatch(const std::vector<Message>& msgs, int32_t timeoutMillis = -1);
    SendResult sendBatch(const std::vector<Message>& msgs, const MessageQueue& mq,
                         int32_t timeoutMillis = -1);

    // SendMessageHook（before/after）**照跑**：复用的同步批量内核内部就是 sendWithHooks，
    // 每轮尝试一个 context。差别只在重试语义——这一条链走的
    // 是同步内核的 retryTimesWhenSendFailed 循环，不是异步链 onSendException 的换 broker
    // 重试；所以 completeAsync 不再重复跑 after（context 归内核自己收尾）。
    //
    // 背压闸与单条异步同口径，只是「字节」许可按整批每条 body 长度累加（空 body 也算 1，
    // 整批为空算 1），见 batchBackPressureMsgLen。未 start() 时同样是同步抛。
    void sendBatchAsync(const std::vector<Message>& msgs, std::shared_ptr<SendCallback> callback,
                        int32_t timeoutMillis = -1);
    void sendBatchAsync(const std::vector<Message>& msgs, const MessageQueue& mq,
                        std::shared_ptr<SendCallback> callback, int32_t timeoutMillis = -1);

    TransactionSendResult sendMessageInTransaction(const Message& msg, TransactionListener& listener,
                                                   const std::string& arg = std::string());

    // ---------------- 查询 / 管理 ----------------
    std::vector<MessageExt> queryMessage(const std::string& topic, const std::string& key,
                                         int32_t maxNum, int64_t beginTimestamp,
                                         int64_t endTimestamp);
    std::vector<MessageQueue> fetchPublishMessageQueues(const std::string& topic);

    std::string recallMessage(const std::string& topic, const std::string& recallHandle);
    void createTopic(const std::string& key, const std::string& newTopic, int32_t queueNum = 4);
    int64_t searchOffset(const MessageQueue& mq, int64_t timestamp);
    int64_t maxOffset(const MessageQueue& mq);
    int64_t minOffset(const MessageQueue& mq);

protected:
    void checkMessage(const Message& msg) const;
    SendResult sendDefaultImpl(const Message& msg, int32_t timeoutMillis);
    SendResult sendToMqImpl(const Message& msg, const MessageQueue& mq, int32_t timeoutMillis);
    void sendAsyncImpl(const Message& msg, std::shared_ptr<SendCallback> callback,
                       int32_t timeoutMillis);
    void sendAsyncToMqImpl(const Message& msg, const MessageQueue& mq,
                           std::shared_ptr<SendCallback> callback, int32_t timeoutMillis);
    Message withNamespace(const Message& msg) const;
    void checkPinnedTopic(const std::string& msgTopic, const MessageQueue& mq,
                          const std::string& message) const;
    // request() 的公共收尾（两个公开重载都会走到这里；outbound 已过 withNamespace/checkMessage）
    Message requestWithQueue(Message& outbound, const MessageQueue& mq, int32_t timeout);
    Message waitRequestResponse(const Message& outbound, int32_t timeout,
                                const std::shared_ptr<RequestResponseFuture>& future,
                                int64_t costMillis);
    int32_t prepareForSend(Message& msg) const;

    std::shared_ptr<TopicPublishInfo> topicPublishInfo(MQClientInstance& client,
                                                      const std::string& topic);
    void validateNameServerSetting(MQClientInstance& client);

    SendResult sendBatchKernel(const std::vector<Message>& msgs, const MessageQueue* pinned,
                               int32_t timeoutMillis, const std::string& pinnedGuardMessage);
    SendResult sendPreparedBatch(const MessageBatch& batch, const MessageQueue* pinned,
                                 int32_t timeoutMillis);
    SendResult sendWithHooks(MQClientInstance& client, const Message& msg,
                             const MessageQueue& mq, int32_t timeout, int32_t sysFlag,
                             const std::string* arg = nullptr,
                             CommunicationMode mode = CommunicationMode::SYNC);
    void executeSendMessageHookBefore(SendMessageContext& context);
    void executeSendMessageHookAfter(SendMessageContext& context);
    // 构造 CheckForbiddenContext 并执行（每次发送尝试都会调一次，含重试）
    void runCheckForbidden(const Message& msg, const MessageQueue& mq,
                           const std::string& brokerAddr, const std::string* arg,
                           CommunicationMode mode);
    void executeEndTransactionHook(const Message& msg, const std::string& brokerAddr,
                                   const std::string& msgId, const std::string& transactionId,
                                   const std::string& transactionState, bool fromTransactionCheck);
    // start() 里按 enableTrace 建分发器并注册钩子；任何异常只记日志，不影响正常发送。
    void startTraceDispatcher();
    // 发轨迹前的公共上下文（brokerAddr 用消息将要落到的 broker 地址）
    SendMessageContext buildSendMessageContext(const Message& msg, const MessageQueue& mq,
                                               const std::string& brokerAddr) const;

    struct AsyncSendState {
        Message msg;                                   // 已套 namespace、已压缩
        std::shared_ptr<TopicPublishInfo> publish;     // nullptr = 定点发送，不换 broker
        RemotingCommand request;                       // 跨重试复用的那一份请求
        MessageQueue mq;                               // 当前尝试要打的队列
        std::string brokerName;                        // 当前尝试的 broker（重选时避开）
        std::shared_ptr<SendCallback> callback;
        std::shared_ptr<SendMessageContext> context;   // nullptr = 无发送钩子，不建上下文
        int32_t sysFlag = 0;                           // 压缩位，整条链只算一次
        int32_t timeout = 0;                           // **剩余**预算（每轮扣掉已花掉的）
        int32_t times = 0;
        bool pinned = false;                           // 定点发送：重试不换 broker
        bool numAcquired = false;                      // 已拿到 1 份「条数」许可
        bool sizeAcquired = false;                     // 已拿到 msgLen 份「字节」许可
        bool permitsReleased = false;                  // 只归还一次（链上多条终点会重复收尾）
        int64_t msgLen = 1;                            // 扣字节许可的份数，**压缩前**的 body 长度
        // ---- 批量异步（sendBatchAsync）----
        // batch=true 时这条链不建 request/publish/context，而是把 batchMsgs 交给同步批量内核
        // （见 sendBatchAsyncInner）。msg 留空，重试链自然不参与。
        bool batch = false;
        std::vector<Message> batchMsgs;
        std::chrono::steady_clock::time_point attemptBegan;  // 本次尝试起点（单调钟）
    };

    // start() 里建 AsyncSenderExecutor + NettyClientPublicExecutor 两个池。
    void createAsyncExecutors();
    void enqueueAsync(const std::shared_ptr<AsyncSendState>& state, const MessageQueue* pinned,
                      int32_t timeoutMillis);
    void executeAsyncMessageSend(const std::shared_ptr<AsyncSendState>& state,
                                 const std::function<void()>& runnable, int32_t timeout,
                                 const std::chrono::steady_clock::time_point& began,
                                 const std::shared_ptr<ConsumeExecutor>& pool);
    void releaseBackPressure(const std::shared_ptr<AsyncSendState>& state);
    void sendAsyncInner(const std::shared_ptr<AsyncSendState>& state, const MessageQueue* pinned);
    // 批量分支：在
    // AsyncSenderExecutor 线程里跑同步批量内核，结果挪到回调线程交付。
    void sendBatchAsyncInner(const std::shared_ptr<AsyncSendState>& state,
                             const MessageQueue* pinned);
    // 建请求 + before 钩子，然后发出第一笔尝试
    void sendKernelAsync(const std::shared_ptr<AsyncSendState>& state);
    void sendAttempt(const std::shared_ptr<AsyncSendState>& state, const std::string& addr);
    void onTransportThrow(const std::shared_ptr<AsyncSendState>& state, const std::string& message,
                          InvokeError::Kind kind);
    // 一笔尝试的结局（在 NettyClientPublicExecutor_N 上跑）：记容错表，然后要么收尾要么重试
    void onAttemptComplete(const std::shared_ptr<AsyncSendState>& state, const SendResult& result,
                           const InvokeError& error);
    void onSendException(const std::shared_ptr<AsyncSendState>& state, const InvokeError& error,
                         bool needRetry);
    // 链的终点：先 after 钩子，再回调用户（用户回调抛的异常吞掉，不能带走回调线程）
    void completeAsync(const std::shared_ptr<AsyncSendState>& state, const SendResult* result,
                       const InvokeError* error);
    void executeOnCallbackThread(std::function<void()> fn);

    void endTransaction(const Message& msg, const SendResult& sendResult,
                        LocalTransactionState state, bool hasLocalException,
                        const std::string& localExceptionText, bool fromCheck,
                        const CheckTransactionStateRequestHeader* checkHeader,
                        const MessageExt* checkMsg, const std::string& brokerAddr);
    // broker 主动发起的事务回查（CHECK_TRANSACTION_STATE=39）入口，由传输层回调。
    void checkTransactionState(const RemotingCommand& cmd, const std::string& addr);

    // 向所有已知 broker 发一次心跳（含 ProducerData）。
    int32_t sendHeartbeatToAllBroker();

    void detectorLoop();
    std::string pickDetectTopic();
    void rememberDetectTopic(const std::string& topic);
    bool detectBrokerByMaxOffset(const std::string& addr, int32_t timeoutMillis);

    TransactionListener* txListener_ = nullptr;    // 回查处理线程句柄，shutdown 时统一 join 回收
    std::vector<std::thread> txThreads_;
    std::mutex txThreadsMutex_;

    std::thread heartbeatThread_;
    // 停止标志（false=运行中，true=已停），与 sleepUntilDeadline(stop, ...) 的契约一致：
    // 传"运行中"语义的标志会把极性搞反——运行期零节流热循环、且 next 每轮 +interval
    // 无限超前；stop 后线程反而要睡到一个被推到几十小时外的 deadline，join 永久挂死。
    std::atomic<bool> heartbeatStop_{true};
    std::atomic<int64_t> lastHeartbeatMs_{0};
    int32_t heartbeatIntervalMillis_ = 30000;
    std::atomic<int32_t> heartbeatCount_{0};

    std::thread detectorThread_;
    std::atomic<bool> detectorStop_{true};
    std::mutex detectTopicsMutex_;
    std::set<std::string> detectTopics_;

    std::string producerGroup_;
    std::string instanceName_ = "DEFAULT";
    std::string clientId_;
    std::string unitName_;
    bool unitMode_ = false;
    bool enableStreamRequestType_ = false;
    int32_t pollNameServerIntervalMillis_ = 30000;
    std::string createTopicKey_ = MixAll::DEFAULT_TOPIC;
    int32_t defaultTopicQueueNums_ = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
    bool tlsEnable_ = MQClientInstance::tlsEnabledFromEnv();
    TlsOptions tlsOptions_;
    bool tlsOptionsSet_ = false;
    // SOCKS 代理（start() 时透传给 MQClientInstance -> RemotingClient）
    SocksProxyConfig socksProxy_;
    bool socksProxySet_ = false;
    // 缺省读 env ROCKETMQ_TRACE_CONTEXT_ENABLE，与 setEnableTraceContext 的注释一致；
    // 写死 false 会让这条 env 开关形同虚设。
    bool enableTraceContext_ = traceContextEnabledFromEnv();
    int32_t sendMsgTimeout_ = 3000;
    int32_t requestTimeoutMillis_ = DEFAULT_REQUEST_TIMEOUT_MILLIS;
    int32_t retryTimesWhenSendFailed_ = 2;
    bool retryAnotherBrokerWhenNotStoreOK_ = false;
    int32_t sendMsgMaxTimeoutPerRequest_ = -1;
    std::set<int32_t> retryResponseCodes_ = {
        ResponseCode::SYSTEM_ERROR,        ResponseCode::SYSTEM_BUSY,
        ResponseCode::SERVICE_NOT_AVAILABLE, ResponseCode::NO_PERMISSION,
        ResponseCode::TOPIC_NOT_EXIST,     ResponseCode::NO_BUYER_ID,
        ResponseCode::NOT_IN_CURRENT_UNIT, ResponseCode::GO_AWAY};
    int32_t maxMessageSize_ = 1024 * 1024 * 4;
    bool autoBatch_ = false;
    int32_t batchMaxDelayMs_ = -1;
    int64_t batchMaxBytes_ = -1;
    int64_t totalBatchMaxBytes_ = -1;
    std::shared_ptr<ProduceAccumulator> produceAccumulator_;
    int32_t compressMsgBodyOverHowmuch_ = 1024 * 4;
    int32_t compressLevel_ = 5;
    int32_t compressType_ = CompressionType::ZLIB;
    std::vector<std::string> nameServerAddrs_;
    std::string namespace_;
    std::string namespaceV2_;
    MQFaultStrategy mqFaultStrategy_{false};

    std::unique_ptr<MQClientInstance> mqClient_;
    // ACL 钩子，start() 时绑定到 MQClientInstance 的传输层
    std::shared_ptr<RPCHook> rpcHook_;
    // ---------------- 消息轨迹 / 钩子 ----------------
    bool enableTrace_ = false;
    std::string traceTopic_;                     // 空 => 用 MixAll::TRACE_TOPIC
    int32_t traceMsgBatchNum_ = 10;
    std::vector<std::shared_ptr<SendMessageHook>> sendMessageHookList_;
    std::vector<std::shared_ptr<EndTransactionHook>> endTransactionHookList_;
    std::vector<std::shared_ptr<CheckForbiddenHook>> checkForbiddenHookList_;
    std::shared_ptr<AsyncTraceDispatcher> traceDispatcher_;
    bool started_ = false;
    std::mutex lock_;
    int32_t retryTimesWhenSendAsyncFailed_ = 2;
    int32_t asyncSenderQueueCapacity_ = 50000;
    int32_t clientCallbackExecutorThreads_ = 0;
    // 用 shared_ptr 而不是 unique_ptr：submit 前会在锁内拷一份引用，这样即使
    // shutdown() 同时把成员换走并排空队列，正在提交的那一次也不会摸到悬垂对象。
    std::shared_ptr<ConsumeExecutor> asyncSenderExecutor_;
    std::shared_ptr<ConsumeExecutor> callbackExecutor_;
    bool enableBackpressureForAsyncMode_ = false;
    int32_t backPressureForAsyncSendNum_ = 1024;
    int32_t backPressureForAsyncSendSize_ = 100 * 1024 * 1024;
    FairSemaphore semaphoreAsyncSendNum_;
    FairSemaphore semaphoreAsyncSendSize_;
};

class TransactionMQProducer : public DefaultMQProducer {
public:
    explicit TransactionMQProducer(
        const std::string& producerGroup = MixAll::DEFAULT_PRODUCER_GROUP)
        : DefaultMQProducer(producerGroup) {}

    void setTransactionListener(std::shared_ptr<TransactionListener> listener) {
        transactionListener_ = std::move(listener);
    }
    std::shared_ptr<TransactionListener> getTransactionListener() const {
        return transactionListener_;
    }

    TransactionSendResult sendMessageInTransaction(const Message& msg,
                                                   const std::string& arg = std::string());

private:
    std::shared_ptr<TransactionListener> transactionListener_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_PRODUCER_H
