// Capability coverage: synchronous send (polling queue selection / pinned send to a specific queue), send by selector (ordered messages),
// async send, oneway send, batch send, transactional messages (two-phase: half message → local transaction → END_TRANSACTION → broker check-back),
// **auto batching (autoBatch + ProduceAccumulator, see client/produce_accumulator.h)**,
// query by key,
// offset queries, topic creation.
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

    // ---------------- Configuration ----------------
    // Semicolon-separated name server address string, e.g. "127.0.0.1:9876"
    void setNamesrvAddr(const std::string& addr);
    void setNameServerAddresses(const std::vector<std::string>& addrs);
    std::string getNamesrvAddr() const;
    void setInstanceName(const std::string& name) { instanceName_ = name; }
    void setTlsEnable(bool b) { tlsEnable_ = b; }
    // TLS strict-validation options (non-empty caCert = strict certificate-chain + hostname validation; clientCert/Key = mTLS).
    // Setting this automatically enables TLS; must be configured before start().
    void setTlsOptions(const TlsOptions& opts) {
        tlsOptions_ = opts;
        tlsOptionsSet_ = true;
        tlsEnable_ = true;
    }
    void setSocksProxy(const SocksProxyConfig& proxy) {
        socksProxy_ = proxy;
        socksProxySet_ = true;
    }
    // W3C tracepass-through (opt-in; default read from env ROCKETMQ_TRACE_CONTEXT_ENABLE)
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
    // Max hold time per batch, valid range (0, 30s] (default 10ms)
    void setBatchMaxDelayMs(int32_t holdMs);
    int32_t getBatchMaxDelayMs() const;
    // Max body size per batch, valid range (0, 2MB] (default 32KB)
    void setBatchMaxBytes(int64_t holdSize);
    int64_t getBatchMaxBytes() const;
    // In-flight total cap of the accumulator, must be > 0 (default 32MB)
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
    // Broker VIP channel: when true the rewritten broker request family is sent to port - 2 of the
    // route address. Must be set before start(); forwarded to the MQClientInstance that owns the RPCs.
    void setVipChannelEnabled(bool enable) { vipChannelEnabled_ = enable; }
    bool isVipChannelEnabled() const { return vipChannelEnabled_; }

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
    // Convenience entry: build an AclClientRPCHook from accessKey/secretKey (optional securityToken).
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
    // Custom trace topic (default RMQ_SYS_TRACE_TOPIC)
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
    // Return false when the "interception/hook-bearing" send kernel is not needed (zero-overhead fast path)
    bool hasSendInterceptors() const {
        return !sendMessageHookList_.empty() || !checkForbiddenHookList_.empty();
    }
    void executeCheckForbiddenHook(CheckForbiddenContext& context);
    // Trace dispatcher (null when tracing is off); integration scripts read state such as the discard counter from it
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

    // ---------------- Lifecycle ----------------
    void start();
    void shutdown();

    MQClientInstance& client();

    // ---------------- Synchronous send ----------------
    // No queue specified: polling selection, retry on failure per retryTimesWhenSendFailed
    SendResult send(const Message& msg, int32_t timeoutMillis = -1);
    // Pinned send to the specified queue
    SendResult send(const Message& msg, const MessageQueue& mq, int32_t timeoutMillis = -1);

    // Send via selector (ordered messages: the same arg lands on the same queue)
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
    // No queue specified: polling selection
    Message request(const Message& msg, int32_t timeoutMillis = -1);
    // Pinned request send to the specified queue
    Message request(const Message& msg, const MessageQueue& mq, int32_t timeoutMillis = -1);

    // ---------------- Batch ----------------
    SendResult sendBatch(const std::vector<Message>& msgs, int32_t timeoutMillis = -1);
    SendResult sendBatch(const std::vector<Message>& msgs, const MessageQueue& mq,
                         int32_t timeoutMillis = -1);

    // SendMessageHook (before/after) **still runs**: the reused synchronous batch kernel internally is sendWithHooks,
    // one context per attempt. The difference lies only in retry semantics — this chain
    // goes through the synchronous kernel's retryTimesWhenSendFailed loop, not the async chain's onSendException broker-switch
    // retry; so completeAsync does not re-run after (the context is finalized by the kernel itself).
    //
    // The backpressure gate matches the single-message async path, except the "byte" permit is charged as the sum of every body length
    // in the batch (an empty body still counts as 1, an empty batch counts as 1), see batchBackPressureMsgLen. Not started() throws synchronously as well.
    void sendBatchAsync(const std::vector<Message>& msgs, std::shared_ptr<SendCallback> callback,
                        int32_t timeoutMillis = -1);
    void sendBatchAsync(const std::vector<Message>& msgs, const MessageQueue& mq,
                        std::shared_ptr<SendCallback> callback, int32_t timeoutMillis = -1);

    TransactionSendResult sendMessageInTransaction(const Message& msg, TransactionListener& listener,
                                                   const std::string& arg = std::string());

    // ---------------- Query / management ----------------
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
    // Common tail of request() (both public overloads end up here; outbound already passed withNamespace/checkMessage)
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
    // Build a CheckForbiddenContext and execute it (called once per send attempt, retries included)
    void runCheckForbidden(const Message& msg, const MessageQueue& mq,
                           const std::string& brokerAddr, const std::string* arg,
                           CommunicationMode mode);
    void executeEndTransactionHook(const Message& msg, const std::string& brokerAddr,
                                   const std::string& msgId, const std::string& transactionId,
                                   const std::string& transactionState, bool fromTransactionCheck);
    // In start(), build the dispatcher per enableTrace and register hooks; any exception is only logged and never affects normal sending.
    void startTraceDispatcher();
    // Common context before sending a trace (brokerAddr is the broker address the message is about to land on)
    SendMessageContext buildSendMessageContext(const Message& msg, const MessageQueue& mq,
                                               const std::string& brokerAddr) const;

    struct AsyncSendState {
        Message msg;                                   // namespace applied, compressed
        std::shared_ptr<TopicPublishInfo> publish;     // nullptr = pinned send, no broker switch
        RemotingCommand request;                       // the one request object reused across retries
        MessageQueue mq;                               // queue targeted by the current attempt
        std::string brokerName;                        // broker of the current attempt (avoided on reselection)
        std::shared_ptr<SendCallback> callback;
        std::shared_ptr<SendMessageContext> context;   // nullptr = no send hooks, no context built
        int32_t sysFlag = 0;                           // compression bit, computed once for the whole chain
        int32_t timeout = 0;                           // **remaining** timeout budget (each round deducts what was spent)
        int32_t times = 0;
        bool pinned = false;                           // pinned send: retries do not switch broker
        bool numAcquired = false;                      // 1 "count" permit already acquired
        bool sizeAcquired = false;                     // msgLen "byte" permits already acquired
        bool permitsReleased = false;                  // release exactly once (several chain end-points would finalize twice)
        int64_t msgLen = 1;                            // units charged to the byte permit: **pre-compression** body length
        // ---- batch async (sendBatchAsync) ----
        // when batch=true this chain builds no request/publish/context; it hands batchMsgs to the synchronous batch
        // kernel instead (see sendBatchAsyncInner). msg stays empty, so the retry chain naturally does not participate.
        bool batch = false;
        std::vector<Message> batchMsgs;
        std::chrono::steady_clock::time_point attemptBegan;  // start of this attempt (monotonic clock)
    };

    // In start(), create the AsyncSenderExecutor + NettyClientPublicExecutor pools.
    void createAsyncExecutors();
    void enqueueAsync(const std::shared_ptr<AsyncSendState>& state, const MessageQueue* pinned,
                      int32_t timeoutMillis);
    void executeAsyncMessageSend(const std::shared_ptr<AsyncSendState>& state,
                                 const std::function<void()>& runnable, int32_t timeout,
                                 const std::chrono::steady_clock::time_point& began,
                                 const std::shared_ptr<ConsumeExecutor>& pool);
    void releaseBackPressure(const std::shared_ptr<AsyncSendState>& state);
    void sendAsyncInner(const std::shared_ptr<AsyncSendState>& state, const MessageQueue* pinned);
    // Batch branch: run the synchronous batch kernel inside the
    // AsyncSenderExecutor thread, then deliver the result on the callback thread.
    void sendBatchAsyncInner(const std::shared_ptr<AsyncSendState>& state,
                             const MessageQueue* pinned);
    // Build the request + run before hooks, then issue the first attempt
    void sendKernelAsync(const std::shared_ptr<AsyncSendState>& state);
    void sendAttempt(const std::shared_ptr<AsyncSendState>& state, const std::string& addr);
    void onTransportThrow(const std::shared_ptr<AsyncSendState>& state, const std::string& message,
                          InvokeError::Kind kind);
    // Outcome of one attempt (runs on NettyClientPublicExecutor_N): record into the fault-tolerance table, then either finalize or retry
    void onAttemptComplete(const std::shared_ptr<AsyncSendState>& state, const SendResult& result,
                           const InvokeError& error);
    void onSendException(const std::shared_ptr<AsyncSendState>& state, const InvokeError& error,
                         bool needRetry);
    // End of the chain: after hooks first, then the user callback (exceptions thrown by the user callback are swallowed; they must not take down the callback thread)
    void completeAsync(const std::shared_ptr<AsyncSendState>& state, const SendResult* result,
                       const InvokeError* error);
    void executeOnCallbackThread(std::function<void()> fn);

    void endTransaction(const Message& msg, const SendResult& sendResult,
                        LocalTransactionState state, bool hasLocalException,
                        const std::string& localExceptionText, bool fromCheck,
                        const CheckTransactionStateRequestHeader* checkHeader,
                        const MessageExt* checkMsg, const std::string& brokerAddr);
    // Transaction check-back initiated by the broker (CHECK_TRANSACTION_STATE=39) entry point, invoked by the transport layer.
    void checkTransactionState(const RemotingCommand& cmd, const std::string& addr);

    // Send one heartbeat (including ProducerData) to every known broker.
    int32_t sendHeartbeatToAllBroker();

    void detectorLoop();
    std::string pickDetectTopic();
    void rememberDetectTopic(const std::string& topic);
    bool detectBrokerByMaxOffset(const std::string& addr, int32_t timeoutMillis);

    TransactionListener* txListener_ = nullptr;    // check-back handler thread handle, joined and reclaimed uniformly at shutdown
    std::vector<std::thread> txThreads_;
    std::mutex txThreadsMutex_;

    std::thread heartbeatThread_;
    // Stop flag (false=running, true=stopped), consistent with the sleepUntilDeadline(stop, ...) contract:
    // passing a "running"-semantics flag inverts the polarity — a zero-throttling hot loop while running with next advancing
    // +interval every round without bound; after stop the thread would instead sleep to a deadline pushed tens of hours out, and join hangs forever.
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
    bool vipChannelEnabled_ = false;
    std::string createTopicKey_ = MixAll::DEFAULT_TOPIC;
    int32_t defaultTopicQueueNums_ = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
    bool tlsEnable_ = MQClientInstance::tlsEnabledFromEnv();
    TlsOptions tlsOptions_;
    bool tlsOptionsSet_ = false;
    // SOCKS proxy (passed through at start() to MQClientInstance -> RemotingClient)
    SocksProxyConfig socksProxy_;
    bool socksProxySet_ = false;
    // Default read from env ROCKETMQ_TRACE_CONTEXT_ENABLE, consistent with the setEnableTraceContext comment;
    // hardcoding false would make this env switch a no-op.
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
    // ACL hook, bound at start() to the transport layer of MQClientInstance
    std::shared_ptr<RPCHook> rpcHook_;
    // ---------------- Message trace / hooks ----------------
    bool enableTrace_ = false;
    std::string traceTopic_;                     // empty => use MixAll::TRACE_TOPIC
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
    // shared_ptr rather than unique_ptr: a reference copy is taken under the lock before submit, so even if
    // shutdown() concurrently swaps the member and drains the queue, the in-flight submission will not touch a dangling object.
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
