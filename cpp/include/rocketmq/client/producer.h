// 生产者（对应 org.apache.rocketmq.client.producer.DefaultMQProducer /
// TransactionMQProducer 与 Python client/producer.py）。
//
// 能力覆盖：同步发送（轮询选队列 / 定点发送）、按选择器发送（顺序消息）、
// 异步发送、单向发送、批量发送、事务消息（两阶段：半消息 → 本地事务 → END_TRANSACTION → broker 回查）、
// 按 Key 查询、
// offset 查询、建 topic。
#ifndef ROCKETMQ_CLIENT_PRODUCER_H
#define ROCKETMQ_CLIENT_PRODUCER_H

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/hook.h"
#include "rocketmq/client/backpressure.h"
#include "rocketmq/client/consume_executor.h"
#include "rocketmq/client/latency.h"
#include "rocketmq/client/mq_client.h"
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

class DefaultMQProducer {
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
    // TLS（对应 Java tls.enable；缺省读 env ROCKETMQ_TLS_ENABLE）
    void setTlsEnable(bool b) { tlsEnable_ = b; }
    // W3C traceparent 透传（opt-in；缺省读 env ROCKETMQ_TRACE_CONTEXT_ENABLE）
    void setEnableTraceContext(bool b) { enableTraceContext_ = b; }
    void setSendMsgTimeout(int32_t millis) { sendMsgTimeout_ = millis; }
    void setRetryTimesWhenSendFailed(int32_t n) { retryTimesWhenSendFailed_ = n; }
    // 对应 Java DefaultMQProducer.retryAnotherBrokerWhenNotStoreOK（默认 false）：
    // 同步发送拿到 FLUSH_DISK_TIMEOUT / FLUSH_SLAVE_TIMEOUT / SLAVE_NOT_AVAILABLE 时，
    // 是否换个 broker 重试；false 时直接把该结果返回给调用方。
    void setRetryAnotherBrokerWhenNotStoreOK(bool b) { retryAnotherBrokerWhenNotStoreOK_ = b; }
    bool isRetryAnotherBrokerWhenNotStoreOK() const { return retryAnotherBrokerWhenNotStoreOK_; }
    // 对应 Java sendMsgMaxTimeoutPerRequest（默认 -1 表示不限制）：还有重试机会时，
    // 单次请求最多用掉这么多毫秒，把剩余超时留给后面的 broker。
    void setSendMsgMaxTimeoutPerRequest(int32_t millis) { sendMsgMaxTimeoutPerRequest_ = millis; }
    int32_t getSendMsgMaxTimeoutPerRequest() const { return sendMsgMaxTimeoutPerRequest_; }
    // 对应 Java DefaultMQProducer.addRetryResponseCode / getRetryResponseCodes：
    // broker 返回这些响应码（MQBrokerException）时才重试，其余立即抛出。
    // 默认集合与 Java 一致，见 retryResponseCodes_ 初始化。
    void addRetryResponseCode(int32_t responseCode) { retryResponseCodes_.insert(responseCode); }
    const std::set<int32_t>& getRetryResponseCodes() const { return retryResponseCodes_; }
    bool isRetryResponseCode(int32_t responseCode) const {
        return retryResponseCodes_.count(responseCode) > 0;
    }
    void setMaxMessageSize(int32_t bytes) { maxMessageSize_ = bytes; }
    // ---------------- 异步发送（对应 Java DefaultMQProducerImpl 的 AsyncSenderExecutor）----
    // 异步链的失败重试次数。Java DefaultMQProducer:140 默认 2，与同步的
    // retryTimesWhenSendFailed 是**两个独立**配置（同步循环用它、异步 onExceptionImpl 用它）。
    void setRetryTimesWhenSendAsyncFailed(int32_t n) { retryTimesWhenSendAsyncFailed_ = n; }
    int32_t getRetryTimesWhenSendAsyncFailed() const { return retryTimesWhenSendAsyncFailed_; }
    // AsyncSenderExecutor 的队列长度（Java 是 LinkedBlockingQueue(50000)）。队满时
    // sendAsync 向调用方抛 MQClientException("executor rejected")。
    void setAsyncSenderQueueCapacity(int32_t n) { asyncSenderQueueCapacity_ = n; }
    int32_t getAsyncSenderQueueCapacity() const { return asyncSenderQueueCapacity_; }
    // ---------------- 异步发送背压（对应 Java DefaultMQProducer:1368-1408）----------------
    // 开关默认关闭（与 Java 同），而且**不是**启动期配置：跑到一半也能打开/关掉，
    // 因为只有闸本身读它（见 producer.cpp 的 executeAsyncMessageSend）。
    void setEnableBackpressureForAsyncMode(bool b) { enableBackpressureForAsyncMode_ = b; }
    bool isEnableBackpressureForAsyncMode() const { return enableBackpressureForAsyncMode_; }
    // 运行时改「在途条数 / 在途字节数」上限。语义不是「设成 num」而是「总量变成 num、
    // 已经在途的那几份原样保留」，所以调小之后 availablePermits 可能为负（Java
    // setBackPressureForAsyncSendNum:1383-1391 的 new Semaphore(num - acquired) 同理）。
    // 低于地板值时夹到地板（10 条 / 1M 字节）。
    void setBackPressureForAsyncSendNum(int32_t num);
    int32_t getBackPressureForAsyncSendNum() const { return backPressureForAsyncSendNum_; }
    void setBackPressureForAsyncSendSize(int32_t size);
    int32_t getBackPressureForAsyncSendSize() const { return backPressureForAsyncSendSize_; }
    // 观测用（Java DefaultMQProducerImpl:200-206）：当前空闲许可，负数表示在途超额。
    int64_t getSemaphoreAsyncSendNumAvailablePermits() const;
    int64_t getSemaphoreAsyncSendSizeAvailablePermits() const;
    // 跑用户回调的线程数（Java NettyClientConfig.clientCallbackExecutorThreads）。
    // <=0 时取 CPU 核数 —— 那才是 Java 的**默认**口径（NettyClientConfig:28 =
    // availableProcessors，4 只是显式配 <=0 时 NettyRemotingClient 的兜底）。
    void setClientCallbackExecutorThreads(int32_t n) { clientCallbackExecutorThreads_ = n; }
    int32_t getClientCallbackExecutorThreads() const { return clientCallbackExecutorThreads_; }
    void setDefaultTopicQueueNums(int32_t n) { defaultTopicQueueNums_ = n; }
    void setCreateTopicKey(const std::string& key) { createTopicKey_ = key; }
    void setProducerGroup(const std::string& g);
    // 命名空间（对应 Java DefaultMQProducer namespace）。非空时发送前把 topic
    // 包装成 "namespace%topic" 再发给 broker（系统资源 / retry / DLQ 前缀除外）。
    void setNamespace(const std::string& ns) { namespace_ = ns; }
    const std::string& namespaceOf() const { return namespace_; }

    // ---------------- unitName / unitMode / enableStreamRequestType ----------------
    // 对应 Java `ClientConfig` 的三个同名开关（差异见 `cpp/README.md`）。
    // 必须在 start() 之前设置：unitName/@STREAM 决定 clientId，stream 决定钩子链，
    // 两者都要在首个请求发出前定下来。
    void setUnitName(const std::string& unitName) { unitName_ = unitName; }
    const std::string& unitName() const { return unitName_; }
    // Java `DefaultMQProducer` 不置 unitMode（只有消费者/事务链路读它），默认 false。
    void setUnitMode(bool unitMode) { unitMode_ = unitMode; }
    bool isUnitMode() const { return unitMode_; }
    void setEnableStreamRequestType(bool enable) { enableStreamRequestType_ = enable; }
    bool isEnableStreamRequestType() const { return enableStreamRequestType_; }

    // ---------------- 故障规避（对应 Java sendLatencyFaultEnable，默认关闭）----------------
    // 开启后发送选队列会按 broker 延迟/隔离状态过滤（MQFaultStrategy）；发送结果回写
    // 容错表：成功记实测延迟（超阈值隔离该 broker 一段时间），异常记隔离 10000ms 档。
    void setSendLatencyFaultEnable(bool enable) { mqFaultStrategy_.setSendLatencyFaultEnable(enable); }
    bool isSendLatencyFaultEnable() const { return mqFaultStrategy_.isSendLatencyFaultEnable(); }
    MQFaultStrategy& mqFaultStrategy() { return mqFaultStrategy_; }

    // ---------------- ACL 鉴权（对应 Java DefaultMQProducer(rpcHook)）----------------
    // 必须在 start() 之前调用：钩子在 start() 里绑定到 MQClientInstance（同一 clientId
    // 复用实例时以先注册者为准，与 Java 的绑定时机一致）。
    void setRPCHook(std::shared_ptr<RPCHook> hook) { rpcHook_ = std::move(hook); }
    // 便捷入口：用 accessKey/secretKey（可选 securityToken）构造 AclClientRPCHook。
    void setCredentials(const std::string& accessKey, const std::string& secretKey,
                        const std::string& securityToken = std::string()) {
        rpcHook_ = std::make_shared<AclClientRPCHook>(
            SessionCredentials(accessKey, secretKey, securityToken));
    }
    const std::shared_ptr<RPCHook>& rpcHook() const { return rpcHook_; }

    // ---------------- 压缩配置（对应 Java DefaultMQProducer 同名属性）----------------
    // body 长度 >= 该阈值时自动压缩（默认 4096，与 Java 一致）；批量消息永不压缩。
    void setCompressMsgBodyOverHowmuch(int32_t bytes) { compressMsgBodyOverHowmuch_ = bytes; }
    int32_t getCompressMsgBodyOverHowmuch() const { return compressMsgBodyOverHowmuch_; }
    // 压缩级别，仅 ZLIB 有意义（Java 默认 5）
    void setCompressLevel(int32_t level) { compressLevel_ = level; }
    int32_t getCompressLevel() const { return compressLevel_; }
    // 压缩算法：CompressionType::ZLIB / LZ4 / ZSTD（Java 默认 ZLIB）
    void setCompressType(int32_t type) { compressType_ = type; }
    int32_t getCompressType() const { return compressType_; }

    // ---------------- 消息轨迹（对应 Java DefaultMQProducer.setEnableTrace 等）----------------
    // enableTrace=true 时 start() 会建 AsyncTraceDispatcher（Type=PRODUCE）并自动注册
    // SendMessageTraceHook + EndTransactionTraceHook；轨迹生产者自身不追踪自身（防递归）。
    void setEnableTrace(bool enable) { enableTrace_ = enable; }
    bool isEnableTrace() const { return enableTrace_; }
    // 自定义轨迹 topic（默认 RMQ_SYS_TRACE_TOPIC）
    void setTraceTopic(const std::string& topic) { traceTopic_ = topic; }
    const std::string& getTraceTopic() const { return traceTopic_; }
    // 一次批量发送的最大轨迹条数（对应 Java traceMsgBatchNum，最大 20）
    void setTraceMsgBatchNum(int32_t n) { traceMsgBatchNum_ = n; }
    int32_t getTraceMsgBatchNum() const { return traceMsgBatchNum_; }

    // ---------------- 钩子（对应 Java registerSendMessageHook / registerEndTransactionHook）----
    void registerSendMessageHook(std::shared_ptr<SendMessageHook> hook) {
        if (hook) sendMessageHookList_.push_back(std::move(hook));
    }
    void registerEndTransactionHook(std::shared_ptr<EndTransactionHook> hook) {
        if (hook) endTransactionHookList_.push_back(std::move(hook));
    }
    bool hasSendMessageHook() const { return !sendMessageHookList_.empty(); }
    size_t sendMessageHookCount() const { return sendMessageHookList_.size(); }

    // 发送前拦截钩子（对应 Java registerCheckForbiddenHook / hasCheckForbiddenHook）。
    // ⚠ 它的异常**不被吞掉**，会沿发送重试链向上传播 —— 见 executeCheckForbiddenHook。
    void registerCheckForbiddenHook(std::shared_ptr<CheckForbiddenHook> hook) {
        if (hook) checkForbiddenHookList_.push_back(std::move(hook));
    }
    bool hasCheckForbiddenHook() const { return !checkForbiddenHookList_.empty(); }
    size_t checkForbiddenHookCount() const { return checkForbiddenHookList_.size(); }
    // 不需要走「带拦截/钩子」发送内核时返回 false（零开销快路径）
    bool hasSendInterceptors() const {
        return !sendMessageHookList_.empty() || !checkForbiddenHookList_.empty();
    }
    // 供单测/联调直接驱动钩子执行（不经过网络）。
    // ⚠ 与 send/consume 钩子**相反**：这里**不吞异常** —— 钩子抛出的异常会原样传播出去，
    // 这正是"禁止发送"的实现方式（Java executeCheckForbiddenHook 的语义）。
    void executeCheckForbiddenHook(CheckForbiddenContext& context);
    // 轨迹分发器（未开轨迹时为空），供联调脚本读取丢弃计数等状态
    std::shared_ptr<AsyncTraceDispatcher> traceDispatcher() const { return traceDispatcher_; }

    const std::string& producerGroup() const { return producerGroup_; }
    const std::string& clientId() const { return clientId_; }
    int32_t sendMsgTimeout() const { return sendMsgTimeout_; }
    int32_t maxMessageSize() const { return maxMessageSize_; }
    bool isStarted() const { return started_; }

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

    // ---------------- 异步 / 单向 ----------------
    // 真正的异步发送（对应 Java DefaultMQProducerImpl#send(msg, SendCallback, timeout)）：
    // **调用方立即返回**，准备工作在 AsyncSenderExecutor_N 上跑，请求走
    // MQClientInstance::sendMessageAsync（传输层 invokeAsync），失败按
    // retryTimesWhenSendAsyncFailed 换 broker 重试（Java onExceptionImpl：复用同一个请求、
    // 每次尝试换新 opaque、超时用共享的剩余预算），用户回调和 SendMessageHook.after 在
    // NettyClientPublicExecutor_N 上跑。callback 以 shared_ptr 持有，调用方可安全释放。
    //
    // 开了 setEnableBackpressureForAsyncMode(true) 之后，投队列**之前**还要过一道闸
    // （executeAsyncMessageSend，Java DefaultMQProducerImpl:635-682）：按条数和字节数各拿
    // 一份许可，拿不到就回调 RemotingTooMuchRequestException。这道闸**在调用方线程上等**，
    // 所以背压打满时「异步」会退化成「等满 timeout 再报错」；队满时 Java 也允许就地跑完
    // 这一笔（同样阻塞调用方），为的是把已经扣掉的许可还得回来。
    //
    // 与 Java 的两处有意差别：未 start() 时同步抛而不是走回调；批量消息复用同步批量内核
    // （只是不阻塞调用方）。信号量本身另有两处（原地改容量、不需要 ReadWriteCASLock），
    // 见 backpressure.h。
    void sendAsync(const Message& msg, std::shared_ptr<SendCallback> callback,
                   int32_t timeoutMillis = -1);
    // 定点异步发送：mq 非空时失败只在**同一台 broker** 上换 opaque 重试
    // （Java send(msg, mq, callback, timeout) 传下去的 topicPublishInfo 是 null）。
    void sendAsync(const Message& msg, const MessageQueue& mq, std::shared_ptr<SendCallback> callback,
                   int32_t timeoutMillis = -1);
    void sendOneway(const Message& msg);

    // ---------------- Request-Reply（5.x）----------------
    // 同步 request：给请求消息写 CORRELATION_ID（随机 UUID）/ REPLY_TO_CLIENT（本客户端
    // clientId）/ TTL（= timeout），发送后阻塞等应答，应答由 broker 经
    // PUSH_REPLY_MESSAGE_TO_CLIENT(326) 推回（注册在 MQClientInstance 构造里）。
    // 对应 Java DefaultMQProducerImpl#request(msg, timeout)。
    // 超时抛 RequestTimeoutException（消息已发出但没等到应答）；
    // 发送本身失败抛 MQClientException。REPLY_TO_CLIENT 要靠心跳登记 channel，
    // 所以 start() 之后本实现会再补一次心跳（对齐 Java prepareSendRequest）。
    void setRequestTimeout(int32_t millis) { requestTimeoutMillis_ = millis; }
    int32_t requestTimeout() const { return requestTimeoutMillis_; }
    // 不指定队列：轮询选择
    Message request(const Message& msg, int32_t timeoutMillis = -1);
    // 定点发送请求到指定队列
    Message request(const Message& msg, const MessageQueue& mq, int32_t timeoutMillis = -1);

    // ---------------- 批量 ----------------
    SendResult sendBatch(const std::vector<Message>& msgs, int32_t timeoutMillis = -1);

    // ---------------- 事务消息 ----------------
    // 对齐 Java DefaultMQProducerImpl.sendMessageInTransaction 的**两阶段**：
    //   1) 半消息：给 msg 打 TRAN_MSG / PGROUP 属性，sysFlag 置 TRANSACTION_PREPARED_TYPE；
    //   2) 本地事务：仅 SEND_OK 时执行；FLUSH_* / SLAVE_NOT_AVAILABLE -> ROLLBACK；
    //   3) endTransaction：以 END_TRANSACTION(37, oneway) 告知 broker 提交 / 回滚 / 未知；
    //   4) UNKNOW 时由 broker 回查 CHECK_TRANSACTION_STATE(39)，回调
    //      listener.checkLocalTransaction 后再发 END_TRANSACTION(fromTransactionCheck=true)。
    TransactionSendResult sendMessageInTransaction(const Message& msg, TransactionListener& listener,
                                                   const std::string& arg = std::string());

    // ---------------- 查询 / 管理 ----------------
    std::vector<MessageExt> queryMessage(const std::string& topic, const std::string& key,
                                         int32_t maxNum, int64_t beginTimestamp,
                                         int64_t endTimestamp);
    std::vector<MessageQueue> fetchPublishMessageQueues(const std::string& topic);

    // ---------------- 定时消息撤回（对应 Java recallMessage）----------------
    // 撤回一条定时/延迟消息，返回被撤回消息的 uniqKey。句柄来自定时消息的
    // SendResult.recallHandle（对应 Java DefaultMQProducer#recallMessage(:1140) 会先给
    // topic 套 namespace，所以这里传**业务原始 topic**）。
    std::string recallMessage(const std::string& topic, const std::string& recallHandle);
    void createTopic(const std::string& key, const std::string& newTopic, int32_t queueNum = 4);
    int64_t searchOffset(const MessageQueue& mq, int64_t timestamp);
    int64_t maxOffset(const MessageQueue& mq);
    int64_t minOffset(const MessageQueue& mq);

protected:
    void checkMessage(const Message& msg) const;
    // 发送前给 topic 套上 namespace 前缀（对应 Java withNamespace）；namespace 为空原样返回。
    Message withNamespace(const Message& msg) const;
    // request() 的公共收尾（两个公开重载都会走到这里；outbound 已过 withNamespace/checkMessage）
    Message requestWithQueue(Message& outbound, const MessageQueue& mq, int32_t timeout);
    // 对应 Java waitResponse：超时/发送失败分别抛 RequestTimeoutException / MQClientException
    Message waitRequestResponse(const Message& outbound, int32_t timeout,
                                const std::shared_ptr<RequestResponseFuture>& future,
                                int64_t costMillis);
    // 对应 Java DefaultMQProducerImpl.tryToCompressMessage + sendKernelImpl 的 sysFlag 组装：
    // 满足阈值且非批量时**就地压缩 msg.body**，返回应下发的 sysFlag
    // （COMPRESSED_FLAG | 压缩类型位）；不压缩时返回 0。
    int32_t prepareForSend(Message& msg) const;

    // ---------------- 轨迹 / 钩子内部实现 ----------------
    // 带 before/after 钩子的同步发送（对应 Java sendKernelImpl 的钩子点）：
    // 无钩子时直接透传，零开销。msgType 判定与 Java 一致：
    // TRAN_MSG=true -> Trans_Msg_Half；带 DELAY 属性 -> Delay_Msg；否则 Normal_Msg。
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
    // 事务收尾轨迹（对应 Java endTransaction 里的 EndTransactionTraceHook）
    void executeEndTransactionHook(const Message& msg, const std::string& brokerAddr,
                                   const std::string& msgId, const std::string& transactionId,
                                   const std::string& transactionState, bool fromTransactionCheck);
    // start() 里按 enableTrace 建分发器并注册钩子；任何异常只记日志，不影响正常发送。
    void startTraceDispatcher();
    // 发轨迹前的公共上下文（brokerAddr 用消息将要落到的 broker 地址）
    SendMessageContext buildSendMessageContext(const Message& msg, const MessageQueue& mq,
                                               const std::string& brokerAddr) const;

    // ---------------- 异步发送链（Java AsyncSenderExecutor + sendMessageAsync）----------------
    // 一笔异步发送从头到尾的可变状态。用 shared_ptr 传递：它同时被
    // 「在途请求的回调」和「重试链」持有，最后一份释放时才会析构。
    // ⚠ msg 的地址会被 SendMessageContext 以裸指针引用，所以状态必须在堆上且不再移动。
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
        int32_t times = 0;                             // 已失败次数（Java onExceptionImpl 的 times）
        bool pinned = false;                           // 定点发送：重试不换 broker
        // ---- 背压闸的两份许可（Java BackpressureSendCallBack:577-633 的两个 boolean）----
        // 没走闸（开关关着）时全 false，releaseBackPressure 就什么都不做。
        bool numAcquired = false;                      // 已拿到 1 份「条数」许可
        bool sizeAcquired = false;                     // 已拿到 msgLen 份「字节」许可
        bool permitsReleased = false;                  // 只归还一次（链上多条终点会重复收尾）
        int64_t msgLen = 1;                            // 扣字节许可的份数，**压缩前**的 body 长度
        std::chrono::steady_clock::time_point attemptBegan;  // 本次尝试起点（单调钟）
    };

    // start() 里建 AsyncSenderExecutor + NettyClientPublicExecutor 两个池。
    void createAsyncExecutors();
    // 入口：算好超时、把准备工作投进 AsyncSenderExecutor_N（调用方在此返回）。
    // pinned 非空时是定点异步发送：重试不换 broker（Java 传下去的 publish 是 null）。
    void enqueueAsync(const std::shared_ptr<AsyncSendState>& state, const MessageQueue* pinned,
                      int32_t timeoutMillis);
    // 投队列之前过闸（Java DefaultMQProducerImpl.executeAsyncMessageSend:635-682）：
    // 「条数」「字节数」两个许可**顺序**申请，都用从 began 算起的剩余预算去等，所以第一个闸
    // 就能把预算花光。拿不到就调 completeAsync 报错；队满时开了背压就地跑 runnable（许可已经
    // 扣掉，就地跑完才还得回来），关着则抛 MQClientException("executor rejected")。
    void executeAsyncMessageSend(const std::shared_ptr<AsyncSendState>& state,
                                 const std::function<void()>& runnable, int32_t timeout,
                                 const std::chrono::steady_clock::time_point& began,
                                 const std::shared_ptr<ConsumeExecutor>& pool);
    // 链的终点归还许可：先还字节、再还条数（Java semaphoreProcessor:599-610），
    // 且只还**本次真正拿到**的那份、只还一次。
    void releaseBackPressure(const std::shared_ptr<AsyncSendState>& state);
    // 出队后的准备工作（Java sendDefaultImpl(ASYNC) → sendKernelImpl）
    void sendAsyncInner(const std::shared_ptr<AsyncSendState>& state, const MessageQueue* pinned);
    // 建请求 + before 钩子，然后发出第一笔尝试
    void sendKernelAsync(const std::shared_ptr<AsyncSendState>& state);
    // 一笔在途尝试（对应 Java MQClientAPIImpl#sendMessageAsync）。
    // 就地抛出的异常（连不上、写失败）按 Java 的外层 catch 处理：**原样**传递、needRetry=true。
    void sendAttempt(const std::shared_ptr<AsyncSendState>& state, const std::string& addr);
    // 一笔尝试的结局（在 NettyClientPublicExecutor_N 上跑）：记容错表，然后要么收尾要么重试
    void onAttemptComplete(const std::shared_ptr<AsyncSendState>& state, const SendResult& result,
                           const InvokeError& error);
    // 失败分类 + 换 broker 重试（对应 Java onExceptionImpl）
    void onSendException(const std::shared_ptr<AsyncSendState>& state, const InvokeError& error,
                         bool needRetry);
    // 链的终点：先 after 钩子，再回调用户（用户回调抛的异常吞掉，不能带走回调线程）
    void completeAsync(const std::shared_ptr<AsyncSendState>& state, const SendResult* result,
                       const InvokeError* error);
    // 把回调处理挪到 NettyClientPublicExecutor_N；池已关或投不进时就地跑（Java runInThisThread）
    void executeOnCallbackThread(std::function<void()> fn);

    // 对应 Java endTransaction / checkTransactionState 的收尾：
    // 以 END_TRANSACTION(37, oneway) 告知 broker 事务最终状态。
    // fromCheck=true 时表示这是**回查**的收尾，偏移等字段取自 broker 的回查 header。
    void endTransaction(const Message& msg, const SendResult& sendResult,
                        LocalTransactionState state, bool hasLocalException,
                        const std::string& localExceptionText, bool fromCheck,
                        const CheckTransactionStateRequestHeader* checkHeader,
                        const MessageExt* checkMsg, const std::string& brokerAddr);
    // broker 主动发起的事务回查（CHECK_TRANSACTION_STATE=39）入口，由传输层回调。
    void checkTransactionState(const RemotingCommand& cmd, const std::string& addr);

    // 向所有已知 broker 发一次心跳（含 ProducerData）。
    //
    // Java 里 producer 与 consumer 一样定期心跳注册到 broker；**broker 的事务回查正是
    // 通过 ProducerManager 里登记的 channel 反向联系生产者的**。生产者不发心跳时，
    // COMMIT/ROLLBACK 仍能成功（客户端主动 END_TRANSACTION），但 UNKNOW 状态的半消息
    // 会因为 broker 找不到客户端而**永远不被回查**。
    int32_t sendHeartbeatToAllBroker();

    // 最近一次 sendMessageInTransaction 使用的监听器（broker 回查时回调它）。
    // 裸引用：调用方需保证其生命周期覆盖事务回查（与 Java 的 TransactionListener 引用语义一致）。
    TransactionListener* txListener_ = nullptr;    // 回查处理线程句柄，shutdown 时统一 join 回收
    std::vector<std::thread> txThreads_;
    std::mutex txThreadsMutex_;

    // 心跳线程（对齐 Java MQClientInstance 的定时心跳；间隔默认 30s）
    std::thread heartbeatThread_;
    std::atomic<bool> heartbeatRunning_{false};
    std::atomic<int64_t> lastHeartbeatMs_{0};
    int32_t heartbeatIntervalMillis_ = 30000;
    std::atomic<int32_t> heartbeatCount_{0};

    std::string producerGroup_;
    std::string instanceName_ = "DEFAULT";
    std::string clientId_;
    std::string unitName_;
    bool unitMode_ = false;
    // Java 的 pull / lite 消费者在**每个构造函数**里置 true（DefaultMQPullConsumer:113/126、
    // DefaultLitePullConsumer:213/228），生产者与推送消费者保持 false。
    bool enableStreamRequestType_ = false;
    std::string createTopicKey_ = MixAll::DEFAULT_TOPIC;
    int32_t defaultTopicQueueNums_ = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
    bool tlsEnable_ = MQClientInstance::tlsEnabledFromEnv();
    // 缺省读 env ROCKETMQ_TRACE_CONTEXT_ENABLE，与 setEnableTraceContext 的注释和
    // Python/dotnet 一致；写死 false 会让这条 env 开关形同虚设。
    bool enableTraceContext_ = traceContextEnabledFromEnv();
    int32_t sendMsgTimeout_ = 3000;
    // Request-Reply 默认超时（对应 Java DefaultMQProducer 的 request 兜底 3000ms）
    int32_t requestTimeoutMillis_ = DEFAULT_REQUEST_TIMEOUT_MILLIS;
    int32_t retryTimesWhenSendFailed_ = 2;
    bool retryAnotherBrokerWhenNotStoreOK_ = false;
    int32_t sendMsgMaxTimeoutPerRequest_ = -1;
    // 可重试的 broker 响应码，默认集合与 Java DefaultMQProducer#retryResponseCodes 一致
    std::set<int32_t> retryResponseCodes_ = {
        ResponseCode::SYSTEM_ERROR,        ResponseCode::SYSTEM_BUSY,
        ResponseCode::SERVICE_NOT_AVAILABLE, ResponseCode::NO_PERMISSION,
        ResponseCode::TOPIC_NOT_EXIST,     ResponseCode::NO_BUYER_ID,
        ResponseCode::NOT_IN_CURRENT_UNIT, ResponseCode::GO_AWAY};
    int32_t maxMessageSize_ = 1024 * 1024 * 4;
    // 压缩配置，默认值与 Java DefaultMQProducer 一致
    int32_t compressMsgBodyOverHowmuch_ = 1024 * 4;
    int32_t compressLevel_ = 5;
    int32_t compressType_ = CompressionType::ZLIB;
    std::vector<std::string> nameServerAddrs_;
    std::string namespace_;
    // 发送延迟故障规避（默认关闭，对应 Java MQFaultStrategy 的默认开关）
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
    // 异步发送的两个池（Java DefaultMQProducerImpl.defaultAsyncSenderExecutor 与
    // NettyRemotingAbstract.publicExecutor）。start() 建、shutdown() 关，未启动时为空。
    int32_t retryTimesWhenSendAsyncFailed_ = 2;
    int32_t asyncSenderQueueCapacity_ = 50000;
    int32_t clientCallbackExecutorThreads_ = 0;
    // 用 shared_ptr 而不是 unique_ptr：submit 前会在锁内拷一份引用，这样即使
    // shutdown() 同时把成员换走并排空队列，正在提交的那一次也不会摸到悬垂对象。
    std::shared_ptr<ConsumeExecutor> asyncSenderExecutor_;
    std::shared_ptr<ConsumeExecutor> callbackExecutor_;
    // ---------------- 异步发送背压（Java DefaultMQProducerImpl:122-153 的两个公平信号量）----
    // 构造函数里按默认配置建（Java 也是在 impl 构造时建），改容量走 setter。
    // 生命周期：链上的 completeAsync 用这两个对象归还许可，前提同样是「生产者活得比在途发送久」
    // —— 与 asyncSenderExecutor_ 的提交回调捕获 this 是同一个约束。
    bool enableBackpressureForAsyncMode_ = false;
    int32_t backPressureForAsyncSendNum_ = 1024;
    int32_t backPressureForAsyncSendSize_ = 100 * 1024 * 1024;
    FairSemaphore semaphoreAsyncSendNum_;
    FairSemaphore semaphoreAsyncSendSize_;
};

// 事务生产者（对应 Java TransactionMQProducer）：可预设 TransactionListener
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
