// 生产者（对应 org.apache.rocketmq.client.producer.DefaultMQProducer /
// TransactionMQProducer 与 Python client/producer.py）。
//
// 能力覆盖：同步发送（轮询选队列 / 定点发送）、按选择器发送（顺序消息）、
// 异步发送、单向发送、批量发送、事务消息（两阶段：半消息 → 本地事务 → END_TRANSACTION → broker 回查）、
// 按 Key 查询、
// offset 查询、建 topic。
#ifndef ROCKETMQ_CLIENT_PRODUCER_H
#define ROCKETMQ_CLIENT_PRODUCER_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/request_reply.h"
#include "rocketmq/client/result.h"
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
    void setSendMsgTimeout(int32_t millis) { sendMsgTimeout_ = millis; }
    void setRetryTimesWhenSendFailed(int32_t n) { retryTimesWhenSendFailed_ = n; }
    void setMaxMessageSize(int32_t bytes) { maxMessageSize_ = bytes; }
    void setDefaultTopicQueueNums(int32_t n) { defaultTopicQueueNums_ = n; }
    void setCreateTopicKey(const std::string& key) { createTopicKey_ = key; }
    void setProducerGroup(const std::string& g);
    // 命名空间（对应 Java DefaultMQProducer namespace）。非空时发送前把 topic
    // 包装成 "namespace%topic" 再发给 broker（系统资源 / retry / DLQ 前缀除外）。
    void setNamespace(const std::string& ns) { namespace_ = ns; }
    const std::string& namespaceOf() const { return namespace_; }

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
    // 后台线程执行发送并回调；callback 以 shared_ptr 持有，调用方可安全释放
    void sendAsync(const Message& msg, std::shared_ptr<SendCallback> callback,
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
    TransactionListener* txListener_ = nullptr;
    // 回查处理线程句柄，shutdown 时统一 join 回收
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
    std::string createTopicKey_ = MixAll::DEFAULT_TOPIC;
    int32_t defaultTopicQueueNums_ = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
    int32_t sendMsgTimeout_ = 3000;
    // Request-Reply 默认超时（对应 Java DefaultMQProducer 的 request 兜底 3000ms）
    int32_t requestTimeoutMillis_ = DEFAULT_REQUEST_TIMEOUT_MILLIS;
    int32_t retryTimesWhenSendFailed_ = 2;
    int32_t maxMessageSize_ = 1024 * 1024 * 4;
    // 压缩配置，默认值与 Java DefaultMQProducer 一致
    int32_t compressMsgBodyOverHowmuch_ = 1024 * 4;
    int32_t compressLevel_ = 5;
    int32_t compressType_ = CompressionType::ZLIB;
    std::vector<std::string> nameServerAddrs_;
    std::string namespace_;

    std::unique_ptr<MQClientInstance> mqClient_;
    // ACL 钩子，start() 时绑定到 MQClientInstance 的传输层
    std::shared_ptr<RPCHook> rpcHook_;
    bool started_ = false;
    std::mutex lock_;
    // 异步发送线程句柄，shutdown 时统一 join 回收
    std::vector<std::thread> asyncThreads_;
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
