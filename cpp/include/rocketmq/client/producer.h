// 生产者（对应 org.apache.rocketmq.client.producer.DefaultMQProducer /
// TransactionMQProducer 与 Python client/producer.py）。
//
// 能力覆盖：同步发送（轮询选队列 / 定点发送）、按选择器发送（顺序消息）、
// 异步发送、单向发送、批量发送、事务消息（简化单阶段）、按 Key 查询、
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
#include "rocketmq/client/result.h"
#include "rocketmq/common/compression.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"

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

    // ---------------- 批量 ----------------
    SendResult sendBatch(const std::vector<Message>& msgs, int32_t timeoutMillis = -1);

    // ---------------- 事务消息 ----------------
    // 注意：与 Python 参考实现一致，为**简化单阶段**实现 —— 发送普通消息后执行
    // 本地事务并回填状态，未实现 broker 半消息 / 回查 / END_TRANSACTION 两阶段提交。
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
    // 对应 Java DefaultMQProducerImpl.tryToCompressMessage + sendKernelImpl 的 sysFlag 组装：
    // 满足阈值且非批量时**就地压缩 msg.body**，返回应下发的 sysFlag
    // （COMPRESSED_FLAG | 压缩类型位）；不压缩时返回 0。
    int32_t prepareForSend(Message& msg) const;

    std::string producerGroup_;
    std::string instanceName_ = "DEFAULT";
    std::string clientId_;
    std::string createTopicKey_ = MixAll::DEFAULT_TOPIC;
    int32_t defaultTopicQueueNums_ = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
    int32_t sendMsgTimeout_ = 3000;
    int32_t retryTimesWhenSendFailed_ = 2;
    int32_t maxMessageSize_ = 1024 * 1024 * 4;
    // 压缩配置，默认值与 Java DefaultMQProducer 一致
    int32_t compressMsgBodyOverHowmuch_ = 1024 * 4;
    int32_t compressLevel_ = 5;
    int32_t compressType_ = CompressionType::ZLIB;
    std::vector<std::string> nameServerAddrs_;

    std::unique_ptr<MQClientInstance> mqClient_;
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
