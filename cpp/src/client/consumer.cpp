// 推模式消费者实现（对应 Python client/consumer.py 的 DefaultMQPushConsumer）。
#include "rocketmq/client/consumer.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <string>
#include <utility>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/headers.h"

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
        std::string piece =
            addr.substr(start, pos == std::string::npos ? std::string::npos : pos - start);
        std::string t = trim(piece);
        if (!t.empty()) out.push_back(t);
        if (pos == std::string::npos) break;
        start = pos + 1;
    }
    return out;
}

}  // namespace

DefaultMQPushConsumer::DefaultMQPushConsumer(const std::string& consumerGroup) {
    if (UtilAll::isBlank(consumerGroup)) {
        throw MQClientException("consumerGroup is empty");
    }
    consumerGroup_ = consumerGroup;
}

DefaultMQPushConsumer::~DefaultMQPushConsumer() {
    try {
        shutdown();
    } catch (...) {
        // 析构不抛
    }
}

// ---------------------------------------------------------------- 配置
void DefaultMQPushConsumer::setNamesrvAddr(const std::string& addr) {
    nameServerAddrs_ = splitSemicolon(addr);
}

void DefaultMQPushConsumer::setNameServerAddresses(const std::vector<std::string>& addrs) {
    nameServerAddrs_ = addrs;
}

void DefaultMQPushConsumer::setConsumeThreadNums(int32_t n) {
    consumeThreadNums_ = std::max(1, n);
}

void DefaultMQPushConsumer::setMessageListener(std::shared_ptr<MessageListener> listener) {
    messageListener_ = std::move(listener);
}

// ---------------------------------------------------------------- 订阅
void DefaultMQPushConsumer::subscribe(const std::string& topic, const std::string& subExpression) {
    if (started_.load()) {
        throw MQClientException("consumer already started, cannot change configuration");
    }
    SubscriptionData sub = FilterAPI::buildSubscriptionData(topic, subExpression);
    std::lock_guard<std::mutex> lk(lock_);
    subscriptionData_[topic] = sub;
}

void DefaultMQPushConsumer::subscribe(const std::string& topic, const MessageSelector& selector) {
    if (started_.load()) {
        throw MQClientException("consumer already started, cannot change configuration");
    }
    SubscriptionData sub(topic, selector.expression);
    sub.expressionType = selector.type;
    if (selector.type == ExpressionType::TAG) {
        SubscriptionData built = FilterAPI::buildSubscriptionData(topic, selector.expression);
        sub.tagsSet = built.tagsSet;
    }
    std::lock_guard<std::mutex> lk(lock_);
    subscriptionData_[topic] = sub;
}

void DefaultMQPushConsumer::unsubscribe(const std::string& topic) {
    std::lock_guard<std::mutex> lk(lock_);
    subscriptionData_.erase(topic);
}

std::vector<std::string> DefaultMQPushConsumer::subscribedTopics() const {
    std::lock_guard<std::mutex> lk(lock_);
    std::vector<std::string> out;
    out.reserve(subscriptionData_.size());
    for (const auto& kv : subscriptionData_) {
        out.push_back(kv.first);
    }
    return out;
}

// ---------------------------------------------------------------- 生命周期
void DefaultMQPushConsumer::start() {
    {
        std::lock_guard<std::mutex> lk(lock_);
        if (started_.load()) {
            return;
        }
        if (nameServerAddrs_.empty()) {
            throw MQClientException("name server address is not set");
        }
        if (subscriptionData_.empty()) {
            throw MQClientException("subscription is not set, call subscribe() first");
        }
        if (messageListener_ == nullptr) {
            throw MQClientException("message listener is not set");
        }
        if (clientId_.empty()) {
            clientId_ = buildClientId(instanceName_);
        }
        mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                             /*connectTimeoutMillis=*/3000,
                                             /*invokeTimeoutMillis=*/pullTimeoutMillis_));
        mqClient_->start();
        stop_.store(false);
        started_.store(true);
    }

    const int32_t n = std::max(1, consumeThreadNums_);
    consumeThreads_.clear();
    for (int32_t i = 0; i < n; ++i) {
        consumeThreads_.emplace_back([this]() { consumeLoop(); });
    }
    std::string topics;
    for (const std::string& t : subscribedTopics()) {
        if (!topics.empty()) topics += ",";
        topics += t;
    }
    logger_info("DefaultMQPushConsumer[" + consumerGroup_ + "] started, clientId=" + clientId_
                + ", topics=" + topics + ", pullTimeout=" + std::to_string(pullTimeoutMillis_)
                + "ms, pullSuspend=" + std::to_string(pullSuspendTimeoutMillis_) + "ms");
}

void DefaultMQPushConsumer::shutdown() {
    if (!started_.exchange(false)) {
        return;
    }
    stop_.store(true);
    cv_.notify_all();
    for (std::thread& t : consumeThreads_) {
        if (t.joinable()) t.join();
    }
    consumeThreads_.clear();
    if (mqClient_) {
        mqClient_->shutdown();
    }
}

MQClientInstance& DefaultMQPushConsumer::client() {
    if (!started_.load() || mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    return *mqClient_;
}

// ---------------------------------------------------------------- 消费循环
void DefaultMQPushConsumer::consumeLoop() {
    while (!stop_.load()) {
        try {
            if (!started_.load()) break;
            pullAndConsumeOnce();
        } catch (const std::exception& e) {
            logger_warn("consume loop error: " + std::string(e.what()));
        } catch (...) {
            logger_warn("consume loop error: unknown");
        }
        int32_t interval = pullIntervalMillis_ > 0 ? pullIntervalMillis_ : 10;
        std::unique_lock<std::mutex> lk(waitMutex_);
        cv_.wait_for(lk, std::chrono::milliseconds(interval), [this]() { return stop_.load(); });
    }
}

std::string DefaultMQPushConsumer::offsetKey(const MessageQueue& mq) {
    return mq.topic + mq.brokerName + std::to_string(mq.queueId);
}

std::vector<MessageQueue> DefaultMQPushConsumer::assignedQueues() {
    MQClientInstance& c = client();
    std::vector<MessageQueue> result;
    for (const std::string& topic : subscribedTopics()) {
        try {
            std::shared_ptr<TopicPublishInfo> publish = c.getTopicPublishInfo(topic);
            for (const MessageQueue& q : publish->msgQueueList) {
                MessageQueue mq(topic, q.brokerName, q.queueId);
                if (std::find(result.begin(), result.end(), mq) == result.end()) {
                    result.push_back(mq);
                }
            }
        } catch (const std::exception& e) {
            logger_warn("assigned_queues: skip topic " + topic + ": " + e.what());
        }
    }
    return result;
}

int64_t DefaultMQPushConsumer::resolveInitialOffset(const MessageQueue& mq,
                                                    const SubscriptionData& sub) {
    MQClientInstance& c = client();
    if (sub.expressionType == ExpressionType::SQL92) {
        // SQL 过滤无 offset 语义，默认最新
        return c.getMaxOffset(mq);
    }
    try {
        if (consumeFromWhere_ == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET) {
            return c.getMinOffset(mq);
        }
        if (consumeFromWhere_ == ConsumeFromWhere::CONSUME_FROM_TIMESTAMP) {
            int64_t ts = UtilAll::currentTimeMillis() - 30 * 60 * 1000LL;
            return c.searchOffsetByTimestamp(mq, ts);
        }
        return c.getMaxOffset(mq);  // 默认 CONSUME_FROM_LAST_OFFSET
    } catch (const std::exception&) {
        return 0;
    }
}

void DefaultMQPushConsumer::pullAndConsumeOnce() {
    MQClientInstance& c = client();
    std::vector<MessageQueue> queues = assignedQueues();
    // 路由已加载（或尝试过），此时发心跳才能拿到 broker 地址
    maybeSendHeartbeat();

    for (const MessageQueue& mq : queues) {
        if (stop_.load() || !started_.load()) return;

        SubscriptionData sub;
        {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = subscriptionData_.find(mq.topic);
            if (it == subscriptionData_.end()) continue;
            sub = it->second;
        }

        const std::string key = offsetKey(mq);
        int64_t offset;
        {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = offsetTable_.find(key);
            if (it != offsetTable_.end()) {
                offset = it->second;
            } else {
                offset = -1;
            }
        }
        if (offset < 0) {
            offset = resolveInitialOffset(mq, sub);
            std::lock_guard<std::mutex> lk(lock_);
            offsetTable_[key] = offset;
        }

        PullResult result;
        try {
            const int32_t sysFlag =
                PullSysFlag::buildSysFlag(/*commitOffset=*/false, /*suspend=*/true,
                                          /*subscription=*/true, /*classFilter=*/false);
            const std::string expr = sub.subString.empty() ? std::string("*") : sub.subString;
            result = c.pullMessage(consumerGroup_, mq, offset, pullBatchSize_, sysFlag,
                                   /*commitOffset=*/0, expr, sub.subVersion, sub.expressionType,
                                   pullTimeoutMillis_, pullBatchSizeInBytes_,
                                   pullSuspendTimeoutMillis_);
        } catch (const MQBrokerException& e) {
            // PULL_OFFSET_MOVED 等已映射到 PullStatus；其余 broker 错误跳过本轮
            logger_debug("pull broker error for " + mq.toString() + ": " + e.what());
            continue;
        } catch (const RemotingTimeoutException& e) {
            // 长轮询在 suspend 期间无新消息触发客户端超时属正常行为：broker 会把
            // suspend 时间钳制到自身 brokerSuspendMaxTimeMillis，忽略客户端下发值，
            // 故空闲队列会周期性超时。非错误，仅 debug，避免污染运行日志。
            logger_debug("pull long-poll timeout for " + mq.toString()
                         + " (benign, will retry): " + e.what());
            continue;
        } catch (const std::exception& e) {
            logger_warn("pull error for " + mq.toString() + ": " + e.what());
            continue;
        }

        if (result.status == PullStatus::FOUND && !result.msgFoundList.empty()) {
            int32_t dispatched = dispatchMessages(mq, result.msgFoundList);
            std::lock_guard<std::mutex> lk(lock_);
            offsetTable_[key] = offset + dispatched;
        } else if (result.status == PullStatus::NO_NEW_MSG ||
                   result.status == PullStatus::OFFSET_ILLEGAL) {
            std::lock_guard<std::mutex> lk(lock_);
            offsetTable_[key] = result.nextBeginOffset;
        }
    }
}

int32_t DefaultMQPushConsumer::dispatchMessages(const MessageQueue& mq,
                                               const std::vector<MessageExt>& msgs) {
    std::shared_ptr<MessageListener> listener = messageListener_;
    if (listener == nullptr) {
        return 0;
    }
    const size_t batchSize =
        static_cast<size_t>(std::max(1, consumeMessageBatchMaxSize_));
    int32_t consumed = 0;
    size_t i = 0;
    while (i < msgs.size()) {
        size_t end = std::min(i + batchSize, msgs.size());
        std::vector<MessageExt> batch(msgs.begin() + static_cast<long>(i),
                                      msgs.begin() + static_cast<long>(end));
        try {
            if (listener->orderly()) {
                auto* orderly = static_cast<MessageListenerOrderly*>(listener.get());
                ConsumeOrderlyContext ctx(mq);
                ConsumeOrderlyStatus status = orderly->consumeMessage(batch, ctx);
                if (status == ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT) {
                    std::unique_lock<std::mutex> lk(waitMutex_);
                    cv_.wait_for(lk, std::chrono::milliseconds(suspendCurrentQueueTimeMillis_),
                                 [this]() { return stop_.load(); });
                    break;
                }
            } else {
                auto* conc = static_cast<MessageListenerConcurrently*>(listener.get());
                ConsumeConcurrentlyContext ctx(mq);
                ConsumeConcurrentlyStatus status = conc->consumeMessage(batch, ctx);
                if (status == ConsumeConcurrentlyStatus::RECONSUME_LATER) {
                    // 简化：本批不推进 offset，留给后续重投
                    break;
                }
            }
            consumed += static_cast<int32_t>(batch.size());
            consumedCount_.fetch_add(static_cast<int64_t>(batch.size()));
            i = end;
        } catch (const std::exception& e) {
            logger_warn("listener error: " + std::string(e.what()));
            break;
        } catch (...) {
            logger_warn("listener error: unknown");
            break;
        }
    }
    return consumed;
}

// ---------------------------------------------------------------- 心跳
int32_t DefaultMQPushConsumer::sendHeartbeatToAllBroker() {
    if (mqClient_ == nullptr) {
        return 0;
    }
    std::vector<std::string> addrs;
    try {
        addrs = mqClient_->knownBrokerAddrs();
    } catch (const std::exception& e) {
        logger_warn("heartbeat: gather brokers failed: " + std::string(e.what()));
        return 0;
    }
    if (addrs.empty()) {
        return 0;
    }

    HeartbeatData hb(clientId_);
    ConsumerData cd;
    cd.groupName = consumerGroup_;
    cd.consumeType = ConsumeType::CONSUME_PASSIVELY;
    cd.messageModel = messageModel_;
    cd.consumeFromWhere = consumeFromWhere_;
    cd.unitMode = false;
    {
        std::lock_guard<std::mutex> lk(lock_);
        for (const auto& kv : subscriptionData_) {
            cd.addSubscriptionData(kv.second);
        }
    }
    // 保持 fingerprint = 0，让 broker 走 V1 注册路径（用完整 subscriptionDataSet 注册）
    // withoutSub 仅在 fingerprint != 0 的 V2 路径下被读取，这里保持默认 false 即可。
    hb.heartbeatFingerprint = 0;
    hb.addConsumerData(cd);

    int32_t okCount = 0;
    for (const std::string& addr : addrs) {
        try {
            mqClient_->sendHeartbeat(addr, hb, 5000);
            ++okCount;
            heartbeatCount_.fetch_add(1);
        } catch (const std::exception& e) {
            logger_warn("heartbeat to " + addr + " failed: " + e.what());
        }
    }
    return okCount;
}

void DefaultMQPushConsumer::maybeSendHeartbeat() {
    if (!heartbeatEnabled_) {
        return;
    }
    int64_t now = UtilAll::currentTimeMillis();
    int64_t last = lastHeartbeatMs_.load();
    if (last != 0 && now - last < heartbeatIntervalMillis_) {
        return;
    }
    lastHeartbeatMs_.store(now);
    sendHeartbeatToAllBroker();
}

// ---------------------------------------------------------------- 管理
std::vector<MessageQueue> DefaultMQPushConsumer::fetchSubscribeMessageQueues(
    const std::string& topic) {
    MQClientInstance& c = client();
    std::shared_ptr<TopicPublishInfo> publish = c.getTopicPublishInfo(topic);
    std::vector<MessageQueue> out;
    out.reserve(publish->msgQueueList.size());
    for (const MessageQueue& q : publish->msgQueueList) {
        out.emplace_back(q.topic, q.brokerName, q.queueId);
    }
    return out;
}

bool DefaultMQPushConsumer::sendMessageBack(const MessageExt& msg, int32_t delayLevel,
                                           const std::string& brokerNameIn) {
    MQClientInstance& c = client();
    std::string brokerName = brokerNameIn.empty() ? msg.brokerName : brokerNameIn;
    std::string addr = c.brokerAddrOf(brokerName);
    if (addr.empty()) {
        throw MQClientException("broker " + brokerName + " not found");
    }
    auto header = std::make_shared<ConsumerSendMsgBackRequestHeader>();
    header->offset = msg.commitLogOffset;
    header->group = consumerGroup_;
    header->delayLevel = delayLevel;
    header->originMsgId = msg.msgId;
    header->originTopic = msg.topic;
    header->unitMode = false;
    header->maxReconsumeTimes = maxReconsumeTimes_;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::CONSUMER_SEND_MSG_BACK, header);
    RemotingCommand response = c.remotingClient().invokeSync(addr, request, 5000);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
    return true;
}

}  // namespace rocketmq
