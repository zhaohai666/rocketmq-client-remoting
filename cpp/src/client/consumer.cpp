// 推模式消费者实现（对应 Python client/consumer.py 的 DefaultMQPushConsumer）。
#include "rocketmq/client/consumer.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/json.h"

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
        // 集群模式自动订阅重试 topic（对齐 Java copySubscription → getRetryTopic）：
        // broker 回投的消息写到 %RETRY%group，客户端不订阅就收不到
        if (messageModel_ != MessageModel::BROADCASTING) {
            const std::string retryTopic = MixAll::getRetryTopic(consumerGroup_);
            if (subscriptionData_.find(retryTopic) == subscriptionData_.end()) {
                subscriptionData_[retryTopic] =
                    FilterAPI::buildSubscriptionData(retryTopic, "*");
            }
        }
        mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                             /*connectTimeoutMillis=*/3000,
                                             /*invokeTimeoutMillis=*/pullTimeoutMillis_));
        mqClient_->start();
        stop_.store(false);
        started_.store(true);
    }

    // 拉取：每队列一个线程（并发长轮询，避免空队列 suspend 阻塞其他队列投递）
    rebalancePullThreads();
    dispatchThread_ = std::thread([this]() {
        setThreadName("ConsumeMessageThread");
        dispatchLoop();
    });
    persistThread_ = std::thread([this]() {
        setThreadName("MQClientFactoryScheduledThread");
        offsetPersistLoop();
    });
    lockThread_ = std::thread([this]() {
        setThreadName("ConsumeMessageOrderlyServiceThread");
        lockLoop();
    });
    rebalanceThread_ = std::thread([this]() {
        setThreadName("RebalanceThread");
        rebalanceLoop();
    });

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
    // 退出前把已消费位点持久化一次（对齐 Java shutdown → persistAllConsumerOffset）
    try {
        persistOffsetsOnce();
    } catch (const std::exception& e) {
        logger_debug(std::string("persist offsets on shutdown failed: ") + e.what());
    }
    // 顺序消费清退时解锁队列（对齐 Java ConsumeMessageOrderlyService.shutdown → unlockAll）
    if (isOrderly() && messageModel_ != MessageModel::BROADCASTING) {
        try {
            std::vector<MessageQueue> mqs = assignedQueues();
            if (!mqs.empty()) {
                mqClient_->unlockBatchMq(consumerGroup_, clientId_, mqs);
            }
        } catch (const std::exception& e) {
            logger_debug(std::string("unlock on shutdown failed: ") + e.what());
        }
    }
    for (auto& kv : pullThreads_) {
        if (kv.second.joinable()) kv.second.join();
    }
    pullThreads_.clear();
    if (dispatchThread_.joinable()) dispatchThread_.join();
    if (persistThread_.joinable()) persistThread_.join();
    if (lockThread_.joinable()) lockThread_.join();
    if (rebalanceThread_.joinable()) rebalanceThread_.join();
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
bool DefaultMQPushConsumer::isOrderly() const {
    return messageListener_ != nullptr && messageListener_->orderly();
}

void DefaultMQPushConsumer::rebalancePullThreads() {
    std::vector<MessageQueue> queues = assignedQueues();
    std::map<std::string, MessageQueue> current;
    for (const MessageQueue& mq : queues) {
        current[offsetKey(mq)] = mq;
    }
    std::vector<std::pair<std::string, MessageQueue>> toStart;
    {
        std::lock_guard<std::mutex> lk(lock_);
        for (const auto& kv : current) {
            if (pullThreads_.find(kv.first) == pullThreads_.end()) {
                toStart.emplace_back(kv.first, kv.second);
            }
        }
    }
    for (const auto& kv : toStart) {
        // 捕获 kv.second（拷贝），线程内再通过成员访问共享状态
        std::thread t([this, mq = kv.second]() {
            setThreadName("PullMessageService");
            queuePullLoop(mq);
        });
        std::lock_guard<std::mutex> lk(lock_);
        // 竞态保护：rebalance 可能把同 key 再起一次
        if (pullThreads_.find(kv.first) != pullThreads_.end()) {
            if (t.joinable()) t.detach();  // 多余的线程自己退出
            continue;
        }
        pullThreads_[kv.first] = std::move(t);
    }
}

void DefaultMQPushConsumer::rebalanceLoop() {
    // 简化 rebalance：周期刷新分配集，为新增队列（如 %RETRY%topic 建立路由后）补拉取线程
    while (!stop_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        if (stop_.load() || !started_.load()) return;
        try {
            maybeSendHeartbeat();
            rebalancePullThreads();
        } catch (const std::exception& e) {
            logger_debug(std::string("rebalance pull threads error: ") + e.what());
        }
    }
}

void DefaultMQPushConsumer::queuePullLoop(const MessageQueue& mq) {
    MQClientInstance& c = client();
    const bool orderly = isOrderly();
    const std::string key = offsetKey(mq);
    while (!stop_.load() && started_.load()) {
        SubscriptionData sub;
        {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = subscriptionData_.find(mq.topic);
            if (it == subscriptionData_.end()) return;
            sub = it->second;
        }
        // 顺序消费：broker 未确认锁定（LOCK_BATCH_MQ）的队列不拉取
        if (orderly) {
            bool locked = false;
            {
                std::lock_guard<std::mutex> lk(lock_);
                locked = lockOk_.find(key) != lockOk_.end();
            }
            if (!locked) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                continue;
            }
        }
        // 流控（对齐 Java ProcessQueue 的 pullThresholdForQueue 检查）：
        // 已拉未消费的条数超过阈值就暂停本队列拉取
        {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = pending_.find(key);
            const size_t pendingN = (it == pending_.end()) ? 0 : it->second.size();
            if (pendingN >= static_cast<size_t>(std::max(1, pullThresholdForQueue_))) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            } else {
                // 未触发流控，继续拉取
                goto pullNow;
            }
        }
        flowControlTriggered_.fetch_add(1);
        logger_debug("flow control: queue " + mq.toString() + " pause pull");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
    pullNow:
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
            try {
                offset = resolveInitialOffset(mq, sub);
            } catch (const std::exception& e) {
                logger_debug("resolve initial offset failed for " + mq.toString()
                             + ": " + e.what());
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                continue;
            }
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
            // TOPIC_NOT_EXIST / PULL_NOT_FOUND 等多为预期路径（topic 未创建等），debug + 退避
            logger_debug("pull broker error for " + mq.toString() + ": " + e.what());
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        } catch (const RemotingTimeoutException& e) {
            // 长轮询在 suspend 期间无新消息触发客户端超时属正常行为：broker 会把
            // suspend 时间钳制到自身 brokerSuspendMaxTimeMillis，忽略客户端下发值，
            // 故空闲队列会周期性超时。非错误，仅 debug，避免污染运行日志。
            logger_debug("pull long-poll timeout for " + mq.toString()
                         + " (benign, will retry): " + e.what());
            continue;
        } catch (const std::exception& e) {
            logger_debug("pull error for " + mq.toString() + ": " + e.what());
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        {
            std::lock_guard<std::mutex> lk(lock_);
            if (pending_.find(key) == pending_.end()) {
                pending_[key] = std::deque<MessageExt>();
                mqMap_[key] = mq;
            }
        }
        if (result.status == PullStatus::FOUND && !result.msgFoundList.empty()) {
            std::lock_guard<std::mutex> lk(lock_);
            std::deque<MessageExt>& dq = pending_[key];
            for (const MessageExt& m : result.msgFoundList) {
                dq.push_back(m);
            }
        }
        // 拉取游标推进到 nextBeginOffset；"已消费位点"由 consumeOffsetTable_ 跟踪并持久化
        if (result.nextBeginOffset >= 0) {
            std::lock_guard<std::mutex> lk(lock_);
            offsetTable_[key] = result.nextBeginOffset;
        }
    }
}

void DefaultMQPushConsumer::dispatchLoop() {
    while (!stop_.load()) {
        bool progressed = false;
        std::vector<std::string> keys;
        {
            std::lock_guard<std::mutex> lk(lock_);
            for (const auto& kv : pending_) {
                keys.push_back(kv.first);
            }
        }
        for (const std::string& key : keys) {
            if (stop_.load() || !started_.load()) return;
            MessageQueue mq;
            {
                std::lock_guard<std::mutex> lk(lock_);
                auto it = mqMap_.find(key);
                if (it == mqMap_.end()) continue;
                mq = it->second;
            }
            std::vector<MessageExt> batch;
            {
                std::lock_guard<std::mutex> lk(lock_);
                auto it = pending_.find(key);
                if (it == pending_.end() || it->second.empty()) continue;
                const size_t n = static_cast<size_t>(
                    std::min<size_t>(it->second.size(),
                                     static_cast<size_t>(std::max(1, consumeMessageBatchMaxSize_))));
                for (size_t i = 0; i < n; ++i) {
                    batch.push_back(it->second.front());
                    it->second.pop_front();
                }
            }
            try {
                bool done = consumeBatch(key, mq, batch);
                progressed = progressed || done;
            } catch (const std::exception& e) {
                // 分发路径意外异常：批次塞回队首，稍后重试（不要让它杀死分发线程）
                logger_warn("dispatch batch error (will retry): " + std::string(e.what()));
                std::lock_guard<std::mutex> lk(lock_);
                auto it = pending_.find(key);
                if (it != pending_.end()) {
                    for (auto rit = batch.rbegin(); rit != batch.rend(); ++rit) {
                        it->second.push_front(*rit);
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        if (!progressed) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
}

bool DefaultMQPushConsumer::consumeBatch(const std::string& key, const MessageQueue& mq,
                                         const std::vector<MessageExt>& batch) {
    const bool broadcast = (messageModel_ == MessageModel::BROADCASTING);
    // ---- 顺序消费（Java ConsumeMessageOrderlyService）----
    if (isOrderly()) {
        auto* orderly = static_cast<MessageListenerOrderly*>(messageListener_.get());
        ConsumeOrderlyContext ctx(mq);
        ConsumeOrderlyStatus status;
        try {
            status = orderly->consumeMessage(batch, ctx);
        } catch (const std::exception& e) {
            // Java 顺序消费：异常 → 不提交 offset，原地重试
            logger_debug(std::string("orderly listener error (retry in place): ") + e.what());
            status = ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
        if (status == ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT) {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = pending_.find(key);
            if (it != pending_.end()) {
                for (auto rit = batch.rbegin(); rit != batch.rend(); ++rit) {
                    it->second.push_front(*rit);
                }
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(suspendCurrentQueueTimeMillis_));
            return false;
        }
        advanceConsumeOffset(key, batch);
        consumedCount_.fetch_add(static_cast<int64_t>(batch.size()));
        return true;
    }
    // ---- 并发消费（Java ConsumeMessageConcurrentlyService$ConsumeRequest.run）----
    auto* conc = static_cast<MessageListenerConcurrently*>(messageListener_.get());
    ConsumeConcurrentlyContext ctx(mq);
    ConsumeConcurrentlyStatus status;
    try {
        status = conc->consumeMessage(batch, ctx);
    } catch (const std::exception& e) {
        // Java：消费抛异常按 RECONSUME_LATER 处理
        logger_debug(std::string("listener error, treat as RECONSUME_LATER: ") + e.what());
        status = ConsumeConcurrentlyStatus::RECONSUME_LATER;
    }
    if (status == ConsumeConcurrentlyStatus::CONSUME_SUCCESS) {
        advanceConsumeOffset(key, batch);
        consumedCount_.fetch_add(static_cast<int64_t>(batch.size()));
        return true;
    }
    // RECONSUME_LATER：广播模式不回投（仅告警，位点前进，重启后不重投）；
    // 集群模式回投 %RETRY%topic（延迟梯度 3+reconsumeTimes，超限由 broker 转 %DLQ%）
    if (broadcast) {
        logger_warn("BROADCASTING: message consume failed, no redelivery: "
                    + std::to_string(batch.size()) + " msgs in " + mq.toString());
        advanceConsumeOffset(key, batch);
        consumedCount_.fetch_add(static_cast<int64_t>(batch.size()));
        return true;
    }
    if (sendBackBatch(batch, ctx)) {
        advanceConsumeOffset(key, batch);
        consumedCount_.fetch_add(static_cast<int64_t>(batch.size()));
        return true;
    }
    // 回投失败：批次塞回队首稍后重试（Java 中这些消息不从 ProcessQueue 移除）
    {
        std::lock_guard<std::mutex> lk(lock_);
        auto it = pending_.find(key);
        if (it != pending_.end()) {
            for (auto rit = batch.rbegin(); rit != batch.rend(); ++rit) {
                it->second.push_front(*rit);
            }
        }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    return false;
}

bool DefaultMQPushConsumer::sendBackBatch(const std::vector<MessageExt>& batch,
                                          const ConsumeConcurrentlyContext& ctx) {
    bool ok = true;
    for (const MessageExt& msg : batch) {
        try {
            // Java：delayLevelWhenNextConsume == 0 → 3 + reconsumeTimes
            //（reconsumeTimes 在 MessageExt 线上格式第 13 字段，broker 重投时 +1）
            int32_t delayLevel = ctx.delayLevelWhenNextConsume;
            if (delayLevel == 0) {
                delayLevel = 3 + msg.getReconsumeTimes();
            }
            sendMessageBack(msg, delayLevel);
        } catch (const std::exception& e) {
            logger_debug("send message back failed for msg " + msg.msgId + ": " + e.what());
            ok = false;
        }
    }
    return ok;
}

void DefaultMQPushConsumer::advanceConsumeOffset(const std::string& key,
                                                 const std::vector<MessageExt>& batch) {
    int64_t nextOffset = 0;
    for (const MessageExt& m : batch) {
        if (m.queueOffset + 1 > nextOffset) {
            nextOffset = m.queueOffset + 1;
        }
    }
    std::lock_guard<std::mutex> lk(lock_);
    auto it = consumeOffsetTable_.find(key);
    if (it == consumeOffsetTable_.end() || it->second < nextOffset) {
        consumeOffsetTable_[key] = nextOffset;
    }
}

// ---------------------------------------------------------------- 位点持久化
void DefaultMQPushConsumer::offsetPersistLoop() {
    // Java MQClientInstance.startScheduledTask：persistAllConsumerOffset 每 5s
    while (!stop_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        if (stop_.load() || !started_.load()) return;
        try {
            persistOffsetsOnce();
        } catch (const std::exception& e) {
            logger_debug(std::string("persist offsets error: ") + e.what());
        }
    }
}

void DefaultMQPushConsumer::persistOffsetsOnce() {
    if (messageModel_ == MessageModel::BROADCASTING) {
        saveLocalOffsets();
        return;
    }
    if (mqClient_ == nullptr) return;
    std::vector<std::pair<std::string, int64_t>> items;
    {
        std::lock_guard<std::mutex> lk(lock_);
        for (const auto& kv : consumeOffsetTable_) {
            items.emplace_back(kv.first, kv.second);
        }
    }
    for (const auto& kv : items) {
        auto it = mqMap_.find(kv.first);
        if (it == mqMap_.end()) continue;
        try {
            mqClient_->updateConsumerOffset(consumerGroup_, it->second, kv.second);
        } catch (const std::exception& e) {
            logger_debug("update consumer offset failed for " + it->second.toString()
                         + ": " + e.what());
        }
    }
}

std::string DefaultMQPushConsumer::localOffsetPath() const {
    // Java LocalFileOffsetStore：$HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json
    const char* home = std::getenv("HOME");
    std::string base = (home != nullptr && *home != '\0') ? std::string(home) : std::string(".");
    return base + "/.rocketmq_offsets/" + (clientId_.empty() ? "DEFAULT" : clientId_)
           + "/" + consumerGroup_ + "/offsets.json";
}

void DefaultMQPushConsumer::saveLocalOffsets() {
    std::map<std::string, int64_t> items;
    {
        std::lock_guard<std::mutex> lk(lock_);
        items = consumeOffsetTable_;
    }
    std::string path = localOffsetPath();
    std::string dir = path.substr(0, path.find_last_of('/'));
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    JsonValue root = JsonValue::makeObject();
    for (const auto& kv : items) {
        root.set(kv.first, JsonValue::makeInt(kv.second));
    }
    std::ofstream f(path, std::ios::trunc);
    if (f.is_open()) {
        f << root.dump();
    }
}

std::map<std::string, int64_t> DefaultMQPushConsumer::loadLocalOffsets() const {
    std::ifstream f(localOffsetPath());
    std::map<std::string, int64_t> out;
    if (!f.is_open()) return out;
    std::string text((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    JsonValue root;
    if (!jsonParse(text, root)) return out;
    for (const auto& kv : root.objectItems()) {
        out[kv.first] = kv.second.intValue();
    }
    return out;
}

// ---------------------------------------------------------------- 顺序消费队列锁
void DefaultMQPushConsumer::lockLoop() {
    if (!isOrderly() || messageModel_ == MessageModel::BROADCASTING) {
        return;
    }
    // Java ConsumeMessageOrderlyService.lockMQ：每 20s 批量锁分到的队列；
    // 启动时立刻尝试一次，避免首个 20s 空转
    while (!stop_.load()) {
        try {
            std::vector<MessageQueue> mqs = assignedQueues();
            if (!mqs.empty()) {
                std::vector<MessageQueue> ok =
                    mqClient_->lockBatchMq(consumerGroup_, clientId_, mqs);
                std::set<std::string> okKeys;
                for (const MessageQueue& mq : ok) {
                    okKeys.insert(offsetKey(mq));
                }
                std::lock_guard<std::mutex> lk(lock_);
                lockOk_ = std::move(okKeys);
                logger_debug("lock_batch_mq: " + std::to_string(lockOk_.size()) + "/"
                             + std::to_string(mqs.size()) + " queues locked");
            }
        } catch (const std::exception& e) {
            logger_debug(std::string("lock mq error: ") + e.what());
        }
        // 等待 20s（期间响应 stop）
        for (int i = 0; i < 200 && !stop_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
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
            // %RETRY%topic 在首次回投前无路由，属预期路径，debug 即可
            logger_debug("assigned_queues: skip topic " + topic + ": " + e.what());
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
    if (messageModel_ == MessageModel::BROADCASTING) {
        // 广播模式：offset 只存本地（对齐 Java LocalFileOffsetStore）
        std::map<std::string, int64_t> stored = loadLocalOffsets();
        auto it = stored.find(offsetKey(mq));
        if (it != stored.end()) {
            return it->second;
        }
        if (consumeFromWhere_ == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET) {
            return c.getMinOffset(mq);
        }
        return c.getMaxOffset(mq);
    }
    // 集群模式：先查 broker 上已提交的位点（对齐 Java RemoteBrokerOffsetStore.readOffset）
    try {
        int64_t stored = 0;
        if (c.queryConsumerOffset(consumerGroup_, mq, stored, 5000, std::string(),
                                  /*setZeroIfNotFound=*/false)) {
            return stored;
        }
    } catch (const std::exception& e) {
        logger_debug("query consumer offset for " + mq.toString() + " not found: " + e.what());
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
    // Java：maxReconsumeTimes == -1 时按 16 传给 broker（超限由 broker 转 %DLQ%）
    header->maxReconsumeTimes = maxReconsumeTimes_ == -1 ? 16 : maxReconsumeTimes_;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::CONSUMER_SEND_MSG_BACK, header);
    RemotingCommand response = c.remotingClient().invokeSync(addr, request, 5000);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
    return true;
}

}  // namespace rocketmq
