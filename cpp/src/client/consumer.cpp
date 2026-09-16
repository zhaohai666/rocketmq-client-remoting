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
        startMillis_ = UtilAll::currentTimeMillis();
        stop_.store(false);
        started_.store(true);
    }

    // 注册 broker 主动通知：消费者上下线时立刻重算分配（对齐 Java ClientRemotingProcessor
    // → NOTIFY_CONSUMER_IDS_CHANGED → rebalanceImmediately）。
    mqClient_->remotingClient().registerProcessor(
        RequestCode::NOTIFY_CONSUMER_IDS_CHANGED,
        [this](const RemotingCommand& cmd, const std::string&) {
            this->onConsumerIdsChanged(cmd);
        });

    // 对齐 Java DefaultMQPushConsumerImpl.start 的顺序：
    // 拉路由（登记 topic 在用 + 填 broker 地址表）→ 发心跳（broker 先认识本消费者）
    // → 立即 rebalance → 起消费线程。心跳必须在 rebalance 之前：rebalance 要向 broker
    // 查消费者列表（GET_CONSUMER_LIST_BY_GROUP），broker 只有收到心跳才登记本 clientId。
    for (const std::string& t : subscribedTopics()) {
        mqClient_->registerTopicInUse(t);
        try {
            mqClient_->getTopicPublishInfo(t);
        } catch (const std::exception& e) {
            logger_debug("refresh route for " + t + " failed: " + e.what());
        }
    }
    try {
        sendHeartbeatToAllBroker();
    } catch (const std::exception& e) {
        logger_debug("initial heartbeat failed: " + std::string(e.what()));
    }
    // 首轮分配必须同步完成：否则拉取线程会在空分配集上白转，直到第一轮 rebalance 才生效。
    try {
        doRebalance();
    } catch (const std::exception& e) {
        logger_debug("initial rebalance failed: " + std::string(e.what()));
    }

    // 拉取：每队列一个线程（并发长轮询，避免空队列 suspend 阻塞其他队列投递）
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
    for (std::thread& t : retiredThreads_) {
        if (t.joinable()) t.join();
    }
    retiredThreads_.clear();
    if (dispatchThread_.joinable()) dispatchThread_.join();
    if (persistThread_.joinable()) persistThread_.join();
    if (lockThread_.joinable()) lockThread_.join();
    if (rebalanceThread_.joinable()) rebalanceThread_.join();
    // 优雅注销（对齐 Java MQClientInstance.unregisterClient）：关闭连接**之前**对
    // brokerAddrTable 里所有 broker 发 UNREGISTER_CLIENT(35)，broker 端立刻摘除本 clientId，
    // 不必等心跳超时（默认 ~120s）——否则这段时间内消费者变更通知仍可能发往已退出的实例。
    if (mqClient_) {
        try {
            mqClient_->unregisterClientAllBrokers(clientId_, "", consumerGroup_);
        } catch (const std::exception& e) {
            logger_debug("unregister on shutdown failed: " + std::string(e.what()));
        }
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
    // 对齐 Java RebalanceImpl.updateProcessQueueTableInRebalance：
    // 按当前分配集同步拉取线程，并为被撤销的队列做收尾（持久化已消费位点、
    // 丢弃在途缓冲、顺序消费集群模式解锁）。少任何一步，被撤销队列里的在途消息
    // 会被旧实例继续消费，与新属主重复（同组多实例重复消费的根因）。
    std::vector<MessageQueue> queues = assignedQueues();
    std::map<std::string, MessageQueue> current;
    for (const MessageQueue& mq : queues) {
        current[offsetKey(mq)] = mq;
    }
    std::vector<std::pair<std::string, MessageQueue>> toStart;
    std::vector<std::pair<MessageQueue, int64_t>> revoked;
    {
        std::lock_guard<std::mutex> lk(lock_);
        // 1. 新分配的队列：起拉取线程
        for (const auto& kv : current) {
            if (pullThreads_.find(kv.first) == pullThreads_.end()) {
                toStart.emplace_back(kv.first, kv.second);
            }
        }
        // 2. 被撤销的队列：清状态 + 收集 (mq, 已消费位点)，把旧线程移到 retiredThreads_
        //    等待其自然退出（线程循环里 ownsQueue 返回 false 即退出）
        for (auto it = pullThreads_.begin(); it != pullThreads_.end();) {
            if (current.find(it->first) == current.end()) {
                MessageQueue mq;
                auto mit = mqMap_.find(it->first);
                if (mit != mqMap_.end()) mq = mit->second;
                int64_t off = -1;
                auto oit = consumeOffsetTable_.find(it->first);
                if (oit != consumeOffsetTable_.end()) off = oit->second;
                revoked.emplace_back(mq, off);
                retiredThreads_.push_back(std::move(it->second));
                mqMap_.erase(it->first);
                pending_.erase(it->first);
                lockOk_.erase(it->first);
                offsetTable_.erase(it->first);
                consumeOffsetTable_.erase(it->first);
                it = pullThreads_.erase(it);
            } else {
                ++it;
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
    // 网络/落盘在锁外做
    if (!revoked.empty()) {
        onQueuesRevoked(revoked);
    }
}

void DefaultMQPushConsumer::rebalanceLoop() {
    // 周期重算分配（对齐 Java RebalanceService 默认 20s），或被 NOTIFY_CONSUMER_IDS_CHANGED
    // 通知时立即重算（rebalanceNow_ 置位）。启动后 60s 内且当前无任何分配时缩短为 2s 重试：
    // 消费者可能先于 topic 被创建启动，此时真实路由还拉不到——消费端不做默认 topic 兜底，
    // 死等 20s 会长时间不消费；快速重试只在启动后 60s 内生效，避免长期订阅不存在 topic 时
    // 高频打 NameServer。
    while (!stop_.load()) {
        bool fast = false;
        {
            std::lock_guard<std::mutex> lk(lock_);
            if (assignedQueues_.empty()
                && (UtilAll::currentTimeMillis() - startMillis_) < 60000) {
                fast = true;
            }
        }
        {
            std::unique_lock<std::mutex> lk(rebalanceMutex_);
            rebalanceCv_.wait_for(lk, std::chrono::milliseconds(fast ? 2000 : 20000),
                                  [this]() { return rebalanceNow_.load() || stop_.load(); });
            rebalanceNow_.store(false);
        }
        if (stop_.load() || !started_.load()) return;
        try {
            maybeSendHeartbeat();
            doRebalance();
        } catch (const std::exception& e) {
            logger_debug(std::string("rebalance error: ") + e.what());
        }
    }
}

void DefaultMQPushConsumer::queuePullLoop(const MessageQueue& mq) {
    MQClientInstance& c = client();
    const bool orderly = isOrderly();
    const std::string key = offsetKey(mq);
    while (!stop_.load() && started_.load()) {
        // 长轮询期间被 rebalance 撤走（队列或换了拉取线程）即失效：直接退出本线程，
        // 由新属主从我们最后持久化的位点接手，避免两实例重复消费同一条消息。
        if (!ownsQueue(key)) {
            return;
        }
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
        // 入队与「是否仍持有该队列」必须一致：长轮询期间被 rebalance 撤走的队列，这批消息按
        // Java 语义（ProcessQueue.isDropped()）直接丢弃——不消费、不推进位点，由新属主从我们
        // 最后持久化的位点重投，否则两实例会重复消费同一条消息。
        if (!ownsQueue(key)) {
            logger_debug("queue " + mq.toString()
                         + " revoked during pull, discard fetched messages");
            return;
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
    // 分发前把重投消息的 topic 还原成业务原始 topic（对应 Java resetRetryAndNamespace）：
    // broker 回投的消息实际写在 %RETRY%group，原始 topic 在 RETRY_TOPIC 属性里，不还原
    // 用户按 topic 分支的代码会走错。
    std::vector<MessageExt> restored = batch;
    resetRetryTopicAndNamespace(restored);
    const bool broadcast = (messageModel_ == MessageModel::BROADCASTING);
    // ---- 顺序消费（Java ConsumeMessageOrderlyService）----
    if (isOrderly()) {
        auto* orderly = static_cast<MessageListenerOrderly*>(messageListener_.get());
        ConsumeOrderlyContext ctx(mq);
        ConsumeOrderlyStatus status;
        try {
            status = orderly->consumeMessage(restored, ctx);
        } catch (const std::exception& e) {
            // Java 顺序消费：异常 → 不提交 offset，原地重试
            logger_debug(std::string("orderly listener error (retry in place): ") + e.what());
            status = ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
        if (status == ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT) {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = pending_.find(key);
            if (it != pending_.end()) {
                for (auto rit = restored.rbegin(); rit != restored.rend(); ++rit) {
                    it->second.push_front(*rit);
                }
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(suspendCurrentQueueTimeMillis_));
            return false;
        }
        advanceConsumeOffset(key, restored);
        consumedCount_.fetch_add(static_cast<int64_t>(restored.size()));
        return true;
    }
    // ---- 并发消费（Java ConsumeMessageConcurrentlyService$ConsumeRequest.run）----
    auto* conc = static_cast<MessageListenerConcurrently*>(messageListener_.get());
    ConsumeConcurrentlyContext ctx(mq);
    ConsumeConcurrentlyStatus status;
    try {
        status = conc->consumeMessage(restored, ctx);
    } catch (const std::exception& e) {
        // Java：消费抛异常按 RECONSUME_LATER 处理
        logger_debug(std::string("listener error, treat as RECONSUME_LATER: ") + e.what());
        status = ConsumeConcurrentlyStatus::RECONSUME_LATER;
    }
    if (status == ConsumeConcurrentlyStatus::CONSUME_SUCCESS) {
        advanceConsumeOffset(key, restored);
        consumedCount_.fetch_add(static_cast<int64_t>(restored.size()));
        return true;
    }
    // RECONSUME_LATER：广播模式不回投（仅告警，位点前进，重启后不重投）；
    // 集群模式回投 %RETRY%topic（延迟梯度 3+reconsumeTimes，超限由 broker 转 %DLQ%）
    if (broadcast) {
        logger_warn("BROADCASTING: message consume failed, no redelivery: "
                    + std::to_string(restored.size()) + " msgs in " + mq.toString());
        advanceConsumeOffset(key, restored);
        consumedCount_.fetch_add(static_cast<int64_t>(restored.size()));
        return true;
    }
    if (sendBackBatch(restored, ctx)) {
        advanceConsumeOffset(key, restored);
        consumedCount_.fetch_add(static_cast<int64_t>(restored.size()));
        return true;
    }
    // 回投失败：批次塞回队首稍后重试（Java 中这些消息不从 ProcessQueue 移除）
    {
        std::lock_guard<std::mutex> lk(lock_);
        auto it = pending_.find(key);
        if (it != pending_.end()) {
            for (auto rit = restored.rbegin(); rit != restored.rend(); ++rit) {
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
    // 返回真实 rebalance 计算出的分配集（doRebalance 写入 assignedQueues_）。
    // 不再返回「订阅 topic 的全部队列」——那正是同组多实例重复消费的根源。
    std::lock_guard<std::mutex> lk(lock_);
    return assignedQueues_;
}

// ---------------------------------------------------------------- 真实 rebalance
void DefaultMQPushConsumer::doRebalance() {
    // 对齐 Java RebalanceImpl.rebalanceByTopic：BROADCASTING 全给自己；CLUSTERING 查 broker
    // 消费者列表 → 排序 → AllocateMessageQueueAveragely → 取本实例那一份。查不到消费者列表时
    // 保留现有分配（Java 仅告警），绝不回退成"独占全部队列"（否则同组多实例互相重复消费）。
    if (mqClient_ == nullptr) return;
    MQClientInstance& c = *mqClient_;
    std::vector<std::string> topics;
    {
        std::lock_guard<std::mutex> lk(lock_);
        for (const auto& kv : subscriptionData_) {
            topics.push_back(kv.first);
        }
    }
    std::set<std::string> was;
    {
        std::lock_guard<std::mutex> lk(lock_);
        for (const MessageQueue& mq : assignedQueues_) {
            was.insert(offsetKey(mq));
        }
    }
    std::vector<MessageQueue> assigned;
    if (messageModel_ == MessageModel::BROADCASTING) {
        for (const std::string& topic : topics) {
            for (const MessageQueue& mq : allQueuesOfTopic(topic)) {
                assigned.push_back(mq);
            }
        }
    } else {
        for (const std::string& topic : topics) {
            std::vector<MessageQueue> mqAll = allQueuesOfTopic(topic);
            // mqAll 与 cidAll 都排序（对齐 Java：排序后才分配，否则不同实例算出不同结果）
            std::sort(mqAll.begin(), mqAll.end(),
                      [](const MessageQueue& a, const MessageQueue& b) {
                          return a.compareTo(b) < 0;
                      });
            if (mqAll.empty()) {
                continue;
            }
            std::vector<std::string> cidAll = c.getConsumerIdListByGroup(topic, consumerGroup_);
            if (cidAll.empty()) {
                logger_debug("rebalance: no consumer id list for " + consumerGroup_ + "/" + topic
                             + ", keep current assignment");
                std::lock_guard<std::mutex> lk(lock_);
                for (const MessageQueue& mq : assignedQueues_) {
                    if (mq.topic == topic) assigned.push_back(mq);
                }
                continue;
            }
            std::sort(cidAll.begin(), cidAll.end());
            std::vector<MessageQueue> got =
                allocateMessageQueueAveragely(consumerGroup_, clientId_, mqAll, cidAll);
            assigned.insert(assigned.end(), got.begin(), got.end());
        }
    }
    {
        std::lock_guard<std::mutex> lk(lock_);
        assignedQueues_ = assigned;
    }
    std::set<std::string> now;
    for (const MessageQueue& mq : assigned) {
        now.insert(offsetKey(mq));
    }
    if (now != was) {
        logger_info("rebalance result changed, group=" + consumerGroup_
                    + " clientId=" + clientId_ + " assigned=" + std::to_string(assigned.size()));
    }
    // 新分配的队列**立刻**解析初始位点写入 offsetTable_（对齐 Java
    // updateProcessQueueTableInRebalance → computePullFromWhereWithException →
    // offsetStore.updateOffset）。不能留到第一次拉取时才惰性解析：CONSUME_FROM_LAST_OFFSET 语义
    // 是"分配时刻的最新位点"，惰性解析会跳过「分配之后、首次拉取之前」新产生的消息。
    for (const MessageQueue& mq : assigned) {
        std::string key = offsetKey(mq);
        if (was.count(key)) {
            continue;
        }
        SubscriptionData sub;
        {
            std::lock_guard<std::mutex> lk(lock_);
            if (offsetTable_.count(key)) continue;
            auto it = subscriptionData_.find(mq.topic);
            if (it == subscriptionData_.end()) continue;
            sub = it->second;
        }
        try {
            int64_t off = resolveInitialOffset(mq, sub);
            std::lock_guard<std::mutex> lk(lock_);
            if (!offsetTable_.count(key)) {
                offsetTable_[key] = off;
            }
        } catch (const std::exception& e) {
            logger_debug("resolve initial offset for " + mq.toString() + " failed: " + e.what());
        }
    }
    rebalancePullThreads();
}

std::vector<MessageQueue> DefaultMQPushConsumer::allQueuesOfTopic(const std::string& topic) {
    // 对齐 Java RebalanceImpl.topicSubscribeInfoTable：topic 路由里的全部队列。
    std::vector<MessageQueue> out;
    if (mqClient_ == nullptr) return out;
    try {
        std::shared_ptr<TopicPublishInfo> publish = mqClient_->getTopicPublishInfo(topic);
        out.reserve(publish->msgQueueList.size());
        for (const MessageQueue& q : publish->msgQueueList) {
            out.emplace_back(q.topic, q.brokerName, q.queueId);
        }
    } catch (const std::exception& e) {
        logger_debug("rebalance: no route for topic " + topic + ": " + e.what());
    }
    return out;
}

std::vector<MessageQueue> DefaultMQPushConsumer::allocateMessageQueueAveragely(
    const std::string& consumerGroup, const std::string& currentCid,
    const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) {
    (void)consumerGroup;
    std::vector<MessageQueue> result;
    int mqCount = static_cast<int>(mqAll.size());
    int cidCount = static_cast<int>(cidAll.size());
    if (mqCount == 0 || cidCount == 0) return result;
    auto it = std::find(cidAll.begin(), cidAll.end(), currentCid);
    if (it == cidAll.end()) return result;  // 本实例不在消费组列表里 → 不分配
    int index = static_cast<int>(it - cidAll.begin());
    // 对齐 Java AllocateMessageQueueAveragely
    int mod = mqCount % cidCount;
    int averageSize = (mqCount <= cidCount)
                          ? 1
                          : (mod > 0 && index < mod ? mqCount / cidCount + 1 : mqCount / cidCount);
    int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
    int range = std::min(averageSize, mqCount - startIndex);
    for (int i = 0; i < range; ++i) {
        if (startIndex + i < mqCount) {
            result.push_back(mqAll[startIndex + i]);
        }
    }
    return result;
}

bool DefaultMQPushConsumer::ownsQueue(const std::string& key) const {
    // 本拉取线程是否仍持有该队列（rebalance 撤走或换了拉取线程后即失效）。
    std::lock_guard<std::mutex> lk(lock_);
    auto it = pullThreads_.find(key);
    if (it == pullThreads_.end()) return false;
    return it->second.get_id() == std::this_thread::get_id();
}

void DefaultMQPushConsumer::onQueuesRevoked(
    const std::vector<std::pair<MessageQueue, int64_t>>& revoked) {
    // 对齐 Java RebalanceImpl.removeUnnecessaryMessageQueue
    if (messageModel_ == MessageModel::BROADCASTING) {
        // 广播模式位点只存本地
        saveLocalOffsets();
        return;
    }
    if (mqClient_ == nullptr) return;
    std::vector<MessageQueue> toUnlock;
    for (const auto& kv : revoked) {
        const MessageQueue& mq = kv.first;
        int64_t off = kv.second;
        // 仅该队列的已消费位点：一条 UPDATE_CONSUMER_OFFSET(15)，info 级别只记一次摘要
        if (off >= 0) {
            try {
                mqClient_->updateConsumerOffset(consumerGroup_, mq, off);
            } catch (const std::exception& e) {
                logger_debug("persist offset on revoke failed for " + mq.toString() + ": "
                             + e.what());
            }
        }
        if (isOrderly()) {
            toUnlock.push_back(mq);
        }
    }
    if (!toUnlock.empty()) {
        try {
            mqClient_->unlockBatchMq(consumerGroup_, clientId_, toUnlock);
        } catch (const std::exception& e) {
            logger_debug("unlock on revoke failed: " + std::string(e.what()));
        }
    }
    logger_info("queues revoked, group=" + consumerGroup_
                + " count=" + std::to_string(revoked.size()));
}

void DefaultMQPushConsumer::onConsumerIdsChanged(const RemotingCommand& cmd) {
    (void)cmd;
    // broker 通知消费组实例变化 → 立即重算（对齐 Java rebalanceImmediately）。
    rebalanceNow_.store(true);
    rebalanceCv_.notify_all();
}

void DefaultMQPushConsumer::resetRetryTopicAndNamespace(std::vector<MessageExt>& msgs) {
    // 对应 Java DefaultMQPushConsumerImpl.resetRetryAndNamespace（分发前调用）。
    // 本客户端不使用 namespace，故只做 topic 还原。
    const std::string groupTopic = MixAll::getRetryTopic(consumerGroup_);
    for (MessageExt& msg : msgs) {
        std::string retryTopic = msg.getProperty(MessageConst::PROPERTY_RETRY_TOPIC);
        if (!retryTopic.empty() && msg.topic == groupTopic) {
            msg.setTopic(retryTopic);
        }
    }
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
        // Java RebalancePushImpl：首次消费且无已提交位点时，%RETRY% 主题从 0 开始
        // （重试消息要全量重试），而不是从最大位点跳过。
        if (MixAll::isRetryTopic(mq.topic)) {
            return 0;
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

std::vector<std::string> DefaultMQPushConsumer::assignedQueueKeys() const {
    // 当前分给本实例的队列 key 列表（真机验证"同组两实例不重不漏"用）。
    // key 格式与 offsetKey 一致：topic + brokerName + queueId。
    std::lock_guard<std::mutex> lk(lock_);
    std::vector<std::string> out;
    out.reserve(assignedQueues_.size());
    for (const MessageQueue& mq : assignedQueues_) {
        out.push_back(offsetKey(mq));
    }
    return out;
}

std::vector<std::string> DefaultMQPushConsumer::consumerIdListOfGroup(
    const std::string& topic) const {
    // 查消费组在某 topic 上的全部 clientId（对应 Java findConsumerIdList），用于验证多实例注册。
    if (mqClient_ == nullptr) return {};
    try {
        return mqClient_->getConsumerIdListByGroup(topic, consumerGroup_);
    } catch (...) {
        return {};
    }
}

}  // namespace rocketmq
