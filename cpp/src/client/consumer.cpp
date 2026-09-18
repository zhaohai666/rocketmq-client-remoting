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
#include "rocketmq/client/trace_hook.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/extra_info.h"
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
    // 便捷方法：min 与 max 一起设（Java 4.x setConsumeThreadNums 的语义）。
    // Java 5.x 已拆成 setConsumeThreadMin/Max，本方法保留是为了兼容既有调用点。
    const int32_t v = std::max(1, n);
    consumeThreadMin_ = v;
    consumeThreadMax_ = v;
    corePoolSize_ = v;
    if (popConsumeExecutor_) {
        popConsumeExecutor_->setCorePoolSize(v);
    }
}

void DefaultMQPushConsumer::setConsumeThreadMin(int32_t n) {
    consumeThreadMin_ = std::max(1, n);
    corePoolSize_ = consumeThreadMin_;
    if (popConsumeExecutor_) {
        popConsumeExecutor_->setCorePoolSize(corePoolSize_);
    }
}

void DefaultMQPushConsumer::setConsumeThreadMax(int32_t n) {
    consumeThreadMax_ = std::max(1, n);
    if (popConsumeExecutor_) {
        popConsumeExecutor_->setCorePoolSize(corePoolSize_);
    }
}

// ---------------------------------------------------------------- 消费线程弹性

bool DefaultMQPushConsumer::updateCorePoolSize(int32_t corePoolSize) {
    // Java AbstractConsumeMessageService:63-71 的守卫逐条照抄：
    //   ownsConsumeExecutor && corePoolSize > 0
    //       && corePoolSize <= Short.MAX_VALUE   (32767)
    //       && corePoolSize < consumeThreadMax
    // 任一条不满足就静默忽略（Java 也是静默 return，不抛异常）。
    // 本实现不支持外部注入执行器，ownsConsumeExecutor 恒为 true。
    if (corePoolSize <= 0 || corePoolSize > 32767 || corePoolSize >= consumeThreadMax_) {
        return false;
    }
    corePoolSize_ = corePoolSize;
    if (popConsumeExecutor_) {
        popConsumeExecutor_->setCorePoolSize(corePoolSize_);
    }
    return true;
}

int32_t DefaultMQPushConsumer::getCorePoolSize() const {
    if (popConsumeExecutor_) {
        return popConsumeExecutor_->getCorePoolSize();
    }
    return corePoolSize_;
}

int64_t DefaultMQPushConsumer::computeAccumulationTotal() const {
    std::lock_guard<std::mutex> lk(lock_);
    int64_t total = 0;
    for (const auto& kv : msgAccCntTable_) {
        total += kv.second;
    }
    return total;
}

int64_t DefaultMQPushConsumer::msgAccCnt(const std::string& key) const {
    std::lock_guard<std::mutex> lk(lock_);
    if (key.empty()) {
        int64_t total = 0;
        for (const auto& kv : msgAccCntTable_) {
            total += kv.second;
        }
        return total;
    }
    auto it = msgAccCntTable_.find(key);
    return it == msgAccCntTable_.end() ? 0 : it->second;
}

void DefaultMQPushConsumer::updateMsgAccCnt(const std::string& key,
                                           const std::vector<MessageExt>& msgs) {
    std::lock_guard<std::mutex> lk(lock_);
    updateMsgAccCntLocked(key, msgs);
}

void DefaultMQPushConsumer::updateMsgAccCntLocked(const std::string& key,
                                                 const std::vector<MessageExt>& msgs) {
    // Java ProcessQueue.java:148-158：
    //   long accTotal = Long.parseLong(msg.getProperty(MAX_OFFSET)) - msg.getQueueOffset();
    //   if (accTotal > 0) this.msgAccCnt = accTotal;
    // 取**本批最后一条**；属性缺失/非数字/非正一律不更新（Java 里 parse 失败会抛，
    // 但 broker 恒会带上该属性，这里做容错以免脏数据打断拉取线程）。
    if (msgs.empty()) return;
    const MessageExt& last = msgs.back();
    const std::string maxOffset = last.getProperty(MessageConst::PROPERTY_MAX_OFFSET);
    if (maxOffset.empty()) return;
    int64_t parsed = 0;
    try {
        size_t pos = 0;
        parsed = std::stoll(maxOffset, &pos);
        if (pos != maxOffset.size()) return;
    } catch (const std::exception&) {
        return;
    }
    const int64_t accTotal = parsed - static_cast<int64_t>(last.queueOffset);
    if (accTotal > 0) {
        msgAccCntTable_[key] = accTotal;
    }
}

void DefaultMQPushConsumer::adjustThreadPool() {
    // ⚠ 在 Java 5.5.1 这是 no-op：真正被调的 consumeMessageService.incCorePoolSize() /
    // decCorePoolSize() 在 AbstractConsumeMessageService:70-75 是**空方法体**。
    // 这里保留阈值比较与日志，仅为让 msgAccCnt / 阈值配置可观测；**不要"修好"它**。
    const int64_t accTotal = computeAccumulationTotal();
    const int64_t threshold = adjustThreadPoolNumsThreshold_;
    const int64_t incThreshold = static_cast<int64_t>(static_cast<double>(threshold) * 1.0);
    const int64_t decThreshold = static_cast<int64_t>(static_cast<double>(threshold) * 0.8);
    if (accTotal >= incThreshold) {
        logger_debug("adjustThreadPool: acc=" + std::to_string(accTotal) + " >= incThreshold="
                     + std::to_string(incThreshold) + " (inc is a no-op upstream)");
    }
    if (accTotal < decThreshold) {
        logger_debug("adjustThreadPool: acc=" + std::to_string(accTotal) + " < decThreshold="
                     + std::to_string(decThreshold) + " (dec is a no-op upstream)");
    }
}

int32_t DefaultMQPushConsumer::consumeExecutorWorkers() const {
    return popConsumeExecutor_ ? popConsumeExecutor_->workerCount() : 0;
}

int32_t DefaultMQPushConsumer::consumeExecutorQueued() const {
    return popConsumeExecutor_ ? popConsumeExecutor_->queuedCount() : 0;
}

void DefaultMQPushConsumer::setMessageListener(std::shared_ptr<MessageListener> listener) {
    messageListener_ = std::move(listener);
}

// ---------------------------------------------------------------- 订阅
void DefaultMQPushConsumer::subscribe(const std::string& topic, const std::string& subExpression) {
    if (started_.load()) {
        throw MQClientException("consumer already started, cannot change configuration");
    }
    const std::string realTopic = NamespaceUtil::wrapNamespace(namespace_, topic);
    SubscriptionData sub = FilterAPI::buildSubscriptionData(realTopic, subExpression);
    std::lock_guard<std::mutex> lk(lock_);
    subscriptionData_[realTopic] = sub;
}

void DefaultMQPushConsumer::subscribe(const std::string& topic, const MessageSelector& selector) {
    if (started_.load()) {
        throw MQClientException("consumer already started, cannot change configuration");
    }
    const std::string realTopic = NamespaceUtil::wrapNamespace(namespace_, topic);
    SubscriptionData sub(realTopic, selector.expression);
    sub.expressionType = selector.type;
    if (selector.type == ExpressionType::TAG) {
        SubscriptionData built = FilterAPI::buildSubscriptionData(realTopic, selector.expression);
        sub.tagsSet = built.tagsSet;
    }
    std::lock_guard<std::mutex> lk(lock_);
    subscriptionData_[realTopic] = sub;
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
        // 静态地址与动态取址（ROCKETMQ_NAMESRV_DOMAIN）二选一必须可用
        if (nameServerAddrs_.empty() && !DefaultTopAddressing::isConfigured()) {
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
        // 对齐 Java DefaultMQPushConsumer.start()：把消费组套上命名空间（ns%group），
        // 之后所有面向 broker 的组名（心跳 / rebalance / 位点 / 锁 / 回投）都用包装后的值。
        if (!namespace_.empty()) {
            consumerGroup_ = NamespaceUtil::wrapNamespace(namespace_, consumerGroup_);
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
                                             /*invokeTimeoutMillis=*/pullTimeoutMillis_,
                                             tlsEnable_));
        mqClient_->start();
        // 动态 name server：实例启动时可能已从地址服务器拿到地址，回填到本消费者
        // （Java 由共享的 ClientConfig 天然同步）
        if (nameServerAddrs_.empty() && !mqClient_->nameServerAddrs().empty()) {
            nameServerAddrs_ = mqClient_->nameServerAddrs();
        }
        // ACL 鉴权钩子：必须在首包（路由拉取 / 心跳 / rebalance）发出之前绑定。
        if (rpcHook_ && !mqClient_->registerRPCHook(rpcHook_)) {
            logger_warn("consumer rpc hook ignored: MQClientInstance already has one (clientId="
                        + clientId_ + ")");
        }
        // POP 消费执行器必须在 rebalance（会立刻起每队列 POP 循环）之前建好，
        // 否则循环弹出消息后无处投递（对齐 Java 在 service 构造时就建 consumeExecutor）。
        if (popMode_) {
            popConsumeExecutor_ = std::make_shared<ConsumeExecutor>(
                std::max(1, corePoolSize_), std::max(1, consumeThreadMax_),
                /*keepAliveSeconds=*/60.0, "rmq-popconsume-" + consumerGroup_);
        }
        startMillis_ = UtilAll::currentTimeMillis();
        stop_.store(false);
        started_.store(true);
    }

    // 注册 broker 主动通知：消费者上下线时立刻重算分配（对齐 Java ClientRemotingProcessor
    // → NOTIFY_CONSUMER_IDS_CHANGED → rebalanceImmediately）。
    mqClient_->remotingClient().registerProcessor(
        RequestCode::NOTIFY_CONSUMER_IDS_CHANGED,
        [this](const RemotingCommand& cmd, const std::string&) -> std::optional<RemotingCommand> {
            this->onConsumerIdsChanged(cmd);
            // 通知类请求（broker 用 oneway 发），不需要回响应。
            return std::nullopt;
        });

    // 对应 Java ClientRemotingProcessor GET_CONSUMER_RUNNING_INFO(307)：
    // admin / broker 查询本消费者运行信息，回 ConsumerRunningInfo JSON body。
    mqClient_->remotingClient().registerProcessor(
        RequestCode::GET_CONSUMER_RUNNING_INFO,
        [this](const RemotingCommand& cmd, const std::string&) -> std::optional<RemotingCommand> {
            auto it = cmd.extFields.find("consumerGroup");
            const std::string group = it == cmd.extFields.end() ? std::string() : it->second;
            if (group != consumerGroup_) {
                // 与 Java 一致：组不匹配回 SYSTEM_ERROR（broker 端会打 warn）
                return RemotingCommand::createResponseCommand(
                    ResponseCode::SYSTEM_ERROR,
                    "consumerGroup not matched, expect " + consumerGroup_ + ", got " + group);
            }
            RemotingCommand resp = RemotingCommand::createResponseCommand(
                ResponseCode::SUCCESS, std::string());
            resp.body = consumerRunningInfo().encode();
            return resp;
        });

    // RESET_CONSUMER_CLIENT_OFFSET(220)：broker 用 invokeOneway 发，无需应答。
    // 重置逻辑里会触发 rebalance（lock/unlock/batch 等 invokeSync），不能在读线程上
    // 同步跑（自死锁，见本文件顶部工程点）——丢到后台线程，立即返回 nullopt。
    mqClient_->remotingClient().registerProcessor(
        RequestCode::RESET_CONSUMER_CLIENT_OFFSET,
        [this](const RemotingCommand& cmd, const std::string&) -> std::optional<RemotingCommand> {
            auto git = cmd.extFields.find("group");
            const std::string group = git == cmd.extFields.end() ? std::string() : git->second;
            if (group != consumerGroup_) return std::nullopt;  // oneway，不回响应
            std::string topic;
            auto tit = cmd.extFields.find("topic");
            if (tit != cmd.extFields.end()) topic = tit->second;
            std::map<MessageQueue, int64_t> table;
            if (!cmd.body.empty()) {
                ResetOffsetBody body;
                if (ResetOffsetBody::decode(cmd.body, body)) table = std::move(body.offsetTable);
            }
            auto tablePtr = std::make_shared<std::map<MessageQueue, int64_t>>(std::move(table));
            std::thread([this, topic, tablePtr]() {
                try {
                    this->resetOffset(topic, *tablePtr);
                } catch (const std::exception& e) {
                    logger_warn("reset offset failed (group=" + consumerGroup_
                                + " topic=" + topic + "): " + e.what());
                } catch (...) {
                    logger_warn("reset offset failed (group=" + consumerGroup_
                                + " topic=" + topic + ")");
                }
            }).detach();
            return std::nullopt;
        });

    // GET_CONSUMER_STATUS_FROM_CLIENT(221)：admin 查询本消费者已消费位点表，
    // 回 GetConsumerStatusBody JSON body（messageQueueTable 内联对象键）。
    mqClient_->remotingClient().registerProcessor(
        RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT,
        [this](const RemotingCommand& cmd, const std::string&) -> std::optional<RemotingCommand> {
            auto git = cmd.extFields.find("group");
            const std::string group = git == cmd.extFields.end() ? std::string() : git->second;
            if (group != consumerGroup_) {
                // 与 Java 一致：组不匹配回 SYSTEM_ERROR（broker 端会打 warn）
                return RemotingCommand::createResponseCommand(
                    ResponseCode::SYSTEM_ERROR,
                    "consumerGroup not matched, expect " + consumerGroup_ + ", got " + group);
            }
            std::string topic;
            auto tit = cmd.extFields.find("topic");
            if (tit != cmd.extFields.end()) topic = tit->second;
            GetConsumerStatusBody body;
            body.messageQueueTable = getConsumerStatus(topic);
            RemotingCommand resp = RemotingCommand::createResponseCommand(
                ResponseCode::SUCCESS, std::string());
            resp.body = body.encode();
            return resp;
        });

    // CONSUME_MESSAGE_DIRECTLY(309)：broker 把一条消息推下来，要求本地真实消费一次。
    // 与 Python 一致：监听器在读线程上同步跑（admin 一次性探针；监听器里如果再发
    // 同步请求会自死锁，但那是用户代码职责，Java 读线程同样有此约束）。
    mqClient_->remotingClient().registerProcessor(
        RequestCode::CONSUME_MESSAGE_DIRECTLY,
        [this](const RemotingCommand& cmd, const std::string&) -> std::optional<RemotingCommand> {
            auto git = cmd.extFields.find("consumerGroup");
            const std::string group = git == cmd.extFields.end() ? std::string() : git->second;
            if (group != consumerGroup_) {
                return RemotingCommand::createResponseCommand(
                    ResponseCode::SYSTEM_ERROR,
                    "consumerGroup not matched, expect " + consumerGroup_ + ", got " + group);
            }
            if (cmd.body.empty()) {
                return RemotingCommand::createResponseCommand(
                    ResponseCode::SYSTEM_ERROR, "empty message body");
            }
            MessageExt msg;
            if (!decodeMessage(cmd.body, msg, /*readBody=*/true, /*decompressBody=*/true,
                               /*isClient=*/true, /*checkCrc=*/false)) {
                return RemotingCommand::createResponseCommand(
                    ResponseCode::SYSTEM_ERROR, "decode message failed");
            }
            std::string brokerName;
            auto bit = cmd.extFields.find("brokerName");
            if (bit != cmd.extFields.end()) brokerName = bit->second;
            ConsumeMessageDirectlyResult result = consumeMessageDirectly(msg, brokerName);
            RemotingCommand resp = RemotingCommand::createResponseCommand(
                ResponseCode::SUCCESS, std::string());
            resp.body = result.encode();
            return resp;
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

    // 消息轨迹：enableMsgTrace=true 时建 AsyncTraceDispatcher 并注册 ConsumeMessageTraceHook。
    // 放在消费线程起来之后：分发器的内部生产者要先把轨迹 topic 的路由拉起来。
    startTraceDispatcher();

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
    // POP：把所有队列标成 dropped，在途批次不再 ack（交给 broker 复活重投）
    if (popMode_) {
        std::lock_guard<std::mutex> lk(lock_);
        for (auto& kv : popQueues_) {
            kv.second->setDropped(true);
        }
        popQueues_.clear();
    }
    // POP 消费执行器收工（对齐 Java shutdownGracefully：不再收新任务，把手上的批次跑完）。
    // ⚠ 必须显式 join：工作线程捕获了 this，留着跑就是 use-after-free。
    if (popConsumeExecutor_) {
        popConsumeExecutor_->shutdown(true);
        popConsumeExecutor_.reset();
    }
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
    // 消费线程都停了之后再关轨迹分发器：先把队列里剩余的轨迹强刷出去（SubBefore/SubAfter
    // 落盘就靠这一步），再关内部生产者。必须在 mqClient_->shutdown() 之前。
    if (traceDispatcher_) {
        try {
            traceDispatcher_->shutdown();
        } catch (const std::exception& e) {
            logger_warn(std::string("trace dispatcher shutdown failed: ") + e.what());
        }
        traceDispatcher_.reset();
    }
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
                if (popMode_ && popQueues_.find(kv.first) == popQueues_.end()) {
                    popQueues_[kv.first] = std::make_shared<PopProcessQueue>();
                }
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
                // POP：标记 dropped，在途批次不再消费也不 ack（交给 broker 复活）
                auto pqit = popQueues_.find(it->first);
                if (pqit != popQueues_.end()) {
                    pqit->second->setDropped(true);
                    popQueues_.erase(pqit);
                }
                it = pullThreads_.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (const auto& kv : toStart) {
        // 捕获 kv.second（拷贝），线程内再通过成员访问共享状态
        std::thread t([this, mq = kv.second, key = kv.first]() {
            if (popMode_) {
                setThreadName("PopMessageService");
                queuePopLoop(mq);
            } else {
                setThreadName("PullMessageService");
                queuePullLoop(mq);
            }
            (void)key;
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
            const int64_t pullBegan = UtilAll::currentTimeMillis();
            result = c.pullMessage(consumerGroup_, mq, offset, pullBatchSize_, sysFlag,
                                   /*commitOffset=*/0, expr, sub.subVersion, sub.expressionType,
                                   pullTimeoutMillis_, pullBatchSizeInBytes_,
                                   pullSuspendTimeoutMillis_);
            // 消费统计（Java PullCallback.onSuccess：RT 每次都记，TPS 只在有消息时记）
            mqClient_->consumerStats().incPullRT(consumerGroup_, mq.topic,
                                                 UtilAll::currentTimeMillis() - pullBegan);
            if (result.isFound() && !result.msgFoundList.empty()) {
                mqClient_->consumerStats().incPullTPS(consumerGroup_, mq.topic,
                                                      static_cast<int64_t>(result.msgFoundList.size()));
            }
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
            // 投递前的客户端侧过滤（对齐 Java PullAPIWrapper.processPullResult:113-128）：
            // 先二次 tag 过滤，再跑 FilterMessageHook。**必须在拿 lock_ 之前做** ——
            // 钩子是用户代码，可能阻塞，不能压在入队的临界区里。
            // 拉取路径被摘掉的消息**不 ack**（Java 亦然）：位点照常推进 = 静默跳过。
            std::vector<MessageExt> deliverable = filterMessagesForDelivery(mq, &sub, result.msgFoundList);
            if (deliverable.size() != result.msgFoundList.size()) {
                filteredMessageCount_ +=
                    static_cast<int64_t>(result.msgFoundList.size() - deliverable.size());
            }
            std::lock_guard<std::mutex> lk(lock_);
            std::deque<MessageExt>& dq = pending_[key];
            for (const MessageExt& m : deliverable) {
                dq.push_back(m);
            }
            // ProcessQueue.msgAccCnt：用**过滤后入队**的那批算（Java 是先过滤再 putMessage）
            updateMsgAccCntLocked(key, deliverable);
        }
        // 拉取游标推进到 nextBeginOffset；"已消费位点"由 consumeOffsetTable_ 跟踪并持久化
        if (result.nextBeginOffset >= 0) {
            std::lock_guard<std::mutex> lk(lock_);
            offsetTable_[key] = result.nextBeginOffset;
        }
    }
}

// ---------------------------------------------------------------- POP 消费循环
void DefaultMQPushConsumer::queuePopLoop(const MessageQueue& mq) {
    // 与 pull 循环的关键差别：
    //   - **不查、不提交消费位点**：进度由 broker 侧的 checkpoint 跟踪，确认只靠 ack；
    //   - 弹出即投递给消费线程，本轮循环立刻继续（不等消费结果）；
    //   - POLLING_NOT_FOUND（队列暂时没消息）是**正常态**，直接下一轮，不算错误。
    const std::string key = offsetKey(mq);
    int64_t invisible = popInvisibleTime_;
    if (invisible < kMinPopInvisibleTime || invisible > kMaxPopInvisibleTime) {
        // Java 的钳制：超出 [5s, 300s] 一律回落到 60s
        invisible = 60000;
    }
    // Java PopRequest 默认 ConsumeInitMode.MAX；这里按 consumeFromWhere 映射，
    // 让"从头消费"的语义在 POP 模式下也成立。
    const int32_t initMode =
        consumeFromWhere_ == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET
            ? static_cast<int32_t>(ConsumeInitMode::MIN)
            : static_cast<int32_t>(ConsumeInitMode::MAX);

    while (!stop_.load() && started_.load()) {
        if (!ownsQueue(key)) return;
        std::shared_ptr<PopProcessQueue> pq;
        {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = popQueues_.find(key);
            if (it == popQueues_.end()) return;
            pq = it->second;
        }
        if (pq->isDropped()) return;
        SubscriptionData sub;
        {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = subscriptionData_.find(mq.topic);
            if (it == subscriptionData_.end()) return;
            sub = it->second;
        }
        // 流控：已弹未 ack 太多就先缓一缓（Java popThresholdForQueue）
        if (pq->waitAckCount() > popThresholdForQueue_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
        }
        const int64_t began = UtilAll::currentTimeMillis();
        PopResult result;
        try {
            result = client().popMessage(consumerGroup_, mq.topic, mq.queueId, popBatchNums_,
                                         invisible, popPollTimeMillis_, initMode,
                                         sub.subString.empty() ? "*" : sub.subString,
                                         sub.expressionType, false, popTimeoutMillis_, mq.brokerName);
        } catch (const RemotingTimeoutException& e) {
            // 长轮询挂起期间没有消息 → 客户端先超时，属正常行为，直接下一轮
            logger_debug("pop long-poll timeout for " + key + " (benign): " + e.what());
            continue;
        } catch (const std::exception& e) {
            logger_debug(std::string("pop error for ") + key + ": " + e.what());
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }
        // 弹出后队列被 rebalance 撤走：这一批**既不消费也不 ack**
        // （Java 对应 PopProcessQueue.isDropped() 分支），交给 invisibleTime 到期后
        // broker 自动复活重投给新属主。
        if (!ownsQueue(key) || pq->isDropped()) {
            logger_debug("queue " + key + " revoked during pop, discard "
                         + std::to_string(result.msgFoundList.size()) + " messages un-acked");
            return;
        }
        if (result.status == PopStatus::FOUND && !result.msgFoundList.empty()) {
            pq->incFoundMsg(static_cast<int32_t>(result.msgFoundList.size()));
            // 投递前过滤（对齐 Java processPopResult:621-661）：POP 路径**必须 ack 被摘掉的**，
            // 否则 invisibleTime 到期后 broker 会复活重投 —— 表现为"过滤没生效"。
            std::vector<MessageExt> kept = filterMessagesForDelivery(mq, &sub, result.msgFoundList);
            if (kept.size() != result.msgFoundList.size()) {
                std::vector<MessageExt> dropped = droppedMessages(result.msgFoundList, kept);
                filteredMessageCount_ += static_cast<int64_t>(dropped.size());
                for (const MessageExt& msg : dropped) {
                    ackPopMsg(msg);
                    pq->ack();
                }
                logger_info("pop filter dropped " + std::to_string(dropped.size()) + " of "
                            + std::to_string(result.msgFoundList.size()) + " messages (acked)");
            }
            if (!kept.empty()) submitPopConsumeRequest(kept, pq, mq);
        } else if (UtilAll::currentTimeMillis() - began < 200) {
            // 空结果：若 broker 没按 pollTime 挂起（立即返回）就会变成热循环，
            // 这里按"本轮耗时过短"兜底退避，避免打爆 broker。
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        // NO_NEW_MSG / POLLING_NOT_FOUND / POLLING_FULL 都直接进下一轮
    }
}

void DefaultMQPushConsumer::submitPopConsumeRequest(std::vector<MessageExt> msgs,
                                                    std::shared_ptr<PopProcessQueue> pq,
                                                    const MessageQueue& mq) {
    // 对应 Java ConsumeMessagePopConcurrentlyService.submitPopConsumeRequest。
    // 投给**有界线程池**（core=consumeThreadMin / max=consumeThreadMax）：
    // 此前这里每批起一个 detached 线程（无上限），慢监听器一上来就线程爆炸，
    // 而 setConsumeThreadNums() 设的值完全没作用。Java 用线程池 + 无界队列，
    // 因此真实并发度 == corePoolSize，updateCorePoolSize() 在运行时能改它。
    const size_t size = static_cast<size_t>(std::max(1, consumeMessageBatchMaxSize_));
    std::shared_ptr<ConsumeExecutor> exec = popConsumeExecutor_;
    for (size_t i = 0; i < msgs.size(); i += size) {
        auto batch = std::make_shared<std::vector<MessageExt>>(
            msgs.begin() + static_cast<long>(i),
            msgs.begin() + static_cast<long>(std::min(i + size, msgs.size())));
        if (batch->empty()) continue;
        if (exec) {
            exec->submit([this, batch, pq, mq]() {
                consumePopBatch(std::move(*batch), pq, mq);
            });
        } else {
            // 未 start（单测）时没有执行器：同步执行，保持与改动前一致的可测行为。
            consumePopBatch(std::move(*batch), pq, mq);
        }
    }
}

void DefaultMQPushConsumer::consumePopBatch(std::vector<MessageExt> msgs,
                                            std::shared_ptr<PopProcessQueue> pq,
                                            const MessageQueue& mq) {
    // 对应 Java ConsumeMessagePopConcurrentlyService$ConsumeRequest.run。
    if (pq->isDropped() || msgs.empty()) return;

    int64_t popTime = 0;
    int64_t invisible = 0;
    try {
        auto it = msgs[0].properties.find(MessageConst::PROPERTY_POP_CK);
        if (it != msgs[0].properties.end()) {
            std::vector<std::string> seg = extra_info::split(it->second);
            popTime = extra_info::getPopTime(seg);
            invisible = extra_info::getInvisibleTime(seg);
        }
    } catch (const std::exception& e) {
        logger_debug(std::string("parse pop ck failed: ") + e.what());
    }
    if (isPopTimeout(popTime, invisible)) {
        // 已经超过 invisibleTime：ack 也不会被承认，直接放弃本批（等 broker 复活重投）
        pq->decFoundMsg(-static_cast<int32_t>(msgs.size()));
        return;
    }

    resetRetryTopicAndNamespace(msgs);
    ConsumeConcurrentlyContext ctx(mq);
    // ⚠ 对齐 Java ConsumeConcurrentlyContext.ackIndex = Integer.MAX_VALUE：
    // 默认就是"全部 ack"。本项目的默认值是 -1（push 回投路径的语义），若不在 POP 这里
    // 改成 size-1，CONSUME_SUCCESS 会**一条都不 ack**，消息在 invisibleTime 到期后被
    // broker 复活重投 —— 短观测窗口下会伪装成通过。
    ctx.ackIndex = static_cast<int32_t>(msgs.size()) - 1;
    // 消费钩子：before 在 listener 之前，after 紧跟在 listener 之后
    // （Java ConsumeMessagePopConcurrentlyService:360-422 就是这个顺序：
    //  after 钩子在「队列被撤走 / pop 超时」判定**之前**跑）
    const bool useHook = hasConsumeMessageHook();
    ConsumeMessageContext hookCtx;
    if (useHook) {
        hookCtx = buildConsumeHookContext(msgs, mq);
        executeConsumeHookBefore(hookCtx);
    }
    ConsumeConcurrentlyStatus status = ConsumeConcurrentlyStatus::RECONSUME_LATER;
    bool hookHasException = false;
    int64_t hookBeginMs = UtilAll::currentTimeMillis();
    try {
        auto* conc = static_cast<MessageListenerConcurrently*>(messageListener_.get());
        status = conc->consumeMessage(msgs, ctx);
    } catch (const std::exception& e) {
        // Java：消费抛异常按 RECONSUME_LATER 处理
        logger_debug(std::string("pop listener error, treat as RECONSUME_LATER: ") + e.what());
        hookHasException = true;
    }
    recordConsumeStats(mq.topic, static_cast<int64_t>(msgs.size()), hookBeginMs,
                       status != ConsumeConcurrentlyStatus::CONSUME_SUCCESS);
    if (useHook) {
        const bool ok = (status == ConsumeConcurrentlyStatus::CONSUME_SUCCESS);
        finishConsumeHook(&hookCtx, hookHasException, hookBeginMs, !ok, ok,
                          ok ? "CONSUME_SUCCESS" : "RECONSUME_LATER");
    }
    if (pq->isDropped() || isPopTimeout(popTime, invisible)) {
        // 消费期间队列被撤走或已超时：结果不再处理
        pq->decFoundMsg(-static_cast<int32_t>(msgs.size()));
        return;
    }
    processPopConsumeResult(status, ctx, msgs, pq);
}

bool DefaultMQPushConsumer::isPopTimeout(int64_t popTime, int64_t invisible) {
    // Java ConsumeRequest.isPopTimeout：不能解析出 popTime/invisibleTime 时按超时处理
    if (popTime <= 0 || invisible <= 0) return true;
    return UtilAll::currentTimeMillis() - popTime >= invisible;
}

void DefaultMQPushConsumer::processPopConsumeResult(
    ConsumeConcurrentlyStatus status, const ConsumeConcurrentlyContext& ctx,
    std::vector<MessageExt>& msgs, const std::shared_ptr<PopProcessQueue>& pq) {
    // 对应 Java ConsumeMessagePopConcurrentlyService.processConsumeResult。
    int32_t ackIndex = ctx.ackIndex;
    if (status == ConsumeConcurrentlyStatus::CONSUME_SUCCESS) {
        if (ackIndex >= static_cast<int32_t>(msgs.size())) {
            ackIndex = static_cast<int32_t>(msgs.size()) - 1;
        }
    } else {
        ackIndex = -1;  // RECONSUME_LATER：一条都不 ack
    }
    for (int32_t i = 0; i <= ackIndex; i++) {
        ackPopMsg(msgs[static_cast<size_t>(i)]);
        pq->ack();
    }
    for (int32_t i = ackIndex + 1; i < static_cast<int32_t>(msgs.size()); i++) {
        pq->ack();
        const MessageExt& msg = msgs[static_cast<size_t>(i)];
        // 超过最大重试次数：Java 走 checkNeedAckOrDelay（太老就直接 ack 丢弃，
        // 否则按消息已存活时间选一个延迟档位）
        if (maxReconsumeTimes_ >= 0 && msg.reconsumeTimes >= maxReconsumeTimes_) {
            checkNeedAckOrDelay(msg);
            continue;
        }
        changePopInvisibleTime(msg, ctx.delayLevelWhenNextConsume);
    }
}

void DefaultMQPushConsumer::checkNeedAckOrDelay(const MessageExt& msg) {
    // Java checkNeedAckOrDelay：重试次数用尽后的兜底。
    // 消息存活时间已经超过最大延迟档位的 2 倍 → 直接 ack 丢弃（不再无限重试）；
    // 否则按存活时间选一个档位继续延长不可见时间。
    const std::vector<int32_t>& table = popDelayLevel_;
    const int64_t msgDelayTime = UtilAll::currentTimeMillis() - msg.bornTimestamp;
    if (msgDelayTime > static_cast<int64_t>(table.back()) * 1000 * 2) {
        logger_warn("pop consume too many times, ack and drop: " + msg.msgId);
        ackPopMsg(msg);
        return;
    }
    int32_t level = static_cast<int32_t>(table.size()) - 1;
    for (; level >= 0; level--) {
        if (msgDelayTime >= static_cast<int64_t>(table[static_cast<size_t>(level)]) * 1000) {
            level++;
            break;
        }
    }
    // ⚠ 有意偏离 Java：存活时间小于首档时 Java 会算出 level=-1 并索引
    // delayLevelTable[-1] 抛 ArrayIndexOutOfBounds。这里钳到首档。
    changePopInvisibleTime(msg, level);
}

std::optional<PopCkTarget> DefaultMQPushConsumer::popCkTarget(const MessageExt& msg) {
    // ⚠ 两处都不能想当然：
    //   1. topic 要用 ExtraInfoUtil.getRealTopic 按 CK 的 retryFlag 还原 —— 复活消息
    //      （retryFlag=1）的真实 topic 是 %RETRY%<group>_<topic>，**不是**消息上的 topic；
    //   2. 地址要按 CK 里的 brokerName 反查，不能按 topic 查路由 —— retry topic 通常没有
    //      独立路由表项，按 topic 查会失败（Java 同理走 findBrokerAddressInSubscribe）。
    auto pit = msg.properties.find(MessageConst::PROPERTY_POP_CK);
    if (pit == msg.properties.end() || pit->second.empty()) {
        logger_debug("pop message without POP_CK, cannot ack: " + msg.msgId);
        return std::nullopt;
    }
    PopCkTarget out;
    out.extraInfo = pit->second;
    try {
        std::vector<std::string> seg = extra_info::split(out.extraInfo);
        out.brokerName = extra_info::getBrokerName(seg);
        out.queueId = extra_info::getQueueId(seg);
        out.offset = extra_info::getQueueOffset(seg);
        std::string retry = extra_info::getRetry(seg);
        out.topic = extra_info::getRealTopic(msg.topic, consumerGroup_, retry);
    } catch (const std::exception& e) {
        logger_debug("bad POP_CK " + out.extraInfo + ": " + e.what());
        return std::nullopt;
    }
    return out;
}

void DefaultMQPushConsumer::ackPopMsg(const MessageExt& msg) {
    // 对应 Java DefaultMQPushConsumerImpl.ackAsync
    auto target = popCkTarget(msg);
    if (!target) return;
    try {
        client().ackMessage(consumerGroup_, target->topic, target->queueId, target->extraInfo,
                            target->offset, 3000, target->brokerName);
    } catch (const std::exception& e) {
        // ack 失败不致命：消息会在 invisibleTime 到期后被 broker 复活重投
        logger_debug(std::string("ack failed for ") + msg.msgId + ": " + e.what());
    }
}

void DefaultMQPushConsumer::changePopInvisibleTime(const MessageExt& msg, int32_t delayLevel) {
    // 对应 Java changePopInvisibleTime。
    // delayLevel == 0 时 Java 用消息已重试次数当档位；档位表是**秒**，接口要毫秒。
    auto target = popCkTarget(msg);
    if (!target) return;
    if (delayLevel == 0) {
        delayLevel = msg.reconsumeTimes;
    }
    const std::vector<int32_t>& table = popDelayLevel_;
    int32_t delaySecond = 0;
    if (delayLevel >= static_cast<int32_t>(table.size())) {
        delaySecond = table.back();
    } else {
        delaySecond = table[static_cast<size_t>(std::max(0, delayLevel))];
    }
    try {
        client().changeInvisibleTime(consumerGroup_, target->topic, target->queueId,
                                     target->extraInfo, target->offset,
                                     static_cast<int64_t>(delaySecond) * 1000, 3000,
                                     target->brokerName);
    } catch (const std::exception& e) {
        logger_debug(std::string("change invisible time failed for ") + msg.msgId + ": "
                     + e.what());
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

// ---------------------------------------------------------------- 消费钩子 / 轨迹
//
// 对应 Java 的 ConsumeMessageHook 调用点（ConsumeMessageConcurrentlyService:348-412、
// ConsumeMessageOrderlyService:440-512、ConsumeMessagePopConcurrentlyService:360-422）：
//   1) 调 listener **之前** 建 context 并跑 before 钩子（生成 SubBefore 轨迹）；
//   2) listener 返回后算出 ConsumeReturnType 名字写进 props（决定 SubAfter 的 contextCode），
//      再跑 after 钩子（生成 SubAfter 轨迹）。
// 钩子异常一律吞掉并记 warn —— 轨迹挂了绝不能影响消费。
ConsumeMessageContext DefaultMQPushConsumer::buildConsumeHookContext(
    const std::vector<MessageExt>& msgs, const MessageQueue& mq) const {
    ConsumeMessageContext context(consumerGroup_, msgs, mq);
    context.success = false;            // Java 初始值
    context.props.clear();              // props 用来放 ConsumeReturnType 名字
    context.accessChannel = accessChannel_;
    return context;
}

void DefaultMQPushConsumer::executeConsumeHookBefore(ConsumeMessageContext& context) {
    for (auto& hook : consumeMessageHookList_) {
        try {
            hook->consumeMessageBefore(context);
        } catch (const std::exception& e) {
            logger_warn(std::string("consumeMessageHook executeHookBefore exception: ") + e.what());
        } catch (...) {
            logger_warn("consumeMessageHook executeHookBefore exception: unknown");
        }
    }
}

void DefaultMQPushConsumer::executeConsumeHookAfter(ConsumeMessageContext& context) {
    for (auto& hook : consumeMessageHookList_) {
        try {
            hook->consumeMessageAfter(context);
        } catch (const std::exception& e) {
            logger_warn(std::string("consumeMessageHook executeHookAfter exception: ") + e.what());
        } catch (...) {
            logger_warn("consumeMessageHook executeHookAfter exception: unknown");
        }
    }
}

void DefaultMQPushConsumer::executeFilterMessageHook(FilterMessageContext& context) {
    // 钩子异常一律吞掉并记 error（Java PullAPIWrapper.executeHook:171-178 / processPopResult:646-653）。
    // 与 CheckForbiddenHook 相反：过滤钩子挂了不能影响消费。
    for (auto& hook : filterMessageHookList_) {
        try {
            hook->filterMessage(context);
        } catch (const std::exception& e) {
            logger_error("execute hook error. hookName=" + hook->hookName() + ": " + e.what());
        } catch (...) {
            logger_error("execute hook error. hookName=" + hook->hookName() + ": unknown");
        }
    }
}

std::vector<MessageExt> DefaultMQPushConsumer::filterMessagesForDelivery(
    const MessageQueue& mq, const SubscriptionData* sub, const std::vector<MessageExt>& msgs) {
    // 投递前过滤：① 客户端二次 tag 过滤 ② FilterMessageHook（拉取 / POP 两条路径共用）。
    //
    // ① 对齐 Java PullAPIWrapper.processPullResult:113-122 与 processPopResult:625-635：
    //    broker 侧是按 tag 的**哈希（codeSet）**过滤的，存在哈希碰撞误放，客户端要再按
    //    字符串核一遍。守卫 `!tagsSet.isEmpty()` 意味着订阅 "*"（SUB_ALL）时不过滤 ——
    //    所以 FilterAPI.buildSubscriptionData 对 SUB_ALL 必须留空。
    //
    // ② 钩子拿到的是**可变的** msgList，被摘掉的消息由调用方决定怎么处置：
    //    拉取路径 = 静默跳过（位点照常推进，不 ack）；POP 路径 = 立刻 ack。
    std::vector<MessageExt> out = msgs;
    if (out.empty()) return out;
    if (sub != nullptr && !sub->tagsSet.empty() && !sub->classFilterMode) {
        std::vector<MessageExt> kept;
        kept.reserve(out.size());
        for (const MessageExt& m : out) {
            std::string tags = m.getTags();
            if (!tags.empty() && sub->tagsSet.count(tags) > 0) kept.push_back(m);
        }
        out.swap(kept);
    }
    if (!filterMessageHookList_.empty() && !out.empty()) {
        FilterMessageContext context;
        context.consumerGroup = consumerGroup_;
        context.msgList = out;
        context.mq = mq;
        context.unitMode = false;  // 本项目无 unit mode
        executeFilterMessageHook(context);
        out = context.msgList;
    }
    return out;
}

std::vector<MessageExt> DefaultMQPushConsumer::droppedMessages(
    const std::vector<MessageExt>& original, const std::vector<MessageExt>& kept) {
    // Java 用的是 List.contains(Object)（Object.equals = 引用同一性），拿到的 msgListFilterAgain
    // 与原 msgFoundList 共享元素引用，所以差集是精确的。C++ 里 vector 拷贝后没有同一性可用，
    // 改用 **msgId** 求差 —— 同一批 POP 结果的 msgId 必不相同（msgId 由 storeHost+commitLogOffset
    // 生成），语义等价且不会被"值相等"误判。
    if (kept.size() >= original.size()) return {};
    std::vector<std::string> keptIds;
    keptIds.reserve(kept.size());
    for (const MessageExt& m : kept) keptIds.push_back(m.msgId);
    std::vector<MessageExt> dropped;
    dropped.reserve(original.size() - kept.size());
    for (const MessageExt& m : original) {
        bool found = false;
        for (const std::string& id : keptIds) {
            if (id == m.msgId) { found = true; break; }
        }
        if (!found) dropped.push_back(m);
    }
    return dropped;
}

const char* DefaultMQPushConsumer::consumeReturnTypeOf(bool hasException, int64_t consumeRtMs,
                                                       bool failed, bool succeeded) {
    // 判定顺序照抄 Java ConsumeMessageConcurrentlyService:378-393
    if (hasException) return "EXCEPTION";
    if (consumeRtMs >= 15LL * 60 * 1000) return "TIME_OUT";
    if (failed) return "FAILED";
    if (succeeded) return "SUCCESS";
    return "SUCCESS";
}

void DefaultMQPushConsumer::finishConsumeHook(ConsumeMessageContext* hookCtx, bool hasException,
                                              int64_t beginMs, bool failed, bool succeeded,
                                              const std::string& statusText) {
    if (hookCtx == nullptr) return;
    int64_t rt = UtilAll::currentTimeMillis() - beginMs;
    hookCtx->props = consumeReturnTypeOf(hasException, rt, failed, succeeded);
    hookCtx->status = statusText;
    hookCtx->success = succeeded;
    executeConsumeHookAfter(*hookCtx);
}

void DefaultMQPushConsumer::recordConsumeStats(const std::string& topic, int64_t msgCount,
                                               int64_t beginMs, bool failed) {
    if (!mqClient_) return;
    auto& stats = mqClient_->consumerStats();
    const int64_t rt = UtilAll::currentTimeMillis() - beginMs;
    if (failed) {
        stats.incConsumeFailedTPS(consumerGroup_, topic, msgCount);
    } else {
        stats.incConsumeOKTPS(consumerGroup_, topic, msgCount);
    }
    stats.incConsumeRT(consumerGroup_, topic, rt);
}

ConsumerRunningInfo DefaultMQPushConsumer::consumerRunningInfo() {
    // 对应 Java DefaultMQPushConsumerImpl.consumerRunningInfo（307 的应答体）。
    ConsumerRunningInfo info;
    std::string namesrv;
    for (const std::string& a : nameServerAddrs_) {
        if (!namesrv.empty()) namesrv += ";";
        namesrv += a;
    }
    info.properties[ConsumerRunningInfo::PROP_NAMESERVER_ADDR] = namesrv + ";";
    info.properties[ConsumerRunningInfo::PROP_CONSUME_TYPE] = "CONSUME_PASSIVELY";
    info.properties[ConsumerRunningInfo::PROP_CONSUME_ORDERLY] = isOrderly() ? "true" : "false";
    info.properties[ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE] =
        std::to_string(getCorePoolSize());
    info.properties[ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP] =
        std::to_string(startMillis_);
    info.properties[ConsumerRunningInfo::PROP_CLIENT_VERSION] = "V5_5_1";

    JsonValue subs = JsonValue::makeArray();
    JsonValue statusTable = JsonValue::makeObject();
    {
        std::lock_guard<std::mutex> lk(lock_);
        for (const auto& kv : subscriptionData_) {
            subs.pushArray(kv.second.toJson());
        }
        auto makePqi = [&](int64_t commitOffset, int64_t cachedMsgCount, bool droped) {
            // ProcessQueueInfo 全字段（Java body.ProcessQueueInfo；"droped" 拼写照抄）
            JsonValue pqi = JsonValue::makeObject();
            pqi.set("commitOffset", JsonValue::makeInt(commitOffset));
            pqi.set("cachedMsgMinOffset", JsonValue::makeInt(0));
            pqi.set("cachedMsgMaxOffset", JsonValue::makeInt(0));
            pqi.set("cachedMsgCount", JsonValue::makeInt(cachedMsgCount));
            pqi.set("cachedMsgSizeInMiB", JsonValue::makeInt(0));
            pqi.set("transactionMsgMinOffset", JsonValue::makeInt(0));
            pqi.set("transactionMsgMaxOffset", JsonValue::makeInt(0));
            pqi.set("transactionMsgCount", JsonValue::makeInt(0));
            pqi.set("locked", JsonValue::makeBool(false));
            pqi.set("tryUnlockTimes", JsonValue::makeInt(0));
            pqi.set("lastLockTimestamp", JsonValue::makeInt(0));
            pqi.set("droped", JsonValue::makeBool(droped));
            pqi.set("lastPullTimestamp", JsonValue::makeInt(0));
            pqi.set("lastConsumeTimestamp", JsonValue::makeInt(0));
            return pqi;
        };
        for (const auto& kv : mqMap_) {
            const MessageQueue& mq = kv.second;
            // fastjson2 内联对象键（键按字母序），与 Python message_queue_key 同款
            const std::string mqKey = "{\"brokerName\":\"" + mq.brokerName + "\",\"queueId\":"
                + std::to_string(mq.queueId) + ",\"topic\":\"" + mq.topic + "\"}";
            int64_t commit = 0;
            auto ot = consumeOffsetTable_.find(kv.first);
            if (ot != consumeOffsetTable_.end()) commit = ot->second;
            int64_t cached = 0;
            auto pt = pending_.find(kv.first);
            if (pt != pending_.end()) cached = static_cast<int64_t>(pt->second.size());
            info.mqTable.set(mqKey, makePqi(commit, cached, false));
        }
        if (popMode_) {
            for (const auto& kv : popQueues_) {
                auto mt = mqMap_.find(kv.first);
                if (mt == mqMap_.end()) continue;
                const MessageQueue& mq = mt->second;
                const std::string mqKey = "{\"brokerName\":\"" + mq.brokerName + "\",\"queueId\":"
                    + std::to_string(mq.queueId) + ",\"topic\":\"" + mq.topic + "\"}";
                info.mqPopTable.set(mqKey,
                                    makePqi(0, kv.second->waitAckCount(), kv.second->isDropped()));
            }
        }
    }
    // statusTable（Java consumerRunningInfo：consumeStatus(group, topic)，minute 快照）
    for (const auto& kv : subscriptionData_) {
        ConsumeStatus cs;
        if (mqClient_) cs = mqClient_->consumerStats().consumeStatus(consumerGroup_, kv.first);
        statusTable.set(kv.first, cs.toJson());
    }
    info.subscriptionSet = subs;
    info.statusTable = statusTable;
    return info;
}

void DefaultMQPushConsumer::resetOffset(const std::string& topic,
                                        const std::map<MessageQueue, int64_t>& offsetTable) {
    // 对应 Java MQClientInstance.resetOffset（220 的消费者侧逻辑）：
    // suspend → 命中的队列 drop+clear → 等一会儿让在途消费跑完 → 写新位点 →
    // 撤销该队列（触发 rebalance 重新分配并从新位点开始）。
    if (topic.empty() || offsetTable.empty()) return;
    std::vector<std::pair<MessageQueue, int64_t>> hit;
    {
        std::lock_guard<std::mutex> lk(lock_);
        for (const auto& kv : mqMap_) {
            const MessageQueue& mq = kv.second;
            if (mq.topic != topic) continue;
            auto it = offsetTable.find(mq);
            if (it == offsetTable.end()) continue;
            pending_.erase(kv.first);        // 等价 ProcessQueue.clear()
            offsetTable_.erase(kv.first);    // 拉取游标一并清掉
            consumeOffsetTable_[kv.first] = it->second;
            hit.emplace_back(mq, it->second);
        }
    }
    if (hit.empty()) return;
    // Java 用 RESET_OFFSET_MAX_WAIT（5 秒）等并发消费跑完；这里缩短以免阻塞太久
    // （220 是 oneway，broker 不等响应，但仍应尽快返回）。
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // 撤销队列：onQueuesRevoked 会把 hit 里带的（新）已消费位点持久化到 broker，
    // 并对顺序消费解锁——与 Java resetOffset 写位点后走 revoke 收尾一致。
    onQueuesRevoked(hit);
    try {
        doRebalance();
    } catch (const std::exception& e) {
        logger_debug(std::string("rebalance after reset offset failed: ") + e.what());
    }
    logger_info("reset offset applied, group=" + consumerGroup_ + " topic=" + topic
                + " queues=" + std::to_string(hit.size()));
}

std::map<MessageQueue, int64_t> DefaultMQPushConsumer::getConsumerStatus(const std::string& topic) {
    // 对应 Java MQClientInstance.getConsumerStatus（221 的应答数据源）：
    // 返回**已消费位点**表（不是拉取游标）。
    std::map<MessageQueue, int64_t> out;
    std::lock_guard<std::mutex> lk(lock_);
    for (const auto& kv : mqMap_) {
        if (!topic.empty() && kv.second.topic != topic) continue;
        auto it = consumeOffsetTable_.find(kv.first);
        if (it != consumeOffsetTable_.end()) out[kv.second] = it->second;
    }
    return out;
}

ConsumeMessageDirectlyResult DefaultMQPushConsumer::consumeMessageDirectly(
    const MessageExt& msg, const std::string& brokerName) {
    // 对应 Java ConsumeMessageConcurrentlyService.consumeMessageDirectly（309）。
    ConsumeMessageDirectlyResult result;
    result.autoCommit = true;
    std::vector<MessageExt> msgs{msg};
    MessageQueue mq(msg.topic, brokerName, msg.queueId);
    result.order = isOrderly();
    resetRetryTopicAndNamespace(msgs);
    const int64_t begin = UtilAll::currentTimeMillis();
    if (messageListener_ == nullptr) {
        result.consumeResult = "CR_RETURN_NULL";
    } else if (isOrderly()) {
        auto* orderly = static_cast<MessageListenerOrderly*>(messageListener_.get());
        ConsumeOrderlyContext ctx(mq);
        try {
            const ConsumeOrderlyStatus status = orderly->consumeMessage(msgs, ctx);
            result.consumeResult = (status == ConsumeOrderlyStatus::SUCCESS)
                ? "CR_SUCCESS" : "CR_LATER";
        } catch (const std::exception& e) {
            result.consumeResult = "CR_THROW_EXCEPTION";
            result.remark = std::string("std::exception: ") + e.what();
        } catch (...) {
            result.consumeResult = "CR_THROW_EXCEPTION";
            result.remark = "unknown exception";
        }
    } else {
        auto* conc = static_cast<MessageListenerConcurrently*>(messageListener_.get());
        ConsumeConcurrentlyContext ctx(mq);
        try {
            const ConsumeConcurrentlyStatus status = conc->consumeMessage(msgs, ctx);
            result.consumeResult = (status == ConsumeConcurrentlyStatus::CONSUME_SUCCESS)
                ? "CR_SUCCESS" : "CR_LATER";
        } catch (const std::exception& e) {
            result.consumeResult = "CR_THROW_EXCEPTION";
            result.remark = std::string("std::exception: ") + e.what();
        } catch (...) {
            result.consumeResult = "CR_THROW_EXCEPTION";
            result.remark = "unknown exception";
        }
    }
    result.spentTimeMills = UtilAll::currentTimeMillis() - begin;
    return result;
}

void DefaultMQPushConsumer::startTraceDispatcher() {
    if (!enableMsgTrace_) return;
    try {
        auto dispatcher = std::make_shared<AsyncTraceDispatcher>(
            consumerGroup_, TraceDispatcherType::CONSUME, traceMsgBatchNum_, traceTopic_, rpcHook_);
        dispatcher->setHostConsumer(this);
        dispatcher->setHostClientId(clientId_);
        std::string namesrv;
        for (const std::string& a : nameServerAddrs_) {
            if (!namesrv.empty()) namesrv += ";";
            namesrv += a;
        }
        dispatcher->start(namesrv, accessChannel_);
        traceDispatcher_ = dispatcher;
        registerConsumeMessageHook(std::make_shared<ConsumeMessageTraceHook>(dispatcher.get()));
        logger_info("consumer trace enabled, traceTopic=" + dispatcher->getTraceTopicName());
    } catch (const std::exception& e) {
        logger_warn(std::string("start trace dispatcher failed: ") + e.what());
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
        const bool useHook = hasConsumeMessageHook();
        ConsumeMessageContext hookCtx;
        if (useHook) {
            hookCtx = buildConsumeHookContext(restored, mq);
            executeConsumeHookBefore(hookCtx);
        }
        auto* orderly = static_cast<MessageListenerOrderly*>(messageListener_.get());
        ConsumeOrderlyContext ctx(mq);
        ConsumeOrderlyStatus status;
        bool hookHasException = false;
        int64_t hookBeginMs = UtilAll::currentTimeMillis();
        try {
            status = orderly->consumeMessage(restored, ctx);
        } catch (const std::exception& e) {
            // Java 顺序消费：异常 → 不提交 offset，原地重试
            logger_debug(std::string("orderly listener error (retry in place): ") + e.what());
            status = ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT;
            hookHasException = true;
        }
        // 顺序消费的钩子同样在拿到 status 后触发（Java ConsumeMessageOrderlyService:511）
        recordConsumeStats(mq.topic, static_cast<int64_t>(restored.size()), hookBeginMs,
                           status != ConsumeOrderlyStatus::SUCCESS);
        if (useHook) {
            const bool ok = (status == ConsumeOrderlyStatus::SUCCESS);
            finishConsumeHook(&hookCtx, hookHasException, hookBeginMs, !ok, ok,
                              ok ? "SUCCESS" : "SUSPEND_CURRENT_QUEUE_A_MOMENT");
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
    const bool useHook = hasConsumeMessageHook();
    ConsumeMessageContext hookCtx;
    if (useHook) {
        // 顺序与 Java 一致：before 在 listener **之前**（生成 SubBefore），
        // after 在拿到 status 之后（生成 SubAfter，带 contextCode）
        hookCtx = buildConsumeHookContext(restored, mq);
        executeConsumeHookBefore(hookCtx);
    }
    auto* conc = static_cast<MessageListenerConcurrently*>(messageListener_.get());
    ConsumeConcurrentlyContext ctx(mq);
    ConsumeConcurrentlyStatus status;
    bool hookHasException = false;
    int64_t hookBeginMs = UtilAll::currentTimeMillis();
    try {
        status = conc->consumeMessage(restored, ctx);
    } catch (const std::exception& e) {
        // Java：消费抛异常按 RECONSUME_LATER 处理
        logger_debug(std::string("listener error, treat as RECONSUME_LATER: ") + e.what());
        status = ConsumeConcurrentlyStatus::RECONSUME_LATER;
        hookHasException = true;
    }
    recordConsumeStats(mq.topic, static_cast<int64_t>(restored.size()), hookBeginMs,
                       status == ConsumeConcurrentlyStatus::RECONSUME_LATER);
    if (useHook) {
        const bool ok = (status == ConsumeConcurrentlyStatus::CONSUME_SUCCESS);
        finishConsumeHook(&hookCtx, hookHasException, hookBeginMs, !ok, ok,
                          ok ? "CONSUME_SUCCESS" : "RECONSUME_LATER");
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
    std::string base = UtilAll::userHome();
    if (base.empty()) base = ".";
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
    // 重投消息 topic 还原成业务原始 topic，并剥掉命名空间前缀。
    const std::string groupTopic = MixAll::getRetryTopic(consumerGroup_);
    for (MessageExt& msg : msgs) {
        std::string retryTopic = msg.getProperty(MessageConst::PROPERTY_RETRY_TOPIC);
        if (!retryTopic.empty() && msg.topic == groupTopic) {
            msg.setTopic(namespace_.empty()
                             ? retryTopic
                             : NamespaceUtil::withoutNamespace(retryTopic, namespace_));
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
