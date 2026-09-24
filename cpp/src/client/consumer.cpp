// 推模式消费者实现（对应 Python client/consumer.py 的 DefaultMQPushConsumer）。
#include "rocketmq/client/consumer.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/trace_hook.h"
#include "rocketmq/client/validators.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/extra_info.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/json.h"
#include "schedule_util.h"

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
    // Java DefaultMQPushConsumer 的默认策略（构造参数可覆盖，这里等价于 setter）。
    allocateStrategy_ = std::make_shared<AllocateMessageQueueAveragely>();
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

void DefaultMQPushConsumer::setAllocateMessageQueueStrategy(
    std::shared_ptr<AllocateMessageQueueStrategy> strategy) {
    // 与 Java 一致：setter 不校验，null 由 start() 的 checkConfig 拒绝
    //（Java DefaultMQPushConsumerImpl.checkConfig:1067）。
    allocateStrategy_ = std::move(strategy);
}

std::shared_ptr<AllocateMessageQueueStrategy> DefaultMQPushConsumer::allocateMessageQueueStrategy() const {
    return allocateStrategy_;
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
    const std::string realTopic = NamespaceUtil::wrapNamespace(namespace_, topic);
    SubscriptionData sub = FilterAPI::buildSubscriptionData(realTopic, subExpression);
    {
        std::lock_guard<std::mutex> lk(lock_);
        subscriptionData_[realTopic] = sub;
    }
    notifySubscriptionChanged(realTopic);
}

void DefaultMQPushConsumer::subscribe(const std::string& topic, const MessageSelector& selector) {
    const std::string realTopic = NamespaceUtil::wrapNamespace(namespace_, topic);
    SubscriptionData sub(realTopic, selector.expression);
    sub.expressionType = selector.type;
    if (selector.type == ExpressionType::TAG) {
        SubscriptionData built = FilterAPI::buildSubscriptionData(realTopic, selector.expression);
        sub.tagsSet = built.tagsSet;
    }
    {
        std::lock_guard<std::mutex> lk(lock_);
        subscriptionData_[realTopic] = sub;
    }
    notifySubscriptionChanged(realTopic);
}

// 订阅表新增/更新后的立即动作，对齐 Java DefaultMQPushConsumerImpl.subscribe:1265-1287。
//
// Java 三处 subscribe 都是 `subscriptionInner.put(...)` 之后
// `if (mQClientFactory != null) mQClientFactory.sendHeartbeatToAllBrokerWithLock();`
// —— **start() 之后仍可订阅**，且 put 完立即同步推一轮心跳。为什么必须立即：broker 的
// ConsumerManager 只在收到心跳时才把 topic→group 记进它自己那张 topicGroupTable
// （ClientManageProcessor → registerConsumer），而 QUERY_TOPIC_CONSUME_BY_WHO(300)
// 读的正是这张表；晚一轮就是默认 30s 的空窗。
//
// 另外把新 topic 登记为「在用」，让后台路由刷新任务覆盖到它（对应 Java
// MQClientInstance:438-454 直接遍历消费者的**当前**订阅表收集要刷的 topic）。
// 未启动时没有 client（Java 的 mQClientFactory == null），只落订阅表。
// unsubscribe 不调本函数：Java 那边也只删表项、不发心跳（:1317-1319）。
void DefaultMQPushConsumer::notifySubscriptionChanged(const std::string& topic) {
    if (!started_.load() || mqClient_ == nullptr) {
        return;
    }
    try {
        mqClient_->registerTopicInUse(topic);
        sendHeartbeatToAllBroker();
    } catch (const std::exception& e) {
        logger_debug("immediate heartbeat after subscribe(" + topic + ") failed: " + e.what());
    }
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

// ---------------------------------------------------------------- 配置数值闸门
// 对应 Java DefaultMQPushConsumerImpl#checkConfig 的数值段（:1099-1209）。
//
// 为什么放在 start() 而不是各 setter 里：Java 就是这样——setter 是裸赋值，只有
// checkConfig 统一在启动时把关（DefaultMQPushConsumerImpl:1025 起）。 setter 里
// clamp（setConsumeMessageBatchMaxSize 的 max(1,n)）对应的是 Java 运行期的
// Math.max 守卫，两道各有分工：setter 保证"拿到 0 也不会除零/空转"，闸门保证
// "写 0 的人当场收到错误"。
//
// 文案里 Java 拼的 FAQUrl.suggestTodo(CLIENT_PARAMETER_CHECK_URL) 一律不带上
// （本仓库所有客户端错误的口径）。
void DefaultMQPushConsumer::checkConfigRanges() const {
    // consumeThreadMin
    if (consumeThreadMin_ < 1 || consumeThreadMin_ > 1000) {
        throw MQClientException("consumeThreadMin Out of range [1, 1000]");
    }
    // consumeThreadMax
    if (consumeThreadMax_ < 1 || consumeThreadMax_ > 1000) {
        throw MQClientException("consumeThreadMax Out of range [1, 1000]");
    }
    // consumeThreadMin can't be larger than consumeThreadMax
    if (consumeThreadMin_ > consumeThreadMax_) {
        throw MQClientException("consumeThreadMin (" + std::to_string(consumeThreadMin_)
                                + ") is larger than consumeThreadMax ("
                                + std::to_string(consumeThreadMax_) + ")");
    }
    // consumeConcurrentlyMaxSpan
    if (consumeConcurrentlyMaxSpan_ < 1 || consumeConcurrentlyMaxSpan_ > 65535) {
        throw MQClientException("consumeConcurrentlyMaxSpan Out of range [1, 65535]");
    }
    // pullThresholdForQueue
    if (pullThresholdForQueue_ < 1 || pullThresholdForQueue_ > 65535) {
        throw MQClientException("pullThresholdForQueue Out of range [1, 65535]");
    }
    // pullThresholdForTopic：-1 是 Java 的"未设置，用队列级阈值"哨兵，不在区间内也不算错
    if (pullThresholdForTopic_ != -1
        && (pullThresholdForTopic_ < 1 || pullThresholdForTopic_ > 6553500)) {
        throw MQClientException("pullThresholdForTopic Out of range [1, 6553500]");
    }
    // pullThresholdSizeForQueue
    if (pullThresholdSizeForQueue_ < 1 || pullThresholdSizeForQueue_ > 1024) {
        throw MQClientException("pullThresholdSizeForQueue Out of range [1, 1024]");
    }
    // pullThresholdSizeForTopic：同样只有 -1 是哨兵
    if (pullThresholdSizeForTopic_ != -1
        && (pullThresholdSizeForTopic_ < 1 || pullThresholdSizeForTopic_ > 102400)) {
        throw MQClientException("pullThresholdSizeForTopic Out of range [1, 102400]");
    }
    // pullInterval（下界是 0，与其它闸门不同）
    if (pullIntervalMillis_ < 0 || pullIntervalMillis_ > 65535) {
        throw MQClientException("pullInterval Out of range [0, 65535]");
    }
    // consumeMessageBatchMaxSize
    if (consumeMessageBatchMaxSize_ < 1 || consumeMessageBatchMaxSize_ > 1024) {
        throw MQClientException("consumeMessageBatchMaxSize Out of range [1, 1024]");
    }
    // pullBatchSize
    if (pullBatchSize_ < 1 || pullBatchSize_ > 1024) {
        throw MQClientException("pullBatchSize Out of range [1, 1024]");
    }
    // popInvisibleTime
    if (popInvisibleTime_ < kMinPopInvisibleTime || popInvisibleTime_ > kMaxPopInvisibleTime) {
        throw MQClientException("popInvisibleTime Out of range ["
                                + std::to_string(kMinPopInvisibleTime) + ", "
                                + std::to_string(kMaxPopInvisibleTime) + "]");
    }
    // popBatchNums（Java 写的就是 <= 0，不是 < 1）
    if (popBatchNums_ <= 0 || popBatchNums_ > 32) {
        throw MQClientException("popBatchNums Out of range [1, 32]");
    }
}

// ---------------------------------------------------------------- 生命周期
void DefaultMQPushConsumer::start() {
    {
        std::lock_guard<std::mutex> lk(lock_);
        if (started_.load()) {
            return;
        }
        // 对齐 Java DefaultMQPushConsumer.start()：把消费组套上命名空间（ns%group），
        // 之后所有面向 broker 的组名（心跳 / rebalance / 位点 / 锁 / 回投）都用包装后的值。
        if (!namespace_.empty()) {
            consumerGroup_ = NamespaceUtil::wrapNamespace(namespace_, consumerGroup_);
        }
        // 对应 Java DefaultMQPushConsumerImpl.checkConfig(:1026) / Python：先 Validators.checkGroup
        // （blank / 120 长度 / 字符表），再挡 DEFAULT_CONSUMER —— 共用默认组会让
        // broker 侧的订阅关系判定把两组混在一起，回投与重平衡都错乱。
        // ⚠ checkConfig 是 Java start() 的第一步，这里也领先于地址/订阅检查：
        // 配置非法时既不碰网络，也不该被后面的"未订阅"错误盖掉真正原因。
        Validators::checkGroup(consumerGroup_);
        if (consumerGroup_ == MixAll::DEFAULT_CONSUMER_GROUP) {
            throw MQClientException(
                "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");
        }
        // 静态地址与动态取址（ROCKETMQ_NAMESRV_DOMAIN）二选一必须可用
        if (nameServerAddrs_.empty() && !DefaultTopAddressing::isConfigured()) {
            throw MQClientException("name server address is not set");
        }
        // 对应 Java DefaultMQPushConsumerImpl.checkConfig(:1067)：策略为 null 直接拒绝启动，
        // 而不是等 rebalance 解引用空指针（UB）。
        if (allocateStrategy_ == nullptr) {
            throw MQClientException("allocateMessageQueueStrategy is null");
        }
        if (subscriptionData_.empty()) {
            throw MQClientException("subscription is not set, call subscribe() first");
        }
        if (messageListener_ == nullptr) {
            throw MQClientException("message listener is not set");
        }
        // 对应 Java checkConfig 的数值段（:1099-1209）：必须排在所有 null 检查之后、
        // 建 MQClientInstance 之前 —— 起了后台线程再抛错就泄漏线程了。
        checkConfigRanges();
        // Java `DefaultMQPushConsumerImpl#start`:934-936：只有 CLUSTERING 才
        // `changeInstanceNameToPID`（BROADCASTING 保持 "DEFAULT"，Java 的 MQClientManager
        // 因此让同进程的广播消费者复用同一份实例），再由 `ClientConfig#buildMQClientId`
        // 拼 `<本机 IP>@<instanceName>`。
        if (messageModel_ == MessageModel::CLUSTERING) {
            instanceName_ = changeInstanceNameToPID(instanceName_);
        }
        if (clientId_.empty()) {
            clientId_ = buildClientId(instanceName_, unitName_, enableStreamRequestType_);
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
                                             tlsEnable_, unitName_,
                                             pollNameServerIntervalMillis_));
        // 请求钩子（ACL 签名 / stream 的 `ReqT`）：绑定在 **start() 之前** ——
        // Java 的 rpcHook 在 MQClientAPIImpl 构造时传入，实例第一笔报文就带着它。
        std::shared_ptr<RPCHook> requestHook =
            composeRequestHooks(enableStreamRequestType_, rpcHook_);
        if (requestHook && !mqClient_->registerRPCHook(requestHook)) {
            logger_warn("consumer rpc hook ignored: MQClientInstance already has one (clientId="
                        + clientId_ + ")");
        }
        mqClient_->start();
        // 动态 name server：实例启动时可能已从地址服务器拿到地址，回填到本消费者
        // （Java 由共享的 ClientConfig 天然同步）
        if (nameServerAddrs_.empty() && !mqClient_->nameServerAddrs().empty()) {
            nameServerAddrs_ = mqClient_->nameServerAddrs();
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

    // broker 主动通知 40（成员变化 → 立即重算）**不**在这里注册处理器：
    // Java 把它注册在 MQClientAPIImpl（实例级），而 remoting 表里一个 code 只有一个
    // 处理器，各自注册会互相覆盖。改成向实例登记「叫醒」回调，由实例收到后扇出。
    // 回调跑在 remoting 读线程上，只做置位 + notify，不发 RPC。
    mqClient_->registerRebalanceWakeup(consumerGroup_, [this] { this->wakeRebalanceLoop(); });

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
    // 对齐 Java DefaultMQPushConsumerImpl.start:1013-1020：路由到手之后、心跳之前，把
    // 非 TAG 订阅发给 broker 校验（CHECK_CLIENT_CONFIG 46）。SQL92 写错时 broker 的过滤层
    // 会**静默放行全部消息**，只有这一笔请求能把它变成启动错误；失败就地回滚后再上抛。
    try {
        std::vector<SubscriptionData> subs;
        subs.reserve(subscriptionData_.size());
        for (const auto& kv : subscriptionData_) subs.push_back(kv.second);
        mqClient_->checkSubscriptionsInBroker(consumerGroup_, subs);
    } catch (const std::exception&) {
        shutdown();
        throw;
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
    // 这四个常驻循环与拉取线程同一口径：**异常必须在边界上接住**。线程里逃出一个异常
    // 就是 std::terminate（整个进程陪葬，而不是这一个消费者少干活）；shutdown 先把
    // started_ 置 false 再依次 join 线程，中间任何一句 client() 都会抛 "consumer not started"。
    dispatchThread_ = std::thread([this]() {
        setThreadName("ConsumeMessageThread");
        runLoop("dispatch", [this] { dispatchLoop(); });
    });
    persistThread_ = std::thread([this]() {
        setThreadName("MQClientFactoryScheduledThread");
        runLoop("offsetPersist", [this] { offsetPersistLoop(); });
    });
    lockThread_ = std::thread([this]() {
        setThreadName("ConsumeMessageOrderlyServiceThread");
        runLoop("orderlyLock", [this] { lockLoop(); });
    });
    rebalanceThread_ = std::thread([this]() {
        setThreadName("RebalanceThread");
        runLoop("rebalance", [this] { rebalanceLoop(); });
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
    pullOwners_.clear();
    {
        std::lock_guard<std::mutex> lk(lock_);
        lastPullAt_.clear();
    }
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
        // 叫醒回调捕获了 this：消费线程都已 join，先把回调摘掉，剩下
        // （注销客户端、关连接）这段时间里 broker 再推 40 也不会回调到半个线程上。
        mqClient_->unregisterRebalanceWakeup(consumerGroup_);
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

// 常驻循环线程的边界。两件事缺一不可：
//   1. **接住异常**：线程里逃出异常 = std::terminate，整个进程陪葬，而不是这一个消费者
//      少干活。shutdown 第一步就把 started_ 置 false，之后任何一句 client() 都会抛
//      "consumer not started"，而它 join 各线程是在更后面 —— 中间的空档必然有人踩到。
//   2. **还在运行就重进循环**：只接不重跑，等于把一次偶发异常变成"心跳照发、位点照刷、
//      就是不再消费"的静默停摆，比崩溃更难查。重进前睡 1s，持续失败会在日志里留痕。
void DefaultMQPushConsumer::runLoop(const std::string& what, const std::function<void()>& body) {
    while (started_.load() && !stop_.load()) {
        try {
            body();
            return;  // 循环自己退出了（看到停机），正常收工
        } catch (const std::exception& e) {
            logger_warn(what + " loop threw: " + e.what());
        } catch (...) {
            logger_warn(what + " loop threw: unknown exception");
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
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
    std::vector<std::pair<MessageQueue, int64_t>> retired;
    {
        std::lock_guard<std::mutex> lk(lock_);
        // 1. 撤：被分走的 + **拉取停摆**的（Java 的 !mqSet.contains(mq) 与
        //    pq.isPullExpired() 两个分支，RebalanceImpl:438-461）。停摆这一支防的是
        //    "循环线程死了/卡住了但队列还归本实例"——不撤就永久静默，且没有任何异常。
        for (auto it = pullThreads_.begin(); it != pullThreads_.end();) {
            const std::string key = it->first;
            const bool revoked = current.find(key) == current.end();
            // 停机期间线程本来就陆续退出，这时不判停摆（否则会刷一堆假 [BUG] 日志）
            const bool stalled = !revoked && started_.load() && !stop_.load()
                && pullStalledLocked(key);
            if (!revoked && !stalled) {
                ++it;
                continue;
            }
            if (stalled) {
                // Java RebalanceImpl:449 的告警原文，用于排查"消费停摆被自愈"的现场
                logger_error("[BUG]doRebalance, " + consumerGroup_
                             + ", try remove unnecessary mq, " + key
                             + ", because pull is pause, so try to fixed it");
            }
            MessageQueue mq;
            auto mit = mqMap_.find(key);
            if (mit != mqMap_.end()) mq = mit->second;
            else {
                auto cit = current.find(key);
                if (cit != current.end()) mq = cit->second;   // 停摆队列可能一条都没拉过
            }
            int64_t off = -1;
            auto oit = consumeOffsetTable_.find(key);
            if (oit != consumeOffsetTable_.end()) off = oit->second;
            if (!mq.topic.empty()) retired.emplace_back(mq, off);
            retiredThreads_.push_back(std::move(it->second));
            // 撤走归属：旧线程下一轮 ownsQueue 即失效并退出，即便同一队列马上
            // 重新分配给本实例，也会拿到一份**新**凭据、起一条新线程。
            pullOwners_.erase(key);
            lastPullAt_.erase(key);
            mqMap_.erase(key);
            pending_.erase(key);
            lockOk_.erase(key);
            offsetTable_.erase(key);
            consumeOffsetTable_.erase(key);
            // POP：标记 dropped，在途批次不再消费也不 ack（交给 broker 复活）
            auto pqit = popQueues_.find(key);
            if (pqit != popQueues_.end()) {
                pqit->second->setDropped(true);
                popQueues_.erase(pqit);
            }
            it = pullThreads_.erase(it);
        }
    }
    // 撤的收尾（持久化位点 / UNLOCK）必须在起新线程**之前**做完：反过来会让新循环
    // 拿旧位点起拉、又把更小的位点写回去，白增重复投递。网络/落盘在锁外做。
    if (!retired.empty()) {
        onQueuesRevoked(retired);
    }
    {
        std::lock_guard<std::mutex> lk(lock_);
        // 2. 建：为缺失的队列起拉取线程。
        //    std::thread 一构造就跑（不像 Python/.NET 能「先入表再 start」），所以
        //    「写 pullOwners_」必须在构造之前，而「写 pullThreads_」必须与它同批完成。
        //    错开一步就有两种坏结果：
        //      * 新线程先跑到 ownsQueue()，看到表里还没有自己 ⇒ 当场退出；而 key
        //        随后被登记成「已有拉取线程」，之后每轮 rebalance 都不会再起 ——
        //        这条队列**永久静默**，落在它上面的消息一条都不会投（真机少一整批
        //        消息、且重复数=0 的根因）。
        //      * 两轮 rebalance 交错给同一 key 起两条线程 ⇒ 同一队列重复消费。
        //    新线程第一轮要拿的正是这把锁，它会等到我们 release，看到的一定是登记完
        //    成之后的状态，所以「先跑起来看不到归属」的窗口被彻底关死。
        for (const auto& kv : current) {
            if (pullThreads_.find(kv.first) != pullThreads_.end()) continue;
            // shutdown 已经开始就只撤不建：`started_` 在 shutdown 第一步就被置成 false，
            // 而 `client()` 此后必抛（"consumer not started"）——此刻起的线程一进门就死在
            // 第一句上；更糟的是 shutdown 的 join 循环可能已经过去，没人 join 这条新线程，
            // 它能活到本对象析构之后。Java 的 RebalanceService 是先 `isStopped` 再建，同一口径。
            if (stop_.load() || !started_.load()) return;
            if (popMode_ && popQueues_.find(kv.first) == popQueues_.end()) {
                popQueues_[kv.first] = std::make_shared<PopProcessQueue>();
            }
            const uint64_t token = ++nextPullToken_;
            pullOwners_[kv.first] = token;
            // 队列一旦分配就进 mqMap_（Java ProcessQueueTable 的键集即"已分配"），不等
            // 第一条消息：位点持久化、307 运行信息、停摆自愈都要靠这份映射找得到队列。
            mqMap_[kv.first] = kv.second;
            // 线程刚建、还没跑到盖章处，先用当前时刻占位，避免下一趟误判停摆
            lastPullAt_[kv.first] = UtilAll::currentTimeMillis();
            const std::string key = kv.first;
            const MessageQueue mq = kv.second;
            try {
                std::thread t([this, mq, key, token]() {
                    // 线程体的**边界**必须自己接住异常：这里逃出去就是 std::terminate，
                    // 整个进程陪葬（真机实测：shutdown 与一轮 rebalance 抢在一起时，
                    // 新起的线程第一句 client() 抛 "consumer not started"，把跑了一般的
                    // 用例全带走）。消费循环内部的异常一律就地转成停摆上报。
                    try {
                        if (popMode_) {
                            setThreadName("PopMessageService");
                            queuePopLoop(mq, token);
                        } else {
                            setThreadName("PullMessageService");
                            queuePullLoop(mq, token);
                        }
                    } catch (const std::exception& e) {
                        logger_warn("pull loop died, queue=" + key + ": " + e.what());
                    } catch (...) {
                        logger_warn("pull loop died, queue=" + key + ": unknown exception");
                    }
                    // 走到这里说明循环自己返回了。若它返回时**仍然持有**该队列，就不是
                    // 被 rebalance 撤走的正常退出，而是"订阅没了/异常打穿"这类死法：
                    // 报到停摆，下一趟撤掉重建（std::thread 死了问不出 is_alive）。
                    // 停机路上这一步无害：此刻已经没有下一趟 rebalance 了。
                    markPullLoopExited(key, token);
                });
                pullThreads_[key] = std::move(t);
            } catch (const std::system_error& e) {
                // 起线程失败（资源耗尽）必须把归属收回，留着一个没人认领的 token
                // 等于给这条队列判了永久静默；收回去下一轮 rebalance 会重试。
                pullOwners_.erase(key);
                lastPullAt_.erase(key);
                logger_warn("rebalance: cannot start pull thread for " + key + ": " + e.what());
            }
        }
    }
}

void DefaultMQPushConsumer::stampPullAt(const std::string& key) {
    const int64_t now = UtilAll::currentTimeMillis();
    {
        std::lock_guard<std::mutex> lk(lock_);
        lastPullAt_[key] = now;
    }
    if (popMode_) {
        std::shared_ptr<PopProcessQueue> pq;
        {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = popQueues_.find(key);
            if (it != popQueues_.end()) pq = it->second;
        }
        // Java PopProcessQueue.lastPopTimestamp 同一个时刻盖章（:508）
        if (pq) pq->setLastPopTimestamp(now);
    }
}

bool DefaultMQPushConsumer::pullStalledLocked(const std::string& key) const {
    // Java ProcessQueue.isPullExpired / PopProcessQueue.isPullExpired：只看"多久没发起过
    // 一次拉取/弹出"。没盖过章（新线程还没跑到入口）不算停摆，否则刚分配就被撤。
    auto it = lastPullAt_.find(key);
    if (it == lastPullAt_.end()) return false;
    if (it->second == 0) return true;      // 循环自行退出的标记
    return UtilAll::currentTimeMillis() - it->second > kPullMaxIdleTime;
}

void DefaultMQPushConsumer::markPullLoopExited(const std::string& key, uint64_t token) {
    if (stop_.load() || !started_.load()) return;
    std::lock_guard<std::mutex> lk(lock_);
    // 直接查归属表：ownsQueue() 也要拿同一把非递归锁，这里已经持着它了。
    auto it = pullOwners_.find(key);
    if (it == pullOwners_.end() || it->second != token) return;   // 被撤走的正常退出
    lastPullAt_[key] = 0;
}

bool DefaultMQPushConsumer::pullStalled(const std::string& key) const {
    std::lock_guard<std::mutex> lk(lock_);
    return pullStalledLocked(key);
}

bool DefaultMQPushConsumer::flowControlHit(const MessageQueue& mq, const std::string& key) {
    // Python `_flow_control_hit`（Java ProcessQueue 的五个阈值）：先队列级三条（条数、字节、
    // 位点跨度），再 topic 级累计两条。字节阈值单位是 **MiB**，跨度是 pending 里
    // queueOffset 的 max-min 且**严格大于**才算（Java 同）。
    // 只有开着 topic 级阈值时才去遍历别的队列，否则一次判定多走一遍全表。
    size_t count = 0;
    double sizeMb = 0.0;
    int64_t span = 0;
    size_t topicCount = 0;
    double topicSizeMb = 0.0;
    {
        std::lock_guard<std::mutex> lk(lock_);
        auto it = pending_.find(key);
        if (it != pending_.end()) {
            int64_t bytes = 0;
            int64_t minOffset = 0;
            int64_t maxOffset = 0;
            bool first = true;
            for (const MessageExt& m : it->second) {
                bytes += m.storeSize;
                if (first) {
                    minOffset = maxOffset = m.queueOffset;
                    first = false;
                } else {
                    minOffset = std::min(minOffset, m.queueOffset);
                    maxOffset = std::max(maxOffset, m.queueOffset);
                }
            }
            count = it->second.size();
            sizeMb = static_cast<double>(bytes) / (1024.0 * 1024.0);
            span = maxOffset - minOffset;
        }
        if (pullThresholdForTopic_ > 0 || pullThresholdSizeForTopic_ > 0) {
            int64_t topicBytes = 0;
            for (const auto& kv : pending_) {
                auto mqIt = mqMap_.find(kv.first);
                if (mqIt == mqMap_.end() || mqIt->second.topic != mq.topic) continue;
                for (const MessageExt& m : kv.second) {
                    ++topicCount;
                    topicBytes += m.storeSize;
                }
            }
            topicSizeMb = static_cast<double>(topicBytes) / (1024.0 * 1024.0);
        }
    }

    char number[32] = {0};
    std::string reason;
    if (count >= static_cast<size_t>(std::max(1, pullThresholdForQueue_))) {
        reason = "count=" + std::to_string(count);
    } else if (pullThresholdSizeForQueue_ > 0 && sizeMb >= pullThresholdSizeForQueue_) {
        std::snprintf(number, sizeof(number), "%.1f", sizeMb);
        reason = std::string("size=") + number + "MB";
    } else if (consumeConcurrentlyMaxSpan_ > 0 && span > consumeConcurrentlyMaxSpan_) {
        reason = "span=" + std::to_string(span);
    } else if (pullThresholdForTopic_ > 0
               && topicCount >= static_cast<size_t>(pullThresholdForTopic_)) {
        reason = "topicCount=" + std::to_string(topicCount);
    } else if (pullThresholdSizeForTopic_ > 0 && topicSizeMb >= pullThresholdSizeForTopic_) {
        std::snprintf(number, sizeof(number), "%.1f", topicSizeMb);
        reason = std::string("topicSize=") + number + "MB";
    }
    if (reason.empty()) return false;
    flowControlTriggered_.fetch_add(1);
    logger_debug("flow control: queue " + mq.toString() + " " + reason + ", pause pull");
    return true;
}

int64_t DefaultMQPushConsumer::lastPullAt(const std::string& key) const {
    std::lock_guard<std::mutex> lk(lock_);
    auto it = lastPullAt_.find(key);
    return it == lastPullAt_.end() ? -1 : it->second;
}

void DefaultMQPushConsumer::setLastPullAt(const std::string& key, int64_t millis) {
    std::lock_guard<std::mutex> lk(lock_);
    lastPullAt_[key] = millis;
}

void DefaultMQPushConsumer::syncPullThreads() { rebalancePullThreads(); }

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

void DefaultMQPushConsumer::queuePullLoop(const MessageQueue& mq, uint64_t token) {
    // 停机路上被起来过一趟（见 rebalancePullThreads 的"只撤不建"守卫）：直接收工，
    // 别去碰 client()——shutdown 第一步就把 started_ 置了 false，那时它必抛。
    if (!started_.load() || stop_.load()) return;
    MQClientInstance& c = client();
    const bool orderly = isOrderly();
    const std::string key = offsetKey(mq);
    while (!stop_.load() && started_.load()) {
        // 长轮询期间被 rebalance 撤走（队列或换了拉取线程）即失效：直接退出本线程，
        // 由新属主从我们最后持久化的位点接手，避免两实例重复消费同一条消息。
        if (!ownsQueue(key, token)) {
            return;
        }
        // Java DefaultMQPushConsumerImpl.pullMessage:253 —— 每次**发起**拉取就盖时刻，
        // 在流控/锁判定之前：判据是"这条循环还在跑"，不是"这轮真的打了网络"。
        stampPullAt(key);
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
        // 流控（Java ProcessQueue 的五个阈值，见 flowControlHit）：命中任一条就暂停本队列拉取
        if (flowControlHit(mq, key)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }
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
        if (!ownsQueue(key, token)) {
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
void DefaultMQPushConsumer::queuePopLoop(const MessageQueue& mq, uint64_t token) {
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
        if (!ownsQueue(key, token)) return;
        std::shared_ptr<PopProcessQueue> pq;
        {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = popQueues_.find(key);
            if (it == popQueues_.end()) return;
            pq = it->second;
        }
        if (pq->isDropped()) return;
        // Java DefaultMQPushConsumerImpl.popMessage:508 —— 发起即盖章（POP 模式的
        // isPullExpired 读的就是 PopProcessQueue.lastPopTimestamp）。
        stampPullAt(key);
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
        if (!ownsQueue(key, token) || pq->isDropped()) {
            logger_debug("queue " + key + " revoked during pop, discard "
                         + std::to_string(result.msgFoundList.size()) + " messages un-acked");
            return;
        }
        // 拉取统计（Java PopCallback.onSuccess:556-563，判定见 recordPopPullStats）
        recordPopPullStats(client().consumerStats(), mq.topic, result, began);
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

void DefaultMQPushConsumer::recordPopPullStats(ConsumerStatsManager& stats,
                                               const std::string& topic,
                                               const PopResult& result, int64_t beganMs) {
    // Java 的 pull 回调每次都记 RT，POP 回调（popMessage:556-563）只在 FOUND 记，而且
    // 是在判空**之前**记；TPS 只按真正弹到的条数记。照抄这个不对称：POP 的空手而归是
    // 长轮询常态（POLLING_NOT_FOUND），把它算进 RT 等于用挂起时长稀释平均拉取耗时。
    if (result.status != PopStatus::FOUND) return;
    stats.incPullRT(consumerGroup_, topic, UtilAll::currentTimeMillis() - beganMs);
    if (!result.msgFoundList.empty()) {
        stats.incPullTPS(consumerGroup_, topic,
                         static_cast<int64_t>(result.msgFoundList.size()));
    }
}

void DefaultMQPushConsumer::submitPopConsumeRequest(std::vector<MessageExt> msgs,
                                                    std::shared_ptr<PopProcessQueue> pq,
                                                    const MessageQueue& mq) {
    // 对应 Java ConsumeMessagePopConcurrentlyService.submitPopConsumeRequest。
    // 投给**core/max 两档线程池**（core=consumeThreadMin / max=consumeThreadMax，队列无界）：
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
    // 对应 Java ConsumeMessagePopConcurrentlyService：POP 路径把 ackIndex 当
    // 「已 ack 到第几条」用，语义与 classic 回投路径一致，默认值 Integer.MAX_VALUE
    // 已经表示「整批 ack」；这里钳到 size-1 只是让 ctx 上的值与本批条数对齐，
    // 不影响 CONSUME_SUCCESS 的行为（不 ack 会让消息在 invisibleTime 后被复活重投）。
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
        // Java `DefaultMQPushConsumerImpl:640`：filterMessageContext.setUnitMode(
        // this.defaultMQPushConsumer.isUnitMode()) —— 钩子据此判断是否单元化流量
        context.unitMode = unitMode_;
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
                                               int64_t beginMs, bool failed,
                                               const std::optional<int64_t>& ackCount) {
    if (!mqClient_) return;
    auto& stats = mqClient_->consumerStats();
    const int64_t rt = UtilAll::currentTimeMillis() - beginMs;
    if (failed) {
        stats.incConsumeFailedTPS(consumerGroup_, topic, msgCount);
    } else {
        // Java processConsumeResult:217-225 —— ok = ackIndex + 1，部分 ack 时
        // 前缀记 OK、尾巴记 FAILED（否则整批算成功会低估失败量、看不出有多少条要重投）
        const int64_t ok = ackCount.has_value() ? *ackCount : msgCount;
        stats.incConsumeOKTPS(consumerGroup_, topic, ok);
        if (msgCount > ok) stats.incConsumeFailedTPS(consumerGroup_, topic, msgCount - ok);
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
        auto makePqi = [&](int64_t commitOffset, int64_t cachedMsgCount, bool droped,
                           int64_t lastPullTimestamp) {
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
            pqi.set("lastPullTimestamp", JsonValue::makeInt(lastPullTimestamp));
            pqi.set("lastConsumeTimestamp", JsonValue::makeInt(0));
            return pqi;
        };
        for (const auto& kv : mqMap_) {
            // POP 模式下弹出去 popQueues_（Java 的 popProcessQueueTable），classic 的
            // processQueueTable 是空的 —— 两把表在 Java 里互斥，307 里也必须互斥：同一把队列
            // 既进 mqTable 又进 mqPopTable 会让控制台把一路消费数成两路。mqMap_ 是"已分配"
            // 注册表（自愈、位点持久化都靠它），两种模式都写，所以这里按模式过滤而不是不写。
            if (popMode_ && popQueues_.find(kv.first) != popQueues_.end()) continue;
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
            // Java ProcessQueue.fillOutRunningInfo:456 报 lastPullTimestamp。
            // 写死 0 等于把"这一路多久没拉了"这份停摆判据的现场证据全丢了。
            int64_t lastPull = 0;
            auto lt = lastPullAt_.find(kv.first);
            if (lt != lastPullAt_.end()) lastPull = lt->second;
            info.mqTable.set(mqKey, makePqi(commit, cached, false, lastPull));
        }
        if (popMode_) {
            for (const auto& kv : popQueues_) {
                auto mt = mqMap_.find(kv.first);
                if (mt == mqMap_.end()) continue;
                const MessageQueue& mq = mt->second;
                const std::string mqKey = "{\"brokerName\":\"" + mq.brokerName + "\",\"queueId\":"
                    + std::to_string(mq.queueId) + ",\"topic\":\"" + mq.topic + "\"}";
                // Java PopProcessQueue 用 lastPopTimestamp 顶替 lastPullTimestamp 判停摆
                // （isPullExpired:74-76），这里填同一个时刻保持可比。
                info.mqPopTable.set(mqKey, makePqi(0, kv.second->waitAckCount(),
                                                   kv.second->isDropped(),
                                                   kv.second->lastPopTimestamp()));
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
        if (status == ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT &&
            checkOrderlyReconsumeTimes(restored)) {
            // Java processConsumeResult:254-266：挂起之前要先过 checkReconsumeTimes。
            // 只有「还在重试次数内 / 回投失败」才把这一批塞回队首原地重试；已经交给
            // broker 的（回投成功）要前进位点，否则一条毒消息永久占住这条队列
            //（顺序消费的 head-of-line blocking 在真机上就是"这个组停在第 N 条不动"）。
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
    // Java processConsumeResult:207-229 —— CONSUME_SUCCESS 用 listener 设的 ackIndex
    // 划分「已认可前缀 / 待回投后缀」（默认 Integer.MAX_VALUE，钳到 size-1 即整批认可）；
    // RECONSUME_LATER 强制 ackIndex=-1，整批回投。
    const auto size32 = static_cast<int32_t>(restored.size());
    int32_t ackIndex = ctx.ackIndex;
    if (status == ConsumeConcurrentlyStatus::CONSUME_SUCCESS) {
        if (ackIndex >= size32) {
            ackIndex = size32 - 1;
        }
    } else {
        ackIndex = -1;
    }
    const size_t acked = static_cast<size_t>(ackIndex + 1);  // 0..size
    recordConsumeStats(mq.topic, size32, hookBeginMs,
                       status == ConsumeConcurrentlyStatus::RECONSUME_LATER,
                       static_cast<int64_t>(acked));
    if (useHook) {
        const bool ok = (status == ConsumeConcurrentlyStatus::CONSUME_SUCCESS);
        finishConsumeHook(&hookCtx, hookHasException, hookBeginMs, !ok, ok,
                          ok ? "CONSUME_SUCCESS" : "RECONSUME_LATER");
    }
    if (broadcast) {
        // Java:232-237 —— 广播模式不回投：未认可的尾巴只打一条 warn 就丢掉，
        // 整批位点照样前进（重启后不重投）
        const size_t dropped = restored.size() - acked;
        if (dropped > 0) {
            logger_warn("BROADCASTING, the message consume failed, drop it: "
                        + std::to_string(dropped) + " msgs in " + mq.toString());
        }
        advanceConsumeOffset(key, restored);
        consumedCount_.fetch_add(static_cast<int64_t>(restored.size()));
        return true;
    }
    if (acked >= restored.size()) {
        // 整批认可（默认路径）：一条都不用回投，位点直接前进
        advanceConsumeOffset(key, restored);
        consumedCount_.fetch_add(static_cast<int64_t>(restored.size()));
        return true;
    }
    // 集群模式：未认可的 [acked, size) 逐条回投 %RETRY%topic
    //（延迟梯度 3+reconsumeTimes，超限由 broker 转 %DLQ%）
    const std::vector<std::pair<size_t, MessageExt>> msgBackFailed = sendBackBatch(restored, ctx, acked);
    std::set<size_t> failedIdx;
    int64_t floorVal = 0;
    bool hasFloor = false;
    for (const std::pair<size_t, MessageExt>& p : msgBackFailed) {
        failedIdx.insert(p.first);
        const int64_t off = restored[p.first].queueOffset;
        if (!hasFloor || off < floorVal) {
            floorVal = off;
            hasFloor = true;
        }
    }
    // Java:256-260 —— 回投失败的那几条塞回队首稍后重试（ProcessQueue 里不摘掉它们）
    if (!msgBackFailed.empty()) {
        std::lock_guard<std::mutex> lk(lock_);
        auto it = pending_.find(key);
        if (it != pending_.end()) {
            for (auto rit = msgBackFailed.rbegin(); rit != msgBackFailed.rend(); ++rit) {
                it->second.push_front(rit->second);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    // Java:266-269 —— 提交的是「本批已处理条目里最大的 queueOffset + 1」，且不能越过
    // 回投失败、仍留在缓冲里的那几条，否则那几条会被位点静默跳过（丢消息）
    std::vector<MessageExt> handled;
    handled.reserve(restored.size() - failedIdx.size());
    for (size_t i = 0; i < restored.size(); i++) {
        if (failedIdx.count(i) == 0) {
            handled.push_back(restored[i]);
        }
    }
    advanceConsumeOffset(key, handled, hasFloor ? std::optional<int64_t>(floorVal) : std::nullopt);
    consumedCount_.fetch_add(static_cast<int64_t>(handled.size()));
    return msgBackFailed.empty();
}

std::vector<std::pair<size_t, MessageExt>> DefaultMQPushConsumer::sendBackBatch(
    const std::vector<MessageExt>& batch, const ConsumeConcurrentlyContext& ctx, size_t base) {
    std::vector<std::pair<size_t, MessageExt>> failed;
    for (size_t i = base; i < batch.size(); i++) {
        const MessageExt& msg = batch[i];
        // Java：delayLevelWhenNextConsume == 0 → 3 + reconsumeTimes
        //（reconsumeTimes 在 MessageExt 线上格式第 13 字段，broker 重投时 +1）
        int32_t delayLevel = ctx.delayLevelWhenNextConsume;
        if (delayLevel == 0) {
            delayLevel = 3 + msg.getReconsumeTimes();
        }
        // ⚠ sendMessageBack 自己吞掉所有异常、用返回值表成败；只看异常等于把
        // 「回投失败」当成「回投成功」，位点会越过这条静默丢消息。
        if (!sendMessageBack(msg, delayLevel)) {
            logger_debug("send message back failed for msg " + msg.msgId);
            MessageExt retried = msg;
            // 与 Java :251 一致：次数加在**要被重新消费的副本**上，broker 没记成功
            retried.setReconsumeTimes(retried.getReconsumeTimes() + 1);
            failed.emplace_back(i, retried);
        }
    }
    return failed;
}

void DefaultMQPushConsumer::advanceConsumeOffset(const std::string& key,
                                                 const std::vector<MessageExt>& batch,
                                                 const std::optional<int64_t>& floor) {
    if (batch.empty()) {
        // 整批回投都失败时没有任何条目被认可，位点原地不动
        return;
    }
    int64_t nextOffset = 0;
    for (const MessageExt& m : batch) {
        if (m.queueOffset + 1 > nextOffset) {
            nextOffset = m.queueOffset + 1;
        }
    }
    if (floor.has_value() && *floor < nextOffset) {
        nextOffset = *floor;
    }
    std::lock_guard<std::mutex> lk(lock_);
    auto it = consumeOffsetTable_.find(key);
    if (it == consumeOffsetTable_.end() || it->second < nextOffset) {
        consumeOffsetTable_[key] = nextOffset;
    }
}

// ---------------------------------------------------------------- 位点持久化
void DefaultMQPushConsumer::offsetPersistLoop() {
    // Java MQClientInstance.startScheduledTask:417-423：
    //   scheduleAtFixedRate(persistAllConsumerOffset, 1000 * 10, persistConsumerOffsetInterval)
    // ——首个任务延迟 10s，之后周期取 clientConfig.persistConsumerOffsetInterval（默认 5s）；
    // 首笔落盘发生在 initialDelay **这一刻**，不是 initialDelay + 一个周期后（旧写法要 15s）。
    // 周期是**固定速率**：每跳锚定在 10s + n×周期（见 schedule_util.h），不会像"干完再按
    // 100ms 切片睡一个周期"那样被每段多出来的几毫秒越拖越长。
    // 与 Java 一致：周期只在启动时读一次，运行期改字段不改变已排定的节奏。
    auto next = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!stop_.load()) {
        if (!started_.load()) return;
        sleepUntilDeadline(stop_, next);
        next += std::chrono::milliseconds(persistConsumerOffsetIntervalMillis_);
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
        // 等待 20s（期间响应 stop）。按绝对 deadline 分段睡：100ms 的系统 sleep 实测多几毫秒，
        // 200 段会把 Java 的 20s 轮次拖长；对着 deadline 算就不累积。
        sleepUntilDeadline(stop_, std::chrono::steady_clock::now() + std::chrono::seconds(20));
    }
}

std::string DefaultMQPushConsumer::offsetKey(const MessageQueue& mq) {
    return mq.topic + mq.brokerName + std::to_string(mq.queueId);
}

void DefaultMQPushConsumer::setPendingMessages(const std::string& key,
                                               const std::vector<MessageExt>& msgs) {
    std::lock_guard<std::mutex> lk(lock_);
    pending_[key] = std::deque<MessageExt>(msgs.begin(), msgs.end());
}

std::vector<MessageExt> DefaultMQPushConsumer::pendingMessages(const std::string& key) const {
    std::lock_guard<std::mutex> lk(lock_);
    auto it = pending_.find(key);
    if (it == pending_.end()) return {};
    return std::vector<MessageExt>(it->second.begin(), it->second.end());
}

std::optional<int64_t> DefaultMQPushConsumer::consumeOffset(const std::string& key) const {
    std::lock_guard<std::mutex> lk(lock_);
    auto it = consumeOffsetTable_.find(key);
    if (it == consumeOffsetTable_.end()) return std::nullopt;
    return it->second;
}

void DefaultMQPushConsumer::setAssignedQueue(const std::string& key, const MessageQueue& mq) {
    std::lock_guard<std::mutex> lk(lock_);
    mqMap_[key] = mq;
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
            // 自定义策略抛异常时：**保持现有分配**并结束本轮 rebalance
            // （Python client/consumer.py 的 try/except → return；Java 是 catch Throwable →
            //  log error → return false）。绝不能把该 topic 的队列撤走。
            std::vector<MessageQueue> got;
            try {
                got = allocateStrategy_->allocate(consumerGroup_, clientId_, mqAll, cidAll);
            } catch (const std::exception& e) {
                logger_error("allocate message queue exception. strategy name: "
                             + allocateStrategy_->getName() + ", ex: " + e.what());
                return;
            }
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

bool DefaultMQPushConsumer::ownsQueue(const std::string& key, uint64_t token) const {
    // 本拉取线程是否仍持有该队列（rebalance 撤走或换了拉取线程后即失效）。
    // 比 token 而不是比 std::thread 的 id：归属是在**起线程之前**登记的，线程自己
    // 拿不到自己的 std::thread 句柄，也就没有「先跑起来、后登记」的窗口。
    std::lock_guard<std::mutex> lk(lock_);
    auto it = pullOwners_.find(key);
    return it != pullOwners_.end() && it->second == token;
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

void DefaultMQPushConsumer::wakeRebalanceLoop() {
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
    // Java `MQClientInstance:1039` 心跳里带 consumerData.setUnitMode(tc.isUnitMode())：
    // broker 据此决定 %RETRY% topic 建出来带不带 UNIT_SUB 位
    cd.unitMode = unitMode_;
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

int32_t DefaultMQPushConsumer::maxReconsumeTimesOrDefault() const {
    // Java getMaxReconsumeTimes()：-1 表示用 broker 默认的 16
    return maxReconsumeTimes_ == -1 ? 16 : maxReconsumeTimes_;
}

// 对应 Java DefaultMQPushConsumerImpl#sendMessageBackAsNormalMessage：
// 回投请求失败时，把这条消息当成普通消息重新发到 %RETRY%group，
// 由 broker 按 DELAY 档位重投。属性置法与 Java 逐条一致。
void DefaultMQPushConsumer::sendMessageBackAsNormalMessage(const MessageExt& msg) {
    MQClientInstance& c = client();
    Message newMsg = buildRetryMessage(msg, maxReconsumeTimesOrDefault());
    std::shared_ptr<TopicPublishInfo> publish =
        c.getTopicPublishInfo(newMsg.topic, /*isDefault=*/true);
    MessageQueue selected = publish->selectOneMessageQueue();
    // 走的是 MQClientInstance 的**内部生产者**，Java 在构造它时调过
    // `resetClientConfig(clientConfig)`（MQClientInstance.java:218-219），
    // 所以 unitMode 与本消费者一致 —— 这里必须带上，否则重投消息会丢单元标记。
    c.sendMessage(MixAll::CLIENT_INNER_PRODUCER_GROUP, newMsg, selected, 3000, /*sysFlag=*/0,
                  unitMode_);
}

// Java DefaultMQPushConsumerImpl#sendMessageBackAsNormalMessage:1148-1160 与
// ConsumeMessageOrderlyService#sendMessageBack:341-362 的两个 newMsg 构造体**逐行相同**
// （只有 maxReconsumeTimes 各调各的 getter），所以抽成一个函数，避免两处属性置法漂移。
Message DefaultMQPushConsumer::buildRetryMessage(const MessageExt& msg,
                                                 int32_t maxReconsumeTimes) const {
    Message newMsg(MixAll::getRetryTopic(consumerGroup_), msg.body);
    newMsg.properties = msg.properties;
    newMsg.flag = msg.flag;
    std::string originMsgId = msg.getProperty(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID);
    if (originMsgId.empty()) originMsgId = msg.msgId;
    if (!originMsgId.empty()) {
        newMsg.putProperty(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID, originMsgId);
    }
    newMsg.putProperty(MessageConst::PROPERTY_RETRY_TOPIC, msg.topic);
    newMsg.putProperty(MessageConst::PROPERTY_RECONSUME_TIME,
                       std::to_string(msg.reconsumeTimes + 1));
    newMsg.putProperty(MessageConst::PROPERTY_MAX_RECONSUME_TIMES,
                       std::to_string(maxReconsumeTimes));
    // 半消息重投时不能带上 TRAN_MSG，否则 broker 会把它再当回查消息处理
    newMsg.properties.erase(MessageConst::PROPERTY_TRANSACTION_PREPARED);
    newMsg.putProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL,
                       std::to_string(3 + msg.reconsumeTimes));
    if (newMsg.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX).empty()) {
        newMsg.putProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                           InnerIdGenerator::createUniqId());
    }
    return newMsg;
}

int32_t DefaultMQPushConsumer::orderlyMaxReconsumeTimes() const {
    // Java ConsumeMessageOrderlyService#getMaxReconsumeTimes:313-320。
    // 顺序消费的消息一直停在本地队列里原地重试，broker 侧压根没有重投计数，所以默认
    // 就该重试到成功为止 —— -1 在这里读成 Integer.MAX_VALUE。**不是**并发侧的 16
    //（那边每轮回投都过一遍 broker，16 是 broker 的默认 retryMaxTimes）；把两套并成
    // 一个常量，等于要么给顺序消费凭空造出死信，要么让并发消息无限重投。
    return maxReconsumeTimes_ == -1 ? std::numeric_limits<int32_t>::max() : maxReconsumeTimes_;
}

bool DefaultMQPushConsumer::orderlySendMessageBack(const MessageExt& msg) {
    // Java ConsumeMessageOrderlyService#sendMessageBack:341-362：整个函数包在
    // try/catch(Exception) 里，只按返回值表成败。抛给消费线程等于这条既没 ack 也没
    // 回投，只能等 broker 锁超时 —— 而顺序消费的锁超时是分钟级，看起来就像卡死。
    try {
        MQClientInstance& c = client();
        Message newMsg = buildRetryMessage(msg, orderlyMaxReconsumeTimes());
        std::shared_ptr<TopicPublishInfo> publish =
            c.getTopicPublishInfo(newMsg.topic, /*isDefault=*/true);
        if (!publish || publish->msgQueueList.empty()) {
            logger_debug("orderly send back has no writable queue, topic=" + newMsg.topic);
            return false;
        }
        const MessageQueue selected = publish->selectOneMessageQueue();
        c.sendMessage(MixAll::CLIENT_INNER_PRODUCER_GROUP, newMsg, selected, 3000, /*sysFlag=*/0,
                      unitMode_);
        return true;
    } catch (const std::exception& e) {
        logger_debug(std::string("orderly send message back failed, group=") + consumerGroup_ +
                     ": " + e.what());
        return false;
    }
}

bool DefaultMQPushConsumer::checkOrderlyReconsumeTimes(std::vector<MessageExt>& msgs) {
    // Java ConsumeMessageOrderlyService#checkReconsumeTimes:322-339。逐条两种走法：
    //   - 次数没用尽：本地 reconsumeTimes + 1（broker 那边没记这次失败，客户端不补就
    //     永远到不了阈值），继续挂起；
    //   - 次数已用尽：交给 broker 回投。**回投成功就不挂起**（Java 这时 commit 位点，
    //     毒消息让路、队列继续往前），回投失败才 +1 并挂起。
    bool suspend = false;
    const int32_t maxTimes = orderlyMaxReconsumeTimes();
    for (MessageExt& msg : msgs) {
        if (msg.reconsumeTimes >= maxTimes) {
            if (!orderlySendMessageBack(msg)) {
                suspend = true;
                msg.setReconsumeTimes(msg.reconsumeTimes + 1);
            }
        } else {
            suspend = true;
            msg.setReconsumeTimes(msg.reconsumeTimes + 1);
        }
    }
    return suspend;
}

bool DefaultMQPushConsumer::sendMessageBack(const MessageExt& msg, int32_t delayLevel,
                                           const std::string& brokerNameIn) {
    std::string brokerName = brokerNameIn.empty() ? msg.brokerName : brokerNameIn;
    // Java：整个回投过程包在 try/catch(Throwable) 里，失败退化到"普通消息重投"，
    // 绝不把异常抛给消费线程（否则这条消息既没 ack 也没回投，只能等超时重复消费）。
    // ⚠ client() 在未 start 时会抛，所以必须留在 try 内：本函数的契约是「只按返回值
    // 表成败」，调用方（sendBackBatch）已经不再看异常了。
    try {
        MQClientInstance& c = client();
        std::string addr = c.brokerAddrOf(brokerName);
        if (addr.empty()) {
            throw MQClientException("Broker[" + brokerName + "] master node does not exist");
        }
        auto header = std::make_shared<ConsumerSendMsgBackRequestHeader>();
        header->offset = msg.commitLogOffset;
        header->group = consumerGroup_;
        header->delayLevel = delayLevel;
        header->originMsgId = msg.msgId;
        header->originTopic = msg.topic;
        // ⚠ 有意超出 Java：MQClientAPIImpl#consumerSendMessageBack(:1684-1693) 只填
        // group/offset/delayLevel/originMsgId/originTopic/maxReconsumeTimes/brokerName，
        // 从不写 unitMode，字段恒为 false。broker 侧确实读它
        // （AbstractSendMessageProcessor:135-138 → buildSysFlag(false, true)），所以这里
        // 按消费者配置如实上报，单元化重试 topic 才会带上 UNIT_SUB 标记。
        header->unitMode = unitMode_;
        header->maxReconsumeTimes = maxReconsumeTimesOrDefault();
        RemotingCommand request =
            RemotingCommand::createRequestCommand(RequestCode::CONSUMER_SEND_MSG_BACK, header);
        RemotingCommand response = c.remotingClient().invokeSync(addr, request, 5000);
        if (response.code != ResponseCode::SUCCESS) {
            throw MQBrokerException(response.code, response.remark);
        }
        return true;
    } catch (const std::exception& e) {
        logger_error("Failed to send message back, consumerGroup=" + consumerGroup_
                     + ", brokerName=" + brokerName + ", msg=" + msg.msgId
                     + ", fallback to normal send: " + e.what());
    } catch (...) {
        logger_error("Failed to send message back, consumerGroup=" + consumerGroup_
                     + ", brokerName=" + brokerName + ", msg=" + msg.msgId
                     + ", fallback to normal send: unknown error");
    }
    try {
        sendMessageBackAsNormalMessage(msg);
        return true;
    } catch (const std::exception& e) {
        logger_error("Failed to send message back as normal message, consumerGroup="
                     + consumerGroup_ + ", msg=" + msg.msgId + ": " + e.what());
    } catch (...) {
        logger_error("Failed to send message back as normal message, consumerGroup="
                     + consumerGroup_ + ", msg=" + msg.msgId + ": unknown error");
    }
    return false;
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
