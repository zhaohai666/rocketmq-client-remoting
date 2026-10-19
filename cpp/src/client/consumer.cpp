// Push-consumer implementation.
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
    // Default allocation strategy AVG (the constructor argument may override it; equivalent to the setter here).
    allocateStrategy_ = std::make_shared<AllocateMessageQueueAveragely>();
}

DefaultMQPushConsumer::~DefaultMQPushConsumer() {
    try {
        shutdown();
    } catch (...) {
        // The destructor must not throw
    }
}

// ---------------------------------------------------------------- Configuration
void DefaultMQPushConsumer::setNamesrvAddr(const std::string& addr) {
    nameServerAddrs_ = splitSemicolon(addr);
}

void DefaultMQPushConsumer::setNameServerAddresses(const std::vector<std::string>& addrs) {
    nameServerAddrs_ = addrs;
}

void DefaultMQPushConsumer::setAllocateMessageQueueStrategy(
    std::shared_ptr<AllocateMessageQueueStrategy> strategy) {
    // The setter does not validate: a null strategy is rejected by checkConfig in start().
    allocateStrategy_ = std::move(strategy);
}

std::shared_ptr<AllocateMessageQueueStrategy> DefaultMQPushConsumer::allocateMessageQueueStrategy() const {
    return allocateStrategy_;
}

void DefaultMQPushConsumer::setConsumeThreadNums(int32_t n) {
    // Convenience method: sets min and max together. The thread count is split into the
    // setConsumeThreadMin/Max switches; this method stays for existing call sites.
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

// ---------------------------------------------------------------- Consume-thread elasticity

bool DefaultMQPushConsumer::updateCorePoolSize(int32_t corePoolSize) {
    // Guards fixed one by one:
    //   ownsConsumeExecutor && corePoolSize > 0
    //       && corePoolSize <= Short.MAX_VALUE   (32767)
    //       && corePoolSize < consumeThreadMax
    // Any failing guard is silently ignored (no exception thrown).
    // This implementation takes no externally injected executor, so ownsConsumeExecutor is always true.
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
    // Rule: accTotal = the MAX_OFFSET property minus this message's queueOffset, and msgAccCnt updates only when it is > 0.
    // Uses the last message of this batch; a missing / non-numeric / non-positive property never updates (the broker
    // always sends that property, so this tolerance only keeps dirty data from breaking the pull thread).
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
    // No-op by design: inc/decCorePoolSize are empty implementations, so reaching the backlog threshold does not really grow the pool.
    // The threshold comparison and log stay only to make msgAccCnt / the threshold config observable; do not "fix" this.
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

// ---------------------------------------------------------------- Subscription
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

// Immediate action after the subscription table gains or updates an entry (same tail as every subscribe).
//
// All three subscribe paths issue a heartbeat right after putting into the subscription
// table - subscribing stays legal after start(), and the put is followed
// immediately by one synchronous heartbeat round. Why it must be
// immediate: the broker's consumer manager records topic -> group into its own topicGroupTable
// only when a heartbeat arrives, and QUERY_TOPIC_CONSUME_BY_WHO(300)
// reads exactly that table; one round late means a gap of the default 30s.
//
// Also registers the new topic as in use so the background route refresh task covers it (the task walks
// the consumer's current subscription table to collect the topics to refresh).
// Before start there is no client instance, so only the subscription table is written.
// unsubscribe does not call this function: it just deletes the entry and sends no heartbeat.
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

// ---------------------------------------------------------------- Config numeric gates
// The numeric part of checkConfig.
//
// Why it runs in start() and not in each setter: setters are bare assignments, only checkConfig
// gates the values at startup. Clamping inside a setter (max(1,n) in setConsumeMessageBatchMaxSize)
// is the runtime counterpart of the max guard, and the two stages have separate jobs: the setter
// ensures "even a 0 cannot divide by zero or spin idle", the gate ensures "whoever writes 0 gets
// an error on the spot".
//
// Error texts carry no trailing FAQ-suggestion suffix
// (the convention for every client error in this repository).
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
    // pullThresholdForTopic: -1 is the "unset, use the queue-level threshold" sentinel, so it is legal outside the range
    if (pullThresholdForTopic_ != -1
        && (pullThresholdForTopic_ < 1 || pullThresholdForTopic_ > 6553500)) {
        throw MQClientException("pullThresholdForTopic Out of range [1, 6553500]");
    }
    // pullThresholdSizeForQueue
    if (pullThresholdSizeForQueue_ < 1 || pullThresholdSizeForQueue_ > 1024) {
        throw MQClientException("pullThresholdSizeForQueue Out of range [1, 1024]");
    }
    // pullThresholdSizeForTopic: here too only -1 is a sentinel
    if (pullThresholdSizeForTopic_ != -1
        && (pullThresholdSizeForTopic_ < 1 || pullThresholdSizeForTopic_ > 102400)) {
        throw MQClientException("pullThresholdSizeForTopic Out of range [1, 102400]");
    }
    // pullInterval (lower bound 0, unlike the other gates)
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
    // popBatchNums (the test is exactly <= 0, not < 1)
    if (popBatchNums_ <= 0 || popBatchNums_ > 32) {
        throw MQClientException("popBatchNums Out of range [1, 32]");
    }
}

// ---------------------------------------------------------------- Lifecycle
void DefaultMQPushConsumer::start() {
    {
        std::lock_guard<std::mutex> lk(lock_);
        if (started_.load()) {
            return;
        }
        // Wrap the consumer group with the namespace (ns%group), so
        // every group name facing the broker afterwards (heartbeat / rebalance / offset / lock / redelivery) uses the wrapped value.
        if (!namespace_.empty()) {
            consumerGroup_ = NamespaceUtil::wrapNamespace(namespace_, consumerGroup_);
        }
        // checkConfig step one: Validators::checkGroup
        // (blank / 120 length / character set), then block DEFAULT_CONSUMER - a shared default group would make
        // the broker mix the two groups' subscription relation, scrambling redelivery and rebalance.
        // checkConfig runs ahead of the address/subscription checks:
        // with illegal config nothing touches the network, and the real cause must not be masked by the later "not subscribed" error.
        Validators::checkGroup(consumerGroup_);
        if (consumerGroup_ == MixAll::DEFAULT_CONSUMER_GROUP) {
            throw MQClientException(
                "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");
        }
        // Either static addresses or dynamic resolution (ROCKETMQ_NAMESRV_DOMAIN) must be available
        if (nameServerAddrs_.empty() && !DefaultTopAddressing::isConfigured()) {
            throw MQClientException("name server address is not set");
        }
        // A null strategy refuses start outright
        // instead of letting rebalance dereference a null pointer (UB).
        if (allocateStrategy_ == nullptr) {
            throw MQClientException("allocateMessageQueueStrategy is null");
        }
        if (subscriptionData_.empty()) {
            throw MQClientException("subscription is not set, call subscribe() first");
        }
        if (messageListener_ == nullptr) {
            throw MQClientException("message listener is not set");
        }
        // The numeric section of checkConfig: must sit after all null checks and
        // before creating the MQClientInstance - throwing once background threads run would leak them.
        checkConfigRanges();
        // Only CLUSTERING
        // changes the instance name to the PID (BROADCASTING keeps "DEFAULT", so the client manager
        // lets broadcast consumers in the same process share one instance); buildMQClientId then
        // assembles `<local IP>@<instanceName>`.
        if (messageModel_ == MessageModel::CLUSTERING) {
            instanceName_ = changeInstanceNameToPID(instanceName_);
        }
        if (clientId_.empty()) {
            clientId_ = buildClientId(instanceName_, unitName_, enableStreamRequestType_);
        }
        // Clustering auto-subscribes the retry topic (copySubscription -> getRetryTopic):
        // the broker writes redelivered messages to %RETRY%group, and a client that does not subscribe never receives them
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
        // Broker VIP channel: forwarded to the instance that owns the broker RPCs (off = plain address).
        mqClient_->setVipChannelEnabled(vipChannelEnabled_);
        // TLS strict verification option (non-empty caCert = strict verification): forwarded to the instance and the underlying transport
        if (tlsOptionsSet_) {
            mqClient_->setTlsOptions(tlsOptions_);
        }
        if (socksProxySet_) {
            mqClient_->setSocksProxy(socksProxy_);
        }
        // Request hooks (namespaceV2's `nsd`/`ns`, ACL signing, the stream `ReqT`): attached
        // before start(), so the instance's very first frame already carries them. namespaceV2 is passed as a getter
        // (each request reads the config live, so changing the value after the hook is chained still takes effect).
        std::shared_ptr<RPCHook> requestHook =
            composeRequestHooks(enableStreamRequestType_, rpcHook_,
                                [this] { return namespaceV2_; });
        if (requestHook && !mqClient_->registerRPCHook(requestHook)) {
            logger_warn("consumer rpc hook ignored: MQClientInstance already has one (clientId="
                        + clientId_ + ")");
        }
        mqClient_->start();
        // Dynamic name server: the instance may already have addresses from the address server at start, so backfill them here
        // (otherwise this consumer never sees the address list actually in effect)
        if (nameServerAddrs_.empty() && !mqClient_->nameServerAddrs().empty()) {
            nameServerAddrs_ = mqClient_->nameServerAddrs();
        }
        // The POP consume executor must exist before rebalance (which immediately starts the per-queue POP loop),
        // otherwise the loop pops messages with nowhere to deliver them (the executor must be ready while the service is being built).
        if (popMode_) {
            popConsumeExecutor_ = std::make_shared<ConsumeExecutor>(
                std::max(1, corePoolSize_), std::max(1, consumeThreadMax_),
                /*keepAliveSeconds=*/60.0, "rmq-popconsume-" + consumerGroup_);
        }
        startMillis_ = UtilAll::currentTimeMillis();
        stop_.store(false);
        started_.store(true);
    }

    // Broker push notification 40 (membership changed -> recompute immediately) does NOT register a handler here:
    // the remoting table holds one handler per code, so consumers registering their own would overwrite each other.
    // Instead each consumer registers a "wake up" callback with the instance, which fans it out on receipt.
    // The callback runs on the remoting read thread and only sets a flag + notifies; it issues no RPC.
    mqClient_->registerRebalanceWakeup(consumerGroup_, [this] { this->wakeRebalanceLoop(); });

    // GET_CONSUMER_RUNNING_INFO(307):
    // admin / broker queries this consumer's running info; replies with a ConsumerRunningInfo JSON body.
    mqClient_->remotingClient().registerProcessor(
        RequestCode::GET_CONSUMER_RUNNING_INFO,
        [this](const RemotingCommand& cmd, const std::string&) -> std::optional<RemotingCommand> {
            auto it = cmd.extFields.find("consumerGroup");
            const std::string group = it == cmd.extFields.end() ? std::string() : it->second;
            if (group != consumerGroup_) {
                // Group mismatch answers SYSTEM_ERROR (the broker logs a warn; the error text is part of the matching contract)
                return RemotingCommand::createResponseCommand(
                    ResponseCode::SYSTEM_ERROR,
                    "consumerGroup not matched, expect " + consumerGroup_ + ", got " + group);
            }
            RemotingCommand resp = RemotingCommand::createResponseCommand(
                ResponseCode::SUCCESS, std::string());
            resp.body = consumerRunningInfo().encode();
            return resp;
        });

    // RESET_CONSUMER_CLIENT_OFFSET(220): the broker sends it with invokeOneway, so no reply is needed.
    // The reset logic triggers rebalance (invokeSync calls such as lock/unlock/batch), which must not run
    // synchronously on the read thread (self-deadlock, see the engineering notes at the top of this file) - handed to a background thread, returns nullopt at once.
    mqClient_->remotingClient().registerProcessor(
        RequestCode::RESET_CONSUMER_CLIENT_OFFSET,
        [this](const RemotingCommand& cmd, const std::string&) -> std::optional<RemotingCommand> {
            auto git = cmd.extFields.find("group");
            const std::string group = git == cmd.extFields.end() ? std::string() : git->second;
            if (group != consumerGroup_) return std::nullopt;  // oneway, no response
            std::string topic;
            auto tit = cmd.extFields.find("topic");
            if (tit != cmd.extFields.end()) topic = tit->second;
            std::map<MessageQueue, int64_t> table = parseResetOffsetBody(cmd.body);
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

    // GET_CONSUMER_STATUS_FROM_CLIENT(221): admin queries this consumer's consumed-offset table,
    // replies with a GetConsumerStatusBody JSON body (messageQueueTable with inline object keys).
    mqClient_->remotingClient().registerProcessor(
        RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT,
        [this](const RemotingCommand& cmd, const std::string&) -> std::optional<RemotingCommand> {
            auto git = cmd.extFields.find("group");
            const std::string group = git == cmd.extFields.end() ? std::string() : git->second;
            if (group != consumerGroup_) {
                // Group mismatch answers SYSTEM_ERROR (the broker logs a warn; the error text is part of the matching contract)
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

    // CONSUME_MESSAGE_DIRECTLY(309): the broker pushes one message down and asks the client to really consume it once.
    // The listener runs synchronously on the read thread (a one-shot admin probe; a synchronous request issued from
    // inside the listener would self-deadlock, but that is the user code's responsibility).
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

    // The fixed order of start:
    // fetch routes (register topics in use + fill the broker address table) -> heartbeat (the broker learns this consumer first)
    // -> rebalance immediately -> start the consume threads. The heartbeat must come before rebalance: rebalance asks the broker
    // for the consumer id list (GET_CONSUMER_LIST_BY_GROUP), and the broker only records this clientId after receiving a heartbeat.
    for (const std::string& t : subscribedTopics()) {
        mqClient_->registerTopicInUse(t);
        try {
            mqClient_->getTopicPublishInfo(t);
        } catch (const std::exception& e) {
            logger_debug("refresh route for " + t + " failed: " + e.what());
        }
    }
    // Between the routes arriving and the heartbeat,
    // non-TAG subscriptions are validated by the broker (CHECK_CLIENT_CONFIG 46). A malformed SQL92 expression makes the broker's
    // filter layer pass every message silently; only this request turns that into a startup error, so roll back locally and rethrow on failure.
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
    // The first allocation must complete synchronously: otherwise pull threads spin on an empty assignment set until the first rebalance round takes effect.
    try {
        doRebalance();
    } catch (const std::exception& e) {
        logger_debug("initial rebalance failed: " + std::string(e.what()));
    }

    // Pulling: one thread per queue (concurrent long polling, so a suspended empty queue does not block delivery on other queues)
    // These four resident loops follow the same rule as the pull threads: exceptions must be caught at the boundary. One
    // escaping from a thread means std::terminate (the whole process dies, not merely this consumer doing less work); shutdown
    // sets started_ to false first and joins the threads afterwards, so any client() call in that gap throws "consumer not started".
    dispatchThread_ = std::thread([this]() {
        setThreadName("ConsumeMessageThread");
        runLoop("dispatch", [this] { dispatchLoop(); });
    });
    // The suspended-listener cleanup scheduler exists only on the classic concurrent path: it is built together
    // with the concurrent consume service at construction/start; orderly consumption's cleanExpiredMsg already
    // returns directly, and the POP concurrent consume service has no matching scheduler either.
    if (!isOrderly() && !popMode_) {
        cleanExpireThread_ = std::thread([this]() {
            setThreadName("CleanExpireMsgScheduledThread_" + consumerGroup_);
            runLoop("cleanExpiredMsg", [this] { cleanExpiredMsgLoop(); });
        });
    }
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

    // Message trace: with enableMsgTrace=true an AsyncTraceDispatcher is created and ConsumeMessageTraceHook registered.
    // Placed after the consume threads start: the dispatcher's internal producer must first bring up the trace topic's route.
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
    // POP: mark every queue dropped so in-flight batches are no longer acked (the broker revives and redelivers them)
    if (popMode_) {
        std::lock_guard<std::mutex> lk(lock_);
        for (auto& kv : popQueues_) {
            kv.second->setDropped(true);
        }
        popQueues_.clear();
    }
    // Retire the POP consume executor (graceful shutdown: accept no new tasks, finish the batches in hand).
    // An explicit join is mandatory: worker threads captured this, so letting them keep running is a use-after-free.
    if (popConsumeExecutor_) {
        popConsumeExecutor_->shutdown(true);
        popConsumeExecutor_.reset();
    }
    // Persist consumed offsets once before exiting (the persistAllConsumerOffset tail of shutdown)
    try {
        persistOffsetsOnce();
    } catch (const std::exception& e) {
        logger_debug(std::string("persist offsets on shutdown failed: ") + e.what());
    }
    // Unlock the queues when orderly consumption retires (shutdown -> unlockAll).
    // The POP orderly service uses the same unlockAll, but it only reads processQueueTable - an empty table in POP mode,
    // so it sends no frames.
    if (isOrderly() && !popMode_ && messageModel_ != MessageModel::BROADCASTING) {
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
    if (cleanExpireThread_.joinable()) cleanExpireThread_.join();
    // Close the trace dispatcher only after all consume threads stopped: first flush the traces still queued (this step is what
    // persists SubBefore/SubAfter), then close the internal producer. Must happen before mqClient_->shutdown().
    if (traceDispatcher_) {
        try {
            traceDispatcher_->shutdown();
        } catch (const std::exception& e) {
            logger_warn(std::string("trace dispatcher shutdown failed: ") + e.what());
        }
        traceDispatcher_.reset();
    }
    // Graceful unregister (unregisterClient): before closing any connection, send
    // UNREGISTER_CLIENT(35) to every broker in the broker address table so the broker drops this clientId at once
    // instead of waiting for the heartbeat timeout (~120s by default); otherwise consumer-change notifications could still be sent to the exited instance meanwhile.
    if (mqClient_) {
        // The wake-up callback captured this: consume threads are all joined, so remove the callback first; during what
        // remains (unregister client, close connections) a broker push of code 40 can no longer call into a half-dead thread.
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

// Boundary of the resident loop threads; both duties are indispensable:
//   1. Catch exceptions: one escaping a thread is std::terminate and the whole process dies, not just this
//      consumer doing less work. shutdown sets started_ to false as its first step, after which any client() call
//      throws "consumer not started", and it joins the threads only later - the gap in between is bound to be hit.
//   2. Re-enter the loop while still running: catching without re-running turns an occasional exception into a silent
//      stall ("heartbeats sent, offsets flushed, consumption just stops"), harder to diagnose than a crash; sleep 1s before re-entering and persistent failure leaves traces in the log.
void DefaultMQPushConsumer::runLoop(const std::string& what, const std::function<void()>& body) {
    while (started_.load() && !stop_.load()) {
        try {
            body();
            return;  // the loop exited on its own (it saw the stop flag), a normal finish
        } catch (const std::exception& e) {
            logger_warn(what + " loop threw: " + e.what());
        } catch (...) {
            logger_warn(what + " loop threw: unknown exception");
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

// ---------------------------------------------------------------- Consume loop
bool DefaultMQPushConsumer::isOrderly() const {
    return messageListener_ != nullptr && messageListener_->orderly();
}

void DefaultMQPushConsumer::rebalancePullThreads() {
    // Same contract as updateProcessQueueTableInRebalance:
    // sync the pull threads with the current assignment set and finish up the revoked queues (persist consumed offsets,
    // drop in-flight buffers, unlock queues in orderly clustering mode). Skipping any step lets the old instance keep
    // consuming the revoked queue's in-flight messages, duplicating the new owner (root cause of duplicate consumption across instances in one group).
    std::vector<MessageQueue> queues = assignedQueues();
    std::map<std::string, MessageQueue> current;
    for (const MessageQueue& mq : queues) {
        current[offsetKey(mq)] = mq;
    }
    std::vector<std::pair<MessageQueue, int64_t>> retired;
    {
        std::lock_guard<std::mutex> lk(lock_);
        // 1. Retire: queues allocated elsewhere (!mqSet.contains(mq)) plus pull-stalled ones
        //    (pq.isPullExpired()). The stalled branch guards against
        //    "the loop thread died or is stuck while the queue still belongs to this instance" - not retiring it means permanent silence with no exception at all.
        //    Collect the keys first, then retire them one by one: retireQueueLocked erases pullThreads_ entries, and
        //    erasing straight from the iterator would tangle with std::map erase semantics.
        std::vector<std::string> toRetire;
        for (auto it = pullThreads_.begin(); it != pullThreads_.end(); ++it) {
            const std::string& key = it->first;
            const bool revoked = current.find(key) == current.end();
            // During shutdown threads are exiting anyway, so do not test for stalls then (it would spam a pile of fake [BUG] logs)
            const bool stalled = !revoked && started_.load() && !stop_.load()
                && pullStalledLocked(key);
            if (!revoked && !stalled) {
                continue;
            }
            if (stalled) {
                // This warning keeps its established wording, used to diagnose "consumption stalled and self-healed" incidents
                logger_error("[BUG]doRebalance, " + consumerGroup_
                             + ", try remove unnecessary mq, " + key
                             + ", because pull is pause, so try to fixed it");
            }
            toRetire.push_back(key);
        }
        for (const std::string& key : toRetire) {
            MessageQueue fallback;
            auto cit = current.find(key);
            if (cit != current.end()) fallback = cit->second;   // a stalled queue may never have pulled anything
            retireQueueLocked(key, fallback, retired);
        }
    }
    // The retire tail (offset persistence / UNLOCK) must finish before starting new threads: otherwise the new loop starts
    // pulling from the old offset and writes a smaller one back, adding needless redelivery. Network and disk work happen outside the lock.
    if (!retired.empty()) {
        onQueuesRevoked(retired);
    }
    {
        std::lock_guard<std::mutex> lk(lock_);
        // 2. Build: start pull threads for the missing queues.
        //    std::thread runs on construction, so "register first, then start" is impossible:
        //    writing pullOwners_ must precede the construction, and writing pullThreads_ must complete in the same batch.
        //    Offsetting them by one step yields two bad outcomes:
        //      * the new thread reaches ownsQueue() first, sees itself absent from the table and exits on the spot; the key is
        //        then registered as "already has a pull thread", so no later rebalance round ever starts one again -
        //        this queue goes permanently silent and not one of its messages is delivered (root cause of a real run
        //        missing a whole batch with duplicate count = 0).
        //      * two interleaved rebalance rounds start two threads for the same key => duplicate consumption on one queue.
        //    The new thread's first round takes exactly this lock, so it waits until we release and can only observe the state
        //    after registration finished, which closes the "started but sees no ownership" window completely.
        for (const auto& kv : current) {
            if (pullThreads_.find(kv.first) != pullThreads_.end()) continue;
            // Once shutdown has begun only retire, never build: started_ is set false in shutdown's first step and
            // client() then always throws ("consumer not started"), so a thread started now dies on its very first line; worse,
            // shutdown's join loop may already have passed, nobody joins this new thread, and it can outlive this object's
            // destructor. Hence the isStopped check (stop_/started_) before creating threads, same rule.
            if (stop_.load() || !started_.load()) return;
            if (popMode_ && popQueues_.find(kv.first) == popQueues_.end()) {
                popQueues_[kv.first] = std::make_shared<PopProcessQueue>();
            }
            // A new ProcessQueue means unfreeze (removeOffset inside removeProcessQueue clears the frozen state): the
            // rebuilt queue resumes advancing from the corrected offset. Staying in the frozen set would leave the queue
            // pulling forever without acking - the offset sticks at the correction value and redelivery never moves it forward.
            frozenOffsets_.erase(kv.first);
            const uint64_t token = ++nextPullToken_;
            pullOwners_[kv.first] = token;
            // A queue enters mqMap_ as soon as it is allocated (the ProcessQueueTable key set is "allocated"), without waiting for
            // the first message: offset persistence, 307 running info and stall self-healing all rely on this mapping to find the queue.
            mqMap_[kv.first] = kv.second;
            // The thread was just created and has not reached the stamping point yet: placeholder with the current time so the next round does not misjudge a stall
            lastPullAt_[kv.first] = UtilAll::currentTimeMillis();
            const std::string key = kv.first;
            const MessageQueue mq = kv.second;
            try {
                std::thread t([this, mq, key, token]() {
                    // The thread body's boundary must catch its own exceptions: escaping here means std::terminate and the
                    // whole process dies (measured on a real cluster: when shutdown raced a rebalance round, the freshly started
                    // thread threw "consumer not started" from its first client() call and took down every test case halfway).
                    // Exceptions inside the consume loop are always converted into a stall report in place.
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
                    // Reaching here means the loop returned by itself. If it still holds the queue when returning, that is not a normal
                    // exit revoked by rebalance but a death like "subscription gone / exception punched through": report it as
                    // stalled so the next round retires and rebuilds it (a dead std::thread cannot be asked is_alive).
                    // Harmless on the shutdown path: by then there is no next rebalance round.
                    markPullLoopExited(key, token);
                });
                pullThreads_[key] = std::move(t);
            } catch (const std::system_error& e) {
                // Failing to start a thread (resource exhaustion) must reclaim the ownership: leaving a token nobody claimed is a
                // permanent silence sentence for this queue; reclaiming lets the next rebalance round retry it.
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
        // The POP queue's lastPopTimestamp is stamped at the same instant
        if (pq) pq->setLastPopTimestamp(now);
    }
}

bool DefaultMQPushConsumer::pullStalledLocked(const std::string& key) const {
    // isPullExpired (same test for ProcessQueue / PopProcessQueue): only "how long since the last
    // pull/pop was issued". Never stamped (a new thread that has not reached the entry) is not a stall, otherwise a queue would be revoked right after allocation.
    auto it = lastPullAt_.find(key);
    if (it == lastPullAt_.end()) return false;
    if (it->second == 0) return true;      // marker that the loop exited on its own
    return UtilAll::currentTimeMillis() - it->second > kPullMaxIdleTime;
}

void DefaultMQPushConsumer::markPullLoopExited(const std::string& key, uint64_t token) {
    if (stop_.load() || !started_.load()) return;
    std::lock_guard<std::mutex> lk(lock_);
    // Query the ownership table directly: ownsQueue() takes the same non-recursive lock, which is already held here.
    auto it = pullOwners_.find(key);
    if (it == pullOwners_.end() || it->second != token) return;   // normal exit after being revoked
    lastPullAt_[key] = 0;
}

bool DefaultMQPushConsumer::pullStalled(const std::string& key) const {
    std::lock_guard<std::mutex> lk(lock_);
    return pullStalledLocked(key);
}

bool DefaultMQPushConsumer::flowControlHit(const MessageQueue& mq, const std::string& key) {
    // The five flow-control thresholds of a ProcessQueue: first the three queue-level ones (message count, bytes,
    // offset span), then the two topic-level cumulative ones. The byte threshold is in MiB; the span is max-min of the
    // pending queueOffsets and counts only when strictly greater.
    // Other queues are walked only when topic-level thresholds are enabled, otherwise one check would scan the whole table needlessly.
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
    // Recompute the allocation periodically (20s by default), or immediately on a
    // NOTIFY_CONSUMER_IDS_CHANGED notification (rebalanceNow_ set). Within 60s after start and while nothing is assigned, retry every 2s:
    // the consumer may have been created before the topic, so the real route is not fetchable yet - the consumer does no default-topic fallback, and
    // waiting the full 20s would mean not consuming for a long time; the fast retry only applies during the first 60s after start, avoiding
    // hammering the name server when a non-existent topic is subscribed long-term.
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

void DefaultMQPushConsumer::resume() {
    // resume: run doRebalance right after clearing the pause,
    // so allocations missed while suspended are picked up immediately.
    paused_.store(false);
    doRebalance();
}

void DefaultMQPushConsumer::queuePullLoop(const MessageQueue& mq, uint64_t token) {
    // A loop started on the shutdown path (see the "retire only, never build" guard in rebalancePullThreads): finish at once and
    // do not touch client() - shutdown sets started_ false in its first step, so client() would certainly throw.
    if (!started_.load() || stop_.load()) return;
    MQClientInstance& c = client();
    const bool orderly = isOrderly();
    const std::string key = offsetKey(mq);
    while (!stop_.load() && started_.load()) {
        // Revoked by rebalance during long polling (the queue may have a new pull thread) means stale: exit this thread
        // outright and let the new owner take over from our last persisted offset, so two instances never consume the same message twice.
        if (!ownsQueue(key, token)) {
            return;
        }
        // Stamp the time at every pull issue, before the flow-control/lock checks:
        // the test is "this loop is still running", not "this round really hit the network".
        stampPullAt(key);
        SubscriptionData sub;
        {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = subscriptionData_.find(mq.topic);
            if (it == subscriptionData_.end()) return;
            sub = it->second;
        }
        // While paused this queue's pull is delayed by
        // PULL_TIME_DELAY_MILLS_WHEN_SUSPEND (1000ms) before retrying; already fetched messages keep
        // being consumed and the offset never rewinds. Checked before flow control / the orderly lock (pause wins).
        if (paused_.load()) {
            logger_debug("consumer was paused, execute pull request later. group=" +
                         consumerGroup_);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        }
        // Orderly consumption: queues the broker has not confirmed as locked (LOCK_BATCH_MQ) are not pulled
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
        // Flow control (the five ProcessQueue thresholds, see flowControlHit): any hit pauses pulling this queue
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
                offset = resolveInitialOffset(mq);
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
            // Whether the subscription bit is set
            // depends solely on subExpression being non-empty; when disabled by default the expression does not even enter the frame's
            // subscription field (see MQClientInstance::pullMessage).
            const bool postSub = shouldPostSubscriptionWhenPull(postSubscriptionWhenPull_,
                                                               sub.classFilterMode);
            const int32_t sysFlag =
                PullSysFlag::buildSysFlag(/*commitOffset=*/false, /*suspend=*/true,
                                          /*subscription=*/postSub, /*classFilter=*/false);
            const std::string expr = postSub
                                         ? (sub.subString.empty() ? std::string("*") : sub.subString)
                                         : std::string();
            int64_t brokerId;
            {
                std::lock_guard<std::mutex> lk(lock_);
                // recalculatePullFromWhichNode: no record means master=0
                auto it = pullFromWhichNode_.find(key);
                brokerId = (it != pullFromWhichNode_.end()) ? it->second : MixAll::MASTER_ID;
            }
            const int64_t pullBegan = UtilAll::currentTimeMillis();
            result = c.pullMessage(consumerGroup_, mq, offset, pullBatchSize_, sysFlag,
                                   /*commitOffset=*/0, expr, sub.subVersion, sub.expressionType,
                                   pullTimeoutMillis_, pullBatchSizeInBytes_,
                                   pullSuspendTimeoutMillis_, /*addr=*/std::string(),
                                   /*requestSource=*/0, /*brokerId=*/brokerId);
            {
                // processPullResult: every response writes back
                // pullFromWhichNodeTable (defaulting to master=0)
                std::lock_guard<std::mutex> lk(lock_);
                pullFromWhichNode_[key] =
                    result.suggestWhichBrokerId.value_or(MixAll::MASTER_ID);
            }
            // Consume statistics (PullCallback.onSuccess: RT always recorded, TPS only when messages came back)
            mqClient_->consumerStats().incPullRT(consumerGroup_, mq.topic,
                                                 UtilAll::currentTimeMillis() - pullBegan);
            if (result.isFound() && !result.msgFoundList.empty()) {
                mqClient_->consumerStats().incPullTPS(consumerGroup_, mq.topic,
                                                      static_cast<int64_t>(result.msgFoundList.size()));
            }
        } catch (const MQBrokerException& e) {
            // TOPIC_NOT_EXIST / PULL_NOT_FOUND and friends are mostly expected paths (topic not created yet), so debug + backoff
            logger_debug("pull broker error for " + mq.toString() + ": " + e.what());
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        } catch (const RemotingTimeoutException& e) {
            // Long polling timing out on the client when nothing arrives during suspend is normal: the broker clamps the
            // suspend time to its own brokerSuspendMaxTimeMillis and ignores the client value,
            // so idle queues time out periodically. Not an error: debug only, to keep the run log clean.
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
        // Enqueuing and "still owning this queue" must stay consistent: for a queue revoked during long polling the fetched batch
        // follows the ProcessQueue.isDropped() semantics - drop it, consume nothing, advance no offset; the new owner
        // redelivers from our last persisted offset, otherwise two instances consume the same message twice.
        if (!ownsQueue(key, token)) {
            logger_debug("queue " + mq.toString()
                         + " revoked during pull, discard fetched messages");
            return;
        }
        if (result.status == PullStatus::FOUND && !result.msgFoundList.empty()) {
            // Client-side filtering before delivery (the filter stage of processPullResult):
            // a second tag filter first, then the FilterMessageHook. This must run before taking lock_ -
            // hooks are user code and may block, so they cannot sit inside the enqueue critical section.
            // Messages removed on the pull path are not acked: the offset advances anyway, i.e. they are silently skipped.
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
            // ProcessQueue.msgAccCnt: computed from the filtered batch that got enqueued (filter first, then putMessage)
            updateMsgAccCntLocked(key, deliverable);
        }
        // The pull cursor advances to nextBeginOffset; the "consumed offset" is tracked and persisted by consumeOffsetTable_
        if (result.nextBeginOffset >= 0) {
            bool illegal = false;
            {
                std::lock_guard<std::mutex> lk(lock_);
                offsetTable_[key] = result.nextBeginOffset;
                if (result.status == PullStatus::OFFSET_ILLEGAL) {
                    // The broker corrected the offset;
                    // the whole chain of correction + queue revoke + persistence is described in handleOffsetIllegal.
                    // Freezing the offset must land inside the lock; the remaining correction steps (network RPCs) run outside it.
                    illegal = true;
                } else {
                    // On an empty response (NO_NEW_MSG / NO_MATCHED_MSG) correct the
                    // "consumed offset", see correctTagsOffsetLocked
                    correctTagsOffsetLocked(key, result.status, result.nextBeginOffset);
                }
            }
            if (illegal) {
                handleOffsetIllegal(key, result.nextBeginOffset);
            }
        }
    }
}

// ---------------------------------------------------------------- POP consume loop
void DefaultMQPushConsumer::queuePopLoop(const MessageQueue& mq, uint64_t token) {
    // Key differences from the pull loop:
    //   - it neither queries nor commits a consume offset: progress is tracked by the broker-side checkpoint and confirmed only by ack;
    //   - popped messages go straight to the consume threads and the loop continues at once (no waiting for consume results);
    //   - POLLING_NOT_FOUND (queue momentarily empty) is a normal state: just take the next round, it is not an error.
    const std::string key = offsetKey(mq);
    int64_t invisible = popInvisibleTime_;
    if (invisible < kMinPopInvisibleTime || invisible > kMaxPopInvisibleTime) {
        // Clamp rule: anything outside [5s, 300s] falls back to 60s
        invisible = 60000;
    }
    // PopRequest defaults to ConsumeInitMode.MAX; map it through consumeFromWhere here, so
    // "consume from the beginning" keeps working in POP mode as well.
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
        // Stamp as soon as the request is issued (in POP mode
        // isPullExpired reads exactly PopProcessQueue.lastPopTimestamp).
        stampPullAt(key);
        SubscriptionData sub;
        {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = subscriptionData_.find(mq.topic);
            if (it == subscriptionData_.end()) return;
            sub = it->second;
        }
        // While paused this queue's pop is delayed by
        // PULL_TIME_DELAY_MILLS_WHEN_SUSPEND (1000ms) before retrying (same as the pull loop).
        if (paused_.load()) {
            logger_debug("consumer was paused, execute pop request later. group=" +
                         consumerGroup_);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        }
        // Flow control: too many popped-but-unacked messages, so hold back first (popThresholdForQueue)
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
            // No message arrived while long polling was suspended -> the client times out first: normal behaviour, go to the next round
            logger_debug("pop long-poll timeout for " + key + " (benign): " + e.what());
            continue;
        } catch (const std::exception& e) {
            logger_debug(std::string("pop error for ") + key + ": " + e.what());
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }
        // The queue was revoked by rebalance after popping: this batch is neither consumed nor acked
        // (the PopProcessQueue.isDropped() branch); when invisibleTime expires the
        // broker revives the messages and redelivers them to the new owner.
        if (!ownsQueue(key, token) || pq->isDropped()) {
            logger_debug("queue " + key + " revoked during pop, discard "
                         + std::to_string(result.msgFoundList.size()) + " messages un-acked");
            return;
        }
        // Pull statistics after popping (PopCallback.onSuccess; the test is in recordPopPullStats)
        recordPopPullStats(client().consumerStats(), mq.topic, result, began);
        if (result.status == PopStatus::FOUND && !result.msgFoundList.empty()) {
            pq->incFoundMsg(static_cast<int32_t>(result.msgFoundList.size()));
            // Filtering before delivery (processPopResult): the POP path must ack the dropped entries,
            // otherwise the broker revives and redelivers them when invisibleTime expires - which looks like "the filter did not take effect".
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
            // Empty result: if the broker did not suspend for pollTime (returned at once) this would spin hot,
            // so fall back to a backoff when the round was too short, keeping the broker from being flooded.
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        // NO_NEW_MSG / POLLING_NOT_FOUND / POLLING_FULL all go straight into the next round
    }
}

void DefaultMQPushConsumer::recordPopPullStats(ConsumerStatsManager& stats,
                                               const std::string& topic,
                                               const PopResult& result, int64_t beganMs) {
    // The pull callback records RT every time, the POP callback only on FOUND, and it does so
    // before the emptiness check; TPS counts only the messages actually popped. This asymmetry is kept on purpose: an empty-handed POP is
    // the long-polling norm (POLLING_NOT_FOUND), and folding it into RT would dilute the average pull time with suspend durations.
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
    // The consume service is chosen by listener type: an orderly listener plus
    // POP goes to the orderly POP service, and that chain is an unfinished skeleton (POPTODO): after the deduplicated
    // request is queued, run() returns as soon as it holds the queue lock - messages are not consumed and not acked,
    // the broker revives and redelivers them when invisibleTime expires, so the macro behaviour is
    // "orderly + POP receives no messages and the backlog never drains". Keep the skeleton behaviour, do not "fix" it: the orderly
    // semantics of the POP path (queue locks, COMMIT/ROLLBACK) are undecided. msgs is ignored on this path (ConsumeRequest carries only pq + mq);
    // the pre-filter of the popped batch and the acking of dropped entries happen before dispatch, shared by both services and unaffected.
    if (isOrderly()) {
        submitPopOrderlyRequest(pq, mq, /*force=*/false);
        return;
    }
    // Submitted to the core/max two-tier pool (core=consumeThreadMin / max=consumeThreadMax, unbounded queue):
    // previously each batch spawned a detached thread with no cap, so one slow listener blew up the thread count and
    // the value set by setConsumeThreadNums() had no effect at all. A pool plus an unbounded queue
    // means real concurrency == corePoolSize, which updateCorePoolSize() can change at runtime.
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
            // Not started (unit tests) means no executor: run synchronously to keep the testable behaviour from before the change.
            consumePopBatch(std::move(*batch), pq, mq);
        }
    }
}

void DefaultMQPushConsumer::submitPopOrderlyRequest(const std::shared_ptr<PopProcessQueue>& pq,
                                                    const MessageQueue& mq, bool force) {
    // Same contract as submitConsumeRequest:
    // dedupe under the (mq, shardingKeyIndex=0) lock (MessageQueueLock + ConcurrentSet,
    // equality = (shardingKeyIndex, pq reference, mq)) and only new requests go to the executor. When rebalance revokes a queue
    // it swaps in a new PopProcessQueue (different pointer identity), so the new request queues normally.
    // The per-key lock is folded into lock_ here: the critical section is a single set insert, equivalent and one less lock table.
    {
        std::lock_guard<std::mutex> lk(lock_);
        const bool isNew = popOrderlyRequests_.insert({pq.get(), mq}).second;
        if (!force && !isNew) return;
    }
    std::shared_ptr<ConsumeExecutor> exec = popConsumeExecutor_;
    try {
        if (exec) {
            exec->submit([this, pq, mq]() { runPopOrderlyRequest(pq, mq); });
        } else {
            // Not started (unit tests) means no executor: run synchronously, matching the concurrent path's testable behaviour.
            runPopOrderlyRequest(pq, mq);
        }
    } catch (const std::exception& e) {
        // A failed submit only logs; the request stays in the set (it is not re-offered automatically).
        logger_error("error submit pop orderly request: " + std::string(e.what())
                     + ", mq: " + mq.toString());
    }
}

void DefaultMQPushConsumer::runPopOrderlyRequest(const std::shared_ptr<PopProcessQueue>& pq,
                                                 const MessageQueue& mq) {
    // The skeleton of ConsumeRequest.run ends here: the request is removed only when pq was revoked (same log as removeConsumeRequest);
    // otherwise it takes the queue lock and does nothing. The counterpart of fetchLockObject is the caller's dedupe set
    // (at most one request running per queue), so no extra lock is needed.
    if (pq->isDropped()) {
        logger_warn("run, message queue not be able to consume, because it's dropped. "
                    + mq.toString());
        std::lock_guard<std::mutex> lk(lock_);
        popOrderlyRequests_.erase({pq.get(), mq});
        return;
    }
}

size_t DefaultMQPushConsumer::popOrderlyRequestCount() const {
    std::lock_guard<std::mutex> lk(lock_);
    return popOrderlyRequests_.size();
}

void DefaultMQPushConsumer::consumePopBatch(std::vector<MessageExt> msgs,
                                            std::shared_ptr<PopProcessQueue> pq,
                                            const MessageQueue& mq) {
    // ConsumeRequest.run for POP concurrent consumption.
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
        // Already past invisibleTime: an ack would not be accepted either, so abandon this batch (wait for the broker to revive and redeliver)
        pq->decFoundMsg(-static_cast<int32_t>(msgs.size()));
        return;
    }

    resetRetryTopicAndNamespace(msgs);
    ConsumeConcurrentlyContext ctx(mq);
    // The POP path uses ackIndex as
    // "up to which message has been acked", the same semantics as the classic redelivery path; the default Integer.MAX_VALUE
    // already means "ack the whole batch", and clamping to size-1 here only aligns the ctx value with this batch size
    // without changing CONSUME_SUCCESS behaviour (not acking would let the messages be revived and redelivered after invisibleTime).
    ctx.ackIndex = static_cast<int32_t>(msgs.size()) - 1;
    // Consume hooks: before runs ahead of the listener, after runs right behind it
    // (fixed order:
    //  the after hook runs before the "queue revoked / pop timed out" checks)
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
        // The POP concurrent path also stamps CONSUME_START_TIME on every message before handing them
        // to the listener (POP has no cleanExpiredMsg sweep, but this property is visible to the
        // listener and to tracing, matching the classic concurrent contract).
        const int64_t popStampNow = UtilAll::currentTimeMillis();
        for (MessageExt& m : msgs) {
            m.putProperty(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP,
                          std::to_string(popStampNow));
        }
        auto* conc = static_cast<MessageListenerConcurrently*>(messageListener_.get());
        status = conc->consumeMessage(msgs, ctx);
    } catch (const std::exception& e) {
        // A consume exception is treated as RECONSUME_LATER
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
        // The queue was revoked or the pop timed out while consuming: the result is no longer processed
        pq->decFoundMsg(-static_cast<int32_t>(msgs.size()));
        return;
    }
    processPopConsumeResult(status, ctx, msgs, pq);
}

bool DefaultMQPushConsumer::isPopTimeout(int64_t popTime, int64_t invisible) {
    // isPopTimeout contract: when popTime/invisibleTime cannot be parsed, treat it as timed out
    if (popTime <= 0 || invisible <= 0) return true;
    return UtilAll::currentTimeMillis() - popTime >= invisible;
}

void DefaultMQPushConsumer::processPopConsumeResult(
    ConsumeConcurrentlyStatus status, const ConsumeConcurrentlyContext& ctx,
    std::vector<MessageExt>& msgs, const std::shared_ptr<PopProcessQueue>& pq) {
    // processConsumeResult (POP concurrent).
    int32_t ackIndex = ctx.ackIndex;
    if (status == ConsumeConcurrentlyStatus::CONSUME_SUCCESS) {
        if (ackIndex >= static_cast<int32_t>(msgs.size())) {
            ackIndex = static_cast<int32_t>(msgs.size()) - 1;
        }
    } else {
        ackIndex = -1;  // RECONSUME_LATER: ack not one message
    }
    for (int32_t i = 0; i <= ackIndex; i++) {
        ackPopMsg(msgs[static_cast<size_t>(i)]);
        pq->ack();
    }
    for (int32_t i = ackIndex + 1; i < static_cast<int32_t>(msgs.size()); i++) {
        pq->ack();
        const MessageExt& msg = msgs[static_cast<size_t>(i)];
        // Past the max reconsume count: hand it to checkNeedAckOrDelay (too old means ack and drop,
        // otherwise pick a delay level from how long the message has already lived)
        if (maxReconsumeTimes_ >= 0 && msg.reconsumeTimes >= maxReconsumeTimes_) {
            checkNeedAckOrDelay(msg);
            continue;
        }
        changePopInvisibleTime(msg, ctx.delayLevelWhenNextConsume);
    }
}

void DefaultMQPushConsumer::checkNeedAckOrDelay(const MessageExt& msg) {
    // checkNeedAckOrDelay: the fallback once the retry count is exhausted.
    // If the message lived longer than twice the largest delay level -> ack and drop (no endless retries);
    // otherwise pick a level by its age and keep extending the invisible time.
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
    // Deliberate tolerance: when the age is below the first level the original formula yields level=-1 and indexing
    // delayLevelTable[-1] would throw ArrayIndexOutOfBounds, so the value is clamped to the first level.
    changePopInvisibleTime(msg, level);
}

std::optional<PopCkTarget> DefaultMQPushConsumer::popCkTarget(const MessageExt& msg) {
    // Two things must not be assumed:
    //   1. the topic has to be restored with ExtraInfoUtil.getRealTopic from the CK retryFlag - a revived message
    //      (retryFlag=1) really belongs to %RETRY%<group>_<topic>, NOT the topic on the message;
    //   2. the address must be looked up from the CK brokerName, not from the topic's route - a retry topic usually has
    //      no route entry of its own, so looking it up by topic fails (findBrokerAddressInSubscribe is the way).
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
    // Same contract as ackAsync.
    auto target = popCkTarget(msg);
    if (!target) return;
    try {
        // Uses findBrokerAddressInSubscribe(brokerName, MASTER_ID, true):
        // master only; if one route refresh still yields nothing it throws "The broker[X] not exist".
        MQClientInstance& c = client();
        c.ackMessage(consumerGroup_, target->topic, target->queueId, target->extraInfo,
                     target->offset, 3000, target->brokerName,
                     c.publishAddrFor(target->brokerName, target->topic));
    } catch (const std::exception& e) {
        // An ack failure is not fatal: the broker revives and redelivers the message once invisibleTime expires
        logger_debug(std::string("ack failed for ") + msg.msgId + ": " + e.what());
    }
}

void DefaultMQPushConsumer::changePopInvisibleTime(const MessageExt& msg, int32_t delayLevel) {
    // Same contract as changePopInvisibleTime.
    // With delayLevel == 0 the message's reconsume count is used as the level; the level table is in seconds, the API takes milliseconds.
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
        // changePopInvisibleTimeAsync likewise is "master only" plus one route refresh
        MQClientInstance& c = client();
        c.changeInvisibleTime(consumerGroup_, target->topic, target->queueId,
                              target->extraInfo, target->offset,
                              static_cast<int64_t>(delaySecond) * 1000, 3000,
                              target->brokerName,
                              c.publishAddrFor(target->brokerName, target->topic));
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
            // Take the batch together with the ProcessQueue epoch it belongs to, and compare before consuming/acking:
            // a mismatch invalidates the whole batch (the isDropped test, same contract as ConsumeMessageConcurrentlyService).
            uint64_t epoch = 0;
            std::vector<MessageExt> batch = takeBatchForConsume(key, &epoch);
            if (batch.empty()) continue;
            try {
                bool done = consumeBatch(key, mq, batch, epoch);
                progressed = progressed || done;
            } catch (const std::exception& e) {
                // Unexpected exception on the dispatch path: push the batch back to the head and retry later (do not let it kill the dispatch thread)
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
            finishBatchConsume(key);
        }
        if (!progressed) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
}

// ---------------------------------------------------------------- Consume hooks / trace
//
// Call sites of ConsumeMessageHook (the concurrent / orderly / POP concurrent consume paths):
//   1) build the context and run the before hook before calling the listener (produces the SubBefore trace);
//   2) after the listener returns, compute the ConsumeReturnType name and write it into props (it decides the SubAfter
//      contextCode), then run the after hook (produces the SubAfter trace).
// Hook exceptions are always swallowed and logged as warn - broken tracing must never affect consumption.
ConsumeMessageContext DefaultMQPushConsumer::buildConsumeHookContext(
    const std::vector<MessageExt>& msgs, const MessageQueue& mq) const {
    ConsumeMessageContext context(consumerGroup_, msgs, mq);
    context.success = false;            // the initial value is already false
    context.props.clear();              // props carries the ConsumeReturnType name
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
    // Hook exceptions are always swallowed and logged as error (same contract as executeHook / processPopResult).
    // Unlike CheckForbiddenHook: a failing filter hook must not affect consumption.
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
    // Pre-delivery filtering: (1) the client-side second tag filter, (2) FilterMessageHook (shared by the pull and POP paths).
    //
    // (1) Contract: the second tag filter of processPullResult / processPopResult -
    //    the broker filters by the tag hash (codeSet), so hash collisions can let wrong messages through and the client has to
    //    verify against the tag string again. The `!tagsSet.isEmpty()` guard means no filtering when subscribed to "*"
    //    (SUB_ALL), which is why FilterAPI::buildSubscriptionData must leave the tag set empty for SUB_ALL.
    //
    // (2) The hook receives a mutable msgList, and the caller decides what to do with the removed messages:
    //    the pull path skips them silently (the offset advances as usual, no ack); the POP path acks them immediately.
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
        // filterMessageContext.setUnitMode(consumer.isUnitMode()) - the hook uses this to tell whether the traffic is unitised
        context.unitMode = unitMode_;
        executeFilterMessageHook(context);
        out = context.msgList;
    }
    return out;
}

std::vector<MessageExt> DefaultMQPushConsumer::droppedMessages(
    const std::vector<MessageExt>& original, const std::vector<MessageExt>& kept) {
    // The original contract used List.contains(Object) (Object.equals = reference identity), and msgListFilterAgain
    // shared element references with the original msgFoundList, so the difference was exact. After a vector copy there is no
    // identity left, so the difference is taken by msgId instead - msgIds within one POP result are guaranteed distinct
    // (they derive from storeHost + commitLogOffset), which is equivalent and immune to false matches on value equality.
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
    // The decision order is a fixed contract (exception -> timeout -> failed -> succeeded) and must not be reordered
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
        // Same contract as processConsumeResult: ok = ackIndex + 1, and on a partial ack the
        // prefix counts as OK while the tail counts as FAILED (treating the whole batch as successful understates failures and hides how many messages need redelivery)
        const int64_t ok = ackCount.has_value() ? *ackCount : msgCount;
        stats.incConsumeOKTPS(consumerGroup_, topic, ok);
        if (msgCount > ok) stats.incConsumeFailedTPS(consumerGroup_, topic, msgCount - ok);
    }
    stats.incConsumeRT(consumerGroup_, topic, rt);
}

ConsumerRunningInfo DefaultMQPushConsumer::consumerRunningInfo() {
    // consumerRunningInfo: the response body of request code 307.
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
            // Every ProcessQueueInfo field (protocol JSON; the key spelling "droped" is what the broker-side parser matches)
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
            // In POP mode messages land in popQueues_ (popProcessQueueTable) and the classic
            // processQueueTable stays empty - the two tables are mutually exclusive, so they must be in 307 as well: reporting the same
            // queue in both mqTable and mqPopTable makes the console count one consume stream as two. mqMap_ is the "allocated"
            // registry (self-healing and offset persistence rely on it) and is written in both modes, so filter by mode here rather than skipping the write.
            if (popMode_ && popQueues_.find(kv.first) != popQueues_.end()) continue;
            const MessageQueue& mq = kv.second;
            // The map key is an inline JSON object whose fields stay in alphabetical order, as the peer's parser expects
            const std::string mqKey = "{\"brokerName\":\"" + mq.brokerName + "\",\"queueId\":"
                + std::to_string(mq.queueId) + ",\"topic\":\"" + mq.topic + "\"}";
            int64_t commit = 0;
            auto ot = consumeOffsetTable_.find(kv.first);
            if (ot != consumeOffsetTable_.end()) commit = ot->second;
            int64_t cached = 0;
            auto pt = pending_.find(kv.first);
            if (pt != pending_.end()) cached = static_cast<int64_t>(pt->second.size());
            // fillOutRunningInfo reports lastPullTimestamp.
            // Hard-coding 0 throws away the scene evidence behind the stall test "how long has this stream not pulled anything".
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
                // PopProcessQueue uses lastPopTimestamp in place of lastPullTimestamp for the stall test
                // (isPullExpired), so report the same value here to keep the two comparable.
                info.mqPopTable.set(mqKey, makePqi(0, kv.second->waitAckCount(),
                                                   kv.second->isDropped(),
                                                   kv.second->lastPopTimestamp()));
            }
        }
    }
    // statusTable (consumeStatus(group, topic), a minute-level snapshot)
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
    // Same contract as resetOffset: for every hit queue do `pq.setDropped(true);
    // pq.clear()` (in-flight batches voided) -> wait for concurrent consumption to end -> `updateConsumeOffset(mq, offset)` plus
    // `removeUnnecessaryMessageQueue` (persist first, then removeOffset) -> take it out of the
    // processQueueTable, and rebalance rebuilds it at the new offset.
    //
    // Only writing the new offset into the in-memory table fails silently: a batch fetched before the reset and acked after
    // it pushes the offset right back, the fresh offset just written on the broker is overwritten, and consumption continues from
    // the pre-reset position - while the offsetTable in the 222 reply still looks completely normal. So retireQueueLocked is required here
    // (bumping the epoch is setDropped, and it also removes the pull thread and the ownership record).
    if (topic.empty() || offsetTable.empty()) return;
    std::vector<std::pair<MessageQueue, int64_t>> retired;
    {
        std::lock_guard<std::mutex> lk(lock_);
        // Collect the keys first, then retire one by one: retireQueueLocked erases mqMap_ entries
        std::vector<std::pair<std::string, MessageQueue>> hits;
        for (const auto& kv : mqMap_) {
            if (kv.second.topic != topic) continue;
            auto it = offsetTable.find(kv.second);
            if (it == offsetTable.end()) continue;
            // Store the new offset in the table first: the revoke hands this value to onQueuesRevoked to persist it
            // (the persist that immediately follows updateConsumeOffset)
            consumeOffsetTable_[kv.first] = it->second;
            hits.emplace_back(kv.first, kv.second);
        }
        for (const auto& hit : hits) {
            retireQueueLocked(hit.first, hit.second, retired);
        }
    }
    if (retired.empty()) return;
    // The original contract waited RESET_OFFSET_MAX_WAIT (10s) for concurrent consumption to drain before writing offsets; shortened here to avoid blocking long
    // (220 is oneway, the broker awaits no response). The epoch bump already invalidates every in-flight ack, so waiting is not needed to avoid races.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    onQueuesRevoked(retired);
    try {
        doRebalance();
    } catch (const std::exception& e) {
        logger_debug(std::string("rebalance after reset offset failed: ") + e.what());
    }
    logger_info("reset offset applied, group=" + consumerGroup_ + " topic=" + topic
                + " queues=" + std::to_string(retired.size()));
}

std::map<MessageQueue, int64_t> DefaultMQPushConsumer::parseResetOffsetBody(const Bytes& body) {
    std::map<MessageQueue, int64_t> table;
    if (body.empty()) return table;
    ResetOffsetBody mapForm;
    if (ResetOffsetBody::decode(body, mapForm)) table = std::move(mapForm.offsetTable);
    if (!table.empty()) return table;
    // Array shape (ResetOffsetBodyForC): the map-shaped parser only yields an empty table for an array, and without this
    // fallback the whole reset request would be discarded silently.
    ResetOffsetBodyForC forC;
    if (ResetOffsetBodyForC::decode(body, forC)) {
        for (const MessageQueueForC& e : forC.offsetTable) {
            table[MessageQueue(e.topic, e.brokerName, e.queueId)] = e.offset;
        }
    }
    return table;
}

std::map<MessageQueue, int64_t> DefaultMQPushConsumer::getConsumerStatus(const std::string& topic) {
    // Data source of the 221 reply, getConsumerStatus:
    // returns the consumed-offset table (not the pull cursor).
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
    // consumeMessageDirectly (the real local consumption behind 309).
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
            // The orderly side of consumeMessageDirectly maps two more members than the concurrent side
            // (COMMIT -> CR_COMMIT, ROLLBACK -> CR_ROLLBACK, which the broker uses to tell the binlog contract apart),
            // and autoCommit reads the context value after the listener has finished.
            const ConsumeOrderlyStatus status = orderly->consumeMessage(msgs, ctx);
            switch (status) {
                case ConsumeOrderlyStatus::COMMIT:
                    result.consumeResult = "CR_COMMIT";
                    break;
                case ConsumeOrderlyStatus::ROLLBACK:
                    result.consumeResult = "CR_ROLLBACK";
                    break;
                case ConsumeOrderlyStatus::SUCCESS:
                    result.consumeResult = "CR_SUCCESS";
                    break;
                case ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    result.consumeResult = "CR_LATER";
                    break;
            }
        } catch (const std::exception& e) {
            result.consumeResult = "CR_THROW_EXCEPTION";
            result.remark = std::string("std::exception: ") + e.what();
        } catch (...) {
            result.consumeResult = "CR_THROW_EXCEPTION";
            result.remark = "unknown exception";
        }
        // autoCommit reads the context value after the listener returns (the exception path reads it too). The binlog usage that
        // sets it false inside the listener relies on this reply to tell the broker "this direct consume did not commit"; reading the
        // initial value early swallows it, and on a real cluster it only shows up as mqadmin always returning autoCommit=true.
        result.autoCommit = ctx.autoCommit;
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
        // Match the traceProducer.setNamespaceV2(namespaceV2) contract:
        // the trace dispatcher's internal producer must carry the instance namespace too, otherwise the broker writes the trace under a different instance.
        dispatcher->setNamespaceV2(namespaceV2_);
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
                                         const std::vector<MessageExt>& batch,
                                         const std::optional<uint64_t>& epoch) {
    // The queue was dropped while this batch was queued (rebalance revoke / OFFSET_ILLEGAL correction / stall self-healing):
    // these messages can neither be consumed nor acked - they belong to the already-dropped old ProcessQueue, so void them outright.
    // Same short-circuit as isDropped() in the concurrent and orderly consume chains: do not push them back (the new owner would consume them twice).
    if (epoch.has_value() && *epoch != queueEpoch(key)) {
        logger_warn("the message queue not be able to consume, because it's dropped. group="
                    + consumerGroup_ + " mq=" + key
                    + " msgs=" + std::to_string(batch.size()));
        return false;
    }
    // Restore the original business topic of redelivered messages before dispatching (resetRetryAndNamespace):
    // the broker writes redelivered messages into %RETRY%group and keeps the original topic in the RETRY_TOPIC property; without
    // restoring it, user code that branches on the topic takes the wrong path.
    std::vector<MessageExt> restored = batch;
    resetRetryTopicAndNamespace(restored);
    const bool broadcast = (messageModel_ == MessageModel::BROADCASTING);
    // ---- Orderly consumption (same contract as ConsumeMessageOrderlyService) ----
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
            // An orderly consume error -> commit no offset and retry in place
            logger_debug(std::string("orderly listener error (retry in place): ") + e.what());
            status = ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT;
            hookHasException = true;
        }
        if (status == ConsumeOrderlyStatus::ROLLBACK ||
            status == ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT) {
            // Log one warn whenever the result is not OK (including the suspend normalised from a thrown exception).
            // This log is the only clue to orderly head-of-line blocking; without it one can only see "this group stopped moving".
            logger_warn("consumeMessage Orderly return not OK, Group: " + consumerGroup_
                        + " Msgs: " + std::to_string(restored.size()) + " MQ: " + key);
        }
        // The orderly consume hooks fire once status is known too (same contract as the orderly service):
        // status is the normalised enum name, success is SUCCESS||COMMIT, while returnType
        // (CONSUME_CONTEXT_TYPE in props) is computed from the pre-normalisation status.
        if (useHook) {
            const bool ok = (status == ConsumeOrderlyStatus::SUCCESS ||
                             status == ConsumeOrderlyStatus::COMMIT);
            finishConsumeHook(&hookCtx, hookHasException, hookBeginMs,
                              status == ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT,
                              ok, consumeOrderlyStatusName(status));
        }
        if (ctx.autoCommit) {
            // ---- processConsumeResult (autoCommit=true) ----
            if (status == ConsumeOrderlyStatus::COMMIT ||
                status == ConsumeOrderlyStatus::ROLLBACK) {
                // With autoCommit=true, COMMIT/ROLLBACK is illegal usage (binlog consumption only):
                // only a warn is emitted and no break is written, so control falls through to the SUCCESS
                // branch - the messages are acked and the rollback request is ignored.
                logger_warn("the message queue consume result is illegal, we think you want to "
                            "ack these message " + key);
                status = ConsumeOrderlyStatus::SUCCESS;
            }
            if (status == ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT) {
                recordConsumeStats(mq.topic, static_cast<int64_t>(restored.size()), hookBeginMs, true);
                // Before suspending, run checkReconsumeTimes: only while inside the retry budget /
                // when the redelivery failed does this batch go back to the head and retry in place; a batch already
                // handed to the broker (redelivery succeeded) must advance the offset, otherwise one poison message blocks
                // this queue forever (head-of-line blocking on a real cluster looks like "this group stops at message N").
                if (checkOrderlyReconsumeTimes(restored)) {
                    requeuePending(key, restored);
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(orderlySuspendMillis(ctx)));
                    return false;
                }
            } else {
                recordConsumeStats(mq.topic, static_cast<int64_t>(restored.size()), hookBeginMs, false);
            }
            advanceConsumeOffset(key, restored, std::nullopt, epoch);
            consumedCount_.fetch_add(static_cast<int64_t>(restored.size()));
            return true;
        }
        // ---- autoCommit=false (binlog consumption: the commit decision belongs to the listener) ----
        if (status == ConsumeOrderlyStatus::COMMIT) {
            // Explicit commit: the offset advances but no TPS is recorded (RT is still recorded outside the branch)
            recordConsumeRt(mq.topic, hookBeginMs);
            advanceConsumeOffset(key, restored, std::nullopt, epoch);
            consumedCount_.fetch_add(static_cast<int64_t>(restored.size()));
            return true;
        }
        if (status == ConsumeOrderlyStatus::ROLLBACK) {
            // rollback() returns the messages to the ProcessQueue and delays the retry
            recordConsumeRt(mq.topic, hookBeginMs);
            requeuePending(key, restored);
            std::this_thread::sleep_for(std::chrono::milliseconds(orderlySuspendMillis(ctx)));
            return false;
        }
        if (status == ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT) {
            recordConsumeStats(mq.topic, static_cast<int64_t>(restored.size()), hookBeginMs, true);
            if (checkOrderlyReconsumeTimes(restored)) {
                requeuePending(key, restored);
                std::this_thread::sleep_for(std::chrono::milliseconds(orderlySuspendMillis(ctx)));
            }
            // Difference from the auto-commit branch: once the poison message is handed to the broker nothing is committed,
            // and whether the offset advances is left to the binlog consumer to decide.
            return false;
        }
        // SUCCESS + autoCommit=false: the original contract only records OK TPS and commits nothing, leaving the
        // messages in ProcessQueue.consumingMsgOrderlyTreeMap awaiting an explicit commit(); here the ProcessQueue is not
        // exposed to the listener (there is no commit entry point), so copying "do nothing" would let the dispatch thread
        // swallow this batch while the offset never moves. Equivalent behaviour here: push back to the head and wait one suspend
        // period - the offset still does not advance, no message is lost, and the thread is not turned into a busy loop.
        recordConsumeStats(mq.topic, static_cast<int64_t>(restored.size()), hookBeginMs, false);
        requeuePending(key, restored);
        std::this_thread::sleep_for(std::chrono::milliseconds(orderlySuspendMillis(ctx)));
        return false;
    }
    // ---- Concurrent consumption (ConsumeRequest.run of ConsumeMessageConcurrentlyService) ----
    // "register in-flight -> consume -> deregister in-flight" is wrapped in RAII: the concurrent branch has many returns
    // (whole batch accepted, broadcast discard, redelivery failed ...). Missing one manual deregistration leaves the registry
    // dirty forever, and correctTagsOffset's in-flight gate would then block the offset permanently.
    struct InflightGuard {
        DefaultMQPushConsumer* self;
        const std::string* guardKey;
        const std::vector<MessageExt>* guardMsgs;
        ~InflightGuard() { self->deregisterInflight(*guardKey, *guardMsgs); }
    } inflightGuard{this, &key, &restored};
    return consumeConcurrentBatch(key, mq, restored, broadcast, epoch);
}

bool DefaultMQPushConsumer::consumeConcurrentBatch(const std::string& key, const MessageQueue& mq,
                                                   std::vector<MessageExt>& restored, bool broadcast,
                                                   const std::optional<uint64_t>& epoch) {
    const bool useHook = hasConsumeMessageHook();
    ConsumeMessageContext hookCtx;
    if (useHook) {
        // Fixed hook order: before runs ahead of the listener (produces SubBefore),
        // after runs once status is known (produces SubAfter, carrying the contextCode)
        hookCtx = buildConsumeHookContext(restored, mq);
        executeConsumeHookBefore(hookCtx);
    }
    // Stamp CONSUME_START_TIME on each message before handing them to the listener, re-stamped on every
    // delivery (retries included); cleanExpiredMsg's suspend escape uses it to judge how long a message has been held. Registration
    // follows the stamping: batches here are MessageExt value
    // copies (not shared references), so registering early would leave un-stamped copies in the table that the sweep never sees.
    const int64_t stampNow = UtilAll::currentTimeMillis();
    for (MessageExt& m : restored) {
        m.putProperty(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP, std::to_string(stampNow));
    }
    registerInflight(key, restored);
    auto* conc = static_cast<MessageListenerConcurrently*>(messageListener_.get());
    ConsumeConcurrentlyContext ctx(mq);
    ConsumeConcurrentlyStatus status;
    bool hookHasException = false;
    int64_t hookBeginMs = UtilAll::currentTimeMillis();
    try {
        status = conc->consumeMessage(restored, ctx);
    } catch (const std::exception& e) {
        // A consume exception is treated as RECONSUME_LATER
        logger_debug(std::string("listener error, treat as RECONSUME_LATER: ") + e.what());
        status = ConsumeConcurrentlyStatus::RECONSUME_LATER;
        hookHasException = true;
    }
    // processConsumeResult: CONSUME_SUCCESS uses the ackIndex set by the listener
    // to split "accepted prefix / tail to send back" (default Integer.MAX_VALUE; clamping to size-1 means the whole batch is accepted);
    // RECONSUME_LATER forces ackIndex=-1 and sends the whole batch back.
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
        // Broadcasting mode sends nothing back: the unaccepted tail only gets one warn and is dropped,
        // and the offset still advances for the whole batch (no redelivery after a restart)
        const size_t dropped = restored.size() - acked;
        if (dropped > 0) {
            logger_warn("BROADCASTING, the message consume failed, drop it: "
                        + std::to_string(dropped) + " msgs in " + mq.toString());
        }
        advanceConsumeOffset(key, restored, std::nullopt, epoch);
        consumedCount_.fetch_add(static_cast<int64_t>(restored.size()));
        return true;
    }
    if (acked >= restored.size()) {
        // Whole batch accepted (the default path): nothing needs to be sent back, so the offset advances directly
        advanceConsumeOffset(key, restored, std::nullopt, epoch);
        consumedCount_.fetch_add(static_cast<int64_t>(restored.size()));
        return true;
    }
    // Clustering: every unaccepted message in [acked, size) is sent back one by one to %RETRY%topic
    // (delay ladder 3+reconsumeTimes; past the limit the broker moves it to %DLQ%)
    const std::vector<std::pair<size_t, MessageExt>> msgBackFailed =
        sendBackBatch(key, restored, ctx, acked);
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
    // The messages whose send-back failed go back to the head and retry later (they are not removed from the ProcessQueue)
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
    // What gets committed is "the largest queueOffset among this batch's handled entries + 1", and it must not step over
    // the send-back failures still left in the buffer, otherwise those would be silently skipped by the offset (message loss)
    std::vector<MessageExt> handled;
    handled.reserve(restored.size() - failedIdx.size());
    for (size_t i = 0; i < restored.size(); i++) {
        if (failedIdx.count(i) == 0) {
            handled.push_back(restored[i]);
        }
    }
    advanceConsumeOffset(key, handled, hasFloor ? std::optional<int64_t>(floorVal) : std::nullopt,
                         epoch);
    consumedCount_.fetch_add(static_cast<int64_t>(handled.size()));
    return msgBackFailed.empty();
}

std::vector<std::pair<size_t, MessageExt>> DefaultMQPushConsumer::sendBackBatch(
    const std::string& key, const std::vector<MessageExt>& batch,
    const ConsumeConcurrentlyContext& ctx, size_t base) {
    std::vector<std::pair<size_t, MessageExt>> failed;
    for (size_t i = base; i < batch.size(); i++) {
        const MessageExt& msg = batch[i];
        // containsMessage first: a message swept and sent back (and removed) by cleanExpiredMsg while the listener was
        // still running must not be sent back a second time when that listener later returns
        // RECONSUME_LATER, otherwise the same offset would be delivered twice.
        if (!processQueueContains(key, msg)) {
            logger_info("Message is not found in its process queue; skip send-back-procedure"
                        ", topic=" + msg.topic + ", brokerName=" + msg.brokerName
                        + ", queueId=" + std::to_string(msg.queueId)
                        + ", queueOffset=" + std::to_string(msg.queueOffset));
            continue;
        }
        // delayLevelWhenNextConsume == 0 -> 3 + reconsumeTimes
        // (reconsumeTimes is field 13 of the MessageExt wire format; the broker increments it on redelivery)
        int32_t delayLevel = ctx.delayLevelWhenNextConsume;
        if (delayLevel == 0) {
            delayLevel = 3 + msg.getReconsumeTimes();
        }
        // sendMessageBack swallows every exception itself and reports success through its return value; watching only for
        // exceptions would read "send-back failed" as "send-back succeeded", and the offset would pass this message over silently.
        if (!sendMessageBack(msg, delayLevel)) {
            logger_debug("send message back failed for msg " + msg.msgId);
            MessageExt retried = msg;
            // The count is bumped on the copy that will be consumed again; the broker recorded no success
            retried.setReconsumeTimes(retried.getReconsumeTimes() + 1);
            failed.emplace_back(i, retried);
        }
    }
    return failed;
}

void DefaultMQPushConsumer::advanceConsumeOffset(const std::string& key,
                                                 const std::vector<MessageExt>& batch,
                                                 const std::optional<int64_t>& floor,
                                                 const std::optional<uint64_t>& epoch) {
    if (batch.empty()) {
        // When every message of the batch failed to be sent back, nothing was accepted and the offset stays put
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
    // An epoch mismatch means this batch belongs to a revoked/rebuilt old ProcessQueue, so the whole ack is voided
    // (the !processQueue.isDropped() gate).
    // Without it the stale batch's ack would push the offset the broker just corrected back to an illegal value,
    // bouncing back and forth between client and broker.
    if (epoch.has_value() && *epoch != queueEpochLocked(key)) {
        logger_debug("drop ack for " + key + ": process queue was dropped");
        return;
    }
    // Frozen by the OFFSET_ILLEGAL correction (ControllableOffset with allowToUpdate=false):
    // no ack may change it until the queue is rebuilt at the corrected offset.
    if (frozenOffsets_.count(key) > 0) {
        return;
    }
    auto it = consumeOffsetTable_.find(key);
    if (it == consumeOffsetTable_.end() || it->second < nextOffset) {
        consumeOffsetTable_[key] = nextOffset;
    }
}

// correctTagsOffset (the empty-response correction in the pull callback). lock_ must be held.
//
// When the pull response is NO_NEW_MSG (the queue really holds nothing) or NO_MATCHED_MSG (the broker applied the
// subscription expression over the window and matched not a single message; PULL_RETRY_IMMEDIATELY maps to
// it), this queue's "consumed offset" must follow the pull cursor or it sticks forever: messages nobody acked
// belong to nobody (broker-side filtered ones are absent from the response, and entries removed by the client's second tag
// filter are explicitly not acked), so a frozen offset never moves and redelivery/restart rescans that unwanted batch from the start.
//
// The update goes through offsetStore.updateOffset(mq, nextOffset, increaseOnly=true) (forward only, never backwards),
// behind one gate: 0L == processQueue.getMsgCount(). msgCount counts the messages still inside the ProcessQueue -
// concurrent consumption calls removeMessage only after the listener returns,
// so in-flight batches count as well; raising the offset before they settle means a process crash silently skips those messages.
// The emptiness of pending_ and inflightCount_ == 0 are combined here into one
// test, and the in-flight registration happens in the same dispatchLoop lock as "take the batch" (see takeBatchLocked).
//
// The status test is deliberately folded into this function too (it used to be spread over the callback's switch), so an
// offline unit test can pin down both "which statuses need correcting" and "when a correction is allowed".
void DefaultMQPushConsumer::correctTagsOffsetLocked(const std::string& key, PullStatus status,
                                                    int64_t nextOffset) {
    if (status != PullStatus::NO_NEW_MSG && status != PullStatus::NO_MATCHED_MSG) {
        return;
    }
    if (frozenOffsets_.count(key) > 0) {
        // The offset is frozen by the OFFSET_ILLEGAL correction (ControllableOffset.update drops the update
        // outright when allowToUpdate=false), so this path must not change it either
        return;
    }
    auto pendingIt = pending_.find(key);
    if (pendingIt != pending_.end() && !pendingIt->second.empty()) {
        return;
    }
    auto inflightIt = inflightCount_.find(key);
    if (inflightIt != inflightCount_.end() && inflightIt->second > 0) {
        return;
    }
    auto it = consumeOffsetTable_.find(key);
    if (it == consumeOffsetTable_.end() || it->second < nextOffset) {
        consumeOffsetTable_[key] = nextOffset;
    }
}

void DefaultMQPushConsumer::correctTagsOffset(const std::string& key, PullStatus status,
                                              int64_t nextOffset) {
    std::lock_guard<std::mutex> lk(lock_);
    correctTagsOffsetLocked(key, status, nextOffset);
}

std::vector<MessageExt> DefaultMQPushConsumer::takeBatchLocked(const std::string& key) {
    auto it = pending_.find(key);
    if (it == pending_.end() || it->second.empty()) {
        return {};
    }
    const size_t n = static_cast<size_t>(
        std::min<size_t>(it->second.size(),
                         static_cast<size_t>(std::max(1, consumeMessageBatchMaxSize_))));
    std::vector<MessageExt> batch;
    batch.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        batch.push_back(it->second.front());
        it->second.pop_front();
    }
    // Register in-flight in the same critical section as taking the batch: correctTagsOffsetLocked's gate uses it to tell
    // "messages still in the buffer" from "messages in the listener's hands" - neither one counts as empty.
    ++inflightCount_[key];
    return batch;
}

void DefaultMQPushConsumer::finishBatchLocked(const std::string& key) {
    auto it = inflightCount_.find(key);
    if (it == inflightCount_.end()) {
        return;
    }
    if (--it->second <= 0) {
        inflightCount_.erase(it);
    }
}

std::vector<MessageExt> DefaultMQPushConsumer::takeBatchForConsume(const std::string& key,
                                                                  uint64_t* epochOut) {
    std::lock_guard<std::mutex> lk(lock_);
    // The epoch and the batch must be read in the same critical section: reading them separately opens a "take the batch, then read the epoch" window,
    // and if the queue is revoked and rebuilt in between, the old batch gets stamped with the new epoch and its ack is wrongly allowed through.
    if (epochOut != nullptr) {
        *epochOut = queueEpochLocked(key);
    }
    return takeBatchLocked(key);
}

void DefaultMQPushConsumer::finishBatchConsume(const std::string& key) {
    std::lock_guard<std::mutex> lk(lock_);
    finishBatchLocked(key);
}

// ---------------------------------------------------------------- Offset persistence
void DefaultMQPushConsumer::offsetPersistLoop() {
    // Scheduling contract:
    //   scheduleAtFixedRate(persistAllConsumerOffset, 1000 * 10, persistConsumerOffsetInterval)
    // - the first task is delayed 10s, then the period comes from clientConfig.persistConsumerOffsetInterval (5s by default);
    // the first flush happens exactly at initialDelay, not at initialDelay + one period (the old form took 15s).
    // The rate is fixed: every tick is anchored at 10s + n x period (see schedule_util.h), so it does not drift
    // longer and longer the way "finish the work, then sleep one period sliced into 100ms chunks" would.
    // The period is read once at startup; changing the field at runtime does not alter the already scheduled cadence.
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
    // Local offset file path (the LocalFileOffsetStore convention): $HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json
    std::string base = UtilAll::userHome();
    if (base.empty()) base = ".";
    return base + "/.rocketmq_offsets/" + (clientId_.empty() ? "DEFAULT" : clientId_)
           + "/" + consumerGroup_ + "/offsets.json";
}

namespace {
// Low-level pieces of the "object as JSON key" scanner (no regex dependency, hand-written scan, tolerant of the
// pretty-printed variant's newlines and indentation). File content is external input: any structural error fails the whole parse.

void offsetScanSkipWs(const std::string& s, size_t& pos) {
    while (pos < s.size()
           && (s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\r' || s[pos] == '\n')) {
        ++pos;
    }
}

bool offsetScanEat(const std::string& s, size_t& pos, char c) {
    offsetScanSkipWs(s, pos);
    if (pos < s.size() && s[pos] == c) {
        ++pos;
        return true;
    }
    return false;
}

bool offsetScanPeek(const std::string& s, size_t pos, char c) {
    offsetScanSkipWs(s, pos);
    return pos < s.size() && s[pos] == c;
}

bool offsetScanString(const std::string& s, size_t& pos, std::string& out) {
    offsetScanSkipWs(s, pos);
    if (pos >= s.size() || s[pos] != '"') return false;
    ++pos;
    out.clear();
    while (pos < s.size()) {
        char c = s[pos++];
        if (c == '"') return true;
        if (c != '\\') {
            out += c;
            continue;
        }
        if (pos >= s.size()) return false;
        char e = s[pos++];
        switch (e) {
            case '"': out += '"'; break;
            case '\\': out += '\\'; break;
            case '/': out += '/'; break;
            case 'b': out += '\b'; break;
            case 'f': out += '\f'; break;
            case 'n': out += '\n'; break;
            case 'r': out += '\r'; break;
            case 't': out += '\t'; break;
            case 'u': {
                if (pos + 4 > s.size()) return false;
                int cp = 0;
                for (int i = 0; i < 4; ++i) {
                    char h = s[pos + i];
                    int d;
                    if (h >= '0' && h <= '9') d = h - '0';
                    else if (h >= 'a' && h <= 'f') d = h - 'a' + 10;
                    else if (h >= 'A' && h <= 'F') d = h - 'A' + 10;
                    else return false;
                    cp = cp * 16 + d;
                }
                pos += 4;
                // The offset file only holds topic/broker names, so encoding BMP code points as UTF-8 is enough
                if (cp < 0x80) {
                    out += static_cast<char>(cp);
                } else if (cp < 0x800) {
                    out += static_cast<char>(0xC0 | (cp >> 6));
                    out += static_cast<char>(0x80 | (cp & 0x3F));
                } else {
                    out += static_cast<char>(0xE0 | (cp >> 12));
                    out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
                    out += static_cast<char>(0x80 | (cp & 0x3F));
                }
                break;
            }
            default:
                return false;
        }
    }
    return false;
}

bool offsetScanInt(const std::string& s, size_t& pos, int64_t& out) {
    offsetScanSkipWs(s, pos);
    size_t start = pos;
    if (pos < s.size() && s[pos] == '-') ++pos;
    size_t digits = pos;
    while (pos < s.size() && s[pos] >= '0' && s[pos] <= '9') ++pos;
    if (pos == digits || pos - digits > 18) return false;
    out = std::stoll(s.substr(start, pos - start));
    return true;
}
}  // namespace

std::string DefaultMQPushConsumer::buildLocalOffsetsJson(
    const std::map<std::string, int64_t>& items,
    const std::map<std::string, MessageQueue>& mqMap) {
    // On-disk format (mutually readable with files written by other clients, verified compatible in practice):
    // {"offsetTable":{{"brokerName":"broker-a","queueId":1,"topic":"Tt"}:9,...}}
    // - the MessageQueue object is used directly as the JSON key; entries without queue info are skipped (same as persistAll).
    std::string s = "{\"offsetTable\":{";
    bool first = true;
    for (const auto& kv : items) {
        auto it = mqMap.find(kv.first);
        if (it == mqMap.end()) continue;
        if (!first) s += ',';
        first = false;
        s += "{\"brokerName\":" + JsonValue::makeString(it->second.brokerName).dump();
        s += ",\"queueId\":" + std::to_string(it->second.queueId);
        s += ",\"topic\":" + JsonValue::makeString(it->second.topic).dump();
        s += "}:" + std::to_string(kv.second);
    }
    s += "}}";
    return s;
}

std::optional<std::map<std::string, int64_t>> DefaultMQPushConsumer::parseLocalOffsetsText(
    const std::string& text) {
    // Try strict JSON first (the flat map older versions used to write); the "object as key"
    // format always fails strict parsing (the pretty variant included), so it falls through to the scanner below.
    size_t b = text.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return std::nullopt;
    size_t e = text.find_last_not_of(" \t\r\n");
    std::string trimmed = text.substr(b, e - b + 1);

    JsonValue root;
    if (jsonParse(trimmed, root) && root.isObject()) {
        std::map<std::string, int64_t> flat;
        bool allNumbers = true;
        for (const auto& kv : root.objectItems()) {
            if (!kv.second.isNumber()) {
                allNumbers = false;
                break;
            }
            flat[kv.first] = kv.second.intValue();
        }
        if (allNumbers) return flat;
    }

    size_t pos = 0;
    if (!offsetScanEat(trimmed, pos, '{')) return std::nullopt;
    std::string key;
    if (!offsetScanString(trimmed, pos, key) || key != "offsetTable") return std::nullopt;
    if (!offsetScanEat(trimmed, pos, ':') || !offsetScanEat(trimmed, pos, '{')) {
        return std::nullopt;
    }
    std::map<std::string, int64_t> out;
    while (true) {
        if (offsetScanEat(trimmed, pos, '}')) break;
        if (!offsetScanEat(trimmed, pos, '{')) return std::nullopt;
        std::string topic;
        std::string broker;
        bool hasTopic = false;
        bool hasBroker = false;
        int64_t queueId = 0;
        bool hasQueueId = false;
        while (true) {
            if (offsetScanEat(trimmed, pos, '}')) break;
            std::string fkey;
            if (!offsetScanString(trimmed, pos, fkey) || !offsetScanEat(trimmed, pos, ':')) {
                return std::nullopt;
            }
            if (offsetScanPeek(trimmed, pos, '"')) {
                std::string v;
                if (!offsetScanString(trimmed, pos, v)) return std::nullopt;
                if (fkey == "topic") {
                    topic = v;
                    hasTopic = true;
                } else if (fkey == "brokerName") {
                    broker = v;
                    hasBroker = true;
                }
            } else {
                int64_t v = 0;
                if (!offsetScanInt(trimmed, pos, v)) return std::nullopt;
                if (fkey == "queueId") {
                    queueId = v;
                    hasQueueId = true;
                }
            }
            if (offsetScanEat(trimmed, pos, ',')) continue;
            if (offsetScanEat(trimmed, pos, '}')) break;
            return std::nullopt;
        }
        int64_t off = 0;
        if (!offsetScanEat(trimmed, pos, ':') || !hasTopic || !hasBroker || !hasQueueId
            || !offsetScanInt(trimmed, pos, off)) {
            return std::nullopt;
        }
        out[topic + broker + std::to_string(queueId)] = off;
        if (offsetScanEat(trimmed, pos, ',')) continue;
        if (offsetScanEat(trimmed, pos, '}')) break;
        return std::nullopt;
    }
    // After the table closes there is still the wrapper's own } ({"offsetTable":{...}} has two brace levels)
    if (!offsetScanEat(trimmed, pos, '}')) return std::nullopt;
    offsetScanSkipWs(trimmed, pos);
    if (pos != trimmed.size()) return std::nullopt;
    return out;
}

void DefaultMQPushConsumer::saveLocalOffsetsAt(const std::string& path,
                                               const std::map<std::string, int64_t>& items,
                                               const std::map<std::string, MessageQueue>& mqMap) {
    std::string dir = path.substr(0, path.find_last_of('/'));
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    // Offset-file writer contract: the previous content rolls into .bak before the new file is written
    std::ifstream prevIn(path);
    if (prevIn.is_open()) {
        std::string prev((std::istreambuf_iterator<char>(prevIn)),
                         std::istreambuf_iterator<char>());
        prevIn.close();
        if (!prev.empty()) {
            std::ofstream bak(path + ".bak", std::ios::trunc);
            if (bak.is_open()) bak << prev;
        }
    }
    // Write .tmp first, then rename atomically, so a half-written file never appears (same writer contract)
    std::string tmp = path + ".tmp";
    {
        std::ofstream f(tmp, std::ios::trunc);
        if (f.is_open()) f << buildLocalOffsetsJson(items, mqMap);
    }
    std::filesystem::rename(tmp, path, ec);
}

std::map<std::string, int64_t> DefaultMQPushConsumer::loadLocalOffsetsAt(const std::string& path) {
    // Fall back to .bak when the main file is missing or unparseable
    for (const std::string& candidate : {path, path + ".bak"}) {
        std::ifstream f(candidate);
        if (!f.is_open()) continue;
        std::string text((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        auto parsed = parseLocalOffsetsText(text);
        if (parsed.has_value()) return *parsed;
    }
    return {};
}

void DefaultMQPushConsumer::saveLocalOffsets() {
    std::map<std::string, int64_t> items;
    std::map<std::string, MessageQueue> mqMap;
    {
        std::lock_guard<std::mutex> lk(lock_);
        items = consumeOffsetTable_;
        mqMap = mqMap_;
    }
    saveLocalOffsetsAt(localOffsetPath(), items, mqMap);
}

void DefaultMQPushConsumer::saveLocalOffsets(
    const std::vector<std::pair<MessageQueue, int64_t>>& extra) {
    std::map<std::string, int64_t> items;
    std::map<std::string, MessageQueue> mqMap;
    {
        std::lock_guard<std::mutex> lk(lock_);
        items = consumeOffsetTable_;
        mqMap = mqMap_;
    }
    for (const auto& kv : extra) {
        // No offset at all (absent from the table at revoke time, expressed here as -1): do not invent an entry
        if (kv.second < 0) continue;
        const std::string key = offsetKey(kv.first);
        items[key] = kv.second;
        // The queue descriptor must be merged too: buildLocalOffsetsJson silently skips an entry whose queue key
        // is missing from mqMap_, so merging only the offsets would drop exactly the ones being rescued.
        mqMap.emplace(key, kv.first);
    }
    saveLocalOffsetsAt(localOffsetPath(), items, mqMap);
}

std::map<std::string, int64_t> DefaultMQPushConsumer::loadLocalOffsets() const {
    return loadLocalOffsetsAt(localOffsetPath());
}

// ---------------------------------------------------------------- Orderly consume queue locks
void DefaultMQPushConsumer::lockLoop() {
    if (!isOrderly() || messageModel_ == MessageModel::BROADCASTING) {
        return;
    }
    // lockMQ: batch-lock the assigned queues every 20s;
    // the first attempt runs immediately at start so the first 20s is not spent idle
    while (!stop_.load()) {
        // The POP orderly service runs this same 20s
        // timer too, but the batch lock only walks the classic pull queue table - in POP mode the queues
        // are registered in the POP table and that one stays empty, so the timer turns without ever sending.
        // Same contract here: POP mode keeps the timer and sends no LOCK_BATCH_MQ.
        if (!popMode_) {
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
        }
        // Wait 20s (responsive to stop meanwhile). Sleep in chunks measured against an absolute deadline:
        // a 100ms system sleep runs a few ms long, so 200 chunks stretch the round; per-chunk deltas do not add up.
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

// ---------------------------------------------------------------- Expired message cleanup
//
// Full contract of the expired-message sweep: one dedicated scheduler thread per group,
 // fixed-rate with initial delay == period == consumeTimeout minutes, walking the process queues
 // one at a time; the per-queue body below mirrors that loop statement for statement
 // (see cleanExpiredQueue).
//
// The only reclamation path when a listener hangs and never returns: messages held longer
// than consumeTimeout minutes are sent back to the broker (delayLevel 3) for %RETRY% redelivery;
// without it one stuck message pins the queue offset forever, invisible to logs and timeouts.
// Three rules, each wrong direction a silent failure: head only, strictly greater, max 16 a round.
bool DefaultMQPushConsumer::isConsumeExpired(const MessageExt& msg, int32_t consumeTimeoutMinutes,
                                             int64_t nowMs) {
    const std::string stamp = msg.getProperty(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP);
    if (stamp.empty()) {
        // Never stamped (a buffered message that has not entered a listener yet) does not count as expired.
        // This clause also keeps the whole "backlog nobody has consumed yet" outside the sweep.
        return false;
    }
    // The expiry test is STRICTLY greater than the threshold (no action when exactly equal). std::stoll
    // throws invalid_argument on a non-numeric stamp, taking the same road as a parse failure:
    // cleanExpiredMsgLoop's scheduling shell catches it and logs - swallowing it here would silence the sweep.
    return nowMs - std::stoll(stamp) > static_cast<int64_t>(consumeTimeoutMinutes) * 60 * 1000;
}

int64_t DefaultMQPushConsumer::cleanExpirePeriodMillis(int32_t consumeTimeoutMinutes) {
    // initialDelay and period share one source: consumeTimeout minutes (default 15 -> 900000ms).
    // The max(1, .) fallback is defensive: setConsumeTimeout does not validate, and 0 would turn the
    // schedule into a fire every 0 minutes (a busy wait if it really ran), so 1 minute is the floor.
    return static_cast<int64_t>(std::max(1, consumeTimeoutMinutes)) * 60 * 1000;
}

void DefaultMQPushConsumer::setInflightMessages(const std::string& key,
                                                const std::vector<MessageExt>& msgs) {
    std::lock_guard<std::mutex> lk(lock_);
    inflightMsgs_[key] = msgs;
}

void DefaultMQPushConsumer::registerInflight(const std::string& key,
                                             const std::vector<MessageExt>& msgs) {
    std::lock_guard<std::mutex> lk(lock_);
    auto& bucket = inflightMsgs_[key];
    bucket.insert(bucket.end(), msgs.begin(), msgs.end());
}

void DefaultMQPushConsumer::deregisterInflight(const std::string& key,
                                               const std::vector<MessageExt>& msgs) {
    // Remove only on the in-flight side (the buffer may hold a same-offset copy pushed back after a failed send-back, and that one must stay).
    // Removal keys on queueOffset: batches here are copies with no object identity to use, and the
    // process queue is an offset -> msg mapping anyway, so one message can only appear once on the table.
    std::lock_guard<std::mutex> lk(lock_);
    auto it = inflightMsgs_.find(key);
    if (it == inflightMsgs_.end()) return;
    for (const MessageExt& m : msgs) {
        for (auto vit = it->second.begin(); vit != it->second.end(); ++vit) {
            if (vit->queueOffset == m.queueOffset) {
                it->second.erase(vit);
                break;
            }
        }
    }
    if (it->second.empty()) inflightMsgs_.erase(it);
}

std::vector<MessageExt> DefaultMQPushConsumer::processQueueEntriesLocked(
    const std::string& key) const {
    // pending_ (pulled but not dispatched) union inflightMsgs_ (dispatched but not settled) = the complete process-queue view.
    // Both sides are maintained in pairs inside the same lock_, so a message can only live on one side at any
    // instant; de-duplicating by queueOffset again is only defence against future call sites producing overlaps.
    std::vector<MessageExt> out;
    std::set<int64_t> seen;
    auto iit = inflightMsgs_.find(key);
    if (iit != inflightMsgs_.end()) {
        for (const MessageExt& m : iit->second) {
            if (seen.insert(m.queueOffset).second) out.push_back(m);
        }
    }
    auto pit = pending_.find(key);
    if (pit != pending_.end()) {
        for (const MessageExt& m : pit->second) {
            if (seen.insert(m.queueOffset).second) out.push_back(m);
        }
    }
    return out;
}

std::optional<MessageExt> DefaultMQPushConsumer::processQueueHeadLocked(
    const std::string& key) const {
    // The head is the entry with the smallest queueOffset (the first entry of the offset-ordered view).
    const std::vector<MessageExt> entries = processQueueEntriesLocked(key);
    if (entries.empty()) return std::nullopt;
    const MessageExt* best = &entries[0];
    for (const MessageExt& m : entries) {
        if (m.queueOffset < best->queueOffset) best = &m;
    }
    return *best;
}

bool DefaultMQPushConsumer::processQueueContainsLocked(const std::string& key,
                                                       const MessageExt& msg) const {
    // containsMessage semantics: "is this queueOffset still keyed on the table".
    // Not object identity: batches here are copies, and the offset membership is exactly the question asked.
    auto iit = inflightMsgs_.find(key);
    if (iit != inflightMsgs_.end()) {
        for (const MessageExt& m : iit->second) {
            if (m.queueOffset == msg.queueOffset) return true;
        }
    }
    auto pit = pending_.find(key);
    if (pit != pending_.end()) {
        for (const MessageExt& m : pit->second) {
            if (m.queueOffset == msg.queueOffset) return true;
        }
    }
    return false;
}

void DefaultMQPushConsumer::removeProcessQueueEntryLocked(const std::string& key,
                                                          const MessageExt& msg) {
    auto iit = inflightMsgs_.find(key);
    if (iit != inflightMsgs_.end()) {
        for (auto vit = iit->second.begin(); vit != iit->second.end(); ++vit) {
            if (vit->queueOffset == msg.queueOffset) {
                iit->second.erase(vit);
                break;
            }
        }
        if (iit->second.empty()) inflightMsgs_.erase(iit);
    }
    // Remove from the buffer side too: after a failed send-back both sides can briefly hold the same
    // offset, so removing only one side lets the next sweep see it again and deliver it a second time.
    // Scan by offset rather than head only: push_front entries leave the buffer not strictly offset-ordered.
    auto pit = pending_.find(key);
    if (pit != pending_.end()) {
        for (auto vit = pit->second.begin(); vit != pit->second.end(); ++vit) {
            if (vit->queueOffset == msg.queueOffset) {
                pit->second.erase(vit);
                break;
            }
        }
    }
}

std::vector<MessageExt> DefaultMQPushConsumer::processQueueEntries(const std::string& key) const {
    std::lock_guard<std::mutex> lk(lock_);
    return processQueueEntriesLocked(key);
}

std::optional<MessageExt> DefaultMQPushConsumer::processQueueHead(const std::string& key) const {
    std::lock_guard<std::mutex> lk(lock_);
    return processQueueHeadLocked(key);
}

bool DefaultMQPushConsumer::processQueueContains(const std::string& key,
                                                 const MessageExt& msg) const {
    std::lock_guard<std::mutex> lk(lock_);
    return processQueueContainsLocked(key, msg);
}

void DefaultMQPushConsumer::removeExpiredEntryIfStillHead(const std::string& key,
                                                          const MessageExt& msg) {
    std::lock_guard<std::mutex> lk(lock_);
    // Reached only after the send-back succeeded; remove it only while it IS still the head:
    // if the listener wrapped up normally in the meantime (or a smaller offset surfaced ahead of it), yield to the normal path instead of grabbing it.
    const std::optional<MessageExt> head = processQueueHeadLocked(key);
    if (!head.has_value() || head->queueOffset != msg.queueOffset) {
        return;
    }
    removeProcessQueueEntryLocked(key, msg);
}

int32_t DefaultMQPushConsumer::cleanExpiredQueue(const std::string& key, int64_t nowMs) {
    // The loop body of the per-queue expired-message sweep.
    if (isOrderly()) {
        // Orderly consumption has no such path (messages are retried in place; sending them back would reorder the queue)
        return 0;
    }
    size_t loop = 0;
    {
        // loop is computed once BEFORE entering (at most 16 per round); it does not shrink dynamically as entries are removed
        std::lock_guard<std::mutex> lk(lock_);
        loop = std::min<size_t>(processQueueEntriesLocked(key).size(), 16);
    }
    int32_t attempts = 0;
    for (size_t i = 0; i < loop; i++) {
        std::optional<MessageExt> head;
        {
            std::lock_guard<std::mutex> lk(lock_);
            head = processQueueHeadLocked(key);
        }
        if (!head.has_value()) break;
        if (!isConsumeExpired(*head, consumeTimeoutMinutes_, nowMs)) {
            break;  // the head is not expired, so later ones cannot be
        }
        const MessageExt msg = *head;
        attempts++;
        if (!sendMessageBack(msg, 3)) {  // delayLevel fixed at 3
            // A failed send-back only logs: the message stays where it is and the next round retries it; never remove it here.
            logger_error("send expired msg exception: send message back failed, msgId="
                         + msg.msgId + ", topic=" + msg.topic
                         + ", queueOffset=" + std::to_string(msg.queueOffset));
            continue;
        }
        logger_info("send expire msg back. topic=" + msg.topic + ", msgId=" + msg.msgId
                    + ", storeHost=" + msg.storeHost
                    + ", queueId=" + std::to_string(msg.queueId)
                    + ", queueOffset=" + std::to_string(msg.queueOffset));
        removeExpiredEntryIfStillHead(key, msg);
    }
    return attempts;
}

int32_t DefaultMQPushConsumer::cleanExpiredMsgOnce(int64_t nowMs) {
    // Sweep entry: walk the queues of the current rebalance assignment and clean them one by
    // one. Sweep scope = the queues HELD right now (a revoked one is never scanned again).
    std::vector<std::string> keys;
    {
        std::lock_guard<std::mutex> lk(lock_);
        for (const auto& kv : mqMap_) {
            keys.push_back(kv.first);
        }
    }
    int32_t attempts = 0;
    for (const std::string& key : keys) {
        attempts += cleanExpiredQueue(key, nowMs);
    }
    return attempts;
}

void DefaultMQPushConsumer::cleanExpiredMsgLoop() {
    // Scheduling contract of the expired-message task:
    //   fixed-rate schedule(sweep, initialDelay = consumeTimeout, period = consumeTimeout, unit MINUTES)
    // so initialDelay and period are the same value (consumeTimeout minutes) and even the first round waits a full period;
    // sweeping immediately would walk every message that just started consuming and cannot be expired yet (harmless, but pointless).
    // Same contract as the other periodic tasks (fixed-rate advance from schedule_util.h + 100ms chunks responsive to stop_).
    const auto period = std::chrono::milliseconds(cleanExpirePeriodMillis(consumeTimeoutMinutes_));
    auto next = std::chrono::steady_clock::now() + period;
    while (!stop_.load()) {
        if (!started_.load()) return;
        sleepUntilDeadline(stop_, next);
        next += period;
        try {
            cleanExpiredMsgOnce(UtilAll::currentTimeMillis());
        } catch (const std::exception& e) {
            // The scheduling shell swallows every throwable: one failing sweep round must not kill the schedule,
            // otherwise a single bad timestamp would leave this consumer without any sweeping for the rest of its life.
            logger_error(std::string("scheduleAtFixedRate cleanExpireMsg exception: ") + e.what());
        }
    }
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
    // Return the assignment set computed by the real rebalance (what doRebalance writes into assignedQueues_).
    // Not "all queues of the subscribed topic" anymore - that was the root of duplicate consumption across group instances.
    std::lock_guard<std::mutex> lk(lock_);
    return assignedQueues_;
}

// ---------------------------------------------------------------- Real rebalance
void DefaultMQPushConsumer::doRebalance() {
    // rebalanceByTopic: BROADCASTING keeps every queue; CLUSTERING asks the broker for the
    // consumer id list -> sorts it -> runs the allocation strategy -> takes this instance's share.
    // No consumer list => keep the current assignment (debug log only), never fall back to "own every queue": that means duplicate consumption across group instances.
    //
    // NOTE: POP queues also come from this SAME client-side rebalance (one POP loop + acks per queue).
    // Broker-side allocation is another path (QUERY_ASSIGNMENT = 400, answering MessageQueueAssignment
    // with mode=POP) and is not implemented here: the semantics are equivalent, only "who decides the queue set" differs.
    // Read this paragraph before changing anything here.
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
            // Both mqAll and cidAll are sorted: allocation happens only after sorting, otherwise different instances compute different results
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
            // When a custom strategy throws: KEEP THE CURRENT ASSIGNMENT and end this rebalance round
            // (catch the error, log it, give up). Never revoke the topic's queues because of a failing strategy.
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
    // Newly assigned queues resolve their initial offset IMMEDIATELY into offsetTable_ (same contract as
    // updateProcessQueueTableInRebalance -> computePullFromWhere -> persisting it through the offset store).
    // It must not be resolved lazily at the first pull: CONSUME_FROM_LAST_OFFSET means "the max offset at
    // assignment time", and lazy resolution would skip messages produced between assignment and the first pull.
    for (const MessageQueue& mq : assigned) {
        std::string key = offsetKey(mq);
        if (was.count(key)) {
            continue;
        }
        {
            std::lock_guard<std::mutex> lk(lock_);
            if (offsetTable_.count(key)) continue;
            // sanity: still subscribed to this topic (no offset semantics attached to
            // the subscription itself -- see resolveInitialOffset)
            if (subscriptionData_.find(mq.topic) == subscriptionData_.end()) continue;
        }
        try {
            int64_t off = resolveInitialOffset(mq);
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
    // Taken from the SUBSCRIBE side of the route (read queues + readQueueNums, no master filtering),
    // not the publish side - the two differ on a perm=4 read-only topic and on "master down, only slaves
    // left", and the consumer must use the wider one (the rebalance queue set has always been subscribe
    // info; publish info only serves a producer picking a queue).
    if (mqClient_ == nullptr) return {};
    std::vector<MessageQueue> queues = mqClient_->getTopicSubscribeInfo(topic);
    if (queues.empty()) {
        logger_debug("rebalance: no subscribe info for topic " + topic);
    }
    return queues;
}

bool DefaultMQPushConsumer::ownsQueue(const std::string& key, uint64_t token) const {
    // Whether this pull thread still owns the queue (invalid once rebalance revokes it or swaps the thread).
    // Comparing tokens instead of std::thread ids: ownership is registered BEFORE the thread starts, and a
    // thread cannot obtain its own std::thread handle, so there is no "run first, register later" window.
    std::lock_guard<std::mutex> lk(lock_);
    auto it = pullOwners_.find(key);
    return it != pullOwners_.end() && it->second == token;
}

void DefaultMQPushConsumer::onQueuesRevoked(
    const std::vector<std::pair<MessageQueue, int64_t>>& revoked) {
    // Same contract as removeUnnecessaryMessageQueue
    if (messageModel_ == MessageModel::BROADCASTING) {
        // Broadcasting stores offsets only locally. A revoked offset must be written back to the file together
        // with its queue info (persist first, then drop it), or a rebuild cannot see the old offset and rescans from consumeFromWhere.
        saveLocalOffsets(revoked);
        return;
    }
    if (mqClient_ == nullptr) return;
    std::vector<MessageQueue> toUnlock;
    for (const auto& kv : revoked) {
        const MessageQueue& mq = kv.first;
        int64_t off = kv.second;
        // Only this queue's consumed offset: one UPDATE_CONSUMER_OFFSET(15); the info log keeps a single summary
        if (off >= 0) {
            try {
                mqClient_->updateConsumerOffset(consumerGroup_, mq, off);
            } catch (const std::exception& e) {
                logger_debug("persist offset on revoke failed for " + mq.toString() + ": "
                             + e.what());
            }
        }
        // The per-queue unlock during revoke belongs to classic orderly consumption only (the queue
        // comes from the classic pull-side process queue table
        // - in POP mode that table is empty, so this path never runs there).
        if (isOrderly() && !popMode_) {
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

// Drop every piece of local state for a queue (the caller must hold lock_); the consumed offset is handed
// to the caller through retired for persistence outside the lock. Two call sites: rebalance revoking a
// queue (including self-healing of a stalled one) and the OFFSET_ILLEGAL correction (drop + remove).
//
// Withdrawing ownership is the key step for the latter: once the key is gone from pullOwners_, the old
// pull thread fails ownsQueue on its next round and exits; even if the same queue is reassigned to this
// instance immediately, it gets a NEW token and a NEW thread, so the old loop can never re-inject messages pulled at its old offset.
void DefaultMQPushConsumer::retireQueueLocked(
    const std::string& key, const MessageQueue& fallbackMq,
    std::vector<std::pair<MessageQueue, int64_t>>& retired) {
    MessageQueue mq;
    auto mit = mqMap_.find(key);
    if (mit != mqMap_.end()) {
        mq = mit->second;
    } else if (!fallbackMq.topic.empty()) {
        // A stalled queue may never have pulled anything (no mqMap_ entry yet), but its consumed offset is real,
        // and skipping the persist would make the new owner redeliver everything from the start.
        mq = fallbackMq;
    }
    int64_t off = -1;
    auto oit = consumeOffsetTable_.find(key);
    if (oit != consumeOffsetTable_.end()) off = oit->second;
    if (!mq.topic.empty()) retired.emplace_back(mq, off);
    auto tit = pullThreads_.find(key);
    if (tit != pullThreads_.end()) {
        retiredThreads_.push_back(std::move(tit->second));
        pullThreads_.erase(tit);
    }
    pullOwners_.erase(key);
    lastPullAt_.erase(key);
    mqMap_.erase(key);
    pending_.erase(key);
    // The queue is off the process-queue tables, so the sweep will not touch it again - the in-flight
    // registry goes too, otherwise stale copies would stay visible in the sweep's union view.
    inflightMsgs_.erase(key);
    lockOk_.erase(key);
    offsetTable_.erase(key);
    consumeOffsetTable_.erase(key);
    // Epoch +1 makes every in-flight batch's acks invalid (equivalent to marking the ProcessQueue
    // dropped; the ack path rejects on isDropped). The freeze flag STAYS
    // until the queue is rebuilt (see the unfreeze branch in rebalancePullThreads), otherwise a corrected offset gets overwritten by a stale ack.
    ++queueEpoch_[key];
    // POP: mark dropped so in-flight batches neither consume nor ack (the broker revives them)
    auto pqit = popQueues_.find(key);
    if (pqit != popQueues_.end()) {
        pqit->second->setDropped(true);
        popQueues_.erase(pqit);
    }
}

uint64_t DefaultMQPushConsumer::queueEpochLocked(const std::string& key) const {
    auto it = queueEpoch_.find(key);
    return it == queueEpoch_.end() ? 0 : it->second;
}

uint64_t DefaultMQPushConsumer::queueEpoch(const std::string& key) const {
    std::lock_guard<std::mutex> lk(lock_);
    return queueEpochLocked(key);
}

bool DefaultMQPushConsumer::offsetFrozen(const std::string& key) const {
    std::lock_guard<std::mutex> lk(lock_);
    return frozenOffsets_.count(key) > 0;
}

// The OFFSET_ILLEGAL correction branch. Must be called on a path that does NOT hold lock_
// (it performs network RPCs internally).
//
// The broker says "the requested offset is illegal" (queue truncated / commitlog expired / a
// one-shot server-side reset - all three answer PULL_OFFSET_MOVED, which the remoting layer
// maps to this status) and returns the corrected value in nextBeginOffset. The fix is more than
// moving the cursor: every message already taken from this queue but not consumed or acked is
// voided - they sit inside the skipped range, acking them would push the offset back to an illegal
// value and not acking them would strand it, so the batch is dropped and the new loop restarts at the corrected offset.
//
// Four-step contract: set the next offset -> mark the ProcessQueue dropped ->
// { updateAndFreezeOffset; persist; remove the queue } -> immediate rebalance.
// Here: rewrite + freeze the offset (updateAndFreezeOffset is an overwrite, and from then on acks and
// empty-response corrections are all void) -> retireQueueLocked (the drop, epoch +1) -> onQueuesRevoked
// persists outside the lock (removing the queue would persist once anyway)
// -> wake rebalance so the queue is rebuilt at the corrected offset.
void DefaultMQPushConsumer::handleOffsetIllegal(const std::string& key, int64_t nextOffset) {
    std::vector<std::pair<MessageQueue, int64_t>> retired;
    {
        std::lock_guard<std::mutex> lk(lock_);
        // updateAndFreezeOffset: the offset OVERWRITES to the broker's corrected value (not increaseOnly)
        // and freezes it (allowToUpdate=false). The freeze must precede the queue revoke,
        // otherwise a stale ack could write the offset back in between the two steps.
        if (nextOffset >= 0) {
            consumeOffsetTable_[key] = nextOffset;
        }
        frozenOffsets_.insert(key);
        retireQueueLocked(key, MessageQueue(), retired);
    }
    // persist: push the corrected offset to the broker immediately (an explicit persist that does not
    // wait for the periodic flush). This is the branch's only observable proof of an immediate flush: without
    // it the broker keeps the illegal value until the first periodic persist after the queue is rebuilt.
    onQueuesRevoked(retired);
    wakeRebalanceLoop();
    logger_warn("the pull request offset illegal, fix it, queue=" + key);
}

void DefaultMQPushConsumer::wakeRebalanceLoop() {
    // The broker signalled that the consumer group's membership changed -> recompute immediately.
    rebalanceNow_.store(true);
    rebalanceCv_.notify_all();
}

void DefaultMQPushConsumer::resetRetryTopicAndNamespace(std::vector<MessageExt>& msgs) {
    // resetRetryAndNamespace (called before dispatch).
    // A redelivered message's topic becomes the original business topic again and the namespace prefix is stripped.
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

int64_t DefaultMQPushConsumer::resolveInitialOffset(const MessageQueue& mq) {
    MQClientInstance& c = client();
    // Java parity (RebalancePushImpl#computePullFromWhereWithException): the initial
    // offset NEVER depends on the subscription type. An earlier draft short-circuited
    // SQL92 subscriptions to the max offset, which silently skipped every message
    // produced *before* the consumer started (send-first + CONSUME_FROM_FIRST_OFFSET
    // received nothing; consumer-first only passed because the queues were empty and
    // the max offset was 0 anyway). Java reads the stored offset and, when absent,
    // uses 0 for FIRST_OFFSET / max for LAST_OFFSET etc. -- the broker applies the
    // SQL92 filter to whatever range the pull covers.
    if (messageModel_ == MessageModel::BROADCASTING) {
        // Broadcasting: the offset lives only in the local offset file
        std::map<std::string, int64_t> stored = loadLocalOffsets();
        auto it = stored.find(offsetKey(mq));
        if (it != stored.end()) {
            return it->second;
        }
        if (consumeFromWhere_ == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET) {
            // FIRST_OFFSET with no local offset returns 0 directly (the local-file and broker paths share
            // this branch); the real start point is settled by the OFFSET_ILLEGAL correction using the
            // nextBeginOffset the broker returns.
            return 0;
        }
        return c.getMaxOffset(mq);
    }
    // Clustering: first query the committed offset on the broker (remote offset store)
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
            // FIRST_OFFSET returns 0 directly, with the offset "corrected by the OFFSET_ILLEGAL process":
            // NO minOffset query is sent. The cost of that extra call is more than latency:
            // minOffset only accepts a master (the admin path), so while the master is down a fresh
            // consumer would pull nothing at all - whereas a slave can start from 0 and, on overrun, the
            // broker corrects the offset back with PULL_OFFSET_MOVED (see handleOffsetIllegal).
            return 0;
        }
        if (consumeFromWhere_ == ConsumeFromWhere::CONSUME_FROM_TIMESTAMP) {
            int64_t ts = UtilAll::currentTimeMillis() - 30 * 60 * 1000LL;
            return c.searchOffsetByTimestamp(mq, ts);
        }
        // On first consumption with no committed offset, the %RETRY% topic starts at 0
        // (redelivery must replay everything) instead of jumping to the max offset.
        if (MixAll::isRetryTopic(mq.topic)) {
            return 0;
        }
        return c.getMaxOffset(mq);  // default CONSUME_FROM_LAST_OFFSET
    } catch (const std::exception&) {
        return 0;
    }
}

// ---------------------------------------------------------------- Heartbeat
int32_t DefaultMQPushConsumer::sendHeartbeatToAllBroker() {
    if (mqClient_ == nullptr) {
        return 0;
    }
    std::vector<std::string> addrs;
    try {
        // Send to every broker (master AND slave): iterate all brokerIds of the address table and skip one
        // only when the heartbeat carries no ConsumerData and the id is not MASTER_ID; a consumer heartbeat
        // always carries ConsumerData, so slaves are not skipped. Each broker keeps its own consumer
        // manager, and a slave that never heard the heartbeat answers a pull pointed at it with
        // SUBSCRIPTION_NOT_EXIST (pulls sent with the default postSubscriptionWhenPull=false carry no
        // subscription flag, so that is exactly the path taken). Producer heartbeats are the opposite: they
        // carry only ProducerData (consumerEmpty); see knownBrokerAddrs in DefaultMQProducer::sendHeartbeatToAllBroker.
        addrs = mqClient_->getAllBrokerAddrs();
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
    // The heartbeat's ConsumerData carries unitMode (the setUnitMode semantics):
    // the broker uses it to decide whether the %RETRY% topic it creates gets the UNIT_SUB flag
    cd.unitMode = unitMode_;
    {
        std::lock_guard<std::mutex> lk(lock_);
        for (const auto& kv : subscriptionData_) {
            cd.addSubscriptionData(kv.second);
        }
    }
    // Keep fingerprint = 0 so the broker takes the V1 registration path (registers the full subscriptionDataSet);
    // withoutSub is only read on the V2 path (fingerprint != 0), so its default false is fine here.
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

// ---------------------------------------------------------------- Administration
std::vector<MessageQueue> DefaultMQPushConsumer::fetchSubscribeMessageQueues(
    const std::string& topic) {
    // Reads the subscribe side of the route (every queue, no master filtering), the same source
    // rebalance uses, not the publish side.
    MQClientInstance& c = client();
    return c.getTopicSubscribeInfo(topic);
}

int32_t DefaultMQPushConsumer::maxReconsumeTimesOrDefault() const {
    // -1 means the broker default retryMaxTimes = 16
    return maxReconsumeTimes_ == -1 ? 16 : maxReconsumeTimes_;
}

// When the send-back request fails, resend the message to %RETRY%group as an ordinary message
// and let the broker redeliver it by delay level; see buildRetryMessage for the property setup.
void DefaultMQPushConsumer::sendMessageBackAsNormalMessage(const MessageExt& msg) {
    MQClientInstance& c = client();
    Message newMsg = buildRetryMessage(msg, maxReconsumeTimesOrDefault());
    std::shared_ptr<TopicPublishInfo> publish =
        c.getTopicPublishInfo(newMsg.topic, /*isDefault=*/true);
    MessageQueue selected = publish->selectOneMessageQueue();
    // This goes through the INTERNAL producer of MQClientInstance, which had
    // resetClientConfig(clientConfig) applied when it was built, so its unitMode matches this consumer's -
    // it must be passed here, otherwise the redelivered message loses its unit marker.
    c.sendMessage(MixAll::CLIENT_INNER_PRODUCER_GROUP, newMsg, selected, 3000, /*sysFlag=*/0,
                  unitMode_);
}

// The newMsg construction of the plain and the orderly send-back paths is identical line by line
// (only maxReconsumeTimes uses each path's own getter), so it is one function to stop the two property setups drifting apart.
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
    // A half-message redelivery must not carry TRAN_MSG, otherwise the broker handles it as a check-back message again
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
    // Orderly messages stay in the local queue and retry in place, so the broker counts no redelivery at all
    // and the default should be retry-until-success - here -1 reads as the int32 max value. NOT the
    // concurrent side's 16 (that path sends every round back through the broker, and 16 is the broker's
    // retryMaxTimes); one shared constant would fabricate dead letters for orderly consume or make concurrent messages redeliver forever.
    return maxReconsumeTimes_ == -1 ? std::numeric_limits<int32_t>::max() : maxReconsumeTimes_;
}

bool DefaultMQPushConsumer::orderlySendMessageBack(const MessageExt& msg) {
    // The whole function is try/catch wrapped and reports the outcome only through its return value: an
    // exception reaching the consume thread leaves the message neither acked nor sent back, waiting for a broker lock timeout - minutes for orderly consume, and it looks like a hang.
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

void DefaultMQPushConsumer::requeuePending(const std::string& key, const std::vector<MessageExt>& msgs) {
    std::lock_guard<std::mutex> lk(lock_);
    auto it = pending_.find(key);
    if (it == pending_.end()) return;
    for (auto rit = msgs.rbegin(); rit != msgs.rend(); ++rit) {
        it->second.push_front(*rit);
    }
}

void DefaultMQPushConsumer::recordConsumeRt(const std::string& topic, int64_t beginMs) {
    if (!mqClient_) return;
    mqClient_->consumerStats().incConsumeRT(consumerGroup_, topic,
                                            UtilAll::currentTimeMillis() - beginMs);
}

int32_t DefaultMQPushConsumer::orderlySuspendMillis(const ConsumeOrderlyContext& ctx) const {
    int32_t ms = ctx.suspendCurrentQueueTimeMillis;
    if (ms == -1) ms = suspendCurrentQueueTimeMillis_;
    if (ms < 10) return 10;
    if (ms > 30000) return 30000;
    return ms;
}

bool DefaultMQPushConsumer::checkOrderlyReconsumeTimes(std::vector<MessageExt>& msgs) {
    // Per message there are two paths:
    //   - retries left: bump reconsumeTimes locally (the broker did not record this failure, so without
    //     the client's increment the threshold would never be reached) and keep the queue suspended;
    //   - retries used up: hand the message to the broker send-back. A SUCCESSFUL send-back does not
    //     suspend (success commits the offset, the poison message steps aside, the queue moves on); only a failure bumps +1 and suspends.
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
    // The whole send-back is wrapped in try/catch and degrades to "resend as a normal message" on failure,
    // never letting an exception reach the consume thread (that would leave the message neither acked nor
    // sent back, waiting for a timeout redelivery). NOTE: client() throws before start(), so it must stay
    // inside the try - this function only reports the outcome through its return value, and the caller (sendBackBatch) no longer looks at exceptions.
    try {
        MQClientInstance& c = client();
        // Uses findBrokerAddressInPublish (master only); when it returns nothing the error is
        // "Broker[X] master node does not exist" (the contract wording callers match on);
        // slaves do not accept CONSUMER_SEND_MSG_BACK.
        std::string addr = c.findBrokerAddressInPublish(brokerName);
        if (addr.empty()) {
            throw MQClientException("Broker[" + brokerName + "] master node does not exist");
        }
        auto header = std::make_shared<ConsumerSendMsgBackRequestHeader>();
        header->offset = msg.commitLogOffset;
        header->group = consumerGroup_;
        header->delayLevel = delayLevel;
        header->originMsgId = msg.msgId;
        header->originTopic = msg.topic;
        // NOTE: unitMode is reported faithfully here. A common shortcut fills only
        // group/offset/delayLevel/originMsgId/originTopic/maxReconsumeTimes/brokerName and
        // never writes unitMode, leaving the field permanently false. The broker does read it
        // (it builds the system flag from it, buildSysFlag(false, true)), so reporting the consumer's
        // configuration is what makes a unit-scoped retry topic carry the UNIT_SUB marker.
        header->unitMode = unitMode_;
        header->maxReconsumeTimes = maxReconsumeTimesOrDefault();
        RemotingCommand request =
            RemotingCommand::createRequestCommand(RequestCode::CONSUMER_SEND_MSG_BACK, header);
        // CONSUMER_SEND_MSG_BACK is a broker write the VIP switch rewrites; applied per request at
        // this invoke boundary, the shifted address is never cached.
        RemotingCommand response = c.remotingClient().invokeSync(
            MixAll::brokerVipChannel(c.isVipChannelEnabled(), addr), request, 5000);
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
    // The keys of the queues currently assigned to this instance (used by live-cluster checks that two
    // instances of one group neither overlap nor miss queues). Key format matches offsetKey: topic + brokerName + queueId.
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
    // Query every clientId of the consumer group on a topic, used to verify multi-instance registration.
    if (mqClient_ == nullptr) return {};
    try {
        return mqClient_->getConsumerIdListByGroup(topic, consumerGroup_);
    } catch (...) {
        return {};
    }
}

}  // namespace rocketmq
