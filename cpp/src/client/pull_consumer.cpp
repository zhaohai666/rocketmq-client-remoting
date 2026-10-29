#include "rocketmq/client/pull_consumer.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <map>
#include <utility>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/validators.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

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

std::string localOffsetFilePath(const std::string& clientId, const std::string& group) {
    std::string base = UtilAll::userHome();
    if (base.empty()) base = ".";
    return base + "/.rocketmq_offsets/" + (clientId.empty() ? "DEFAULT" : clientId) + "/"
           + group + "/offsets.json";
}

int64_t extInt(const RemotingCommand& r, const std::string& key, int64_t fallback) {
    auto it = r.extFields.find(key);
    if (it == r.extFields.end() || it->second.empty()) return fallback;
    try {
        return std::stoll(it->second);
    } catch (...) {
        return fallback;
    }
}

}  // namespace

DefaultMQPullConsumer::DefaultMQPullConsumer(const std::string& consumerGroup) {
    // First guard, same as in the push and lite-pull consumer constructors: rejects blank group
    // names only. Full validation (length / character set / DEFAULT_CONSUMER reserved name) runs in
    // start() via Validators::checkGroup. empty() let all-blank names pass, hence UtilAll::isBlank.
    if (UtilAll::isBlank(consumerGroup)) {
        throw MQClientException("consumerGroup is empty");
    }
    consumerGroup_ = consumerGroup;
}

DefaultMQPullConsumer::~DefaultMQPullConsumer() {
    try {
        shutdown();
    } catch (...) {
    }
}

// ---------------------------------------------------------------- Configuration
void DefaultMQPullConsumer::setNamesrvAddr(const std::string& addr) {
    nameServerAddrs_ = splitSemicolon(addr);
}

void DefaultMQPullConsumer::setNameServerAddresses(const std::vector<std::string>& addrs) {
    nameServerAddrs_ = addrs;
}

void DefaultMQPullConsumer::registerMessageQueueListener(
    const std::string& topic, std::shared_ptr<MessageQueueListener> listener) {
    if (topic.empty()) return;
    registerTopics_.insert(topic);
    if (listener != nullptr) {
        std::lock_guard<std::mutex> lk(assignmentNotifyMutex_);
        messageQueueListeners_[topic] = std::move(listener);
    }
}

// ---------------------------------------------------------------- Lifecycle
void DefaultMQPullConsumer::start() {
    if (started_) return;
    if (!namespace_.empty()) {
        consumerGroup_ = NamespaceUtil::wrapNamespace(namespace_, consumerGroup_);
    }
    Validators::checkGroup(consumerGroup_);
    if (consumerGroup_ == MixAll::DEFAULT_CONSUMER_GROUP) {
        throw MQClientException(
            "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");
    }
    if (nameServerAddrs_.empty()) {
        throw MQClientException("name server address is not set");
    }
    if (!allocateStrategy_) {
        throw MQClientException("allocateMessageQueueStrategy is null");
    }
    if (messageModel_ == MessageModel::CLUSTERING) {
        instanceName_ = changeInstanceNameToPID(instanceName_);
    }
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_, unitName_, enableStreamRequestType_);
    }
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                        /*connectTimeoutMillis=*/3000,
                                        consumerPullTimeoutMillis_,
                                        MQClientInstance::tlsEnabledFromEnv(), unitName_,
                                        pollNameServerIntervalMillis_));
    // Broker VIP channel: forwarded to the instance that owns the broker RPCs (off = plain address).
    mqClient_->setVipChannelEnabled(vipChannelEnabled_);
    mqClient_->setDecodeReadBody(decodeReadBody_);
    mqClient_->setMqClientApiTimeout(mqClientApiTimeout_);
    mqClient_->setEnableConcurrentHeartbeat(enableConcurrentHeartbeat_);
    mqClient_->setConcurrentHeartbeatThreadPoolSize(concurrentHeartbeatThreadPoolSize_);
    std::shared_ptr<RPCHook> requestHook =
        composeRequestHooks(enableStreamRequestType_, rpcHook_,
                            [this] { return namespaceV2_; });
    if (requestHook && !mqClient_->registerRPCHook(requestHook)) {
        logger_warn("pull consumer rpc hook ignored: MQClientInstance already has one (clientId="
                    + clientId_ + ")");
    }
    mqClient_->start();
    createAsyncExecutors();
    refreshRouteForHeartbeat();
    started_ = true;
    try {
        sendHeartbeatToAllBroker();
    } catch (const std::exception& e) {
        logger_debug(std::string("initial heartbeat failed: ") + e.what());
    }
    startHeartbeatLoop();
}

void DefaultMQPullConsumer::shutdown() {
    if (!started_) return;
    // Detach the pullAsync pools first: a late pullAsync then throws instead of enqueueing into a
    // dying pool, while already-queued rounds still run to completion.
    // ⚠ started_ must flip only **after** the queues drain -- an in-flight round still calls pull(),
    //   which needs mqClient_; flipping early would turn queued pulls into spurious onException calls.
    std::shared_ptr<ConsumeExecutor> asyncPool;
    std::shared_ptr<ConsumeExecutor> callbackPool;
    {
        std::lock_guard<std::mutex> lk(asyncLock_);
        asyncPool.swap(asyncPullExecutor_);
        callbackPool.swap(callbackExecutor_);
    }
    if (asyncPool) {
        asyncPool->shutdown(true);
    }
    if (callbackPool) {
        callbackPool->shutdown(true);
    }
    started_ = false;
    heartbeatStop_.store(true);
    heartbeatCv_.notify_all();
    if (heartbeatThread_.joinable()) heartbeatThread_.join();
    if (mqClient_ != nullptr) {
        try {
            mqClient_->unregisterClientAllBrokers(clientId_, "", consumerGroup_);
        } catch (const std::exception& e) {
            logger_debug(std::string("unregister on shutdown failed: ") + e.what());
        }
        mqClient_->shutdown();
        mqClient_.reset();
    }
}

// ---------------------------------------------------------------- Queues
std::vector<MessageQueue> DefaultMQPullConsumer::fetchSubscribeMessageQueues(
    const std::string& topic) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    const std::string realTopic = NamespaceUtil::wrapNamespace(namespace_, topic);
    std::shared_ptr<TopicRouteData> route = mqClient_->getTopicRouteData(realTopic);
    if (route == nullptr) {
        throw MQClientException("the topic[" + topic + "] not exist");
    }
    return route->getAllSubscribeMessageQueue(realTopic);
}

// ---------------------------------------------------------------- Balanced queue view
std::vector<MessageQueue> DefaultMQPullConsumer::fetchMessageQueuesInBalance(
    const std::string& topic) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    const std::string realTopic = NamespaceUtil::wrapNamespace(namespace_, topic);

    // Current assignment: queues of the topic pulled locally (keys written by pull(), topic is already realTopic).
    std::vector<MessageQueue> pulled;
    {
        // pullAsync workers write the table concurrently with this read.
        std::lock_guard<std::mutex> lk(pullFromWhichNodeMutex_);
        for (const auto& kv : pullFromWhichNode_) {
            if (kv.first.topic == realTopic) {
                pulled.push_back(MessageQueue(NamespaceUtil::withoutNamespace(kv.first.topic,
                                                                              namespace_),
                                              kv.first.brokerName, kv.first.queueId));
            }
        }
    }
    std::sort(pulled.begin(), pulled.end());

    std::vector<MessageQueue> allocated;
    bool computed = false;
    std::vector<MessageQueue> allOfTopic;
    std::shared_ptr<TopicRouteData> route = mqClient_->getTopicRouteData(realTopic);
    if (route != nullptr) {
        std::vector<MessageQueue> mqAll = route->getAllSubscribeMessageQueue(realTopic);
        std::sort(mqAll.begin(), mqAll.end());
        for (const MessageQueue& mq : mqAll) {
            allOfTopic.push_back(MessageQueue(NamespaceUtil::withoutNamespace(mq.topic, namespace_),
                                              mq.brokerName, mq.queueId));
        }
        if (messageModel_ == MessageModel::BROADCASTING) {
            allocated = mqAll;
            computed = true;
        } else if (!mqAll.empty()) {
            std::vector<std::string> cidAll =
                mqClient_->getConsumerIdListByGroup(realTopic, consumerGroup_);
            if (!cidAll.empty()) {
                std::sort(cidAll.begin(), cidAll.end());
                try {
                    allocated = allocateStrategy_->allocate(consumerGroup_, clientId_, mqAll, cidAll);
                    computed = true;
                } catch (const std::exception& e) {
                    logger_error("allocate message queue exception. strategy name: "
                                 + allocateStrategy_->getName() + ", ex: " + e.what());
                }
            }
        }
    }
    if (!computed) {
        logger_debug("fetchMessageQueuesInBalance: no route/consumer list for " + consumerGroup_
                     + "/" + topic + ", keep current assignment");
        // The assignment could not be recomputed, so nothing moved and the listener stays quiet --
        // unless the pull threads' own view differs from the one last handed out.
        notifyAssignmentChanged(topic, allOfTopic, pulled);
        return pulled;
    }

    std::vector<MessageQueue> result;
    for (const MessageQueue& mq : allocated) {
        if (mq.topic != realTopic) continue;
        result.push_back(
            MessageQueue(NamespaceUtil::withoutNamespace(mq.topic, namespace_), mq.brokerName,
                         mq.queueId));
    }
    std::sort(result.begin(), result.end());
    notifyAssignmentChanged(topic, allOfTopic, result);
    return result;
}

bool DefaultMQPullConsumer::assignmentChanged(const std::vector<MessageQueue>& before,
                                              const std::vector<MessageQueue>& after) {
    // A queue set, so a re-ordering of the same queues is not a change and a same-count swap to
    // another broker is one.
    return std::set<MessageQueue>(before.begin(), before.end())
        != std::set<MessageQueue>(after.begin(), after.end());
}

bool DefaultMQPullConsumer::notifyAssignmentChanged(
    const std::string& topic, const std::vector<MessageQueue>& all,
    const std::vector<MessageQueue>& divided) {
    bool changed = false;
    {
        std::lock_guard<std::mutex> lk(assignmentNotifyMutex_);
        auto it = lastBalancedQueues_.find(topic);
        if (it == lastBalancedQueues_.end()) {
            // The first view only reports a change when this consumer actually owns queues: an empty
            // opening has nothing for the application to react to.
            changed = assignmentChanged(std::vector<MessageQueue>(), divided) && !divided.empty();
        } else {
            changed = assignmentChanged(it->second, divided);
        }
        lastBalancedQueues_[topic] = divided;
    }
    if (!changed) {
        return false;
    }
    std::shared_ptr<MessageQueueListener> perTopic;
    std::shared_ptr<MessageQueueListener> global;
    {
        std::lock_guard<std::mutex> lk(assignmentNotifyMutex_);
        auto it = messageQueueListeners_.find(topic);
        if (it != messageQueueListeners_.end()) {
            perTopic = it->second;
        }
        global = messageQueueListener_;
    }
    if (perTopic != nullptr) {
        try {
            perTopic->messageQueueChanged(topic, all, divided);
        } catch (const std::exception& e) {
            logger_error("message queue listener failed for " + topic + ": " + e.what());
        } catch (...) {
            logger_error("message queue listener failed for " + topic);
        }
    }
    // The single listener is a different sink from the topic-keyed one; it is skipped only when the
    // application handed the same object to both.
    if (global != nullptr && global != perTopic) {
        try {
            global->messageQueueChanged(topic, all, divided);
        } catch (const std::exception& e) {
            logger_error("message queue listener failed for " + topic + ": " + e.what());
        } catch (...) {
            logger_error("message queue listener failed for " + topic);
        }
    }
    return true;
}

// ---------------------------------------------------------------- Pull
PullResult DefaultMQPullConsumer::pull(const MessageQueue& mq, const std::string& subExpression,
                                      int64_t offset, int32_t maxNums, int32_t timeoutMillis) {
    return pull(mq, MessageSelector::byTag(subExpression), offset, maxNums, timeoutMillis);
}

PullResult DefaultMQPullConsumer::pull(const MessageQueue& mq, const MessageSelector& selector,
                                      int64_t offset, int32_t maxNums, int32_t timeoutMillis) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    const int32_t timeout = timeoutMillis > 0 ? timeoutMillis : consumerPullTimeoutMillis_;
    MessageQueue real = mq;
    real.topic = NamespaceUtil::wrapNamespace(namespace_, mq.topic);
    const SubscriptionData sub =
        FilterAPI::buildSubscriptionData(real.topic, selector.expression, selector.type);
    // The class-filter bit stays clear on this path: it asks the client to re-route the pull to a
    // legacy filter server, which this SDK does not speak. A CLASS_FILTER subscription is still
    // reported by its own expression type in the header.
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/false,
                                                      /*suspend=*/false,
                                                      /*subscription=*/true,
                                                      /*classFilter=*/false);
    PullResult result = mqClient_->pullMessage(
        consumerGroup_, real, offset, maxNums, sysFlag, /*commitOffset=*/0,
        FilterAPI::pullExpression(sub), FilterAPI::pullSubVersionOf(sub), sub.expressionType,
        timeout, /*maxMsgBytes=*/-1,
        /*suspendTimeoutMillis=*/15000, /*addr=*/std::string(), /*requestSource=*/0,
        /*brokerId=*/recalculatePullFromWhichNode(real));
    updatePullFromWhichNode(real, result);
    return result;
}

PullResult DefaultMQPullConsumer::pullBlockIfNotFound(const MessageQueue& mq,
                                                      const std::string& subExpression,
                                                      int64_t offset, int32_t maxNums) {
    return pullBlockIfNotFound(mq, MessageSelector::byTag(subExpression), offset, maxNums);
}

PullResult DefaultMQPullConsumer::pullBlockIfNotFound(const MessageQueue& mq,
                                                      const MessageSelector& selector,
                                                      int64_t offset, int32_t maxNums) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    MessageQueue real = mq;
    real.topic = NamespaceUtil::wrapNamespace(namespace_, mq.topic);
    const SubscriptionData sub =
        FilterAPI::buildSubscriptionData(real.topic, selector.expression, selector.type);
    // Same class-filter rule as the short-poll path above: the bit stays clear, the type travels.
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/false,
                                                      /*suspend=*/true,
                                                      /*subscription=*/true,
                                                      /*classFilter=*/false);
    PullResult result = mqClient_->pullMessage(
        consumerGroup_, real, offset, maxNums, sysFlag, /*commitOffset=*/0,
        FilterAPI::pullExpression(sub), FilterAPI::pullSubVersionOf(sub), sub.expressionType,
        consumerTimeoutMillisWhenSuspend_,
        /*maxMsgBytes=*/-1, brokerSuspendMaxTimeMillis_, /*addr=*/std::string(),
        /*requestSource=*/0, /*brokerId=*/recalculatePullFromWhichNode(real));
    updatePullFromWhichNode(real, result);
    return result;
}

int64_t DefaultMQPullConsumer::recalculatePullFromWhichNode(const MessageQueue& mq) const {
    std::lock_guard<std::mutex> lk(pullFromWhichNodeMutex_);
    auto it = pullFromWhichNode_.find(mq);
    return it != pullFromWhichNode_.end() ? it->second : MixAll::MASTER_ID;
}

void DefaultMQPullConsumer::updatePullFromWhichNode(const MessageQueue& mq,
                                                    const PullResult& result) {
    std::lock_guard<std::mutex> lk(pullFromWhichNodeMutex_);
    pullFromWhichNode_[mq] = result.suggestWhichBrokerId.value_or(MixAll::MASTER_ID);
}

// ---------------------------------------------------------------- Async pull
// Producer-shaped two pools: PullAsyncExecutor runs the (synchronous-kernel) pull off the caller thread,
// PullCallbackExecutor delivers the user callback so a slow onSuccess cannot block the pull workers.
void DefaultMQPullConsumer::createAsyncExecutors() {
    int32_t cores = static_cast<int32_t>(std::thread::hardware_concurrency());
    if (cores <= 0) {
        cores = 1;  // never build a pool that cannot run tasks
    }
    std::lock_guard<std::mutex> lk(asyncLock_);
    asyncPullExecutor_ = std::make_shared<ConsumeExecutor>(
        cores, cores, 60.0, "PullAsyncExecutor", asyncPullQueueCapacity_, "_", 1);
    callbackExecutor_ = std::make_shared<ConsumeExecutor>(
        cores, cores, 60.0, "PullCallbackExecutor", 0, "_", 1);
}

void DefaultMQPullConsumer::executeOnCallbackThread(std::function<void()> fn) {
    std::shared_ptr<ConsumeExecutor> pool;
    {
        std::lock_guard<std::mutex> lk(asyncLock_);
        pool = callbackExecutor_;
    }
    if (pool == nullptr) {
        fn();
        return;
    }
    try {
        pool->submit(std::move(fn));
    } catch (const RejectedExecutionError&) {
        fn();
    }
}

void DefaultMQPullConsumer::pullAsync(const MessageQueue& mq, const std::string& subExpression,
                                      int64_t offset, int32_t maxNums,
                                      std::shared_ptr<PullCallback> callback,
                                      int32_t timeoutMillis) {
    pullAsync(mq, MessageSelector::byTag(subExpression), offset, maxNums, std::move(callback),
              timeoutMillis);
}

void DefaultMQPullConsumer::pullAsync(const MessageQueue& mq, const MessageSelector& selector,
                                      int64_t offset, int32_t maxNums,
                                      std::shared_ptr<PullCallback> callback,
                                      int32_t timeoutMillis) {
    if (mqClient_ == nullptr || !started_) {
        throw MQClientException("consumer not started, call start() first");
    }
    if (callback == nullptr) {
        throw MQClientException("PullCallback is null");
    }
    std::shared_ptr<ConsumeExecutor> pool;
    {
        std::lock_guard<std::mutex> lk(asyncLock_);
        pool = asyncPullExecutor_;
    }
    if (pool == nullptr) {
        throw MQClientException("consumer already shutdown");
    }
    // Exactly-once latch: whichever path reaches the flag first delivers, every other path no-ops
    // (covers the submit/shutdown race where delivery falls back to inline).
    auto delivered = std::make_shared<std::atomic<bool>>(false);
    auto deliver = [callback, delivered](const PullResult* result, const std::exception_ptr* eptr) {
        if (delivered->exchange(true)) {
            return;
        }
        // A throwing user callback must not take down the pool thread (producer convention: swallowed + logged).
        try {
            if (result != nullptr) {
                callback->onSuccess(*result);
            } else {
                callback->onException(*eptr);
            }
        } catch (const std::exception& e) {
            logger_warn(std::string("pull callback raised: ") + e.what());
        } catch (...) {
            logger_warn("pull callback raised unknown exception");
        }
    };
    try {
        pool->submit([this, mq, selector, offset, maxNums, timeoutMillis, deliver]() {
            try {
                const PullResult result = pull(mq, selector, offset, maxNums, timeoutMillis);
                executeOnCallbackThread([deliver, result]() { deliver(&result, nullptr); });
            } catch (...) {
                const std::exception_ptr eptr = std::current_exception();
                executeOnCallbackThread([deliver, eptr]() { deliver(nullptr, &eptr); });
            }
        });
    } catch (const RejectedExecutionError&) {
        // The pool shut down concurrently: report it through the callback instead of dropping it silently.
        const std::exception_ptr eptr = std::make_exception_ptr(
            MQClientException("pullAsync rejected: consumer is shutting down"));
        deliver(nullptr, &eptr);
    }
}

// ---------------------------------------------------------------- Consumer offset management
bool DefaultMQPullConsumer::fetchConsumeOffset(const MessageQueue& mq, int64_t& outOffset) {
    if (messageModel_ == MessageModel::BROADCASTING) {
        const std::map<std::string, int64_t> table = DefaultMQPushConsumer::loadLocalOffsetsAt(
            localOffsetFilePath(clientId_, consumerGroup_));
        auto it = table.find(DefaultMQPushConsumer::offsetKey(mq));
        if (it == table.end()) return false;
        outOffset = it->second;
        return true;
    }
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    return mqClient_->queryConsumerOffset(consumerGroup_, mq, outOffset);
}

void DefaultMQPullConsumer::updateConsumeOffset(const MessageQueue& mq, int64_t offset) {
    if (messageModel_ == MessageModel::BROADCASTING) {
        const std::string key = DefaultMQPushConsumer::offsetKey(mq);
        DefaultMQPushConsumer::saveLocalOffsetsAt(localOffsetFilePath(clientId_, consumerGroup_),
                                                  {{key, offset}}, {{key, mq}});
        return;
    }
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    mqClient_->updateConsumerOffset(consumerGroup_, mq, offset);
}

int64_t DefaultMQPullConsumer::searchOffset(const MessageQueue& mq, int64_t timestamp) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    return mqClient_->searchOffsetByTimestamp(mq, timestamp);
}

int64_t DefaultMQPullConsumer::maxOffset(const MessageQueue& mq) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    return mqClient_->getMaxOffset(mq);
}

int64_t DefaultMQPullConsumer::minOffset(const MessageQueue& mq) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    return mqClient_->getMinOffset(mq);
}

int64_t DefaultMQPullConsumer::earliestMsgStoreTime(const MessageQueue& mq) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    const std::string addr = mqClient_->publishAddrFor(mq.brokerName, mq.topic);
    PropertyMap ext;
    ext["topic"] = mq.topic;
    ext["queueId"] = std::to_string(mq.queueId);
    ext["brokerName"] = mq.brokerName;
    // GET_EARLIEST_MSG_STORETIME belongs to the offset family the VIP switch rewrites; the shifted
    // address is computed here at the invoke boundary and never stored back into addr.
    const RemotingCommand response = mqClient_->invokeSync(
        MixAll::brokerVipChannel(mqClient_->isVipChannelEnabled(), addr),
        RequestCode::GET_EARLIEST_MSG_STORETIME, ext, Bytes(), false, 5000);
    return extInt(response, "timestamp", 0);
}

namespace {

// The retry copy a failed send-back degrades to: the original body and properties, re-addressed to
// %RETRY%group with the redelivery counters bumped and the delay gradient the retry topic expects.
Message buildRetryCopy(const std::string& consumerGroup, const MessageExt& msg,
                       int32_t maxReconsumeTimes) {
    Message newMsg(MixAll::getRetryTopic(consumerGroup), msg.body);
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
    // A redelivery must not carry TRAN_MSG, otherwise the broker handles it as a check-back message again
    newMsg.properties.erase(MessageConst::PROPERTY_TRANSACTION_PREPARED);
    newMsg.putProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL,
                       std::to_string(3 + msg.reconsumeTimes));
    if (newMsg.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX).empty()) {
        newMsg.putProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                           InnerIdGenerator::createUniqId());
    }
    return newMsg;
}

}  // namespace

void DefaultMQPullConsumer::sendMessageBack(const MessageExt& msg, int32_t delayLevel) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    const int32_t maxReconsume = maxReconsumeTimes_ == -1 ? 16 : maxReconsumeTimes_;
    std::exception_ptr original;
    try {
        const std::string addr = mqClient_->findBrokerAddressInPublish(msg.brokerName);
        if (addr.empty()) {
            throw MQClientException("Broker[" + msg.brokerName + "] master node does not exist");
        }
        auto header = std::make_shared<ConsumerSendMsgBackRequestHeader>();
        header->offset = msg.commitLogOffset;
        header->group = consumerGroup_;
        header->delayLevel = delayLevel;
        header->originMsgId = msg.msgId;
        header->originTopic = msg.topic;
        header->unitMode = unitMode_;
        header->maxReconsumeTimes = maxReconsume;
        RemotingCommand request =
            RemotingCommand::createRequestCommand(RequestCode::CONSUMER_SEND_MSG_BACK, header);
        // CONSUMER_SEND_MSG_BACK is a broker write the VIP switch rewrites; applied per request here,
        // the shifted address is never cached.
        const RemotingCommand response = mqClient_->remotingClient().invokeSync(
            MixAll::brokerVipChannel(mqClient_->isVipChannelEnabled(), addr), request, 3000);
        if (response.code != ResponseCode::SUCCESS) {
            throw MQBrokerException(response.code, response.remark);
        }
        return;
    } catch (const std::exception& e) {
        original = std::current_exception();
        logger_error("Failed to send message back, consumerGroup=" + consumerGroup_
                     + ", brokerName=" + msg.brokerName + ", msg=" + msg.msgId
                     + ", fallback to normal send: " + e.what());
    } catch (...) {
        original = std::current_exception();
        logger_error("Failed to send message back, consumerGroup=" + consumerGroup_
                     + ", brokerName=" + msg.brokerName + ", msg=" + msg.msgId
                     + ", fallback to normal send: unknown error");
    }
    try {
        // Degrade to an ordinary send to %RETRY%group: the broker then redelivers the message by
        // its delay level, so a broken send-back request does not lose the message.
        Message newMsg = buildRetryCopy(consumerGroup_, msg, maxReconsume);
        std::shared_ptr<TopicPublishInfo> publish =
            mqClient_->getTopicPublishInfo(newMsg.topic, /*isDefault=*/true);
        if (publish == nullptr || publish->msgQueueList.empty()) {
            throw MQClientException("no writable message queue for topic " + newMsg.topic);
        }
        // The retry copy leaves through the instance's internal producer, configured from this
        // consumer; unitMode is passed explicitly so the redelivery keeps its unit marker.
        mqClient_->sendMessage(MixAll::CLIENT_INNER_PRODUCER_GROUP, newMsg,
                               publish->selectOneMessageQueue(), 3000, /*sysFlag=*/0,
                               unitMode_);
    } catch (const std::exception& fallbackError) {
        logger_error("Failed to send message back as normal message, consumerGroup="
                     + consumerGroup_ + ", msg=" + msg.msgId + ": " + fallbackError.what());
        std::rethrow_exception(original);
    }
}

void DefaultMQPullConsumer::createTopic(const std::string& key, const std::string& newTopic,
                                       int32_t queueNum) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    // createTopicInRoute ignores the key parameter and always builds the route via MixAll::DEFAULT_TOPIC.
    (void)key;
    const std::string real = NamespaceUtil::wrapNamespace(namespace_, newTopic);
    mqClient_->createTopicInRoute(real, queueNum, queueNum, /*perm=*/6);
}

// ---------------------------------------------------------------- Heartbeat
// Why pull mode must heartbeat too: the broker's consumer table is per broker instance, and only
// a heartbeat (or a pull carrying the subscription flag) creates the entry. Without heartbeats:
//   1. consumerConnection lookups never show this group and GET_CONSUMER_LIST_BY_GROUP(38) is
//      empty;
//   2. a broker with isRejectPullConsumerEnabled=true answers every pull of this group with
//      "the pull consumer is rejected by server".
// Heartbeat subscription registration is skipped for CONSUME_ACTIVELY heartbeats, so pull mode
// relies on subscription-flagged pulls hitting the compensation branch: missing heartbeats fail
// silently - pulls keep succeeding while the broker never learns that the group exists.
void DefaultMQPullConsumer::refreshRouteForHeartbeat() {
    if (mqClient_ == nullptr) return;
    for (const std::string& t : registerTopics_) {
        const std::string real = NamespaceUtil::wrapNamespace(namespace_, t);
        mqClient_->registerTopicInUse(real);
        try {
            mqClient_->getTopicPublishInfo(real);
        } catch (const std::exception& e) {
            logger_debug("refresh route for " + real + " failed: " + e.what());
        }
    }
}

HeartbeatData DefaultMQPullConsumer::buildHeartbeat() const {
    HeartbeatData hb(clientId_);
    ConsumerData cd(consumerGroup_, ConsumeType::CONSUME_ACTIVELY, messageModel_,
                    ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    cd.unitMode = unitMode_;
    for (const std::string& t : registerTopics_) {
        const std::string subTopic = NamespaceUtil::wrapNamespace(namespace_, t);
        SubscriptionData sub = FilterAPI::buildSubscriptionData(subTopic, "*");
        sub.subVersion = 0;
        cd.addSubscriptionData(sub);
    }
    hb.addConsumerData(cd);
    return hb;
}

int32_t DefaultMQPullConsumer::sendHeartbeatToAllBroker() {
    if (mqClient_ == nullptr) {
        return 0;
    }
    std::vector<std::string> addrs;
    try {
        addrs = mqClient_->getAllBrokerAddrs();
    } catch (const std::exception& e) {
        logger_warn("heartbeat: gather brokers failed: " + std::string(e.what()));
        return 0;
    }
    if (addrs.empty()) {
        return 0;
    }
    const HeartbeatData hb = buildHeartbeat();
    int32_t okCount = 0;
    if (mqClient_->isEnableConcurrentHeartbeat()) {
        okCount = mqClient_->sendHeartbeatConcurrently(hb);
        if (okCount > 0) heartbeatCount_.fetch_add(1);
        return okCount;
    }
    for (const std::string& addr : addrs) {
        try {
            mqClient_->sendHeartbeat(addr, hb);
            ++okCount;
        } catch (const std::exception& e) {
            logger_warn("heartbeat to " + addr + " failed: " + e.what());
        }
    }
    if (okCount > 0) {
        heartbeatCount_.fetch_add(1);
    }
    return okCount;
}

void DefaultMQPullConsumer::startHeartbeatLoop() {
    // std::thread runs on construction, so the stop flag is prepared beforehand (false before the ctor) and usable at once.
    heartbeatThread_ = std::thread([this]() {
        setThreadName("PullConsumerHeartbeatThread");
        heartbeatLoop();
    });
}

void DefaultMQPullConsumer::heartbeatLoop() {
    while (!heartbeatStop_.load()) {
        {
            std::unique_lock<std::mutex> lk(heartbeatLock_);
            // On shutdown, shutdown() sets the flag and notifies, so the wait wakes after at most one period.
            if (heartbeatCv_.wait_for(lk, std::chrono::milliseconds(heartbeatBrokerIntervalMillis_),
                                      [this] { return heartbeatStop_.load(); })) {
                break;
            }
        }
        if (!heartbeatEnabled_) {
            continue;
        }
        // Boundary of the resident loop: an exception escaping the thread means std::terminate, taking the process down.
        // sendHeartbeatToAllBroker already catches per-broker failures; this fallback covers e.g. heartbeat assembly.
        try {
            sendHeartbeatToAllBroker();
        } catch (const std::exception& e) {
            logger_warn(std::string("heartbeat loop error: ") + e.what());
        }
    }
}

}  // namespace rocketmq
