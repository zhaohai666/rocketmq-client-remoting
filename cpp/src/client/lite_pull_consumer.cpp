#include "rocketmq/client/lite_pull_consumer.h"

#include <algorithm>
#include <ctime>
#include <optional>
#include <utility>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/validators.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/codes.h"

namespace rocketmq {

namespace {

// Same as in pull_consumer.cpp: split the name server address list on ';'.
std::vector<std::string> splitSemicolon(const std::string& addr) {
    std::vector<std::string> out;
    size_t start = 0;
    while (start <= addr.size()) {
        size_t pos = addr.find(';', start);
        std::string piece = addr.substr(start, pos == std::string::npos ? std::string::npos : pos - start);
        // trim
        size_t b = piece.find_first_not_of(" \t\r\n");
        if (b != std::string::npos) {
            size_t e = piece.find_last_not_of(" \t\r\n");
            std::string t = piece.substr(b, e - b + 1);
            if (!t.empty()) out.push_back(t);
        }
        if (pos == std::string::npos) break;
        start = pos + 1;
    }
    return out;
}

int64_t nowMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

int64_t parseConsumeTimestamp(const std::string& ts) {
    return UtilAll::parseConsumeTimestamp(ts);
}

}  // namespace

DefaultLitePullConsumer::DefaultLitePullConsumer(const std::string& consumerGroup) {
    // First guard, same as in the push consumer: blocks blank group names only (the old empty() check
    // let all-blank names pass, hence UtilAll::isBlank); full validation runs in start() via Validators::checkGroup.
    if (UtilAll::isBlank(consumerGroup)) {
        throw MQClientException("consumerGroup is empty");
    }
    consumerGroup_ = consumerGroup;
    consumeTimestamp_ = UtilAll::timeMillisToHumanString3(nowMillis() - 30 * 60 * 1000);
    allocateStrategy_ = std::make_shared<AllocateMessageQueueAveragely>();
}

DefaultLitePullConsumer::~DefaultLitePullConsumer() {
    try {
        shutdown();
    } catch (...) {
    }
}

// ---------------------------------------------------------------- Configuration
void DefaultLitePullConsumer::setNamesrvAddr(const std::string& addr) {
    nameServerAddrs_ = splitSemicolon(addr);
}

void DefaultLitePullConsumer::setNameServerAddresses(const std::vector<std::string>& addrs) {
    nameServerAddrs_ = addrs;
}

void DefaultLitePullConsumer::setAllocateMessageQueueStrategy(
    std::shared_ptr<AllocateMessageQueueStrategy> strategy) {
    allocateStrategy_ = std::move(strategy);
}

std::shared_ptr<AllocateMessageQueueStrategy>
DefaultLitePullConsumer::allocateMessageQueueStrategy() const {
    return allocateStrategy_;
}

// ---------------------------------------------------------------- Subscribe / assign
void DefaultLitePullConsumer::subscribe(const std::string& topic, const std::string& subExpression) {
    subscribe(topic, MessageSelector::byTag(subExpression));
}

void DefaultLitePullConsumer::subscribe(const std::string& topic, const MessageSelector& selector) {
    assignMode_ = false;
    std::string ns = withNamespace(topic);
    subscription_[ns] = selector.expression;
    try {
        subscriptionData_[ns] =
            FilterAPI::buildSubscriptionData(ns, selector.expression, selector.type);
    } catch (...) {
        subscriptionData_.erase(ns);
    }
}

void DefaultLitePullConsumer::subscribe(const std::string& topic,
                                        const std::string& subExpression,
                                        std::shared_ptr<LiteMessageQueueListener> messageQueueListener) {
    subscribe(topic, subExpression);
    if (messageQueueListener != nullptr) {
        std::lock_guard<std::mutex> lk(topicListenerMutex_);
        topicListeners_[withNamespace(topic)] = std::move(messageQueueListener);
    }
}

void DefaultLitePullConsumer::subscribe(const std::string& topic, const MessageSelector& selector,
                                        std::shared_ptr<LiteMessageQueueListener> messageQueueListener) {
    subscribe(topic, selector);
    if (messageQueueListener != nullptr) {
        std::lock_guard<std::mutex> lk(topicListenerMutex_);
        topicListeners_[withNamespace(topic)] = std::move(messageQueueListener);
    }
}

void DefaultLitePullConsumer::registerTopicMessageQueueChangeListener(
    const std::string& topic, std::shared_ptr<TopicMessageQueueChangeListener> listener) {
    if (topic.empty() || listener == nullptr) {
        throw MQClientException("Topic or listener is null");
    }
    std::string ns = withNamespace(topic);
    std::lock_guard<std::mutex> lk(topicListenerMutex_);
    if (topicChangeListeners_.find(ns) != topicChangeListeners_.end()) {
        logger_warn("Topic " + ns + " had been registered, new listener will overwrite the old one");
    }
    topicChangeListeners_[ns] = std::move(listener);
    if (started_.load()) {
        try {
            messageQueuesForTopic_[ns] = fetchMessageQueues(topic);
        } catch (const std::exception& e) {
            logger_debug("register listener: fetch queues for " + ns + " failed: " + e.what());
        }
    }
}

void DefaultLitePullConsumer::fetchTopicMessageQueuesAndCompare() {
    std::vector<std::pair<std::string, std::shared_ptr<TopicMessageQueueChangeListener>>> entries;
    {
        std::lock_guard<std::mutex> lk(topicListenerMutex_);
        for (const auto& kv : topicChangeListeners_) {
            entries.emplace_back(kv.first, kv.second);
        }
    }
    for (auto& kv : entries) {
        const std::string& topic = kv.first;
        try {
            std::vector<MessageQueue> newQueues = fetchMessageQueues(topic);
            std::vector<MessageQueue> oldQueues;
            bool changed = false;
            {
                std::lock_guard<std::mutex> lk(topicListenerMutex_);
                auto it = messageQueuesForTopic_.find(topic);
                if (it != messageQueuesForTopic_.end()) {
                    oldQueues = it->second;
                }
                std::set<MessageQueue> oldSet(oldQueues.begin(), oldQueues.end());
                std::set<MessageQueue> newSet(newQueues.begin(), newQueues.end());
                changed = !(oldSet == newSet);
                if (changed) {
                    messageQueuesForTopic_[topic] = newQueues;
                }
            }
            if (changed) {
                kv.second->onChanged(topic, newQueues);
            }
        } catch (const std::exception& e) {
            logger_warn("ScheduledTask fetchMessageQueuesAndCompare exception: " +
                        std::string(e.what()));
        }
    }
}

void DefaultLitePullConsumer::metadataLoop() {
    auto next = std::chrono::steady_clock::now() + std::chrono::milliseconds(10000);
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (!running_.load()) return;
        if (std::chrono::steady_clock::now() < next) continue;
        next += std::chrono::milliseconds(topicMetadataCheckIntervalMillis_);
        try {
            fetchTopicMessageQueuesAndCompare();
        } catch (const std::exception& e) {
            logger_warn("metadata compare error: " + std::string(e.what()));
        }
    }
}

void DefaultLitePullConsumer::setSubExpressionForAssign(const std::string& topic,
                                                       const std::string& subExpression) {
    setSubExpressionForAssign(topic, MessageSelector::byTag(subExpression));
}

void DefaultLitePullConsumer::setSubExpressionForAssign(const std::string& topic,
                                                        const MessageSelector& selector) {
    std::string ns = withNamespace(topic);
    assignSubExpr_[ns] = selector.expression;
    try {
        subscriptionData_[ns] =
            FilterAPI::buildSubscriptionData(ns, selector.expression, selector.type);
    } catch (...) {
        subscriptionData_.erase(ns);
    }
}

void DefaultLitePullConsumer::assign(const std::vector<MessageQueue>& messageQueues) {
    std::set<MessageQueue> queues(messageQueues.begin(), messageQueues.end());
    std::vector<MessageQueue> toResolve;
    std::vector<MessageQueue> revokedQueues;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        assignMode_ = true;
        for (const MessageQueue& mq : assigned_) {
            if (queues.find(mq) == queues.end()) {
                nextOffset_.erase(mq);
                consumeOffset_.erase(mq);
                revokedQueues.push_back(mq);
            }
        }
        assigned_ = queues;
        for (const MessageQueue& mq : assigned_) {
            if (nextOffset_.find(mq) == nextOffset_.end()) toResolve.push_back(mq);
        }
    }
    // Dropping the revoked cache runs on the caller thread, where no pull round is left to prune it:
    // an assign() that narrows the set must not keep the revoked queue's messages pollable.
    dropQueueCache(revokedQueues);
    // resolveInitialOffset issues an RPC, so it must never run while stateMutex_ is held.
    for (const MessageQueue& mq : toResolve) {
        int64_t offset = 0;
        try {
            offset = resolveInitialOffset(mq);
        } catch (...) {
            logger_debug("lite assign: resolve initial offset failed for " + mq.toString());
            continue;
        }
        std::lock_guard<std::mutex> lk(stateMutex_);
        // rebalance/assign may have changed things meanwhile: write the cursor back only for queues still held.
        if (assigned_.find(mq) != assigned_.end() && nextOffset_.find(mq) == nextOffset_.end()) {
            nextOffset_[mq] = offset;
        }
    }
}

// ---------------------------------------------------------------- Lifecycle
void DefaultLitePullConsumer::start() {
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
    if (allocateStrategy_ == nullptr) {
        throw MQClientException("allocateMessageQueueStrategy is null");
    }
    if (subscription_.empty() && !assignMode_) {
        throw MQClientException("subscription is not set, call subscribe() or assign() first");
    }
    parseConsumeTimestamp(consumeTimestamp_);
    if (messageModel_ == MessageModel::CLUSTERING) {
        instanceName_ = changeInstanceNameToPID(instanceName_);
    }
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_, unitName_, enableStreamRequestType_);
    }
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                        /*connectTimeoutMillis=*/3000,
                                        /*invokeTimeoutMillis=*/10000,
                                        MQClientInstance::tlsEnabledFromEnv(), unitName_,
                                        pollNameServerIntervalMillis_));
    // Broker VIP channel: forwarded to the instance that owns the broker RPCs (off = plain address).
    mqClient_->setVipChannelEnabled(vipChannelEnabled_);
    mqClient_->setDecodeReadBody(decodeReadBody_);
    mqClient_->setMqClientApiTimeout(mqClientApiTimeout_);
    mqClient_->setEnableConcurrentHeartbeat(enableConcurrentHeartbeat_);
    mqClient_->setConcurrentHeartbeatThreadPoolSize(concurrentHeartbeatThreadPoolSize_);
    const std::shared_ptr<RPCHook> requestHook =
        composeRequestHooks(enableStreamRequestType_, rpcHook_,
                            [this] { return namespaceV2_; });
    if (requestHook && !mqClient_->registerRPCHook(requestHook)) {
        logger_warn("lite pull consumer rpc hook ignored: MQClientInstance already has one (clientId="
                    + clientId_ + ")");
    }
    mqClient_->start();
    std::set<std::string> routeTopics;
    for (const auto& kv : subscription_) {
        routeTopics.insert(kv.first);
    }
    for (const MessageQueue& mq : assigned_) {
        routeTopics.insert(mq.topic);
    }
    for (const std::string& t : routeTopics) {
        mqClient_->registerTopicInUse(t);
        try {
            mqClient_->getTopicPublishInfo(t);
        } catch (const std::exception& e) {
            logger_debug("lite start: refresh route for " + t + " failed: " + e.what());
        }
    }
    if (assignMode_) {
        for (const MessageQueue& mq : assigned_) {
            if (nextOffset_.find(mq) == nextOffset_.end()) {
                try {
                    nextOffset_[mq] = resolveInitialOffset(mq);
            } catch (...) {
                logger_debug("lite start: resolve initial offset failed for " + mq.toString());
            }
            }
        }
    }
    // Register the tag subscription with the broker (heartbeat) before background pulling starts, so the first pull round cannot lose messages because the broker does not know the subscription.
    sendHeartbeatToAllBroker();
    // running_ must be set before the heartbeat thread starts: heartbeatLoop loops on it, so a thread that
    // starts first sees false and exits, and this instance never heartbeats again (the broker drops it after 120s).
    running_ = true;
    heartbeatThread_ = std::thread(&DefaultLitePullConsumer::heartbeatLoop, this);
    started_ = true;
    pullThread_ = std::thread(&DefaultLitePullConsumer::pullServiceLoop, this);
    metadataThread_ = std::thread(&DefaultLitePullConsumer::metadataLoop, this);
}

void DefaultLitePullConsumer::shutdown() {
    if (!started_) return;
    started_ = false;
    running_ = false;
    try {
        if (autoCommit_) {
            commit();
        } else {
            std::set<MessageQueue> scope;
            {
                std::lock_guard<std::mutex> lk(stateMutex_);
                scope = assigned_;
            }
            persistOffsetTable(scope);
        }
    } catch (...) {
        logger_debug("lite shutdown commit failed");
    }
    {
        std::lock_guard<std::mutex> lk(bufferMutex_);
        bufferCv_.notify_all();
    }
    if (pullThread_.joinable()) pullThread_.join();
    if (heartbeatThread_.joinable()) heartbeatThread_.join();
    if (metadataThread_.joinable()) metadataThread_.join();
    if (mqClient_ != nullptr) {
        mqClient_->shutdown();
        mqClient_.reset();
    }
}

// ---------------------------------------------------------------- Pull service
void DefaultLitePullConsumer::pullServiceLoop() {
    while (running_) {
        bool gotAny = false;
        try {
            auto now = std::chrono::steady_clock::now();
            if (!assignMode_ && (lastRebalanceTs_ == std::chrono::steady_clock::time_point() ||
                                 std::chrono::duration_cast<std::chrono::milliseconds>(
                                     now - lastRebalanceTs_)
                                         .count() > 1000)) {
                rebalance();
                lastRebalanceTs_ = now;
            }
            // assign() and pause() on the caller thread mutate assigned_ / paused_: snapshot under the lock first, then iterate.
            std::set<MessageQueue> snapshot;
            std::set<MessageQueue> paused;
            {
                std::lock_guard<std::mutex> lk(stateMutex_);
                snapshot = assigned_;
                paused = paused_;
            }
            // A revoked queue's accumulator must not refuse a later pull when the same queue is
            // re-assigned. Prune every accumulator this round no longer owns (only the pull thread
            // touches cacheAccums_/parkUntil_ besides the buffer-mutation sites, all under
            // bufferMutex_).
            std::vector<MessageQueue> noLongerOwned;
            {
                std::lock_guard<std::mutex> lk(bufferMutex_);
                for (const auto& kv : cacheAccums_) {
                    if (snapshot.find(kv.first) == snapshot.end()) {
                        noLongerOwned.push_back(kv.first);
                    }
                }
            }
            dropQueueCache(noLongerOwned);
            for (const MessageQueue& mq : snapshot) {
                if (!running_) break;
                if (paused.find(mq) != paused.end()) continue;
                // A queue parked by an earlier refusal (cache gate = 50ms, broker FLOW_CONTROL = 20ms)
                // is skipped until its deadline passes; parked only this queue, siblings keep pulling,
                // and shutdown stays prompt (the loop's bounded backoff sleep is the only wait).
                const auto now = std::chrono::steady_clock::now();
                auto parkIt = parkUntil_.find(mq);
                if (parkIt != parkUntil_.end()) {
                    if (parkIt->second > now) continue;
                    parkUntil_.erase(parkIt);
                }
                if (cacheFlowControlHit(mq)) {
                    // Refuse without touching the wire: park this queue for the client-cache pause and
                    // record the applied value. Nothing else changes -- no offset commits, cursor holds.
                    parkUntil_[mq] = now + std::chrono::milliseconds(kPullPauseWhenClientFlowControl);
                    lastLitePullPauseMillis_.store(kPullPauseWhenClientFlowControl);
                    continue;
                }
                if (pullOne(mq)) gotAny = true;
            }
        } catch (...) {
            logger_debug("lite pull service error");
        }
        // Backoff: slow down while polls come up empty so the broker is not flooded; refill the buffer quickly when there were messages.
        auto backoff = gotAny ? std::chrono::milliseconds(5)
                              : std::chrono::milliseconds(pullIntervalMillis_);
        std::this_thread::sleep_for(backoff);
    }
}

std::string DefaultLitePullConsumer::subscriptionFor(const std::string& topic) const {
    auto it = subscription_.find(topic);
    if (it != subscription_.end()) return it->second;
    auto ait = assignSubExpr_.find(topic);
    if (ait != assignSubExpr_.end()) return ait->second;
    return "*";
}

// The subscription this round of pulling is described by: the registered entry when the topic has one
// (subscribe / setSubExpressionForAssign build it selector-aware, so its expression type and version
// reach the wire) and the plain tag form of the raw table entry otherwise.
SubscriptionData DefaultLitePullConsumer::subscriptionDataFor(const std::string& topic) const {
    auto it = subscriptionData_.find(topic);
    if (it != subscriptionData_.end()) return it->second;
    return SubscriptionData(topic, subscriptionFor(topic));
}

bool DefaultLitePullConsumer::pullOne(const MessageQueue& mq) {
    int64_t offset = 0;
    bool known = false;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        auto it = nextOffset_.find(mq);
        if (it != nextOffset_.end()) {
            offset = it->second;
            known = true;
        }
    }
    if (!known) {
        // The first pull has to ask the broker for a start offset, which is an RPC: never do it while holding stateMutex_.
        int64_t resolved = 0;
        try {
            resolved = resolveInitialOffset(mq);
        } catch (...) {
            return false;
        }
        std::lock_guard<std::mutex> lk(stateMutex_);
        auto it = nextOffset_.find(mq);
        if (it == nextOffset_.end()) {
            nextOffset_[mq] = resolved;
            offset = resolved;
        } else {
            offset = it->second;
        }
    }
    const SubscriptionData subData = subscriptionDataFor(mq.topic);
    // The wire expression follows the subscription's own type: a tag subscription sends the compiled
    // tag form, a selector subscription sends its expression verbatim -- never re-tagged.
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/false,
                                                      /*suspend=*/false,
                                                      /*subscription=*/true,
                                                      /*classFilter=*/false,
                                                      /*litePull=*/true);
    PullResult result;
    try {
        result = mqClient_->pullMessage(consumerGroup_, mq, offset, pullBatchSize_, sysFlag, 0,
                                        FilterAPI::pullExpression(subData),
                                        FilterAPI::pullSubVersionOf(subData), subData.expressionType,
                                        /*timeoutMillis=*/30000, /*maxMsgBytes=*/-1,
                                        /*suspendTimeoutMillis=*/15000);
    } catch (const MQBrokerException& e) {
        // MQBrokerException is a sibling of RemotingTimeoutException under std::runtime_error, so
        // catching it before catch(...) is live code and unmapped broker codes reach the mapping.
        // The broker's FLOW_CONTROL(215) throttle gets its own short pause via the same mapping the
        // push path uses (no second constant); every other code keeps the port's existing handling
        // (fall through to the loop's idle backoff).
        if (e.getResponseCode() == ResponseCode::FLOW_CONTROL) {
            const int32_t pauseMs = DefaultMQPushConsumer::pullPauseMillisFor(e.getResponseCode());
            parkUntil_[mq] = std::chrono::steady_clock::now() + std::chrono::milliseconds(pauseMs);
            lastLitePullPauseMillis_.store(pauseMs);
        }
        logger_debug("lite pull_one failed for " + mq.topic + "@" + std::to_string(mq.queueId)
                     + " brokerCode=" + std::to_string(e.getResponseCode()));
        return false;
    } catch (...) {
        logger_debug("lite pull_one failed for " + mq.topic + "@" + std::to_string(mq.queueId));
        return false;
    }
    bool intact = false;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        auto it = nextOffset_.find(mq);
        if (it != nextOffset_.end() && it->second == offset) {
            it->second = result.nextBeginOffset;
            intact = true;
        }
    }
    if (!intact || result.status != PullStatus::FOUND || result.msgFoundList.empty()) {
        return false;
    }
    std::vector<MessageExt> msgs = std::move(result.msgFoundList);
    filterTags(msgs, subData);
    if (msgs.empty()) {
        // Everything was filtered out by the tag: the broker already counts this window as read (the cursor was advanced
        // above), there was simply nothing deliverable - unlike the old behaviour, the same window is not re-pulled in place.
        return false;
    }
    enqueue(msgs);
    return true;
}

// The client-side second pass over the tag list (the broker's codeSet hash filter can collide and let a
// message through). It runs on a tag subscription only: a selector expression is not a tag list, so its
// tagsSet is empty by construction and nothing is filtered locally -- the broker's filter layer alone
// decides what this subscription matches.
bool DefaultLitePullConsumer::filterTags(std::vector<MessageExt>& msgs,
                                        const SubscriptionData& subData) {
    if (subData.tagsSet.empty()) return true;
    size_t w = 0;
    for (size_t r = 0; r < msgs.size(); ++r) {
        const std::string tag = msgs[r].getTags();
        if (subData.tagsSet.find(tag) != subData.tagsSet.end()) {
            if (w != r) msgs[w] = std::move(msgs[r]);
            ++w;
        }
    }
    msgs.resize(w);
    return true;
}

void DefaultLitePullConsumer::enqueue(const std::vector<MessageExt>& msgs) {
    // One call is one PULL ROUND: however many queues its batch spans, each queue the round left
    // something at gets one entry in its pending-round list, carrying how many of this round's
    // messages landed there.
    std::map<MessageQueue, int64_t> perQueue;
    for (const MessageExt& m : msgs) {
        ++perQueue[MessageQueue(m.topic, m.brokerName, m.queueId)];
    }
    std::lock_guard<std::mutex> lk(bufferMutex_);
    std::set<MessageQueue> booked;
    for (const MessageExt& m : msgs) {
        localBuffer_.push_back(m);
        // Buffer and accumulator mutate in one place under one lock, so the counters cannot drift.
        const MessageQueue mq(m.topic, m.brokerName, m.queueId);
        QueueCacheAccum& acc = cacheAccums_[mq];
        ++acc.count;
        // The gate's byte budget is the message BODY: storeSize is what the broker charged for the
        // whole storage record and belongs to telemetry, not to flow control.
        acc.sizeSum += static_cast<int64_t>(m.getBody().size());
        acc.offsets.insert(m.queueOffset);
        if (booked.insert(mq).second) {
            acc.rounds.push_back(perQueue[mq]);
        }
    }
    bufferCv_.notify_all();
}

// ---------------------------------------------------------------- cache flow-control gate
// Four per-queue thresholds, judged strictly-greater in this exact order before the pull is built:
//   1) aggregate (pending pull rounds x pullBatchSize > pullThresholdForAll)
//   2) count   (this queue's cached count > pullThresholdForQueue)        -- shared counter with (3)
//   3) size    (this queue's cached body bytes // 1MiB > pullThresholdSizeForQueue)
//   4) span    (this queue's cached maxOffset - minOffset > consumeMaxSpan)
// A queue sitting exactly on a threshold still pulls. Gates 2..4 judge only this queue: a hot sibling
// never parks a cold one. The aggregate gate is a CAPACITY projection, not a sum of the buffer: every
// round that still holds a message is budgeted with a whole pullBatchSize, whether that round came
// back full or with a single message, because the application has not drained those rounds yet. The
// threshold is stated in messages while this gate counts rounds, so the warn translates it back into
// the number of rounds that fit (ceiling division).
// The measurement (round count and this queue's accumulator) happens in a single short critical
// section under bufferMutex_ so the counters cannot drift from the buffer; the judgment and the
// counter/log side effects run outside it.
bool DefaultLitePullConsumer::cacheFlowControlHit(const MessageQueue& mq) {
    int64_t rounds = 0;
    int64_t count = 0;
    int64_t sizeMb = 0;
    int64_t minOff = 0;
    int64_t maxOff = 0;
    const int64_t batch = std::max(1, pullBatchSize_);
    {
        std::lock_guard<std::mutex> lk(bufferMutex_);
        for (const auto& kv : cacheAccums_) {
            rounds += static_cast<int64_t>(kv.second.rounds.size());
        }
        auto it = cacheAccums_.find(mq);
        if (it != cacheAccums_.end()) {
            count = it->second.count;
            // Integer MiB division of the cached BODY bytes: a 1 MiB budget tolerates a 1.9 MiB
            // buffer, because 1.9 truncated is 1 and only a whole MiB above the budget refuses.
            sizeMb = it->second.sizeSum / (1024LL * 1024LL);
            if (!it->second.offsets.empty()) {
                minOff = *it->second.offsets.begin();
                maxOff = *it->second.offsets.rbegin();
            }
        }
    }
    const int64_t span = maxOff - minOff;
    const int64_t reserved = rounds * batch;

    // (1) aggregate gate.
    if (reserved > static_cast<int64_t>(pullThresholdForAll_)) {
        const int64_t times = aggregateFlowControlTimes_.fetch_add(1) + 1;
        const int64_t fits =
            (static_cast<int64_t>(pullThresholdForAll_) + batch - 1) / batch;
        logger_debug("lite flow control aggregate: queue " + mq.toString()
                     + " rounds=" + std::to_string(rounds)
                     + " x pullBatchSize=" + std::to_string(batch)
                     + " -> " + std::to_string(reserved)
                     + " pullThresholdForAll=" + std::to_string(pullThresholdForAll_)
                     + ", pause pull");
        if (isFlowControlWarnTrip(times)) {
            logger_warn("the consume request count exceeds threshold "
                        + std::to_string(fits)
                        + ", so do flow control, consume request count=" + std::to_string(rounds)
                        + ", flowControlTimes=" + std::to_string(times));
        }
        return true;
    }
    // (2)+(3) per-queue count and size gates share one counter.
    if (count > static_cast<int64_t>(pullThresholdForQueue_)) {
        const int64_t times = queueFlowControlTimes_.fetch_add(1) + 1;
        logger_debug("lite flow control count: queue " + mq.toString() + " count="
                     + std::to_string(count) + " pullThresholdForQueue="
                     + std::to_string(pullThresholdForQueue_) + ", pause pull");
        if (isFlowControlWarnTrip(times)) {
            logger_warn("The cached message count exceeds the threshold "
                        + std::to_string(pullThresholdForQueue_)
                        + ", so do flow control, minOffset=" + std::to_string(minOff)
                        + ", maxOffset=" + std::to_string(maxOff)
                        + ", count=" + std::to_string(count)
                        + ", size=" + std::to_string(sizeMb) + " MiB"
                        + ", flowControlTimes=" + std::to_string(times));
        }
        return true;
    }
    if (sizeMb > static_cast<int64_t>(pullThresholdSizeForQueue_)) {
        const int64_t times = queueFlowControlTimes_.fetch_add(1) + 1;
        logger_debug("lite flow control size: queue " + mq.toString() + " sizeMb="
                     + std::to_string(sizeMb) + " pullThresholdSizeForQueue="
                     + std::to_string(pullThresholdSizeForQueue_) + ", pause pull");
        if (isFlowControlWarnTrip(times)) {
            logger_warn("The cached message size exceeds the threshold "
                        + std::to_string(pullThresholdSizeForQueue_)
                        + " MiB, so do flow control, minOffset=" + std::to_string(minOff)
                        + ", maxOffset=" + std::to_string(maxOff)
                        + ", count=" + std::to_string(count)
                        + ", size=" + std::to_string(sizeMb) + " MiB"
                        + ", flowControlTimes=" + std::to_string(times));
        }
        return true;
    }
    // (4) span gate.
    if (span > consumeMaxSpan_) {
        const int64_t times = spanFlowControlTimes_.fetch_add(1) + 1;
        logger_debug("lite flow control span: queue " + mq.toString() + " span="
                     + std::to_string(span) + " consumeMaxSpan="
                     + std::to_string(consumeMaxSpan_) + ", pause pull");
        if (isFlowControlWarnTrip(times)) {
            logger_warn("The queue's messages, span too long, so do flow control, minOffset="
                        + std::to_string(minOff) + ", maxOffset=" + std::to_string(maxOff)
                        + ", maxSpan=" + std::to_string(span)
                        + ", flowControlTimes=" + std::to_string(times));
        }
        return true;
    }
    return false;
}

bool DefaultLitePullConsumer::cacheFlowControlHitForTest(const MessageQueue& mq) {
    return cacheFlowControlHit(mq);
}

int32_t DefaultLitePullConsumer::pendingPullRounds() {
    std::lock_guard<std::mutex> lk(bufferMutex_);
    int64_t rounds = 0;
    for (const auto& kv : cacheAccums_) {
        rounds += static_cast<int64_t>(kv.second.rounds.size());
    }
    return static_cast<int32_t>(rounds);
}

int32_t DefaultLitePullConsumer::cachedMessageCount(const MessageQueue& mq) {
    std::lock_guard<std::mutex> lk(bufferMutex_);
    auto it = cacheAccums_.find(mq);
    return it == cacheAccums_.end() ? 0 : static_cast<int32_t>(it->second.count);
}

void DefaultLitePullConsumer::injectCachedForTest(const std::vector<MessageExt>& msgs) {
    enqueue(msgs);
}

// One pass for every revoke path (caller-thread assign(), the rebalance round, the pull round's
// backstop): the queue's buffered rows leave the pollable buffer and its accumulator goes with them,
// so neither the caller nor the gate can still see messages of a queue this instance no longer owns.
void DefaultLitePullConsumer::dropQueueCache(const std::vector<MessageQueue>& revokedQueues) {
    if (revokedQueues.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lk(bufferMutex_);
    for (const MessageQueue& mq : revokedQueues) {
        std::deque<MessageExt> kept;
        for (MessageExt& m : localBuffer_) {
            if (m.topic == mq.topic && m.brokerName == mq.brokerName && m.queueId == mq.queueId) {
                continue;
            }
            kept.push_back(std::move(m));
        }
        localBuffer_.swap(kept);
        cacheAccums_.erase(mq);
    }
}

void DefaultLitePullConsumer::maybeAutoCommit() {
    const int64_t now = nowMillis();
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        if (now < nextAutoCommitDeadline_) return;
        nextAutoCommitDeadline_ = now + autoCommitIntervalMillis_;
    }
    commit();
}

int64_t DefaultLitePullConsumer::resolveInitialOffset(const MessageQueue& mq) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        auto sit = seekOffset_.find(mq);
        if (sit != seekOffset_.end()) return sit->second;
    }
    int64_t stored = -1;
    try {
        int64_t off = 0;
        if (mqClient_->queryConsumerOffset(consumerGroup_, mq, off)) stored = off;
    } catch (...) {
    }
    // The same policy table the push consumer uses: the start never depends on the subscription type.
    switch (planInitialOffset(stored, consumeFromWhere_, MixAll::isRetryTopic(mq.topic))) {
        case StartOffsetPlan::kStored:
            return stored;
        case StartOffsetPlan::kFromZero:
            return 0;
        case StartOffsetPlan::kQueueTail:
            return mqClient_->getMaxOffset(mq);
        case StartOffsetPlan::kSearchAtConfiguredTime:
            return mqClient_->searchOffsetByTimestamp(mq, parseConsumeTimestamp(consumeTimestamp_));
    }
    return 0;
}

// ---------------------------------------------------------------- rebalance
void DefaultLitePullConsumer::rebalance() {
    std::set<MessageQueue> newSet;
    // mqAll (per topic) for the per-subscription listener callbacks; recorded inside the loop so the
    // callback phase does not need another round of subscription-info RPCs.
    std::map<std::string, std::vector<MessageQueue>> mqAllByTopic;
    for (const auto& kv : subscription_) {
        std::vector<MessageQueue> mqAll = mqClient_->getTopicSubscribeInfo(kv.first);
        mqAllByTopic[kv.first] = mqAll;
        std::sort(mqAll.begin(), mqAll.end());
        std::vector<std::string> cidAll =
            mqClient_->getConsumerIdListByGroup(kv.first, consumerGroup_);
        if (std::find(cidAll.begin(), cidAll.end(), clientId_) == cidAll.end()) {
            cidAll.push_back(clientId_);
        }
        std::sort(cidAll.begin(), cidAll.end());
        std::vector<MessageQueue> allocated;
        try {
            allocated = allocateStrategy_->allocate(consumerGroup_, clientId_, mqAll, cidAll);
        } catch (const std::exception& e) {
            logger_error("allocate message queue exception. strategy name: "
                         + allocateStrategy_->getName() + ", ex: " + e.what());
            std::lock_guard<std::mutex> lk(stateMutex_);
            for (const MessageQueue& mq : assigned_) {
                if (mq.topic == kv.first) allocated.push_back(mq);
            }
        }
        newSet.insert(allocated.begin(), allocated.end());
    }
    std::set<MessageQueue> old;
    std::vector<MessageQueue> toResolve;
    std::vector<std::pair<MessageQueue, int64_t>> revoked;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        changed = (newSet != assigned_);
        if (changed) {
            old = assigned_;
            assigned_ = newSet;
            for (const MessageQueue& mq : newSet) {
                if (nextOffset_.find(mq) == nextOffset_.end()) toResolve.push_back(mq);
            }
            for (const MessageQueue& mq : old) {
                if (newSet.find(mq) == newSet.end()) {
                    auto ot = offsetTable_.find(mq);
                    if (ot != offsetTable_.end()) revoked.push_back(*ot);
                    nextOffset_.erase(mq);
                    // Drop the AssignedMessageQueue entry together with consumeOffset: keeping it
                    // would leave a stale offset that nobody ever commits again.
                    consumeOffset_.erase(mq);
                    offsetTable_.erase(mq);
                    seekOffset_.erase(mq);
                }
            }
        }
    }
    if (!changed) return;
            // Flush the last commit before letting go, so the new owner does not redeliver from the previous window.
    for (const auto& kv : revoked) {
        try {
            mqClient_->updateConsumerOffset(consumerGroup_, kv.first, kv.second);
        } catch (...) {
            logger_debug("lite persist on revoke failed for " + kv.first.toString());
        }
    }
    // A queue this consumer no longer owns keeps no cache behind its back: its buffered messages and
    // its accumulator (so its pending rounds and its per-queue gate numbers) go with it, in the same
    // pass that let go of the offset tables above. Leaving them would let a queue
    // this instance handed over still refuse a later pull of its own re-assigned self on counters
    // belonging to messages it no longer consumes. (A parked deadline is not touched here: parkUntil_
    // belongs to the pull thread alone, and that thread prunes it for a queue it no longer owns.)
    if (!revoked.empty()) {
        std::vector<MessageQueue> dropped;
        dropped.reserve(revoked.size());
        for (const auto& kv : revoked) dropped.push_back(kv.first);
        dropQueueCache(dropped);
    }
    for (const MessageQueue& mq : toResolve) {
        int64_t offset = 0;
        try {
            offset = resolveInitialOffset(mq);
        } catch (...) {
            logger_debug("lite rebalance: resolve offset failed for " + mq.toString());
            continue;
        }
        std::lock_guard<std::mutex> lk(stateMutex_);
        if (assigned_.find(mq) != assigned_.end() && nextOffset_.find(mq) == nextOffset_.end()) {
            nextOffset_[mq] = offset;
        }
    }
    if (messageQueueListener_ != nullptr) {
        try {
            messageQueueListener_->messageQueueChanged(mqAllOfSubscription(), newSetAsVector(newSet));
        } catch (...) {
        }
    }
    std::vector<std::pair<std::string, std::shared_ptr<LiteMessageQueueListener>>> topicListeners;
    {
        std::lock_guard<std::mutex> lk(topicListenerMutex_);
        for (const auto& kv : topicListeners_) topicListeners.push_back(kv);
    }
    for (auto& kv : topicListeners) {
        std::vector<MessageQueue> dividedNew;
        std::vector<MessageQueue> dividedOld;
        for (const MessageQueue& mq : newSet) {
            if (mq.topic == kv.first) dividedNew.push_back(mq);
        }
        for (const MessageQueue& mq : old) {
            if (mq.topic == kv.first) dividedOld.push_back(mq);
        }
        if (dividedNew == dividedOld) continue;
        auto it = mqAllByTopic.find(kv.first);
        try {
            kv.second->messageQueueChanged(
                it != mqAllByTopic.end() ? it->second : std::vector<MessageQueue>(),
                dividedNew);
        } catch (...) {
        }
    }
}

// Collect every queue of the subscription (for listener callbacks) - same source as rebalance: topic subscribe info.
std::vector<MessageQueue> DefaultLitePullConsumer::mqAllOfSubscription() const {
    std::vector<MessageQueue> all;
    for (const auto& kv : subscription_) {
        for (const MessageQueue& mq : mqClient_->getTopicSubscribeInfo(kv.first)) {
            all.push_back(mq);
        }
    }
    return all;
}

std::vector<MessageQueue> DefaultLitePullConsumer::newSetAsVector(const std::set<MessageQueue>& s) const {
    return std::vector<MessageQueue>(s.begin(), s.end());
}

// ---------------------------------------------------------------- poll / consumer offset
std::vector<MessageExt> DefaultLitePullConsumer::poll(int32_t timeoutMillis) {
    int32_t timeout = timeoutMillis > 0 ? timeoutMillis : pollTimeoutMillis_;
    if (autoCommit_) maybeAutoCommit();
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout);
    std::vector<MessageExt> out;
    {
        std::unique_lock<std::mutex> lk(bufferMutex_);
        while (localBuffer_.empty()) {
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 deadline - std::chrono::steady_clock::now())
                                 .count();
            if (remaining <= 0) return {};
            bufferCv_.wait_for(lk, std::chrono::milliseconds(remaining));
        }
        while (!localBuffer_.empty() && out.size() < 1024) {
            out.push_back(std::move(localBuffer_.front()));
            // Decrement this queue's accumulator in lockstep with the pop (metadata is read from the
            // moved-to copy since the deque's front has already been moved from).
            const MessageExt& m = out.back();
            const MessageQueue mq(m.topic, m.brokerName, m.queueId);
            auto ait = cacheAccums_.find(mq);
            if (ait != cacheAccums_.end()) {
                --ait->second.count;
                ait->second.sizeSum -= static_cast<int64_t>(m.getBody().size());
                // A queue's messages leave in round order (the buffer is drained from its head and a
                // round's messages of one queue are contiguous in it), so taking one message away eats
                // the front pending round; a half-drained round is still pending.
                if (!ait->second.rounds.empty()) {
                    if (--ait->second.rounds.front() <= 0) {
                        ait->second.rounds.pop_front();
                    }
                }
                auto oit = ait->second.offsets.find(m.queueOffset);
                if (oit != ait->second.offsets.end()) ait->second.offsets.erase(oit);
                if (ait->second.count <= 0) cacheAccums_.erase(ait);
            }
            localBuffer_.pop_front();
        }
    }
    advanceConsumeOffset(out);
    return out;
}

void DefaultLitePullConsumer::advanceConsumeOffset(const std::vector<MessageExt>& msgs) {
    if (msgs.empty()) return;
    std::lock_guard<std::mutex> lk(stateMutex_);
    for (const MessageExt& m : msgs) {
        const MessageQueue mq(m.topic, m.brokerName, m.queueId);
        if (nextOffset_.find(mq) == nextOffset_.end()) continue;
        const int64_t next = m.queueOffset + 1;
        auto it = consumeOffset_.find(mq);
        if (it == consumeOffset_.end() || next > it->second) consumeOffset_[mq] = next;
    }
}

void DefaultLitePullConsumer::seek(const MessageQueue& mq, int64_t offset) {
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        seekOffset_[mq] = offset;
        nextOffset_[mq] = offset;
        consumeOffset_[mq] = offset;
    }
    std::lock_guard<std::mutex> lk(bufferMutex_);
    std::deque<MessageExt> kept;
    // Prune the buffer and this queue's accumulator in one pass, by SUBTRACTING what leaves rather
    // than rebuilding from what stays: a round is booked when its batch lands, and only the removal
    // rule ("this queue's messages leave in round order, so a removal eats the front pending round")
    // keeps the pending-round count honest. Rebuilding would collapse the surviving rounds into one.
    auto accIt = cacheAccums_.find(mq);
    for (MessageExt& m : localBuffer_) {
        const bool sameQueue = (m.topic == mq.topic && m.brokerName == mq.brokerName &&
                                m.queueId == mq.queueId);
        const bool drop = (sameQueue && m.queueOffset >= offset);
        if (drop) {
            if (accIt != cacheAccums_.end()) {
                --accIt->second.count;
                accIt->second.sizeSum -= static_cast<int64_t>(m.getBody().size());
                auto oit = accIt->second.offsets.find(m.queueOffset);
                if (oit != accIt->second.offsets.end()) accIt->second.offsets.erase(oit);
                if (!accIt->second.rounds.empty()) {
                    if (--accIt->second.rounds.front() <= 0) {
                        accIt->second.rounds.pop_front();
                    }
                }
            }
            continue;
        }
        kept.push_back(std::move(m));
    }
    localBuffer_.swap(kept);
    if (accIt != cacheAccums_.end() && accIt->second.count <= 0) {
        cacheAccums_.erase(accIt);
    }
}

void DefaultLitePullConsumer::seekToBegin(const MessageQueue& mq) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    seek(mq, mqClient_->getMinOffset(mq));
}

void DefaultLitePullConsumer::seekToEnd(const MessageQueue& mq) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    seek(mq, mqClient_->getMaxOffset(mq));
}

int64_t DefaultLitePullConsumer::committed(const MessageQueue& mq) {
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        auto it = offsetTable_.find(mq);
        if (it != offsetTable_.end()) return it->second;
    }
    if (mqClient_ == nullptr) return -1;
    try {
        int64_t off = 0;
        if (mqClient_->queryConsumerOffset(consumerGroup_, mq, off)) {
            std::lock_guard<std::mutex> lk(stateMutex_);
            offsetTable_[mq] = off;
            return off;
        }
    } catch (...) {
    }
    return -1;
}

void DefaultLitePullConsumer::persistOffset(const MessageQueue& mq) {
    int64_t offset = 0;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        auto it = offsetTable_.find(mq);
        if (it == offsetTable_.end()) return;
        offset = it->second;
    }
    if (mqClient_ == nullptr) return;
    try {
        mqClient_->updateConsumerOffset(consumerGroup_, mq, offset);
    } catch (const std::exception& e) {
        logger_debug("lite persist failed for " + mq.toString() + ": " + e.what());
    } catch (...) {
        logger_debug("lite persist failed for " + mq.toString());
    }
}

void DefaultLitePullConsumer::persistOffsetTable(const std::set<MessageQueue>& mqs) {
    if (mqs.empty()) return;
    std::vector<std::pair<MessageQueue, int64_t>> toSend;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        for (auto it = offsetTable_.begin(); it != offsetTable_.end();) {
            if (mqs.find(it->first) == mqs.end()) {
                it = offsetTable_.erase(it);
                continue;
            }
            toSend.push_back(*it);
            ++it;
        }
    }
    if (mqClient_ == nullptr) return;
    for (const auto& kv : toSend) {
        try {
            mqClient_->updateConsumerOffset(consumerGroup_, kv.first, kv.second);
        } catch (...) {
            logger_debug("lite persist failed for " + kv.first.toString());
        }
    }
}

void DefaultLitePullConsumer::commit() {
    std::map<MessageQueue, int64_t> targets;
    std::set<MessageQueue> scope;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        scope = assigned_;
        for (const MessageQueue& mq : assigned_) {
            auto it = consumeOffset_.find(mq);
            targets[mq] = (it == consumeOffset_.end()) ? -1 : it->second;
        }
    }
    commitOffsets(targets, scope, true);
}

void DefaultLitePullConsumer::commit(const std::map<MessageQueue, int64_t>& offsets,
                                    bool persist) {
    if (offsets.empty()) {
        logger_warn("MessageQueues is empty, Ignore this commit ");
        return;
    }
    std::set<MessageQueue> scope;
    for (const auto& kv : offsets) scope.insert(kv.first);
    commitOffsets(offsets, scope, persist);
}

void DefaultLitePullConsumer::commit(const std::set<MessageQueue>& messageQueues, bool persist) {
    if (messageQueues.empty()) return;
    std::map<MessageQueue, int64_t> targets;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        for (const MessageQueue& mq : messageQueues) {
            auto it = consumeOffset_.find(mq);
            targets[mq] = (it == consumeOffset_.end()) ? -1 : it->second;
        }
    }
    commitOffsets(targets, messageQueues, persist);
}

void DefaultLitePullConsumer::commitOffsets(const std::map<MessageQueue, int64_t>& targets,
                                           const std::set<MessageQueue>& scope, bool persist) {
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        for (const auto& kv : targets) {
            if (kv.second == -1) {
                logger_error("consumerOffset is -1 in messageQueue [" + kv.first.toString() + "].");
                continue;
            }
            if (assigned_.find(kv.first) == assigned_.end()) {
                continue;
            }
            offsetTable_[kv.first] = kv.second;
        }
    }
    if (persist) persistOffsetTable(scope);
}

int64_t DefaultLitePullConsumer::offsetForTimestamp(const MessageQueue& mq, int64_t timestamp) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    return mqClient_->searchOffsetByTimestamp(mq, timestamp);
}

int64_t DefaultLitePullConsumer::pullCursorOf(const MessageQueue& mq) {
    std::lock_guard<std::mutex> lk(stateMutex_);
    auto it = nextOffset_.find(mq);
    return it == nextOffset_.end() ? -1 : it->second;
}

int64_t DefaultLitePullConsumer::consumeCursorOf(const MessageQueue& mq) {
    std::lock_guard<std::mutex> lk(stateMutex_);
    auto it = consumeOffset_.find(mq);
    return it == consumeOffset_.end() ? -1 : it->second;
}

// ---------------------------------------------------------------- Queue lookup / control
std::vector<MessageQueue> DefaultLitePullConsumer::fetchMessageQueues(const std::string& topic) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    std::string fullTopic = withNamespace(topic);
    try {
        mqClient_->updateTopicRouteInfoFromNameServer(fullTopic);
    } catch (const std::exception& e) {
        // On route jitter fall back to the currently cached route and test again (the outer catch reports "still no queues" as one error log).
        logger_debug("fetch queues: route refresh for " + fullTopic + " failed: " + e.what());
    }
    std::vector<MessageQueue> queues = mqClient_->getTopicSubscribeInfo(fullTopic);
    if (queues.empty()) {
        throw MQClientException("Can not find Message Queue for this topic, " + fullTopic +
                                " Namesrv return empty");
    }
    return queues;
}

std::vector<MessageQueue> DefaultLitePullConsumer::assignment() {
    std::lock_guard<std::mutex> lk(stateMutex_);
    return std::vector<MessageQueue>(assigned_.begin(), assigned_.end());
}

void DefaultLitePullConsumer::pause(const std::vector<MessageQueue>& messageQueues) {
    std::lock_guard<std::mutex> lk(stateMutex_);
    paused_.insert(messageQueues.begin(), messageQueues.end());
}

void DefaultLitePullConsumer::resume(const std::vector<MessageQueue>& messageQueues) {
    std::lock_guard<std::mutex> lk(stateMutex_);
    for (const MessageQueue& mq : messageQueues) {
        paused_.erase(mq);
    }
}

// ---------------------------------------------------------------- Heartbeat
HeartbeatData DefaultLitePullConsumer::buildHeartbeat() const {
    HeartbeatData hb(clientId_);
    ConsumerData cd(consumerGroup_, ConsumeType::CONSUME_ACTIVELY, messageModel_,
                    consumeFromWhere_);
    cd.unitMode = unitMode_;
    for (const auto& kv : subscriptionData_) {
        cd.subscriptionDataSet.push_back(kv.second);
    }
    hb.consumerDataSet.push_back(cd);
    return hb;
}

int32_t DefaultLitePullConsumer::sendHeartbeatToAllBroker() {
    if (mqClient_ == nullptr) return 0;
    HeartbeatData hb = buildHeartbeat();
    if (mqClient_->isEnableConcurrentHeartbeat()) {
        return mqClient_->sendHeartbeatConcurrently(hb);
    }
    int32_t ok = 0;
    for (const std::string& addr : mqClient_->getAllBrokerAddrs()) {
        try {
            mqClient_->sendHeartbeat(addr, hb);
            ++ok;
        } catch (...) {
            logger_debug("lite heartbeat to " + addr + " failed");
        }
    }
    return ok;
}

void DefaultLitePullConsumer::heartbeatLoop() {
    while (running_) {
        try {
            sendHeartbeatToAllBroker();
        } catch (...) {
            logger_debug("lite heartbeat loop error");
        }
        // 5s heartbeat interval (same as the push consumer)
        for (int i = 0; i < 50; ++i) {
            if (!running_) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}

}  // namespace rocketmq
