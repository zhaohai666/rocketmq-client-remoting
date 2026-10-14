// Push consumer.
//
// Architecture (three-layer model):
//   1. Pull layer: one pull thread per queue (concurrent long polling -- the broker suspends a
//      long-polling request per queue and returns as soon as a message arrives); pulled messages
//      go into the pending_ buffer (per-queue ProcessQueue state) and the pull cursor advances to nextBeginOffset;
//   2. Dispatch layer: a single dispatch thread takes batches of consumeMessageBatchMaxSize from
//      the buffer and hands them to the MessageListener; RECONSUME_LATER / failed batches are sent
//      back one by one to %RETRY%topic (delay gradient 3+reconsumeTimes; above maxReconsumeTimes the broker moves them to %DLQ%);
//   3. Offset layer: _consume_offsets records the committed consumer offset and every 5s submits it
//      to the broker with UPDATE_CONSUMER_OFFSET (persistAllConsumerOffset); startup first issues QUERY_CONSUMER_OFFSET.
//
// Key engineering points (established against a real broker; do not delete):
//   - Pulling must be parallel per queue: with one thread polling sequentially, the suspend of an
//     idle queue blocks delivery on every other queue (this once caused 'the first batch arrives, everything later is late').
//   - The broker clamps the client's suspend time to its own brokerSuspendMaxTimeMillis, so idle
//     queues still hit periodic client-side timeouts -- this is **benign**; treat it as debug and do not log ERROR.
#ifndef ROCKETMQ_CLIENT_CONSUMER_H
#define ROCKETMQ_CLIENT_CONSUMER_H

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/allocate_strategy.h"
#include "rocketmq/client/consume_executor.h"
#include "rocketmq/client/hook.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/client/trace_dispatcher.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/namespace_util.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/heartbeat.h"

namespace rocketmq {

// Subscription selector (tag / SQL92 expression)
struct MessageSelector {
    std::string type = ExpressionType::TAG;
    std::string expression = "*";

    static MessageSelector byTag(const std::string& tag) {
        return MessageSelector{ExpressionType::TAG, tag};
    }
    static MessageSelector bySql(const std::string& sql) {
        return MessageSelector{ExpressionType::SQL92, sql};
    }
};

// POP redelivery delay-level table (the protocol side interprets it in **seconds**).
// This is not the send-side delay-level table (whose first entry is 1s); do not mix them up.
inline const std::vector<int32_t>& popDelayLevelTable() {
    static const std::vector<int32_t> kTable = {10, 30, 60, 120, 180, 240, 300, 360,
                                                420, 480, 540, 600, 1200, 1800, 3600, 7200};
    return kTable;
}

// Valid range for POP invisibleTime: anything outside it falls back to 60000
constexpr int64_t kMinPopInvisibleTime = 5000;
constexpr int64_t kMaxPopInvisibleTime = 300000;

// Stall-detection threshold (config key `rocketmq.client.pull.pullMaxIdleTime`, default 120000ms):
// if a queue still owned by this instance has not initiated a single pull/pop for longer than this,
// that loop is dead or stuck, and updateProcessQueueTableInRebalance revokes and rebuilds it on
// exactly this criterion (otherwise that queue goes **silent** and never consumes again, and the client reports no error).
constexpr int64_t kPullMaxIdleTime = 120000;

// POP initial-offset mode (protocol parameter ConsumeInitMode)
enum class ConsumeInitMode : int32_t { MIN = 0, MAX = 1 };

// Queue state for POP mode.
//
// Unlike the pull-mode ProcessQueue, POP has **no pulled-but-unconsumed buffer**: a message goes
// straight to a consumer thread once popped, and confirmation relies on ack. Two things are tracked:
//   - waitAckCounter: messages popped but not yet acked and not yet given an extended invisible time;
//     used for flow control;
//   - dropped: whether rebalance has revoked the queue (once revoked, the batch is neither consumed nor acked; the broker revives and redelivers it after invisibleTime expires).
class PopProcessQueue {
public:
    void incFoundMsg(int32_t n) {
        std::lock_guard<std::mutex> lk(lock_);
        waitAckCounter_ += n;
    }
    // Accumulate directly: callers pass a negative number to decrement (decFoundMsg(-msgs.size())).
    void decFoundMsg(int32_t n) {
        std::lock_guard<std::mutex> lk(lock_);
        waitAckCounter_ += n;
    }
    int32_t ack() {
        std::lock_guard<std::mutex> lk(lock_);
        return --waitAckCounter_;
    }
    int32_t waitAckCount() const {
        std::lock_guard<std::mutex> lk(lock_);
        return waitAckCounter_;
    }
    bool isDropped() const { return dropped_.load(); }
    void setDropped(bool v) { dropped_.store(v); }

    // lastPopTimestamp is stamped when the pop is **initiated**, and isPullExpired reads exactly
    // that: POP mode has no 'pull' action, so stall detection uses this timestamp.
    int64_t lastPopTimestamp() const { return lastPopTimestamp_.load(); }
    void setLastPopTimestamp(int64_t v) { lastPopTimestamp_.store(v); }

private:
    mutable std::mutex lock_;
    int32_t waitAckCounter_ = 0;
    std::atomic<bool> dropped_{false};
    std::atomic<int64_t> lastPopTimestamp_{0};
};

// Ack / change-invisible-time target decoded from a POP_CK.
struct PopCkTarget {
    std::string topic;        // real topic restored by getRealTopic from retryFlag
    std::string brokerName;   // CK segment 6
    int32_t queueId = 0;      // CK segment 7
    int64_t offset = 0;       // CK segment 8 = consumeQueue offset
    std::string extraInfo;    // the CK string, echoed back unchanged
};

class DefaultMQPushConsumer {
public:
    explicit DefaultMQPushConsumer(
        const std::string& consumerGroup = MixAll::DEFAULT_CONSUMER_GROUP);
    ~DefaultMQPushConsumer();

    DefaultMQPushConsumer(const DefaultMQPushConsumer&) = delete;
    DefaultMQPushConsumer& operator=(const DefaultMQPushConsumer&) = delete;

    // ---------------- Configuration ----------------
    void setNamesrvAddr(const std::string& addr);
    void setNameServerAddresses(const std::vector<std::string>& addrs);
    void setInstanceName(const std::string& name) { instanceName_ = name; }

    // ---------------- unitName / unitMode / enableStreamRequestType ----------------
    // The three same-named switches of ClientConfig. They must be set before start():
    // unitName / @STREAM determine the shape of clientId, stream decides the hook chain (`ReqT` enters the ACL signature).
    void setUnitName(const std::string& unitName) { unitName_ = unitName; }
    const std::string& unitName() const { return unitName_; }
    // unitMode has three consumption points: the message filtering context, the protocol field
    // `ConsumerData.unitMode` in the heartbeat, and the redelivery request header.
    void setUnitMode(bool unitMode) { unitMode_ = unitMode; }
    bool isUnitMode() const { return unitMode_; }

    // When true every request carries the extension field `ReqT=0` and clientId gains a trailing `@STREAM` segment.
    void setEnableStreamRequestType(bool enable) { enableStreamRequestType_ = enable; }
    bool isEnableStreamRequestType() const { return enableStreamRequestType_; }

    // postSubscriptionWhenPull (default false): when true every PULL_MESSAGE sends the
    // subscription expression with the request (the SUBSCRIPTION flag is set) so the broker filters;
    // off by default -- tag filtering falls back to client-side filterMessagesForDelivery.
    void setPostSubscriptionWhenPull(bool enable) { postSubscriptionWhenPull_ = enable; }
    bool isPostSubscriptionWhenPull() const { return postSubscriptionWhenPull_; }

    // Subscription gating on the pull path: subString is sent with the pull only when the switch
    // is on **and** the mode is **not** class filtering (SUBSCRIPTION flag = subExpression != null).
    // Extracted into a static pure function only so the single decision in the pull loop is unit-testable.
    static bool shouldPostSubscriptionWhenPull(bool postSubscriptionWhenPull,
                                               bool classFilterMode) {
        return postSubscriptionWhenPull && !classFilterMode;
    }
    // pollNameServerInterval (default 30000ms): the route refresh period for topics in use,
    // passed through to MQClientInstance in start().
    void setPollNameServerIntervalMillis(int32_t millis) { pollNameServerIntervalMillis_ = millis; }
    int32_t pollNameServerIntervalMillis() const { return pollNameServerIntervalMillis_; }
    // persistConsumerOffsetInterval (default 5000ms): the background consumer-offset flush period;
    // the final flush of shutdown() is independent of it (raising it only delays the timing, it never drops an offset).
    void setPersistConsumerOffsetIntervalMillis(int32_t millis) {
        persistConsumerOffsetIntervalMillis_ = millis;
    }
    int32_t persistConsumerOffsetIntervalMillis() const {
        return persistConsumerOffsetIntervalMillis_;
    }
    void setMessageModel(const std::string& model) { messageModel_ = model; }
    void setConsumeFromWhere(const std::string& where) { consumeFromWhere_ = where; }
    // suspend/resume: suspend sets the pause flag -- the pull/pop loops postpone the next attempt by
    // 1s when they see it (PULL_TIME_DELAY_MILLS_WHEN_SUSPEND=1000), messages already pulled are still
    // consumed and no offset rolls back; resume clears the flag and runs doRebalance() at once.
    void suspend() { paused_.store(true); }
    void resume();
    bool isPaused() const { return paused_.load(); }
    // Queue allocation strategy. The setter accepts nullptr and start()'s checkConfig rejects it
    // (the error text "allocateMessageQueueStrategy is null" is the contract with callers).
    // Default AllocateMessageQueueAveragely.
    void setAllocateMessageQueueStrategy(std::shared_ptr<AllocateMessageQueueStrategy> strategy);
    // Read back the current allocation strategy (same notion as the RebalanceImpl field of the same name).
    std::shared_ptr<AllocateMessageQueueStrategy> allocateMessageQueueStrategy() const;
    void setConsumeThreadNums(int32_t n);
    // ---- Consumer-thread elasticity ----
    void setConsumeThreadMin(int32_t n);
    void setConsumeThreadMax(int32_t n);
    int32_t getConsumeThreadMin() const { return consumeThreadMin_; }
    int32_t getConsumeThreadMax() const { return consumeThreadMax_; }
    void setAdjustThreadPoolNumsThreshold(int64_t v) { adjustThreadPoolNumsThreshold_ = v; }
    int64_t getAdjustThreadPoolNumsThreshold() const { return adjustThreadPoolNumsThreshold_; }
    // updateCorePoolSize: **silently ignored** when a guard is not satisfied.
    // The return value exists only for unit-test assertions on whether the change really applied (the operation itself returns nothing).
    bool updateCorePoolSize(int32_t corePoolSize);
    int32_t getCorePoolSize() const;
    // computeAccumulationTotal / adjustThreadPool.
    // ⚠ adjustThreadPool is a no-op (inc/dec are empty implementations); real elastic adjustment goes through updateCorePoolSize.
    int64_t computeAccumulationTotal() const;
    void adjustThreadPool();
    // ProcessQueue.msgAccCnt of one queue (summed over all queues when the key is omitted).
    int64_t msgAccCnt(const std::string& key = std::string()) const;
    // Update a queue's msgAccCnt following the rule of ProcessQueue.putMessage
    // (the last message's MAX_OFFSET property minus its queueOffset, updated only when > 0).
    void updateMsgAccCnt(const std::string& key, const std::vector<MessageExt>& msgs);
    // Observation: live worker count / queued task count of the current POP consume executor (0 before it is created).
    int32_t consumeExecutorWorkers() const;
    int32_t consumeExecutorQueued() const;
    // ConsumerRunningInfo, the response body of 307 (GET_CONSUMER_RUNNING_INFO).
    ConsumerRunningInfo consumerRunningInfo();
    // Client-side logic of 220 (RESET_CONSUMER_CLIENT_OFFSET):
    // for assigned queues matching this topic -> clear the in-flight buffer and the pull cursor -> write the
    // new consumer offset -> revoke that queue (persist the new offset + unlock in order) -> rebalance at once and re-pull from the new offset.
    void resetOffset(const std::string& topic, const std::map<MessageQueue, int64_t>& offsetTable);
    // Parse the offsetTable of 220 (both shapes accepted). The broker picks the shape from the requester's language:
    // language=JAVA always gets the map-shaped ResetOffsetBody; language=CPP requesters
    // (admin tooling) get the array-shaped ResetOffsetBodyForC. Omitting the array branch makes such
    // resets a **silent discard** for this consumer: the offset table is untouched and messages keep flowing from the old offset.
    static std::map<MessageQueue, int64_t> parseResetOffsetBody(const Bytes& body);
    // Response data source for 221:
    // returns the **committed consumer offset** table (not the pull cursor); returns everything when topic is empty.
    std::map<MessageQueue, int64_t> getConsumerStatus(const std::string& topic);
    // Local implementation of 309 (CONSUME_MESSAGE_DIRECTLY):
    // really consume one message locally (restoring the retry topic first) and report the result to the admin.
    ConsumeMessageDirectlyResult consumeMessageDirectly(const MessageExt& msg,
                                                        const std::string& brokerName);
    void setMessageListener(std::shared_ptr<MessageListener> listener);
    void setPullBatchSize(int32_t n) { pullBatchSize_ = n; }
    void setPullBatchSizeInBytes(int32_t n) { pullBatchSizeInBytes_ = n; }
    void setConsumeMessageBatchMaxSize(int32_t n) { consumeMessageBatchMaxSize_ = std::max(1, n); }
    // The dispatch layer slices batches with it.
    // The max(1,·) in the setter is a runtime clamp; the gate has a different job (see checkConfigRanges).
    int32_t consumeMessageBatchMaxSize() const { return consumeMessageBatchMaxSize_; }
    void setPullTimeoutMillis(int32_t t) { pullTimeoutMillis_ = t; }
    void setPullSuspendTimeoutMillis(int32_t t) { pullSuspendTimeoutMillis_ = t; }
    void setSuspendCurrentQueueTimeMillis(int32_t t) { suspendCurrentQueueTimeMillis_ = t; }
    void setMaxReconsumeTimes(int32_t n) { maxReconsumeTimes_ = n; }
    void setPullIntervalMillis(int32_t t) { pullIntervalMillis_ = t; }
    // Per-queue 'pulled but unconsumed' message threshold; pulling for that queue pauses above it (pullThresholdForQueue, default 1000)
    void setPullThresholdForQueue(int32_t n) { pullThresholdForQueue_ = n; }
    int32_t pullThresholdForQueue() const { return pullThresholdForQueue_; }
    // Per-queue 'pulled but unconsumed' **byte** threshold, in MiB (pullThresholdSizeForQueue, default 100; <=0 disables it)
    void setPullThresholdSizeForQueue(int32_t n) { pullThresholdSizeForQueue_ = n; }
    int32_t pullThresholdSizeForQueue() const { return pullThresholdSizeForQueue_; }
    // Upper bound on the queueOffset **span** of pulled-but-unconsumed messages (consumeMessageMaxSpan, default 2000; <=0 disables it).
    // Guards the case where the head message keeps failing to consume and everything behind it stretches the offset span.
    void setConsumeConcurrentlyMaxSpan(int64_t n) { consumeConcurrentlyMaxSpan_ = n; }
    int64_t consumeConcurrentlyMaxSpan() const { return consumeConcurrentlyMaxSpan_; }
    // Topic-level cumulative thresholds (computed over **all** queues of this topic in this instance,
    // pullThresholdForTopic / pullThresholdSizeForTopic; default -1 = disabled). This port does not
    // apportion them by queue count; it compares the cumulative value directly.
    void setPullThresholdForTopic(int32_t n) { pullThresholdForTopic_ = n; }
    int32_t pullThresholdForTopic() const { return pullThresholdForTopic_; }
    void setPullThresholdSizeForTopic(int32_t n) { pullThresholdSizeForTopic_ = n; }
    int32_t pullThresholdSizeForTopic() const { return pullThresholdSizeForTopic_; }
    // Whether the consume loop periodically sends HEART_BEAT (enabled by default; failures only warn and never affect consumption)
    // ---- POP mode (5.x lightweight consumption) ----
    // When disabled the original pull long-polling path is used unchanged, behaving exactly as before.
    void setPopMode(bool b) { popMode_ = b; }
    bool popMode() const { return popMode_; }
    // TLS (switch tls.enable; read from env ROCKETMQ_TLS_ENABLE when unset)
    void setTlsEnable(bool b) { tlsEnable_ = b; }
    // TLS strict-verification options (non-empty caCert = strict certificate-chain + hostname verification; clientCert/Key = mTLS).
    // Calling this enables TLS automatically; must happen before start().
    void setTlsOptions(const TlsOptions& opts) {
        tlsOptions_ = opts;
        tlsOptionsSet_ = true;
        tlsEnable_ = true;
    }
    void setSocksProxy(const SocksProxyConfig& proxy) {
        socksProxy_ = proxy;
        socksProxySet_ = true;
    }
    void setPopInvisibleTime(int64_t t) { popInvisibleTime_ = t; }
    int64_t popInvisibleTime() const { return popInvisibleTime_; }
    // Maximum messages per single POP (popBatchNums default 32; the broker answers INVALID_PARAMETER above 32)
    void setPopBatchNums(int32_t n) { popBatchNums_ = n; }
    int32_t popBatchNums() const { return popBatchNums_; }
    // Ceiling for this queue's 'popped but unacked' counter; POP pauses above it (popThresholdForQueue default 96)
    void setPopThresholdForQueue(int32_t n) { popThresholdForQueue_ = n; }
    int32_t popThresholdForQueue() const { return popThresholdForQueue_; }
    // Suspension time of POP long polling. 0 = short polling (the broker returns immediately or NO_NEW_MSG).
    // ⚠ When non-zero the request timeout must exceed it, otherwise the client times out first.
    void setPopPollTimeMillis(int32_t t) { popPollTimeMillis_ = t; }
    int32_t popPollTimeMillis() const { return popPollTimeMillis_; }
    void setPopTimeoutMillis(int32_t t) { popTimeoutMillis_ = t; }
    int32_t popTimeoutMillis() const { return popTimeoutMillis_; }

    // The three following are **pure logic** (they send no request) and are public so they can be
    // tested offline -- popCkTarget is the easiest part of POP to get wrong (retry topic restoration),
    // so it needs unit tests rather than only real-broker coverage.
    // The numeric section of checkConfig: thirteen range/size gates.
    // **The order, the ranges and the error texts are the contract matched with callers: never change them**
    // (the texts carry no FAQUrl.suggestTodo(...) suffix). start() calls it after every null check and
    // before creating the MQClientInstance; public for the same reason as popCkTarget: positive cases (legal boundary values) cannot go through start() offline, since that really connects to a name server.
    void checkConfigRanges() const;
    // Restore the ack target from POP_CK; returns nullopt when the CK is missing or has too few segments (give up the ack and let the broker revive the message).
    std::optional<PopCkTarget> popCkTarget(const MessageExt& msg);
    // Unparsable popTime/invisibleTime is treated as a timeout
    static bool isPopTimeout(int64_t popTime, int64_t invisible);

    void setHeartbeatEnabled(bool b) { heartbeatEnabled_ = b; }
    void setHeartbeatIntervalMillis(int32_t t) { heartbeatIntervalMillis_ = t; }

    // Namespace: when non-empty, topic / group are wrapped with an "ns%" prefix before talking
    // to the broker (start() wraps consumerGroup the same way).
    void setNamespace(const std::string& ns) { namespace_ = ns; }
    const std::string& namespaceOf() const { return namespace_; }
    // The 5.x namespace (protocol field namespaceV2): when non-empty NamespaceRpcHook adds
    // `nsd=true` / `ns=<namespaceV2>` to **every request** and the broker resolves the instance --
    // a different mechanism from setNamespace above (which rewrites to "ns%topic" client-side).
    // The hook chain order is fixed: installed **before** ACL signing, so `nsd`/`ns` are inside the signed content (see composeRequestHooks).
    void setNamespaceV2(const std::string& ns) { namespaceV2_ = ns; }
    const std::string& namespaceV2() const { return namespaceV2_; }

    // ---------------- ACL authentication (rpcHook) ----------------
    // Must be called before start(): the hooks are bound to the MQClientInstance inside start().
    void setRPCHook(std::shared_ptr<RPCHook> hook) { rpcHook_ = std::move(hook); }
    void setCredentials(const std::string& accessKey, const std::string& secretKey,
                        const std::string& securityToken = std::string()) {
        rpcHook_ = std::make_shared<AclClientRPCHook>(
            SessionCredentials(accessKey, secretKey, securityToken));
    }
    const std::shared_ptr<RPCHook>& rpcHook() const { return rpcHook_; }

    // ---------------- Message trace (enableMsgTrace etc.) ----
    // With enableMsgTrace=true, start() creates an AsyncTraceDispatcher (Type=CONSUME) and registers
    // ConsumeMessageTraceHook automatically; all three consume paths (concurrent / orderly / POP) produce SubBefore/SubAfter.
    void setEnableMsgTrace(bool enable) { enableMsgTrace_ = enable; }
    bool isEnableMsgTrace() const { return enableMsgTrace_; }
    // Alias of setEnableMsgTrace (both spellings accepted)
    void setEnableTrace(bool enable) { enableMsgTrace_ = enable; }
    void setTraceTopic(const std::string& t) { traceTopic_ = t; }
    void setTraceMsgBatchNum(int32_t n) { traceMsgBatchNum_ = n; }
    // Decides whether SubAfter gets the extra timestamp + groupName entries (CLOUD omits them; default LOCAL)
    void setAccessChannel(AccessChannel ch) { accessChannel_ = ch; }
    AccessChannel accessChannel() const { return accessChannel_; }
    // Per-batch consume timeout in minutes, used for the ConsumeReturnType.TIME_OUT decision (default 15);
    // the sweep threshold and period of cleanExpiredMsg read the same value (getConsumeTimeout()).
    void setConsumeTimeout(int32_t minutes) { consumeTimeoutMinutes_ = minutes; }
    int32_t consumeTimeoutMinutes() const { return consumeTimeoutMinutes_; }

    void registerConsumeMessageHook(std::shared_ptr<ConsumeMessageHook> hook) {
        if (hook) consumeMessageHookList_.push_back(std::move(hook));
    }
    bool hasConsumeMessageHook() const { return !consumeMessageHookList_.empty(); }
    size_t consumeMessageHookCount() const { return consumeMessageHookList_.size(); }

    // Pre-delivery filter hooks (registerFilterMessageHook / hasFilterMessageHook).
    // Messages removed by a hook: the pull path skips them silently (the offset still advances), the POP path acks them at once.
    void registerFilterMessageHook(std::shared_ptr<FilterMessageHook> hook) {
        if (hook) filterMessageHookList_.push_back(std::move(hook));
    }
    bool hasFilterMessageHook() const { return !filterMessageHookList_.empty(); }
    size_t filterMessageHookCount() const { return filterMessageHookList_.size(); }
    // Number of messages dropped (pull + POP combined), for integration scripts and unit tests
    int64_t filteredMessageCount() const { return filteredMessageCount_.load(); }

    // ---- Pre-delivery filtering (the filtering section of processPullResult plus executeHook) ----
    // The three below are driven directly by unit tests / integration checks (no network).
    // Runs the filter hooks in order and **swallows every exception** (the hook logs its own error);
    // the opposite of CheckForbiddenHook: a broken filter hook must not affect consumption.
    void executeFilterMessageHook(FilterMessageContext& context);
    // Pre-delivery filtering shared by the pull and POP paths:
    //   1) client-side second-pass tag filtering (the broker's codeSet hash filter can collide and let messages through)
    //   2) FilterMessageHook (may rewrite msgList)
    // With a nullptr sub only 2) runs.
    std::vector<MessageExt> filterMessagesForDelivery(const MessageQueue& mq,
                                                      const SubscriptionData* sub,
                                                      const std::vector<MessageExt>& msgs);
    // Set difference original \ kept (the POP path must ack the removed messages).
    // Reference identity is unavailable in C++ (copying a vector destroys it), so the difference is computed by msgId (semantically equivalent).
    static std::vector<MessageExt> droppedMessages(const std::vector<MessageExt>& original,
                                                   const std::vector<MessageExt>& kept);

    std::shared_ptr<AsyncTraceDispatcher> traceDispatcher() const { return traceDispatcher_; }

    const std::string& consumerGroup() const { return consumerGroup_; }
    const std::string& clientId() const { return clientId_; }
    const std::string& messageModel() const { return messageModel_; }
    bool isStarted() const { return started_.load(); }
    int32_t pullTimeoutMillis() const { return pullTimeoutMillis_; }
    int32_t pullSuspendTimeoutMillis() const { return pullSuspendTimeoutMillis_; }
    // Total number of messages consumed successfully (for tests / monitoring)
    int64_t consumedCount() const { return consumedCount_.load(); }
    // Number of successful broker heartbeats (for verifying the heartbeat capability)
    int64_t heartbeatCount() const { return heartbeatCount_.load(); }
    // Number of flow-control hits (for verifying the flow-control capability)
    int64_t flowControlTriggered() const { return flowControlTriggered_.load(); }
    // Keys of the queues currently assigned to this instance (real-broker check that two consumers
    // in one group cover every queue exactly once, no overlap no gap). Key format matches offsetKey: topic + brokerName + queueId.
    std::vector<std::string> assignedQueueKeys() const;
    // All clientIds of the consumer group on a topic (findConsumerIdList), for verifying multi-instance registration.
    std::vector<std::string> consumerIdListOfGroup(const std::string& topic) const;

    // ---------------- Subscription ----------------
    void subscribe(const std::string& topic, const std::string& subExpression = "*");
    void subscribe(const std::string& topic, const MessageSelector& selector);
    void unsubscribe(const std::string& topic);
    std::vector<std::string> subscribedTopics() const;

    // ---------------- Lifecycle ----------------
    void start();
    void shutdown();

    MQClientInstance& client();

    // ---------------- Administration ----------------
    std::vector<MessageQueue> fetchSubscribeMessageQueues(const std::string& topic);
    // Message redelivery (sendMessageBack): never throws internally; returning false means both
    // CONSUMER_SEND_MSG_BACK and the plain-message redelivery fallback failed.
    bool sendMessageBack(const MessageExt& msg, int32_t delayLevel,
                         const std::string& brokerName = std::string());
    // Send one heartbeat to every known broker (sendHeartbeatToAllBrokerWithLock)
    int32_t sendHeartbeatToAllBroker();

    // ---------------- Drive consume dispatch directly from tests (no network) ----------------
    // The ackIndex semantics of the classic consume path fail very quietly (the tail is dropped
    // silently and the offset moves past unconsumed messages), so this small piece must be pinned
    // offline before going to a real broker; that is why it is public.
    // Consume one batch, handling redelivery / suspension; returns whether the consumer offset advanced. key is offsetKey(mq).
    // epoch is the generation of the ProcessQueue this batch was taken from: a mismatch means the queue
    // was revoked and rebuilt (rebalance revocation, OFFSET_ILLEGAL correction, stall self-healing) and then this batch is **neither consumed nor acked** -- simply voided (the isDropped() short circuit).
    bool consumeBatch(const std::string& key, const MessageQueue& mq,
                      const std::vector<MessageExt>& batch,
                      const std::optional<uint64_t>& epoch = std::nullopt);
    // Key of a queue in the buffer and offset tables (topic + brokerName + queueId).
    static std::string offsetKey(const MessageQueue& mq);
    // Pure-function pieces of the broadcast local offset file (LocalFileOffsetStore), opened to unit tests.
    // On-disk format uses 'a MessageQueue object as the JSON key' (verified compatible with files written by the companion tooling):
    // {"offsetTable":{{"brokerName":"b","queueId":0,"topic":"T"}:7,...}}
    static std::string buildLocalOffsetsJson(const std::map<std::string, int64_t>& items,
                                             const std::map<std::string, MessageQueue>& mqMap);
    // Strict JSON (the old flat-map shape) first, then a scan of the object-as-key shape; nullopt when both fail.
    static std::optional<std::map<std::string, int64_t>> parseLocalOffsetsText(
        const std::string& text);
    // Persist / read back at an explicit path (.bak rotation and the atomic .tmp rename all live here); the instance methods delegate to these.
    static void saveLocalOffsetsAt(const std::string& path,
                                   const std::map<std::string, int64_t>& items,
                                   const std::map<std::string, MessageQueue>& mqMap);
    static std::map<std::string, int64_t> loadLocalOffsetsAt(const std::string& path);
    // Preload a queue's 'pulled but unconsumed' buffer (ProcessQueue), to assert what was pushed back to the head after a redelivery failed
    void setPendingMessages(const std::string& key, const std::vector<MessageExt>& msgs);
    std::vector<MessageExt> pendingMessages(const std::string& key) const;
    // Read back a queue's committed consumer offset; nullopt = nothing recorded yet
    std::optional<int64_t> consumeOffset(const std::string& key) const;
    // The two parts of the orderly-consume retry gate; likewise 'wrong fails quietly', so they are exposed to unit
    // tests together with consumeBatch: one missing +1 never reaches the threshold (a poison message spins in place
    // forever), reading -1 as 16 invents dead letters for orderly consumption. Returns 'should this batch stay suspended and retry in place'; true = suspend, false = handed to the broker and the offset may advance.
    bool checkOrderlyReconsumeTimes(std::vector<MessageExt>& msgs);
    int32_t orderlyMaxReconsumeTimes() const;
    // This port's pending buffer is the pending_ deque and a batch is popped from the head **before** dispatch
    // to the listener; the pull cursor already advanced to nextBeginOffset at pull time, so 'the offset does not
    // advance' does not make the broker resend those messages -- retrying them in place requires putting them back
    // explicitly (under ProcessQueue semantics messages stay in the queue state and this step is not needed). Equivalent to makeMessageToConsumeAgain / rollback.
    void requeuePending(const std::string& key, const std::vector<MessageExt>& msgs);
    // Suspension duration of orderly consumption (submitConsumeRequestLater):
    // the value on the context wins, -1 falls back to the consumer configuration, then it is clamped to [10, 30000].
    // A listener passing 0 still waits 10ms (otherwise a listener that returns 'suspend' turns the consume thread into a busy loop) and one hour is only ever 30s.
    // Same 'wrong fails quietly' as the two above, so it is exposed to unit tests (on a real broker it only shows as wrong consumption pacing).
    int32_t orderlySuspendMillis(const ConsumeOrderlyContext& ctx) const;
    // With maxReconsumeTimes=-1 on the concurrent side, getMaxReconsumeTimes falls back to the default 16.
    // Must stay visible **side by side** with the orderly one: the orderly and concurrent paths each resolve their own
    // getMaxReconsumeTimes, so merging them into one constant would either create dead letters for orderly consumption or let concurrent messages redeliver forever.
    int32_t maxReconsumeTimesOrDefault() const;
    // Build the plain message sent to %RETRY%group from the retry count carried by the message. Shared by both
    // redelivery paths, which differ only in the maxReconsumeTimes they use (each path calls its own getMaxReconsumeTimes), so it is passed explicitly.
    Message buildRetryMessage(const MessageExt& msg, int32_t maxReconsumeTimes) const;

    // ---- correctTagsOffset ----
    // When a pull response is NO_NEW_MSG / NO_MATCHED_MSG (not a single deliverable message), lift the
    // committed consumer offset to the response's nextBeginOffset. Background and criteria: see the function
    // comment in the .cpp; both 'which states to correct' and 'when a correction is allowed' live in this one
    // place, so an offline unit test can pin them at once (on a real broker omitting it only shows as: a queue whose tag never matches keeps its offset, and a restart rescans those unwanted messages over and over).
    void correctTagsOffset(const std::string& key, PullStatus status, int64_t nextOffset);
    // ---- OFFSET_ILLEGAL correction ----
    // The broker says 'the offset you requested is illegal' (queue truncated / commitlog expired / a
    // one-off server-side reset -- all answered with PULL_OFFSET_MOVED, which the remoting layer maps to
    // OFFSET_ILLEGAL) and puts the corrected value in nextBeginOffset. Correcting is not just moving the
    // cursor: every message on this queue already fetched but not yet consumed or acked is voided (those
    // fall inside the skipped range, and acking would push the offset back to the illegal value), then the
    // queue is rebuilt at the corrected offset. Four steps: setNextOffset -> setDropped(true) -> { updateAndFreezeOffset + persist
    // + removeProcessQueue } -> rebalanceImmediately. The whole chain lives here; it performs network RPCs, so it must be called on a path **not holding lock_**.
    void handleOffsetIllegal(const std::string& key, int64_t nextOffset);
    // 'Take a batch + register it in flight' / 'finish consuming + deregister': these are exactly the two
    // dispatchLoop uses on the production path (registered under the same lock as pending_ when taken).
    // Missing the registration = the gate cannot see in-flight batches and crash recovery silently skips
    // messages; missing the deregistration = the gate stays shut forever and correctTagsOffset never fires once.
    // When epochOut is non-null it receives the ProcessQueue generation these messages belong to (read in the same critical section as the take, see queueEpoch): a mismatch before ack voids the whole batch (the isDropped criterion).
    std::vector<MessageExt> takeBatchForConsume(const std::string& key,
                                                uint64_t* epochOut = nullptr);
    void finishBatchConsume(const std::string& key);
    // The queue's 'ProcessQueue generation' (the instance identity of a ProcessQueue): +1 on revoke/rebuild,
    // and in-flight batches use it to decide whether they are already voided. Getting it wrong is **silent**
    // (silently duplicated consumption or silently lost acks), so it is exposed to unit tests together with the requestOffset group.
    uint64_t queueEpoch(const std::string& key) const;
    // Whether this queue's committed offset is frozen by an OFFSET_ILLEGAL correction (ControllableOffset's
    // allowToUpdate=false): while frozen, neither ack nor empty-response correction may change it.
    bool offsetFrozen(const std::string& key) const;

    // ---- cleanExpiredMsg (expired sweep of the concurrent consume service + ProcessQueue.cleanExpiredMsg) ----
    // The only recovery path when a listener hangs and never returns: messages held longer than
    // consumeTimeout minutes go back to the broker (delayLevel fixed at 3) for %RETRY% redelivery.
    // Without it, one stuck message pins that queue's offset forever with no exception, log or timeout
    // visible -- on a real broker it only surfaces indirectly as 'a message was sent and never comes
    // back a second time'. Choosing, removing and the cap are all quietly wrong, so the whole thing is
    // exposed to offline unit tests (the real-broker window is minutes; seconds are only reachable with a fake clock).
    // The expiry criterion itself: no CONSUME_START_TIME stamped (empty string) -> not expired; otherwise
    // nowMs - stamp > consumeTimeout*60*1000 (**strictly greater**). nowMs is passed explicitly: the real
    // path supplies currentTimeMillis(), offline cases use a fake clock to pin the boundary.
    // ⚠ A non-numeric stamp makes std::stoll throw (NumberFormatException semantics) and cleanExpireMsg's scheduling shell catches it with a catch-all -- callers must not swallow that semantic.
    static bool isConsumeExpired(const MessageExt& msg, int32_t consumeTimeoutMinutes, int64_t nowMs);
    // One sweep round of a queue (ProcessQueue.cleanExpiredMsg); orderly consumption returns 0 immediately.
    // Returns **the number of redeliveries actually issued this round** (same convention as updateCorePoolSize:
    // the operation itself has no return value and this is only used by unit tests to assert the 16-message cap and the selection order).
    int32_t cleanExpiredQueue(const std::string& key, int64_t nowMs);
    // Sweep every queue **currently held** (walks processQueueTable).
    int32_t cleanExpiredMsgOnce(int64_t nowMs);
    // After a successful redelivery, remove the entry only while it **is still** the head (smallest queueOffset):
    // if the listener happened to finish normally meanwhile (or a smaller offset appeared in front), yield to the normal path.
    void removeExpiredEntryIfStillHead(const std::string& key, const MessageExt& msg);
    // 'On-book' view = in flight (dispatched, not settled) union pulled-not-dispatched, i.e. ProcessQueue.msgTreeMap.
    std::vector<MessageExt> processQueueEntries(const std::string& key) const;
    // Head = the entry with the smallest queueOffset (msgTreeMap.firstEntry()).
    std::optional<MessageExt> processQueueHead(const std::string& key) const;
    // containsMessage semantics: decided **by queueOffset** (not object identity) whether this message is
    // still attached to the queue: the post-listener redelivery must skip entries the sweeper already redelivered, otherwise it is delivered twice.
    bool processQueueContains(const std::string& key, const MessageExt& msg) const;
    // Preload a queue's 'in flight' registration (the part of ProcessQueue already dispatched to a listener),
    // same purpose as setPendingMessages: drive sweeps offline without starting a real dispatch thread.
    void setInflightMessages(const std::string& key, const std::vector<MessageExt>& msgs);
    // Scheduling convention: initialDelay == period == consumeTimeout minutes (default 15 -> 900000ms,
    // both scheduleAtFixedRate arguments come from the same source).
    static int64_t cleanExpirePeriodMillis(int32_t consumeTimeoutMinutes);

    // ---- POP orderly consumption (ConsumeMessagePopOrderlyService, incomplete skeleton) ----
    // This chain is an unfinished skeleton: the request is enqueued for dedup and run() returns as soon as it
    // holds the queue lock (POPTODO) -- messages are **neither consumed nor acked**, and the broker revives and
    // redelivers them when invisibleTime expires, so the macroscopic symptom is 'orderly + POP receives no messages and the backlog never drains'.
    // checkReconsumeTimes / sendMessageBack / submitConsumeRequestLater / processConsumeResult
    // checkReconsumeTimes / sendMessageBack / submitConsumeRequestLater / processConsumeResult in this skeleton
    // are not wired up yet and will be ported once the feature is completed.
    // Unified entry for a popped POP batch: orderly listeners are routed into the submitPopOrderlyRequest
    // skeleton, concurrent listeners go to ConsumeMessagePopConcurrentlyService. Exposed to unit tests together with consumeBatch: misdispatch fails quietly (orderly messages get consumed / concurrent messages disappear).
    void submitPopConsumeRequest(std::vector<MessageExt> msgs,
                                 std::shared_ptr<PopProcessQueue> pq, const MessageQueue& mq);
    // Submit a POP orderly request (submitConsumeRequest): with force=false dedup by
    // (pq, mq) and duplicate requests are dropped outright.
    void submitPopOrderlyRequest(const std::shared_ptr<PopProcessQueue>& pq,
                                 const MessageQueue& mq, bool force);
    // The request body (ConsumeRequest.run): the request is removed only when pq was revoked; otherwise it
    // does nothing once the queue lock is held -- no listener call, no ack.
    void runPopOrderlyRequest(const std::shared_ptr<PopProcessQueue>& pq, const MessageQueue& mq);
    // Size of the request set (consumeRequestSet.size(); the dedup behaviour must be pinnable offline)
    size_t popOrderlyRequestCount() const;

    // Register 'this instance owns this queue' (the key set of ProcessQueueTable).
    // Topic-level flow-control thresholds use it to group the sibling queues of the same topic.
    void setAssignedQueue(const std::string& key, const MessageQueue& mq);
    // Pre-pull flow-control decision (the five ProcessQueue thresholds):
    // returns true when any is hit and records the hit count in flowControlTriggered().
    // A wrong criterion means 'messages pile up in memory without bound / the offset span runs away', problems a
    // real broker run does not reveal quickly, so it must be pinnable offline; same reason as consumeBatch for keeping it public.
    bool flowControlHit(const MessageQueue& mq, const std::string& key);
    // Record one POP round into the pull statistics (the PopCallback.onSuccess of popMessage,
    // same convention as the pull path's PullCallback): on FOUND record pullRT (recorded before the
    // empty check), and record pullTPS only when messages were popped. These two cells are the sole
    // source of the statusTable in the 307 response: omit them and consumption stays perfectly healthy
    // while the dashboard reads all zeros, which on a real broker only shows after two full 10s sampling windows -- so the decision itself must accept a PopResult fed directly offline.
    void recordPopPullStats(ConsumerStatsManager& stats, const std::string& topic,
                            const PopResult& result, int64_t beganMs);

    // ---- Observation / injection surface of stall self-healing (isPullExpired) ----
    // Getting the stall criterion's direction wrong is a **silent** failure: a threshold too small keeps
    // revoking healthy queues and redelivering them (inventing duplicates); one too large, or no check at all,
    // leaves queues whose loop died unconsumed forever. A short real-broker run cannot tell the two apart, so the threshold arithmetic and the timestamp tables must be directly readable and writable from offline unit tests.
    bool pullStalled(const std::string& key) const;
    // Millisecond timestamp of this queue's last initiated pull/pop; -1 = nothing recorded yet
    int64_t lastPullAt(const std::string& key) const;
    // Inject a timestamp (fault injection against a real broker): moving it outside the threshold is equivalent to 'this loop is stuck'
    void setLastPullAt(const std::string& key, int64_t millis);
    // Immediately revoke/build pull threads according to the current assignment set (updateProcessQueueTableInRebalance)
    void syncPullThreads();

private:
    // Pulling: one thread per queue (concurrent long-polling semantics: the broker suspends a long-polling
    // request per queue and returns as soon as a message arrives; with one thread polling sequentially, the
    // suspend of an empty queue blocks delivery on the other queues).
    // Uniform wrapper for the resident loop threads: exceptions are caught at the boundary (an exception escaping
    // a thread means std::terminate and the whole process goes down with it), and while the consumer is still running the loop is re-entered after 1s, so one stray exception does not turn into a silent 'no more consumption' stall.
    void runLoop(const std::string& what, const std::function<void()>& body);
    void rebalancePullThreads();
    void rebalanceLoop();
    void queuePullLoop(const MessageQueue& mq, uint64_t token);
    // Dispatch: a single thread takes batches from each queue buffer and hands them to the listener
    void dispatchLoop();
    // From base, redeliver batch[base, size) one by one (processConsumeResult -> sendMessageBack):
    // base is the start index of the tail within the whole batch (with partial acks the prefix was
    // already accepted and must not be redelivered).
    // Returns the redelivery **failures** as (index in the whole batch, message); a failed entry already
    // has reconsumeTimes +1 (the broker did not record that count, and without the client compensating it can never reach the DLQ); the caller pushes the tail back to the head and clamps the offset accordingly.
    std::vector<std::pair<size_t, MessageExt>> sendBackBatch(
        const std::string& key, const std::vector<MessageExt>& batch,
        const ConsumeConcurrentlyContext& ctx, size_t base);
    // Concurrently consume one batch (the listener / redelivery / offset section of ConsumeRequest.run).
    // Extracted only so consumeBatch can wrap 'register in flight -> consume -> deregister in flight' with RAII:
    // any return along the way must deregister, otherwise the registry can never be cleaned up.
    // epoch is passed through unchanged to the offset advance (the ack voiding criterion, see advanceConsumeOffset).
    bool consumeConcurrentBatch(const std::string& key, const MessageQueue& mq,
                                std::vector<MessageExt>& restored, bool broadcast,
                                const std::optional<uint64_t>& epoch);
    // Scheduling loop of cleanExpiredMsg (fixed rate: delay == period == consumeTimeout minutes).
    void cleanExpiredMsgLoop();
    // Lock / unlock of the 'in flight' registry (must share lock_ with pending_: the union view has to be read atomically).
    void registerInflight(const std::string& key, const std::vector<MessageExt>& msgs);
    void deregisterInflight(const std::string& key, const std::vector<MessageExt>& msgs);
    std::vector<MessageExt> processQueueEntriesLocked(const std::string& key) const;
    std::optional<MessageExt> processQueueHeadLocked(const std::string& key) const;
    bool processQueueContainsLocked(const std::string& key, const MessageExt& msg) const;
    // Remove one entry by queueOffset (from both inflight and pending: the moment a failed redelivery is
    // pushed back into the buffer it exists on both sides, and removing only one lets the next sweep see it again).
    void removeProcessQueueEntryLocked(const std::string& key, const MessageExt& msg);
    // Advance the offset to the largest queueOffset in the batch plus 1; when floor is non-null never pass it
    // (ProcessQueue.removeMessage semantics: while the tree still holds unconsumed messages, the committed
    //  offset must stay at firstKey, otherwise they are silently dropped). An empty batch returns immediately.
    // When epoch is non-null the batch's generation is compared first: on a mismatch, or if the offset is frozen, the whole ack is voided (the isDropped criterion).
    void advanceConsumeOffset(const std::string& key, const std::vector<MessageExt>& batch,
                              const std::optional<int64_t>& floor = std::nullopt,
                              const std::optional<uint64_t>& epoch = std::nullopt);
    // The three below require the caller to already hold lock_ (production paths call them inside the
    // existing critical section; the three public same-named functions are only locking wrappers so unit tests can drive them directly).
    void correctTagsOffsetLocked(const std::string& key, PullStatus status, int64_t nextOffset);
    std::vector<MessageExt> takeBatchLocked(const std::string& key);
    void finishBatchLocked(const std::string& key);
    // Drop all local state of a queue (the caller must hold lock_); the committed offset is handed to the caller
    // through retired for persistence outside the lock. Clears the thread table / buffer / cursor / offset, bumps
    // the generation +1 (every in-flight batch's ack is voided) and keeps the freeze marker until the queue is rebuilt
    // (the unfreeze branch of rebalancePullThreads) -- otherwise the corrected offset would be overwritten by an old ack. Two call sites: revoking a queue during rebalance (including stall self-healing) and OFFSET_ILLEGAL correction.
    void retireQueueLocked(const std::string& key, const MessageQueue& fallbackMq,
                           std::vector<std::pair<MessageQueue, int64_t>>& retired);
    // Current ProcessQueue generation of this queue; no record means 0 (matching takeBatchLocked's default fill)
    uint64_t queueEpochLocked(const std::string& key) const;
    // Redelivery fallback (sendMessageBackAsNormalMessage)
    void sendMessageBackAsNormalMessage(const MessageExt& msg);
    // The orderly-consume redelivery gate (checkReconsumeTimes / sendMessageBack on the orderly
    // path), not the same path as the concurrent one above
    bool orderlySendMessageBack(const MessageExt& msg);
    // Offset persistence: every 5s submit the committed consumer offset to the broker (persistAllConsumerOffset)
    void offsetPersistLoop();
    void persistOffsetsOnce();
    // Broadcast-mode local offset file (LocalFileOffsetStore)
    std::string localOffsetPath() const;
    void saveLocalOffsets();
    // Fold the offsets carried down by revoke finalization (no longer in consumeOffsetTable_/mqMap_) into the
    // same flush: for broadcast revoke, persist(mq) runs before removeOffset(mq) and the file must keep the
    // value, otherwise when a queue with the same name is assigned back the offset cannot be read and consumeFromWhere rescans it.
    void saveLocalOffsets(const std::vector<std::pair<MessageQueue, int64_t>>& extra);
    std::map<std::string, int64_t> loadLocalOffsets() const;
    // Orderly consumption: broker queue locks (lockMQ, every 20s)
    void lockLoop();
    bool isOrderly() const;
    void maybeSendHeartbeat();
    // Immediate action after the subscription table grows or changes (the
    // sendHeartbeatToAllBrokerWithLock at the end of subscribe): register the route refresh plus push one synchronous heartbeat round. unsubscribe does not call this.
    void notifySubscriptionChanged(const std::string& topic);

    std::vector<MessageQueue> assignedQueues();
    int64_t resolveInitialOffset(const MessageQueue& mq, const SubscriptionData& sub);

    // ---- Real rebalance (rebalanceByTopic) ----
    // Compute the queue set this instance should hold and write it into assignedQueues_, then sync the pull
    // threads; newly assigned queues have their **initial offset resolved immediately** and written into offsetTable_.
    void doRebalance();
    // 'All queues of the topic' inside the current assignment (topicSubscribeInfoTable).
    std::vector<MessageQueue> allQueuesOfTopic(const std::string& topic);
    // Whether this pull thread still owns the queue (invalid once rebalance revoked it or replaced the thread).
    // The token is written into pullOwners_ **before the thread is started**: std::thread runs as soon as it is
    // constructed, and if registration waited for construction the new thread might reach ownsQueue() first, see
    // 'I am not in that table yet' and exit on the spot, while the queue is recorded as 'already has a pull thread' -- so nobody would ever pull it again (root cause of a lost batch observed on a real broker).
    bool ownsQueue(const std::string& key, uint64_t token) const;
    // Stamp the timestamp each time a pull/pop is **initiated** (the stamping sites of pullMessage/popMessage:
    // before flow control and the lock decision -- the criterion is 'this loop is still running', not 'this round really hit the network').
    void stampPullAt(const std::string& key);
    // Whether this queue has stalled (the isPullExpired criterion). The caller must hold lock_.
    bool pullStalledLocked(const std::string& key) const;
    // The loop function returned yet still owns this queue (it was not revoked by rebalance): mark it stalled
    // so the next rebalance round revokes and rebuilds it. A finished std::thread cannot be probed from outside
    // (joinable() is still true), so the thread wrapper reports its own exit.
    void markPullLoopExited(const std::string& key, uint64_t token);
    // Finalization when a queue is revoked (removeUnnecessaryMessageQueue): persist the committed offset,
    // discard the pending buffer, and unlock for orderly consumption in cluster mode. revoked is a list of (queue, committed offset) pairs.
    void onQueuesRevoked(const std::vector<std::pair<MessageQueue, int64_t>>& revoked);
    // The broker signals a consumer-group instance change -> rebalance at once (rebalanceImmediately).
    // Called by name for each consumer from MQClientInstance's processor 40 (registered per instance, see start()).
    void wakeRebalanceLoop();
    // Before dispatch, restore a retry message's topic to the original business topic (resetRetryAndNamespace).
    void resetRetryTopicAndNamespace(std::vector<MessageExt>& msgs);

    // ---- POP consume loop (5.x lightweight consumption: popMessage callback + POP concurrent consume service) ----
    // POP loop of a single queue. It **neither queries nor commits consumer offsets**: progress is tracked by the broker-side checkpoint and confirmation relies on ack alone.
    void queuePopLoop(const MessageQueue& mq, uint64_t token);
    // Consume one batch and, based on the result, ack or extend the invisible time
    void consumePopBatch(std::vector<MessageExt> msgs,
                         std::shared_ptr<PopProcessQueue> pq, const MessageQueue& mq);
    void processPopConsumeResult(ConsumeConcurrentlyStatus status,
                                 const ConsumeConcurrentlyContext& ctx,
                                 std::vector<MessageExt>& msgs,
                                 const std::shared_ptr<PopProcessQueue>& pq);
    // Fallback once the retry count is exhausted (checkNeedAckOrDelay)
    void checkNeedAckOrDelay(const MessageExt& msg);
    // The caller must already hold lock_ (the pull loop calls it directly inside its enqueue critical section)
    void updateMsgAccCntLocked(const std::string& key, const std::vector<MessageExt>& msgs);
    void ackPopMsg(const MessageExt& msg);
    void changePopInvisibleTime(const MessageExt& msg, int32_t delayLevel);

    // ---- Consume hooks / trace ----
    // Build the consume hook context (initial values: success=false, props empty)
    ConsumeMessageContext buildConsumeHookContext(const std::vector<MessageExt>& msgs,
                                                  const MessageQueue& mq) const;
    void executeConsumeHookBefore(ConsumeMessageContext& context);
    void executeConsumeHookAfter(ConsumeMessageContext& context);
    // Consume result -> ConsumeReturnType name (written into props, deciding the SubAfter contextCode of the trace).
    // The decision order is an established contract and must not be reordered.
    static const char* consumeReturnTypeOf(bool hasException, int64_t consumeRtMs,
                                           bool failed, bool succeeded);
    // Finalization: write props/status/success then fire the after hook (the executeHookAfter section)
    void finishConsumeHook(ConsumeMessageContext* hookCtx, bool hasException, int64_t beginMs,
                           bool failed, bool succeeded, const std::string& statusText);
    // Consumer-side RT/TPS accounting (ConsumeRequest.run: RT is always recorded, OK/FAILED by outcome).
    // Only the **concurrent classic** path passes ackCount (splitting ok/failed at ackIndex+1; the tail of a
    // partial ack is recorded FAILED and also redelivered); POP and orderly consumption pass nullopt = the whole batch shares one status.
    void recordConsumeStats(const std::string& topic, int64_t msgCount, int64_t beginMs,
                            bool failed, const std::optional<int64_t>& ackCount = std::nullopt);
    // Record RT only (incConsumeRT lives **outside** processConsumeResult and always
    // records): orderly consumption records no TPS at all on the three SUCCESS/COMMIT/ROLLBACK branches
    // with autoCommit=false, but RT is still recorded.
    void recordConsumeRt(const std::string& topic, int64_t beginMs);
    // In start(), build the dispatcher per enableMsgTrace and register ConsumeMessageTraceHook
    void startTraceDispatcher();

    std::string consumerGroup_;
    // Queue allocation strategy (same notion as RebalanceImpl.allocateMessageQueueStrategy, passed in at construction).
    // Default AllocateMessageQueueAveragely.
    std::shared_ptr<AllocateMessageQueueStrategy> allocateStrategy_;
    std::string namespace_;
    // namespaceV2 (protocol field): when non-empty NamespaceRpcHook adds the nsd/ns headers to every request
    std::string namespaceV2_;
    // ACL hook, bound to MQClientInstance's transport layer in start()
    std::shared_ptr<RPCHook> rpcHook_;
    std::string instanceName_ = "DEFAULT";
    std::string clientId_;
    std::string unitName_;
    bool unitMode_ = false;
    // This SDK's convention: pull / lite consumers set true in **every constructor**; producers and push consumers stay false.
    bool enableStreamRequestType_ = false;

    // postSubscriptionWhenPull (default false). Pull-side gating for push consumers:
    // subString is sent with PULL_MESSAGE only when it is on **and** the mode is **not** class filtering.
    bool postSubscriptionWhenPull_ = false;
    // Facade-level copies of the ClientConfig fields (each passed through / read once in start())
    int32_t pollNameServerIntervalMillis_ = 30000;
    int32_t persistConsumerOffsetIntervalMillis_ = 5000;
    std::string messageModel_ = MessageModel::CLUSTERING;
    std::string consumeFromWhere_ = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;

    // ---- Consumer thread pool (consumeThreadMin/Max) ----
    // The current default is min=20 **and** max=20 -- both sides are 20 (only the older generation
    // used min=20/max=64; this port once used that older pair).
    // The **pull** path is 'one pull thread per queue + a single dispatch thread',
    // and only the **POP** path uses a real thread pool (the POP concurrent consume service),
    // so corePoolSize directly decides POP consume concurrency (with unbounded queue semantics max is never reached);
    // under the default configuration updateCorePoolSize can only move **down** (guard n < max = 20).
    int32_t consumeThreadMin_ = 20;
    int32_t consumeThreadMax_ = 20;
    // adjustThreadPoolNumsThreshold defaults to 100000 (automatic elastic threshold; the inc/dec paths are empty implementations, so nothing scales automatically)
    int64_t adjustThreadPoolNumsThreshold_ = 100000;
    // Declarative core pool size (equivalent of setCorePoolSize), default = consumeThreadMin
    int32_t corePoolSize_ = 20;
    // key -> ProcessQueue.msgAccCnt (backlog count computed by the most recent pull)
    mutable std::map<std::string, int64_t> msgAccCntTable_;
    // POP consume executor (created when started with popMode_; shut down on stop)
    std::shared_ptr<ConsumeExecutor> popConsumeExecutor_;
    // Dedup set of POP orderly requests (same notion as ConcurrentSet<ConsumeRequest>, equality =
    // (shardingKeyIndex, pq reference, mq); shardingKeyIndex is always 0 and pq compares by pointer identity --
    // after rebalance revokes a queue and installs a new PopProcessQueue, new requests enqueue as usual)
    std::set<std::pair<const PopProcessQueue*, MessageQueue>> popOrderlyRequests_;

    int32_t pullBatchSize_ = 32;
    int32_t pullBatchSizeInBytes_ = 256 * 1024;
    int32_t consumeMessageBatchMaxSize_ = 1;
    int32_t pullTimeoutMillis_ = 30000;
    int32_t pullSuspendTimeoutMillis_ = 15000;
    int32_t suspendCurrentQueueTimeMillis_ = 1000;
    int32_t maxReconsumeTimes_ = -1;
    int32_t pullIntervalMillis_ = 0;
    int32_t pullThresholdForQueue_ = 1000;
    // Per-queue **byte** cap of pulled-but-unconsumed messages (MiB), pullThresholdSizeForQueue default 100
    int32_t pullThresholdSizeForQueue_ = 100;
    // Per-queue cap of the queueOffset span of pulled-but-unconsumed messages, consumeMessageMaxSpan default 2000
    int64_t consumeConcurrentlyMaxSpan_ = 2000;
    // Topic-level cumulative thresholds (count / MiB), default -1 = disabled
    int32_t pullThresholdForTopic_ = -1;
    int32_t pullThresholdSizeForTopic_ = -1;

    // ---- POP mode (5.x lightweight consumption) ----
    bool popMode_ = false;
    bool tlsEnable_ = MQClientInstance::tlsEnabledFromEnv();
    TlsOptions tlsOptions_;
    bool tlsOptionsSet_ = false;
    // SOCKS proxy (passed through to MQClientInstance -> RemotingClient in start())
    SocksProxyConfig socksProxy_;
    bool socksProxySet_ = false;
    int64_t popInvisibleTime_ = 60000;
    int32_t popBatchNums_ = 32;
    int32_t popThresholdForQueue_ = 96;
    int32_t popPollTimeMillis_ = 15000;
    int32_t popTimeoutMillis_ = 25000;
    std::vector<int32_t> popDelayLevel_ = popDelayLevelTable();
    // queue key -> PopProcessQueue (popped-but-unacked counter + whether revoked by rebalance)
    std::map<std::string, std::shared_ptr<PopProcessQueue>> popQueues_;

    bool heartbeatEnabled_ = true;
    int32_t heartbeatIntervalMillis_ = 30000;

    std::vector<std::string> nameServerAddrs_;
    mutable std::mutex lock_;
    std::map<std::string, SubscriptionData> subscriptionData_;
    std::shared_ptr<MessageListener> messageListener_;
    // Pull cursor (nextBeginOffset)
    std::map<std::string, int64_t> offsetTable_;
    // Committed consumer offset (what gets persisted periodically; the commitOffset after ProcessQueue.removeMessage)
    std::map<std::string, int64_t> consumeOffsetTable_;
    std::map<std::string, MessageQueue> mqMap_;
    // Pulled-but-unconsumed buffer (a simplified ProcessQueue)
    std::map<std::string, std::deque<MessageExt>> pending_;
    // **Message bodies** of in-flight batches (the part of ProcessQueue.msgTreeMap already dispatched to a
    // listener and not yet acked). pending_ (pulled but undispatched) unioned with it is the complete
    // msgTreeMap view: both the head-only sweep of cleanExpiredMsg and the containsMessage check before redelivery read this union.
    std::map<std::string, std::vector<MessageExt>> inflightMsgs_;
    // Count of in-flight batches (the part of ProcessQueue.msgCount **currently being consumed by a
    // listener**): +1 when dispatchLoop takes a batch, -1 when consumption finishes (including pushing back
    // after an exception). pending_ together with it is the complete getMsgCount() view, and the gate in correctTagsOffsetLocked looks at both.
    std::map<std::string, int> inflightCount_;
    // The queue's current ProcessQueue generation (instance identity of a ProcessQueue). +1 on revoke/rebuild:
    // an in-flight batch carries the generation it was taken with and, if the comparison fails before the ack,
    // the whole batch is voided (the !processQueue.isDropped() criterion).
    std::map<std::string, uint64_t> queueEpoch_;
    // Offsets frozen by an OFFSET_ILLEGAL correction (ControllableOffset.allowToUpdate=false):
    // while frozen, neither ack nor empty-response correction may change them; unfrozen when the queue is rebuilt.
    std::set<std::string> frozenOffsets_;
    // Orderly consumption: queue keys whose broker LOCK_BATCH_MQ confirmation reported a successful lock
    std::set<std::string> lockOk_;

    std::unique_ptr<MQClientInstance> mqClient_;
    std::atomic<bool> started_{false};
    std::atomic<bool> stop_{false};
    // Pause flag (volatile boolean, default false)
    std::atomic<bool> paused_{false};
    std::map<std::string, std::thread> pullThreads_;
    // The current pull thread's 'ownership credential' per queue: registered **before the thread is
    // started** and checked by the thread on every round (see ownsQueue). std::thread begins running as soon
    // as it is constructed, so 'insert first, start second' is impossible; ownership cannot be resolved from the thread id and must rely on this pre-written table.
    std::map<std::string, uint64_t> pullOwners_;
    uint64_t nextPullToken_ = 0;
    // Millisecond timestamp of each queue's last initiated pull/pop (ProcessQueue.lastPullTimestamp /
    // PopProcessQueue.lastPopTimestamp). rebalance uses it to decide whether pulling stalled (kPullMaxIdleTime).
    // 0 = the loop returned by itself while still owning the queue (an exception broke through); the next round treats it as stalled and rebuilds it.
    std::map<std::string, int64_t> lastPullAt_;
    // pullFromWhichNodeTable (key = this port's offsetKey): suggestWhichBrokerId from each pull response
    // header is written back here, and the next pull selects master/slave accordingly (default MASTER_ID=0).
    std::map<std::string, int64_t> pullFromWhichNode_;
    // Old pull threads of revoked queues (already detached from pullThreads_, reaped once they exit naturally)
    std::vector<std::thread> retiredThreads_;
    std::thread dispatchThread_;
    std::thread persistThread_;
    std::thread lockThread_;
    std::thread rebalanceThread_;
    // Sweep thread of cleanExpiredMsg (single-threaded cleanExpireMsgExecutors scheduling; not created for orderly / POP).
    std::thread cleanExpireThread_;
    // Queue set of this instance as computed by the real rebalance (the key set of ProcessQueueTable).
    // Replaces 'every queue of every subscribed topic' from the old implementation, which made multiple instances in one group consume the same messages twice.
    std::vector<MessageQueue> assignedQueues_;
    std::condition_variable cv_;
    std::mutex waitMutex_;
    // Immediate-rebalance signal (set when the broker sends NOTIFY_CONSUMER_IDS_CHANGED)
    std::atomic<bool> rebalanceNow_{false};
    std::mutex rebalanceMutex_;
    std::condition_variable rebalanceCv_;
    int64_t startMillis_ = 0;
    std::atomic<int64_t> consumedCount_{0};
    std::atomic<int64_t> heartbeatCount_{0};
    std::atomic<int64_t> lastHeartbeatMs_{0};
    std::atomic<int64_t> flowControlTriggered_{0};

    // ---------------- Message trace / consume hooks ----------------
    bool enableMsgTrace_ = false;
    std::string traceTopic_;                      // empty => use MixAll::TRACE_TOPIC
    int32_t traceMsgBatchNum_ = 10;
    int32_t consumeTimeoutMinutes_ = 15;
    AccessChannel accessChannel_ = AccessChannel::LOCAL;
    std::vector<std::shared_ptr<ConsumeMessageHook>> consumeMessageHookList_;
    std::vector<std::shared_ptr<FilterMessageHook>> filterMessageHookList_;
    std::atomic<int64_t> filteredMessageCount_{0};
    std::shared_ptr<AsyncTraceDispatcher> traceDispatcher_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_CONSUMER_H
