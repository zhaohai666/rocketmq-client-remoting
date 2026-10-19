// Scheduled pull consumer: register a PullTaskCallback per topic; a resident scheduler thread
// owns the queue-dimension pull scheduling (which queue's round is due, and when the next one runs)
// (detector-thread style: stop flag + sleepUntilDeadline, see src/client/schedule_util.h); it periodically
// recomputes **this instance's share** of every registered topic (fetchMessageQueuesInBalance for
// CLUSTERING / fetchSubscribeMessageQueues for BROADCASTING -- this service has no rebalance thread, so the
// refresh doubles as the queue-change notification), fans the share out into queue-dimension tasks
// (putTask) and dispatches due tasks onto a ConsumeExecutor worker pool ("PullMsgThread", pullThreadNums
// wide, 20 by default).
//
// Per-queue execution is **serial** (a running flag gates re-dispatch); different queues run in parallel.
// Each round the service pulls on the task's behalf (offset bookkeeping included: the first round starts
// from fetchConsumeOffset or maxOffset, later rounds follow the reply's nextBeginOffset, and
// the offset is fed into updateConsumeOffset), then hands the PullResult to doPullTask.
//
// When the next round runs is decided by three channels, in strict priority order:
//   1. the doPullTask **return value** (>= 0 = delay millis) -- highest;
//   2. PullTaskContext::pullMessageLater(delayMillis) -- a one-shot deferral of just the next round;
//   3. PullTaskContext::setPullInterval(millis) -- the interval channel for every following round;
//   4. none used -> status default: FOUND = 0 (immediately), empty/illegal reply =
//      pullTimeDelayMillisWhenException (3s by default); a failed service pull or a throwing callback =
//      pullTaskExceptionDelayMillis (1000ms by default, which a callback exception also lands on).
//
// shutdown() stops the scheduler first, then **drains** the in-flight tasks (ConsumeExecutor::shutdown(true))
// before the consumer goes down, so no callback is cut in half.
#ifndef ROCKETMQ_CLIENT_PULL_SCHEDULE_SERVICE_H
#define ROCKETMQ_CLIENT_PULL_SCHEDULE_SERVICE_H

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consume_executor.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"

namespace rocketmq {

// The per-round context, carrying the three scheduling channels (see the file header for the
// priority order). A fresh context is built for every round, so one round leaks nothing into the next.
class PullTaskContext {
public:
    // Channel 3: the delay to the next round when neither the return value nor pullMessageLater is used.
    void setPullInterval(int32_t millis) {
        pullInterval_ = millis;
        intervalSet_ = true;
    }
    int32_t pullInterval() const { return pullInterval_; }
    bool pullIntervalSet() const { return intervalSet_; }

    // Channel 2: one-shot deferral of the next round; the interval for the rounds after it is unchanged.
    void pullMessageLater(int32_t delayMillis) {
        laterDelay_ = delayMillis;
        laterSet_ = true;
    }
    int32_t pullLaterDelay() const { return laterDelay_; }
    bool pullLaterSet() const { return laterSet_; }

    // The service-owned consumer, so a callback can pull other queues / manage offsets itself
    // through the same client the service pulls with.
    DefaultMQPullConsumer* pullConsumer() const { return consumer_; }
    void setPullConsumer(DefaultMQPullConsumer* consumer) { consumer_ = consumer; }

private:
    int32_t pullInterval_ = 0;
    bool intervalSet_ = false;
    int32_t laterDelay_ = 0;
    bool laterSet_ = false;
    DefaultMQPullConsumer* consumer_ = nullptr;
};

// The per-topic task callback. Differences forced by this scheduling design:
//   * the service pulls **before** the callback runs and passes the PullResult in (mutable, so a
//     callback may even rewrite what it hands to its own downstream);
//   * doPullTask returns the highest-priority scheduling channel: the next delay in millis, or
//     kUseContextDelay (-1) to defer to the PullTaskContext channels / status default.
class PullTaskCallback {
public:
    static constexpr int32_t kUseContextDelay = -1;

    virtual ~PullTaskCallback() = default;
    virtual int32_t doPullTask(const MessageQueue& mq, PullResult& pullResult,
                               PullTaskContext& context) = 0;
};

class MQPullConsumerScheduleService {
public:
    explicit MQPullConsumerScheduleService(const std::string& consumerGroup);
    ~MQPullConsumerScheduleService();

    MQPullConsumerScheduleService(const MQPullConsumerScheduleService&) = delete;
    MQPullConsumerScheduleService& operator=(const MQPullConsumerScheduleService&) = delete;

    // Configure the wrapped consumer (namesrv addr / namespace / message model / rpc hook ...) before start().
    DefaultMQPullConsumer& defaultMQPullConsumer() { return consumer_; }

    void setPullThreadNums(int32_t n) { pullThreadNums_ = n > 0 ? n : 1; }
    int32_t pullThreadNums() const { return pullThreadNums_; }
    // How often the scheduler recomputes this instance's share (polled here, since no rebalance thread pushes it).
    void setQueueRefreshIntervalMillis(int32_t millis) {
        queueRefreshIntervalMillis_ = millis > 0 ? millis : 1;
    }
    int32_t queueRefreshIntervalMillis() const { return queueRefreshIntervalMillis_; }
    // maxMsgNums of the service pull -- the service pulls on the callback's behalf, so the batch size is set here.
    void setPullBatchNums(int32_t n) { pullBatchNums_ = n > 0 ? n : 1; }
    int32_t pullBatchNums() const { return pullBatchNums_; }
    // The status default (channel 4) for an empty / OFFSET_ILLEGAL reply; 3000ms unless overridden.
    void setPullTimeDelayMillisWhenException(int32_t millis) {
        pullTimeDelayMillisWhenException_ = millis >= 0 ? millis : 0;
    }
    int32_t pullTimeDelayMillisWhenException() const { return pullTimeDelayMillisWhenException_; }
    // The retry delay when the service pull itself failed or doPullTask threw (1000ms unless overridden).
    void setPullTaskExceptionDelayMillis(int32_t millis) {
        pullTaskExceptionDelayMillis_ = millis >= 0 ? millis : 0;
    }
    int32_t pullTaskExceptionDelayMillis() const { return pullTaskExceptionDelayMillis_; }

    // Binds the callback (and the subExpression the service pull uses)
    // to the topic and registers the topic on the consumer so heartbeats carry the subscription.
    // May be called before or after start(); the refresh loop picks new topics up on the next pass.
    void registerPullTaskCallback(const std::string& topic,
                                  std::shared_ptr<PullTaskCallback> callback,
                                  const std::string& subExpression = "*");

    // ---------------- lifecycle ----------------
    void start();
    // Stops the scheduler thread, drains the in-flight tasks, cancels the task table, shuts the consumer down.
    void shutdown();
    bool isStarted() const { return started_; }

    // Aligns the task table with a new queue set for one topic -- tasks of queues that
    // left the set are cancelled (an in-flight round finishes but is never rescheduled), new queues are
    // scheduled immediately. Public so a caller can drive the table itself; the scheduler calls it after every refresh.
    void putTask(const std::string& topic, const std::vector<MessageQueue>& mqNewSet);

    // Live entries of the task table (for tests/diagnostics).
    size_t taskCount();

private:
    struct PullTask {
        MessageQueue mq;
        std::string subExpression = "*";
        std::atomic<bool> cancelled{false};
        std::atomic<bool> running{false};
        int64_t nextRunAtMillis = 0;  // steady clock millis, guarded by stateM_
        int64_t offset = -1;          // -1 = uninitialized; only touched by the per-queue serial worker
    };

    void scheduleLoop();
    void refreshAssignedQueues();
    void dispatchDueTasks(int64_t nowMillis);
    void runTask(const std::shared_ptr<PullTask>& task);
    int64_t initializeOffset(const MessageQueue& mq);
    MessageQueue realQueue(const MessageQueue& mq) const;
    std::shared_ptr<PullTaskCallback> callbackFor(const std::string& topic) const;
    std::string subExpressionFor(const std::string& topic) const;

    DefaultMQPullConsumer consumer_;
    int32_t pullThreadNums_ = 20;
    int32_t queueRefreshIntervalMillis_ = 5000;
    int32_t pullBatchNums_ = 32;
    int32_t pullTimeDelayMillisWhenException_ = 3000;
    int32_t pullTaskExceptionDelayMillis_ = 1000;

    mutable std::mutex stateM_;
    std::map<MessageQueue, std::shared_ptr<PullTask>> taskTable_;
    struct TopicCallback {
        std::shared_ptr<PullTaskCallback> callback;
        std::string subExpression = "*";
    };
    std::map<std::string, TopicCallback> callbackTable_;

    std::shared_ptr<ConsumeExecutor> executor_;
    std::thread schedulerThread_;
    // Detector-thread convention: true = stopped (the polarity sleepUntilDeadline expects).
    std::atomic<bool> stop_{true};
    bool started_ = false;
    std::mutex lifecycleM_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_PULL_SCHEDULE_SERVICE_H
