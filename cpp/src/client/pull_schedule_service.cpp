#include "rocketmq/client/pull_schedule_service.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <utility>

#include "rocketmq/client/exception.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/namespace_util.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "schedule_util.h"

namespace rocketmq {

namespace {

// Monotonic scheduling clock (steady_clock): wall-clock jumps must not move task deadlines.
int64_t steadyNowMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}  // namespace

MQPullConsumerScheduleService::MQPullConsumerScheduleService(const std::string& consumerGroup)
    : consumer_(consumerGroup) {
    // The service divides the queue set across the group's instances, so it pins CLUSTERING here: the
    // consumer already defaults to it, but the explicit set keeps the requirement visible (and survives a future default change).
    consumer_.setMessageModel(MessageModel::CLUSTERING);
}

MQPullConsumerScheduleService::~MQPullConsumerScheduleService() {
    try {
        shutdown();
    } catch (...) {
    }
}

// ---------------------------------------------------------------- registration

void MQPullConsumerScheduleService::registerPullTaskCallback(
    const std::string& topic, std::shared_ptr<PullTaskCallback> callback,
    const std::string& subExpression) {
    if (topic.empty()) {
        throw MQClientException("topic is empty");
    }
    if (callback == nullptr) {
        throw MQClientException("PullTaskCallback is null");
    }
    {
        std::lock_guard<std::mutex> lk(stateM_);
        callbackTable_[topic] = TopicCallback{std::move(callback), subExpression};
    }
    // Registering a callback also registers the topic on the consumer (registerMessageQueueListener with an
    // empty listener): the topic enters the consumer's register table so heartbeats carry the subscription
    // (broker-side group registration); the listener slot stays empty -- the refresh loop drives queue changes here.
    consumer_.registerMessageQueueListener(topic, nullptr);
}

std::shared_ptr<PullTaskCallback> MQPullConsumerScheduleService::callbackFor(
    const std::string& topic) const {
    std::lock_guard<std::mutex> lk(stateM_);
    auto it = callbackTable_.find(topic);
    return it == callbackTable_.end() ? nullptr : it->second.callback;
}

std::string MQPullConsumerScheduleService::subExpressionFor(const std::string& topic) const {
    std::lock_guard<std::mutex> lk(stateM_);
    auto it = callbackTable_.find(topic);
    return it == callbackTable_.end() ? std::string("*") : it->second.subExpression;
}

// ---------------------------------------------------------------- lifecycle

void MQPullConsumerScheduleService::start() {
    std::lock_guard<std::mutex> lk(lifecycleM_);
    if (started_) {
        return;
    }
    // Let consumer-side validation errors (group/namespace/namesrv) propagate before any thread exists.
    consumer_.start();
    executor_ = std::make_shared<ConsumeExecutor>(
        pullThreadNums_, pullThreadNums_, 60.0, "PullMsgThread", /*max_queue_size=*/0, "_", 1);
    stop_.store(false);
    schedulerThread_ = std::thread([this]() { scheduleLoop(); });
    started_ = true;
    logger_info("MQPullConsumerScheduleService start OK, " + consumer_.consumerGroup());
}

void MQPullConsumerScheduleService::shutdown() {
    std::lock_guard<std::mutex> lk(lifecycleM_);
    if (!started_) {
        return;
    }
    // 1. the scheduler stops handing out new rounds;
    stop_.store(true);
    if (schedulerThread_.joinable()) {
        schedulerThread_.join();
    }
    // 2. drain: an in-flight doPullTask finishes before the consumer (and its transport) goes down;
    std::shared_ptr<ConsumeExecutor> pool;
    {
        std::lock_guard<std::mutex> sl(stateM_);
        pool.swap(executor_);
    }
    if (pool) {
        pool->shutdown(true);
    }
    // 3. cancel whatever is left in the table (a queued-but-never-run round must not fire later);
    {
        std::lock_guard<std::mutex> sl(stateM_);
        for (auto& kv : taskTable_) {
            kv.second->cancelled.store(true);
        }
        taskTable_.clear();
    }
    consumer_.shutdown();
    started_ = false;
    logger_info("MQPullConsumerScheduleService shutdown, " + consumer_.consumerGroup());
}

// ---------------------------------------------------------------- task table (putTask)

void MQPullConsumerScheduleService::putTask(const std::string& topic,
                                            const std::vector<MessageQueue>& mqNewSet) {
    std::lock_guard<std::mutex> lk(stateM_);
    for (auto it = taskTable_.begin(); it != taskTable_.end();) {
        if (it->first.topic == topic
            && std::find(mqNewSet.begin(), mqNewSet.end(), it->first) == mqNewSet.end()) {
            // An in-flight round of a cancelled task still runs to completion (shutdown drains it),
            // but runTask sees the flag and never reschedules -- cancellation only bites on the next round.
            it->second->cancelled.store(true);
            it = taskTable_.erase(it);
        } else {
            ++it;
        }
    }
    const std::string subExpression = [&]() {
        auto cb = callbackTable_.find(topic);
        return cb == callbackTable_.end() ? std::string("*") : cb->second.subExpression;
    }();
    const int64_t now = steadyNowMillis();
    for (const MessageQueue& mq : mqNewSet) {
        if (taskTable_.count(mq) > 0) {
            continue;
        }
        auto task = std::make_shared<PullTask>();
        task->mq = mq;
        task->subExpression = subExpression;
        // New queues run at once: their first deadline is "now", i.e. zero delay.
        task->nextRunAtMillis = now;
        taskTable_.emplace(mq, task);
    }
}

size_t MQPullConsumerScheduleService::taskCount() {
    std::lock_guard<std::mutex> lk(stateM_);
    return taskTable_.size();
}

// ---------------------------------------------------------------- scheduler thread

void MQPullConsumerScheduleService::scheduleLoop() {
    setThreadName("PullTaskScheduleThread");
    // The first pass refreshes immediately so tasks appear without waiting one interval.
    int64_t nextRefreshAt = steadyNowMillis();
    while (!stop_.load()) {
        const int64_t now = steadyNowMillis();
        if (now >= nextRefreshAt) {
            try {
                refreshAssignedQueues();
            } catch (const std::exception& e) {
                logger_warn(std::string("pull schedule refresh failed: ") + e.what());
            } catch (...) {
                logger_warn("pull schedule refresh failed: unknown exception");
            }
            nextRefreshAt = steadyNowMillis() + queueRefreshIntervalMillis_;
        }
        try {
            dispatchDueTasks(steadyNowMillis());
        } catch (const std::exception& e) {
            logger_warn(std::string("pull schedule dispatch failed: ") + e.what());
        } catch (...) {
            logger_warn("pull schedule dispatch failed: unknown exception");
        }
        // Sleep to the earliest interesting deadline (next due task / next refresh), 5ms floor so a
        // zero-delay FOUND loop does not hot-spin; sleepUntilDeadline slices <=100ms and bails out at once
        // when stop_ flips, so shutdown is not held up by a long interval.
        int64_t nextWake = nextRefreshAt;
        {
            std::lock_guard<std::mutex> lk(stateM_);
            for (const auto& kv : taskTable_) {
                const auto& task = kv.second;
                if (task->cancelled.load() || task->running.load()) {
                    continue;
                }
                nextWake = std::min(nextWake, task->nextRunAtMillis);
            }
        }
        int64_t waitMs = nextWake - steadyNowMillis();
        waitMs = std::min<int64_t>(100, std::max<int64_t>(5, waitMs));
        sleepUntilDeadline(stop_,
                           std::chrono::steady_clock::now() + std::chrono::milliseconds(waitMs));
    }
}

void MQPullConsumerScheduleService::refreshAssignedQueues() {
    // Snapshot the topics under the lock; the queue computation (route/consumer-list RPCs) runs outside it.
    std::vector<std::string> topics;
    {
        std::lock_guard<std::mutex> lk(stateM_);
        topics.reserve(callbackTable_.size());
        for (const auto& kv : callbackTable_) {
            topics.push_back(kv.first);
        }
    }
    const bool broadcasting = consumer_.messageModel() == MessageModel::BROADCASTING;
    for (const std::string& topic : topics) {
        std::vector<MessageQueue> assigned;
        try {
            if (broadcasting) {
                // Under BROADCASTING the full routed queue set is used; fetchSubscribeMessageQueues returns the
                // route's (namespaced) queues, so strip the namespace to keep the task table on bare topics
                // (pull() wraps it again on the way out).
                assigned = consumer_.fetchSubscribeMessageQueues(topic);
                for (MessageQueue& mq : assigned) {
                    mq.topic = NamespaceUtil::withoutNamespace(mq.topic, consumer_.namespaceOf());
                }
            } else {
                // Under CLUSTERING only this instance's share of the queues is pulled, and it is already bare-topic.
                assigned = consumer_.fetchMessageQueuesInBalance(topic);
            }
        } catch (const std::exception& e) {
            logger_debug("pull schedule refresh: queues of " + topic + " unavailable: " + e.what());
            continue;  // keep the existing tasks; a route blip must not cancel the share
        }
        putTask(topic, assigned);
    }
}

void MQPullConsumerScheduleService::dispatchDueTasks(int64_t nowMillis) {
    std::vector<std::shared_ptr<PullTask>> due;
    {
        std::lock_guard<std::mutex> lk(stateM_);
        for (const auto& kv : taskTable_) {
            const auto& task = kv.second;
            if (task->cancelled.load() || task->nextRunAtMillis > nowMillis) {
                continue;
            }
            // The flag is what makes one queue strictly serial: while a round is in flight the task is
            // invisible to the dispatcher, and runTask reschedules it only after clearing the flag.
            if (task->running.exchange(true)) {
                continue;
            }
            due.push_back(task);
        }
    }
    if (due.empty()) {
        return;
    }
    std::shared_ptr<ConsumeExecutor> pool;
    {
        std::lock_guard<std::mutex> lk(stateM_);
        pool = executor_;
    }
    for (const auto& task : due) {
        if (pool == nullptr) {
            task->running.store(false);
            continue;
        }
        try {
            pool->submit([this, task]() { runTask(task); });
        } catch (const RejectedExecutionError&) {
            // Pool shut down concurrently: un-mark so a later pass can pick the task up again.
            task->running.store(false);
        }
    }
}

// ---------------------------------------------------------------- one round of one queue

MessageQueue MQPullConsumerScheduleService::realQueue(const MessageQueue& mq) const {
    MessageQueue real = mq;
    real.topic = NamespaceUtil::wrapNamespace(consumer_.namespaceOf(), mq.topic);
    return real;
}

int64_t MQPullConsumerScheduleService::initializeOffset(const MessageQueue& mq) {
    // The start point of a fresh task under CONSUME_FROM_LAST_OFFSET: the stored consume offset
    // when the group has one, else the queue's maxOffset (start at the tail, never replay history).
    const MessageQueue real = realQueue(mq);
    int64_t stored = 0;
    try {
        if (consumer_.fetchConsumeOffset(real, stored) && stored >= 0) {
            return stored;
        }
    } catch (const std::exception& e) {
        logger_debug(std::string("pull schedule: fetchConsumeOffset failed: ") + e.what());
    }
    try {
        return consumer_.maxOffset(real);
    } catch (const std::exception& e) {
        logger_debug(std::string("pull schedule: maxOffset failed: ") + e.what());
    }
    return 0;
}

void MQPullConsumerScheduleService::runTask(const std::shared_ptr<PullTask>& task) {
    int32_t delay = pullTimeDelayMillisWhenException_;
    try {
        const std::shared_ptr<PullTaskCallback> callback = callbackFor(task->mq.topic);
        if (callback == nullptr) {
            // The round is skipped with a warning but the task stays alive (a callback registered
            // later must find its queue again) and simply waits one exception delay.
            logger_warn("Pull Task Callback not exist , " + task->mq.topic);
            delay = pullTaskExceptionDelayMillis_;
        } else {
            PullResult result;
            bool pulled = false;
            try {
                if (task->offset < 0) {
                    task->offset = initializeOffset(task->mq);
                }
                result = consumer_.pull(task->mq, task->subExpression, task->offset, pullBatchNums_);
                pulled = true;
                // The round's offset bookkeeping: follow nextBeginOffset on every reply and
                // keep the local offset table current (persisted by the instance-level periodic task).
                task->offset = result.nextBeginOffset;
                try {
                    consumer_.updateConsumeOffset(realQueue(task->mq), task->offset);
                } catch (const std::exception& e) {
                    logger_debug(std::string("pull schedule: updateConsumeOffset failed: ")
                                 + e.what());
                }
            } catch (const std::exception& e) {
                logger_warn("pull task pull failed, " + task->mq.toString() + ", " + e.what());
                delay = pullTaskExceptionDelayMillis_;
            }
            if (pulled) {
                // Status default (channel 4): FOUND -> immediately, empty/illegal -> the 3s-class delay.
                delay = result.status == PullStatus::FOUND ? 0 : pullTimeDelayMillisWhenException_;
                PullTaskContext context;
                context.setPullConsumer(&consumer_);
                int32_t returned = PullTaskCallback::kUseContextDelay;
                bool callbackThrew = false;
                try {
                    returned = callback->doPullTask(task->mq, result, context);
                } catch (const std::exception& e) {
                    callbackThrew = true;
                    delay = pullTaskExceptionDelayMillis_;  // the callback exception lands on the exception delay, 1000ms by default
                    logger_error(std::string("doPullTask Exception: ") + e.what());
                } catch (...) {
                    callbackThrew = true;
                    delay = pullTaskExceptionDelayMillis_;
                    logger_error("doPullTask Exception: unknown");
                }
                if (!callbackThrew) {
                    if (returned >= 0) {
                        delay = returned;                        // channel 1: return value
                    } else if (context.pullLaterSet()) {
                        delay = context.pullLaterDelay();        // channel 2: pullMessageLater
                    } else if (context.pullIntervalSet()) {
                        delay = context.pullInterval();          // channel 3: setPullInterval
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        delay = pullTaskExceptionDelayMillis_;
        logger_error(std::string("pull task round failed: ") + e.what());
    } catch (...) {
        delay = pullTaskExceptionDelayMillis_;
        logger_error("pull task round failed: unknown exception");
    }
    if (delay < 0) {
        delay = 0;
    }
    std::lock_guard<std::mutex> lk(stateM_);
    task->running.store(false);
    if (task->cancelled.load()) {
        logger_warn("The Pull Task is cancelled after doPullTask, " + task->mq.toString());
        return;
    }
    task->nextRunAtMillis = steadyNowMillis() + delay;
}

}  // namespace rocketmq
