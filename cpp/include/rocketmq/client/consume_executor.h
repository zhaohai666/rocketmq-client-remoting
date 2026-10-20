#ifndef ROCKETMQ_CLIENT_CONSUME_EXECUTOR_H
#define ROCKETMQ_CLIENT_CONSUME_EXECUTOR_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace rocketmq {

class RejectedExecutionError : public std::runtime_error {
public:
    explicit RejectedExecutionError(const std::string& msg) : std::runtime_error(msg) {}
};

class ConsumeExecutor {
 public:
    ConsumeExecutor(int32_t core_pool_size, int32_t maximum_pool_size,
                    double keep_alive_seconds = 60.0, std::string thread_name_prefix = "rmq-consume",
                    int32_t max_queue_size = 0, std::string thread_name_sep = "-",
                    int32_t thread_index_from = 0);
    ~ConsumeExecutor();

    ConsumeExecutor(const ConsumeExecutor&) = delete;
    ConsumeExecutor& operator=(const ConsumeExecutor&) = delete;

    void submit(std::function<void()> task);

    // Resize the core pool at runtime; workers above the new core count are left to time out.
    void setCorePoolSize(int32_t n);

    int32_t getCorePoolSize() const;
    int32_t getMaximumPoolSize() const;

    int32_t workerCount() const;
    int32_t queuedCount() const;
    int64_t handlerExceptionCount() const;

    // graceful retire: accept no more tasks, drain what is already queued.
    // wait=true joins every worker; awaitTerminationMillis is the ceiling on that drain (>0): the
    // backlog is walked step by step and the ceiling is evaluated at every step, so an expired ceiling
    // stops the drain **while batches are still queued** and the rest are dropped (counted by
    // droppedTaskCount()). 0 or less = no ceiling, the drain goes to the end. The ceiling never
    // interrupts a task that is already running -- a worker evaluates it between tasks, exactly like the
    // join itself waits for the task in hand.
    void shutdown(bool wait = false, int64_t awaitTerminationMillis = 0);

    // Number of queued tasks dropped because the drain ceiling expired.
    int64_t droppedTaskCount() const;

 private:
    void spawnLocked();
    void run();

    mutable std::mutex m_;
    std::condition_variable cv_;
    int32_t core_;
    int32_t max_;
    double keepAliveSeconds_;
    std::string prefix_;
    int32_t maxQueueSize_;
    std::string nameSep_;
    std::deque<std::function<void()>> queue_;
    int32_t workers_ = 0;
    int32_t idle_ = 0;
    int32_t seq_ = 0;
    bool shutdown_ = false;
    // Millisecond deadline of the drain, 0 = no ceiling (guarded by m_: shutdown arms it, run reads it)
    int64_t drainDeadlineMs_ = 0;
    std::atomic<int64_t> droppedTasks_{0};
    // Handles of **every** thread ever created stay here (the vector only grows): an exited thread is
    // still joinable, and dropping it here then destroying the std::thread would call std::terminate. Already-exited threads join immediately.
    std::vector<std::thread> threads_;
    std::atomic<int64_t> handlerExceptions_{0};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_CONSUME_EXECUTOR_H
