#include "rocketmq/client/consume_executor.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <stdexcept>
#include <utility>

#include "rocketmq/common/logging.h"

namespace rocketmq {

namespace {
int32_t clampInt32(int32_t v, int32_t lo) { return v < lo ? lo : v; }
}  // namespace

ConsumeExecutor::ConsumeExecutor(int32_t core_pool_size, int32_t maximum_pool_size,
                                 double keep_alive_seconds, std::string thread_name_prefix,
                                 int32_t max_queue_size, std::string thread_name_sep,
                                 int32_t thread_index_from)
    : core_(clampInt32(core_pool_size, 0)),
      max_(clampInt32(std::max(core_, maximum_pool_size), 0)),
      keepAliveSeconds_(keep_alive_seconds),
      prefix_(std::move(thread_name_prefix)),
      maxQueueSize_(clampInt32(max_queue_size, 0)),
      nameSep_(std::move(thread_name_sep)) {
    seq_ = clampInt32(thread_index_from, 0);
}

ConsumeExecutor::~ConsumeExecutor() { shutdown(true); }

void ConsumeExecutor::submit(std::function<void()> task) {
    std::lock_guard<std::mutex> lk(m_);
    if (shutdown_) {
        throw RejectedExecutionError("ConsumeExecutor has been shut down");
    }
    // Only a failed enqueue (full queue) may start a non-core thread; otherwise reject
    const bool queueFull = maxQueueSize_ > 0 && static_cast<int32_t>(queue_.size()) >= maxQueueSize_;
    const bool canGrow = workers_ < max_;
    if (queueFull && !canGrow) {
        throw RejectedExecutionError("ConsumeExecutor queue is full ("
                                     + std::to_string(maxQueueSize_) + ")");
    }
    queue_.push_back(std::move(task));
    // Unbounded-queue semantics: a new thread is started only while poolSize < corePoolSize; otherwise the task queues.
    // workers == 0 is the fallback branch for the core=0
    // configuration, and when the queue is full while growth is still allowed a thread must be added, otherwise tasks queue forever in a pool that no longer works.
    if (workers_ < core_ || (queueFull && canGrow) || workers_ == 0) {
        spawnLocked();
    }
    cv_.notify_one();
}

void ConsumeExecutor::setCorePoolSize(int32_t n) {
    if (n < 0) {
        throw std::invalid_argument("core pool size must be >= 0");
    }
    std::lock_guard<std::mutex> lk(m_);
    const int32_t delta = n - core_;
    core_ = n;
    if (n > max_) {
        max_ = n;  // allow core > max (equivalent to raising max to core)
    }
    if (delta > 0 && !shutdown_) {
        // Core-growth heuristic: add min(delta, queue length) threads and stop as soon as the queue empties.
        int32_t k = std::min(delta, static_cast<int32_t>(queue_.size()));
        while (k > 0 && workers_ < max_) {
            spawnLocked();
            --k;
            if (queue_.empty()) break;
        }
    }
}

int32_t ConsumeExecutor::getCorePoolSize() const {
    std::lock_guard<std::mutex> lk(m_);
    return core_;
}

int32_t ConsumeExecutor::getMaximumPoolSize() const {
    std::lock_guard<std::mutex> lk(m_);
    return max_;
}

int32_t ConsumeExecutor::workerCount() const {
    std::lock_guard<std::mutex> lk(m_);
    return workers_;
}

int32_t ConsumeExecutor::queuedCount() const {
    std::lock_guard<std::mutex> lk(m_);
    return static_cast<int32_t>(queue_.size());
}

int64_t ConsumeExecutor::handlerExceptionCount() const {
    return handlerExceptions_.load(std::memory_order_relaxed);
}

void ConsumeExecutor::shutdown(bool wait) {
    {
        std::lock_guard<std::mutex> lk(m_);
        shutdown_ = true;
        cv_.notify_all();
    }
    if (!wait) return;
    // Wait for all threads to exit (including those that already finished: join returns at once).
    // ⚠ No thread may join itself -- this path is only entered by an external caller (destructor/stop).
    std::vector<std::thread> pending;
    {
        std::lock_guard<std::mutex> lk(m_);
        pending.swap(threads_);
    }
    for (auto& t : pending) {
        if (t.joinable()) t.join();
    }
}

void ConsumeExecutor::spawnLocked() {
    // The caller must already hold m_
    ++workers_;
    const std::string name = prefix_ + nameSep_ + std::to_string(seq_++);
    threads_.emplace_back([this, name]() {
        setThreadName(name);
        run();
    });
}

void ConsumeExecutor::run() {
    for (;;) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lk(m_);
            while (queue_.empty() && !shutdown_) {
                ++idle_;
                cv_.wait_for(lk, std::chrono::milliseconds(static_cast<int64_t>(keepAliveSeconds_ * 1000.0)));
                --idle_;
                if (!queue_.empty() || shutdown_) break;
                // Idle timeout: only over-comitted threads (> core) exit; core-sized threads never
                // exit (the default allowCoreThreadTimeOut=false behaviour).
                if (workers_ > core_) {
                    --workers_;
                    return;
                }
            }
            if (shutdown_ && queue_.empty()) {
                --workers_;
                return;
            }
            task = std::move(queue_.front());
            queue_.pop_front();
        }
        try {
            task();
        } catch (const std::exception& e) {
            // A task exception must not kill the worker (it must keep draining the queue; equivalent to destroying it and creating a new one).
            handlerExceptions_.fetch_add(1, std::memory_order_relaxed);
            logger_error(std::string("consume executor task raised: ") + e.what());
        } catch (...) {
            handlerExceptions_.fetch_add(1, std::memory_order_relaxed);
            logger_error("consume executor task raised unknown exception");
        }
    }
}

}  // namespace rocketmq
