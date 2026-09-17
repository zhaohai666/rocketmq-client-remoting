#include "rocketmq/client/consume_executor.h"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <utility>

#include "rocketmq/common/logging.h"

namespace rocketmq {

namespace {
int32_t clampInt32(int32_t v, int32_t lo) { return v < lo ? lo : v; }
}  // namespace

ConsumeExecutor::ConsumeExecutor(int32_t core_pool_size, int32_t maximum_pool_size,
                                 double keep_alive_seconds, std::string thread_name_prefix)
    : core_(clampInt32(core_pool_size, 0)),
      max_(clampInt32(std::max(core_, maximum_pool_size), 0)),
      keepAliveSeconds_(keep_alive_seconds),
      prefix_(std::move(thread_name_prefix)) {}

ConsumeExecutor::~ConsumeExecutor() { shutdown(true); }

void ConsumeExecutor::submit(std::function<void()> task) {
    std::lock_guard<std::mutex> lk(m_);
    if (shutdown_) {
        throw std::runtime_error("ConsumeExecutor has been shut down");
    }
    queue_.push_back(std::move(task));
    // Java 无界队列语义：只有 poolSize < corePoolSize 才新建线程；否则入队等待。
    if (workers_ < core_ || workers_ == 0) {
        // 第二个条件是 Java execute() 里 "入队成功后 workerCount == 0 再补一个线程"
        // 的兜底分支 —— core=0 的配置下没有它任务永远没人跑。
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
        max_ = n;  // Java 允许 core > max（等价于把 max 抬到 core）
    }
    if (delta > 0 && !shutdown_) {
        // Java setCorePoolSize 的启发式：按 min(delta, 队列长度) 补线程，队列一空就停。
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
    // 等待所有线程退出（含已自然退出的：join 立即返回）。
    // ⚠ 必须保证没有线程在自己身上 join —— 这里只由外部调用者（析构/stop）触发。
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
    // 调用方必须已持有 m_
    ++workers_;
    const std::string name = prefix_ + "-" + std::to_string(seq_++);
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
                // 空闲超时：只有超编线程（> core）才退出；core 以内的线程永不退出
                // （Java allowCoreThreadTimeOut 默认 false）。
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
            // 任务异常不能杀 worker（Java 会补一个新 worker，效果等价）。
            handlerExceptions_.fetch_add(1, std::memory_order_relaxed);
            logger_error(std::string("consume executor task raised: ") + e.what());
        } catch (...) {
            handlerExceptions_.fetch_add(1, std::memory_order_relaxed);
            logger_error("consume executor task raised unknown exception");
        }
    }
}

}  // namespace rocketmq
