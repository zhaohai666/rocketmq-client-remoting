// 消费执行器：Java ThreadPoolExecutor 的最小等价物（core/max 两档）。
//
// 为什么不用 std::async / 裸 std::thread：
//   * 之前 C++ 的 POP 路径是"每个批次起一个 detached 线程"，**无上限**，慢监听器一上来
//     就线程爆炸；Java 用的是有界线程池（ConsumeMessagePopConcurrentlyService 持有
//     consumeExecutor），并且线程数与 consumeThreadMin/Max 挂钩 —— 但本项目的
//     consumeThreadNums_ 此前**设了没有任何作用**（`setConsumeThreadNums(4)` 实际是死配置）。
//   * Java 在 LinkedBlockingQueue（无界）下 **真实并发度 == corePoolSize**
//     （poolSize < corePoolSize 才新建线程，否则入队），所以线程弹性全挂在 core 上：
//     `AbstractConsumeMessageService.updateCorePoolSize(n)` → `setCorePoolSize(n)`。
//
// 对齐点（与 Python consume_executor.py 逐条同源）：
//   1. 投递时只有 workers < core 才新建线程；否则入队。
//   2. 入队后若 workers == 0 就补一个线程（Java execute 的兜底分支，core=0 时必须）。
//   3. > core 的线程空闲超过 keepAlive 退出；<= core 的线程永不退出
//      （Java allowCoreThreadTimeOut 默认 false）。
//   4. setCorePoolSize(n)：core 变大时按 min(delta, 队列长度) 补线程（Java 的启发式算法）。
//   5. 任务抛异常不杀 worker（Java 会补一个新 worker，效果等价）。
#ifndef ROCKETMQ_CLIENT_CONSUME_EXECUTOR_H
#define ROCKETMQ_CLIENT_CONSUME_EXECUTOR_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace rocketmq {

class ConsumeExecutor {
 public:
    // core_pool_size/maximum_pool_size 直接对应 Java ThreadPoolExecutor 的
    // consumeThreadMin / consumeThreadMax，keep_alive_seconds 默认 60s（Java 构造参数）。
    ConsumeExecutor(int32_t core_pool_size, int32_t maximum_pool_size,
                    double keep_alive_seconds = 60.0, std::string thread_name_prefix = "rmq-consume");
    ~ConsumeExecutor();

    ConsumeExecutor(const ConsumeExecutor&) = delete;
    ConsumeExecutor& operator=(const ConsumeExecutor&) = delete;

    // 投递任务（Java execute）。已 shutdown 时抛 std::runtime_error。
    void submit(std::function<void()> task);

    // 对应 ThreadPoolExecutor.setCorePoolSize。
    void setCorePoolSize(int32_t n);

    int32_t getCorePoolSize() const;
    int32_t getMaximumPoolSize() const;

    // 观测用（对应 Java getPoolSize / getQueue().size()）。
    int32_t workerCount() const;
    int32_t queuedCount() const;
    int64_t handlerExceptionCount() const;

    // 对应 Java shutdown：不再接收新任务，把队列跑完；wait=true 时等待线程收工。
    // 注意这**不是** shutdownNow（不中断正在跑的任务）。
    void shutdown(bool wait = false);

 private:
    void spawnLocked();
    void run();

    mutable std::mutex m_;
    std::condition_variable cv_;
    int32_t core_;
    int32_t max_;
    double keepAliveSeconds_;
    std::string prefix_;
    std::deque<std::function<void()>> queue_;
    int32_t workers_ = 0;
    int32_t idle_ = 0;
    int32_t seq_ = 0;
    bool shutdown_ = false;
    // 所有**曾经**创建过的线程句柄都留在这里（只增不减）：退出的线程仍然是 joinable 的，
    // 若从容器里摘掉再析构 std::thread 会直接 std::terminate。它们已退出，join 立即返回。
    std::vector<std::thread> threads_;
    std::atomic<int64_t> handlerExceptions_{0};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_CONSUME_EXECUTOR_H
