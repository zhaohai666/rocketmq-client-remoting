// 周期任务的「固定速率」推进（各客户端实现共用，对应 Java ScheduledExecutorService
// #scheduleAtFixedRate 的语义）：下一次执行锚定在**计划时刻**（start + initialDelay + n×period），
// 而不是「上一轮干完再睡一个周期」——后者的耗时（以及下面这条坑）会一轮轮累加。
//
// 真机量化过：macOS 上 100ms 的 sleep_for / ManualResetEventSlim.Wait 实测量到 104~131ms
// （系统定时器多给一个 tick），于是「按 100ms 切片睡满 30s」实际要 31~39s，路由刷新、
// 位点落盘、心跳的周期全被拉长。这里每段都重新对着**绝对 deadline** 算，误差不再累积；
// 分段只为让 stop 标志在 100ms 内生效（join 不被整段 sleep 拖住）。
//
// 落后于计划（上一轮超时）时直接返回、调用方立刻补跑，与 Java 的 catch-up 行为一致。
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

namespace rocketmq {

// 睡到 deadline（steady_clock 口径）；stop 变 true 时立刻返回，由调用方的循环条件收尾。
inline void sleepUntilDeadline(const std::atomic<bool>& stop,
                               std::chrono::steady_clock::time_point deadline) {
    while (!stop.load()) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return;
        }
        const auto remainMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
        std::this_thread::sleep_for(
            std::chrono::milliseconds(std::min<int64_t>(100, std::max<int64_t>(1, remainMs))));
    }
}

}  // namespace rocketmq
