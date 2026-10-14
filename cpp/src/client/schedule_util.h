// Measured on real machines: on macOS a 100ms sleep_for / ManualResetEventSlim.Wait takes 104~131ms
// (the system timer adds one tick), so "sleep 30s in 100ms slices" actually costs 31~39s and stretches the
// route refresh, offset persistence and heartbeat periods. Each slice here is recomputed against the **absolute deadline** so errors no longer accumulate;
// slicing only exists so the stop flag takes effect within 100ms (join is not held up by a whole sleep).
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

namespace rocketmq {

// Sleep until the deadline (steady_clock based); return immediately when stop becomes true and let the caller's loop condition finish.
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
