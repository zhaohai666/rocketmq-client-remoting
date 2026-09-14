// 队列选择器实现（对应 Java SelectMessageQueueByHash / SelectMessageQueueByRandom）。
#include "rocketmq/client/result.h"

#include <cstdint>
#include <random>
#include <vector>

#include "rocketmq/client/exception.h"

namespace rocketmq {

MessageQueue SelectMessageQueueByHash::select(const std::vector<MessageQueue>& mqs,
                                              const Message& /*msg*/,
                                              const std::string& arg) const {
    if (mqs.empty()) {
        throw MQClientException("no message queue");
    }
    // 与 Java 一致：arg.hashCode() 取绝对值后对队列数取模。
    // 这里用 Java String.hashCode() 语义（Python 侧用的是 hash()，对字符串会随
    // PYTHONHASHSEED 变化，不可跨进程复现；Java 语义是确定性的，更适合做分片键）。
    int32_t hashCode = javaStringHash(arg);
    if (hashCode < 0) {
        hashCode = hashCode == INT32_MIN ? 0 : -hashCode;  // 避免 INT32_MIN 取负溢出
    }
    size_t index = static_cast<size_t>(hashCode) % mqs.size();
    return mqs[index];
}

MessageQueue SelectMessageQueueByRandom::select(const std::vector<MessageQueue>& mqs,
                                                const Message& /*msg*/,
                                                const std::string& /*arg*/) const {
    if (mqs.empty()) {
        throw MQClientException("no message queue");
    }
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<size_t> dist(0, mqs.size() - 1);
    return mqs[dist(rng)];
}

}  // namespace rocketmq
