// 队列选择器实现：按哈希选择 / 随机选择。
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
    // arg 的哈希取绝对值后对队列数取模。
    // 哈希用 String.hashCode 多项式滚动语义（javaStringHash）：它是确定性的，
    // 可跨进程复现，适合作分片键；不采用进程内随机化哈希。
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

// SelectMessageQueueByMachineRoom 在本 SDK 中是未实现的桩：MessageQueue 是值类型、
// 无法表达"返回 null 让下游空指针"，这里直接抛 —— 用它发送必然失败，只是失败得更早、
// 信息更明确。异常文案里的 stub 说明是与调用方约定的匹配内容，保持不变。
MessageQueue SelectMessageQueueByMachineRoom::select(const std::vector<MessageQueue>& mqs,
                                                     const Message& /*msg*/,
                                                     const std::string& /*arg*/) const {
    (void)mqs;
    throw MQClientException(
        "SelectMessageQueueByMachineRoom.select() is not implemented: Java stub "
        "returns null (send would fail with NPE)");
}

}  // namespace rocketmq
