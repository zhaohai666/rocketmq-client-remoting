// Queue selector implementation: hash-based selection / random selection.
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
    // Take the absolute value of arg's hash, then modulo the queue count.
    // The hash uses the String.hashCode polynomial rolling semantics (javaStringHash): it is deterministic
    // and reproducible across processes, which suits a shard key; no per-process randomised hashing.
    int32_t hashCode = javaStringHash(arg);
    if (hashCode < 0) {
        hashCode = hashCode == INT32_MIN ? 0 : -hashCode;  // avoid negation overflow on INT32_MIN
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

// SelectMessageQueueByMachineRoom is an unimplemented stub in this SDK: MessageQueue is a value type, so
// "return null and let downstream hit a null pointer" cannot be expressed; this throws instead -- sending with it must fail,
// just earlier and with a clearer message.
MessageQueue SelectMessageQueueByMachineRoom::select(const std::vector<MessageQueue>& mqs,
                                                     const Message& /*msg*/,
                                                     const std::string& /*arg*/) const {
    (void)mqs;
    throw MQClientException(
        "SelectMessageQueueByMachineRoom.select() is not implemented "
        "(this selector has no machine-room metadata to route on)");
}

}  // namespace rocketmq
