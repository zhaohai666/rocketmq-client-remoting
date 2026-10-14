// Queue allocation strategy (during rebalance, distribute one topic's queues among the consumers of the same consumer group).
//
// Interface trio: `allocate` (compute the queues assigned to this instance), `getName` (algorithm name),
// and the built-in guard `checkAllocateConfig`.
//
// ## Coverage
//
// 6 strategies in total: AVG / AVG_BY_CIRCLE / CONFIG /
// `CONSISTENT_HASH` (needs the hash ring from `rocketmq/common/consistent_hash.h`) / `MACHINE_ROOM` /
// `MACHINE_ROOM_NEARBY-<inner>` (proxy mode, see `AllocateMachineRoomNearby`).
//
// ## Behavior contract
//
// 1. **Guards never throw**: illegal inputs such as an empty `currentCID`, an empty `mqAll`, or an empty `cidAll`
//    all return an empty list — rebalance is a periodic background task, and one dirty input must not crash the consumer.
//    Two exceptions: **construction-time** errors (e.g. a negative virtual-node count) are not on the background path and throw
//    `MQClientException` directly; when `AllocateMachineRoomNearby`'s resolver yields an empty machine room the exception **is still thrown**,
//    because silently returning an empty list withdraws the whole topic's queues, whereas rebalance catching the exception actually preserves the current assignment.
// 2. **Unconfigured-state normalization**: when no queue list was configured, `AllocateMessageQueueByConfig` returns **a copy of the
//    empty list** (instead of passing through an empty reference), and `allocate` returns a copy on every call; likewise a missing
//    constructor argument in `AllocateMachineRoomNearby` and an unconfigured machine-room set in `AllocateMessageQueueByMachineRoom`
//    are expressed with `MQClientException` (text keeps the existing `... is null` phrasing) and an empty set respectively.
// 3. `allocate` is declared `const`: strategy objects are shared across threads (the rebalance thread and each queue's pull threads),
//    and the const interface expresses "the allocation algorithm itself does not mutate state".
//    The configurable lists of `ByConfig` / `ByMachineRoom` therefore live behind `mutable` + mutex (setter contract).
// 4. **`@` splitting of broker names**: split with `javaSplit` semantics — **drop the trailing empty segment**
//    (`"room1@"` → 1 segment, `"room1@b@"` → 2 segments); naive manual `std::string` splitting does not;
//    splitting wrongly makes it nondeterministic whether a queue participates in allocation.
#ifndef ROCKETMQ_CLIENT_ALLOCATE_STRATEGY_H
#define ROCKETMQ_CLIENT_ALLOCATE_STRATEGY_H

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "rocketmq/common/consistent_hash.h"
#include "rocketmq/common/message.h"

namespace rocketmq {

// Allocation strategy interface.
//
// - `consumerGroup`: the current consumer group; only appears in the "this instance is not in cidAll" log;
// - `currentCid`: this client's id; its index in `cidAll` decides which slice it gets;
// - `mqAll`: all queues of the topic. **The caller is responsible for sorting**: rebalance has
//   already sorted `cidAll` / `mqAll` before invoking the strategy;
// - `cidAll`: all client ids of the consumer group, also required to be sorted.
//
// The strategy itself neither sorts nor deduplicates; it only guarantees "the output is an ordered subsequence of the input", so results are deterministic and reproducible.
class AllocateMessageQueueStrategy {
public:
    virtual ~AllocateMessageQueueStrategy() = default;

    virtual std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const = 0;

    // Algorithm name (AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH /
    // MACHINE_ROOM / MACHINE_ROOM_NEARBY-<inner>), used for logging and strategy identification.
    virtual std::string getName() const = 0;
};

// Split a string with "drop trailing empty segment" semantics: **trailing empty segments are dropped**.
//
// The only difference from manual `std::string` splitting is the trailing empty segment: `"room1@"` yields `{"room1"}` (1 segment),
// naive splitting yields `{"room1", ""}` (2 segments). But **when the separator is not found** the whole string
// is returned as-is (even if it is itself empty), so trailing segments cannot be dropped unconditionally. Established results:
// `""` → 1 segment `{""}`, `"@"` → 0 segments, `"room1@"` → 1 segment, `"room1@b@"` → 2 segments.
std::vector<std::string> javaSplit(const std::string& text, char sep);

// Built-in guard: returns -1 on illegal inputs (empty currentCid / empty mqAll / empty cidAll / currentCid not in
// cidAll); returns the index of `currentCid` in `cidAll` when the check passes.
// Exposed so custom strategies can reuse the same guard, keeping behavior consistent with the built-in strategies.
int32_t checkAllocateConfig(const std::string& consumerGroup, const std::string& currentCid,
                            const std::vector<MessageQueue>& mqAll,
                            const std::vector<std::string>& cidAll);

// Average allocation: cut mqAll into **contiguous ranges**, the remainder mod goes to the first mod consumers
// one each (`getName()` = "AVG").
class AllocateMessageQueueAveragely : public AllocateMessageQueueStrategy {
public:
    std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const override;

    std::string getName() const override { return "AVG"; }
};

// Circular average allocation: consumer at index takes the queues whose index ≡ index (mod cidAll.size())
// (`getName()` = "AVG_BY_CIRCLE").
class AllocateMessageQueueAveragelyByCircle : public AllocateMessageQueueStrategy {
public:
    std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const override;

    std::string getName() const override { return "AVG_BY_CIRCLE"; }
};

// Allocation by explicit configuration: ignores mqAll / cidAll entirely and hands the configured queue list as-is to **every** consumer
// (typically for broadcast-style manual queue binding); `allocate` **does not call check** (`getName()` = "CONFIG").
class AllocateMessageQueueByConfig : public AllocateMessageQueueStrategy {
public:
    AllocateMessageQueueByConfig() = default;
    explicit AllocateMessageQueueByConfig(const std::vector<MessageQueue>& messageQueueList);

    // Replace the whole queue list. Non-const: the strategy object is shared via shared_ptr and can still be
    // modified after registration into the consumer; changes take effect for all subsequent rebalances.
    void setMessageQueueList(const std::vector<MessageQueue>& messageQueueList);

    // Returns a copy (does not hand out the shared mutable reference).
    std::vector<MessageQueue> getMessageQueueList() const;

    std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const override;

    std::string getName() const override { return "CONFIG"; }

private:
    mutable std::mutex lock_;
    std::vector<MessageQueue> messageQueueList_;
};

// Consistent-hash allocation: spread each clientId proportionally into virtual nodes forming a hash ring, then see which
// interval each queue falls into (keyed by `MessageQueue::toString()`) (`getName()` = "CONSISTENT_HASH"); the ring itself lives
// in `rocketmq/common/consistent_hash.h`.
//
// Key difference from AVG: **only boundary queues change owners when consumers join or leave**, instead of the whole
// range being reshuffled, so scaling storms are much smaller; the price is a less even distribution than AVG.
//
// The hash ring is rebuilt in place on every `allocate`; the strategy object itself has no mutable state → inherently thread-safe.
class AllocateMessageQueueConsistentHash : public AllocateMessageQueueStrategy {
public:
    // Defaults to 10 virtual nodes.
    AllocateMessageQueueConsistentHash() : AllocateMessageQueueConsistentHash(10, nullptr) {}

    explicit AllocateMessageQueueConsistentHash(int32_t virtualNodeCnt)
        : AllocateMessageQueueConsistentHash(virtualNodeCnt, nullptr) {}

    // `customHashFunction == nullptr` uses the ring's built-in MD5Hash.
    // Negative `virtualNodeCnt` → throws `MQClientException` (construction-time error, see behavior contract 1).
    AllocateMessageQueueConsistentHash(int32_t virtualNodeCnt,
                                       const std::shared_ptr<const HashFunction>& customHashFunction);

    // Exposes the virtual node count, for self-checks and logging.
    int32_t getVirtualNodeCnt() const { return virtualNodeCnt_; }

    std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const override;

    std::string getName() const override { return "CONSISTENT_HASH"; }

private:
    int32_t virtualNodeCnt_;
    std::shared_ptr<const HashFunction> customHashFunction_;
};

// Machine-room allocation: only queues whose broker name matches `<room>@<cluster>` participate in allocation; the machine-room
// set comes from configuration (`getName()` = "MACHINE_ROOM", for logical-machine-room style scenarios that order queues by machine room).
//
// ⚠ Broker names are split with `javaSplit`; the criterion is "exactly 2 segments after dropping trailing empty segments";
// ⚠ an unset `consumeridcs` is normalized to **an empty set** ⇒ when unconfigured nobody gets any queue,
//    instead of crashing the consumer (see behavior contract 2).
class AllocateMessageQueueByMachineRoom : public AllocateMessageQueueStrategy {
public:
    AllocateMessageQueueByMachineRoom() = default;
    explicit AllocateMessageQueueByMachineRoom(const std::set<std::string>& consumeridcs);

    // Replace the whole machine-room set. Non-const: same setter contract as `ByConfig`
    // (the strategy object can still be modified after registration into the consumer; changes take effect for all subsequent rebalances).
    void setConsumeridcs(const std::set<std::string>& consumeridcs);

    // Returns a copy.
    std::set<std::string> getConsumeridcs() const;

    std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const override;

    std::string getName() const override { return "MACHINE_ROOM"; }

private:
    mutable std::mutex lock_;
    std::set<std::string> consumeridcs_;
};

// MachineRoomResolver: tells the strategy "which machine room a given queue / a given client is deployed in". The interface
// contract requires a **non-empty** return value; an empty string is treated as an unknown machine room and throws
// `MQClientException` (see the exception clause in behavior contract 1).
struct MachineRoomResolver {
    virtual ~MachineRoomResolver() = default;
    virtual std::string brokerDeployIn(const MessageQueue& messageQueue) = 0;
    virtual std::string consumerDeployIn(const std::string& clientId) = 0;
};

// Machine-room nearby proxy strategy: first group queues and consumers by machine room,
//  1. queues in this consumer's machine room go only to consumers in the same machine room (computed with the inner strategy);
//  2. machine rooms with no live consumers have their queues shared by consumers of all machine rooms together over `cidAll` (shared).
// Hence **this consumer will also get queues from other machine rooms**, with this room's queues placed first in the result (that is the concatenation order).
//
// `getName()` = `"MACHINE_ROOM_NEARBY-" + inner name`.
class AllocateMachineRoomNearby : public AllocateMessageQueueStrategy {
public:
    // Throws `MQClientException` when an argument is null (text keeps the existing `... is null` phrasing,
    // callers match the literal; see behavior contract 2).
    AllocateMachineRoomNearby(const std::shared_ptr<AllocateMessageQueueStrategy>& allocateMessageQueueStrategy,
                              const std::shared_ptr<MachineRoomResolver>& machineRoomResolver);

    std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const override;

    std::string getName() const override { return name_; }

private:
    std::shared_ptr<AllocateMessageQueueStrategy> allocateMessageQueueStrategy_;
    std::shared_ptr<MachineRoomResolver> machineRoomResolver_;
    std::string name_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_ALLOCATE_STRATEGY_H
