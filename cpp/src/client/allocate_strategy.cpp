// Queue allocation strategy implementations; for the behaviour contract see the "behaviour contract" section of the header.
#include "rocketmq/client/allocate_strategy.h"

#include <algorithm>
#include <map>
#include <utility>

#include "rocketmq/client/exception.h"
#include "rocketmq/common/logging.h"

namespace rocketmq {

namespace {

// Find the index in cidAll: -1 when absent (indexOf semantics).
int32_t indexOfCid(const std::vector<std::string>& cidAll, const std::string& currentCid) {
    auto it = std::find(cidAll.begin(), cidAll.end(), currentCid);
    if (it == cidAll.end()) return -1;
    return static_cast<int32_t>(it - cidAll.begin());
}

// Copy mqAll's [start, start+range) into the result, clamping any out-of-range bound.
std::vector<MessageQueue> slice(const std::vector<MessageQueue>& mqAll, int32_t start, int32_t range) {
    std::vector<MessageQueue> result;
    const int32_t total = static_cast<int32_t>(mqAll.size());
    if (range <= 0 || start < 0 || start >= total) return result;
    const int32_t end = std::min(total, start + range);
    result.assign(mqAll.begin() + start, mqAll.begin() + end);
    return result;
}

}  // namespace

std::vector<std::string> javaSplit(const std::string& text, char sep) {
    // Split on the separator everywhere first, then drop trailing empty segments;
    // but when the separator **never matches**, return the whole string unchanged (even if it is empty) without trimming.
    std::vector<std::string> parts;
    size_t pos = 0;
    bool matched = false;
    for (;;) {
        const size_t hit = text.find(sep, pos);
        if (hit == std::string::npos) {
            parts.push_back(text.substr(pos));
            break;
        }
        matched = true;
        parts.push_back(text.substr(pos, hit - pos));
        pos = hit + 1;
    }
    if (!matched) return parts;
    while (!parts.empty() && parts.back().empty()) parts.pop_back();
    return parts;
}

int32_t checkAllocateConfig(const std::string& consumerGroup, const std::string& currentCid,
                            const std::vector<MessageQueue>& mqAll,
                            const std::vector<std::string>& cidAll) {
    if (currentCid.empty()) return -1;
    if (mqAll.empty()) return -1;
    if (cidAll.empty()) return -1;
    int32_t index = indexOfCid(cidAll, currentCid);
    if (index < 0) {
        // The log text for this client missing from cidAll is a fixed phrase matched on the ops side; keep it unchanged.
        std::string cids;
        for (size_t i = 0; i < cidAll.size(); ++i) {
            if (i) cids += ", ";
            cids += cidAll[i];
        }
        logger_info("[BUG] ConsumerGroup: " + consumerGroup + " The consumerId: " + currentCid +
                    " not in cidAll: [" + cids + "]");
        return -1;
    }
    return index;
}

// -------------------------------------------------------------------- AVG
std::vector<MessageQueue> AllocateMessageQueueAveragely::allocate(
    const std::string& consumerGroup, const std::string& currentCid,
    const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const {
    int32_t index = checkAllocateConfig(consumerGroup, currentCid, mqAll, cidAll);
    if (index < 0) return {};

    const int32_t mqSize = static_cast<int32_t>(mqAll.size());
    const int32_t cidSize = static_cast<int32_t>(cidAll.size());
    const int32_t mod = mqSize % cidSize;
    const int32_t averageSize = mqSize <= cidSize
                                    ? 1
                                    : (mod > 0 && index < mod ? mqSize / cidSize + 1 : mqSize / cidSize);
    const int32_t startIndex =
        (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
    return slice(mqAll, startIndex, std::min(averageSize, mqSize - startIndex));
}

// ------------------------------------------------------------ AVG_BY_CIRCLE
std::vector<MessageQueue> AllocateMessageQueueAveragelyByCircle::allocate(
    const std::string& consumerGroup, const std::string& currentCid,
    const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const {
    int32_t index = checkAllocateConfig(consumerGroup, currentCid, mqAll, cidAll);
    if (index < 0) return {};

    // Walk from index with stride cidAll.size(): every queue with `i % cidAll.size() == index` belongs to this consumer.
    std::vector<MessageQueue> result;
    const int32_t step = static_cast<int32_t>(cidAll.size());  // the guard guarantees >= 1
    for (int32_t i = index; i < static_cast<int32_t>(mqAll.size()); i += step) {
        result.push_back(mqAll[static_cast<size_t>(i)]);
    }
    return result;
}

// ----------------------------------------------------------------- CONFIG
AllocateMessageQueueByConfig::AllocateMessageQueueByConfig(
    const std::vector<MessageQueue>& messageQueueList) : messageQueueList_(messageQueueList) {}

void AllocateMessageQueueByConfig::setMessageQueueList(
    const std::vector<MessageQueue>& messageQueueList) {
    std::lock_guard<std::mutex> lk(lock_);
    messageQueueList_ = messageQueueList;
}

std::vector<MessageQueue> AllocateMessageQueueByConfig::getMessageQueueList() const {
    std::lock_guard<std::mutex> lk(lock_);
    return messageQueueList_;
}

std::vector<MessageQueue> AllocateMessageQueueByConfig::allocate(
    const std::string& /*consumerGroup*/, const std::string& /*currentCid*/,
    const std::vector<MessageQueue>& /*mqAll*/, const std::vector<std::string>& /*cidAll*/) const {
    // No validation; returns an empty list when unconfigured (a copy, so no shared mutable reference escapes).
    return getMessageQueueList();
}

// -------------------------------------------------------- CONSISTENT_HASH
AllocateMessageQueueConsistentHash::AllocateMessageQueueConsistentHash(
    int32_t virtualNodeCnt, const std::shared_ptr<const HashFunction>& customHashFunction)
    : virtualNodeCnt_(virtualNodeCnt), customHashFunction_(customHashFunction) {
    // A negative virtual-node count is a **construction-time** error (not on the rebalance background path), so the
    // header contract 1 "guard returns empty" does not apply: throw directly.
    if (virtualNodeCnt < 0) {
        throw MQClientException("illegal virtualNodeCnt :" + std::to_string(virtualNodeCnt));
    }
}

std::vector<MessageQueue> AllocateMessageQueueConsistentHash::allocate(
    const std::string& consumerGroup, const std::string& currentCid,
    const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const {
    if (checkAllocateConfig(consumerGroup, currentCid, mqAll, cidAll) < 0) return {};

    // Wrap each cidAll entry as a ClientNode to build the ring; with customHashFunction == nullptr the ring uses MD5Hash.
    std::vector<std::shared_ptr<Node>> cidNodes;
    cidNodes.reserve(cidAll.size());
    for (const std::string& cid : cidAll) {
        cidNodes.push_back(std::make_shared<ClientNode>(cid));
    }
    const ConsistentHashRouter router(cidNodes, virtualNodeCnt_, customHashFunction_);

    std::vector<MessageQueue> result;
    for (const MessageQueue& mq : mqAll) {
        // The ring key is the MessageQueue's full string (toString); after routing to a node, its key is compared with currentCid.
        // The nullptr branch is genuinely reachable: the virtual-node count may be 0 => empty ring => nobody gets any queue.
        const std::shared_ptr<Node> node = router.routeNode(mq.toString());
        if (node != nullptr && node->getKey() == currentCid) result.push_back(mq);
    }
    return result;
}

// ------------------------------------------------------------- MACHINE_ROOM
AllocateMessageQueueByMachineRoom::AllocateMessageQueueByMachineRoom(
    const std::set<std::string>& consumeridcs) : consumeridcs_(consumeridcs) {}

void AllocateMessageQueueByMachineRoom::setConsumeridcs(
    const std::set<std::string>& consumeridcs) {
    std::lock_guard<std::mutex> lk(lock_);
    consumeridcs_ = consumeridcs;
}

std::set<std::string> AllocateMessageQueueByMachineRoom::getConsumeridcs() const {
    std::lock_guard<std::mutex> lk(lock_);
    return consumeridcs_;
}

std::vector<MessageQueue> AllocateMessageQueueByMachineRoom::allocate(
    const std::string& consumerGroup, const std::string& currentCid,
    const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const {
    const int32_t currentIndex = checkAllocateConfig(consumerGroup, currentCid, mqAll, cidAll);
    if (currentIndex < 0) return {};

    const std::set<std::string> rooms = getConsumeridcs();
    // Test: the split broker name has **exactly 2 segments** and the first is in the configured machine-room set.
    // It must be javaSplit: `"room1@"` yields 1 segment => it does **not** take part in allocation, while a naive split would include it.
    std::vector<MessageQueue> premqAll;
    for (const MessageQueue& mq : mqAll) {
        const std::vector<std::string> temp = javaSplit(mq.brokerName, '@');
        if (temp.size() == 2 && rooms.count(temp[0]) > 0) premqAll.push_back(mq);
    }

    // This split formula **differs from AVG**: the remainder queues go to the **first rem** consumers via `rem > currentIndex`,
    // and every consumer's range length stays exactly mod (under AVG, those taking more also get longer ranges).
    const int32_t cidSize = static_cast<int32_t>(cidAll.size());  // the guard guarantees >= 1
    const int32_t mod = static_cast<int32_t>(premqAll.size()) / cidSize;
    const int32_t rem = static_cast<int32_t>(premqAll.size()) % cidSize;
    std::vector<MessageQueue> result = slice(premqAll, mod * currentIndex, mod);
    if (rem > currentIndex) {
        // Out-of-range is impossible: `currentIndex < rem` and `rem < cidSize`
        // imply `currentIndex + mod*cidSize < rem + mod*cidSize == premqAll.size()`.
        result.push_back(premqAll[static_cast<size_t>(currentIndex + mod * cidSize)]);
    }
    return result;
}

// --------------------------------------------------- MACHINE_ROOM_NEARBY
AllocateMachineRoomNearby::AllocateMachineRoomNearby(
    const std::shared_ptr<AllocateMessageQueueStrategy>& allocateMessageQueueStrategy,
    const std::shared_ptr<MachineRoomResolver>& machineRoomResolver)
    : allocateMessageQueueStrategy_(allocateMessageQueueStrategy),
      machineRoomResolver_(machineRoomResolver) {
    // Missing arguments are reported as MQClientException meaning "invalid configuration" (header contract 2);
    // the text keeps the existing `... is null` phrasing because callers match it literally.
    if (allocateMessageQueueStrategy == nullptr) throw MQClientException("allocateMessageQueueStrategy is null");
    if (machineRoomResolver == nullptr) throw MQClientException("machineRoomResolver is null");
    // Name = "MACHINE_ROOM_NEARBY-" + the delegate name; computed at construction.
    name_ = "MACHINE_ROOM_NEARBY-" + allocateMessageQueueStrategy->getName();
}

std::vector<MessageQueue> AllocateMachineRoomNearby::allocate(
    const std::string& consumerGroup, const std::string& currentCid,
    const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const {
    if (checkAllocateConfig(consumerGroup, currentCid, mqAll, cidAll) < 0) return {};

    // Group by machine room: std::map => rooms are visited in **lexicographic order**, matching the required grouping order contract.
    std::map<std::string, std::vector<MessageQueue>> mr2Mq;
    for (const MessageQueue& mq : mqAll) {
        const std::string brokerMachineRoom = machineRoomResolver_->brokerDeployIn(mq);
        // An empty machine-room string means "room unknown" -> throw.
        // Throwing instead of returning empty: a silent return would withdraw every queue of the topic (an exception to header contract 1).
        if (brokerMachineRoom.empty()) {
            throw MQClientException("Machine room is null for mq " + mq.toString());
        }
        mr2Mq[brokerMachineRoom].push_back(mq);
    }
    std::map<std::string, std::vector<std::string>> mr2c;
    for (const std::string& cid : cidAll) {
        const std::string consumerMachineRoom = machineRoomResolver_->consumerDeployIn(cid);
        if (consumerMachineRoom.empty()) {
            throw MQClientException("Machine room is null for consumer id " + cid);
        }
        mr2c[consumerMachineRoom].push_back(cid);
    }

    std::vector<MessageQueue> allocateResults;

    // 1. Queues in this consumer's machine room go only to consumers in the same room.
    //    They are removed from mr2Mq first, so the shared traversal in step 2 does not allocate them again.
    const std::string currentMachineRoom = machineRoomResolver_->consumerDeployIn(currentCid);
    std::vector<MessageQueue> mqInThisMachineRoom;
    auto mqIt = mr2Mq.find(currentMachineRoom);
    if (mqIt != mr2Mq.end()) {
        mqInThisMachineRoom = std::move(mqIt->second);
        mr2Mq.erase(mqIt);
    }
    // A miss passes an empty list to the delegate; the guard ensures currentCid is in cidAll, and the grouping above
    // put all of cidAll into mr2c => a hit is guaranteed, so a miss means "empty", not "absent".
    std::vector<std::string> consumerInThisMachineRoom;
    auto cidIt = mr2c.find(currentMachineRoom);
    if (cidIt != mr2c.end()) consumerInThisMachineRoom = cidIt->second;
    if (!mqInThisMachineRoom.empty()) {
        const std::vector<MessageQueue> got = allocateMessageQueueStrategy_->allocate(
            consumerGroup, currentCid, mqInThisMachineRoom, consumerInThisMachineRoom);
        allocateResults.insert(allocateResults.end(), got.begin(), got.end());
    }

    // 2. Rooms without any live consumer have their queues shared by all consumers (otherwise nothing would be consumed).
    for (const auto& machineRoomEntry : mr2Mq) {
        if (mr2c.count(machineRoomEntry.first) > 0) continue;
        const std::vector<MessageQueue> got = allocateMessageQueueStrategy_->allocate(
            consumerGroup, currentCid, machineRoomEntry.second, cidAll);
        allocateResults.insert(allocateResults.end(), got.begin(), got.end());
    }
    return allocateResults;
}

}  // namespace rocketmq
