// 队列分配策略实现；行为口径见头文件「行为口径」一节。
#include "rocketmq/client/allocate_strategy.h"

#include <algorithm>
#include <map>

#include "rocketmq/client/exception.h"
#include "rocketmq/common/logging.h"

namespace rocketmq {

namespace {

// 在 cidAll 里找下标：找不到返回 -1（indexOf 口径）。
int32_t indexOfCid(const std::vector<std::string>& cidAll, const std::string& currentCid) {
    auto it = std::find(cidAll.begin(), cidAll.end(), currentCid);
    if (it == cidAll.end()) return -1;
    return static_cast<int32_t>(it - cidAll.begin());
}

// 把 mqAll 的 [start, start+range) 拷进结果，越界一律截断。
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
    // 先按分隔符全切，再裁掉末尾空段；
    // 但**一次都没命中**时整串原样早返回（哪怕它本身是空串），不裁尾。
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
        // 本客户端不在 cidAll 里的日志文案是运维侧匹配的固定条款，保持不变。
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

    // 从下标 index 起步进，步长 cidAll.size()：凡 `i % cidAll.size() == index` 的队列归本消费者。
    std::vector<MessageQueue> result;
    const int32_t step = static_cast<int32_t>(cidAll.size());  // 守卫保证 >= 1
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
    // 不做 check；未配置时返回空列表（且是副本，不交出共享可变引用）。
    return getMessageQueueList();
}

// -------------------------------------------------------- CONSISTENT_HASH
AllocateMessageQueueConsistentHash::AllocateMessageQueueConsistentHash(
    int32_t virtualNodeCnt, const std::shared_ptr<const HashFunction>& customHashFunction)
    : virtualNodeCnt_(virtualNodeCnt), customHashFunction_(customHashFunction) {
    // 负数虚拟节点数是**构造期**错误（不在 rebalance 后台路径上），头文件口径 1 的
    // 「守卫返回空」不适用，直接抛。
    if (virtualNodeCnt < 0) {
        throw MQClientException("illegal virtualNodeCnt :" + std::to_string(virtualNodeCnt));
    }
}

std::vector<MessageQueue> AllocateMessageQueueConsistentHash::allocate(
    const std::string& consumerGroup, const std::string& currentCid,
    const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const {
    if (checkAllocateConfig(consumerGroup, currentCid, mqAll, cidAll) < 0) return {};

    // cidAll 逐个包成 ClientNode 建环；customHashFunction == nullptr 时环自带 MD5Hash。
    std::vector<std::shared_ptr<Node>> cidNodes;
    cidNodes.reserve(cidAll.size());
    for (const std::string& cid : cidAll) {
        cidNodes.push_back(std::make_shared<ClientNode>(cid));
    }
    const ConsistentHashRouter router(cidNodes, virtualNodeCnt_, customHashFunction_);

    std::vector<MessageQueue> result;
    for (const MessageQueue& mq : mqAll) {
        // 环的 key 是 MessageQueue 的全量串（toString），路由到节点后再比节点 key 与 currentCid。
        // nullptr 分支真的会走到：虚拟节点数可以是 0 ⇒ 环空 ⇒ 谁都拿不到队列。
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
    // 判据：broker 名切分后**正好 2 段**且首段在配置机房集合里。
    // 必须是 javaSplit：`"room1@"` 切出 1 段 ⇒ **不参与**分配，朴素切分则会给它。
    std::vector<MessageQueue> premqAll;
    for (const MessageQueue& mq : mqAll) {
        const std::vector<std::string> temp = javaSplit(mq.brokerName, '@');
        if (temp.size() == 2 && rooms.count(temp[0]) > 0) premqAll.push_back(mq);
    }

    // 注意这条切分算式与 AVG **不同**：余数队列按 `rem > currentIndex` 分给**前 rem 个**
    // 消费者，且每人的区间长度恒为 mod（AVG 是多拿的人区间也更长）。
    const int32_t cidSize = static_cast<int32_t>(cidAll.size());  // 守卫保证 >= 1
    const int32_t mod = static_cast<int32_t>(premqAll.size()) / cidSize;
    const int32_t rem = static_cast<int32_t>(premqAll.size()) % cidSize;
    std::vector<MessageQueue> result = slice(premqAll, mod * currentIndex, mod);
    if (rem > currentIndex) {
        // 越界不可能：`currentIndex < rem` 且 `rem < cidSize`
        // ⇒ `currentIndex + mod*cidSize < rem + mod*cidSize == premqAll.size()`。
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
    // 参数缺失用 MQClientException 表达「非法配置」（头文件口径 2）；
    // 文案保留 `... is null` 既有句式，调用方按原文匹配。
    if (allocateMessageQueueStrategy == nullptr) throw MQClientException("allocateMessageQueueStrategy is null");
    if (machineRoomResolver == nullptr) throw MQClientException("machineRoomResolver is null");
    // 名字 = "MACHINE_ROOM_NEARBY-" + 内层名；构造期算好。
    name_ = "MACHINE_ROOM_NEARBY-" + allocateMessageQueueStrategy->getName();
}

std::vector<MessageQueue> AllocateMachineRoomNearby::allocate(
    const std::string& consumerGroup, const std::string& currentCid,
    const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const {
    if (checkAllocateConfig(consumerGroup, currentCid, mqAll, cidAll) < 0) return {};

    // 按机房分组：std::map ⇒ 机房按**字典序**遍历，分配结果与分组顺序口径一致。
    std::map<std::string, std::vector<MessageQueue>> mr2Mq;
    for (const MessageQueue& mq : mqAll) {
        const std::string brokerMachineRoom = machineRoomResolver_->brokerDeployIn(mq);
        // 机房串为空即「机房未知」→ 抛异常。
        // 照抛而不返回空：静默返回等于把整个 topic 的队列撤走（头文件口径 1 的例外）。
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

    // 1. 与本消费者同机房的队列只分给同机房的消费者。
    //    先把本机房队列从 mr2Mq 摘走（remove），第 2 步共享遍历才不会重复分它。
    const std::string currentMachineRoom = machineRoomResolver_->consumerDeployIn(currentCid);
    std::vector<MessageQueue> mqInThisMachineRoom;
    auto mqIt = mr2Mq.find(currentMachineRoom);
    if (mqIt != mr2Mq.end()) {
        mqInThisMachineRoom = std::move(mqIt->second);
        mr2Mq.erase(mqIt);
    }
    // 查不到就给内层策略传空列表；守卫保证 currentCid ∈ cidAll，而上面的分组
    // 已把 cidAll 全部放进 mr2c ⇒ 必然命中，查不到是"空"而非"缺"。
    std::vector<std::string> consumerInThisMachineRoom;
    auto cidIt = mr2c.find(currentMachineRoom);
    if (cidIt != mr2c.end()) consumerInThisMachineRoom = cidIt->second;
    if (!mqInThisMachineRoom.empty()) {
        const std::vector<MessageQueue> got = allocateMessageQueueStrategy_->allocate(
            consumerGroup, currentCid, mqInThisMachineRoom, consumerInThisMachineRoom);
        allocateResults.insert(allocateResults.end(), got.begin(), got.end());
    }

    // 2. 没有任何存活消费者的机房，其队列由全部消费者共享（否则无人消费）。
    for (const auto& machineRoomEntry : mr2Mq) {
        if (mr2c.count(machineRoomEntry.first) > 0) continue;
        const std::vector<MessageQueue> got = allocateMessageQueueStrategy_->allocate(
            consumerGroup, currentCid, machineRoomEntry.second, cidAll);
        allocateResults.insert(allocateResults.end(), got.begin(), got.end());
    }
    return allocateResults;
}

}  // namespace rocketmq
