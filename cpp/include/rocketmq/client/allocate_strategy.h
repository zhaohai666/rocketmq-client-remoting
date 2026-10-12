// 队列分配策略（rebalance 时把某个 topic 的队列分给同一消费组里的各个消费者）。
//
// 接口三件套：`allocate`（算出本实例分到的队列）、`getName`（算法名）、
// 内置守卫 `checkAllocateConfig`。
//
// ## 覆盖范围
//
// 共 6 个策略：AVG / AVG_BY_CIRCLE / CONFIG /
// `CONSISTENT_HASH`（要 `rocketmq/common/consistent_hash.h` 的哈希环）/ `MACHINE_ROOM` /
// `MACHINE_ROOM_NEARBY-<内层>`（代理模式，见 `AllocateMachineRoomNearby`）。
//
// ## 行为口径
//
// 1. **守卫不抛异常**：`currentCID` 为空串、`mqAll` 为空、`cidAll` 为空这类非法入参
//    一律返回空列表：rebalance 是后台周期任务，一条脏入参不该把消费者打挂。
//    两个例外：**构造期**错误（如负数虚拟节点数）不在后台路径上，直接抛
//    `MQClientException`；`AllocateMachineRoomNearby` 的 resolver 给出空机房时**照抛**，
//    因为静默返回空列表等于把整个 topic 的队列撤走，而 rebalance 抓住异常时反而会保住现有分配。
// 2. **未配置态规整**：`AllocateMessageQueueByConfig` 没配过队列列表时返回**空列表副本**
//    （而不是透传空引用），且 `allocate` 每次返回副本；`AllocateMachineRoomNearby`
//    构造器缺参数、`AllocateMessageQueueByMachineRoom` 未配置机房集合同理，分别用
//    `MQClientException`（文案保留既有 `... is null` 句式）和空集合表达。
// 3. `allocate` 声明为 `const`：策略对象在多个线程（rebalance 线程与各队列拉取线程）间共享，
//    用 const 接口表达「分配算法本身不改状态」。
//    `ByConfig` / `ByMachineRoom` 的可配置列表因此放在 `mutable` + mutex 后面（setter 口径）。
// 4. **broker 名的 `@` 切分**：按 `javaSplit` 的口径切——**丢掉末尾空段**
//    （`"room1@"` → 1 段、`"room1@b@"` → 2 段），朴素 `std::string` 手工切分不会；
//    切错会让一条队列参不参与分配变得不确定。
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

// 分配策略接口。
//
// - `consumerGroup`：当前消费组，只出现在「本实例不在 cidAll 里」那条日志里；
// - `currentCid`：本客户端 id，它在 `cidAll` 里的下标决定分哪一段；
// - `mqAll`：该 topic 的全部队列。**调用方负责排序**：rebalance 在调用策略前
//   已对 `cidAll` / `mqAll` 排过序；
// - `cidAll`：该消费组的全部客户端 id，同样要求已排序。
//
// 策略本身不排序、不去重，只保证「输出是输入的一个有序子序列」，结果确定可复现。
class AllocateMessageQueueStrategy {
public:
    virtual ~AllocateMessageQueueStrategy() = default;

    virtual std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const = 0;

    // 算法名（AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH /
    // MACHINE_ROOM / MACHINE_ROOM_NEARBY-<内层>），用于日志与策略识别。
    virtual std::string getName() const = 0;
};

// 按「裁掉末尾空段」口径切分字符串：**丢掉末尾的空段**。
//
// 与手工 `std::string` 切分的唯一区别就在尾上空段：`"room1@"` 得 `{"room1"}`（1 段），
// 朴素切分得 `{"room1", ""}`（2 段）。但**没命中分隔符时**整串原样给出
// （哪怕它本身就是空串），所以不能无条件裁尾。既定结果：
// `""` → 1 段 `{""}`、`"@"` → 0 段、`"room1@"` → 1 段、`"room1@b@"` → 2 段。
std::vector<std::string> javaSplit(const std::string& text, char sep);

// 内置守卫：非法入参（currentCid 为空串 / mqAll 为空 / cidAll 为空 / currentCid 不在
// cidAll 里）时返回 -1，通过时返回 `currentCid` 在 `cidAll` 中的下标。
// 暴露出来是为了自定义策略能复用同一段守卫，行为与内置策略保持一致。
int32_t checkAllocateConfig(const std::string& consumerGroup, const std::string& currentCid,
                            const std::vector<MessageQueue>& mqAll,
                            const std::vector<std::string>& cidAll);

// 平均分配：把 mqAll 切成**连续区间**，余数 mod 依次分给前 mod 个消费者
// （`getName()` = "AVG"）。
class AllocateMessageQueueAveragely : public AllocateMessageQueueStrategy {
public:
    std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const override;

    std::string getName() const override { return "AVG"; }
};

// 环形平均分配：第 index 个消费者取走下标 ≡ index (mod cidAll.size()) 的队列
// （`getName()` = "AVG_BY_CIRCLE"）。
class AllocateMessageQueueAveragelyByCircle : public AllocateMessageQueueStrategy {
public:
    std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const override;

    std::string getName() const override { return "AVG_BY_CIRCLE"; }
};

// 按显式配置分配：完全无视 mqAll / cidAll，把配置进去的队列列表原样发给**每一个**消费者
// （通常用于广播式手工绑队列），`allocate` **不调 check**（`getName()` = "CONFIG"）。
class AllocateMessageQueueByConfig : public AllocateMessageQueueStrategy {
public:
    AllocateMessageQueueByConfig() = default;
    explicit AllocateMessageQueueByConfig(const std::vector<MessageQueue>& messageQueueList);

    // 整体替换队列列表。非 const：策略对象按 shared_ptr 共享，注册到消费者之后
    // 仍可改，改动对所有后续 rebalance 生效。
    void setMessageQueueList(const std::vector<MessageQueue>& messageQueueList);

    // 返回副本（不交出共享可变引用）。
    std::vector<MessageQueue> getMessageQueueList() const;

    std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const override;

    std::string getName() const override { return "CONFIG"; }

private:
    mutable std::mutex lock_;
    std::vector<MessageQueue> messageQueueList_;
};

// 一致性哈希分配：把每个 clientId 按比例铺成虚拟节点组成哈希环，再看每条队列
// （以 `MessageQueue::toString()` 为 key）落进哪个区间
// （`getName()` = "CONSISTENT_HASH"），环本体在 `rocketmq/common/consistent_hash.h`。
//
// 与 AVG 的关键差别：**消费者增减时只有边界上的队列换主**，而不是整段重排，
// 所以扩缩容风暴小得多；代价是分布不如 AVG 均匀。
//
// 哈希环每次 `allocate` 现场重建，策略对象本身无可变状态 → 天然线程安全。
class AllocateMessageQueueConsistentHash : public AllocateMessageQueueStrategy {
public:
    // 默认 10 个虚拟节点。
    AllocateMessageQueueConsistentHash() : AllocateMessageQueueConsistentHash(10, nullptr) {}

    explicit AllocateMessageQueueConsistentHash(int32_t virtualNodeCnt)
        : AllocateMessageQueueConsistentHash(virtualNodeCnt, nullptr) {}

    // `customHashFunction == nullptr` 走环自带的 MD5Hash。
    // 负数 `virtualNodeCnt` → 抛 `MQClientException`（构造期错误，见行为口径 1）。
    AllocateMessageQueueConsistentHash(int32_t virtualNodeCnt,
                                       const std::shared_ptr<const HashFunction>& customHashFunction);

    // 暴露虚拟节点数，用于自检与日志。
    int32_t getVirtualNodeCnt() const { return virtualNodeCnt_; }

    std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const override;

    std::string getName() const override { return "CONSISTENT_HASH"; }

private:
    int32_t virtualNodeCnt_;
    std::shared_ptr<const HashFunction> customHashFunction_;
};

// 机房分配：只把 broker 名形如 `<机房>@<集群>` 的队列参与分配，机房集合由配置给出
// （`getName()` = "MACHINE_ROOM"，面向逻辑机房这类按机房排队列的场景）。
//
// ⚠ broker 名用 `javaSplit` 切，判据是「裁掉尾空段后正好 2 段」；
// ⚠ `consumeridcs` 没 set 过就规整成**空集合** ⇒ 未配置时谁都分不到队列，
//    而不是把消费者打挂（见行为口径 2）。
class AllocateMessageQueueByMachineRoom : public AllocateMessageQueueStrategy {
public:
    AllocateMessageQueueByMachineRoom() = default;
    explicit AllocateMessageQueueByMachineRoom(const std::set<std::string>& consumeridcs);

    // 整体替换机房集合。非 const：同 `ByConfig` 的 setter 口径
    // （策略对象注册进消费者之后仍可改，改动对所有后续 rebalance 生效）。
    void setConsumeridcs(const std::set<std::string>& consumeridcs);

    // 返回副本。
    std::set<std::string> getConsumeridcs() const;

    std::vector<MessageQueue> allocate(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) const override;

    std::string getName() const override { return "MACHINE_ROOM"; }

private:
    mutable std::mutex lock_;
    std::set<std::string> consumeridcs_;
};

// MachineRoomResolver：告诉策略「某个队列 / 某个客户端在哪个机房」。接口约定
// 返回值**不能为空**；返回空串视为机房未知，会抛 `MQClientException`
// （见行为口径 1 的例外条款）。
struct MachineRoomResolver {
    virtual ~MachineRoomResolver() = default;
    virtual std::string brokerDeployIn(const MessageQueue& messageQueue) = 0;
    virtual std::string consumerDeployIn(const std::string& clientId) = 0;
};

// 机房就近代理策略：先把队列和消费者按机房分组，
//  1. 本消费者所在机房的队列只分给同机房的消费者（用内层策略算）；
//  2. 没有任何存活消费者的机房，其队列由所有机房的消费者按 `cidAll` 一起分（共享）。
// 因此**本消费者会拿到别机房队列**，且本机房队列排在结果前面（拼接顺序即如此）。
//
// `getName()` = `"MACHINE_ROOM_NEARBY-" + 内层名`。
class AllocateMachineRoomNearby : public AllocateMessageQueueStrategy {
public:
    // 参数为空时抛 `MQClientException`（文案保留既有 `... is null` 句式，
    // 调用方按原文匹配；见行为口径 2）。
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
