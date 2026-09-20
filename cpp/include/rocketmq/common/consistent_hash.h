// 一致性哈希环（对应 Java `org.apache.rocketmq.common.consistenthash` 包）。
//
// 移植 `ConsistentHashRouter` / `Node` / `VirtualNode` / `HashFunction` 四个类型加
// `ConsistentHashRouter.MD5Hash` 这个私有实现；目前唯一的用户是队列分配策略
// `AllocateMessageQueueConsistentHash`。
//
// ## 为什么整套照搬而不是"等价改写"
//
// 环上的落点由三处细节共同决定，任何一处不同都会让**全体**队列换主，与 Java 客户端
// 混跑时表现为重复消费 / 漏消费，而不是"分得稍有不均"：
//
// 1. 哈希函数只取 MD5 摘要的**前 4 个字节**按大端拼成整数（Java
//    `for (int i = 0; i < 4; i++) { h <<= 8; h |= digest[i] & 0xFF; }`），不是完整 128 bit；
// 2. 虚拟节点的 key 是 `物理节点key + "-" + 副本序号`，序号从 `existingReplicas` 起算；
// 3. 查找用 `TreeMap#tailMap(hashVal)`，它**含端点**，所以等价于 `std::map::lower_bound`
//    （相等的 hash 归自己），越过环末尾时回绕到 `begin()`。
//
// ## 与 Java 的有意差异
//
// 1. Java 的环是 `TreeMap<Long, VirtualNode<T>>`；这里物理节点用 `std::shared_ptr<Node>`
//    做动态分发，`routeNode` 因而交出 `std::shared_ptr<Node>`（可能为 nullptr = Java 的 null）。
// 2. `virtualNodeCnt < 0` 在 Java 抛 `IllegalArgumentException`，这里抛
//    `MQClientException`（本端口对"非法配置"的统一口径）。
// 3. MD5 是自带实现（`md5Digest`），不引第三方库：与 `src/remoting/rpchook.cpp` 自带
//    SHA1/HMAC 同一个理由 —— 定长算法几十行，自带反而好与 Java 逐字节对拍。
//    只用于算环上的落点，**不当密码学原语用**。
#ifndef ROCKETMQ_COMMON_CONSISTENT_HASH_H
#define ROCKETMQ_COMMON_CONSISTENT_HASH_H

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace rocketmq {

// 对应 Java `Node#getKey`：能被映到环上的东西，物理节点和虚拟节点都算。
struct Node {
    virtual ~Node() = default;
    virtual std::string getKey() const = 0;
};

// 对应 Java `AllocateMessageQueueConsistentHash.ClientNode`：key 就是 clientId。
//
// 注：Java 里它是策略的 private 静态内部类，本端口与 `common` 的其它节点类型放在一起
// （C++ 没有"内部类前置声明"的等价物，且 Python/Rust 端口同样把它暴露在哈希模块里）。
struct ClientNode : public Node {
    std::string clientId;

    explicit ClientNode(const std::string& clientId) : clientId(clientId) {}

    std::string getKey() const override { return clientId; }
};

// 对应 Java `HashFunction#hash(String)`：把字符串映射到环上的位置。
struct HashFunction {
    virtual ~HashFunction() = default;
    virtual int64_t hash(const std::string& key) const = 0;
};

// 对应 Java `ConsistentHashRouter.MD5Hash`（默认哈希函数）。
//
// ⚠ 只取摘要前 4 字节（不是完整 128 bit）；换取法就不是 Java 的那个环。
struct Md5Hash : public HashFunction {
    int64_t hash(const std::string& key) const override;
};

// RFC 1321 MD5 摘要（16 字节，小端字序输出，与 Java `MessageDigest#digest` 同）。
// 暴露出来给单测对拍官方向量用；生产路径只经 `Md5Hash` 使用它。
std::vector<uint8_t> md5Digest(const std::string& input);

// 对应 Java `ConsistentHashRouter`：虚拟节点环 + "顺时针找最近物理节点"。
class ConsistentHashRouter {
public:
    // 对应 Java `ConsistentHashRouter(Collection<T>, int)`：用默认 MD5Hash。
    ConsistentHashRouter(const std::vector<std::shared_ptr<Node>>& pNodes, int32_t vNodeCount);

    // 对应 Java `ConsistentHashRouter(Collection<T>, int, HashFunction)`。
    // `hashFunction == nullptr` 对应 Java 的 null —— 走默认 MD5Hash（Java 此处抛 NPE，
    // 本端口按"空即默认"处理，与 `AllocateMessageQueueConsistentHash` 的 customHashFunction
    // 判空口径一致）。
    ConsistentHashRouter(const std::vector<std::shared_ptr<Node>>& pNodes, int32_t vNodeCount,
                         const std::shared_ptr<const HashFunction>& hashFunction);

    // 对应 Java `#addNode`：`i + existingReplicas` 那段不是冗余 —— 同一个物理节点分两次
    // addNode 时，Java 靠已有副本数把虚拟节点编号继续往后排；从 0 重编会让两批虚拟节点
    // 撞在同一个 hash 上（Java 是 TreeMap.put，后者覆盖前者 ⇒ 实际少一半节点）。
    void addNode(const std::shared_ptr<Node>& pNode, int32_t vNodeCount);

    // 对应 Java `#removeNode`：摘掉该物理节点的全部虚拟节点（按 key 判定，Java 亦如此）。
    void removeNode(const std::shared_ptr<Node>& pNode);

    // 对应 Java `#routeNode`：环空返回 nullptr，否则返回顺时针第一个（含同 hash）
    // 虚拟节点所属的物理节点。
    std::shared_ptr<Node> routeNode(const std::string& objectKey) const;

    // 对应 Java `#getExistingReplicas`。
    int32_t getExistingReplicas(const std::shared_ptr<Node>& pNode) const;

    // 环上的 hash 集合（调试与单测用；顺序即 Java TreeMap 的升序）。
    std::vector<int64_t> ringHashes() const;

private:
    // Java 是 TreeMap<Long, VirtualNode<T>>；std::map 的 lower_bound 就是含端点的 tailMap。
    // value 即 Java 的 `VirtualNode`：物理节点指针 + 副本序号（`getKey()` = key + "-" + 序号）。
    struct VirtualNode {
        std::shared_ptr<Node> physicalNode;
        int32_t replicaIndex;

        // 对应 Java `VirtualNode#getKey`。
        std::string getKey() const {
            return physicalNode->getKey() + "-" + std::to_string(replicaIndex);
        }

        // 对应 Java `VirtualNode#isVirtualNodeOf`：Java 只比 `getKey()`，从不比对象身份。
        bool isVirtualNodeOf(const std::shared_ptr<Node>& pNode) const {
            return physicalNode->getKey() == pNode->getKey();
        }
    };

    std::map<int64_t, VirtualNode> ring_;
    std::shared_ptr<const HashFunction> hashFunction_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_CONSISTENT_HASH_H
