// Implements the four types `ConsistentHashRouter` / `Node` / `VirtualNode` / `HashFunction` plus the
// default `MD5Hash`; its only user today is the queue allocation strategy `AllocateMessageQueueConsistentHash`.
//
// ## Why the scheme is followed exactly instead of being "equivalently rewritten"
//
// The landing point on the ring is decided jointly by three details; a difference in any one of them makes **all** queues
// change owner, i.e. duplicate / missed consumption when mixed with other consumers of the group, not merely "slightly uneven":
//
// 1. The hash function takes only the **first 4 bytes** of the MD5 digest, assembled big-endian into an integer
//    (`for (int i = 0; i < 4; i++) { h <<= 8; h |= digest[i] & 0xFF; }`), not the full 128 bit;
// 2. A virtual node's key is `physicalNodeKey + "-" + replicaIndex`, the index starting at `existingReplicas`;
// 3. Lookup takes the first ring entry with hash >= the target hash — the **endpoint is inclusive** — so it equals
//    `std::map::lower_bound` (an equal hash belongs to that node itself), wrapping to `begin()` past the ring end.
//
// ## Intentional differences
//
// 1. The ring is a `std::map<int64_t, VirtualNode>` here; physical nodes are held as `std::shared_ptr<Node>` for
//    dynamic dispatch, so `routeNode` hands out a `std::shared_ptr<Node>` (nullptr possible = empty).
// 2. `virtualNodeCnt < 0` throws `MQClientException` (the uniform stance of this codebase for "illegal configuration").
// 3. MD5 is implemented in-tree (`md5Digest`) without a third-party library, for the same reason as the in-tree
//    SHA1/HMAC in `src/remoting/rpchook.cpp` — a fixed-length algorithm is a few tens of lines, and owning it makes
//    byte-for-byte cross-checking easier. It is only used to compute ring positions, **not as a cryptographic primitive**.
#ifndef ROCKETMQ_COMMON_CONSISTENT_HASH_H
#define ROCKETMQ_COMMON_CONSISTENT_HASH_H

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace rocketmq {

// `Node::getKey`: whatever can be mapped onto the ring counts, both physical and virtual nodes.
struct Node {
    virtual ~Node() = default;
    virtual std::string getKey() const = 0;
};

// `ClientNode` of the consistent-hash allocation strategy: the key is just the clientId.
//
// Note: it sits with the other node types of `common` instead of being nested inside the strategy (C++ has no
// forward declaration equivalent for nested classes).
struct ClientNode : public Node {
    std::string clientId;

    explicit ClientNode(const std::string& clientId) : clientId(clientId) {}

    std::string getKey() const override { return clientId; }
};

// `HashFunction::hash`: maps a string key to a position on the ring.
struct HashFunction {
    virtual ~HashFunction() = default;
    virtual int64_t hash(const std::string& key) const = 0;
};

// `Md5Hash`: the default hash function of the router.
//
// ⚠ Only the first 4 digest bytes are taken (not the full 128 bit); another derivation changes every ring position.
struct Md5Hash : public HashFunction {
    int64_t hash(const std::string& key) const override;
};

// RFC 1321 MD5 digest (16 bytes, in the byte order of a standard digest output).
// Exposed so tests can cross-check the official vectors; production paths reach it only through `Md5Hash`.
std::vector<uint8_t> md5Digest(const std::string& input);

// `ConsistentHashRouter`: the virtual node ring plus "nearest physical node clockwise".
class ConsistentHashRouter {
public:
    // The node-list plus count constructor: uses the default MD5Hash.
    ConsistentHashRouter(const std::vector<std::shared_ptr<Node>>& pNodes, int32_t vNodeCount);

    // The node-list, count and hash-function constructor.
    // `hashFunction == nullptr` falls back to the default MD5Hash ("null means default", the same stance as the
    // customHashFunction null check in `AllocateMessageQueueConsistentHash`).
    ConsistentHashRouter(const std::vector<std::shared_ptr<Node>>& pNodes, int32_t vNodeCount,
                         const std::shared_ptr<const HashFunction>& hashFunction);

    // `addNode`: the `i + existingReplicas` offset is not redundant — when the same physical node is added through
    // addNode twice, the existing replica count keeps numbering virtual nodes onward; restarting from 0 would make the
    // two batches collide on the same hash (ring insertion overwrites ⇒ half the nodes would silently disappear).
    void addNode(const std::shared_ptr<Node>& pNode, int32_t vNodeCount);

    // `removeNode`: drop every virtual node of this physical node (matched by key).
    void removeNode(const std::shared_ptr<Node>& pNode);

    // `routeNode`: nullptr on an empty ring, otherwise the physical node owning the first virtual node
    // clockwise (an equal hash included).
    std::shared_ptr<Node> routeNode(const std::string& objectKey) const;

    // `getExistingReplicas`.
    int32_t getExistingReplicas(const std::shared_ptr<Node>& pNode) const;

    // The hash set on the ring (for debugging and tests; the order is ascending by hash).
    std::vector<int64_t> ringHashes() const;

private:
    // lower_bound of std::map is exactly the endpoint-inclusive tail view.
    // The value is a `VirtualNode`: physical node pointer plus replica index (`getKey()` = key + "-" + index).
    struct VirtualNode {
        std::shared_ptr<Node> physicalNode;
        int32_t replicaIndex;

        // `VirtualNode::getKey`.
        std::string getKey() const {
            return physicalNode->getKey() + "-" + std::to_string(replicaIndex);
        }

        // `VirtualNode::isVirtualNodeOf`: compares only `getKey()`, never object identity.
        bool isVirtualNodeOf(const std::shared_ptr<Node>& pNode) const {
            return physicalNode->getKey() == pNode->getKey();
        }
    };

    std::map<int64_t, VirtualNode> ring_;
    std::shared_ptr<const HashFunction> hashFunction_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_CONSISTENT_HASH_H
