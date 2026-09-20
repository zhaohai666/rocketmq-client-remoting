// 一致性哈希环实现 + 自带 MD5，逐条对齐 Java
// `common/consistenthash/{ConsistentHashRouter,VirtualNode,Node,HashFunction}.java`。
#include "rocketmq/common/consistent_hash.h"

#include <cstdint>

#include "rocketmq/client/exception.h"

namespace rocketmq {

namespace {

// RFC 1321 §3.4 的 64 个位移量。
const uint32_t kMd5Shifts[64] = {
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9,  14, 20, 5, 9,  14, 20,
    5, 9,  14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21};

// K[i] = floor(abs(sin(i + 1)) * 2^32)（RFC 1321 §3.4 的表）。
//
// 写成常量而不是运行时用 `std::sin` 算：`floor(abs(sin(x)) * 2^32)` 落在整数边界附近时
// 不同 libm 的最后一位可能差 1，那样 hash 就与 Java 不一致了。
const uint32_t kMd5K[64] = {
    0xd76aa478u, 0xe8c7b756u, 0x242070dbu, 0xc1bdceeeu, 0xf57c0fafu, 0x4787c62au, 0xa8304613u,
    0xfd469501u, 0x698098d8u, 0x8b44f7afu, 0xffff5bb1u, 0x895cd7beu, 0x6b901122u, 0xfd987193u,
    0xa679438eu, 0x49b40821u, 0xf61e2562u, 0xc040b340u, 0x265e5a51u, 0xe9b6c7aau, 0xd62f105du,
    0x02441453u, 0xd8a1e681u, 0xe7d3fbc8u, 0x21e1cde6u, 0xc33707d6u, 0xf4d50d87u, 0x455a14edu,
    0xa9e3e905u, 0xfcefa3f8u, 0x676f02d9u, 0x8d2a4c8au, 0xfffa3942u, 0x8771f681u, 0x6d9d6122u,
    0xfde5380cu, 0xa4beea44u, 0x4bdecfa9u, 0xf6bb4b60u, 0xbebfbc70u, 0x289b7ec6u, 0xeaa127fau,
    0xd4ef3085u, 0x04881d05u, 0xd9d4d039u, 0xe6db99e5u, 0x1fa27cf8u, 0xc4ac5665u, 0xf4292244u,
    0x432aff97u, 0xab9423a7u, 0xfc93a039u, 0x655b59c3u, 0x8f0ccc92u, 0xffeff47du, 0x85845dd1u,
    0x6fa87e4fu, 0xfe2ce6e0u, 0xa3014314u, 0x4e0811a1u, 0xf7537e82u, 0xbd3af235u, 0x2ad7d2bbu,
    0xeb86d391u};

inline uint32_t rotateLeft(uint32_t value, uint32_t shift) {
    return (value << shift) | (value >> (32 - shift));
}

inline uint32_t loadLe32(const uint8_t* at) {
    return static_cast<uint32_t>(at[0]) | (static_cast<uint32_t>(at[1]) << 8) |
           (static_cast<uint32_t>(at[2]) << 16) | (static_cast<uint32_t>(at[3]) << 24);
}

inline void storeLe32(uint8_t* at, uint32_t value) {
    at[0] = static_cast<uint8_t>(value & 0xFF);
    at[1] = static_cast<uint8_t>((value >> 8) & 0xFF);
    at[2] = static_cast<uint8_t>((value >> 16) & 0xFF);
    at[3] = static_cast<uint8_t>((value >> 24) & 0xFF);
}

}  // namespace

std::vector<uint8_t> md5Digest(const std::string& input) {
    uint32_t state[4] = {0x67452301u, 0xefcdab89u, 0x98badcfeu, 0x10325476u};

    // 填充：0x80 + 若干 0x00 把长度顶到 ≡56 (mod 64)，再挂 8 字节小端 bit 长度。
    std::vector<uint8_t> padded(input.begin(), input.end());
    const uint64_t bitLength = static_cast<uint64_t>(input.size()) * 8;
    padded.push_back(0x80);
    while (padded.size() % 64 != 56) padded.push_back(0x00);
    for (int i = 0; i < 8; ++i) padded.push_back(static_cast<uint8_t>((bitLength >> (8 * i)) & 0xFF));

    for (size_t offset = 0; offset + 64 <= padded.size(); offset += 64) {
        const uint8_t* block = padded.data() + offset;
        uint32_t words[16];
        for (int i = 0; i < 16; ++i) words[i] = loadLe32(block + i * 4);

        uint32_t a = state[0], b = state[1], c = state[2], d = state[3];
        for (int i = 0; i < 64; ++i) {
            // 四轮的非线性函数与消息字下标（RFC 1321 §3.4）。
            uint32_t f;
            int g;
            if (i < 16) {
                f = (b & c) | (~b & d);
                g = i;
            } else if (i < 32) {
                f = (d & b) | (~d & c);
                g = (5 * i + 1) % 16;
            } else if (i < 48) {
                f = b ^ c ^ d;
                g = (3 * i + 5) % 16;
            } else {
                f = c ^ (b | ~d);
                g = (7 * i) % 16;
            }
            const uint32_t next = f + a + kMd5K[i] + words[g];
            a = d;
            d = c;
            c = b;
            b = b + rotateLeft(next, kMd5Shifts[i]);
        }
        state[0] += a;
        state[1] += b;
        state[2] += c;
        state[3] += d;
    }

    std::vector<uint8_t> out(16);
    for (int i = 0; i < 4; ++i) storeLe32(out.data() + i * 4, state[i]);
    return out;
}

int64_t Md5Hash::hash(const std::string& key) const {
    // Java：`for (int i = 0; i < 4; i++) { h <<= 8; h |= ((int) digest[i]) & 0xFF; }`
    // —— 那个 `& 0xFF` 是必须的，因为 Java 的 byte 有符号；这里 uint8_t 升位即同值。
    const std::vector<uint8_t> digest = md5Digest(key);
    int64_t value = 0;
    for (int i = 0; i < 4; ++i) value = (value << 8) | digest[static_cast<size_t>(i)];
    return value;
}

ConsistentHashRouter::ConsistentHashRouter(
    const std::vector<std::shared_ptr<Node>>& pNodes, int32_t vNodeCount)
    : ConsistentHashRouter(pNodes, vNodeCount, nullptr) {}

ConsistentHashRouter::ConsistentHashRouter(
    const std::vector<std::shared_ptr<Node>>& pNodes, int32_t vNodeCount,
    const std::shared_ptr<const HashFunction>& hashFunction)
    : hashFunction_(hashFunction ? hashFunction : std::make_shared<const Md5Hash>()) {
    for (const std::shared_ptr<Node>& pNode : pNodes) addNode(pNode, vNodeCount);
}

void ConsistentHashRouter::addNode(const std::shared_ptr<Node>& pNode, int32_t vNodeCount) {
    if (vNodeCount < 0) {
        throw MQClientException("illegal virtual node counts :" + std::to_string(vNodeCount));
    }
    const int32_t existingReplicas = getExistingReplicas(pNode);
    for (int32_t i = 0; i < vNodeCount; ++i) {
        VirtualNode vNode{pNode, i + existingReplicas};
        // Java 是 TreeMap.put：同 hash 时后来者覆盖，位置不变。
        ring_[hashFunction_->hash(vNode.getKey())] = vNode;
    }
}

void ConsistentHashRouter::removeNode(const std::shared_ptr<Node>& pNode) {
    for (auto it = ring_.begin(); it != ring_.end();) {
        if (it->second.isVirtualNodeOf(pNode)) {
            it = ring_.erase(it);
        } else {
            ++it;
        }
    }
}

std::shared_ptr<Node> ConsistentHashRouter::routeNode(const std::string& objectKey) const {
    if (ring_.empty()) return nullptr;
    // Java：`ring.tailMap(hashVal)` —— **含端点**，故 lower_bound 正确（相等的 hash 归自己）。
    const int64_t hashVal = hashFunction_->hash(objectKey);
    auto it = ring_.lower_bound(hashVal);
    if (it == ring_.end()) it = ring_.begin();  // Java 的 ring.firstKey()：越过末尾回绕
    return it->second.physicalNode;
}

int32_t ConsistentHashRouter::getExistingReplicas(const std::shared_ptr<Node>& pNode) const {
    int32_t replicas = 0;
    for (const auto& entry : ring_) {
        if (entry.second.isVirtualNodeOf(pNode)) ++replicas;
    }
    return replicas;
}

std::vector<int64_t> ConsistentHashRouter::ringHashes() const {
    std::vector<int64_t> out;
    out.reserve(ring_.size());
    for (const auto& entry : ring_) out.push_back(entry.first);
    return out;
}

}  // namespace rocketmq
