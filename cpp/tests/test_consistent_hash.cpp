// 一致性哈希环单测：MD5 官方向量 + Java 落点口径（前 4 字节大端 / 含端点 tailMap / 回绕）。
//
// 期望值与 rust/src/common/consistent_hash.rs 的测试**逐值一致**（四语言对拍）。
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/common/consistent_hash.h"

using namespace rocketmq;

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg)                                                       \
    do {                                                                       \
        if (cond) {                                                            \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << "\n";                           \
        }                                                                      \
    } while (0)

#define CHECK_EQ(a, b, msg)                                                    \
    do {                                                                       \
        if ((a) == (b)) {                                                      \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << " (actual=" << (a)             \
                      << ", expect=" << (b) << ")\n";                          \
        }                                                                      \
    } while (0)

namespace {

std::string md5Hex(const std::string& input) {
    static const char* kDigits = "0123456789abcdef";
    std::string out;
    for (uint8_t byte : md5Digest(input)) {
        out.push_back(kDigits[byte >> 4]);
        out.push_back(kDigits[byte & 0x0F]);
    }
    return out;
}

std::vector<std::shared_ptr<Node>> nodesOf(std::vector<std::string> keys) {
    std::vector<std::shared_ptr<Node>> out;
    for (const std::string& key : keys) out.push_back(std::make_shared<ClientNode>(key));
    return out;
}

std::string routedKey(const ConsistentHashRouter& router, const std::string& objectKey) {
    const std::shared_ptr<Node> node = router.routeNode(objectKey);
    return node == nullptr ? std::string("<null>") : node->getKey();
}

// RFC 1321 附录 A 的官方向量 + 跨块/填充边界的几种长度（参考值与 Rust 同源）
void testMd5MatchesReferenceVectors() {
    CHECK_EQ(md5Hex(""), std::string("d41d8cd98f00b204e9800998ecf8427e"), "MD5 空串");
    CHECK_EQ(md5Hex("a"), std::string("0cc175b9c0f1b6a831c399e269772661"), "MD5 \"a\"");
    CHECK_EQ(md5Hex("abc"), std::string("900150983cd24fb0d6963f7d28e17f72"), "MD5 \"abc\"");
    CHECK_EQ(md5Hex("message digest"), std::string("f96b697d7cb7938d525a2f31aaf161d0"),
             "MD5 \"message digest\"");
    CHECK_EQ(md5Hex("abcdefghijklmnopqrstuvwxyz"),
             std::string("c3fcd3d76192e4007dfb496cca67e13b"), "MD5 a..z");
    CHECK_EQ(md5Hex("The quick brown fox jumps over the lazy dog"),
             std::string("9e107d9d372bb6826bd81d3542a419d6"), "MD5 全句");
    struct Vector {
        size_t len;
        const char* hex;
    };
    const std::vector<Vector> longOnes = {
        {55, "04364420e25c512fd958a70738aa8f72"}, {56, "668a72d5ba17f08e62dabcafad6db14b"},
        {57, "693037871c4a9d3d8685018905cb530a"}, {64, "c1bb4f81d892b2d57947682aeb252456"},
        {65, "1bc932052302d074bdec39795fe00cf6"}, {200, "30a83621ce5422fbdfdd539777458c78"},
    };
    for (const Vector& v : longOnes) {
        CHECK_EQ(md5Hex(std::string(v.len, 'x')), std::string(v.hex),
                 "MD5 长度 " + std::to_string(v.len) + " 字节");
    }
}

// Java 只取摘要前 4 字节大端拼接，不是完整 128 bit
void testJavaMd5HashTakesOnlyFourBytes() {
    Md5Hash hash;
    CHECK_EQ(hash.hash(""), static_cast<int64_t>(0xD41D8CD9), "MD5Hash(\"\")");
    CHECK_EQ(hash.hash("abc"), static_cast<int64_t>(0x90015098), "MD5Hash(\"abc\")");
    CHECK(hash.hash("abc") >> 32 == 0, "结果必须落在 32 位内（Java 是 long）");
}

// 同一 key 恒定落点 + 越过环末尾回绕到 firstKey
void testRingRoutesToClockwiseNeighbourAndWraps() {
    const ConsistentHashRouter router(nodesOf({"n0", "n1"}), 5);
    const std::vector<std::string> probes = {
        "MessageQueue [topic=t, brokerName=b, queueId=0]", "k", "另一个"};
    for (const std::string& key : probes) {
        const std::string first = routedKey(router, key);
        for (int i = 0; i < 3; ++i) CHECK_EQ(routedKey(router, key), first, "落点不稳定: " + key);
    }
    std::vector<std::string> seen;
    for (int i = 0; i < 64; ++i) {
        const std::string routed = routedKey(router, "queue-" + std::to_string(i));
        if (std::find(seen.begin(), seen.end(), routed) == seen.end()) seen.push_back(routed);
    }
    CHECK_EQ(seen.size(), static_cast<size_t>(2), "两个物理节点都得被路由到");
}

void testEmptyRingRoutesToNothing() {
    const ConsistentHashRouter router({}, 3);
    CHECK(router.routeNode("anything") == nullptr, "空环 routeNode 返回 nullptr");
    CHECK_EQ(router.ringHashes().size(), static_cast<size_t>(0), "空环没有 hash");
}

// Java 构造器只是遍历 pNodes 调 addNode ⇒ 空集合时负数不会触发检查；检查在 addNode 里
void testNegativeVirtualNodeCountIsRejected() {
    bool threwOnEmpty = false;
    try {
        ConsistentHashRouter({}, -1);
    } catch (const std::exception&) {
        threwOnEmpty = true;
    }
    CHECK(!threwOnEmpty, "空节点集不触发检查（Java 同）");

    std::string text;
    bool threw = false;
    try {
        ConsistentHashRouter(nodesOf({"a"}), -1);
    } catch (const MQClientException& e) {
        threw = true;
        text = e.what();
    }
    CHECK(threw, "必须拒绝负数虚拟节点数");
    CHECK(text.find("illegal virtual node counts :-1") != std::string::npos,
          "文案对齐 Java addNode：" + text);
}

// `i + existingReplicas` 不是冗余：同一物理节点二次 addNode 必须往后编号而不是撞环
void testReAddingANodeKeepsVirtualNodesDistinct() {
    const std::shared_ptr<Node> dup = std::make_shared<ClientNode>("dup");
    ConsistentHashRouter router({}, 0);
    router.addNode(dup, 3);
    CHECK_EQ(router.getExistingReplicas(dup), 3, "首批 3 个虚拟节点");
    router.addNode(dup, 2);
    CHECK_EQ(router.getExistingReplicas(dup), 5, "追加后 5 个");
    CHECK_EQ(router.ringHashes().size(), static_cast<size_t>(5), "5 个虚拟节点占 5 个不同 hash");
}

void testRemoveNodeDropsOnlyThatPhysicalNode() {
    const std::shared_ptr<Node> a = std::make_shared<ClientNode>("a");
    const std::shared_ptr<Node> b = std::make_shared<ClientNode>("b");
    ConsistentHashRouter router(nodesOf({"a", "b"}), 4);
    router.removeNode(a);
    CHECK_EQ(router.getExistingReplicas(a), 0, "a 已摘干净");
    CHECK_EQ(router.getExistingReplicas(b), 4, "b 不受影响");
    CHECK_EQ(routedKey(router, "whatever"), std::string("b"), "剩下的队列都归 b");
}

// 注入的 HashFunction 必须真的参与每一次查找（Java 允许注入）
struct FirstByte : public HashFunction {
    mutable int calls = 0;
    int64_t hash(const std::string& key) const override {
        ++calls;
        return key.empty() ? 0 : static_cast<int64_t>(static_cast<uint8_t>(key[0]));
    }
};

void testCustomHashFunctionDrivesEveryLookup() {
    const std::shared_ptr<FirstByte> injected = std::make_shared<FirstByte>();
    const ConsistentHashRouter router(nodesOf({"aaa", "bbb"}), 1, injected);
    // "a"(97) / "b"(98) 各占一位；"c"(99) 越过末尾 ⇒ 回绕到最小 hash 的 aaa
    CHECK_EQ(routedKey(router, "a!"), std::string("aaa"), "a! → aaa");
    CHECK_EQ(routedKey(router, "b!"), std::string("bbb"), "b! → bbb");
    CHECK_EQ(routedKey(router, "c!"), std::string("aaa"), "c! 回绕 → aaa");
    CHECK(injected->calls > 0, "自定义哈希没被调用");
}

// 环上的 hash 升序 = Java TreeMap 的遍历序（用来确认 lower_bound/回绕口径一致）
void testRingHashesAreSorted() {
    const ConsistentHashRouter router(nodesOf({"CID-0", "CID-1", "CID-2"}), 3);
    const std::vector<int64_t> hashes = router.ringHashes();
    CHECK_EQ(hashes.size(), static_cast<size_t>(9), "3 节点 × 3 虚拟 = 9 个落点");
    bool sorted = true;
    for (size_t i = 1; i < hashes.size(); ++i) {
        if (hashes[i] < hashes[i - 1]) sorted = false;
    }
    CHECK(sorted, "环 hash 必须升序（Java TreeMap）");
}

}  // namespace

int main() {
    testMd5MatchesReferenceVectors();
    testJavaMd5HashTakesOnlyFourBytes();
    testRingRoutesToClockwiseNeighbourAndWraps();
    testEmptyRingRoutesToNothing();
    testNegativeVirtualNodeCountIsRejected();
    testReAddingANodeKeepsVirtualNodesDistinct();
    testRemoveNodeDropsOnlyThatPhysicalNode();
    testCustomHashFunctionDrivesEveryLookup();
    testRingHashesAreSorted();
    std::cout << "consistent_hash: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
