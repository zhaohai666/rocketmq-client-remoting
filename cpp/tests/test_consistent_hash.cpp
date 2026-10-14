// Expected values are pinned **value by value** against the reference hash vectors below and must not drift.
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

// Official vectors from RFC 1321 Appendix A plus lengths around the multi-block / padding boundaries
void testMd5MatchesReferenceVectors() {
    CHECK_EQ(md5Hex(""), std::string("d41d8cd98f00b204e9800998ecf8427e"), "MD5 empty string");
    CHECK_EQ(md5Hex("a"), std::string("0cc175b9c0f1b6a831c399e269772661"), "MD5 \"a\"");
    CHECK_EQ(md5Hex("abc"), std::string("900150983cd24fb0d6963f7d28e17f72"), "MD5 \"abc\"");
    CHECK_EQ(md5Hex("message digest"), std::string("f96b697d7cb7938d525a2f31aaf161d0"),
             "MD5 \"message digest\"");
    CHECK_EQ(md5Hex("abcdefghijklmnopqrstuvwxyz"),
             std::string("c3fcd3d76192e4007dfb496cca67e13b"), "MD5 a..z");
    CHECK_EQ(md5Hex("The quick brown fox jumps over the lazy dog"),
             std::string("9e107d9d372bb6826bd81d3542a419d6"), "MD5 full pangram");
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
                 "MD5 length " + std::to_string(v.len) + " bytes");
    }
}

void testMd5HashTakesOnlyFourBytes() {
    Md5Hash hash;
    CHECK_EQ(hash.hash(""), static_cast<int64_t>(0xD41D8CD9), "MD5Hash(\"\")");
    CHECK_EQ(hash.hash("abc"), static_cast<int64_t>(0x90015098), "MD5Hash(\"abc\")");
    CHECK(hash.hash("abc") >> 32 == 0, "result must stay within 32 bits (the reference implementation uses long)");
}

// A fixed key always lands on the same node; past the ring end routing wraps back to firstKey
void testRingRoutesToClockwiseNeighbourAndWraps() {
    const ConsistentHashRouter router(nodesOf({"n0", "n1"}), 5);
    const std::vector<std::string> probes = {
        "MessageQueue [topic=t, brokerName=b, queueId=0]", "k", "另一个"};
    for (const std::string& key : probes) {
        const std::string first = routedKey(router, key);
        for (int i = 0; i < 3; ++i) CHECK_EQ(routedKey(router, key), first, "unstable landing point for " + key);
    }
    std::vector<std::string> seen;
    for (int i = 0; i < 64; ++i) {
        const std::string routed = routedKey(router, "queue-" + std::to_string(i));
        if (std::find(seen.begin(), seen.end(), routed) == seen.end()) seen.push_back(routed);
    }
    CHECK_EQ(seen.size(), static_cast<size_t>(2), "both physical nodes must be routed to");
}

void testEmptyRingRoutesToNothing() {
    const ConsistentHashRouter router({}, 3);
    CHECK(router.routeNode("anything") == nullptr, "empty ring routeNode returns nullptr");
    CHECK_EQ(router.ringHashes().size(), static_cast<size_t>(0), "empty ring has no hashes");
}

void testNegativeVirtualNodeCountIsRejected() {
    bool threwOnEmpty = false;
    try {
        ConsistentHashRouter({}, -1);
    } catch (const std::exception&) {
        threwOnEmpty = true;
    }
    CHECK(!threwOnEmpty, "empty node set triggers no check (same as the reference implementation)");

    std::string text;
    bool threw = false;
    try {
        ConsistentHashRouter(nodesOf({"a"}), -1);
    } catch (const MQClientException& e) {
        threw = true;
        text = e.what();
    }
    CHECK(threw, "negative virtual node count must be rejected");
    CHECK(text.find("illegal virtual node counts :-1") != std::string::npos,
          "addNode illegal-argument message: " + text);
}

// `i + existingReplicas` is not redundant: adding the same physical node again must continue numbering instead of colliding on the ring
void testReAddingANodeKeepsVirtualNodesDistinct() {
    const std::shared_ptr<Node> dup = std::make_shared<ClientNode>("dup");
    ConsistentHashRouter router({}, 0);
    router.addNode(dup, 3);
    CHECK_EQ(router.getExistingReplicas(dup), 3, "first batch of 3 virtual nodes");
    router.addNode(dup, 2);
    CHECK_EQ(router.getExistingReplicas(dup), 5, "5 after appending");
    CHECK_EQ(router.ringHashes().size(), static_cast<size_t>(5), "5 virtual nodes occupy 5 distinct ring hashes");
}

void testRemoveNodeDropsOnlyThatPhysicalNode() {
    const std::shared_ptr<Node> a = std::make_shared<ClientNode>("a");
    const std::shared_ptr<Node> b = std::make_shared<ClientNode>("b");
    ConsistentHashRouter router(nodesOf({"a", "b"}), 4);
    router.removeNode(a);
    CHECK_EQ(router.getExistingReplicas(a), 0, "a fully removed");
    CHECK_EQ(router.getExistingReplicas(b), 4, "b unaffected");
    CHECK_EQ(routedKey(router, "whatever"), std::string("b"), "all remaining routes go to b");
}

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
    // "a"(97) / "b"(98) each hold one slot; "c"(99) passes the ring end => wraps to the smallest hash, aaa
    CHECK_EQ(routedKey(router, "a!"), std::string("aaa"), "a! -> aaa");
    CHECK_EQ(routedKey(router, "b!"), std::string("bbb"), "b! -> bbb");
    CHECK_EQ(routedKey(router, "c!"), std::string("aaa"), "c! wraps -> aaa");
    CHECK(injected->calls > 0, "custom hash function was never called");
}

void testRingHashesAreSorted() {
    const ConsistentHashRouter router(nodesOf({"CID-0", "CID-1", "CID-2"}), 3);
    const std::vector<int64_t> hashes = router.ringHashes();
    CHECK_EQ(hashes.size(), static_cast<size_t>(9), "3 nodes x 3 virtual = 9 ring entries");
    bool sorted = true;
    for (size_t i = 1; i < hashes.size(); ++i) {
        if (hashes[i] < hashes[i - 1]) sorted = false;
    }
    CHECK(sorted, "ring hashes must be ascending (the reference implementation uses a sorted map)");
}

}  // namespace

int main() {
    testMd5MatchesReferenceVectors();
    testMd5HashTakesOnlyFourBytes();
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
