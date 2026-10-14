// Runs without any network: the strategies are pure algorithms; live rebalance is covered by
// examples/rmq_live_*. The expected values below are a fixed table, cell by cell.
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "rocketmq/client/allocate_strategy.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/pull_consumer.h"

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

std::vector<MessageQueue> queuesOf(int32_t size) {
    std::vector<MessageQueue> out;
    for (int32_t i = 0; i < size; ++i) out.emplace_back("topic", "brokerName", i);
    return out;
}

std::vector<std::string> cidsOf(size_t size) {
    std::vector<std::string> out;
    for (size_t i = 0; i < size; ++i) out.push_back("CID_PREFIX" + std::to_string(i));
    return out;
}

std::vector<int32_t> idsOf(const std::vector<MessageQueue>& mqs) {
    std::vector<int32_t> out;
    for (const MessageQueue& mq : mqs) out.push_back(mq.queueId);
    return out;
}

std::string join(const std::vector<int32_t>& ids) {
    std::string s = "[";
    for (size_t i = 0; i < ids.size(); ++i) {
        if (i) s += ",";
        s += std::to_string(ids[i]);
    }
    return s + "]";
}

std::vector<int32_t> allocateIds(const AllocateMessageQueueStrategy& strategy,
                                 const std::string& currentCid,
                                 const std::vector<MessageQueue>& mqAll,
                                 const std::vector<std::string>& cidAll) {
    return idsOf(strategy.allocate("ConsumerGroupTest", currentCid, mqAll, cidAll));
}

struct Case {
    int32_t mqSize;
    size_t cidSize;
    std::vector<std::vector<int32_t>> expect;
};

std::vector<std::string> chCids(size_t size) {
    std::vector<std::string> out;
    for (size_t i = 0; i < size; ++i) out.push_back("CID-" + std::to_string(i));
    return out;
}

std::vector<int32_t> chAllocateIds(const AllocateMessageQueueStrategy& strategy,
                                   const std::string& currentCid,
                                   const std::vector<MessageQueue>& mqAll,
                                   const std::vector<std::string>& cidAll) {
    return idsOf(strategy.allocate("testConsumerGroup", currentCid, mqAll, cidAll));
}

std::vector<MessageQueue> roomQueuesOf(const std::string& brokerName, int32_t from, int32_t to) {
    std::vector<MessageQueue> out;
    for (int32_t i = from; i < to; ++i) out.emplace_back("topic", brokerName, i);
    return out;
}

struct DashRoom : public MachineRoomResolver {
    std::string brokerDeployIn(const MessageQueue& messageQueue) override {
        return messageQueue.brokerName.substr(0, messageQueue.brokerName.find('-'));
    }
    std::string consumerDeployIn(const std::string& clientId) override {
        return clientId.substr(0, clientId.find('-'));
    }
};

// the (brokerName, queueId) sequence of queues, used to assert the allocation ORDER (how NEARBY concatenates rooms)
std::string describe(const std::vector<MessageQueue>& mqs) {
    std::string s = "[";
    for (size_t i = 0; i < mqs.size(); ++i) {
        if (i) s += ",";
        s += mqs[i].brokerName + "#" + std::to_string(mqs[i].queueId);
    }
    return s + "]";
}

// average-allocation case table: queue count x consumer count cross coverage
const std::vector<Case>& averagelyCases() {
    static const std::vector<Case> kCases = {
        {10, 4, {{0, 1, 2}, {3, 4, 5}, {6, 7}, {8, 9}}},
        {8, 3, {{0, 1, 2}, {3, 4, 5}, {6, 7}}},
        {9, 3, {{0, 1, 2}, {3, 4, 5}, {6, 7, 8}}},
        {4, 4, {{0}, {1}, {2}, {3}}},
        {2, 4, {{0}, {1}, {}, {}}},
        {1, 3, {{0}, {}, {}}},
        {3, 1, {{0, 1, 2}}},
        {0, 2, {{}, {}}},
    };
    return kCases;
}

const std::vector<Case>& circleCases() {
    static const std::vector<Case> kCases = {
        {10, 4, {{0, 4, 8}, {1, 5, 9}, {2, 6}, {3, 7}}},
        {8, 3, {{0, 3, 6}, {1, 4, 7}, {2, 5}}},
        {9, 3, {{0, 3, 6}, {1, 4, 7}, {2, 5, 8}}},
        {4, 4, {{0}, {1}, {2}, {3}}},
        {2, 4, {{0}, {1}, {}, {}}},
        {1, 3, {{0}, {}, {}}},
        {3, 1, {{0, 1, 2}}},
        {0, 2, {{}, {}}},
    };
    return kCases;
}

void runTable(const AllocateMessageQueueStrategy& strategy, const std::vector<Case>& cases) {
    for (const Case& c : cases) {
        std::vector<MessageQueue> mqAll = queuesOf(c.mqSize);
        std::vector<std::string> cidAll = cidsOf(c.cidSize);
        CHECK_EQ(c.expect.size(), c.cidSize, strategy.getName() + " case cid count mismatch");
        for (size_t i = 0; i < c.expect.size() && i < cidAll.size(); ++i) {
            std::vector<int32_t> got = allocateIds(strategy, cidAll[i], mqAll, cidAll);
            CHECK(join(got) == join(c.expect[i]),
                  strategy.getName() + " mq=" + std::to_string(c.mqSize)
                      + " cid=" + std::to_string(c.cidSize) + " index=" + std::to_string(i)
                      + " got " + join(got));
        }
    }
}

void testAveragelyMatchesKnownSplit() {
    runTable(AllocateMessageQueueAveragely(), averagelyCases());
    std::vector<MessageQueue> mqAll = queuesOf(10);
    std::vector<std::string> cidAll = cidsOf(4);
    AllocateMessageQueueAveragely s;
    std::vector<size_t> sizes;
    for (const std::string& cid : cidAll) sizes.push_back(s.allocate("G", cid, mqAll, cidAll).size());
    CHECK(sizes == std::vector<size_t>({3, 3, 2, 2}), "unit-test baseline sizes {3,3,2,2}");
}

void testCircleMatchesKnownSplit() {
    runTable(AllocateMessageQueueAveragelyByCircle(), circleCases());
    std::vector<MessageQueue> mqAll = queuesOf(10);
    std::vector<std::string> cidAll = cidsOf(4);
    AllocateMessageQueueAveragelyByCircle s;
    CHECK_EQ(s.allocate("G", "CID_PREFIX", mqAll, cidAll).size(), static_cast<size_t>(0),
             "not in cidAll -> empty result");
}

void testGuardsReturnEmpty() {
    struct Guard {
        const char* name;
        std::string cid;
        int32_t mqSize;
        size_t cidSize;
    };
    const std::vector<Guard> guards = {
        {"empty currentCid", "", 4, 2},
        {"empty mqAll", "CID_PREFIX0", 0, 2},
        {"empty cidAll", "CID_PREFIX0", 4, 0},
        {"currentCid not in cidAll", "CID_NOT_IN_LIST", 4, 2},
    };
    const AllocateMessageQueueAveragely avg;
    const AllocateMessageQueueAveragelyByCircle circle;
    for (const Guard& g : guards) {
        std::vector<MessageQueue> mqAll = queuesOf(g.mqSize);
        std::vector<std::string> cidAll = cidsOf(g.cidSize);
        CHECK(avg.allocate("G", g.cid, mqAll, cidAll).empty(),
              std::string("AVG / ") + g.name + " returns an empty result");
        CHECK(circle.allocate("G", g.cid, mqAll, cidAll).empty(),
              std::string("AVG_BY_CIRCLE / ") + g.name + " returns an empty result");
    }
}

void testByConfigMatchesKnownSplit() {
    std::vector<MessageQueue> mqAll = queuesOf(4);
    std::vector<std::string> cidAll = cidsOf(2);
    AllocateMessageQueueByConfig s;
    s.setMessageQueueList(mqAll);
    for (const std::string& cid : cidAll) {
        CHECK(join(allocateIds(s, cid, mqAll, cidAll)) == "[0,1,2,3]", "CONFIG gives every consumer the full list");
    }
    CHECK(join(idsOf(s.getMessageQueueList())) == "[0,1,2,3]", "getMessageQueueList returns the configured value");
}

void testByConfigIgnoresGuards() {
    AllocateMessageQueueByConfig s(queuesOf(2));
    std::vector<std::string> emptyCid;
    CHECK(join(allocateIds(s, "", queuesOf(0), emptyCid)) == "[0,1]", "CONFIG skips the empty guards");
    CHECK(join(allocateIds(s, "anyCID", queuesOf(5), emptyCid)) == "[0,1]", "CONFIG ignores the passed-in queues");
}

// unconfigured = empty list, and allocate returns a copy
void testByConfigDefaultsEmptyAndReturnsCopy() {
    AllocateMessageQueueByConfig s;
    CHECK(s.getMessageQueueList().empty(), "default unconfigured = empty");
    std::vector<MessageQueue> before = s.allocate("G", "CID0", {}, {});
    s.setMessageQueueList(queuesOf(3));
    CHECK(before.empty(), "allocate must return a copy");
    CHECK(join(idsOf(s.allocate("G", "CID0", {}, {}))) == "[0,1,2]", "a changed config applies to new calls");
}

void testStrategyNamesMatchProtocol() {
    std::vector<std::shared_ptr<AllocateMessageQueueStrategy>> strategies = {
        std::make_shared<AllocateMessageQueueAveragely>(),
        std::make_shared<AllocateMessageQueueAveragelyByCircle>(),
        std::make_shared<AllocateMessageQueueByConfig>(),
        std::make_shared<AllocateMessageQueueConsistentHash>(),
        std::make_shared<AllocateMessageQueueByMachineRoom>(),
        std::make_shared<AllocateMachineRoomNearby>(std::make_shared<AllocateMessageQueueConsistentHash>(),
                                                   std::make_shared<DashRoom>()),
    };
    std::vector<std::string> names;
    for (const auto& s : strategies) names.push_back(s->getName());
    CHECK(names == std::vector<std::string>({"AVG", "AVG_BY_CIRCLE", "CONFIG", "CONSISTENT_HASH",
                                             "MACHINE_ROOM", "MACHINE_ROOM_NEARBY-CONSISTENT_HASH"}),
          "getName follows the naming convention");
}

// full coverage + no overlap: for any (queue count, consumer count) the union over all consumers is exactly mqAll
void testPartitionCoversEveryQueueOnce() {
    const AllocateMessageQueueAveragely avg;
    const AllocateMessageQueueAveragelyByCircle circle;
    const AllocateMessageQueueStrategy* strategies[2] = {&avg, &circle};
    for (int32_t mqSize = 0; mqSize <= 7; ++mqSize) {
        for (size_t cidSize = 1; cidSize <= 5; ++cidSize) {
            std::vector<MessageQueue> mqAll = queuesOf(mqSize);
            std::vector<std::string> cidAll = cidsOf(cidSize);
            for (const AllocateMessageQueueStrategy* s : strategies) {
                std::vector<int32_t> all;
                for (const std::string& cid : cidAll) {
                    std::vector<int32_t> got = allocateIds(*s, cid, mqAll, cidAll);
                    all.insert(all.end(), got.begin(), got.end());
                }
                CHECK_EQ(all.size(), static_cast<size_t>(mqSize),
                         s->getName() + " total allocation does not equal the queue count");
                std::set<int32_t> distinct(all.begin(), all.end());
                CHECK_EQ(distinct.size(), static_cast<size_t>(mqSize),
                         s->getName() + " allocation overlaps");
            }
        }
    }
}

void testCrossCheckAgainstReferenceFormula() {
    // reference formula (an independent start/end two-branch plus slice implementation, used for cross-checking)
    auto formulaAveragely = [](const std::vector<int32_t>& mq, const std::vector<std::string>& cid,
                              const std::string& current) {
        std::vector<int32_t> out;
        if (mq.empty() || cid.empty()) return out;
        int32_t index = -1;
        for (size_t i = 0; i < cid.size(); ++i)
            if (cid[i] == current) index = static_cast<int32_t>(i);
        if (index < 0 || current.empty()) return out;
        int32_t mod = static_cast<int32_t>(mq.size()) % static_cast<int32_t>(cid.size());
        int32_t avg = static_cast<int32_t>(mq.size()) / static_cast<int32_t>(cid.size());
        if (avg == 0) {
            if (index < static_cast<int32_t>(mq.size())) out.push_back(mq[static_cast<size_t>(index)]);
            return out;
        }
        int32_t start;
        int32_t end;
        if (mod > 0 && index < mod) {
            start = index * (avg + 1);
            end = start + avg + 1;
        } else {
            start = mod * (avg + 1) + (index - mod) * avg;
            end = start + avg;
        }
        end = std::min(end, static_cast<int32_t>(mq.size()));
        for (int32_t i = start; i < end; ++i) out.push_back(mq[static_cast<size_t>(i)]);
        return out;
    };
    auto formulaCircle = [](const std::vector<int32_t>& mq, const std::vector<std::string>& cid,
                           const std::string& current) {
        std::vector<int32_t> out;
        if (mq.empty() || cid.empty()) return out;
        int32_t index = -1;
        for (size_t i = 0; i < cid.size(); ++i)
            if (cid[i] == current) index = static_cast<int32_t>(i);
        if (index < 0 || current.empty()) return out;
        for (int32_t i = index; i < static_cast<int32_t>(mq.size());
             i += static_cast<int32_t>(cid.size()))
            out.push_back(mq[static_cast<size_t>(i)]);
        return out;
    };

    AllocateMessageQueueAveragely avg;
    AllocateMessageQueueAveragelyByCircle circle;
    for (int32_t mqSize = 0; mqSize <= 12; ++mqSize) {
        for (size_t cidSize = 0; cidSize <= 5; ++cidSize) {
            std::vector<MessageQueue> mqAll = queuesOf(mqSize);
            std::vector<std::string> cidAll = cidsOf(cidSize);
            std::vector<int32_t> ids;
            for (int32_t i = 0; i < mqSize; ++i) ids.push_back(i);
            std::vector<std::string> probes = cidAll;
            probes.push_back("CID_NOT_IN_LIST");
            probes.push_back("");
            for (const std::string& cid : probes) {
                std::string avgExpect = join(formulaAveragely(ids, cidAll, cid));
                std::string circleExpect = join(formulaCircle(ids, cidAll, cid));
                CHECK(join(allocateIds(avg, cid, mqAll, cidAll)) == avgExpect,
                      "AVG disagrees with the formula " + std::to_string(mqSize) + "x"
                          + std::to_string(cidSize) + " cid=" + cid);
                CHECK(join(allocateIds(circle, cid, mqAll, cidAll)) == circleExpect,
                      "AVG_BY_CIRCLE disagrees with the formula " + std::to_string(mqSize) + "x"
                          + std::to_string(cidSize) + " cid=" + cid);
            }
        }
    }
}

// real queues (one topic spread over several brokers) are also split by index, independent of brokerName / queueId
void testRealQueuesFromMultipleBrokers() {
    const std::vector<std::pair<std::string, int32_t>> layout = {
        {"broker-a", 0}, {"broker-a", 1}, {"broker-b", 0}, {"broker-b", 1}, {"broker-c", 0}};
    std::vector<MessageQueue> mqAll;
    for (const auto& kv : layout) mqAll.emplace_back("TopicTest", kv.first, kv.second);
    std::vector<std::string> cidAll = cidsOf(2);
    AllocateMessageQueueAveragely s;
    std::vector<MessageQueue> first = s.allocate("G", cidAll[0], mqAll, cidAll);
    std::vector<MessageQueue> second = s.allocate("G", cidAll[1], mqAll, cidAll);
    CHECK_EQ(first.size(), static_cast<size_t>(3), "AVG first consumer gets 3");
    CHECK_EQ(second.size(), static_cast<size_t>(2), "AVG second consumer gets 2");
    CHECK(first[2].brokerName == "broker-b" && first[2].queueId == 0, "the boundary queue belongs to the first consumer");
    CHECK(second[0].brokerName == "broker-b" && second[0].queueId == 1, "the second consumer continues from the boundary");
}

// ============================================================ CONSISTENT_HASH

void runConsistentHashTable(const AllocateMessageQueueStrategy& strategy,
                            const std::vector<Case>& cases) {
    for (const Case& c : cases) {
        const std::vector<MessageQueue> mqAll = queuesOf(c.mqSize);  // ("topic","brokerName",i)
        const std::vector<std::string> cidAll = chCids(c.cidSize);
        CHECK_EQ(c.expect.size(), c.cidSize, strategy.getName() + " case cid count mismatch");
        for (size_t i = 0; i < c.expect.size() && i < cidAll.size(); ++i) {
            const std::vector<int32_t> got = chAllocateIds(strategy, cidAll[i], mqAll, cidAll);
            CHECK(join(got) == join(c.expect[i]),
                  strategy.getName() + " mq=" + std::to_string(c.mqSize)
                      + " cid=" + std::to_string(c.cidSize) + " index=" + std::to_string(i)
                      + " got " + join(got));
        }
    }
}

const std::vector<Case>& consistentHashCases() {
    static const std::vector<Case> kCases = {
        {6, 2, {{2}, {0, 1, 3, 4, 5}}},
        {6, 3, {{2}, {1, 5}, {0, 3, 4}}},
        {10, 4, {{2}, {}, {0, 3, 4, 8}, {1, 5, 6, 7, 9}}},
        {20, 10,
         {{2, 14, 15},
          {17},
          {8, 11},
          {},
          {},
          {1, 5, 9, 10},
          {6, 7, 12},
          {13, 18},
          {16},
          {0, 3, 4, 19}}},
    };
    return kCases;
}

const std::vector<Case>& consistentHashDefaultVcCases() {
    static const std::vector<Case> kCases = {
        {4, 2, {{0, 2}, {1, 3}}},
        {8, 3, {{0, 2, 4}, {3, 5, 6, 7}, {1}}},
    };
    return kCases;
}

void testConsistentHashRingTableLayout() {
    runConsistentHashTable(AllocateMessageQueueConsistentHash(3), consistentHashCases());
    runConsistentHashTable(AllocateMessageQueueConsistentHash(), consistentHashDefaultVcCases());
    CHECK_EQ(AllocateMessageQueueConsistentHash(3).getVirtualNodeCnt(), 3, "vc=3 readable back");
    CHECK_EQ(AllocateMessageQueueConsistentHash().getVirtualNodeCnt(), 10,
             "default vc=10 (no-arg constructor)");
}

void testConsistentHashCoversEveryQueueOnce() {
    const AllocateMessageQueueConsistentHash strategy(3);
    for (int32_t mqSize = 1; mqSize <= 11; ++mqSize) {
        for (size_t cidSize = 1; cidSize <= 7; ++cidSize) {
            const std::vector<MessageQueue> mqAll = queuesOf(mqSize);
            const std::vector<std::string> cidAll = chCids(cidSize);
            std::vector<int32_t> flat;
            for (const std::string& cid : cidAll) {
                std::vector<int32_t> got = chAllocateIds(strategy, cid, mqAll, cidAll);
                flat.insert(flat.end(), got.begin(), got.end());
            }
            std::sort(flat.begin(), flat.end());
            std::vector<int32_t> expected;
            for (int32_t i = 0; i < mqSize; ++i) expected.push_back(i);
            CHECK(join(flat) == join(expected),
                  "CONSISTENT_HASH " + std::to_string(mqSize) + "x" + std::to_string(cidSize));
        }
    }
}

// the whole point of consistent hashing: a membership change moves only the affected arc, other consumers keep their queues
void testConsistentHashIsStableWhenMembershipChanges() {
    const AllocateMessageQueueConsistentHash strategy(3);
    const std::vector<MessageQueue> mqAll = queuesOf(9);
    const std::vector<std::string> cidAll = chCids(4);
    // owner[qid] = the clientId that got this queue
    auto ownerOf = [&](const std::vector<std::string>& cids) {
        std::vector<std::string> owner(9);
        for (const std::string& cid : cids) {
            for (const MessageQueue& mq : strategy.allocate("g", cid, mqAll, cids)) {
                owner[static_cast<size_t>(mq.queueId)] = cid;
            }
        }
        return owner;
    };
    const std::vector<std::string> before = ownerOf(cidAll);

    // Drop CID-0: its queues are taken over by others (that part must happen),
    // but queues that already belonged to another consumer must stay with that same consumer.
    const std::vector<std::string> remaining(cidAll.begin() + 1, cidAll.end());
    const std::vector<std::string> afterRemove = ownerOf(remaining);
    for (size_t qid = 0; qid < before.size(); ++qid) {
        if (before[qid] != "CID-0") {
            CHECK_EQ(afterRemove[qid], before[qid], "removing a consumer: qid=" + std::to_string(qid) + " must not change owner");
        }
    }

    // Add one new consumer: same rule, only the queues handed to CID-NEW are new.
    std::vector<std::string> joined = remaining;
    joined.push_back("CID-NEW");
    const std::vector<std::string> afterAdd = ownerOf(joined);
    for (size_t qid = 0; qid < before.size(); ++qid) {
        if (afterAdd[qid] != "CID-NEW") {
            CHECK_EQ(afterAdd[qid], before[qid], "adding a consumer: qid=" + std::to_string(qid) + " must not change owner");
        }
    }
}

void testConsistentHashRejectsNegativeVirtualNodeCnt() {
    std::string text;
    bool threw = false;
    try {
        const AllocateMessageQueueConsistentHash bad(-1);
        (void)bad;
    } catch (const MQClientException& e) {
        threw = true;
        text = e.what();
    }
    CHECK(threw, "must reject a negative virtualNodeCnt");
    CHECK(text.find("illegal virtualNodeCnt :-1") != std::string::npos,
          "message follows the constructor contract:" + text);
    const AllocateMessageQueueConsistentHash zero(0);
    CHECK(chAllocateIds(zero, "CID-0", queuesOf(4), chCids(2)).empty(), "vc=0 => nobody gets any queue");
}

struct FirstCharHash : public HashFunction {
    mutable int calls = 0;
    int64_t hash(const std::string& key) const override {
        ++calls;
        return key.empty() ? 0 : static_cast<int64_t>(static_cast<unsigned char>(key[0]));
    }
};

void testConsistentHashUsesInjectedHashFunction() {
    const std::shared_ptr<FirstCharHash> injected = std::make_shared<FirstCharHash>();
    const AllocateMessageQueueConsistentHash strategy(2, injected);
    const std::vector<MessageQueue> mqAll = queuesOf(4);
    const std::vector<std::string> cidAll = chCids(2);
    CHECK(chAllocateIds(strategy, "CID-0", mqAll, cidAll).empty(), "production-shaped ground truth: CID-0 gets no queue");
    CHECK(join(chAllocateIds(strategy, "CID-1", mqAll, cidAll)) == "[0,1,2,3]",
          "production-shaped ground truth: only CID-1 remains on the ring");
    CHECK(injected->calls > 0, "the custom hash function was not called");
}

void testConsistentHashGuardsReturnEmpty() {
    const AllocateMessageQueueConsistentHash strategy;
    const std::vector<MessageQueue> mqAll = queuesOf(4);
    const std::vector<std::string> cidAll = chCids(2);
    const std::vector<std::string> noCid;
    CHECK(chAllocateIds(strategy, "CID-NOT-HERE", mqAll, cidAll).empty(), "cid not in cidAll");
    CHECK(chAllocateIds(strategy, "", mqAll, cidAll).empty(), "empty currentCid");
    CHECK(chAllocateIds(strategy, "CID-0", {}, cidAll).empty(), "empty mqAll");
    CHECK(chAllocateIds(strategy, "CID-0", mqAll, noCid).empty(), "empty cidAll");
}

// =============================================================== MACHINE_ROOM

void testByMachineRoomMatchesKnownSplit() {
    std::vector<MessageQueue> mqAll;
    for (int32_t i = 0; i < 10; ++i) {
        mqAll.emplace_back("topic", i < 5 ? "room1@broker-a" : "room2@broker-b", i);
    }
    const std::vector<std::string> cidAll = cidsOf(2);
    const AllocateMessageQueueByMachineRoom strategy({"room1"});
    CHECK(join(allocateIds(strategy, cidAll[0], mqAll, cidAll)) == "[0,1,4]", "room1 first consumer");
    CHECK(join(allocateIds(strategy, cidAll[1], mqAll, cidAll)) == "[2,3]", "room1 second consumer");
    CHECK_EQ(strategy.getConsumeridcs().size(), static_cast<size_t>(1), "the whitelist holds only room1");
}

void testByMachineRoomSplitsBrokerName() {
    const std::vector<std::string> cidAll = cidsOf(1);
    AllocateMessageQueueByMachineRoom strategy({"room1"});
    struct SplitCase {
        const char* broker;
        size_t expectedParts;
        bool allocated;
    };
    const std::vector<SplitCase> cases = {
        {"room1@broker-a", 2, true},
        {"room1@", 1, false},
        {"room1@b@", 2, true},  // still 2 parts after the trailing separator is dropped
        {"@room1", 2, false},   // 2 parts, but the room is an empty string and not in the whitelist
        {"room1@broker@a", 3, false},
        {"@", 0, false},
        {"broker-a", 1, false},
        {"", 1, false},
    };
    for (const SplitCase& c : cases) {
        CHECK_EQ(javaSplit(c.broker, '@').size(), c.expectedParts,
                 std::string("javaSplit(\"") + c.broker + "\") part count differs from expectation");
        const std::vector<MessageQueue> mqAll = roomQueuesOf(c.broker, 0, 1);
        const bool got = !allocateIds(strategy, cidAll[0], mqAll, cidAll).empty();
        CHECK_EQ(got, c.allocated, std::string("broker=\"") + c.broker + "\" included/excluded decision differs");
    }
    AllocateMessageQueueByMachineRoom unset;
    CHECK(unset.allocate("G", cidAll[0], roomQueuesOf("room1@b", 0, 1), cidAll).empty(),
          "no whitelist => nothing allocated (missing configuration is expected to fail upstream)");
    strategy.setConsumeridcs({"room2"});
    CHECK_EQ(strategy.getConsumeridcs().count("room2"), static_cast<size_t>(1), "getConsumeridcs reflects the new configuration");
    CHECK(allocateIds(strategy, cidAll[0], roomQueuesOf("room1@broker-a", 0, 1), cidAll).empty(),
          "room1 no longer participates after the whitelist changes");
}

void testByMachineRoomGuardsReturnEmpty() {
    const std::vector<MessageQueue> mqAll = roomQueuesOf("room1@broker-a", 0, 4);
    const AllocateMessageQueueByMachineRoom strategy({"room1"});
    const std::vector<std::string> two = cidsOf(2);
    const std::vector<std::string> none;
    CHECK(strategy.allocate("G", "", mqAll, two).empty(), "empty currentCid");
    CHECK(strategy.allocate("G", "CID_PREFIX0", mqAll, none).empty(), "empty cidAll");
    CHECK(strategy.allocate("G", "CID_NOT_IN_LIST", mqAll, two).empty(), "cid not in cidAll");
    CHECK(strategy.allocate("G", "CID_PREFIX0", {}, two).empty(), "empty mqAll");
}

// ========================================================= MACHINE_ROOM_NEARBY

std::vector<MessageQueue> nearbyMq(size_t idcSize, int32_t queueSize) {
    std::vector<MessageQueue> out;
    for (size_t i = 1; i <= idcSize; ++i) {
        for (int32_t q = 0; q < queueSize; ++q) {
            out.emplace_back("topic", "IDC" + std::to_string(i) + "-brokerName", q);
        }
    }
    return out;
}

std::vector<std::string> nearbyCids(size_t idcSize, size_t consumerSize) {
    std::vector<std::string> out;
    for (size_t i = 1; i <= idcSize; ++i) {
        for (size_t q = 0; q < consumerSize; ++q) {
            out.push_back("IDC" + std::to_string(i) + "-CID-" + std::to_string(q));
        }
    }
    return out;
}

void testNearbyAllocatesSameRoomOnlyAndCoversEverything() {
    const AllocateMachineRoomNearby strategy(
        std::make_shared<AllocateMessageQueueAveragely>(), std::make_shared<DashRoom>());
    DashRoom resolver;
    const size_t idcSize = 5;
    const int32_t queueSize = 20;
    for (const size_t consumerSize : {10UL, 20UL, 30UL, 1UL}) {
        const std::vector<MessageQueue> mqAll = nearbyMq(idcSize, queueSize);
        const std::vector<std::string> cidAll = nearbyCids(idcSize, consumerSize);
        std::vector<std::string> flat;
        bool sameRoom = true;
        for (const std::string& cid : cidAll) {
            const std::vector<MessageQueue> got = strategy.allocate("Test-C-G", cid, mqAll, cidAll);
            for (const MessageQueue& mq : got) {
                // same room => no leakage; even when consumers are fewer than queues the split stays inside the room
                if (resolver.brokerDeployIn(mq) != resolver.consumerDeployIn(cid)) sameRoom = false;
                flat.push_back(mq.brokerName + "#" + std::to_string(mq.queueId));
            }
        }
        CHECK(sameRoom, "5x20x" + std::to_string(consumerSize) + " someone got a queue from another room");
        CHECK_EQ(flat.size(), mqAll.size(),
                 "5x20x" + std::to_string(consumerSize) + " gaps or duplicates");
        std::sort(flat.begin(), flat.end());
        std::vector<std::string> expected;
        for (const MessageQueue& mq : mqAll) {
            expected.push_back(mq.brokerName + "#" + std::to_string(mq.queueId));
        }
        std::sort(expected.begin(), expected.end());
        CHECK(flat == expected, "5x20x" + std::to_string(consumerSize) + " union is not the full set");
    }
}

void testNearbySharesRoomsWithNoConsumer() {
    const AllocateMachineRoomNearby strategy(
        std::make_shared<AllocateMessageQueueAveragely>(), std::make_shared<DashRoom>());
    std::vector<MessageQueue> mqAll = roomQueuesOf("IDC2-brokerName", 0, 4);
    const std::vector<MessageQueue> idc1 = roomQueuesOf("IDC1-brokerName", 0, 2);
    mqAll.insert(mqAll.end(), idc1.begin(), idc1.end());
    const std::vector<std::string> cidAll = {"IDC1-CID-0", "IDC1-CID-1"};

    CHECK(join(idsOf(strategy.allocate("G", "IDC1-CID-0", mqAll, cidAll))) == "[0,0,1]",
          "IDC1's own 2 queues plus half of the 4 queues of the empty room IDC2, split by AVG");
    CHECK(describe(strategy.allocate("G", "IDC1-CID-0", mqAll, cidAll)) ==
              "[IDC1-brokerName#0,IDC2-brokerName#0,IDC2-brokerName#1]",
          "allocation order of the first consumer");
    CHECK(describe(strategy.allocate("G", "IDC1-CID-1", mqAll, cidAll)) ==
              "[IDC1-brokerName#1,IDC2-brokerName#2,IDC2-brokerName#3]",
          "allocation order of the second consumer");

    // 5 rooms, only the first 2 have consumers: every queue must be consumed and a populated room keeps its own queues.
    const std::vector<MessageQueue> manyMq = nearbyMq(5, 4);
    const std::vector<std::string> manyCids = nearbyCids(2, 3);
    DashRoom resolver;
    size_t claimed = 0;
    bool leaked = false;
    for (const std::string& cid : manyCids) {
        for (const MessageQueue& mq : strategy.allocate("Test-C-G", cid, manyMq, manyCids)) {
            const std::string room = resolver.brokerDeployIn(mq);
            if ((room == "IDC1" || room == "IDC2") && room != resolver.consumerDeployIn(cid)) {
                leaked = true;
            }
            ++claimed;
        }
    }
    CHECK(!leaked, "queues of a populated room leaked to another room");
    CHECK_EQ(claimed, manyMq.size(), "with empty rooms the allocation must stay exact and disjoint");
}

void testNearbyNameExposesInnerStrategy() {
    const std::shared_ptr<MachineRoomResolver> resolver = std::make_shared<DashRoom>();
    CHECK_EQ(AllocateMachineRoomNearby(std::make_shared<AllocateMessageQueueAveragely>(), resolver)
                 .getName(),
             std::string("MACHINE_ROOM_NEARBY-AVG"), "inner AVG");
    CHECK_EQ(AllocateMachineRoomNearby(std::make_shared<AllocateMessageQueueAveragelyByCircle>(),
                                       resolver)
                 .getName(),
             std::string("MACHINE_ROOM_NEARBY-AVG_BY_CIRCLE"), "inner AVG_BY_CIRCLE");
    const auto byConfig = std::make_shared<AllocateMessageQueueByConfig>(queuesOf(1));
    CHECK_EQ(AllocateMachineRoomNearby(byConfig, resolver).getName(),
             std::string("MACHINE_ROOM_NEARBY-CONFIG"), "inner CONFIG is composed the same way");
}

struct BlankBrokerRoom : public MachineRoomResolver {
    std::string brokerDeployIn(const MessageQueue&) override { return std::string(); }
    std::string consumerDeployIn(const std::string&) override { return "IDC1"; }
};
struct BlankConsumerRoom : public MachineRoomResolver {
    std::string brokerDeployIn(const MessageQueue&) override { return "IDC1"; }
    std::string consumerDeployIn(const std::string&) override { return std::string(); }
};

std::string catchAllocate(const AllocateMessageQueueStrategy& strategy,
                          const std::vector<MessageQueue>& mqAll,
                          const std::vector<std::string>& cidAll, bool* threw) {
    try {
        strategy.allocate("G", "CID-0", mqAll, cidAll);
        *threw = false;
    } catch (const std::exception& e) {
        *threw = true;
        return e.what();
    }
    return std::string();
}

void testNearbyThrowsWhenRoomIsUnknown() {
    const std::vector<MessageQueue> mqAll = queuesOf(2);
    const std::vector<std::string> cidAll = chCids(1);
    bool threw = false;
    std::string text = catchAllocate(
        AllocateMachineRoomNearby(std::make_shared<AllocateMessageQueueAveragely>(),
                                  std::make_shared<BlankBrokerRoom>()),
        mqAll, cidAll, &threw);
    CHECK(threw, "an empty broker room must throw");
    CHECK(text.find("Machine room is null for mq MessageQueue [topic=topic, "
                    "brokerName=brokerName, queueId=0]") != std::string::npos,
          "message follows the contract (including MessageQueue#toString):" + text);

    text = catchAllocate(
        AllocateMachineRoomNearby(std::make_shared<AllocateMessageQueueAveragely>(),
                                  std::make_shared<BlankConsumerRoom>()),
        mqAll, cidAll, &threw);
    CHECK(threw, "an empty consumer room must throw");
    CHECK(text.find("Machine room is null for consumer id CID-0") != std::string::npos,
          "message follows the contract:" + text);

    // the guards still win first: an empty cidAll returns empty without touching the resolver
    const AllocateMachineRoomNearby nearby(
        std::make_shared<AllocateMessageQueueAveragely>(), std::make_shared<BlankBrokerRoom>());
    const std::vector<std::string> none;
    CHECK(nearby.allocate("G", "CID-0", mqAll, none).empty(), "guards take precedence over resolver errors");
}

void testNearbyRejectsNullArguments() {
    const std::shared_ptr<MachineRoomResolver> resolver = std::make_shared<DashRoom>();
    bool threw = false;
    std::string text;
    try {
        const AllocateMachineRoomNearby bad(nullptr, resolver);
        (void)bad;
    } catch (const std::exception& e) {
        threw = true;
        text = e.what();
    }
    CHECK(threw, "a null inner strategy must throw");
    CHECK(text.find("allocateMessageQueueStrategy is null") != std::string::npos, "message:" + text);

    threw = false;
    text.clear();
    try {
        const AllocateMachineRoomNearby bad(std::make_shared<AllocateMessageQueueAveragely>(),
                                            nullptr);
        (void)bad;
    } catch (const std::exception& e) {
        threw = true;
        text = e.what();
    }
    CHECK(threw, "a null resolver must throw");
    CHECK(text.find("machineRoomResolver is null") != std::string::npos, "message:" + text);
}

// pin the early-return branch of javaSplit (no trailing separator is dropped when the delimiter never hits)
void testBrokerNameSplitMatchesSpec() {
    CHECK_EQ(javaSplit("", '@').size(), static_cast<size_t>(1), "\"\" -> 1 part");
    CHECK_EQ(javaSplit("@", '@').size(), static_cast<size_t>(0), "\"@\" -> 0 parts");
    CHECK_EQ(javaSplit("room1@", '@').size(), static_cast<size_t>(1), "\"room1@\" -> 1 part");
    CHECK_EQ(javaSplit("room1@b@", '@').size(), static_cast<size_t>(2), "\"room1@b@\" -> 2 parts");
    CHECK_EQ(javaSplit("@room1", '@').size(), static_cast<size_t>(2), "\"@room1\" -> 2 parts");
    CHECK_EQ(javaSplit("room1@@b", '@').size(), static_cast<size_t>(3), "empty middle parts are kept");
    const std::vector<std::string> plain = javaSplit("broker-a", '@');
    CHECK_EQ(plain.size(), static_cast<size_t>(1), "the delimiter never hits => the whole string");
    CHECK_EQ(plain[0], std::string("broker-a"), "the whole string is returned as-is");
}

}  // namespace

void testConsumersExposeStrategy() {
    DefaultMQPushConsumer push("G_test");
    CHECK(push.allocateMessageQueueStrategy() != nullptr, "the push consumer has a default strategy");
    CHECK_EQ(push.allocateMessageQueueStrategy()->getName(), std::string("AVG"),
             "push default strategy = AVG");
    push.setAllocateMessageQueueStrategy(std::make_shared<AllocateMessageQueueAveragelyByCircle>());
    CHECK_EQ(push.allocateMessageQueueStrategy()->getName(), std::string("AVG_BY_CIRCLE"),
             "push can be swapped to AVG_BY_CIRCLE");
    // A missing name server or subscription is blocked earlier by the group-name check, so this
    // step only verifies that the setter accepts null (start() guards are covered by live case S7a).
    push.setAllocateMessageQueueStrategy(nullptr);
    CHECK(push.allocateMessageQueueStrategy() == nullptr, "the push setter accepts null");

    DefaultLitePullConsumer lite("G_test");
    CHECK_EQ(lite.allocateMessageQueueStrategy()->getName(), std::string("AVG"),
             "lite default strategy = AVG");
    auto byConfig = std::make_shared<AllocateMessageQueueByConfig>();
    byConfig->setMessageQueueList(queuesOf(2));
    lite.setAllocateMessageQueueStrategy(byConfig);
    CHECK_EQ(lite.allocateMessageQueueStrategy()->getName(), std::string("CONFIG"),
             "lite can be swapped to CONFIG");
    CHECK(join(allocateIds(*lite.allocateMessageQueueStrategy(), "whoever", queuesOf(9),
                           cidsOf(3))) == "[0,1]",
          "CONFIG ignores mqAll/cidAll and returns the configured queues");
    // lite start() hits the "not subscribed" error first, so only the guard order after the group-name
    // check is tested here: a null strategy must throw before ANY network work (after name/group checks).
    DefaultLitePullConsumer nullStrat("G_test");
    nullStrat.setNamesrvAddr("127.0.0.1:1");
    nullStrat.subscribe("TopicTest", "*");
    nullStrat.setAllocateMessageQueueStrategy(nullptr);
    bool threw = false;
    std::string msg;
    try {
        nullStrat.start();
    } catch (const std::exception& e) {
        threw = true;
        msg = e.what();
    }
    CHECK(threw, "lite start() throws when the strategy is null");
    CHECK(msg.find("allocateMessageQueueStrategy is null") != std::string::npos,
          "the null-strategy lite message matches the contract:" + msg);

    DefaultMQPullConsumer pull("G_test");
    CHECK_EQ(pull.allocateMessageQueueStrategy()->getName(), std::string("AVG"),
             "pull default strategy = AVG");
    pull.setAllocateMessageQueueStrategy(std::make_shared<AllocateMessageQueueByConfig>());
    CHECK_EQ(pull.allocateMessageQueueStrategy()->getName(), std::string("CONFIG"),
             "pull can be swapped to CONFIG");
    pull.setAllocateMessageQueueStrategy(nullptr);
    CHECK(pull.allocateMessageQueueStrategy() == nullptr, "the pull setter accepts null");
    DefaultMQPullConsumer pullNullStrat("G_test");
    pullNullStrat.setNamesrvAddr("127.0.0.1:1");
    pullNullStrat.setAllocateMessageQueueStrategy(nullptr);
    bool pullThrew = false;
    std::string pullMsg;
    try {
        pullNullStrat.start();
    } catch (const std::exception& e) {
        pullThrew = true;
        pullMsg = e.what();
    }
    CHECK(pullThrew, "pull start() throws when the strategy is null");
    CHECK(pullMsg.find("allocateMessageQueueStrategy is null") != std::string::npos,
          "the null-strategy pull message matches the contract:" + pullMsg);
}

// the three newer strategies plug into push / lite / pull consumers and read back (same surface as the old ones)
void testNewStrategiesPlugIntoConsumers() {
    const std::vector<std::string> one = {"CID-0"};

    DefaultMQPushConsumer push("G_test");
    push.setAllocateMessageQueueStrategy(std::make_shared<AllocateMessageQueueConsistentHash>(3));
    CHECK_EQ(push.allocateMessageQueueStrategy()->getName(), std::string("CONSISTENT_HASH"),
             "push can be swapped to CONSISTENT_HASH");
    // single consumer + hash ring: all 4 queues land on it
    CHECK(join(idsOf(push.allocateMessageQueueStrategy()->allocate("g", "CID-0", queuesOf(4), one))) ==
              "[0,1,2,3]",
          "with one consumer the ring holds only me => take everything");

    DefaultLitePullConsumer lite("G_lite");
    lite.setAllocateMessageQueueStrategy(
        std::make_shared<AllocateMessageQueueByMachineRoom>(std::set<std::string>{"room1"}));
    CHECK_EQ(lite.allocateMessageQueueStrategy()->getName(), std::string("MACHINE_ROOM"),
             "lite can be swapped to MACHINE_ROOM");
    CHECK(join(idsOf(lite.allocateMessageQueueStrategy()->allocate(
              "g", "CID-0", roomQueuesOf("room1@broker-a", 0, 3), one))) == "[0,1,2]",
          "the 3 whitelisted room queues all go to the single consumer");

    lite.setAllocateMessageQueueStrategy(std::make_shared<AllocateMachineRoomNearby>(
        std::make_shared<AllocateMessageQueueAveragely>(), std::make_shared<DashRoom>()));
    CHECK_EQ(lite.allocateMessageQueueStrategy()->getName(), std::string("MACHINE_ROOM_NEARBY-AVG"),
             "lite can be swapped to MACHINE_ROOM_NEARBY-AVG");

    DefaultMQPullConsumer pull("G_test");
    pull.setAllocateMessageQueueStrategy(std::make_shared<AllocateMessageQueueConsistentHash>());
    CHECK_EQ(pull.allocateMessageQueueStrategy()->getName(), std::string("CONSISTENT_HASH"),
             "pull can be swapped to CONSISTENT_HASH");
}

int main() {
    testAveragelyMatchesKnownSplit();
    testCircleMatchesKnownSplit();
    testGuardsReturnEmpty();
    testByConfigMatchesKnownSplit();
    testByConfigIgnoresGuards();
    testByConfigDefaultsEmptyAndReturnsCopy();
    testStrategyNamesMatchProtocol();
    testPartitionCoversEveryQueueOnce();
    testCrossCheckAgainstReferenceFormula();
    testRealQueuesFromMultipleBrokers();
    testConsistentHashRingTableLayout();
    testConsistentHashCoversEveryQueueOnce();
    testConsistentHashIsStableWhenMembershipChanges();
    testConsistentHashRejectsNegativeVirtualNodeCnt();
    testConsistentHashUsesInjectedHashFunction();
    testConsistentHashGuardsReturnEmpty();
    testByMachineRoomMatchesKnownSplit();
    testByMachineRoomSplitsBrokerName();
    testByMachineRoomGuardsReturnEmpty();
    testBrokerNameSplitMatchesSpec();
    testNearbyAllocatesSameRoomOnlyAndCoversEverything();
    testNearbySharesRoomsWithNoConsumer();
    testNearbyNameExposesInnerStrategy();
    testNearbyThrowsWhenRoomIsUnknown();
    testNearbyRejectsNullArguments();
    testConsumersExposeStrategy();
    testNewStrategiesPlugIntoConsumers();
    std::cout << "allocate_strategy: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
