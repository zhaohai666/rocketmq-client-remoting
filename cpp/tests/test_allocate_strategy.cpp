// 队列分配策略单测（对应 Java client/test/.../AllocateMessageQueue*Test）。
//
// 不起网络：策略是纯算法，真机 rebalance 另见 examples/rmq_live_*。
// 期望值与 rust/src/client/allocate_strategy.rs 的表**逐格一致**（四语言对拍）。
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

// Java 测试里的 createMessageQueueList(size)：new MessageQueue("topic", "brokerName", i)
std::vector<MessageQueue> queuesOf(int32_t size) {
    std::vector<MessageQueue> out;
    for (int32_t i = 0; i < size; ++i) out.emplace_back("topic", "brokerName", i);
    return out;
}

// Java 测试里的 createConsumerIdList(size)："CID_PREFIX" + i
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

// Java `AllocateMessageQueueConsitentHashTest` 的 createConsumerIdList：`CID-i`。
// 与 [`cidsOf`] 的 "CID_PREFIX i" **刻意不同名** —— clientId 参与哈希，改一个字符整张表就废了。
std::vector<std::string> chCids(size_t size) {
    std::vector<std::string> out;
    for (size_t i = 0; i < size; ++i) out.push_back("CID-" + std::to_string(i));
    return out;
}

// CONSISTENT_HASH 的 Java 单测用组名 "testConsumerGroup"；组名不参与哈希，
// 这里保留同名只为与 Java / Rust 的表可对照。
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

// Java 测试同款 resolver：broker `IDCx-brokerName` / 消费者 `IDCx-CID-i` 取 '-' 前段。
struct DashRoom : public MachineRoomResolver {
    std::string brokerDeployIn(const MessageQueue& messageQueue) override {
        return messageQueue.brokerName.substr(0, messageQueue.brokerName.find('-'));
    }
    std::string consumerDeployIn(const std::string& clientId) override {
        return clientId.substr(0, clientId.find('-'));
    }
};

// 队列的 (brokerName, queueId) 序列，用于断言**分配顺序**（NEARBY 的拼接次序）
std::string describe(const std::vector<MessageQueue>& mqs) {
    std::string s = "[";
    for (size_t i = 0; i < mqs.size(); ++i) {
        if (i) s += ",";
        s += mqs[i].brokerName + "#" + std::to_string(mqs[i].queueId);
    }
    return s + "]";
}

// 与 rust allocate_strategy.rs 的 AVERAGELY_CASES / CIRCLE_CASES 同一张表
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
        CHECK_EQ(c.expect.size(), c.cidSize, strategy.getName() + " 用例 cid 数不一致");
        for (size_t i = 0; i < c.expect.size() && i < cidAll.size(); ++i) {
            std::vector<int32_t> got = allocateIds(strategy, cidAll[i], mqAll, cidAll);
            CHECK(join(got) == join(c.expect[i]),
                  strategy.getName() + " mq=" + std::to_string(c.mqSize)
                      + " cid=" + std::to_string(c.cidSize) + " index=" + std::to_string(i)
                      + " got " + join(got));
        }
    }
}

// Java AllocateMessageQueueAveragelyTest：10 队列 / 4 消费者 → size {3,3,2,2}
void testAveragelyMatchesJavaUnitTest() {
    runTable(AllocateMessageQueueAveragely(), averagelyCases());
    std::vector<MessageQueue> mqAll = queuesOf(10);
    std::vector<std::string> cidAll = cidsOf(4);
    AllocateMessageQueueAveragely s;
    std::vector<size_t> sizes;
    for (const std::string& cid : cidAll) sizes.push_back(s.allocate("G", cid, mqAll, cidAll).size());
    CHECK(sizes == std::vector<size_t>({3, 3, 2, 2}), "Java 单测 size {3,3,2,2}");
}

// Java AllocateMessageQueueAveragelyByCircleTest：10 / 4 → {0,4,8} {1,5,9} {2,6} {3,7}
void testCircleMatchesJavaUnitTest() {
    runTable(AllocateMessageQueueAveragelyByCircle(), circleCases());
    std::vector<MessageQueue> mqAll = queuesOf(10);
    std::vector<std::string> cidAll = cidsOf(4);
    AllocateMessageQueueAveragelyByCircle s;
    // Java 单测第一段断言：currentCID 不在 cidAll 里 → size 0
    CHECK_EQ(s.allocate("G", "CID_PREFIX", mqAll, cidAll).size(), static_cast<size_t>(0),
             "不在 cidAll 里 → 空结果");
}

// 守卫：非法入参返回空列表，而不是像 Java check() 那样抛 IllegalArgumentException
void testGuardsReturnEmpty() {
    struct Guard {
        const char* name;
        std::string cid;
        int32_t mqSize;
        size_t cidSize;
    };
    const std::vector<Guard> guards = {
        {"空 currentCid", "", 4, 2},
        {"空 mqAll", "CID_PREFIX0", 0, 2},
        {"空 cidAll", "CID_PREFIX0", 4, 0},
        {"currentCid 不在 cidAll", "CID_NOT_IN_LIST", 4, 2},
    };
    const AllocateMessageQueueAveragely avg;
    const AllocateMessageQueueAveragelyByCircle circle;
    for (const Guard& g : guards) {
        std::vector<MessageQueue> mqAll = queuesOf(g.mqSize);
        std::vector<std::string> cidAll = cidsOf(g.cidSize);
        CHECK(avg.allocate("G", g.cid, mqAll, cidAll).empty(),
              std::string("AVG / ") + g.name + " 应返回空结果");
        CHECK(circle.allocate("G", g.cid, mqAll, cidAll).empty(),
              std::string("AVG_BY_CIRCLE / ") + g.name + " 应返回空结果");
    }
}

// Java AllocateMessageQueueByConfigTest：配 4 个队列，2 个消费者都拿到 [0,1,2,3]
void testByConfigMatchesJavaUnitTest() {
    std::vector<MessageQueue> mqAll = queuesOf(4);
    std::vector<std::string> cidAll = cidsOf(2);
    AllocateMessageQueueByConfig s;
    s.setMessageQueueList(mqAll);
    for (const std::string& cid : cidAll) {
        CHECK(join(allocateIds(s, cid, mqAll, cidAll)) == "[0,1,2,3]", "CONFIG 每个消费者拿全量");
    }
    CHECK(join(idsOf(s.getMessageQueueList())) == "[0,1,2,3]", "getMessageQueueList 返回配置值");
}

// Java / Python 的 ByConfig.allocate 都不调 check：守卫场景照样返回配置值
void testByConfigIgnoresGuards() {
    AllocateMessageQueueByConfig s(queuesOf(2));
    std::vector<std::string> emptyCid;
    CHECK(join(allocateIds(s, "", queuesOf(0), emptyCid)) == "[0,1]", "CONFIG 跳过空守卫");
    CHECK(join(allocateIds(s, "anyCID", queuesOf(5), emptyCid)) == "[0,1]", "CONFIG 忽略入参队列");
}

// 未配置 = 空列表（Python/Rust 口径），且 allocate 返回副本
void testByConfigDefaultsEmptyAndReturnsCopy() {
    AllocateMessageQueueByConfig s;
    CHECK(s.getMessageQueueList().empty(), "默认未配置 = 空");
    std::vector<MessageQueue> before = s.allocate("G", "CID0", {}, {});
    s.setMessageQueueList(queuesOf(3));
    CHECK(before.empty(), "allocate 返回的必须是副本");
    CHECK(join(idsOf(s.allocate("G", "CID0", {}, {}))) == "[0,1,2]", "改配置后对新调用生效");
}

// getName() 字面值（Java 六个策略的 getName 返回值）
void testNamesMatchJava() {
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
          "getName 与 Java 一致");
}

// 全覆盖 + 不重叠：任意 (队列数, 消费者数) 下所有消费者的并集恰为 mqAll
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
                         s->getName() + " 分配总数不等于队列数");
                std::set<int32_t> distinct(all.begin(), all.end());
                CHECK_EQ(distinct.size(), static_cast<size_t>(mqSize),
                         s->getName() + " 分配重叠");
            }
        }
    }
}

// 与 Rust/Python 的算式对拍（直译 Java 公式，独立于被测实现）
void testCrossCheckAgainstPythonFormula() {
    // Python consumer.py:188-214 的算式（start/end 双分支 + 切片）
    auto pythonAveragely = [](const std::vector<int32_t>& mq, const std::vector<std::string>& cid,
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
    auto pythonCircle = [](const std::vector<int32_t>& mq, const std::vector<std::string>& cid,
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
                std::string avgExpect = join(pythonAveragely(ids, cidAll, cid));
                std::string circleExpect = join(pythonCircle(ids, cidAll, cid));
                CHECK(join(allocateIds(avg, cid, mqAll, cidAll)) == avgExpect,
                      "AVG 与 Python 算式不一致 " + std::to_string(mqSize) + "x"
                          + std::to_string(cidSize) + " cid=" + cid);
                CHECK(join(allocateIds(circle, cid, mqAll, cidAll)) == circleExpect,
                      "AVG_BY_CIRCLE 与 Python 算式不一致 " + std::to_string(mqSize) + "x"
                          + std::to_string(cidSize) + " cid=" + cid);
            }
        }
    }
}

// 真实队列（同 topic 挂多 broker）也按下标切分，与 brokerName / queueId 无关
void testRealQueuesFromMultipleBrokers() {
    const std::vector<std::pair<std::string, int32_t>> layout = {
        {"broker-a", 0}, {"broker-a", 1}, {"broker-b", 0}, {"broker-b", 1}, {"broker-c", 0}};
    std::vector<MessageQueue> mqAll;
    for (const auto& kv : layout) mqAll.emplace_back("TopicTest", kv.first, kv.second);
    std::vector<std::string> cidAll = cidsOf(2);
    AllocateMessageQueueAveragely s;
    std::vector<MessageQueue> first = s.allocate("G", cidAll[0], mqAll, cidAll);
    std::vector<MessageQueue> second = s.allocate("G", cidAll[1], mqAll, cidAll);
    CHECK_EQ(first.size(), static_cast<size_t>(3), "AVG 第一个消费者 3 条");
    CHECK_EQ(second.size(), static_cast<size_t>(2), "AVG 第二个消费者 2 条");
    CHECK(first[2].brokerName == "broker-b" && first[2].queueId == 0, "边界队列归属第一个消费者");
    CHECK(second[0].brokerName == "broker-b" && second[0].queueId == 1, "第二个消费者从边界续上");
}

// ============================================================ CONSISTENT_HASH

void runConsistentHashTable(const AllocateMessageQueueStrategy& strategy,
                            const std::vector<Case>& cases) {
    for (const Case& c : cases) {
        const std::vector<MessageQueue> mqAll = queuesOf(c.mqSize);  // ("topic","brokerName",i)
        const std::vector<std::string> cidAll = chCids(c.cidSize);
        CHECK_EQ(c.expect.size(), c.cidSize, strategy.getName() + " 用例 cid 数不一致");
        for (size_t i = 0; i < c.expect.size() && i < cidAll.size(); ++i) {
            const std::vector<int32_t> got = chAllocateIds(strategy, cidAll[i], mqAll, cidAll);
            CHECK(join(got) == join(c.expect[i]),
                  strategy.getName() + " mq=" + std::to_string(c.mqSize)
                      + " cid=" + std::to_string(c.cidSize) + " index=" + std::to_string(i)
                      + " got " + join(got));
        }
    }
}

// 落点表（virtualNodeCnt = 3）。期望值来自**真实 Java 5.5.1 客户端**，与 Rust / Python /
// .NET 的表逐值相同：四语言 + Java 必须算出同一个环，否则混跑时全体队列换主
// （重复 / 漏消费），而不是"分得稍有不均"。
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

// 同一算法、默认 virtualNodeCnt = 10（Java 无参构造）
const std::vector<Case>& consistentHashDefaultVcCases() {
    static const std::vector<Case> kCases = {
        {4, 2, {{0, 2}, {1, 3}}},
        {8, 3, {{0, 2, 4}, {3, 5, 6, 7}, {1}}},
    };
    return kCases;
}

void testConsistentHashMatchesJavaRingTable() {
    runConsistentHashTable(AllocateMessageQueueConsistentHash(3), consistentHashCases());
    runConsistentHashTable(AllocateMessageQueueConsistentHash(), consistentHashDefaultVcCases());
    CHECK_EQ(AllocateMessageQueueConsistentHash(3).getVirtualNodeCnt(), 3, "vc=3 可读回");
    CHECK_EQ(AllocateMessageQueueConsistentHash().getVirtualNodeCnt(), 10,
             "默认 vc=10（Java 无参构造）");
}

// Java verifyAllocateAll：任意规模下每条队列恰好分给一个消费者（不重不漏）
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
                  "CONSISTENT_HASH " + std::to_string(mqSize) + "×" + std::to_string(cidSize));
        }
    }
}

// 一致性哈希的全部意义：成员变化只动涉及的那段弧，其它消费者的队列不换主
void testConsistentHashIsStableWhenMembershipChanges() {
    const AllocateMessageQueueConsistentHash strategy(3);
    const std::vector<MessageQueue> mqAll = queuesOf(9);
    const std::vector<std::string> cidAll = chCids(4);
    // owner[qid] = 拿到该队列的 clientId
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

    // 摘掉 CID-0：它原来的队列会被别人接走（那是必须发生的），
    // 但**原本就在别人手里**的队列必须还在那个人手里。
    const std::vector<std::string> remaining(cidAll.begin() + 1, cidAll.end());
    const std::vector<std::string> afterRemove = ownerOf(remaining);
    for (size_t qid = 0; qid < before.size(); ++qid) {
        if (before[qid] != "CID-0") {
            CHECK_EQ(afterRemove[qid], before[qid], "摘队时 qid=" + std::to_string(qid) + " 不该换主");
        }
    }

    // 加一个新消费者：同理，只有分给 CID-NEW 的队列是新增的。
    std::vector<std::string> joined = remaining;
    joined.push_back("CID-NEW");
    const std::vector<std::string> afterAdd = ownerOf(joined);
    for (size_t qid = 0; qid < before.size(); ++qid) {
        if (afterAdd[qid] != "CID-NEW") {
            CHECK_EQ(afterAdd[qid], before[qid], "加人时 qid=" + std::to_string(qid) + " 不该换主");
        }
    }
}

// 构造期就抛（Java IllegalArgumentException）；0 合法（Java 只挡 <0）⇒ 环空 ⇒ 一条都分不到
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
    CHECK(threw, "必须拒绝负数虚拟节点数");
    CHECK(text.find("illegal virtualNodeCnt :-1") != std::string::npos,
          "文案对齐 Java 构造器：" + text);
    const AllocateMessageQueueConsistentHash zero(0);
    CHECK(chAllocateIds(zero, "CID-0", queuesOf(4), chCids(2)).empty(), "vc=0 ⇒ 谁都拿不到队列");
}

// Java 探针场景：两个 cid 的虚拟节点 key（"CID-0-0"…）首字符都是 'C' ⇒ 哈希相同
// ⇒ Java TreeMap.put 后者覆盖前者，环上只剩 CID-1 ⇒ 队列全归它。
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
    CHECK(chAllocateIds(strategy, "CID-0", mqAll, cidAll).empty(), "Java 真值：CID-0 拿不到队列");
    CHECK(join(chAllocateIds(strategy, "CID-1", mqAll, cidAll)) == "[0,1,2,3]",
          "Java 真值：环上只剩 CID-1");
    CHECK(injected->calls > 0, "自定义哈希没被调用");
}

void testConsistentHashGuardsReturnEmpty() {
    const AllocateMessageQueueConsistentHash strategy;
    const std::vector<MessageQueue> mqAll = queuesOf(4);
    const std::vector<std::string> cidAll = chCids(2);
    const std::vector<std::string> noCid;
    CHECK(chAllocateIds(strategy, "CID-NOT-HERE", mqAll, cidAll).empty(), "cid 不在 cidAll");
    CHECK(chAllocateIds(strategy, "", mqAll, cidAll).empty(), "空 currentCid");
    CHECK(chAllocateIds(strategy, "CID-0", {}, cidAll).empty(), "空 mqAll");
    CHECK(chAllocateIds(strategy, "CID-0", mqAll, noCid).empty(), "空 cidAll");
}

// =============================================================== MACHINE_ROOM

// Java AllocateMessageQueueByMachineRoomTest：10 队列（0..4 在 room1）+ 白名单 {room1}
// + 2 消费者 → [0,1,4] / [2,3]（真实 Java 客户端复核）。
// 余数队列给**前 rem 个**消费者（rem > currentIndex），与 AVG 的切法不同。
void testByMachineRoomMatchesJavaUnitTest() {
    std::vector<MessageQueue> mqAll;
    for (int32_t i = 0; i < 10; ++i) {
        mqAll.emplace_back("topic", i < 5 ? "room1@broker-a" : "room2@broker-b", i);
    }
    const std::vector<std::string> cidAll = cidsOf(2);
    const AllocateMessageQueueByMachineRoom strategy({"room1"});
    CHECK(join(allocateIds(strategy, cidAll[0], mqAll, cidAll)) == "[0,1,4]", "room1 第 1 个消费者");
    CHECK(join(allocateIds(strategy, cidAll[1], mqAll, cidAll)) == "[2,3]", "room1 第 2 个消费者");
    CHECK_EQ(strategy.getConsumeridcs().size(), static_cast<size_t>(1), "白名单只配了 room1");
}

// broker 名必须是 `机房@名字`，且切分按 Java String#split 的裁尾口径。
// javaParts 一列是 JDK 17 实测的 `String#split("@")` 段数。
void testByMachineRoomUsesJavaSplitOnTheBrokerName() {
    const std::vector<std::string> cidAll = cidsOf(1);
    AllocateMessageQueueByMachineRoom strategy({"room1"});
    struct SplitCase {
        const char* broker;
        size_t javaParts;
        bool allocated;
    };
    const std::vector<SplitCase> cases = {
        {"room1@broker-a", 2, true},
        {"room1@", 1, false},   // 尾空段被 Java 丢掉
        {"room1@b@", 2, true},  // 裁尾后仍是 2 段
        {"@room1", 2, false},   // 2 段，但机房是空串、不在白名单
        {"room1@broker@a", 3, false},
        {"@", 0, false},
        {"broker-a", 1, false},
        {"", 1, false},
    };
    for (const SplitCase& c : cases) {
        CHECK_EQ(javaSplit(c.broker, '@').size(), c.javaParts,
                 std::string("javaSplit(\"") + c.broker + "\") 段数与 Java 不一致");
        const std::vector<MessageQueue> mqAll = roomQueuesOf(c.broker, 0, 1);
        const bool got = !allocateIds(strategy, cidAll[0], mqAll, cidAll).empty();
        CHECK_EQ(got, c.allocated, std::string("broker=\"") + c.broker + "\" 参与/剔除判定不一致");
    }
    // 没配机房 = 一条都不分（Java 此处是 NPE，本端口按"守卫返回空"口径）
    AllocateMessageQueueByMachineRoom unset;
    CHECK(unset.allocate("G", cidAll[0], roomQueuesOf("room1@b", 0, 1), cidAll).empty(),
          "未配置白名单 ⇒ 不分（Java 是 NPE）");
    // 白名单可替换（Java setter），换完立刻对同一条队列生效
    strategy.setConsumeridcs({"room2"});
    CHECK_EQ(strategy.getConsumeridcs().count("room2"), static_cast<size_t>(1), "getConsumeridcs 反映新配置");
    CHECK(allocateIds(strategy, cidAll[0], roomQueuesOf("room1@broker-a", 0, 1), cidAll).empty(),
          "换白名单后 room1 不再参与");
}

void testByMachineRoomGuardsReturnEmpty() {
    const std::vector<MessageQueue> mqAll = roomQueuesOf("room1@broker-a", 0, 4);
    const AllocateMessageQueueByMachineRoom strategy({"room1"});
    const std::vector<std::string> two = cidsOf(2);
    const std::vector<std::string> none;
    CHECK(strategy.allocate("G", "", mqAll, two).empty(), "空 currentCid");
    CHECK(strategy.allocate("G", "CID_PREFIX0", mqAll, none).empty(), "空 cidAll");
    CHECK(strategy.allocate("G", "CID_NOT_IN_LIST", mqAll, two).empty(), "cid 不在 cidAll");
    CHECK(strategy.allocate("G", "CID_PREFIX0", {}, two).empty(), "空 mqAll");
}

// ========================================================= MACHINE_ROOM_NEARBY

// Java AllocateMachineRoomNearbyTest#createMessageQueueList：idc 个机房 × 每机房 size 条队列
std::vector<MessageQueue> nearbyMq(size_t idcSize, int32_t queueSize) {
    std::vector<MessageQueue> out;
    for (size_t i = 1; i <= idcSize; ++i) {
        for (int32_t q = 0; q < queueSize; ++q) {
            out.emplace_back("topic", "IDC" + std::to_string(i) + "-brokerName", q);
        }
    }
    return out;
}

// Java #createConsumerIdList：idc 个机房 × 每机房 consumerSize 个消费者
std::vector<std::string> nearbyCids(size_t idcSize, size_t consumerSize) {
    std::vector<std::string> out;
    for (size_t i = 1; i <= idcSize; ++i) {
        for (size_t q = 0; q < consumerSize; ++q) {
            out.push_back("IDC" + std::to_string(i) + "-CID-" + std::to_string(q));
        }
    }
    return out;
}

// Java testWhenIDCSizeEquals：机房数相等时每人只拿到**同机房**的队列，
// 且全员并集恰好是全集（不重不漏）。四组规模与 Java 参数化用例一致。
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
                // 机房数相等 ⇒ 不该外流；消费者数少于队列数时也只在本机房内切
                if (resolver.brokerDeployIn(mq) != resolver.consumerDeployIn(cid)) sameRoom = false;
                flat.push_back(mq.brokerName + "#" + std::to_string(mq.queueId));
            }
        }
        CHECK(sameRoom, "5×20×" + std::to_string(consumerSize) + " 有人拿到了别机房的队列");
        CHECK_EQ(flat.size(), mqAll.size(),
                 "5×20×" + std::to_string(consumerSize) + " 有漏/重");
        std::sort(flat.begin(), flat.end());
        std::vector<std::string> expected;
        for (const MessageQueue& mq : mqAll) {
            expected.push_back(mq.brokerName + "#" + std::to_string(mq.queueId));
        }
        std::sort(expected.begin(), expected.end());
        CHECK(flat == expected, "5×20×" + std::to_string(consumerSize) + " 并集不等于全集");
    }
}

// Java testWhenConsumerIDCIsLess：broker 机房多于消费者机房时，**没有活消费者**的机房
// 要交给全部消费者共享（否则没人消费），有消费者的机房仍然只给自己的消费者。
void testNearbySharesRoomsWithNoConsumer() {
    const AllocateMachineRoomNearby strategy(
        std::make_shared<AllocateMessageQueueAveragely>(), std::make_shared<DashRoom>());
    // 真实 Java 客户端：mqs = IDC2×4 + IDC1×2，cids = IDC1 的两个消费者
    std::vector<MessageQueue> mqAll = roomQueuesOf("IDC2-brokerName", 0, 4);
    const std::vector<MessageQueue> idc1 = roomQueuesOf("IDC1-brokerName", 0, 2);
    mqAll.insert(mqAll.end(), idc1.begin(), idc1.end());
    const std::vector<std::string> cidAll = {"IDC1-CID-0", "IDC1-CID-1"};

    CHECK(join(idsOf(strategy.allocate("G", "IDC1-CID-0", mqAll, cidAll))) == "[0,0,1]",
          "IDC1 自己 2 条 + 空机房 IDC2 的 4 条按 AVG 分一半");
    // 顺序同样是 Java 的口径：先收同机房队列，再补空机房的共享队列
    CHECK(describe(strategy.allocate("G", "IDC1-CID-0", mqAll, cidAll)) ==
              "[IDC1-brokerName#0,IDC2-brokerName#0,IDC2-brokerName#1]",
          "第 1 个消费者的分配顺序");
    CHECK(describe(strategy.allocate("G", "IDC1-CID-1", mqAll, cidAll)) ==
              "[IDC1-brokerName#1,IDC2-brokerName#2,IDC2-brokerName#3]",
          "第 2 个消费者的分配顺序");

    // 5 个机房、只有前 2 个有消费者：每条队列都得有人消费，健康机房不外流。
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
    CHECK(!leaked, "有消费者的机房队列外流了");
    CHECK_EQ(claimed, manyMq.size(), "有空机房的场景下必须不重不漏");
}

// getName() = "MACHINE_ROOM_NEARBY-<内层策略名>"（Java 复核）
void testNearbyNameExposesInnerStrategy() {
    const std::shared_ptr<MachineRoomResolver> resolver = std::make_shared<DashRoom>();
    CHECK_EQ(AllocateMachineRoomNearby(std::make_shared<AllocateMessageQueueAveragely>(), resolver)
                 .getName(),
             std::string("MACHINE_ROOM_NEARBY-AVG"), "内层 AVG");
    CHECK_EQ(AllocateMachineRoomNearby(std::make_shared<AllocateMessageQueueAveragelyByCircle>(),
                                       resolver)
                 .getName(),
             std::string("MACHINE_ROOM_NEARBY-AVG_BY_CIRCLE"), "内层 AVG_BY_CIRCLE");
    const auto byConfig = std::make_shared<AllocateMessageQueueByConfig>(queuesOf(1));
    CHECK_EQ(AllocateMachineRoomNearby(byConfig, resolver).getName(),
             std::string("MACHINE_ROOM_NEARBY-CONFIG"), "内层 CONFIG 也一样拼出来");
}

// resolver 给出空机房 ⇒ Java 抛 IllegalArgumentException。这里照抛而不返回空：
// 静默返回空等于把整个 topic 的队列撤走，而 rebalance 抓住异常会保住现有分配。
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
    CHECK(threw, "broker 机房为空必须抛");
    CHECK(text.find("Machine room is null for mq MessageQueue [topic=topic, "
                    "brokerName=brokerName, queueId=0]") != std::string::npos,
          "文案对齐 Java（含 MessageQueue#toString）：" + text);

    text = catchAllocate(
        AllocateMachineRoomNearby(std::make_shared<AllocateMessageQueueAveragely>(),
                                  std::make_shared<BlankConsumerRoom>()),
        mqAll, cidAll, &threw);
    CHECK(threw, "consumer 机房为空必须抛");
    CHECK(text.find("Machine room is null for consumer id CID-0") != std::string::npos,
          "文案对齐 Java：" + text);

    // 守卫仍然优先：cidAll 为空时先返回空，不碰 resolver
    const AllocateMachineRoomNearby nearby(
        std::make_shared<AllocateMessageQueueAveragely>(), std::make_shared<BlankBrokerRoom>());
    const std::vector<std::string> none;
    CHECK(nearby.allocate("G", "CID-0", mqAll, none).empty(), "守卫优先于 resolver 报错");
}

// Java 构造器对 null 参数抛 NullPointerException，文案照抄
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
    CHECK(threw, "内层策略为 null 必须抛");
    CHECK(text.find("allocateMessageQueueStrategy is null") != std::string::npos, "文案：" + text);

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
    CHECK(threw, "resolver 为 null 必须抛");
    CHECK(text.find("machineRoomResolver is null") != std::string::npos, "文案：" + text);
}

// javaSplit 的早返回分支（没命中分隔符时不裁尾）单独钉一遍
void testJavaSplitMatchesJdk() {
    CHECK_EQ(javaSplit("", '@').size(), static_cast<size_t>(1), "\"\" → 1 段");
    CHECK_EQ(javaSplit("@", '@').size(), static_cast<size_t>(0), "\"@\" → 0 段");
    CHECK_EQ(javaSplit("room1@", '@').size(), static_cast<size_t>(1), "\"room1@\" → 1 段");
    CHECK_EQ(javaSplit("room1@b@", '@').size(), static_cast<size_t>(2), "\"room1@b@\" → 2 段");
    CHECK_EQ(javaSplit("@room1", '@').size(), static_cast<size_t>(2), "\"@room1\" → 2 段");
    CHECK_EQ(javaSplit("room1@@b", '@').size(), static_cast<size_t>(3), "中间空段保留");
    const std::vector<std::string> plain = javaSplit("broker-a", '@');
    CHECK_EQ(plain.size(), static_cast<size_t>(1), "没命中分隔符 ⇒ 整串");
    CHECK_EQ(plain[0], std::string("broker-a"), "整串原样给出");
}

}  // namespace

// push / lite pull 消费者默认策略 = AVG，可替换；置 null 由 start() 拒绝（Java checkConfig）
void testConsumersExposeStrategy() {
    DefaultMQPushConsumer push("G_test");
    CHECK(push.allocateMessageQueueStrategy() != nullptr, "push 消费者默认有策略");
    CHECK_EQ(push.allocateMessageQueueStrategy()->getName(), std::string("AVG"),
             "push 默认策略 = AVG");
    push.setAllocateMessageQueueStrategy(std::make_shared<AllocateMessageQueueAveragelyByCircle>());
    CHECK_EQ(push.allocateMessageQueueStrategy()->getName(), std::string("AVG_BY_CIRCLE"),
             "push 可替换为 AVG_BY_CIRCLE");
    // 未配 namesrv / 订阅也会被组名检查先挡住，所以这里直接验证 setter 允许置 null
    // （start() 的守卫见 live 用例 S7a）。
    push.setAllocateMessageQueueStrategy(nullptr);
    CHECK(push.allocateMessageQueueStrategy() == nullptr, "push setter 接受 null（同 Java）");

    DefaultLitePullConsumer lite("G_test");
    CHECK_EQ(lite.allocateMessageQueueStrategy()->getName(), std::string("AVG"),
             "lite 默认策略 = AVG");
    auto byConfig = std::make_shared<AllocateMessageQueueByConfig>();
    byConfig->setMessageQueueList(queuesOf(2));
    lite.setAllocateMessageQueueStrategy(byConfig);
    CHECK_EQ(lite.allocateMessageQueueStrategy()->getName(), std::string("CONFIG"),
             "lite 可替换为 CONFIG");
    CHECK(join(allocateIds(*lite.allocateMessageQueueStrategy(), "whoever", queuesOf(9),
                           cidsOf(3))) == "[0,1]",
          "CONFIG 策略无视 mqAll/cidAll 返回配置队列");
    // lite 的 start() 会先撞上"未订阅"，所以这里只测组名之外的第一道守卫顺序：
    // 策略 null 必须在**任何网络**之前抛（组名/地址检查之后）。
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
    CHECK(threw, "lite 策略为 null 时 start() 抛异常");
    CHECK(msg.find("allocateMessageQueueStrategy is null") != std::string::npos,
          "lite 策略为 null 的文案对齐 Java：" + msg);

    // 拉模式消费者同样带这份配置（Java DefaultMQPullConsumer:89 字段 + :196-202 读写口，
    // checkConfig:803 拒绝 null）；本端口拉模式不做 rebalance，所以它是配置面。
    DefaultMQPullConsumer pull("G_test");
    CHECK_EQ(pull.allocateMessageQueueStrategy()->getName(), std::string("AVG"),
             "pull 默认策略 = AVG");
    pull.setAllocateMessageQueueStrategy(std::make_shared<AllocateMessageQueueByConfig>());
    CHECK_EQ(pull.allocateMessageQueueStrategy()->getName(), std::string("CONFIG"),
             "pull 可替换为 CONFIG");
    pull.setAllocateMessageQueueStrategy(nullptr);
    CHECK(pull.allocateMessageQueueStrategy() == nullptr, "pull setter 接受 null（同 Java）");
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
    CHECK(pullThrew, "pull 策略为 null 时 start() 抛异常");
    CHECK(pullMsg.find("allocateMessageQueueStrategy is null") != std::string::npos,
          "pull 策略为 null 的文案对齐 Java：" + pullMsg);
}

// 三个新策略在 push / lite / pull 消费者上都能换上并读回（暴露面与旧策略一致）
void testNewStrategiesPlugIntoConsumers() {
    const std::vector<std::string> one = {"CID-0"};

    DefaultMQPushConsumer push("G_test");
    push.setAllocateMessageQueueStrategy(std::make_shared<AllocateMessageQueueConsistentHash>(3));
    CHECK_EQ(push.allocateMessageQueueStrategy()->getName(), std::string("CONSISTENT_HASH"),
             "push 可换 CONSISTENT_HASH");
    // 单消费者 + 哈希环：4 条队列全落在自己身上
    CHECK(join(idsOf(push.allocateMessageQueueStrategy()->allocate("g", "CID-0", queuesOf(4), one))) ==
              "[0,1,2,3]",
          "单消费者时环上只有我 ⇒ 全拿");

    DefaultLitePullConsumer lite("G_lite");
    lite.setAllocateMessageQueueStrategy(
        std::make_shared<AllocateMessageQueueByMachineRoom>(std::set<std::string>{"room1"}));
    CHECK_EQ(lite.allocateMessageQueueStrategy()->getName(), std::string("MACHINE_ROOM"),
             "lite 可换 MACHINE_ROOM");
    CHECK(join(idsOf(lite.allocateMessageQueueStrategy()->allocate(
              "g", "CID-0", roomQueuesOf("room1@broker-a", 0, 3), one))) == "[0,1,2]",
          "白名单机房的 3 条队列全给单消费者");

    lite.setAllocateMessageQueueStrategy(std::make_shared<AllocateMachineRoomNearby>(
        std::make_shared<AllocateMessageQueueAveragely>(), std::make_shared<DashRoom>()));
    CHECK_EQ(lite.allocateMessageQueueStrategy()->getName(), std::string("MACHINE_ROOM_NEARBY-AVG"),
             "lite 可换 MACHINE_ROOM_NEARBY-AVG");

    DefaultMQPullConsumer pull("G_test");
    pull.setAllocateMessageQueueStrategy(std::make_shared<AllocateMessageQueueConsistentHash>());
    CHECK_EQ(pull.allocateMessageQueueStrategy()->getName(), std::string("CONSISTENT_HASH"),
             "pull 可换 CONSISTENT_HASH");
}

int main() {
    testAveragelyMatchesJavaUnitTest();
    testCircleMatchesJavaUnitTest();
    testGuardsReturnEmpty();
    testByConfigMatchesJavaUnitTest();
    testByConfigIgnoresGuards();
    testByConfigDefaultsEmptyAndReturnsCopy();
    testNamesMatchJava();
    testPartitionCoversEveryQueueOnce();
    testCrossCheckAgainstPythonFormula();
    testRealQueuesFromMultipleBrokers();
    testConsistentHashMatchesJavaRingTable();
    testConsistentHashCoversEveryQueueOnce();
    testConsistentHashIsStableWhenMembershipChanges();
    testConsistentHashRejectsNegativeVirtualNodeCnt();
    testConsistentHashUsesInjectedHashFunction();
    testConsistentHashGuardsReturnEmpty();
    testByMachineRoomMatchesJavaUnitTest();
    testByMachineRoomUsesJavaSplitOnTheBrokerName();
    testByMachineRoomGuardsReturnEmpty();
    testJavaSplitMatchesJdk();
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
