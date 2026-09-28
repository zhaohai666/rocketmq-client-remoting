// 220 RESET_CONSUMER_CLIENT_OFFSET（Java MQClientInstance.resetOffset:1403-1450）单测
// —— 不需要集群。
//
// 为什么必须离线锁死：这条路径错了是**静默**的 —— 220 是 broker 单向推送
// （Broker2Client.resetOffset 用 oneway 发给消费端，发起方 admin 拿到的是另一笔
// INVOKE_BROKER_TO_RESET_OFFSET 的响应），管理端看到的响应完好无损，而消费端如果
// 只是把新位点写进内存表：
//   1. 重置前取回、重置后才返回的批次一 ack 又把位点推回原处（broker 上刚写下的新
//      位点被盖掉，消费从重置前的位置继续）；
//   2. 本端口重建走 offsetTable_ 表失 + 新位点落盘 —— 新位点没经 onQueuesRevoked
//      的尾巴持久化，进程一重启这次重置等于没做。
// 两种错在真机短期窗口里都表现为"重置了但没完全重置"，所以断言放离线；真机另有一条
// 链路（examples/live_reset_offset.cpp 的 S1/S2：「撤下在途批次 + broker 上的位点
// 0.0-0.5s 内跳到重置值」与「前跳位点不越界 + 恢复消费」）。
//
// 判据来源：Java ClientRemotingProcessor 的 RequestCode.RESET_CONSUMER_CLIENT_OFFSET
// 分支 → MQClientInstance.resetOffset:1403-1450：按 group 找消费者 → consumer.suspend()
// → 第一个循环对「topic 匹配且 offsetTable 里有这条」的队列 `pq.setDropped(true);
// pq.clear()`（在途批次作废）→（非顺序消费）等 RESET_OFFSET_MAX_WAIT=10s → 第二个循环
// `consumer.updateConsumeOffset(mq, offset)` ＋ `rebalanceImpl.removeUnnecessaryMessageQueue
// (mq, pq)`（**先 persist 再 removeOffset**，持久化的是新位点）→ 从 processQueueTable 摘掉
// → finally `consumer.resume()`，重建后从新位点起拉。
// 本端口：新位点先落 consumeOffsetTable_ → retireQueueLocked（代号 +1 即 setDropped）→
// 200ms 等并发消费收尾 → onQueuesRevoked 锁外持久化（等价 persist + removeOffset）→
// doRebalance 重建。
//
// 与 python/tests/test_reset_offset_handler.py、rust/src/client/consumer.rs 的同名测试、
// dotnet/tests/RocketMQ.Client.Tests/ResetOffsetTests.cs 同题（四端同一判据）。
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/body.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

const char* kGroup = "GID_ResetOffsetCppUnit";
const char* kTopic = "ResetOffsetCppUnitTopic";
const char* kBroker = "broker-a";

MessageQueue queue0() {
    return MessageQueue(kTopic, kBroker, 0);
}

MessageExt ext(int64_t queueOffset) {
    MessageExt m;
    m.topic = kTopic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.body = "x";
    return m;
}

std::vector<MessageExt> offsetBatch(int64_t from, int64_t toExclusive) {
    std::vector<MessageExt> out;
    for (int64_t i = from; i < toExclusive; i++) {
        out.push_back(ext(i));
    }
    return out;
}

class RecordingListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        ++calls;
        lastSize = msgs.size();
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
    int calls = 0;
    size_t lastSize = 0;
};

// 未 start 的消费者即可：resetOffset 的本地状态改动全在锁下完成，onQueuesRevoked 在
// mqClient_ 为空时不做网络（只剩广播模式的本地落盘），doRebalance 直接返回。
class Harness {
public:
    explicit Harness(const std::string& group = kGroup)
        : consumer_(group), key_(DefaultMQPushConsumer::offsetKey(queue0())) {
        listener_ = std::make_shared<RecordingListener>();
        consumer_.setMessageListener(listener_);
        consumer_.setAssignedQueue(key_, queue0());
    }

    // 用真实消费路径产生一个已知的已消费位点（位点表没有直接写入面）
    void seedConsumed(int64_t uptoOffsetExclusive) {
        std::vector<MessageExt> batch;
        for (int64_t i = 0; i < uptoOffsetExclusive; i++) {
            batch.push_back(ext(i));
        }
        consumer_.consumeBatch(key_, queue0(), batch, consumer_.queueEpoch(key_));
    }

    const std::string& key() const { return key_; }
    RecordingListener& listener() { return *listener_; }
    DefaultMQPushConsumer& consumer() { return consumer_; }

private:
    DefaultMQPushConsumer consumer_;
    std::string key_;
    std::shared_ptr<RecordingListener> listener_;
};

std::string offText(const std::optional<int64_t>& v) {
    return v ? std::to_string(*v) : "none";
}

void expectOffset(Harness& h, std::optional<int64_t> want, const std::string& name) {
    const auto got = h.consumer().consumeOffset(h.key());
    expect(got == want, name, "want=" + offText(want) + " got=" + offText(got));
}

// ------------------------------------------------- 220 请求体的两种形状

void testParseMapForm() {
    // Java 发起方（MQClientAPIImpl:2405/2408 恒传 language=JAVA）经 broker 的
    // `resBody.encode()`（Broker2Client:232）回推的就是这种 map 形状。
    ResetOffsetBody body;
    body.offsetTable[queue0()] = 100;
    body.offsetTable[MessageQueue(kTopic, kBroker, 1)] = -1;  // timestamp=-1 ⇒ broker 算出的 maxOffset
    const auto parsed = DefaultMQPushConsumer::parseResetOffsetBody(body.encode());
    expect(parsed.size() == 2, "mapForm.entries", std::to_string(parsed.size()));
    expect(parsed.count(queue0()) == 1 && parsed.at(queue0()) == 100, "mapForm.offset0");
    expect(parsed.count(MessageQueue(kTopic, kBroker, 1)) == 1
               && parsed.at(MessageQueue(kTopic, kBroker, 1)) == -1,
           "mapForm.offset1");
}

void testParseForCArrayForm() {
    // language=CPP 的发起方（旧 C++ SDK 管理端）会让 broker 推 ResetOffsetBodyForC：
    // 它的 `offsetTable` 字段是**数组**（每条自带 offset，Java
    // Broker2Client.resetOffset 里 `convertOffsetTable2OffsetList` 产出），字段名是 Java
    // 的驼峰。fastjson2 按字母序写出 ⇒ brokerName/offset/queueId/topic。本端口解析它
    // 是为了与这类管理端互通；解析不出来 = 整笔重置在本消费者上静默丢弃。
    const std::string json =
        "{\"offsetTable\":["
        "{\"brokerName\":\"broker-a\",\"offset\":100,\"queueId\":0,"
        "\"topic\":\"ResetOffsetCppUnitTopic\"},"
        "{\"topic\":\"ResetOffsetCppUnitTopic\",\"queueId\":1,\"brokerName\":\"broker-a\","
        "\"offset\":0,\"unknown\":\"ignored\"}]}";
    const auto parsed = DefaultMQPushConsumer::parseResetOffsetBody(json);
    expect(parsed.size() == 2, "forCForm.entries", std::to_string(parsed.size()));
    expect(parsed.count(queue0()) == 1 && parsed.at(queue0()) == 100, "forCForm.offset0");
    expect(parsed.count(MessageQueue(kTopic, kBroker, 1)) == 1
               && parsed.at(MessageQueue(kTopic, kBroker, 1)) == 0,
           "forCForm.offset1");
    // 反向控制：map 形状的解析器对数组只会得到**空表** —— 这正是"没有数组那一支就静默
    // 丢弃"的机关，固定下来防止哪天顺手把 fallback 删了。
    ResetOffsetBody mapOnly;
    expect(ResetOffsetBody::decode(json, mapOnly) && mapOnly.offsetTable.empty(),
           "forCForm.mapParserYieldsEmpty");
}

void testParseGarbageIsEmpty() {
    expect(DefaultMQPushConsumer::parseResetOffsetBody("").empty(), "garbage.emptyBody");
    expect(DefaultMQPushConsumer::parseResetOffsetBody("not json at all").empty(),
           "garbage.notJson");
    expect(DefaultMQPushConsumer::parseResetOffsetBody("{\"offsetTable\":{\"x\":1}}").empty(),
           "garbage.wrongShape");
    // 空数组 / 空 map 都是合法形状，只是没有条目
    expect(DefaultMQPushConsumer::parseResetOffsetBody("{\"offsetTable\":[]}").empty(),
           "garbage.emptyArray");
    expect(DefaultMQPushConsumer::parseResetOffsetBody("{\"offsetTable\":{}}").empty(),
           "garbage.emptyMap");
}

// ------------------------------------------------- 重置 = 丢队列 + 代号 +1 + 位点经撤销尾巴落盘

void testResetRetiresQueueAndDropsPreResetBatch() {
    Harness h;
    h.seedConsumed(3);
    expectOffset(h, 3, "reset.seedOffset");
    h.consumer().setPendingMessages(h.key(), {ext(3), ext(4)});
    h.consumer().setLastPullAt(h.key(), 1234567890);

    h.consumer().resetOffset(kTopic, {{queue0(), 100}});

    // Java 第一个循环的 pq.setDropped(true); pq.clear()：缓冲与游标作废
    expect(h.consumer().pendingMessages(h.key()).empty(), "reset.pendingCleared");
    expect(h.consumer().lastPullAt(h.key()) == -1, "reset.ownershipDropped",
           std::to_string(h.consumer().lastPullAt(h.key())));
    // Java 第二个循环的 updateConsumeOffset + removeOffset：新位点**不在**内存表里
    //（在撤销尾巴上落盘），表里若留着旧值，下面这条 ack 就会把它盖回来
    expectOffset(h, std::nullopt, "reset.offsetTableCleared");
    expect(h.consumer().queueEpoch(h.key()) == 1, "reset.epochBumped",
           std::to_string(h.consumer().queueEpoch(h.key())));
    expect(h.consumer().getConsumerStatus(kTopic).empty(), "reset.mqMapCleared");

    // 重置前取回、重置后才 ack 的旧批次（代号 0）：整批作废，位点不许复活
    const bool staleDone = h.consumer().consumeBatch(h.key(), queue0(), offsetBatch(0, 2), 0);
    expect(!staleDone, "reset.staleAckRefused");
    expectOffset(h, std::nullopt, "reset.staleAckDropped");

    // 反向控制：重建后（代号 1）的新批次照常推进 —— 闸门不能把正常路径也挡掉
    const bool freshDone = h.consumer().consumeBatch(h.key(), queue0(), offsetBatch(4, 6), 1);
    expect(freshDone, "reset.freshAckAccepted");
    expectOffset(h, 6, "reset.freshAckAdvanced");
}

void testResetOnlyTouchesMatchingTopicAndTableEntries() {
    Harness h;
    const MessageQueue sibling(kTopic, kBroker, 1);
    const std::string siblingKey = DefaultMQPushConsumer::offsetKey(sibling);
    h.consumer().setAssignedQueue(siblingKey, sibling);
    h.seedConsumed(3);  // queue0
    std::vector<MessageExt> siblingBatch;
    for (int64_t i = 0; i < 5; i++) {
        MessageExt m = ext(i);
        m.queueId = 1;
        siblingBatch.push_back(m);
    }
    h.consumer().consumeBatch(siblingKey, sibling, siblingBatch, h.consumer().queueEpoch(siblingKey));
    h.consumer().setLastPullAt(siblingKey, 42);

    // 表里只给了 queue0 ⇒ queue1 的一切原样（一条"clear 全表"的实现会让同实例其它队列全重投）
    h.consumer().resetOffset(kTopic, {{queue0(), 100}});

    expect(h.consumer().queueEpoch(siblingKey) == 0, "scope.siblingEpoch");
    expect(h.consumer().consumeOffset(siblingKey) == std::optional<int64_t>(5),
           "scope.siblingOffset");
    expect(h.consumer().lastPullAt(siblingKey) == 42, "scope.siblingStillAssigned");
    // 未分配的队列给了条目也是 no-op：没有 mqMap_ 条目就没有可落盘的对象，不能凭空造位点
    const uint64_t epochBefore = h.consumer().queueEpoch(h.key());
    h.consumer().resetOffset(kTopic, {{MessageQueue(kTopic, kBroker, 9), 7}});
    expect(h.consumer().queueEpoch(h.key()) == epochBefore, "scope.unassignedNoop");
    // 空 topic / 空表直接返回
    h.consumer().resetOffset("", {{queue0(), 1}});
    h.consumer().resetOffset(kTopic, {});
    expect(h.consumer().queueEpoch(h.key()) == epochBefore, "scope.emptyInputNoop");
}

void testSecondResetKeepsEpochMonotonic() {
    // 重置 → rebalance 重新分回 → 再重置：代号只能单调涨 —— 回落会让**第一次**重置丢掉的
    // 旧批次"代号又对上"复活。第二次重置前必须先把队列分回来（Java 第二次推 220 时
    // processQueueTable 里得有条目；队列没分回来时 broker 的第二次推送就是 no-op，
    // 见 scope.unassignedNoop）。
    Harness h;
    h.consumer().resetOffset(kTopic, {{queue0(), 10}});
    expect(h.consumer().queueEpoch(h.key()) == 1, "twice.firstResetEpoch");
    h.consumer().setAssignedQueue(h.key(), queue0());  // rebalance 重建
    h.consumer().resetOffset(kTopic, {{queue0(), 20}});
    expect(h.consumer().queueEpoch(h.key()) == 2, "twice.epochMonotonic",
           std::to_string(h.consumer().queueEpoch(h.key())));
    expectOffset(h, std::nullopt, "twice.tableStillCleared");
}

// ------------------------------------------------- 广播：新位点必须当场落盘（Java persist 在前）

void testResetPersistsNewOffsetInBroadcast() {
    // 广播模式位点只存本地：撤销尾巴就是这份文件的唯一写入口。先 persist 后 removeOffset
    // 的顺序错了（或干脆不 persist），同名队列下次分回来按 consumeFromWhere 从头重扫。
    const std::string home =
        (std::filesystem::temp_directory_path() / "rmq_cpp_reset_offset_home").string();
    std::filesystem::remove_all(home);
    std::filesystem::create_directories(home);
    const char* oldHome = std::getenv("HOME");
    const std::string savedHome = oldHome == nullptr ? std::string() : std::string(oldHome);
    ::setenv("HOME", home.c_str(), 1);

    {
        Harness h("GID_ResetOffsetCppUnitBroadcast");
        h.consumer().setMessageModel(MessageModel::BROADCASTING);
        expect(h.consumer().clientId().empty(), "broadcast.unstartedClientIdEmpty");
        const MessageQueue sibling(kTopic, kBroker, 1);
        const std::string siblingKey = DefaultMQPushConsumer::offsetKey(sibling);
        h.consumer().setAssignedQueue(siblingKey, sibling);
        h.seedConsumed(3);
        std::vector<MessageExt> siblingBatch;
        for (int64_t i = 0; i < 7; i++) {
            MessageExt m = ext(i);
            m.queueId = 1;
            siblingBatch.push_back(m);
        }
        h.consumer().consumeBatch(siblingKey, sibling, siblingBatch,
                                  h.consumer().queueEpoch(siblingKey));

        h.consumer().resetOffset(kTopic, {{queue0(), 100}});

        // 未 start ⇒ clientId 为空，Java LocalFileOffsetStore 的路径回落 DEFAULT
        const std::string path = home + "/.rocketmq_offsets/DEFAULT/"
                                 + "GID_ResetOffsetCppUnitBroadcast/offsets.json";
        const std::map<std::string, int64_t> loaded =
            DefaultMQPushConsumer::loadLocalOffsetsAt(path);
        // 新位点（100）而不是重置前的 3，且带着队列信息（只有位点没队列会被 build 跳过）
        expect(loaded.count(h.key()) == 1 && loaded.at(h.key()) == 100,
               "broadcast.newOffsetPersisted",
               std::to_string(loaded.count(h.key()) ? loaded.at(h.key()) : -999));
        // 同一次落盘是 merge 不是 replace：兄弟队列的旧位点还在
        expect(loaded.count(siblingKey) == 1 && loaded.at(siblingKey) == 7,
               "broadcast.siblingMerged");
        // 内存表照旧清空（Java removeOffset）
        expectOffset(h, std::nullopt, "broadcast.tableCleared");
    }

    if (oldHome == nullptr) {
        ::unsetenv("HOME");
    } else {
        ::setenv("HOME", savedHome.c_str(), 1);
    }
    std::filesystem::remove_all(home);
}

void testSaveLocalOffsetsSkipsQueuesWithoutOffset() {
    // 撤销表里位点是 null（本端口用 -1 表达）时不凭空造条目：下次分回来读到的应该还是
    // "没有记录"，而不是一个假的 -1（把 -1 当合法位点提交上去等于让 broker 从头开始）。
    const std::string home =
        (std::filesystem::temp_directory_path() / "rmq_cpp_reset_offset_extra_home").string();
    std::filesystem::remove_all(home);
    std::filesystem::create_directories(home);
    const char* oldHome = std::getenv("HOME");
    const std::string savedHome = oldHome == nullptr ? std::string() : std::string(oldHome);
    ::setenv("HOME", home.c_str(), 1);

    {
        const MessageQueue mq(kTopic, kBroker, 3);
        Harness h("GID_ResetOffsetCppUnitExtra");
        h.consumer().setMessageModel(MessageModel::BROADCASTING);
        h.consumer().setAssignedQueue(DefaultMQPushConsumer::offsetKey(mq), mq);
        h.consumer().resetOffset(kTopic, {{mq, -1}});
        const std::string path =
            home + "/.rocketmq_offsets/DEFAULT/GID_ResetOffsetCppUnitExtra/offsets.json";
        const std::map<std::string, int64_t> loaded =
            DefaultMQPushConsumer::loadLocalOffsetsAt(path);
        expect(loaded.empty(), "extra.nullSkipped", std::to_string(loaded.size()));
    }

    if (oldHome == nullptr) {
        ::unsetenv("HOME");
    } else {
        ::setenv("HOME", savedHome.c_str(), 1);
    }
    std::filesystem::remove_all(home);
}

void testResetOffsetExtFieldsNames() {
    // 222 报文键名：`isForce` 少一个字母或写成 `force` 都是**静默**的 —— broker 侧
    // isForce 恒为 false，`Broker2Client.resetOffset:152-158` 的分支退化成「取时间戳位点」，
    // 前重（timestamp=-1）把 consumerOffset 原样回显而不是跳到 maxOffset。
    // 5.5.1 真机探针：{"force":"true", timestamp:-1} → 目标 3（=consumerOffset），
    //               {"isForce":"true", timestamp:-1} → 目标 10（=maxOffset）。
    const PropertyMap forw =
        DefaultMQAdminExt::buildResetOffsetExtFields(kTopic, kGroup, -1, /*isForce=*/true, -1, -1);
    expect(forw.count("isForce") == 1 && forw.at("isForce") == "true", "ext.isForceTrue",
           forw.count("isForce") ? forw.at("isForce") : "(missing)");
    // 负向对照：错误拼写不得同时存在（有 force 就说明键名写错了）
    expect(forw.count("force") == 0, "ext.noBareForce");
    expect(forw.at("topic") == kTopic && forw.at("group") == kGroup, "ext.topicGroup");
    expect(forw.at("timestamp") == "-1" && forw.at("offset") == "-1", "ext.timestampOffset");
    // 整 topic 重载不带 queueId（Java 的 null 语义），单队列重载必须带
    expect(forw.count("queueId") == 0, "ext.noQueueIdForTopicReset");

    const PropertyMap single = DefaultMQAdminExt::buildResetOffsetExtFields(
        kTopic, kGroup, 0, /*isForce=*/false, 7, 103);
    expect(single.count("queueId") == 1 && single.at("queueId") == "7", "ext.queueIdPresent",
           single.count("queueId") ? single.at("queueId") : "(missing)");
    // 假值也必须下发（不能"false 就省略"）：broker 的 isForce 默认值恰好也是 false，
    // 省略与显式 false 在真机上等价 —— 但 Java 显式下发，端口照抄，缺了就是行为漂移。
    expect(single.count("isForce") == 1 && single.at("isForce") == "false", "ext.isForceFalse",
           single.count("isForce") ? single.at("isForce") : "(missing)");
    expect(single.at("offset") == "103", "ext.singleOffset", single.at("offset"));
}

}  // namespace

int main() {
    testParseMapForm();
    testParseForCArrayForm();
    testParseGarbageIsEmpty();
    testResetRetiresQueueAndDropsPreResetBatch();
    testResetOnlyTouchesMatchingTopicAndTableEntries();
    testSecondResetKeepsEpochMonotonic();
    testResetPersistsNewOffsetInBroadcast();
    testSaveLocalOffsetsSkipsQueuesWithoutOffset();
    testResetOffsetExtFieldsNames();
    std::printf("reset offset: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
