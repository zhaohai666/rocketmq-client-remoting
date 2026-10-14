// Why it must be locked down offline: getting this path wrong is **silent** -- 220 is a one-way push from the broker
// (Broker2Client.resetOffset sends oneway to the consumer, while the initiating admin receives the response of a different
// INVOKE_BROKER_TO_RESET_OFFSET), so the response the admin side sees is perfectly fine, and if the consumer merely
// writes the new offset into the in-memory table:
//   1. a batch fetched before the reset and acked after it pushes the offset right back (the new offset just written
//      on the broker is overwritten and consumption continues from the pre-reset position);
//   2. rebuilding in this port goes through offsetTable_ loss + the new offset persisted -- a new offset that never
//      went through the onQueuesRevoked tail means a process restart undoes the reset.
// Both mistakes look like "reset but not fully reset" inside a short live window, hence the assertions live offline; the
// live side has its own path (S1/S2 of examples/live_reset_offset.cpp: "in-flight batches withdrawn + the offset on the broker
// jumps to the reset value within 0.0-0.5s" and "the forward-jumped offset stays in range + consumption resumes").
//
//
// The same assertions are covered by the sibling offline reset-offset tests in this repository.
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

// ::setenv / ::unsetenv are POSIX-only and do not exist under MSVC (only _putenv_s, where an empty string deletes)
void setEnvVar(const char* name, const char* value) {
#if defined(_WIN32)
    _putenv_s(name, value);
#else
    ::setenv(name, value, 1);
#endif
}

void unsetEnvVar(const char* name) {
#if defined(_WIN32)
    _putenv_s(name, "");
#else
    ::unsetenv(name);
#endif
}

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

// A consumer that never started is enough: every local state change of resetOffset happens under the locks, onQueuesRevoked
// with a null mqClient_ performs no network work (only the local persist of broadcasting mode) and doRebalance returns directly.
class Harness {
public:
    explicit Harness(const std::string& group = kGroup)
        : consumer_(group), key_(DefaultMQPushConsumer::offsetKey(queue0())) {
        listener_ = std::make_shared<RecordingListener>();
        consumer_.setMessageListener(listener_);
        consumer_.setAssignedQueue(key_, queue0());
    }

    // Produce a known consumed offset through the real consumption path (the offset table has no direct write surface)
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

// ------------------------------------------------- the two shapes of a 220 request body

void testParseMapForm() {
    ResetOffsetBody body;
    body.offsetTable[queue0()] = 100;
    body.offsetTable[MessageQueue(kTopic, kBroker, 1)] = -1;  // timestamp=-1 means the maxOffset computed by the broker
    const auto parsed = DefaultMQPushConsumer::parseResetOffsetBody(body.encode());
    expect(parsed.size() == 2, "mapForm.entries", std::to_string(parsed.size()));
    expect(parsed.count(queue0()) == 1 && parsed.at(queue0()) == 100, "mapForm.offset0");
    expect(parsed.count(MessageQueue(kTopic, kBroker, 1)) == 1
               && parsed.at(MessageQueue(kTopic, kBroker, 1)) == -1,
           "mapForm.offset1");
}

void testParseForCArrayForm() {
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
    // Reverse control: the map-shape parser can only ever get an **empty table** from an array -- that is exactly the
    // mechanism behind "silently dropped when the array branch is missing", pinned here so the fallback is not deleted on a whim.
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
    // An empty array / empty map are both legal shapes, simply with no entries
    expect(DefaultMQPushConsumer::parseResetOffsetBody("{\"offsetTable\":[]}").empty(),
           "garbage.emptyArray");
    expect(DefaultMQPushConsumer::parseResetOffsetBody("{\"offsetTable\":{}}").empty(),
           "garbage.emptyMap");
}

// ------------------------------------------------- reset = drop the queue + epoch +1 + offset persisted through the revoke tail

void testResetRetiresQueueAndDropsPreResetBatch() {
    Harness h;
    h.seedConsumed(3);
    expectOffset(h, 3, "reset.seedOffset");
    h.consumer().setPendingMessages(h.key(), {ext(3), ext(4)});
    h.consumer().setLastPullAt(h.key(), 1234567890);

    h.consumer().resetOffset(kTopic, {{queue0(), 100}});

    expect(h.consumer().pendingMessages(h.key()).empty(), "reset.pendingCleared");
    expect(h.consumer().lastPullAt(h.key()) == -1, "reset.ownershipDropped",
           std::to_string(h.consumer().lastPullAt(h.key())));
    expectOffset(h, std::nullopt, "reset.offsetTableCleared");
    expect(h.consumer().queueEpoch(h.key()) == 1, "reset.epochBumped",
           std::to_string(h.consumer().queueEpoch(h.key())));
    expect(h.consumer().getConsumerStatus(kTopic).empty(), "reset.mqMapCleared");

    // A batch fetched before the reset and acked only after it (epoch 0): the whole batch is void, the offset must not come back
    const bool staleDone = h.consumer().consumeBatch(h.key(), queue0(), offsetBatch(0, 2), 0);
    expect(!staleDone, "reset.staleAckRefused");
    expectOffset(h, std::nullopt, "reset.staleAckDropped");

    // Reverse control: after the rebuild (epoch 1) a fresh batch still advances -- the gate must not block the normal path too
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

    // The table only gives queue0, so everything of queue1 stays as it was (a "clear the whole table" implementation would redeliver every other queue of the same instance)
    h.consumer().resetOffset(kTopic, {{queue0(), 100}});

    expect(h.consumer().queueEpoch(siblingKey) == 0, "scope.siblingEpoch");
    expect(h.consumer().consumeOffset(siblingKey) == std::optional<int64_t>(5),
           "scope.siblingOffset");
    expect(h.consumer().lastPullAt(siblingKey) == 42, "scope.siblingStillAssigned");
    // An entry for an unassigned queue is also a no-op: without an mqMap_ entry there is nothing to persist, no offset may be invented
    const uint64_t epochBefore = h.consumer().queueEpoch(h.key());
    h.consumer().resetOffset(kTopic, {{MessageQueue(kTopic, kBroker, 9), 7}});
    expect(h.consumer().queueEpoch(h.key()) == epochBefore, "scope.unassignedNoop");
    // An empty topic / empty table returns immediately
    h.consumer().resetOffset("", {{queue0(), 1}});
    h.consumer().resetOffset(kTopic, {});
    expect(h.consumer().queueEpoch(h.key()) == epochBefore, "scope.emptyInputNoop");
}

void testSecondResetKeepsEpochMonotonic() {
    Harness h;
    h.consumer().resetOffset(kTopic, {{queue0(), 10}});
    expect(h.consumer().queueEpoch(h.key()) == 1, "twice.firstResetEpoch");
    h.consumer().setAssignedQueue(h.key(), queue0());  // rebuilt by rebalance
    h.consumer().resetOffset(kTopic, {{queue0(), 20}});
    expect(h.consumer().queueEpoch(h.key()) == 2, "twice.epochMonotonic",
           std::to_string(h.consumer().queueEpoch(h.key())));
    expectOffset(h, std::nullopt, "twice.tableStillCleared");
}


void testResetPersistsNewOffsetInBroadcast() {
    // In broadcasting mode offsets live only locally: the revoke tail is the single write entry of that file. Getting the
    // persist-then-removeOffset order wrong (or not persisting at all) makes the same queue rescan from the start per consumeFromWhere once it is assigned again.
    const std::string home =
        (std::filesystem::temp_directory_path() / "rmq_cpp_reset_offset_home").string();
    std::filesystem::remove_all(home);
    std::filesystem::create_directories(home);
    const char* oldHome = std::getenv("HOME");
    const std::string savedHome = oldHome == nullptr ? std::string() : std::string(oldHome);
    setEnvVar("HOME", home.c_str());

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

        const std::string path = home + "/.rocketmq_offsets/DEFAULT/"
                                 + "GID_ResetOffsetCppUnitBroadcast/offsets.json";
        const std::map<std::string, int64_t> loaded =
            DefaultMQPushConsumer::loadLocalOffsetsAt(path);
        // The new offset (100) and not the pre-reset 3, and carrying queue info (an offset without a queue is skipped by build)
        expect(loaded.count(h.key()) == 1 && loaded.at(h.key()) == 100,
               "broadcast.newOffsetPersisted",
               std::to_string(loaded.count(h.key()) ? loaded.at(h.key()) : -999));
        // The same persist merges instead of replacing: the sibling queue keeps its old offset
        expect(loaded.count(siblingKey) == 1 && loaded.at(siblingKey) == 7,
               "broadcast.siblingMerged");
        expectOffset(h, std::nullopt, "broadcast.tableCleared");
    }

    if (oldHome == nullptr) {
        unsetEnvVar("HOME");
    } else {
        setEnvVar("HOME", savedHome.c_str());
    }
    std::filesystem::remove_all(home);
}

void testSaveLocalOffsetsSkipsQueuesWithoutOffset() {
    // A null offset in the revoke table (expressed as -1 in this port) must not invent an entry: on the next assignment the read
    // should still be "no record", not a fake -1 (committing -1 as a legal offset is the same as telling the broker to start over).
    const std::string home =
        (std::filesystem::temp_directory_path() / "rmq_cpp_reset_offset_extra_home").string();
    std::filesystem::remove_all(home);
    std::filesystem::create_directories(home);
    const char* oldHome = std::getenv("HOME");
    const std::string savedHome = oldHome == nullptr ? std::string() : std::string(oldHome);
    setEnvVar("HOME", home.c_str());

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
        unsetEnvVar("HOME");
    } else {
        setEnvVar("HOME", savedHome.c_str());
    }
    std::filesystem::remove_all(home);
}

void testResetOffsetExtFieldsNames() {
    // Key names of the 222 payload: one letter missing in `isForce` or spelling it `force` is **silent** -- on the broker side
    // isForce stays false, the branch in `Broker2Client.resetOffset:152-158` degrades into "take the timestamp offset",
    // and a rewind (timestamp=-1) echoes consumerOffset instead of jumping to maxOffset.
    // Live 5.5.1 probe: {"force":"true", timestamp:-1} -> target 3 (=consumerOffset),
    //                   {"isForce":"true", timestamp:-1} -> target 10 (=maxOffset).
    const PropertyMap forw =
        DefaultMQAdminExt::buildResetOffsetExtFields(kTopic, kGroup, -1, /*isForce=*/true, -1, -1);
    expect(forw.count("isForce") == 1 && forw.at("isForce") == "true", "ext.isForceTrue",
           forw.count("isForce") ? forw.at("isForce") : "(missing)");
    // Negative control: the wrong spelling must not coexist (a `force` key means the key name was written wrong)
    expect(forw.count("force") == 0, "ext.noBareForce");
    expect(forw.at("topic") == kTopic && forw.at("group") == kGroup, "ext.topicGroup");
    expect(forw.at("timestamp") == "-1" && forw.at("offset") == "-1", "ext.timestampOffset");
    expect(forw.count("queueId") == 0, "ext.noQueueIdForTopicReset");

    const PropertyMap single = DefaultMQAdminExt::buildResetOffsetExtFields(
        kTopic, kGroup, 0, /*isForce=*/false, 7, 103);
    expect(single.count("queueId") == 1 && single.at("queueId") == "7", "ext.queueIdPresent",
           single.count("queueId") ? single.at("queueId") : "(missing)");
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
