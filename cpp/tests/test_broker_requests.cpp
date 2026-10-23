// Offline unit test for the broker-pushed NOTIFY_CONSUMER_IDS_CHANGED(40) -- no cluster needed.
//
// Why its own file: these requests come back at the client over the **already established**
// long connection, so a live test cannot inject them (the live tools only send requests outward).
// What can be pinned offline must be pinned first, or a "handler never registered" kind of bug
// would only show in production as "members changed, wait 20s for the periodic rebalance", silent.
//
//
// Covered:
//   - counting plus waking the whole group (two consumers registered, both are named)
//   - after unregisterRebalanceWakeup no more wakeups, but the notification is still handled
//   - a missing consumerGroup does not throw and is still counted
//   - instance shutdown clears the wakeup table (a callback capturing this must not outlive it dangling)
//   - the 220/221/307/309 fan-out: every group registered on one instance keeps its own
//     handler, and each unclaimed code gets the tail the asking side can act on
#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include <string>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

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

/// The broker sends a oneway notification whose extFields carry only consumerGroup.
RemotingCommand notifyCmd(const std::string& group) {
    RemotingCommand cmd = RemotingCommand::createRequestCommand(
        RequestCode::NOTIFY_CONSUMER_IDS_CHANGED);
    if (!group.empty()) cmd.extFields["consumerGroup"] = group;
    return cmd;
}

void testFanOut() {
    MQClientInstance instance("cpp-unit-40@fanout", {"127.0.0.1:9876"});
    int wakeA = 0;
    int wakeB = 0;
    instance.registerRebalanceWakeup("GID_A", [&wakeA] { ++wakeA; });
    instance.registerRebalanceWakeup("GID_B", [&wakeB] { ++wakeB; });

    instance.processNotifyConsumerIdsChanged(notifyCmd("GID_A"), "127.0.0.1:10911");
    expect(instance.consumerIdsChangedCount() == 1, "notify.counted");
    expect(wakeA == 1, "notify.wakesFirst", "wakeA=" + std::to_string(wakeA));
    expect(wakeB == 1, "notify.wakesEveryConsumer", "wakeB=" + std::to_string(wakeB));

    instance.processNotifyConsumerIdsChanged(notifyCmd(""), "127.0.0.1:10911");
    expect(instance.consumerIdsChangedCount() == 2, "notify.blankGroupStillCounts");
    expect(wakeA == 2 && wakeB == 2, "notify.blankGroupWakesAll",
           "wakeA=" + std::to_string(wakeA) + " wakeB=" + std::to_string(wakeB));

    // The deregistered group is no longer woken, but the notification itself is still handled.
    instance.unregisterRebalanceWakeup("GID_B");
    instance.processNotifyConsumerIdsChanged(notifyCmd("GID_A"), "127.0.0.1:10911");
    expect(instance.consumerIdsChangedCount() == 3, "notify.countsAfterUnregister");
    expect(wakeA == 3, "notify.stillWakesRegistered", "wakeA=" + std::to_string(wakeA));
    expect(wakeB == 2, "notify.unregisteredNotWoken", "wakeB=" + std::to_string(wakeB));

    // An empty table must not blow up: with no consumer registered the notice is still consumed.
    instance.unregisterRebalanceWakeup("GID_A");
    instance.rebalanceImmediately();
    expect(instance.consumerIdsChangedCount() == 3, "notify.emptyTableIsNoop");
}

void testShutdownClearsWakeups() {
    MQClientInstance instance("cpp-unit-40@shutdown", {"127.0.0.1:9876"});
    int woken = 0;
    instance.registerRebalanceWakeup("GID_A", [&woken] { ++woken; });
    instance.shutdown();
    instance.rebalanceImmediately();
    expect(woken == 0, "shutdown.clearsWakeups", "woken=" + std::to_string(woken));
    // A notification arriving after shutdown (the read thread may still be draining) must not throw.
    instance.processNotifyConsumerIdsChanged(notifyCmd("GID_A"), "127.0.0.1:10911");
    expect(instance.consumerIdsChangedCount() == 1, "notify.afterShutdownStillCounts");
}

}  // namespace

// ---------------------------------------------------- 220/221/307/309 fan-out (221 parity)
namespace {

const char* kStatusTopic = "ConsumerStatusCppUnitTopic";
const char* kStatusBroker = "broker-a";
const char* kBrokerAddr = "127.0.0.1:10911";

std::string extOf(const RemotingCommand& cmd, const std::string& key) {
    auto it = cmd.extFields.find(key);
    return it == cmd.extFields.end() ? std::string() : it->second;
}

// The reply body a consumer would report for one queue of its own group.
Bytes statusBody(const MessageQueue& mq, int64_t offset) {
    GetConsumerStatusBody b;
    b.messageQueueTable[mq] = offset;
    return b.encode();
}

const Bytes& emptyStatusBody() {
    static const Bytes empty = GetConsumerStatusBody{}.encode();
    return empty;
}

RemotingCommand statusCmd(const std::string& group) {
    RemotingCommand cmd =
        RemotingCommand::createRequestCommand(RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT);
    if (!group.empty()) cmd.extFields["group"] = group;
    return cmd;
}

RemotingCommand runningInfoCmd(const std::string& group) {
    RemotingCommand cmd =
        RemotingCommand::createRequestCommand(RequestCode::GET_CONSUMER_RUNNING_INFO);
    if (!group.empty()) cmd.extFields["consumerGroup"] = group;
    return cmd;
}

RemotingCommand directConsumeCmd(const std::string& group) {
    RemotingCommand cmd =
        RemotingCommand::createRequestCommand(RequestCode::CONSUME_MESSAGE_DIRECTLY);
    if (!group.empty()) cmd.extFields["consumerGroup"] = group;
    return cmd;
}

RemotingCommand resetOffsetCmd(const std::string& group) {
    RemotingCommand cmd =
        RemotingCommand::createRequestCommand(RequestCode::RESET_CONSUMER_CLIENT_OFFSET);
    if (!group.empty()) cmd.extFields["group"] = group;
    cmd.markOnewayRpc();
    return cmd;
}

// Shaped exactly like a consumer's own handler: it answers for its group and stays silent
// for every other one, because the sibling that owns that group must be the one to reply.
RemotingClient::RequestProcessor statusHandler(const std::string& group, const MessageQueue& mq,
                                               int64_t offset, int& asked) {
    return [group, mq, offset, &asked](const RemotingCommand& cmd,
                                       const std::string&) -> std::optional<RemotingCommand> {
        ++asked;
        if (extOf(cmd, "group") != group) return std::nullopt;
        RemotingCommand resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS,
                                                                     std::string());
        resp.body = statusBody(mq, offset);
        return resp;
    };
}

// Every group registered on one instance keeps its own handler: the second registration
// must not displace the first. With the transport table (one slot per code) the last
// consumer wins and asking the first group returns the instance's empty tail instead of
// its table -- which is why the control below names the owning group's offsets.
void testConsumerProcessorKeepsEveryGroup() {
    MQClientInstance instance("cpp-unit-221@fanout", {"127.0.0.1:9876"});
    const MessageQueue mqA(kStatusTopic, kStatusBroker, 0);
    const MessageQueue mqB(kStatusTopic, kStatusBroker, 1);
    int askedA = 0;
    int askedB = 0;
    instance.registerConsumerProcessor(RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, "GID_A",
                                       statusHandler("GID_A", mqA, 11, askedA));
    instance.registerConsumerProcessor(RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, "GID_B",
                                       statusHandler("GID_B", mqB, 22, askedB));

    auto first = instance.dispatchConsumerProcessor(
        RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, statusCmd("GID_A"), kBrokerAddr);
    expect(first.has_value(), "221.hostedGroupIsAnswered");
    expect(first && first->code == ResponseCode::SUCCESS, "221.hostedGroupAnswersSUCCESS",
           first ? std::to_string(first->code) : std::string("(no reply)"));
    expect(first && first->body == statusBody(mqA, 11), "221.firstGroupKeepsItsOwnTable",
           first ? std::string(first->body.begin(), first->body.end()) : std::string("(no reply)"));
    expect(askedA == 1 && askedB == 0, "221.ownerStopsTheSearch",
           "askedA=" + std::to_string(askedA) + " askedB=" + std::to_string(askedB));

    auto second = instance.dispatchConsumerProcessor(
        RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, statusCmd("GID_B"), kBrokerAddr);
    expect(second && second->body == statusBody(mqB, 22), "221.secondGroupKeepsItsOwnTable",
           second ? std::string(second->body.begin(), second->body.end()) : std::string("(no reply)"));
    expect(askedA == 2 && askedB == 1, "221.silentHandlerLetsTheOwnerAnswer",
           "askedA=" + std::to_string(askedA) + " askedB=" + std::to_string(askedB));

    // A group nobody hosts is still SUCCESS with an empty table, and so is a request that
    // names no group at all: the asking side decodes the body whatever the code says.
    auto nobody = instance.dispatchConsumerProcessor(
        RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, statusCmd("GID_Nobody"), kBrokerAddr);
    expect(nobody.has_value(), "221.unhostedGroupIsAnswered");
    expect(nobody && nobody->code == ResponseCode::SUCCESS, "221.unhostedGroupAnswersSUCCESS",
           nobody ? std::to_string(nobody->code) : std::string("(no reply)"));
    expect(nobody && nobody->body == emptyStatusBody(), "221.unhostedGroupReportsAnEmptyTable",
           nobody ? std::string(nobody->body.begin(), nobody->body.end()) : std::string("(no reply)"));

    auto blank = instance.dispatchConsumerProcessor(
        RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, statusCmd(""), kBrokerAddr);
    expect(blank && blank->body == emptyStatusBody(), "221.blankGroupReportsAnEmptyTable");

    // Deregistering one group must leave its sibling alone.
    instance.unregisterConsumerProcessor(RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, "GID_B");
    int askedBBefore = askedB;
    auto gone = instance.dispatchConsumerProcessor(
        RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, statusCmd("GID_B"), kBrokerAddr);
    expect(gone && gone->body == emptyStatusBody(), "221.deregisteredGroupReportsAnEmptyTable",
           gone ? std::string(gone->body.begin(), gone->body.end()) : std::string("(no reply)"));
    expect(askedB == askedBBefore, "221.deregisteredHandlerIsNeverAsked",
           "askedB=" + std::to_string(askedB));
    auto kept = instance.dispatchConsumerProcessor(
        RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, statusCmd("GID_A"), kBrokerAddr);
    expect(kept && kept->body == statusBody(mqA, 11), "221.siblingKeepsItsGroup");

    // Shutdown clears the whole table: a handler capturing a dead consumer must not survive it.
    instance.shutdown();
    int askedAfterShutdown = askedA;
    auto dead = instance.dispatchConsumerProcessor(
        RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, statusCmd("GID_A"), kBrokerAddr);
    expect(askedA == askedAfterShutdown, "shutdown.doesNotAskAClearedConsumer",
           "askedA=" + std::to_string(askedA));
    expect(dead && dead->body == emptyStatusBody(), "shutdown.clearsConsumerProcessors",
           dead ? std::string(dead->body.begin(), dead->body.end()) : std::string("(no reply)"));
}

// One consumer's handler blowing up must not cost the group its answer, and it must not
// reach the transport as an unhandled exception either.
void testThrowingHandlerDoesNotHideTheOwner() {
    MQClientInstance instance("cpp-unit-221@throw", {"127.0.0.1:9876"});
    const MessageQueue mq("StatusThrowTopic", kStatusBroker, 0);
    int askedOwner = 0;
    instance.registerConsumerProcessor(
        RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, "GID_AAA_Bad",
        [](const RemotingCommand&, const std::string&) -> std::optional<RemotingCommand> {
            throw std::runtime_error("handler exploded");
        });
    instance.registerConsumerProcessor(RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, "GID_ZZZ_Ok",
                                       statusHandler("GID_ZZZ_Ok", mq, 7, askedOwner));

    auto resp = instance.dispatchConsumerProcessor(
        RequestCode::GET_CONSUMER_STATUS_FROM_CLIENT, statusCmd("GID_ZZZ_Ok"), kBrokerAddr);
    expect(askedOwner == 1, "221.throwingSiblingStillLetsTheOwnerAnswer",
           "askedOwner=" + std::to_string(askedOwner));
    expect(resp && resp->body == statusBody(mq, 7), "221.throwingSiblingDoesNotReplaceTheTable",
           resp ? std::string(resp->body.begin(), resp->body.end()) : std::string("(no reply)"));
    instance.shutdown();
}

// Only 221 answers an empty table. 307/309 report the group they were asked about, and 220
// is a oneway push that gets no reply at all.
void testUnclaimedCodesAnswerWhatTheAskingSideNeeds() {
    MQClientInstance instance("cpp-unit-221@tails", {"127.0.0.1:9876"});
    const std::string wantRemark = "The Consumer Group <GID_Nobody> not exist in this consumer";
    int asked = 0;
    instance.registerConsumerProcessor(
        RequestCode::GET_CONSUMER_RUNNING_INFO, "GID_A",
        [&asked](const RemotingCommand& cmd, const std::string&) -> std::optional<RemotingCommand> {
            if (extOf(cmd, "consumerGroup") != "GID_A") return std::nullopt;
            ++asked;
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, std::string());
        });

    auto hosted = instance.dispatchConsumerProcessor(RequestCode::GET_CONSUMER_RUNNING_INFO,
                                                     runningInfoCmd("GID_A"), kBrokerAddr);
    expect(hosted && hosted->code == ResponseCode::SUCCESS, "307.hostedGroupAnswersSUCCESS");
    expect(asked == 1, "307.ownerWasAsked");

    auto missing = instance.dispatchConsumerProcessor(RequestCode::GET_CONSUMER_RUNNING_INFO,
                                                      runningInfoCmd("GID_Nobody"), kBrokerAddr);
    expect(missing.has_value(), "307.unhostedGroupIsAnswered");
    expect(missing && missing->code == ResponseCode::SYSTEM_ERROR, "307.unhostedGroupAnswersError",
           missing ? std::to_string(missing->code) : std::string("(no reply)"));
    expect(missing && missing->remark == wantRemark, "307.remarkNamesTheRequestedGroup",
           missing ? missing->remark : std::string("(no reply)"));
    expect(asked == 1, "307.foreignGroupNeverReachesTheOwner",
           "asked=" + std::to_string(asked));

    auto direct = instance.dispatchConsumerProcessor(RequestCode::CONSUME_MESSAGE_DIRECTLY,
                                                     directConsumeCmd("GID_Nobody"), kBrokerAddr);
    expect(direct && direct->code == ResponseCode::SYSTEM_ERROR, "309.unhostedGroupAnswersError",
           direct ? std::to_string(direct->code) : std::string("(no reply)"));
    expect(direct && direct->remark == wantRemark, "309.remarkNamesTheRequestedGroup",
           direct ? direct->remark : std::string("(no reply)"));

    auto reset = instance.dispatchConsumerProcessor(RequestCode::RESET_CONSUMER_CLIENT_OFFSET,
                                                    resetOffsetCmd("GID_Nobody"), kBrokerAddr);
    expect(!reset.has_value(), "220.onewayGetsNoReply");
    instance.shutdown();
}

class OkListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

MessageExt extOf(const MessageQueue& mq, int64_t queueOffset) {
    MessageExt m;
    m.topic = mq.topic;
    m.brokerName = mq.brokerName;
    m.queueId = mq.queueId;
    m.queueOffset = queueOffset;
    m.body = "x";
    return m;
}

/// The offset table a real consumer hands to 221, and its topic argument: only a
/// non-empty topic filters. The asking side leaves the field empty to mean
/// "everything this client has consumed", so an empty string must not read as
/// "match a topic literally named empty" and report nothing.
void testRealOffsetTableAndBlankTopicArgument() {
    DefaultMQPushConsumer consumer("GID_status_table");
    consumer.setMessageListener(std::make_shared<OkListener>());
    const MessageQueue q0(kStatusTopic, kStatusBroker, 0);
    const MessageQueue q1("OtherConsumerStatusCppUnitTopic", kStatusBroker, 1);
    const std::string k0 = DefaultMQPushConsumer::offsetKey(q0);
    const std::string k1 = DefaultMQPushConsumer::offsetKey(q1);
    consumer.setAssignedQueue(k0, q0);
    consumer.setAssignedQueue(k1, q1);
    consumer.consumeBatch(k0, q0, {extOf(q0, 0)});
    consumer.consumeBatch(k1, q1, {extOf(q1, 0)});

    const auto whole = consumer.getConsumerStatus("");
    expect(whole.size() == 2, "221.blankTopicReportsTheWholeTable",
           "size=" + std::to_string(whole.size()));
    expect(whole.count(q0) == 1 && whole.count(q1) == 1, "221.blankTopicCarriesEveryQueue");

    // Control: a named topic really does filter, so the blank case above is the
    // no-filter rule and not a method that ignores its argument.
    const auto scoped = consumer.getConsumerStatus(kStatusTopic);
    expect(scoped.size() == 1 && scoped.count(q0) == 1, "221.namedTopicFilterIsReal",
           "size=" + std::to_string(scoped.size()));
    expect(consumer.getConsumerStatus("NeverHosted").empty(), "221.unhostedTopicReportsNothing");
}

}  // namespace

int main() {
    testFanOut();
    testShutdownClearsWakeups();
    testConsumerProcessorKeepsEveryGroup();
    testThrowingHandlerDoesNotHideTheOwner();
    testUnclaimedCodesAnswerWhatTheAskingSideNeeds();
    testRealOffsetTableAndBlankTopicArgument();
    std::printf("broker requests: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
