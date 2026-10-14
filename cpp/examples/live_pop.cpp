// Live-cluster verification of POP mode (5.x lightweight consumption).
// Usage: rmq_live_pop 127.0.0.1:9876
//
// The semantic differences between POP and pull are the focus of the assertions:
//   - **no offset commit**, confirmation goes through ack; a message left un-acked is revived and redelivered after invisibleTime;
//   - the broker does **not** write POP_CK on normal-topic messages, so the client must reconstruct the 8-segment CK string itself.
//
// Scenarios:
//   S1 create topic (8 queues) + send 10 messages
//   S2 multi-queue POP (queueId=-1, initMode=0) -> FOUND and messages retrieved
//   S3 every message is stamped with an **8-segment** POP_CK whose brokerName/queueId match the message itself
//   S4 ack one message -> SUCCESS
//   S5 changeInvisibleTime -> SUCCESS and returns a **new** 8-segment extraInfo
//   S6 the validations really bite: illegal queueId -> MESSAGE_ILLEGAL; out-of-range offset -> NO_MESSAGE
//   S7 single-queue POP (new consumer group + queueId=0) -> FOUND and every message has queueId 0
//   S8 no ack revives: POP with a small invisibleTime and do not ack, wait, then re-POP to get the same message back,
//      and its POP_CK carries retryFlag=1 (from %RETRY%<group>_<topic>) -- at-least-once semantics
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/extra_info.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string uniqKeyOf(const MessageExt& m) {
    auto it = m.properties.find(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    if (it != m.properties.end()) {
        return it->second;
    }
    return m.msgId;
}

// The CK string comes from the message properties; an empty string means reconstruction failed
std::string popCkOf(const MessageExt& m) {
    auto it = m.properties.find(MessageConst::PROPERTY_POP_CK);
    return it == m.properties.end() ? std::string() : it->second;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string namesrv = argc > 1 ? argv[1] : "127.0.0.1:9876";
    const int64_t stamp = nowMs();
    const std::string TOPIC = "PopLiveCpp_" + std::to_string(stamp);
    const std::string TOPIC_REVIVE = "PopLiveReviveCpp_" + std::to_string(stamp);
    const std::string GROUP = "GID_PopLiveCpp_" + std::to_string(stamp);
    const std::string GROUP_SINGLE = "GID_PopLiveCppSingle_" + std::to_string(stamp);
    const std::string GROUP_REVIVE = "GID_PopLiveCppRevive_" + std::to_string(stamp);
    const int32_t QUEUE_NUM = 8;
    const int32_t N_MSG = 10;
    const int32_t REVIVE_QUEUE_NUM = 4;
    const int32_t REVIVE_N_MSG = 3;
    const int64_t INVISIBLE_SHORT = 5000;
    const int32_t REVIVE_WAIT_SEC = 20;

    DefaultMQProducer producer("PG_PopLiveCpp_" + std::to_string(stamp));
    producer.setNamesrvAddr(namesrv);
    producer.start();
    MQClientInstance& client = producer.client();

    // ------------------------------------------------ S1 create topic + send messages
    std::printf("=== S1 prepare topic and messages ===\n");
    producer.createTopic(MixAll::DEFAULT_TOPIC, TOPIC, QUEUE_NUM);
    producer.createTopic(MixAll::DEFAULT_TOPIC, TOPIC_REVIVE, REVIVE_QUEUE_NUM);
    for (int32_t i = 0; i < N_MSG; ++i) {
        Message m(TOPIC, "pop-live-cpp-" + std::to_string(i));
        m.setKeys("pk" + std::to_string(i));
        producer.send(m);
    }
    std::printf("  sent %d msgs to %s\n", N_MSG, TOPIC.c_str());

    auto route = client.getTopicRouteData(TOPIC);
    check("S1 topic route available", route != nullptr && !route->brokerDatas.empty());
    if (route == nullptr || route->brokerDatas.empty()) {
        std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
        producer.shutdown();
        return 1;
    }
    const std::string brokerName = route->brokerDatas.front().brokerName;
    const std::string addr =
        MQClientInstance::findBrokerAddrInRoute(*route, brokerName);
    std::printf("  brokerName=%s addr=%s\n", brokerName.c_str(), addr.c_str());

    // ------------------------------------------------ S2 multi-queue POP
    std::printf("=== S2 multi-queue POP (addr goes through the auto-resolution path) ===\n");
    PopResult res = client.popMessage(GROUP, TOPIC, -1, 32, 60000, 0, 0);
    check("S2 POP retrieved messages", res.isFound() && !res.msgFoundList.empty(),
          "status=" + std::string(popStatusName(res.status))
              + " count=" + std::to_string(res.msgFoundList.size())
              + " startOffsetInfo=" + res.startOffsetInfo);
    if (res.msgFoundList.empty()) {
        std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
        producer.shutdown();
        return 1;
    }

    // ------------------------------------------------ S3 POP_CK reconstruction
    std::printf("=== S3 POP_CK reconstruction (8 segments) ===\n");
    bool all8 = true;
    bool match = true;
    bool hasFirstPopTime = true;
    std::string sample;
    for (const MessageExt& m : res.msgFoundList) {
        std::string ck = popCkOf(m);
        if (ck.empty()) {
            all8 = false;
            break;
        }
        std::vector<std::string> seg = extra_info::split(ck);
        if (seg.size() != 8) {
            all8 = false;
            break;
        }
        if (extra_info::getBrokerName(seg) != brokerName
            || extra_info::getQueueId(seg) != m.queueId) {
            match = false;
        }
        if (m.properties.find(MessageConst::PROPERTY_FIRST_POP_TIME) == m.properties.end()) {
            hasFirstPopTime = false;
        }
        if (sample.empty()) {
            sample = ck;
        }
    }
    check("S3a every message has an 8-segment POP_CK", all8, "sample='" + sample + "'");
    check("S3b CK brokerName/queueId match the message", match);
    check("S3c 1ST_POP_TIME filled in", hasFirstPopTime);

    // ------------------------------------------------ S4 ack
    std::printf("=== S4 ACK ===\n");
    const MessageExt& first = res.msgFoundList.front();
    const std::string ck1 = popCkOf(first);
    std::vector<std::string> seg1 = extra_info::split(ck1);
    const int32_t qid1 = extra_info::getQueueId(seg1);
    const int64_t off1 = extra_info::getQueueOffset(seg1);
    int32_t ackCode = client.ackMessage(GROUP, TOPIC, qid1, ck1, off1, 3000, brokerName, addr);
    check("S4 ack returns SUCCESS", ackCode == ResponseCode::SUCCESS,
          "code=" + std::to_string(ackCode) + " queueId=" + std::to_string(qid1)
              + " offset=" + std::to_string(off1));

    // ------------------------------------------------ S5 changeInvisibleTime
    std::printf("=== S5 CHANGE_MESSAGE_INVISIBLETIME ===\n");
    const MessageExt& second = res.msgFoundList[1];
    const std::string ck2 = popCkOf(second);
    std::vector<std::string> seg2 = extra_info::split(ck2);
    ChangeInvisibleTimeResult res5 =
        client.changeInvisibleTime(GROUP, TOPIC, extra_info::getQueueId(seg2), ck2,
                                   extra_info::getQueueOffset(seg2), 30000, 3000, brokerName, addr);
    check("S5a invisible time extended successfully", res5.success(),
          "code=" + std::to_string(res5.responseCode) + " popTime=" + std::to_string(res5.popTime)
              + " invisibleTime=" + std::to_string(res5.invisibleTime));
    std::vector<std::string> newSeg =
        res5.extraInfo.empty() ? std::vector<std::string>() : extra_info::split(res5.extraInfo);
    check("S5b returns a new 8-segment extraInfo using the new values",
          newSeg.size() == 8 && extra_info::getInvisibleTime(newSeg) == res5.invisibleTime
              && extra_info::getPopTime(newSeg) == res5.popTime,
          "new='" + res5.extraInfo + "'");

    // ------------------------------------------------ S6 the validations really bite
    std::printf("=== S6 invalid parameters must be rejected ===\n");
    int32_t badQueue =
        client.ackMessage(GROUP, TOPIC, QUEUE_NUM + 90, ck1, off1, 3000, brokerName, addr);
    check("S6a illegal queueId rejected (MESSAGE_ILLEGAL)", badQueue == ResponseCode::MESSAGE_ILLEGAL,
          "code=" + std::to_string(badQueue));
    int32_t badOffset =
        client.ackMessage(GROUP, TOPIC, qid1, ck1, (static_cast<int64_t>(1) << 40), 3000,
                          brokerName, addr);
    check("S6b out-of-range offset rejected (NO_MESSAGE)", badOffset == ResponseCode::NO_MESSAGE,
          "code=" + std::to_string(badOffset));

    // ------------------------------------------------ S7 single-queue POP
    std::printf("=== S7 single-queue POP ===\n");
    // Use a fresh consumer group: the old group already has a pop offset on the same topic, so re-POPping returns nothing
    PopResult res7 = client.popMessage(GROUP_SINGLE, TOPIC, 0, 32, 60000, 0, 0, "", "", false,
                                       10000, brokerName, addr);
    bool onlyQ0 = true;
    std::set<int32_t> qids;
    for (const MessageExt& m : res7.msgFoundList) {
        qids.insert(m.queueId);
        if (m.queueId != 0) {
            onlyQ0 = false;
        }
    }
    check("S7 single-queue POP retrieved messages from queue 0",
          res7.isFound() && !res7.msgFoundList.empty() && onlyQ0,
          "status=" + std::string(popStatusName(res7.status))
              + " count=" + std::to_string(res7.msgFoundList.size()));

    // ------------------------------------------------ S8 no ack revives
    std::printf("=== S8 no ack -> revival and redelivery (at-least-once semantics) ===\n");
    for (int32_t i = 0; i < REVIVE_N_MSG; ++i) {
        Message m(TOPIC_REVIVE, "pop-revive-cpp-" + std::to_string(i));
        m.setKeys("rv" + std::to_string(i));
        producer.send(m);
    }
    auto routeRv = client.getTopicRouteData(TOPIC_REVIVE);
    std::string addrRv;
    if (routeRv != nullptr && !routeRv->brokerDatas.empty()) {
        addrRv = MQClientInstance::findBrokerAddrInRoute(*routeRv,
                                                         routeRv->brokerDatas.front().brokerName);
    }
    PopResult res8a = client.popMessage(GROUP_REVIVE, TOPIC_REVIVE, -1, 32, INVISIBLE_SHORT, 0, 0,
                                        "", "", false, 10000, std::string(), addrRv);
    std::set<std::string> firstKeys;
    for (const MessageExt& m : res8a.msgFoundList) {
        firstKeys.insert(uniqKeyOf(m));
    }
    check("S8a first POP round retrieved messages (left un-acked)",
          res8a.isFound() && !res8a.msgFoundList.empty(),
          "count=" + std::to_string(res8a.msgFoundList.size()));

    std::printf("  waiting %d s for the broker to revive them...\n", REVIVE_WAIT_SEC);
    std::this_thread::sleep_for(std::chrono::seconds(REVIVE_WAIT_SEC));

    PopResult res8b = client.popMessage(GROUP_REVIVE, TOPIC_REVIVE, -1, 32, 60000, 0, 0, "", "",
                                        false, 10000, std::string(), addrRv);
    int32_t revived = 0;
    std::set<std::string> retryFlags;
    for (const MessageExt& m : res8b.msgFoundList) {
        if (firstKeys.find(uniqKeyOf(m)) == firstKeys.end()) {
            continue;
        }
        ++revived;
        std::string ck = popCkOf(m);
        if (!ck.empty()) {
            retryFlags.insert(extra_info::getRetry(extra_info::split(ck)));
        }
    }
    check("S8b un-acked messages were revived and redelivered", revived > 0,
          "revived=" + std::to_string(revived)
              + " total=" + std::to_string(res8b.msgFoundList.size()));
    check("S8c revived messages carry POP_CK retryFlag=1 (from %RETRY%<group>_<topic>)",
          retryFlags.count("1") > 0,
          "retryFlagsSize=" + std::to_string(retryFlags.size()));

    producer.shutdown();
    std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
