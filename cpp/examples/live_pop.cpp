// POP 模式（5.x 轻量消费）真机验证。
// 用法：rmq_live_pop 127.0.0.1:9876
//
// 与 Python verify_pop_live.py 完全同场景（三语言对拍用同一套断言）。
//
// POP 与 pull 的语义差异是断言的重心：
//   - **不提交位点**，靠 ack 确认；不 ack 的消息在 invisibleTime 后被复活重投；
//   - broker 在普通 topic 消息上**不写** POP_CK，客户端必须自己反构 8 段 CK 串。
//
// 场景：
//   S1 建 topic(8 队列) + 发 10 条
//   S2 多队列 POP（queueId=-1, initMode=0）→ FOUND 且拿到消息
//   S3 每条消息都被盖上 **8 段** POP_CK，且 brokerName/queueId 与消息实际一致
//   S4 ack 一条 → SUCCESS
//   S5 changeInvisibleTime → SUCCESS 且返回**新的** 8 段 extraInfo
//   S6 校验真的生效：非法 queueId → MESSAGE_ILLEGAL；越界 offset → NO_MESSAGE
//   S7 单队列 POP（新消费组 + queueId=0）→ FOUND 且消息 queueId 全为 0
//   S8 不 ack 会复活：小 invisibleTime POP 后不 ack，等待后重 POP 能拿到同一条消息，
//      且它的 POP_CK 是 retryFlag=1（来自 %RETRY%<group>_<topic>）—— 至少一次语义
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

// CK 串取自消息属性；没有则返回空串（说明反构失败）
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

    // ------------------------------------------------ S1 建 topic + 发消息
    std::printf("=== S1 准备 topic 与消息 ===\n");
    producer.createTopic(MixAll::DEFAULT_TOPIC, TOPIC, QUEUE_NUM);
    producer.createTopic(MixAll::DEFAULT_TOPIC, TOPIC_REVIVE, REVIVE_QUEUE_NUM);
    for (int32_t i = 0; i < N_MSG; ++i) {
        Message m(TOPIC, "pop-live-cpp-" + std::to_string(i));
        m.setKeys("pk" + std::to_string(i));
        producer.send(m);
    }
    std::printf("  sent %d msgs to %s\n", N_MSG, TOPIC.c_str());

    auto route = client.getTopicRouteData(TOPIC);
    check("S1 topic 路由可用", route != nullptr && !route->brokerDatas.empty());
    if (route == nullptr || route->brokerDatas.empty()) {
        std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
        producer.shutdown();
        return 1;
    }
    const std::string brokerName = route->brokerDatas.front().brokerName;
    const std::string addr =
        MQClientInstance::findBrokerAddrInRoute(*route, brokerName);
    std::printf("  brokerName=%s addr=%s\n", brokerName.c_str(), addr.c_str());

    // ------------------------------------------------ S2 多队列 POP
    std::printf("=== S2 多队列 POP（addr 走自动解析路径）===\n");
    PopResult res = client.popMessage(GROUP, TOPIC, -1, 32, 60000, 0, 0);
    check("S2 POP 取到消息", res.isFound() && !res.msgFoundList.empty(),
          "status=" + std::string(popStatusName(res.status))
              + " count=" + std::to_string(res.msgFoundList.size())
              + " startOffsetInfo=" + res.startOffsetInfo);
    if (res.msgFoundList.empty()) {
        std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
        producer.shutdown();
        return 1;
    }

    // ------------------------------------------------ S3 POP_CK 反构
    std::printf("=== S3 POP_CK 反构（8 段）===\n");
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
    check("S3a 每条消息都有 8 段 POP_CK", all8, "sample='" + sample + "'");
    check("S3b CK 的 brokerName/queueId 与消息一致", match);
    check("S3c 1ST_POP_TIME 已补", hasFirstPopTime);

    // ------------------------------------------------ S4 ack
    std::printf("=== S4 ACK ===\n");
    const MessageExt& first = res.msgFoundList.front();
    const std::string ck1 = popCkOf(first);
    std::vector<std::string> seg1 = extra_info::split(ck1);
    const int32_t qid1 = extra_info::getQueueId(seg1);
    const int64_t off1 = extra_info::getQueueOffset(seg1);
    int32_t ackCode = client.ackMessage(GROUP, TOPIC, qid1, ck1, off1, 3000, brokerName, addr);
    check("S4 ack 返回 SUCCESS", ackCode == ResponseCode::SUCCESS,
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
    check("S5a 延长不可见时间成功", res5.success(),
          "code=" + std::to_string(res5.responseCode) + " popTime=" + std::to_string(res5.popTime)
              + " invisibleTime=" + std::to_string(res5.invisibleTime));
    std::vector<std::string> newSeg =
        res5.extraInfo.empty() ? std::vector<std::string>() : extra_info::split(res5.extraInfo);
    check("S5b 返回新的 8 段 extraInfo 且用新值",
          newSeg.size() == 8 && extra_info::getInvisibleTime(newSeg) == res5.invisibleTime
              && extra_info::getPopTime(newSeg) == res5.popTime,
          "new='" + res5.extraInfo + "'");

    // ------------------------------------------------ S6 校验真的生效
    std::printf("=== S6 非法参数必须被拒绝 ===\n");
    int32_t badQueue =
        client.ackMessage(GROUP, TOPIC, QUEUE_NUM + 90, ck1, off1, 3000, brokerName, addr);
    check("S6a 非法 queueId 被拒（MESSAGE_ILLEGAL）", badQueue == ResponseCode::MESSAGE_ILLEGAL,
          "code=" + std::to_string(badQueue));
    int32_t badOffset =
        client.ackMessage(GROUP, TOPIC, qid1, ck1, (static_cast<int64_t>(1) << 40), 3000,
                          brokerName, addr);
    check("S6b 越界 offset 被拒（NO_MESSAGE）", badOffset == ResponseCode::NO_MESSAGE,
          "code=" + std::to_string(badOffset));

    // ------------------------------------------------ S7 单队列 POP
    std::printf("=== S7 单队列 POP ===\n");
    // 用新消费组：老组在同一 topic 上已有 pop 位点，重复 POP 拿不到东西
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
    check("S7 单队列 POP 取到队列 0 的消息",
          res7.isFound() && !res7.msgFoundList.empty() && onlyQ0,
          "status=" + std::string(popStatusName(res7.status))
              + " count=" + std::to_string(res7.msgFoundList.size()));

    // ------------------------------------------------ S8 不 ack 会复活
    std::printf("=== S8 不 ack → 复活重投（至少一次语义）===\n");
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
    check("S8a 首轮 POP 取到消息（不 ack）",
          res8a.isFound() && !res8a.msgFoundList.empty(),
          "count=" + std::to_string(res8a.msgFoundList.size()));

    std::printf("  等待 %d s 让 broker 复活...\n", REVIVE_WAIT_SEC);
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
    check("S8b 未 ack 的消息被复活重投", revived > 0,
          "revived=" + std::to_string(revived)
              + " total=" + std::to_string(res8b.msgFoundList.size()));
    check("S8c 复活消息的 POP_CK retryFlag=1（来自 %RETRY%<group>_<topic>）",
          retryFlags.count("1") > 0,
          "retryFlagsSize=" + std::to_string(retryFlags.size()));

    producer.shutdown();
    std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
