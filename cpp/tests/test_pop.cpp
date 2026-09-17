// POP 模式（5.x 轻量消费）单测 —— 不需要集群。
//
// 覆盖三块最容易出错、真机上又最难定位的契约：
// 1. extra_info（CK 串）编解码：**空格**做段分隔、复刻 Java String.split 的
//    "丢弃末尾空串"语义、段数校验、重复 key 拒绝、retryFlag 判定；
// 2. 请求头的 extFields **键名逐字等于 Java 字段名** —— broker 用 fastjson2 按 Java
//    属性名反序列化，错一个字母就**静默丢字段**，所以这里当回归守卫逐键断言；
// 3. POP_CK 反构：普通 topic 直连 POP 时 broker **不写** POP_CK，必须由客户端用
//    startOffsetInfo/msgOffsetInfo 拼出 8 段 CK 串，否则 ACK 无从下手。
#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/remoting/protocol/extra_info.h"
#include "rocketmq/remoting/protocol/headers.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("  [FAIL] %s%s%s\n", name.c_str(), detail.empty() ? "" : "  ",
                    detail.c_str());
    }
}

template <typename Fn>
bool throwsInvalidArgument(Fn fn) {
    try {
        fn();
    } catch (const std::invalid_argument&) {
        return true;
    } catch (...) {
        return false;
    }
    return false;
}

MessageExt makeMsg(const std::string& topic, int32_t queueId, int64_t queueOffset) {
    MessageExt m;
    m.topic = topic;
    m.queueId = queueId;
    m.queueOffset = queueOffset;
    m.body = "body";
    return m;
}

PopMessageResponseHeader makeHeader(const std::string& startInfo, const std::string& msgInfo) {
    PopMessageResponseHeader h;
    h.popTime = static_cast<int64_t>(1789613086027LL);
    h.invisibleTime = static_cast<int64_t>(60000);
    h.reviveQid = 0;
    h.startOffsetInfo = startInfo;
    h.msgOffsetInfo = msgInfo;
    return h;
}

}  // namespace

int main() {
    const std::string TOPIC = "PopUnitTestTopic";
    const std::string BROKER = "broker-a";
    const std::string GROUP = "PopUnitTestGroup";

    // ------------------------------------------------ 1. extra_info 拼装/切分
    {
        std::string ck = extra_info::buildExtraInfo(0, 1789613086027LL, 60000, 0, TOPIC, BROKER, 3, 7);
        expect(ck == "0 1789613086027 60000 0 0 broker-a 3 7", "build 8 segments", ck);
        expect(extra_info::split(ck).size() == 8, "split into 8");

        std::string ck7 = extra_info::buildExtraInfo(0, 1, 2, 3, TOPIC, BROKER, 4);
        expect(ck7 == "0 1 2 3 0 broker-a 4", "build 7 segments", ck7);
        expect(extra_info::split(ck7).size() == 7, "split into 7");
    }

    // Java String.split 丢弃末尾空串；C++ 手写实现必须复刻，否则段数校验会不一致
    {
        std::vector<std::string> a = extra_info::split("1 2 3 ");
        expect(a.size() == 3, "split drops trailing empty (1 space)", std::to_string(a.size()));
        std::vector<std::string> b = extra_info::split("1 2 3  ");
        expect(b.size() == 3, "split drops trailing empty (2 spaces)", std::to_string(b.size()));
    }

    // 取值往返
    {
        std::string ck = extra_info::buildExtraInfo(11, 22, 33, 0, TOPIC, BROKER, 5, 123456789);
        std::vector<std::string> seg = extra_info::split(ck);
        expect(extra_info::getCkQueueOffset(seg) == 11, "getCkQueueOffset");
        expect(extra_info::getPopTime(seg) == 22, "getPopTime");
        expect(extra_info::getInvisibleTime(seg) == 33, "getInvisibleTime");
        expect(extra_info::getReviveQid(seg) == 0, "getReviveQid");
        expect(extra_info::getRetry(seg) == extra_info::kNormalTopic, "getRetry normal");
        expect(extra_info::getBrokerName(seg) == BROKER, "getBrokerName");
        expect(extra_info::getQueueId(seg) == 5, "getQueueId");
        expect(extra_info::getQueueOffset(seg) == 123456789, "getQueueOffset");
    }

    // 段数守卫
    {
        std::vector<std::string> seg7 = extra_info::split("0 1 2 3 0 broker-a 4");
        expect(throwsInvalidArgument([&] { extra_info::getQueueOffset(seg7); }),
               "getQueueOffset needs 8 segments");
        std::vector<std::string> seg6 = {"0", "1", "2", "3", "0", "b"};
        expect(throwsInvalidArgument([&] { extra_info::getQueueId(seg6); }),
               "getQueueId needs 7 segments");
        expect(throwsInvalidArgument([] { extra_info::getCkQueueOffset({}); }),
               "getCkQueueOffset needs 1 segment");
    }

    // isOrder / retry topic
    {
        expect(extra_info::isOrder(
                   extra_info::split(extra_info::buildExtraInfo(0, 1, 2, 999, TOPIC, BROKER, 0))),
               "isOrder on reviveQid 999");
        expect(!extra_info::isOrder(
                   extra_info::split(extra_info::buildExtraInfo(0, 1, 2, 0, TOPIC, BROKER, 0))),
               "isOrder false on reviveQid 0");

        expect(extra_info::retryOfTopic(TOPIC) == extra_info::kNormalTopic, "retryOfTopic normal");
        expect(extra_info::retryOfTopic(extra_info::buildPopRetryTopicV1(TOPIC, GROUP))
                   == extra_info::kRetryTopic,
               "retryOfTopic V1");
        // V2 也带 %RETRY% 前缀，判定顺序错了会被误判成 V1
        expect(extra_info::retryOfTopic(extra_info::buildPopRetryTopicV2(TOPIC, GROUP))
                   == extra_info::kRetryTopicV2,
               "retryOfTopic V2 (checked before prefix)");

        expect(extra_info::buildPopRetryTopicV1(TOPIC, GROUP) == "%RETRY%" + GROUP + "_" + TOPIC,
               "buildPopRetryTopicV1");
        expect(extra_info::buildPopRetryTopicV2(TOPIC, GROUP) == "%RETRY%" + GROUP + "+" + TOPIC,
               "buildPopRetryTopicV2");
        // 默认（enableRetryTopicV2 关）走 V1
        expect(extra_info::buildPopRetryTopic(TOPIC, GROUP)
                   == extra_info::buildPopRetryTopicV1(TOPIC, GROUP),
               "buildPopRetryTopic defaults to V1");

        expect(extra_info::getRetry(extra_info::split(extra_info::buildExtraInfo(
                   0, 1, 2, 0, extra_info::buildPopRetryTopicV1(TOPIC, GROUP), BROKER, 0)))
                   == extra_info::kRetryTopic,
               "buildExtraInfo marks retry topic");

        expect(extra_info::getRealTopic(TOPIC, GROUP, "0") == TOPIC, "getRealTopic normal");
        expect(extra_info::getRealTopic(TOPIC, GROUP, "1")
                   == extra_info::buildPopRetryTopicV1(TOPIC, GROUP),
               "getRealTopic V1");
        expect(extra_info::getRealTopic(TOPIC, GROUP, "2")
                   == extra_info::buildPopRetryTopicV2(TOPIC, GROUP),
               "getRealTopic V2");
        expect(throwsInvalidArgument([&] { extra_info::getRealTopic(TOPIC, GROUP, "9"); }),
               "getRealTopic rejects bad retry");
    }

    // ------------------------------------------------ 解析响应头编码
    {
        auto start = extra_info::parseStartOffsetInfo("0 0 0;0 3 0;0 2 0");
        expect(start.has_value() && start->size() == 3, "parseStartOffsetInfo size");
        expect(start.has_value() && (*start)["0@0"] == 0 && (*start)["0@3"] == 0
                   && (*start)["0@2"] == 0,
               "parseStartOffsetInfo values");

        auto single = extra_info::parseStartOffsetInfo("0 3 5");
        expect(single.has_value() && (*single)["0@3"] == 5, "parseStartOffsetInfo single queue");

        expect(!extra_info::parseStartOffsetInfo("").has_value(), "empty startOffsetInfo -> nullopt");
        expect(!extra_info::parseMsgOffsetInfo("").has_value(), "empty msgOffsetInfo -> nullopt");

        auto msg = extra_info::parseMsgOffsetInfo("0 3 0,1,2;0 2 7");
        expect(msg.has_value() && (*msg)["0@3"].size() == 3 && (*msg)["0@3"][2] == 2
                   && (*msg)["0@2"][0] == 7,
               "parseMsgOffsetInfo values");

        auto order = extra_info::parseOrderCountInfo("0 3 5");
        expect(order.has_value() && (*order)["0@3"] == 5, "parseOrderCountInfo");

        expect(throwsInvalidArgument([] { extra_info::parseStartOffsetInfo("0 3"); }),
               "rejects 2-field segment");
        expect(throwsInvalidArgument([] { extra_info::parseStartOffsetInfo("0 3 0 9"); }),
               "rejects 4-field segment");
        expect(throwsInvalidArgument([] { extra_info::parseStartOffsetInfo("0 3 0;0 3 1"); }),
               "rejects duplicate key");

        auto retrySeg = extra_info::parseStartOffsetInfo("1 3 0");
        expect(retrySeg.has_value() && (*retrySeg)["1@3"] == 0, "retry queue key");

        expect(extra_info::getStartOffsetInfoMapKey(TOPIC, 3) == "0@3", "startOffset map key");
        expect(extra_info::getQueueOffsetKeyValueKey(3, 7) == "qo3%7", "queueOffset kv key");
        expect(extra_info::getQueueOffsetMapKey(TOPIC, 3, 7) == "0@qo3%7", "queueOffset map key");
        expect(extra_info::getStartOffsetInfoMapKey(
                   extra_info::buildPopRetryTopicV1(TOPIC, GROUP), 3) == "1@3",
               "retry topic map key carries retryFlag");
    }

    // ------------------------------------------------ 2. 请求/响应头 extFields 键名
    {
        PopMessageRequestHeader h;
        h.consumerGroup = GROUP;
        h.topic = TOPIC;
        h.queueId = -1;
        h.maxMsgNums = 32;
        h.invisibleTime = static_cast<int64_t>(60000);
        h.pollTime = 0;
        h.bornTime = static_cast<int64_t>(1789613086027LL);
        h.initMode = 0;
        h.expType = "TAG";
        h.exp = "*";
        h.attemptId = "attempt-1";
        PropertyMap ext = h.toExtFields();
        const char* expected[] = {"consumerGroup", "topic",    "queueId",   "maxMsgNums",
                                  "invisibleTime", "pollTime", "bornTime",  "initMode",
                                  "expType",       "exp",      "order",     "attemptId"};
        expect(ext.size() == 12, "PopMessageRequestHeader key count", std::to_string(ext.size()));
        for (const char* k : expected) {
            expect(ext.find(k) != ext.end(), std::string("PopMessageRequestHeader has ") + k);
        }
        expect(ext["queueId"] == "-1", "queueId serialized");
        expect(ext["bornTime"] == "1789613086027", "bornTime serialized");
        // Java 侧是 Boolean order = Boolean.FALSE（非 null），总是写出且小写
        expect(ext["order"] == "false", "order always serialized lowercase", ext["order"]);

        PopMessageRequestHeader minimal;
        minimal.topic = TOPIC;
        PropertyMap ext2 = minimal.toExtFields();
        expect(ext2.find("consumerGroup") == ext2.end(), "unset optional absent");
        expect(ext2.find("attemptId") == ext2.end(), "unset attemptId absent");
        expect(ext2.find("order") != ext2.end(), "order still present when unset");
    }

    {
        PopMessageResponseHeader h;
        h.fromExtFields({{"popTime", "1789613086027"},
                         {"invisibleTime", "60000"},
                         {"reviveQid", "0"},
                         {"restNum", "0"},
                         {"startOffsetInfo", "0 0 0;0 3 0;0 2 0"},
                         {"msgOffsetInfo", "0 0 0;0 3 0;0 2 0"}});
        expect(h.popTime.value_or(-1) == 1789613086027LL, "PopMessageResponseHeader popTime");
        expect(h.invisibleTime.value_or(-1) == 60000, "PopMessageResponseHeader invisibleTime");
        expect(h.startOffsetInfo.value_or("") == "0 0 0;0 3 0;0 2 0",
               "PopMessageResponseHeader startOffsetInfo");

        PopMessageResponseHeader empty;
        empty.fromExtFields({});
        expect(!empty.popTime.has_value(), "missing popTime -> nullopt");
        expect(!empty.startOffsetInfo.has_value(), "missing startOffsetInfo -> nullopt");
    }

    {
        AckMessageRequestHeader h;
        h.consumerGroup = GROUP;
        h.topic = TOPIC;
        h.queueId = 3;
        h.extraInfo = "ck";
        h.offset = 7;
        PropertyMap ext = h.toExtFields();
        expect(ext.size() == 5, "AckMessageRequestHeader key count", std::to_string(ext.size()));
        expect(ext["extraInfo"] == "ck" && ext["offset"] == "7" && ext["queueId"] == "3",
               "Ack key names/values");
    }

    {
        ChangeInvisibleTimeRequestHeader h;
        h.consumerGroup = GROUP;
        h.topic = TOPIC;
        h.queueId = 3;
        h.extraInfo = "ck";
        h.offset = 7;
        h.invisibleTime = static_cast<int64_t>(20000);
        PropertyMap ext = h.toExtFields();
        const char* expected[] = {"consumerGroup", "topic",       "queueId", "extraInfo",
                                  "offset",        "invisibleTime", "suspend"};
        expect(ext.size() == 7, "ChangeInvisibleTime key count", std::to_string(ext.size()));
        for (const char* k : expected) {
            expect(ext.find(k) != ext.end(), std::string("ChangeInvisibleTime has ") + k);
        }
        // Java 是 primitive boolean，总是出现且小写
        expect(ext["suspend"] == "false", "suspend always serialized lowercase", ext["suspend"]);

        ChangeInvisibleTimeResponseHeader r;
        r.fromExtFields({{"popTime", "111"}, {"invisibleTime", "222"}, {"reviveQid", "3"}});
        expect(r.popTime.value_or(0) == 111 && r.invisibleTime.value_or(0) == 222
                   && r.reviveQid.value_or(0) == 3,
               "ChangeInvisibleTimeResponseHeader parse");
    }

    // ------------------------------------------------ 3. POP_CK 反构
    {
        std::vector<MessageExt> msgs = {makeMsg(TOPIC, 0, 0), makeMsg(TOPIC, 3, 0),
                                        makeMsg(TOPIC, 2, 0)};
        MQClientInstance::stampPopCk(msgs, BROKER, makeHeader("0 0 0;0 3 0;0 2 0",
                                                             "0 0 5;0 3 6;0 2 7"));
        bool all8 = true;
        bool match = true;
        bool rightOffset = true;
        for (const MessageExt& m : msgs) {
            auto it = m.properties.find(MessageConst::PROPERTY_POP_CK);
            if (it == m.properties.end()) {
                all8 = false;
                continue;
            }
            std::vector<std::string> seg = extra_info::split(it->second);
            if (seg.size() != 8) {
                all8 = false;
                continue;
            }
            if (extra_info::getBrokerName(seg) != BROKER
                || extra_info::getQueueId(seg) != m.queueId) {
                match = false;
            }
            int64_t want = (m.queueId == 0) ? 5 : ((m.queueId == 3) ? 6 : 7);
            if (extra_info::getQueueOffset(seg) != want) {
                rightOffset = false;
            }
        }
        expect(all8, "POP_CK is 8 segments");
        expect(match, "POP_CK brokerName/queueId match the message");
        expect(rightOffset, "POP_CK msgQueueOffset comes from msgOffsetInfo");
    }

    // startOffset 取自 startOffsetInfo
    {
        std::vector<MessageExt> msgs = {makeMsg(TOPIC, 3, 0)};
        MQClientInstance::stampPopCk(msgs, BROKER, makeHeader("0 3 99", "0 3 0"));
        std::vector<std::string> seg = extra_info::split(msgs[0].properties[MessageConst::PROPERTY_POP_CK]);
        expect(extra_info::getCkQueueOffset(seg) == 99, "ckQueueOffset from startOffsetInfo");
    }

    // retry 消息已带 POP_CK：绝不能覆盖
    {
        const std::string existing = "1 1 2 0 1 broker-a 0 0";
        std::vector<MessageExt> msgs = {makeMsg(TOPIC, 3, 0)};
        msgs[0].properties[MessageConst::PROPERTY_POP_CK] = existing;
        MQClientInstance::stampPopCk(msgs, BROKER, makeHeader("0 3 0", "0 3 6"));
        expect(msgs[0].properties[MessageConst::PROPERTY_POP_CK] == existing,
               "does not override existing POP_CK");
    }

    // 同队列多条：按 queueOffset 排序后的下标去取 msgOffsetInfo
    {
        std::vector<MessageExt> msgs = {makeMsg(TOPIC, 3, 10), makeMsg(TOPIC, 3, 11),
                                        makeMsg(TOPIC, 3, 12)};
        MQClientInstance::stampPopCk(msgs, BROKER, makeHeader("0 3 0", "0 3 10,11,12"));
        bool ok = true;
        int64_t want[] = {10, 11, 12};
        for (std::size_t i = 0; i < msgs.size(); ++i) {
            std::vector<std::string> seg =
                extra_info::split(msgs[i].properties[MessageConst::PROPERTY_POP_CK]);
            if (extra_info::getQueueOffset(seg) != want[i]) {
                ok = false;
            }
        }
        expect(ok, "index selects right offset within queue");
    }

    // startOffsetInfo 缺失时的降级分支
    {
        std::vector<MessageExt> msgs = {makeMsg(TOPIC, 3, 42)};
        MQClientInstance::stampPopCk(msgs, BROKER, makeHeader("", ""));
        std::vector<std::string> seg =
            extra_info::split(msgs[0].properties[MessageConst::PROPERTY_POP_CK]);
        expect(seg.size() == 8, "fallback branch still produces 8 segments");
        expect(extra_info::getCkQueueOffset(seg) == 42
                   && extra_info::getQueueOffset(seg) == 42,
               "fallback uses own queueOffset for both");
    }

    // 1ST_POP_TIME 只在缺失时补
    {
        std::vector<MessageExt> msgs = {makeMsg(TOPIC, 0, 0), makeMsg(TOPIC, 3, 0)};
        msgs[1].properties[MessageConst::PROPERTY_FIRST_POP_TIME] = "111";
        MQClientInstance::stampPopCk(msgs, BROKER, makeHeader("0 0 0;0 3 0", "0 0 0;0 3 0"));
        expect(msgs[0].properties[MessageConst::PROPERTY_FIRST_POP_TIME] == "1789613086027",
               "1ST_POP_TIME filled when missing");
        expect(msgs[1].properties[MessageConst::PROPERTY_FIRST_POP_TIME] == "111",
               "1ST_POP_TIME kept when present");
    }

    // 状态名
    {
        expect(std::string(popStatusName(PopStatus::FOUND)) == "FOUND", "popStatusName FOUND");
        expect(std::string(popStatusName(PopStatus::POLLING_NOT_FOUND)) == "POLLING_NOT_FOUND",
               "popStatusName POLLING_NOT_FOUND");
    }

    std::printf("checks=%d fails=%d\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
