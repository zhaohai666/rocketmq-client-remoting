// 路由 / 心跳 / 订阅数据的编解码往返单测。
//
// 重点不是"能跑"，而是**字段名与 Java 一致**——broker 用 fastjson2 按属性名反序列化，
// 字段名错一个（比如 Python 侧曾用 snake_case）就会静默丢字段。
// 因此这里既做往返，也做"精确 JSON 文本"和"不得出现某字段"的守卫。
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "rocketmq/common/subscription_data.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/protocol/serialize.h"

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

static bool contains(const std::string& hay, const std::string& needle) {
    return hay.find(needle) != std::string::npos;
}

// ---------------------------------------------------------------- SubscriptionData
static void testSubscriptionData() {
    // FilterAPI: "*" / 空 / 全空白 -> {"*"}
    SubscriptionData all1 = FilterAPI::buildSubscriptionData("T", "*");
    CHECK(all1.tagsSet.size() == 1 && all1.tagsSet.count("*") == 1, "filter '*' -> tagsSet={*}");
    SubscriptionData all2 = FilterAPI::buildSubscriptionData("T", "");
    CHECK(all2.tagsSet.size() == 1 && all2.tagsSet.count("*") == 1, "filter empty -> tagsSet={*}");
    SubscriptionData all3 = FilterAPI::buildSubscriptionData("T", "   ");
    CHECK(all3.tagsSet.size() == 1 && all3.tagsSet.count("*") == 1, "filter blank -> tagsSet={*}");

    // "||" 切分 + trim + 丢空片段
    SubscriptionData multi = FilterAPI::buildSubscriptionData("T", " TagA || TagB || ");
    CHECK(multi.tagsSet.size() == 2, "filter split count");
    CHECK(multi.tagsSet.count("TagA") == 1 && multi.tagsSet.count("TagB") == 1, "filter split trim");

    // toJson 必须包含 Java 字段名，且**不得**出现 filterClassSource
    SubscriptionData sd("TopicProbe", "TagA||TagB");
    sd.setTagsSet({"TagA", "TagB"});
    sd.setCodeSet({11, 22});
    sd.setClassFilterMode(false);
    sd.setSubVersion(1700000000000LL);
    sd.setFilterClassSource("SHOULD_NOT_APPEAR");
    const std::string json = sd.toJson().dump();
    CHECK(contains(json, "\"classFilterMode\""), "sub json classFilterMode key");
    CHECK(contains(json, "\"subString\":\"TagA||TagB\""), "sub json subString");
    CHECK(contains(json, "\"subVersion\":1700000000000"), "sub json subVersion (not double)");
    CHECK(contains(json, "\"expressionType\":\"TAG\""), "sub json expressionType");
    CHECK(contains(json, "\"topic\":\"TopicProbe\""), "sub json topic");
    CHECK(!contains(json, "filterClassSource"),
          "sub json must NOT contain filterClassSource (@JSONField serialize=false)");
    CHECK(!contains(json, "class_filter_mode"), "sub json must NOT use snake_case");

    // JSON 往返
    SubscriptionData back = SubscriptionData::fromJson(sd.toJson());
    CHECK(back.topic == sd.topic && back.subString == sd.subString, "sub round-trip topic/subString");
    CHECK(back.tagsSet == sd.tagsSet && back.codeSet == sd.codeSet, "sub round-trip sets");
    CHECK(back.subVersion == sd.subVersion, "sub round-trip subVersion");
    CHECK(back.expressionType == sd.expressionType, "sub round-trip expressionType");
    CHECK(back == sd, "sub operator== after round-trip");
    // filterClassSource 不参与序列化，故往返后丢失（与 Java 行为一致）
    CHECK(back.filterClassSource.empty(), "sub filterClassSource not serialized");

    // Java equals 含 subVersion
    SubscriptionData a("T", "x");
    a.setSubVersion(1);
    SubscriptionData b("T", "x");
    b.setSubVersion(2);
    CHECK(a != b, "sub equals includes subVersion (Java semantics)");

    // compareTo: topic@subString
    SubscriptionData c1("A", "z");
    SubscriptionData c2("B", "a");
    CHECK(c1.compareTo(c2) < 0, "sub compareTo by topic@subString");
}

// ---------------------------------------------------------------- QueueData
static void testQueueData() {
    QueueData q("broker-a", 4, 8, 6, 0);
    const std::string json = q.toJson().dump();
    CHECK(contains(json, "\"brokerName\":\"broker-a\""), "queue json brokerName");
    CHECK(contains(json, "\"readQueueNums\":4"), "queue json readQueueNums");
    CHECK(contains(json, "\"writeQueueNums\":8"), "queue json writeQueueNums");
    CHECK(contains(json, "\"perm\":6"), "queue json perm");
    CHECK(contains(json, "\"topicSysFlag\":0"), "queue json topicSysFlag");

    QueueData back = QueueData::fromJson(q.toJson());
    CHECK(back == q, "queue round-trip");
    CHECK(QueueData("a", 1, 1, 1, 0).compareTo(QueueData("b", 1, 1, 1, 0)) < 0,
          "queue compareTo by brokerName only");
}

// ---------------------------------------------------------------- BrokerData
static void testBrokerData() {
    std::map<int64_t, std::string> addrs;
    addrs[0] = "127.0.0.1:10911";
    addrs[1] = "127.0.0.1:10912";
    BrokerData b("DefaultCluster", "broker-a", addrs, "zone-x", false);

    const std::string json = b.toJson().dump();
    CHECK(contains(json, "\"brokerAddrs\""), "broker json brokerAddrs");
    CHECK(contains(json, "\"cluster\":\"DefaultCluster\""), "broker json cluster");
    CHECK(contains(json, "\"enableActingMaster\":false"), "broker json enableActingMaster");
    CHECK(contains(json, "\"zoneName\":\"zone-x\""), "broker json zoneName");

    BrokerData back = BrokerData::fromJson(b.toJson());
    CHECK(back == b, "broker round-trip");

    // master(0) 优先
    CHECK(b.selectBrokerAddr() == "127.0.0.1:10911", "broker select prefers master(id=0)");

    // 只有从节点时也不崩，返回其中之一
    std::map<int64_t, std::string> slaves;
    slaves[1] = "127.0.0.1:10912";
    BrokerData onlySlave("c", "b", slaves, "", false);
    CHECK(onlySlave.selectBrokerAddr() == "127.0.0.1:10912", "broker select falls back to slave");
    BrokerData noAddr("c", "b", {}, "", false);
    CHECK(noAddr.selectBrokerAddr().empty(), "broker select empty when no addrs");

    // 真实 nameserver 会回 fastjson 的裸数字键 {0:"..."}，必须能解析
    JsonValue v;
    CHECK(jsonParse("{\"brokerAddrs\":{0:\"127.0.0.1:10911\",1:\"127.0.0.1:10912\"},"
                    "\"brokerName\":\"b\",\"cluster\":\"c\"}",
                    v),
          "broker parse bare-numeric-key json");
    BrokerData fromBare = BrokerData::fromJson(v);
    CHECK(fromBare.brokerAddrs.size() == 2, "broker bare keys count");
    CHECK(fromBare.brokerAddrs[0] == "127.0.0.1:10911", "broker bare key 0");
    CHECK(fromBare.selectBrokerAddr() == "127.0.0.1:10911", "broker bare key select master");
}

// ---------------------------------------------------------------- TopicRouteData
static void testTopicRouteData() {
    TopicRouteData t;
    t.setOrderTopicConf("");
    // broker-a: 可读写(perm=6)；broker-b: 只读(perm=4) -> 不应进可写队列
    t.queueDatas.push_back(QueueData("broker-a", 4, 4, PermName::PERM_READ | PermName::PERM_WRITE, 0));
    t.queueDatas.push_back(QueueData("broker-b", 2, 2, PermName::PERM_READ, 0));
    std::map<int64_t, std::string> addrs;
    addrs[0] = "127.0.0.1:10911";
    t.brokerDatas.push_back(BrokerData("DefaultCluster", "broker-a", addrs));
    t.brokerDatas.push_back(BrokerData("DefaultCluster", "broker-b", addrs));

    // 只挑可写且有对应 broker 的队列
    std::vector<MessageQueue> mqs = t.getAllMessageQueue("TopicTest");
    CHECK(mqs.size() == 4, "route getAllMessageQueue only writable queues");
    CHECK(mqs[0].topic == "TopicTest" && mqs[0].brokerName == "broker-a" && mqs[0].queueId == 0,
          "route mq topic backfilled + broker");
    CHECK(mqs[3].queueId == 3, "route mq queueId sequence");

    // encode/decode 往返
    Bytes encoded = t.encode();
    TopicRouteData decoded;
    CHECK(TopicRouteData::decode(encoded, decoded), "route decode ok");
    CHECK(decoded.queueDatas.size() == 2 && decoded.brokerDatas.size() == 2, "route decode counts");
    CHECK(decoded.queueDatas[0] == t.queueDatas[0], "route decode queue equal");
    CHECK(decoded.brokerDatas[0] == t.brokerDatas[0], "route decode broker equal");

    // 变化检测
    CHECK(t.topicRouteDataChanged(nullptr), "route changed vs null");
    CHECK(!t.topicRouteDataChanged(&decoded), "route unchanged after round-trip");
    TopicRouteData other = decoded;
    other.queueDatas[0].writeQueueNums = 99;
    CHECK(t.topicRouteDataChanged(&other), "route changed on queue diff");

    // 真实样例（nameserver 响应形如 Java fastjson2 输出）
    TopicRouteData fromJava;
    CHECK(TopicRouteData::decode(
              "{\"brokerDatas\":[{\"brokerAddrs\":{0:\"127.0.0.1:10911\"},"
              "\"brokerName\":\"broker-a\",\"cluster\":\"DefaultCluster\","
              "\"enableActingMaster\":false,\"zoneName\":\"\"}],"
              "\"filterServerTable\":{},\"orderTopicConf\":\"\","
              "\"queueDatas\":[{\"brokerName\":\"broker-a\",\"perm\":6,\"readQueueNums\":4,"
              "\"topicSysFlag\":0,\"writeQueueNums\":4}]}",
              fromJava),
          "route decode java-shaped json");
    CHECK(fromJava.getAllMessageQueue("T").size() == 4, "route java-shaped writable queues");
    CHECK(fromJava.brokerDatas[0].selectBrokerAddr() == "127.0.0.1:10911",
          "route java-shaped select master");
}

// ---------------------------------------------------------------- HeartbeatData
static void testHeartbeatData() {
    // 关键：精确 JSON 文本必须与 Java fastjson2 输出逐字节一致（无订阅时集合有序）
    HeartbeatData hb("cid");
    hb.addProducerData(ProducerData("pg"));
    const std::string expected =
        "{\"clientID\":\"cid\",\"consumerDataSet\":[],\"heartbeatFingerprint\":0,"
        "\"producerDataSet\":[{\"groupName\":\"pg\"}],\"withoutSub\":false}";
    CHECK(hb.toJson().dump() == expected, "heartbeat exact json == fastjson2 output");
    CHECK(hb.toJson().dump() == expected, "heartbeat exact json stable");

    // ConsumerData 字段名守卫：本版本**没有** consumeTimestamp / maxReconsumeTimes
    ConsumerData cd("cg_probe", ConsumeType::CONSUME_PASSIVELY, MessageModel::CLUSTERING,
                    ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    cd.setUnitMode(false);
    SubscriptionData sd = FilterAPI::buildSubscriptionData("TopicProbe", "TagA||TagB");
    sd.setSubVersion(1700000000000LL);
    cd.addSubscriptionData(sd);
    const std::string cdJson = cd.toJson().dump();
    CHECK(contains(cdJson, "\"groupName\":\"cg_probe\""), "consumer json groupName");
    CHECK(contains(cdJson, "\"consumeType\":\"CONSUME_PASSIVELY\""), "consumer json consumeType");
    CHECK(contains(cdJson, "\"messageModel\":\"CLUSTERING\""), "consumer json messageModel");
    CHECK(contains(cdJson, "\"consumeFromWhere\":\"CONSUME_FROM_LAST_OFFSET\""),
          "consumer json consumeFromWhere");
    CHECK(contains(cdJson, "\"unitMode\":false"), "consumer json unitMode");
    CHECK(contains(cdJson, "\"subscriptionDataSet\":["), "consumer json subscriptionDataSet");
    CHECK(!contains(cdJson, "consumeTimestamp"),
          "consumer json must NOT contain consumeTimestamp (absent in this Java version)");
    CHECK(!contains(cdJson, "maxReconsumeTimes"),
          "consumer json must NOT contain maxReconsumeTimes (absent in this Java version)");

    // HeartbeatData 顶层字段守卫
    HeartbeatData full("10.0.0.1@12345");
    full.addProducerData(ProducerData("pg_probe"));
    full.addConsumerData(cd);
    const std::string fullJson = full.toJson().dump();
    CHECK(contains(fullJson, "\"clientID\":\"10.0.0.1@12345\""), "heartbeat json clientID");
    CHECK(contains(fullJson, "\"heartbeatFingerprint\":0"), "heartbeat json heartbeatFingerprint");
    CHECK(contains(fullJson, "\"withoutSub\":false"),
          "heartbeat json withoutSub (not isWithoutSub)");
    CHECK(!contains(fullJson, "isWithoutSub"), "heartbeat json must use withoutSub key");

    // encode/decode 往返
    Bytes enc = full.encode();
    HeartbeatData decoded;
    CHECK(HeartbeatData::decode(enc, decoded), "heartbeat decode ok");
    CHECK(decoded.clientID == full.clientID, "heartbeat round-trip clientID");
    CHECK(decoded.producerDataSet.size() == 1 &&
              decoded.producerDataSet[0].groupName == "pg_probe",
          "heartbeat round-trip producer");
    CHECK(decoded.consumerDataSet.size() == 1, "heartbeat round-trip consumer count");
    CHECK(decoded.consumerDataSet[0].groupName == "cg_probe", "heartbeat round-trip consumer group");
    CHECK(decoded.consumerDataSet[0].subscriptionDataSet.size() == 1,
          "heartbeat round-trip subscription count");
    CHECK(decoded.consumerDataSet[0].subscriptionDataSet[0].tagsSet == sd.tagsSet,
          "heartbeat round-trip tagsSet");
    CHECK(decoded.consumerDataSet[0] == cd, "heartbeat round-trip consumer equal");
    CHECK(decoded.heartbeatFingerprint == 0 && !decoded.withoutSub,
          "heartbeat round-trip defaults (V1 path)");

    // 兼容 Java 字段名 isWithoutSub
    HeartbeatData alt;
    CHECK(HeartbeatData::decode(
              "{\"clientID\":\"c\",\"isWithoutSub\":true,\"heartbeatFingerprint\":7,\"consumerDataSet\":[],\"producerDataSet\":[]}",
              alt),
          "heartbeat decode isWithoutSub alias ok");
    CHECK(alt.withoutSub && alt.heartbeatFingerprint == 7, "heartbeat isWithoutSub alias value");

    // 空 body 应解码失败而不是崩
    HeartbeatData dummy;
    CHECK(!HeartbeatData::decode("", dummy), "heartbeat decode empty fails gracefully");
    CHECK(!HeartbeatData::decode("not-json", dummy), "heartbeat decode garbage fails gracefully");
}

// ---------------------------------------------------------------- 枚举常量
static void testEnumConstants() {
    CHECK(std::string(ConsumeType::CONSUME_ACTIVELY) == "CONSUME_ACTIVELY", "enum CONSUME_ACTIVELY");
    CHECK(std::string(ConsumeType::CONSUME_PASSIVELY) == "CONSUME_PASSIVELY", "enum CONSUME_PASSIVELY");
    CHECK(std::string(ConsumeType::CONSUME_POP) == "CONSUME_POP", "enum CONSUME_POP");
    CHECK(std::string(MessageModel::BROADCASTING) == "BROADCASTING", "enum BROADCASTING");
    CHECK(std::string(MessageModel::CLUSTERING) == "CLUSTERING", "enum CLUSTERING");
    CHECK(std::string(MessageModel::LITE_SELECTIVE) == "LITE_SELECTIVE", "enum LITE_SELECTIVE");
    CHECK(std::string(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET) == "CONSUME_FROM_LAST_OFFSET",
          "enum CONSUME_FROM_LAST_OFFSET");
    CHECK(std::string(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET) == "CONSUME_FROM_FIRST_OFFSET",
          "enum CONSUME_FROM_FIRST_OFFSET");
    CHECK(std::string(ConsumeFromWhere::CONSUME_FROM_TIMESTAMP) == "CONSUME_FROM_TIMESTAMP",
          "enum CONSUME_FROM_TIMESTAMP");
    CHECK(std::string(ExpressionType::TAG) == "TAG", "enum ExpressionType.TAG");
    CHECK(std::string(ExpressionType::SQL92) == "SQL92", "enum ExpressionType.SQL92");
    CHECK(std::string(ExpressionType::CLASS_FILTER) == "CLASS_FILTER", "enum ExpressionType.CLASS_FILTER");

    // subVersion 默认应为当前时间戳量级（非 0）
    SubscriptionData fresh;
    CHECK(fresh.subVersion > 1600000000000LL, "subVersion defaults to current millis");
}

int main() {
    testSubscriptionData();
    testQueueData();
    testBrokerData();
    testTopicRouteData();
    testHeartbeatData();
    testEnumConstants();

    std::cout << "route/heartbeat checks: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
