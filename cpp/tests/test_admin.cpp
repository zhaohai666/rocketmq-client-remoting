// 管理端单测：fastjson2 非法 JSON 容忍、管理响应体 DTO、TopicConfig /
// SubscriptionGroupConfig 字段名与默认值、MixAll properties 语义、权限与分组判定、
// 以及 CreateTopic / QueryMessage 请求头的关键字段。
//
// 本文件里的 JSON 夹具全部是 **Java fastjson2 探针的真实输出**（含非法部分），
// 不是手写理想 JSON —— 这正是本层最容易出错的地方：
//   {"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"t"}:{...}}}
// 注意里面 map 的键是一个**内联 JSON 对象**，标准 JSON 解析器会直接报错。
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/topic_config.h"
#include "rocketmq/remoting/protocol/admin_body.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/serialize.h"
#include "rocketmq/remoting/protocol/subscription.h"

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

#define CHECK_EQ(actual, expect, msg)                                          \
    do {                                                                       \
        auto a_ = (actual);                                                    \
        auto e_ = (expect);                                                    \
        if (a_ == e_) {                                                        \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << " expect=" << e_                \
                      << " actual=" << a_ << "\n";                             \
        }                                                                      \
    } while (0)

namespace {

JsonValue parseMust(const std::string& s, const char* what) {
    JsonValue v;
    std::string err;
    if (!jsonParse(s, v, &err)) {
        ++g_fail;
        std::cout << "[FAIL] jsonParse failed for " << what << ": " << err << "\n";
    }
    return v;
}

// ---------------------------------------------------------------- 1. 非法 JSON 容忍

void testJsonToleratesFastjson2Extensions() {
    // 1.1 map 的键是**内联对象**（fastjson2 对非字符串键的做法）—— 非法 JSON
    const char* mqKeyed =
        "{\"offsetTable\":{{" 
        "\"brokerName\":\"broker-a\",\"queueId\":3,\"topic\":\"t\"}"
        ":{\"maxOffset\":2,\"minOffset\":0}}}";
    JsonValue v = parseMust(mqKeyed, "object-as-map-key");
    const JsonValue* table = v.find("offsetTable");
    CHECK(table != nullptr && table->isObject(), "object-as-key: offsetTable 是 object");
    if (table != nullptr) {
        CHECK_EQ(table->objectItems().size(), static_cast<size_t>(1),
                 "object-as-key: 恰好一个键");
        const std::string& rawKey = table->objectItems()[0].first;
        CHECK(rawKey.find("\"brokerName\"") != std::string::npos,
              "object-as-key: 键名保留原始 JSON 文本");
    }

    // 1.2 无引号数字键
    JsonValue numeric = parseMust("{\"brokerAddrs\":{0:\"127.0.0.1:10911\"}}", "numeric-key");
    const JsonValue* addrs = numeric.find("brokerAddrs");
    CHECK(addrs != nullptr && addrs->contains("0"), "numeric-key: 0 被当作字符串键 \"0\"");

    // 1.3 NaN / Infinity / -Infinity（fastjson2 会写出这三种非标准字面量）
    JsonValue nonFinite = parseMust("{\"a\":NaN,\"b\":Infinity,\"c\":-Infinity}", "non-finite");
    CHECK(nonFinite.get("a").isNumber(), "NaN 被解析为 Number");
    CHECK(nonFinite.get("b").isNumber(), "Infinity 被解析为 Number");
    CHECK(nonFinite.get("c").isNumber(), "-Infinity 被解析为 Number");
    // dump 时非有限值回落 null（JSON 无 NaN/Inf）
    CHECK(nonFinite.dump().find("null") != std::string::npos, "NaN/Inf dump 回落到 null");

    // 1.4 尾随逗号
    JsonValue trailing = parseMust("{\"a\":1,\"b\":[1,2,],}", "trailing-comma");
    CHECK_EQ(trailing.get("a").intValue(), static_cast<int64_t>(1), "trailing-comma: a");
    CHECK_EQ(trailing.get("b").size(), static_cast<size_t>(2), "trailing-comma: b 长度 2");

    // 1.5 真垃圾必须被拒绝（不能"宽容"到静默成功）
    JsonValue bad;
    std::string err;
    CHECK(!jsonParse("{\"a\":}", bad, &err), "垃圾 JSON 必须解析失败");
    CHECK(!jsonParse("", bad, &err), "空串必须解析失败");
}

// ---------------------------------------------------------------- 2. MessageQueue 键

void testMessageQueueKeyRoundTrip() {
    MessageQueue mq("MyTopic", "broker-a", 3);
    std::string key = messageQueueKey(mq);
    // fastjson2 键按字母序：brokerName, queueId, topic
    CHECK_EQ(key, std::string("{\"brokerName\":\"broker-a\",\"queueId\":3,\"topic\":\"MyTopic\"}"),
             "messageQueueKey 输出与 fastjson2 一致");

    MessageQueue back;
    CHECK(parseMessageQueueKey(key, back), "parseMessageQueueKey 成功");
    CHECK_EQ(back.topic, std::string("MyTopic"), "mq 键往返 topic");
    CHECK_EQ(back.brokerName, std::string("broker-a"), "mq 键往返 brokerName");
    CHECK_EQ(back.queueId, 3, "mq 键往返 queueId");

    // 普通字符串键不能被误判成 MessageQueue
    MessageQueue dummy;
    CHECK(!parseMessageQueueKey("0", dummy), "数字键不被当成 MessageQueue");
    CHECK(!parseMessageQueueKey("G1", dummy), "字符串键不被当成 MessageQueue");
}

// ---------------------------------------------------------------- 3. TopicStatsTable / ConsumeStats

void testTopicStatsTableFromRealFixture() {
    // Java 探针：{"offsetTable":{<MessageQueue>:{"lastUpdateTimestamp":..,"maxOffset":..,"minOffset":..}},"topicPutTps":0.0}
    const char* body =
        "{\"offsetTable\":{"
        "{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"t\"}"
        ":{\"lastUpdateTimestamp\":1789374000000,\"maxOffset\":8,\"minOffset\":0},"
        "{\"brokerName\":\"broker-a\",\"queueId\":1,\"topic\":\"t\"}"
        ":{\"lastUpdateTimestamp\":1789374000001,\"maxOffset\":2,\"minOffset\":0}"
        "},\"topicPutTps\":1.5}";

    TopicStatsTable t;
    CHECK(TopicStatsTable::decode(body, t), "TopicStatsTable::decode 成功");
    CHECK_EQ(t.offsetTable.size(), static_cast<size_t>(2), "TopicStatsTable 2 个队列");
    CHECK_EQ(t.totalMaxOffset(), static_cast<int64_t>(10), "totalMaxOffset = 8+2");
    CHECK(t.offsetTable.count(MessageQueue("t", "broker-a", 1)) == 1,
          "按 MessageQueue 能查到 queueId=1");
    CHECK_EQ(t.offsetTable[MessageQueue("t", "broker-a", 1)].maxOffset,
             static_cast<int64_t>(2), "queueId=1 的 maxOffset");

    // 往返
    Bytes encoded = t.encode();
    TopicStatsTable again;
    CHECK(TopicStatsTable::decode(encoded, again), "TopicStatsTable 往返解码");
    CHECK_EQ(again.totalMaxOffset(), static_cast<int64_t>(10), "往返后 totalMaxOffset 不变");
    CHECK_EQ(again.offsetTable.size(), static_cast<size_t>(2), "往返后队列数不变");
}

void testConsumeStatsFromRealFixture() {
    // Java 探针：{"consumeTps":1.5,"offsetTable":{<MessageQueue>:<OffsetWrapper>}}
    const char* body =
        "{\"consumeTps\":0.0,\"offsetTable\":{"
        "{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"t\"}"
        ":{\"brokerOffset\":8,\"consumerOffset\":5,\"lastTimestamp\":1,\"pullOffset\":7}"
        "}}";
    ConsumeStats cs;
    CHECK(ConsumeStats::decode(body, cs), "ConsumeStats::decode 成功");
    CHECK_EQ(cs.offsetTable.size(), static_cast<size_t>(1), "ConsumeStats 1 个队列");
    OffsetWrapper ow = cs.offsetTable[MessageQueue("t", "broker-a", 0)];
    CHECK_EQ(ow.lag(), static_cast<int64_t>(3), "OffsetWrapper.lag = 8-5");
    CHECK_EQ(cs.totalLag(), static_cast<int64_t>(3), "ConsumeStats.totalLag");
    CHECK_EQ(ow.pullOffset, static_cast<int64_t>(7), "pullOffset 被解析");
}

void testResetOffsetBodyUsesMessageQueueKeys() {
    // ResetOffsetBody.offsetTable 是 Map<MessageQueue, Long>，**不是**嵌套 map
    const char* body =
        "{\"offsetTable\":{{" 
        "\"brokerName\":\"broker-a\",\"queueId\":3,\"topic\":\"t\"}:2}}";
    ResetOffsetBody b;
    CHECK(ResetOffsetBody::decode(body, b), "ResetOffsetBody::decode 成功");
    CHECK_EQ(b.offsetTable.size(), static_cast<size_t>(1), "ResetOffsetBody 1 项");
    auto it = b.offsetTable.find(MessageQueue("t", "broker-a", 3));
    CHECK(it != b.offsetTable.end(), "ResetOffsetBody 能按 MessageQueue 查到");
    if (it != b.offsetTable.end()) {
        CHECK_EQ(it->second, static_cast<int64_t>(2), "ResetOffsetBody 位点值 = 2");
    }
}

// ---------------------------------------------------------------- 4. TopicConfig

void testTopicConfigJavaDefaultsAndAttributes() {
    // Java: new TopicConfig("t") -> 16/16/6/SINGLE_TAG/0/false/{}，且 attributes 会被序列化
    TopicConfig c("t");
    CHECK_EQ(c.readQueueNums, 16, "TopicConfig 默认 readQueueNums=16");
    CHECK_EQ(c.writeQueueNums, 16, "TopicConfig 默认 writeQueueNums=16");
    CHECK_EQ(c.perm, 6, "TopicConfig 默认 perm=6");
    CHECK_EQ(c.topicFilterType, std::string("SINGLE_TAG"), "TopicConfig 默认 SINGLE_TAG");
    CHECK_EQ(c.order, false, "TopicConfig 默认 order=false");

    JsonValue v = c.toJson();
    CHECK(v.find("attributes") != nullptr, "TopicConfig 序列化必须带 attributes（即使为空）");
    CHECK(v.find("topicFilterType") != nullptr, "TopicConfig 序列化必须带 topicFilterType");

    // Java 探针：{"attributes":{},"order":false,"perm":4,"readQueueNums":4,...}
    const char* probe =
        "{\"attributes\":{},\"order\":false,\"perm\":4,\"readQueueNums\":4,"
        "\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"t\",\"topicSysFlag\":0,"
        "\"writeQueueNums\":4}";
    TopicConfig parsed;
    CHECK(TopicConfig::decode(probe, parsed), "TopicConfig::decode 成功");
    CHECK_EQ(parsed.readQueueNums, 4, "TopicConfig 解析 readQueueNums=4");
    CHECK_EQ(parsed.perm, 4, "TopicConfig 解析 perm=4");

    // attributes 内容往返
    TopicConfig withAttrs("t");
    withAttrs.attributes["k1"] = "v1";
    TopicConfig back;
    CHECK(TopicConfig::decode(withAttrs.encode(), back), "TopicConfig 带 attributes 往返");
    CHECK_EQ(back.attributes.size(), static_cast<size_t>(1), "attributes 往返 1 项");
    CHECK_EQ(back.attributes["k1"], std::string("v1"), "attributes 往返值");

    // permToString 三位标志
    CHECK_EQ(PermName::permToString(6), std::string("RW-"), "perm=6 -> RW-");
    CHECK_EQ(PermName::permToString(4), std::string("R--"), "perm=4 -> R--");
    CHECK_EQ(PermName::permToString(2), std::string("-W-"), "perm=2 -> -W-");
    CHECK_EQ(PermName::permToString(7), std::string("RWX"), "perm=7 -> RWX");
}

// ---------------------------------------------------------------- 5. SubscriptionGroupConfig

void testSubscriptionGroupConfigProbeFixture() {
    // Java 探针：JSON.toJSONString(new SubscriptionGroupConfig("MyGroup"))
    const char* probe =
        "{\"attributes\":{},\"brokerId\":0,\"consumeBroadcastEnable\":true,"
        "\"consumeEnable\":true,\"consumeFromMinEnable\":true,\"consumeMessageOrderly\":false,"
        "\"consumeTimeoutMinute\":15,\"groupName\":\"MyGroup\","
        "\"groupRetryPolicy\":{\"type\":\"CUSTOMIZED\"},\"groupSysFlag\":0,"
        "\"notifyConsumerIdsChangedEnable\":true,\"retryMaxTimes\":16,\"retryQueueNums\":1,"
        "\"whichBrokerWhenConsumeSlowly\":1}";

    SubscriptionGroupConfig cfg;
    CHECK(SubscriptionGroupConfig::decode(probe, cfg), "SubscriptionGroupConfig::decode 成功");
    CHECK_EQ(cfg.groupName, std::string("MyGroup"), "groupName");
    CHECK_EQ(cfg.retryMaxTimes, 16, "retryMaxTimes=16");
    CHECK_EQ(cfg.retryQueueNums, 1, "retryQueueNums=1");
    CHECK_EQ(cfg.brokerId, 0, "brokerId=0（MASTER_ID）");
    CHECK_EQ(cfg.whichBrokerWhenConsumeSlowly, 1, "whichBrokerWhenConsumeSlowly=1");
    CHECK_EQ(cfg.consumeTimeoutMinute, 15, "consumeTimeoutMinute=15");
    CHECK_EQ(cfg.groupRetryPolicy.type, std::string("CUSTOMIZED"), "groupRetryPolicy.type");
    CHECK_EQ(cfg.hasSubscriptionDataSet, false, "subscriptionDataSet 为 null 时不出现");

    // 默认构造也应与 Java 默认值一致
    SubscriptionGroupConfig def("g");
    CHECK_EQ(def.retryMaxTimes, 16, "默认 retryMaxTimes=16");
    CHECK_EQ(def.consumeEnable, true, "默认 consumeEnable=true");
    CHECK_EQ(def.consumeFromMinEnable, true, "默认 consumeFromMinEnable=true");
    CHECK_EQ(def.consumeBroadcastEnable, true, "默认 consumeBroadcastEnable=true");
    CHECK_EQ(def.notifyConsumerIdsChangedEnable, true, "默认 notifyConsumerIdsChangedEnable=true");
    CHECK_EQ(def.consumeMessageOrderly, false, "默认 consumeMessageOrderly=false");

    // subscriptionDataSet 未设置时不应出现在 JSON 里（fastjson2 跳过 null）
    JsonValue defJson = def.toJson();
    CHECK(defJson.find("subscriptionDataSet") == nullptr,
          "未设置时 subscriptionDataSet 不序列化");

    // 往返
    def.retryMaxTimes = 3;
    SubscriptionGroupConfig again;
    CHECK(SubscriptionGroupConfig::decode(def.encode(), again), "SubscriptionGroupConfig 往返");
    CHECK_EQ(again.retryMaxTimes, 3, "往返后 retryMaxTimes=3");
    CHECK_EQ(again.groupName, std::string("g"), "往返后 groupName");
}

void testSubscriptionGroupWrapperAndMerge() {
    const char* probe =
        "{\"dataVersion\":{\"counter\":1,\"timestamp\":1789374000000},"
        "\"forbiddenTable\":{},"
        "\"subscriptionGroupTable\":{\"g1\":{\"brokerId\":0,\"consumeEnable\":true,"
        "\"groupName\":\"g1\",\"groupRetryPolicy\":{\"type\":\"CUSTOMIZED\"},"
        "\"retryMaxTimes\":5}}}";
    SubscriptionGroupWrapper w;
    CHECK(SubscriptionGroupWrapper::decode(probe, w), "SubscriptionGroupWrapper::decode 成功");
    CHECK_EQ(w.subscriptionGroupTable.size(), static_cast<size_t>(1), "wrapper 1 个组");
    CHECK_EQ(w.subscriptionGroupTable["g1"].retryMaxTimes, 5, "wrapper 组 retryMaxTimes=5");
    CHECK(!w.dataVersion.isNull(), "dataVersion 被保留（翻页要原样回传）");

    // 分页累积语义：mergeFrom 覆盖同名、追加新名
    SubscriptionGroupWrapper page2;
    page2.subscriptionGroupTable["g2"] = SubscriptionGroupConfig("g2");
    page2.subscriptionGroupTable["g1"] = SubscriptionGroupConfig("g1");
    w.mergeFrom(page2);
    CHECK_EQ(w.subscriptionGroupTable.size(), static_cast<size_t>(2), "merge 后 2 个组");
    CHECK_EQ(w.subscriptionGroupTable["g1"].retryMaxTimes, 16,
             "merge 覆盖同名（g1 回到默认 16）");
    CHECK(w.subscriptionGroupTable.count("g2") == 1, "merge 追加 g2");
}

void testTopicConfigSerializeWrapper() {
    const char* probe =
        "{\"dataVersion\":{\"counter\":0,\"timestamp\":1},"
        "\"topicConfigTable\":{\"t\":{\"attributes\":{},\"order\":false,\"perm\":6,"
        "\"readQueueNums\":16,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"t\","
        "\"topicSysFlag\":0,\"writeQueueNums\":16}}}";
    TopicConfigSerializeWrapper w;
    CHECK(TopicConfigSerializeWrapper::decode(probe, w), "TopicConfigSerializeWrapper::decode");
    CHECK_EQ(w.topicConfigTable.size(), static_cast<size_t>(1), "1 个 topicConfig");
    CHECK_EQ(w.topicConfigTable["t"].writeQueueNums, 16, "topicConfig writeQueueNums=16");
}

void testQueryConsumeQueueResponseBody() {
    const char* probe =
        "{\"filterData\":\"*\",\"maxQueueIndex\":88,\"minQueueIndex\":1,"
        "\"subscriptionData\":{\"classFilterMode\":false,\"expressionType\":\"TAG\","
        "\"subString\":\"*\",\"topic\":\"t\",\"version\":1}}";
    QueryConsumeQueueResponseBody b;
    CHECK(QueryConsumeQueueResponseBody::decode(probe, b), "QueryConsumeQueue decode 成功");
    CHECK_EQ(b.minQueueIndex, static_cast<int64_t>(1), "minQueueIndex=1");
    CHECK_EQ(b.maxQueueIndex, static_cast<int64_t>(88), "maxQueueIndex=88");
    CHECK_EQ(b.filterData, std::string("*"), "filterData=*");
    CHECK_EQ(b.hasQueueData, false, "queueData 为 null 时不出现");
    CHECK(!b.subscriptionData.isNull(), "subscriptionData 被透传");

    // 带 queueData 的形态
    const char* withQ =
        "{\"maxQueueIndex\":2,\"minQueueIndex\":0,\"queueData\":["
        "{\"bitMap\":null,\"eval\":false,\"physicOffset\":0,\"physicSize\":100,\"tagsCode\":0}]}";
    QueryConsumeQueueResponseBody b2;
    CHECK(QueryConsumeQueueResponseBody::decode(withQ, b2), "QueryConsumeQueue(带 queueData)");
    CHECK_EQ(b2.hasQueueData, true, "queueData 被解析");
    CHECK_EQ(b2.queueData.size(), static_cast<size_t>(1), "queueData 1 项");
    CHECK_EQ(b2.queueData[0].physicSize, static_cast<int64_t>(100), "physicSize=100");
    CHECK_EQ(b2.queueData[0].hasBitMap, false, "bitMap 为 null");
}

// ---------------------------------------------------------------- 6. 其余公共 body

void testPublicBodies() {
    // KVTable
    KVTable kv;
    CHECK(KVTable::decode(std::string("{\"table\":{\"k1\":\"v1\",\"k2\":\"v2\"}}"), kv),
          "KVTable decode");
    CHECK_EQ(kv.table.size(), static_cast<size_t>(2), "KVTable 2 项");
    CHECK_EQ(kv.table["k1"], std::string("v1"), "KVTable k1=v1");

    // TopicList
    TopicList tl;
    CHECK(TopicList::decode(std::string("{\"topicList\":[\"a\",\"b\"]}"), tl), "TopicList decode");
    CHECK_EQ(tl.topicList.size(), static_cast<size_t>(2), "TopicList 2 项");
    CHECK(tl.contains("b"), "TopicList contains b");
    CHECK(!tl.contains("c"), "TopicList 不含 c");

    // ClusterInfo：真实 broker 形态（brokerAddrTable[name] 是 BrokerData）
    const char* clusterJson =
        "{\"brokerAddrTable\":{\"broker-a\":{\"brokerAddrs\":{\"0\":\"127.0.0.1:10911\"},"
        "\"brokerName\":\"broker-a\",\"cluster\":\"DefaultCluster\",\"enableActingMaster\":false}},"
        "\"clusterAddrTable\":{\"DefaultCluster\":[\"broker-a\"]}}";
    ClusterInfo ci;
    CHECK(ClusterInfo::decode(clusterJson, ci), "ClusterInfo decode 成功");
    CHECK_EQ(ci.brokerAddrTable.size(), static_cast<size_t>(1), "ClusterInfo 1 个 broker");
    std::vector<std::string> addrs = ci.getBrokerAddrs();
    CHECK_EQ(addrs.size(), static_cast<size_t>(1), "getBrokerAddrs 1 项");
    if (!addrs.empty()) CHECK_EQ(addrs[0], std::string("127.0.0.1:10911"), "broker 地址");
    std::vector<std::string> cl = ci.getBrokerAddrsOfCluster("DefaultCluster");
    CHECK_EQ(cl.size(), static_cast<size_t>(1), "按集群查 broker 地址");
    CHECK_EQ(ci.getBrokerAddrsOfCluster("NoSuchCluster").size(), static_cast<size_t>(0),
             "未知集群返回空");

    // GetConsumerListByGroupResponseBody
    GetConsumerListByGroupResponseBody g;
    CHECK(GetConsumerListByGroupResponseBody::decode(
              std::string("{\"consumerIdList\":[\"c1\",\"c2\"]}"), g),
          "GetConsumerListByGroup decode");
    CHECK_EQ(g.consumerIdList.size(), static_cast<size_t>(2), "consumerIdList 2 项");

    // ConsumeStatsList：JSON 键必须是 Java 字段名 consumeStatsList（早期实现写成
    // statsList，真机 341 响应会被解析成空集合，看着像 broker 没有积压）
    ConsumeStatsList sl;
    CHECK(ConsumeStatsList::decode(
              std::string("{\"consumeStatsList\":[{\"G_BROKER\":\"x\"}],"
                          "\"brokerAddr\":\"127.0.0.1:10911\",\"totalDiff\":7,"
                          "\"totalInflightDiff\":2}"),
              sl),
          "ConsumeStatsList decode");
    CHECK_EQ(sl.statsList.size(), static_cast<size_t>(1), "consumeStatsList 1 行");
    CHECK(sl.hasBrokerAddr && sl.brokerAddr == "127.0.0.1:10911", "ConsumeStatsList brokerAddr");
    CHECK_EQ(sl.totalDiff, static_cast<long long>(7), "ConsumeStatsList totalDiff");
    CHECK_EQ(sl.totalInflightDiff, static_cast<long long>(2), "ConsumeStatsList totalInflightDiff");
    ConsumeStatsList wrongKey;
    CHECK(ConsumeStatsList::decode(std::string("{\"statsList\":[{\"G\":\"x\"}]}"), wrongKey),
          "旧键名 JSON 仍可解析（不报错）");
    CHECK_EQ(wrongKey.statsList.size(), static_cast<size_t>(0), "旧键名 statsList 被忽略");
}

// ---------------------------------------------------------------- 7. MixAll properties

void testPropertiesJavaSemantics() {
    // 与 Python test_properties_text_round_trip_matches_java 同一夹具
    const char* text = "a = b\n  c : d  \nk=v\n#comment\n!c2\nempty=\ncont=first\\\n    second\n";
    PropertyMap p = MixAll::string2Properties(text);
    CHECK_EQ(p["a"], std::string("b"), "properties: 键去空白、值去左空白");
    CHECK_EQ(p["c"], std::string("d  "), "properties: 值保留尾部空白");
    CHECK_EQ(p["k"], std::string("v"), "properties: 基本 k=v");
    CHECK_EQ(p["empty"], std::string(""), "properties: 空值");
    CHECK_EQ(p["cont"], std::string("firstsecond"), "properties: \\ 续行拼接");
    CHECK(p.count("#comment") == 0, "properties: # 注释被跳过");
    CHECK(p.count("!c2") == 0, "properties: ! 注释被跳过");

    // 空白也是合法分隔符（java.util.Properties.load 语义）
    PropertyMap p2 = MixAll::string2Properties("a b\nonlykey\nk=v\n");
    CHECK_EQ(p2["a"], std::string("b"), "properties: 空白作分隔符");
    CHECK_EQ(p2["onlykey"], std::string(""), "properties: 只有键");
    PropertyMap p3 = MixAll::string2Properties("a b=c=d\n");
    CHECK_EQ(p3["a"], std::string("b=c=d"), "properties: 空白分隔时值里的 = 保留");
    PropertyMap p4 = MixAll::string2Properties("messageDelayLevel=1s 5s 10s\n");
    CHECK_EQ(p4["messageDelayLevel"], std::string("1s 5s 10s"),
             "properties: 值里的空格保留（分隔符取 '='）");

    // properties2String：每行 "k=v\n"
    PropertyMap m;
    m["brokerName"] = "broker-a";
    m["listenPort"] = "10911";
    std::string out = MixAll::properties2String(m);
    CHECK_EQ(out, std::string("brokerName=broker-a\nlistenPort=10911\n"),
             "properties2String 输出格式");

    // 往返：解析(序列化(x)) == x
    PropertyMap back = MixAll::string2Properties(MixAll::properties2String(m));
    CHECK_EQ(back.size(), m.size(), "properties 往返项数");

    // 真实 broker 配置片段（值里带空格，且含注释）
    const char* realish =
        "# broker config\n"
        "brokerClusterName=DefaultCluster\n"
        "messageDelayLevel=1s 5s 10s 30s 1m 2m 3m\n"
        "autoCreateTopicEnable=true\n";
    PropertyMap rp = MixAll::string2Properties(realish);
    CHECK_EQ(rp.size(), static_cast<size_t>(3), "真实片段解析出 3 个键");
    CHECK_EQ(rp["brokerClusterName"], std::string("DefaultCluster"), "brokerClusterName");
    CHECK_EQ(rp["messageDelayLevel"], std::string("1s 5s 10s 30s 1m 2m 3m"), "messageDelayLevel");
}

// ---------------------------------------------------------------- 8. 权限 / 分组判定

void testPermAndGroupHelpers() {
    // Java PermName.isValid：0 <= perm < PERM_PRIORITY(8)
    CHECK(PermName::isValid(0), "isValid(0)");
    CHECK(PermName::isValid(7), "isValid(7)");
    CHECK(!PermName::isValid(8), "isValid(8) 非法");
    CHECK(!PermName::isValid(-1), "isValid(-1) 非法");

    // LMQ / 系统消费组 / 预定义组
    CHECK(MixAll::isLmq("%LMQ%topic"), "isLmq 命中");
    CHECK(!MixAll::isLmq("normal"), "isLmq 不命中");
    CHECK(MixAll::isSysConsumerGroup("CID_RMQ_SYS_FOO"), "isSysConsumerGroup 命中");
    CHECK(!MixAll::isSysConsumerGroup("NormalGroup"), "isSysConsumerGroup 不命中");
    CHECK(MixAll::isPredefinedGroup(MixAll::TOOLS_CONSUMER_GROUP), "预定义组命中");
    CHECK(MixAll::isPredefinedGroup(MixAll::DEFAULT_CONSUMER_GROUP), "默认消费组是预定义组");
    CHECK(!MixAll::isPredefinedGroup("MyBizGroup"), "业务组不是预定义组");
    CHECK(MixAll::isSysTopic("rmq_sys_TRACE_DATA"), "isSysTopic 命中");
    CHECK(!MixAll::isSysTopic("MyTopic"), "isSysTopic 不命中");

    // UNIQUE_MSG_QUERY_FLAG 是 extFields 的**键名**，不是标志位
    CHECK_EQ(std::string(MixAll::UNIQUE_MSG_QUERY_FLAG), std::string("_UNIQUE_KEY_QUERY"),
             "UNIQUE_MSG_QUERY_FLAG 是键名");
    CHECK_EQ(QueryMsgType::ALL_MESSAGE, 0, "QueryMsgType::ALL_MESSAGE=0");
    CHECK_EQ(QueryMsgType::UNIQUE_KEY, 1, "QueryMsgType::UNIQUE_KEY=1");
    CHECK_EQ(QueryMsgType::NORMAL, 2, "QueryMsgType::NORMAL=2");
    CHECK_EQ(std::string(MessageConst::INDEX_KEY_TYPE), std::string("K"), "INDEX_KEY_TYPE=K");
    CHECK_EQ(std::string(MessageConst::INDEX_UNIQUE_TYPE), std::string("U"), "INDEX_UNIQUE_TYPE=U");
}

// ---------------------------------------------------------------- 9. 请求头关键字段

void testCreateTopicRequestHeaderSendsTopicFilterType() {
    // broker 的 CreateTopicRequestHeader.checkFields() 会拒绝 topicFilterType 为空：
    // "topicFilterType = [null] value invalid"。这条曾在 Python 侧被真实 broker 打回。
    CreateTopicRequestHeader h;
    h.topic = "t";
    h.defaultTopic = "TBW102";
    h.readQueueNums = 4;
    h.writeQueueNums = 4;
    h.perm = 6;
    h.topicFilterType = TopicFilterType::SINGLE_TAG;
    h.topicSysFlag = 0;
    h.order = false;
    h.attributes = "";
    h.force = false;
    PropertyMap ext = h.toExtFields();
    CHECK_EQ(ext["topicFilterType"], std::string("SINGLE_TAG"), "createTopic 必须下发 topicFilterType");
    CHECK_EQ(ext["attributes"], std::string(""), "attributes 必须是空串而非 null");
    CHECK_EQ(ext["force"], std::string("false"), "force 必须是小写 false（Java Boolean.toString）");
    CHECK(ext.count("topic") == 1 && ext.count("perm") == 1, "topic/perm 一并下发");

    // 未设置的字段不应出现（Java "非空才写" 语义）
    CreateTopicRequestHeader minimal;
    minimal.topic = "t";
    PropertyMap ext2 = minimal.toExtFields();
    CHECK(ext2.count("topicFilterType") == 0, "未设置时 topicFilterType 不写");
}

void testQueryMessageRequestHeaderIndexType() {
    QueryMessageRequestHeader h;
    h.topic = "t";
    h.key = "k";
    h.maxNum = 32;
    h.beginTimestamp = 1;
    h.endTimestamp = 2;
    h.indexType = MessageConst::INDEX_UNIQUE_TYPE;
    PropertyMap ext = h.toExtFields();
    CHECK_EQ(ext["indexType"], std::string("U"), "indexType 下发");
    CHECK(ext.count("lastKey") == 0, "未设置 lastKey 时不写");

    // 反解
    QueryMessageRequestHeader back;
    back.fromExtFields(ext);
    CHECK_EQ(back.indexType.value_or(""), std::string("U"), "indexType 回读");
    CHECK_EQ(back.key.value_or(""), std::string("k"), "key 回读");
}

void testSearchOffsetRequestHeaderBoundaryType() {
    // Java DefaultMQAdminExt:133/:137 —— 入网文本是 Enum.toString() 的大写枚举名，
    // 未设置时整键不写（@CFNullable），回解走 BoundaryType.getType 的宽松语义。
    SearchOffsetRequestHeader h;
    h.topic = "t";
    h.queueId = 2;
    h.timestamp = 1700000000000LL;
    PropertyMap ext = h.toExtFields();
    CHECK(ext.count("boundaryType") == 0, "未设置边界类型时不写键（@CFNullable）");

    h.boundaryType = BoundaryType::LOWER;
    CHECK_EQ(h.toExtFields()["boundaryType"], std::string("LOWER"), "LOWER 入网为大写枚举名");
    h.boundaryType = BoundaryType::UPPER;
    PropertyMap up = h.toExtFields();
    CHECK_EQ(up["boundaryType"], std::string("UPPER"), "UPPER 入网为大写枚举名");

    SearchOffsetRequestHeader back;
    back.fromExtFields(up);
    CHECK(back.boundaryType.value_or(BoundaryType::LOWER) == BoundaryType::UPPER,
          "boundaryType 回读 UPPER");
    CHECK_EQ(boundaryTypeLowercaseName(BoundaryType::UPPER), std::string("upper"),
             "getName() 小写名");
    // 宽松解析：只有 upper（大小写不敏感）才是 UPPER
    for (const char* text : {"UPPER", "upper", "Upper"}) {
        CHECK(boundaryTypeFromString(text) == BoundaryType::UPPER, std::string(text) + " ⇒ UPPER");
    }
    for (const char* text : {"LOWER", "lower", "", "junk"}) {
        CHECK(boundaryTypeFromString(text) == BoundaryType::LOWER, std::string(text) + " ⇒ LOWER");
    }
    SearchOffsetRequestHeader empty;
    empty.fromExtFields(PropertyMap{});
    CHECK(!empty.boundaryType.has_value(), "缺键回 nullopt");
}

}  // namespace

int main() {
    testJsonToleratesFastjson2Extensions();
    testMessageQueueKeyRoundTrip();
    testTopicStatsTableFromRealFixture();
    testConsumeStatsFromRealFixture();
    testResetOffsetBodyUsesMessageQueueKeys();
    testTopicConfigJavaDefaultsAndAttributes();
    testSubscriptionGroupConfigProbeFixture();
    testSubscriptionGroupWrapperAndMerge();
    testTopicConfigSerializeWrapper();
    testQueryConsumeQueueResponseBody();
    testPublicBodies();
    testPropertiesJavaSemantics();
    testPermAndGroupHelpers();
    testCreateTopicRequestHeaderSendsTopicFilterType();
    testQueryMessageRequestHeaderIndexType();
    testSearchOffsetRequestHeaderBoundaryType();

    std::cout << "admin: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
